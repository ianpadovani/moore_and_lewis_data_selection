#!/usr/bin/env python3
"""
Moore & Lewis (2010) domain data selection using KenLM.
Python reimplementation of ml_select.sh — streams parquet corpora without
loading them fully into memory. Requires lmplz and build_binary from KenLM.
"""

import argparse
import glob as _glob
import os
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Iterator, Optional

import threading

import pyarrow.parquet as pq


# ── Logging ───────────────────────────────────────────────────────────────────

_log_fh: Optional[object] = None
TOTAL_STEPS = 10


def _ts() -> str:
    return time.strftime("%H:%M:%S", time.gmtime())


def log(msg: str) -> None:
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    if _log_fh:
        print(line, file=_log_fh, flush=True)


def stage_start(n: int, desc: str) -> float:
    log(f"==> [{n}/{TOTAL_STEPS}] {desc}")
    return time.time()


def stage_done(n: int, desc: str, t0: float) -> None:
    s = int(time.time() - t0)
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    dur = f"{h}h {m}m {s}s" if h else f"{m}m {s}s" if m else f"{s}s"
    log(f"==> [{n}/{TOTAL_STEPS}] {desc} completed ({dur})")


# ── File discovery ────────────────────────────────────────────────────────────

def discover_files(prefix: str) -> list[str]:
    """Return sorted list of parquet files matching a directory or path prefix."""
    if prefix.startswith("s3://"):
        import s3fs
        fs = s3fs.S3FileSystem()
        bucket_path = prefix.removeprefix("s3://")
        if fs.isdir(bucket_path):
            files = sorted(f"s3://{p}" for p in fs.glob(f"{bucket_path}/*.parquet"))
        else:
            files = sorted(f"s3://{p}" for p in fs.glob(f"{bucket_path}*.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet files found at {prefix}")
        return files
    else:
        p = Path(prefix)
        if p.is_dir():
            files = sorted(str(f) for f in p.glob("*.parquet"))
        else:
            files = sorted(_glob.glob(f"{prefix}*.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet files found at {prefix}")
        return files


def open_parquet(path: str) -> pq.ParquetFile:
    if path.startswith("s3://"):
        import s3fs
        return pq.ParquetFile(s3fs.S3FileSystem().open(path, "rb"))
    return pq.ParquetFile(path)


# ── Parquet streaming ─────────────────────────────────────────────────────────

def stream_col(files: list[str], col: str, chunk_size: int) -> Iterator[list[str]]:
    """Yield lists of strings from one column across multiple parquet files."""
    for path in files:
        pf = open_parquet(path)
        for batch in pf.iter_batches(batch_size=chunk_size, columns=[col]):
            yield [str(v) if v is not None else "" for v in batch.column(col).to_pylist()]


def stream_cols(
    files: list[str], cols: list[str], chunk_size: int
) -> Iterator[dict[str, list[str]]]:
    """Yield dicts of column→list across multiple parquet files."""
    for path in files:
        pf = open_parquet(path)
        for batch in pf.iter_batches(batch_size=chunk_size, columns=cols):
            yield {
                col: [str(v) if v is not None else "" for v in batch.column(col).to_pylist()]
                for col in cols
            }


# ── Algorithm steps ───────────────────────────────────────────────────────────

def count_rows(files: list[str], col: str, chunk_size: int) -> int:
    total = 0
    for batch in stream_col(files, col, chunk_size):
        total += len(batch)
    return total


def extract_vocab(
    files: list[str], col: str, chunk_size: int, label: str
) -> frozenset[str]:
    """Count all tokens in col across files; return non-singleton types as a frozen set."""
    counter: Counter = Counter()
    n = 0
    for batch in stream_col(files, col, chunk_size):
        for text in batch:
            counter.update(text.split())
        n += len(batch)
        log(f"    {label}: {n:,} rows processed ({len(counter):,} types)...")
    vocab = frozenset(w for w, c in counter.items() if c > 1)
    log(f"    {label}: {len(counter):,} types → {len(vocab):,} non-singletons kept")
    return vocab


def write_filtered_text(
    files: list[str],
    col: str,
    vocab: frozenset[str],
    out_path: str,
    chunk_size: int,
    max_lines: Optional[int] = None,
) -> int:
    """Stream col to a text file, replacing OOV tokens with <unk>. Returns line count."""
    written = 0
    done = False
    with open(out_path, "w", encoding="utf-8") as f:
        for batch in stream_col(files, col, chunk_size):
            lines: list[str] = []
            for text in batch:
                if max_lines is not None and written >= max_lines:
                    done = True
                    break
                words = text.split()
                lines.append(" ".join(w if w in vocab else "<unk>" for w in words))
                written += 1
            if lines:
                f.write("\n".join(lines) + "\n")
            if done:
                break
    return written


def train_lm(
    text_path: str, model_path: str, order: int, tmpdir: str, kenlm_bin: str
) -> None:
    """Train a KenLM binary LM from text_path using lmplz | build_binary."""
    lmplz_bin = os.path.join(kenlm_bin, "lmplz")
    build_bin = os.path.join(kenlm_bin, "build_binary")

    stdin_file = open(text_path, "rb")
    lmplz_proc = subprocess.Popen(
        [lmplz_bin, "-o", str(order), "-S", "80%", "-T", tmpdir,
         "--discount_fallback", "--skip_symbols"],
        stdin=stdin_file,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    stdin_file.close()

    build_proc = subprocess.Popen(
        [build_bin, "/dev/stdin", model_path],
        stdin=lmplz_proc.stdout,
        stdout=subprocess.DEVNULL,
        stderr=sys.stderr,
    )
    lmplz_proc.stdout.close()
    build_proc.wait()
    lmplz_proc.wait()

    if lmplz_proc.returncode != 0:
        raise RuntimeError(f"lmplz failed (exit {lmplz_proc.returncode})")
    if build_proc.returncode != 0:
        raise RuntimeError(f"build_binary failed (exit {build_proc.returncode})")


def score_corpus(
    files: list[str],
    col: str,
    vocab: frozenset[str],
    model_path: str,
    score_path: str,
    chunk_size: int,
    label: str,
    kenlm_bin: str,
) -> None:
    """Score every sentence via kenlm's query binary; write one log10-P per line.

    kenlm query outputs 'Total: <logP> OOV: <N>' for each input sentence.
    We feed filtered sentences (OOV→<unk>) via stdin and capture these lines.
    A writer thread and a reader thread run concurrently to avoid pipe deadlock.
    """
    query_bin = os.path.join(kenlm_bin, "query")

    proc = subprocess.Popen(
        [query_bin, model_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=65536,
    )

    n_in: list[int] = [0]
    n_out: list[int] = [0]
    write_exc: list[Optional[BaseException]] = [None]

    def _write() -> None:
        try:
            for batch in stream_col(files, col, chunk_size):
                lines: list[str] = []
                for text in batch:
                    words = text.split()
                    filtered = " ".join(w if w in vocab else "<unk>" for w in words)
                    lines.append(filtered if filtered else "<unk>")
                proc.stdin.write(("\n".join(lines) + "\n").encode("utf-8"))
                n_in[0] += len(lines)
        except Exception as exc:
            write_exc[0] = exc
        finally:
            proc.stdin.close()

    def _read() -> None:
        buf: list[str] = []
        with open(score_path, "w") as f:
            for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").strip()
                # Each sentence produces one line ending with "... Total: -X.XXX OOV: N"
                if "Total:" in line:
                    score = line.split("Total:")[1].strip().split()[0]
                    buf.append(score)
                    n_out[0] += 1
                    if n_out[0] % 5_000_000 == 0:
                        log(f"    {label}: {n_out[0]:,} sentences scored")
                    if len(buf) >= 100_000:
                        f.write("\n".join(buf) + "\n")
                        buf = []
            if buf:
                f.write("\n".join(buf) + "\n")

    t_write = threading.Thread(target=_write, daemon=True)
    t_read = threading.Thread(target=_read, daemon=True)
    t_write.start()
    t_read.start()
    t_write.join()
    t_read.join()
    proc.wait()

    if write_exc[0]:
        raise RuntimeError(f"query write thread failed: {write_exc[0]}") from write_exc[0]
    if proc.returncode != 0:
        raise RuntimeError(f"kenlm query exited with code {proc.returncode}")
    if n_in[0] != n_out[0]:
        raise RuntimeError(
            f"Score count mismatch for {label}: {n_in[0]} sentences in, {n_out[0]} scores out"
        )
    log(f"    {label}: {n_out[0]:,} sentences total")


def compute_diff(specific_path: str, general_path: str, diff_path: str) -> None:
    """Write logP_specific - logP_general per sentence (higher = more domain-specific)."""
    with (
        open(specific_path) as fs,
        open(general_path) as fg,
        open(diff_path, "w") as fd,
    ):
        buf: list[str] = []
        for s_line, g_line in zip(fs, fg):
            s, g = s_line.strip(), g_line.strip()
            if s and g:
                buf.append(f"{float(s) - float(g):.6f}")
            if len(buf) >= 100_000:
                fd.write("\n".join(buf) + "\n")
                buf = []
        if buf:
            fd.write("\n".join(buf) + "\n")


def sum_diffs(src_diff: str, tgt_diff: str, summed_path: str) -> None:
    """Sum source and target score differences for bilingual ranking."""
    with (
        open(src_diff) as fs,
        open(tgt_diff) as ft,
        open(summed_path, "w") as fo,
    ):
        buf: list[str] = []
        for s_line, t_line in zip(fs, ft):
            s, t = s_line.strip(), t_line.strip()
            if s and t:
                buf.append(f"{float(s) + float(t):.6f}")
            if len(buf) >= 100_000:
                fo.write("\n".join(buf) + "\n")
                buf = []
        if buf:
            fo.write("\n".join(buf) + "\n")


def build_tsv(
    score_path: str,
    general_files: list[str],
    src_col: str,
    tgt_col: str,
    tsv_path: str,
    chunk_size: int,
) -> None:
    """Merge score file with source and target text into a tab-separated file."""
    n = 0
    score_fh = open(score_path)
    try:
        with open(tsv_path, "w", encoding="utf-8") as out:
            for batch in stream_cols(general_files, [src_col, tgt_col], chunk_size):
                srcs = batch[src_col]
                tgts = batch[tgt_col]
                lines: list[str] = []
                for src, tgt in zip(srcs, tgts):
                    score = next(score_fh).strip()
                    lines.append(f"{score}\t{src}\t{tgt}")
                out.write("\n".join(lines) + "\n")
                n += len(srcs)
                if n % 10_000_000 == 0:
                    log(f"    TSV: {n:,} rows written")
    finally:
        score_fh.close()
    log(f"    TSV: {n:,} rows total")


def sort_and_split(
    tsv_path: str, src_out: str, tgt_out: str, tmpdir: str
) -> None:
    """Sort TSV descending by score, deduplicate consecutive rows, split into two files."""
    nproc = os.cpu_count() or 4
    # sort -rn: numeric, descending (highest score = most domain-specific = first)
    sort_proc = subprocess.Popen(
        [
            "sort", "-rn", "-S", "80%", f"--parallel={nproc}",
            "-T", tmpdir, "-t", "\t", "-k", "1,1", tsv_path,
        ],
        stdout=subprocess.PIPE,
    )
    uniq_proc = subprocess.Popen(
        ["uniq"],
        stdin=sort_proc.stdout,
        stdout=subprocess.PIPE,
    )
    sort_proc.stdout.close()

    n = 0
    with (
        open(src_out, "w", encoding="utf-8") as fs,
        open(tgt_out, "w", encoding="utf-8") as ft,
    ):
        for raw in uniq_proc.stdout:
            parts = raw.decode("utf-8").rstrip("\n").split("\t", 2)
            if len(parts) == 3:
                fs.write(parts[1] + "\n")
                ft.write(parts[2] + "\n")
                n += 1
                if n % 10_000_000 == 0:
                    log(f"    Output: {n:,} sentence pairs written")

    uniq_proc.wait()
    sort_proc.wait()
    if sort_proc.returncode != 0:
        raise RuntimeError(f"sort failed (exit {sort_proc.returncode})")
    log(f"    Output: {n:,} sentence pairs total")


# ── CLI helpers ───────────────────────────────────────────────────────────────

def resolve_kenlm_bin(arg: Optional[str]) -> str:
    if arg:
        if os.path.isfile(os.path.join(arg, "lmplz")):
            return arg
        raise FileNotFoundError(f"lmplz not found in --kenlm-bin directory: {arg}")
    env = os.environ.get("KENLM")
    if env:
        for candidate in [os.path.join(env, "bin"), env]:
            if os.path.isfile(os.path.join(candidate, "lmplz")):
                return candidate
    result = subprocess.run(["which", "lmplz"], capture_output=True, text=True)
    if result.returncode == 0:
        return os.path.dirname(result.stdout.strip())
    raise RuntimeError(
        "lmplz not found. Set --kenlm-bin, set $KENLM, or run scripts/setup_kenlm.sh"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Moore & Lewis (2010) domain data selection using KenLM",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--general", required=True,
                   help="S3 URI or local path prefix/dir of general corpus parquet(s)")
    p.add_argument("--specific", required=True,
                   help="S3 URI or local path prefix/dir of specific corpus parquet(s)")
    p.add_argument("--dest", required=True,
                   help="Destination directory for sorted output files")
    p.add_argument("--src-lang", required=True, help="Source language code, e.g. en")
    p.add_argument("--tgt-lang", required=True, help="Target language code, e.g. fr")
    p.add_argument("--src-col", default=None,
                   help="Parquet column for source text (default: --src-lang value). "
                        "Use 'source_tokens' for pre-tokenized text or 'source' for raw.")
    p.add_argument("--tgt-col", default=None,
                   help="Parquet column for target text (default: --tgt-lang value). "
                        "Use 'target_tokens' for pre-tokenized text or 'target' for raw.")
    p.add_argument("--rank-src", default="true",
                   help="Rank by source-side LM (true/false)")
    p.add_argument("--rank-tgt", default="true",
                   help="Rank by target-side LM (true/false)")
    p.add_argument("--kenlm-bin", default=None,
                   help="Dir containing lmplz binary (default: $KENLM/bin or PATH)")
    p.add_argument("--log-file", default=None,
                   help="Also write log output to this file")
    p.add_argument("--chunk-size", type=int, default=500_000,
                   help="Parquet read chunk size in rows")
    return p.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    global _log_fh
    args = parse_args()

    if args.log_file:
        _log_fh = open(args.log_file, "w", encoding="utf-8")

    script_start = time.time()

    src_col = args.src_col or args.src_lang
    tgt_col = args.tgt_col or args.tgt_lang
    rank_src = args.rank_src.lower() == "true"
    rank_tgt = args.rank_tgt.lower() == "true"

    if not rank_src and not rank_tgt:
        sys.exit("ERROR: at least one of --rank-src / --rank-tgt must be true")

    active: list[tuple[str, str]] = []
    if rank_src:
        active.append((args.src_lang, src_col))
    if rank_tgt:
        active.append((args.tgt_lang, tgt_col))
    bilingual = rank_src and rank_tgt

    kenlm_bin = resolve_kenlm_bin(args.kenlm_bin)
    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)
    tmpdir = str(dest / "temp")
    Path(tmpdir).mkdir(parents=True, exist_ok=True)

    # 1 ── Setup ───────────────────────────────────────────────────────────────
    t0 = stage_start(1, "Setup")
    general_files = discover_files(args.general)
    specific_files = discover_files(args.specific)
    mode = "bilingual" if bilingual else "monolingual"
    langs = f"{args.src_lang}+{args.tgt_lang}" if bilingual else (args.src_lang if rank_src else args.tgt_lang)
    log(f"    General corpus : {len(general_files)} parquet file(s)")
    log(f"    Specific corpus: {len(specific_files)} parquet file(s)")
    log(f"    Languages      : {langs} ({mode})")
    log(f"    KenLM bin      : {kenlm_bin}")
    stage_done(1, "Setup", t0)

    # 2 ── Count specific corpus ───────────────────────────────────────────────
    t0 = stage_start(2, "Counting specific corpus segments")
    count_col = src_col if rank_src else tgt_col
    num_specific = count_rows(specific_files, count_col, args.chunk_size)
    log(f"    Specific corpus: {num_specific:,} segments")
    stage_done(2, "Counting specific corpus segments", t0)

    # 3 ── Extract vocabulary ──────────────────────────────────────────────────
    t0 = stage_start(3, "Extracting vocabulary from specific corpus")
    vocabs: dict[str, frozenset[str]] = {}
    for lang, col in active:
        vocabs[lang] = extract_vocab(specific_files, col, args.chunk_size, lang)
    stage_done(3, "Extracting vocabulary from specific corpus", t0)

    # 4 ── Sample general corpus for LM training ───────────────────────────────
    t0 = stage_start(4, "Sampling general corpus for LM training")
    for lang, col in active:
        out = os.path.join(tmpdir, f"general_sample.{lang}")
        n = write_filtered_text(
            general_files, col, vocabs[lang], out, args.chunk_size, max_lines=num_specific
        )
        log(f"    {lang}: {n:,} lines sampled → {out}")
    stage_done(4, "Sampling general corpus for LM training", t0)

    # 5 ── Build general-domain LMs ────────────────────────────────────────────
    t0 = stage_start(5, "Building general-domain LMs")
    for lang, _ in active:
        text = os.path.join(tmpdir, f"general_sample.{lang}")
        model = os.path.join(tmpdir, f"lm_general_{lang}.bin")
        log(f"    Training general {lang} LM (order 5, Kneser-Ney)...")
        train_lm(text, model, order=5, tmpdir=tmpdir, kenlm_bin=kenlm_bin)
        log(f"    {lang}: general LM → {model}")
    stage_done(5, "Building general-domain LMs", t0)

    # 6 ── Build specific-domain LMs ───────────────────────────────────────────
    t0 = stage_start(6, "Building specific-domain LMs")
    for lang, col in active:
        text = os.path.join(tmpdir, f"specific_corpus.{lang}")
        model = os.path.join(tmpdir, f"lm_specific_{lang}.bin")
        log(f"    Writing filtered specific {lang} text...")
        write_filtered_text(specific_files, col, vocabs[lang], text, args.chunk_size)
        log(f"    Training specific {lang} LM (order 5, Kneser-Ney)...")
        train_lm(text, model, order=5, tmpdir=tmpdir, kenlm_bin=kenlm_bin)
        log(f"    {lang}: specific LM → {model}")
    stage_done(6, "Building specific-domain LMs", t0)

    # 7 ── Score with general LMs ──────────────────────────────────────────────
    t0 = stage_start(7, "Scoring general corpus with general-domain LMs")
    for lang, col in active:
        model = os.path.join(tmpdir, f"lm_general_{lang}.bin")
        score_path = os.path.join(tmpdir, f"score_general_{lang}.txt")
        log(f"    Scoring with general {lang} LM...")
        score_corpus(general_files, col, vocabs[lang], model, score_path, args.chunk_size, lang, kenlm_bin)
    stage_done(7, "Scoring general corpus with general-domain LMs", t0)

    # 8 ── Score with specific LMs ─────────────────────────────────────────────
    t0 = stage_start(8, "Scoring general corpus with specific-domain LMs")
    for lang, col in active:
        model = os.path.join(tmpdir, f"lm_specific_{lang}.bin")
        score_path = os.path.join(tmpdir, f"score_specific_{lang}.txt")
        log(f"    Scoring with specific {lang} LM...")
        score_corpus(general_files, col, vocabs[lang], model, score_path, args.chunk_size, lang, kenlm_bin)
    stage_done(8, "Scoring general corpus with specific-domain LMs", t0)

    # 9 ── Compute differences ─────────────────────────────────────────────────
    t0 = stage_start(9, "Computing score differences")
    for lang, _ in active:
        compute_diff(
            os.path.join(tmpdir, f"score_specific_{lang}.txt"),
            os.path.join(tmpdir, f"score_general_{lang}.txt"),
            os.path.join(tmpdir, f"diff_{lang}.txt"),
        )
        log(f"    {lang}: score differences written")

    if bilingual:
        summed = os.path.join(tmpdir, "diff_summed.txt")
        sum_diffs(
            os.path.join(tmpdir, f"diff_{args.src_lang}.txt"),
            os.path.join(tmpdir, f"diff_{args.tgt_lang}.txt"),
            summed,
        )
        final_scores = summed
        log("    Bilingual scores summed")
    else:
        final_scores = os.path.join(tmpdir, f"diff_{active[0][0]}.txt")
    stage_done(9, "Computing score differences", t0)

    # 10 ── Sort, deduplicate, write output ────────────────────────────────────
    t0 = stage_start(10, "Sorting, deduplicating, and writing output")
    tsv_path = os.path.join(tmpdir, "scores_src_tgt.tsv")
    log("    Building score+text TSV...")
    build_tsv(final_scores, general_files, src_col, tgt_col, tsv_path, args.chunk_size)

    src_out = str(dest / f"general_corpus_sorted.{args.src_lang}")
    tgt_out = str(dest / f"general_corpus_sorted.{args.tgt_lang}")
    log("    Sorting descending by score and deduplicating (may take a while)...")
    sort_and_split(tsv_path, src_out, tgt_out, tmpdir)

    log(f"    {src_out}")
    log(f"    {tgt_out}")
    log("    Cleaning up temp directory...")
    shutil.rmtree(tmpdir)
    stage_done(10, "Sorting, deduplicating, and writing output", t0)

    elapsed = int(time.time() - script_start)
    h, r = divmod(elapsed, 3600)
    m, s = divmod(r, 60)
    log(f"==> Done. Total time: {h}h {m}m {s}s")

    if _log_fh:
        _log_fh.close()


if __name__ == "__main__":
    main()
