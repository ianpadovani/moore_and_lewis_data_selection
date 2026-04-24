#!/bin/bash
# Runs on the EC2 instance at first boot via user-data.
# Placeholders (@@VAR@@) are substituted by launch_ec2.sh before upload.

# Re-exec under a transient systemd scope so cloud-init's SIGTERM never reaches
# this script. systemd-run --scope places it in a new cgroup outside cloud-final.
if [ -z "${_SYSTEMD_SCOPE:-}" ]; then
    exec env _SYSTEMD_SCOPE=1 systemd-run --scope \
        bash "$BASH_SOURCE" "$@"
fi

set -euo pipefail
INSTANCE_NAME="@@INSTANCE_NAME@@"
TIMESTAMP=$(date -u +%Y-%m-%d_%H-%M-%S)
SCRIPT_START=$SECONDS
exec > /var/log/pipeline.log 2>&1

echo "[$(date -u +%H:%M:%S)] ==> user-data script started" > /dev/console

# ── Graceful exit: upload log, shut down ──────────────────────────────────────
LOG_UPLOADER_PID=""
_EXITED=0
graceful_exit() {
    [ "$_EXITED" -eq 1 ] && return
    _EXITED=1

    echo "[$(date -u +%H:%M:%S)] ==> graceful_exit triggered" > /dev/console
    [ -n "$LOG_UPLOADER_PID" ] && kill "$LOG_UPLOADER_PID" 2>/dev/null || true

    echo "[$(date -u +%H:%M:%S)] ==> Uploading final log to S3" > /dev/console
    aws s3 cp /var/log/pipeline.log \
        "@@S3_LOGS@@/${INSTANCE_NAME}_${TIMESTAMP}.log" \
        --region @@REGION@@ > /dev/console 2>&1 \
        && echo "[$(date -u +%H:%M:%S)] ==> Log uploaded" > /dev/console \
        || echo "[$(date -u +%H:%M:%S)] ==> Log upload FAILED" > /dev/console

    sleep 5
    shutdown -h now
}
trap graceful_exit EXIT
trap 'echo "[$(date -u +%H:%M:%S)] ==> SIGTERM received" > /dev/console; graceful_exit' SIGTERM
trap 'echo "[$(date -u +%H:%M:%S)] ==> SIGINT received"  > /dev/console; graceful_exit' SIGINT

log() { local m="[$(date -u +%H:%M:%S)] $*"; echo "$m"; echo "$m" > /dev/console; }

# ── Credential cache helper ───────────────────────────────────────────────────
cache_iam_credentials() {
    local _TOKEN _ROLE _CREDS _AK _SK _ST
    _TOKEN=$(curl -sf --connect-timeout 5 \
        -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 21600") || return 1
    _ROLE=$(curl -sf --connect-timeout 5 \
        -H "X-aws-ec2-metadata-token: $_TOKEN" \
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/") || return 1
    _CREDS=$(curl -sf --connect-timeout 5 \
        -H "X-aws-ec2-metadata-token: $_TOKEN" \
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/${_ROLE}") || return 1
    _AK=$(printf '%s' "$_CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)['AccessKeyId'])")
    _SK=$(printf '%s' "$_CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)['SecretAccessKey'])")
    _ST=$(printf '%s' "$_CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)['Token'])")
    mkdir -p /root/.aws
    printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\naws_session_token = %s\n' \
        "$_AK" "$_SK" "$_ST" > /root/.aws/credentials
}

# ── [1/7] Cache IAM credentials ───────────────────────────────────────────────
_T=$SECONDS
log "==> [1/7] Caching IAM credentials"
for _RETRY in 1 2 3; do
    if cache_iam_credentials; then break; fi
    log "    Attempt ${_RETRY} failed"
    [ "$_RETRY" -lt 3 ] && sleep 10 || true
done
log "==> [1/7] IAM credentials cached ($(( SECONDS - _T ))s)"

log "--- Instance info ---"
log "    CPUs  : $(nproc)"
log "    RAM   : $(free -h | awk '/^Mem:/{print $2}')"
log "    Disk  : $(df -h / | awk 'NR==2{print $4 " free of " $2}')"
log "    Kernel: $(uname -r)"

# ── [2/7] Install Python 3.11 and build tools ─────────────────────────────────
_T=$SECONDS
log "==> [2/7] Installing Python 3.11 and build tools"
dnf install -y python3.11 python3.11-pip cmake gcc-c++ make git
log "==> [2/7] System packages installed ($(( SECONDS - _T ))s)"

# ── [3/7] Download and extract code ───────────────────────────────────────────
_T=$SECONDS
log "==> [3/7] Downloading code from @@S3_CODE@@"
mkdir -p /opt/pipeline
aws s3 cp @@S3_CODE@@ /opt/pipeline/code.tar.gz --region @@REGION@@
tar -xzf /opt/pipeline/code.tar.gz -C /opt/pipeline/
rm -f /opt/pipeline/code.tar.gz
aws s3 rm @@S3_CODE@@ --region @@REGION@@
log "==> [3/7] Code extracted ($(( SECONDS - _T ))s)"
log "    Disk: $(df -h / | awk 'NR==2{print $4 " free"}')"

# ── [4/7] Build KenLM ─────────────────────────────────────────────────────────
_T=$SECONDS
log "==> [4/7] Building KenLM from source"
git clone --depth 1 https://github.com/kpu/kenlm.git /tmp/kenlm_src
cmake -S /tmp/kenlm_src -B /tmp/kenlm_src/build -DCMAKE_BUILD_TYPE=Release -DCMAKE_VERBOSE_MAKEFILE=OFF
cmake --build /tmp/kenlm_src/build --target lmplz build_binary query -j"$(nproc)"
mkdir -p /opt/pipeline/bin
cp /tmp/kenlm_src/build/bin/lmplz /tmp/kenlm_src/build/bin/build_binary /tmp/kenlm_src/build/bin/query /opt/pipeline/bin/
rm -rf /tmp/kenlm_src
log "==> [4/7] KenLM built ($(( SECONDS - _T ))s)"

# ── [5/7] Install Python dependencies ─────────────────────────────────────────
_T=$SECONDS
log "==> [5/7] Installing Python dependencies"
cd /opt/pipeline
log "    Installing uv"
python3.11 -m pip install uv
log "    Installing package dependencies"
python3.11 -m uv pip install --system -e .
log "==> [5/7] Python dependencies installed ($(( SECONDS - _T ))s)"
log "    Disk: $(df -h / | awk 'NR==2{print $4 " free"}')"

# ── Start background log uploader ─────────────────────────────────────────────
(while true; do
    sleep 300
    aws s3 cp /var/log/pipeline.log \
        "@@S3_LOGS@@/${INSTANCE_NAME}_${TIMESTAMP}.log" \
        --region @@REGION@@ > /dev/console 2>&1 || true
    cache_iam_credentials > /dev/null 2>&1 || true
done) &
LOG_UPLOADER_PID=$!

# ── [6/7] Run Moore & Lewis data selection ────────────────────────────────────
_T=$SECONDS
log "==> [6/7] Running Moore & Lewis data selection"
log "    General corpus : @@S3_GENERAL@@"
log "    Specific corpus: @@S3_SPECIFIC@@"
log "    Languages      : @@SRC_LANG@@ → @@TGT_LANG@@"

mkdir -p /opt/pipeline/output

PYTHONUNBUFFERED=1 python3.11 /opt/pipeline/ml_select.py \
    --general     "@@S3_GENERAL@@" \
    --specific    "@@S3_SPECIFIC@@" \
    --dest        /opt/pipeline/output \
    --src-lang    @@SRC_LANG@@ \
    --tgt-lang    @@TGT_LANG@@ \
    --src-col     @@SRC_COL@@ \
    --tgt-col     @@TGT_COL@@ \
    --rank-src    @@RANK_SRC@@ \
    --rank-tgt    @@RANK_TGT@@ \
    --kenlm-bin   /opt/pipeline/bin \
    --log-file    /opt/pipeline/output/ml_select.log

log "==> [6/7] Data selection completed ($(( SECONDS - _T ))s)"
log "    Disk: $(df -h / | awk 'NR==2{print $4 " free"}')"

# ── [7/7] Upload results to S3 ────────────────────────────────────────────────
_T=$SECONDS
log "==> [7/7] Uploading results to @@S3_OUTPUT@@"
aws s3 sync /opt/pipeline/output/ @@S3_OUTPUT@@/ --region @@REGION@@
log "==> [7/7] Results uploaded ($(( SECONDS - _T ))s)"

_ELAPSED=$(( SECONDS - SCRIPT_START ))
log "==> Done. Total time: $(( _ELAPSED / 3600 ))h $(( (_ELAPSED % 3600) / 60 ))m $(( _ELAPSED % 60 ))s"

# EXIT trap fires here: uploads final log and shuts down
