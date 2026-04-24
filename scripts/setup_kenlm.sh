#!/usr/bin/env bash
# Build KenLM from source and install lmplz + build_binary into bin/
# Run from the repository root: bash scripts/setup_kenlm.sh
#
# Requires: cmake, gcc/g++, make (install with: sudo apt-get install -y cmake g++ make)

set -euo pipefail

# Check for required system dependencies
if ! dpkg -l libboost-dev 2>/dev/null | grep -q '^ii' && \
   ! ls /usr/include/boost/version.hpp 2>/dev/null | grep -q .; then
    echo "ERROR: Boost development headers not found."
    echo "  Install them with: sudo apt-get install -y libboost-all-dev"
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$(mktemp -d)"
BIN_DIR="$REPO_ROOT/bin"

echo "==> Cloning KenLM..."
git clone --depth 1 https://github.com/kpu/kenlm.git "$BUILD_DIR/kenlm"

echo "==> Building KenLM (lmplz + build_binary)..."
cmake -S "$BUILD_DIR/kenlm" -B "$BUILD_DIR/kenlm/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_VERBOSE_MAKEFILE=OFF \
    > "$BUILD_DIR/cmake.log" 2>&1
cmake --build "$BUILD_DIR/kenlm/build" \
    --target lmplz build_binary query \
    -j"$(nproc)" \
    >> "$BUILD_DIR/cmake.log" 2>&1

echo "==> Installing binaries to $BIN_DIR ..."
mkdir -p "$BIN_DIR"
cp "$BUILD_DIR/kenlm/build/bin/lmplz" "$BIN_DIR/"
cp "$BUILD_DIR/kenlm/build/bin/build_binary" "$BIN_DIR/"
cp "$BUILD_DIR/kenlm/build/bin/query" "$BIN_DIR/"

echo "==> Cleaning up build directory..."
rm -rf "$BUILD_DIR"

echo ""
echo "KenLM installed:"
echo "  $BIN_DIR/lmplz"
echo "  $BIN_DIR/build_binary"
echo "  $BIN_DIR/query"
echo ""
echo "Usage options:"
echo "  export KENLM=$REPO_ROOT          # ml_select.py will find bin/lmplz automatically"
echo "  python3 ml_select.py ... --kenlm-bin $BIN_DIR"
