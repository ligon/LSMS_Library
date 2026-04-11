#!/usr/bin/env bash
# Run the build_feature benchmark with controlled cache state.
#
# Usage:
#   bench/run_bench.sh <Country> <feature> [--keep-cache]
#
# What it does:
#   1. Clears ~/.local/share/lsms_library/<Country>/  (unless --keep-cache)
#   2. Runs bench/build_feature.py in a FRESH subprocess (cold path)
#   3. Runs bench/build_feature.py in a SECOND fresh subprocess (cross-process,
#      data_root populated by run 1)
#
# The two runs together let you see:
#   - Run 1 first call vs second call: in-process warm-up benefit
#   - Run 2 first call vs Run 1 first call: whether the parquet cache is being
#     read back across processes (the v0.7.0 fix's user-visible promise)
#
# Configuration via environment variables:
#   LSMS_REPO_FOR_DATA -- absolute path to a mirror that has .dta files
#                         materialized locally.  Defaults to the current repo;
#                         set to /path/to/LSMS_Library- to point at the old
#                         mirror that has dvc-pulled .dta files.
#   LSMS_DATA_DIR      -- override the data_root.  Default is platformdirs
#                         user data dir, typically ~/.local/share/lsms_library/.
#   BENCH_OUT          -- jsonl path for aggregated records.  Default
#                         bench/results/$(date +%Y-%m-%d).jsonl
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "usage: $0 <Country> <feature> [--keep-cache]" >&2
    exit 2
fi

COUNTRY="$1"
FEATURE="$2"
KEEP_CACHE=0
shift 2
while [ $# -gt 0 ]; do
    case "$1" in
        --keep-cache) KEEP_CACHE=1 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
    shift
done

# Resolve repo paths
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
DATA_REPO="${LSMS_REPO_FOR_DATA:-$REPO_ROOT}"

# Pick a python: prefer the repo's .venv, fall back to PATH
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif [ -x "$DATA_REPO/.venv/bin/python" ]; then
    PYTHON_BIN="$DATA_REPO/.venv/bin/python"
else
    PYTHON_BIN="$(command -v python3)"
fi

# Where to write the JSON record
TODAY=$(date +%Y-%m-%d)
OUT="${BENCH_OUT:-$REPO_ROOT/bench/results/${TODAY}.jsonl}"
mkdir -p "$(dirname "$OUT")"

# Resolve the data_root for the target country
USER_DATA_ROOT="${LSMS_DATA_DIR:-$HOME/.local/share/lsms_library}"
COUNTRY_CACHE="$USER_DATA_ROOT/$COUNTRY"

# Files we'll clear: just the target feature's parquet under var/ and _/.
# We deliberately do NOT remove the whole $COUNTRY_CACHE directory because
# other features in the same country may have cached parquets the user
# values (food_acquired, food_expenditures, etc.) and a too-broad clear
# would force expensive unrelated rebuilds on the next access.
TARGETS=(
    "$COUNTRY_CACHE/var/$FEATURE.parquet"
    "$COUNTRY_CACHE/_/$FEATURE.parquet"
)

echo "================================================================"
echo "  bench: $COUNTRY / $FEATURE"
echo "  python:        $PYTHON_BIN"
echo "  lsms code at:  $DATA_REPO/lsms_library"
echo "  data_root:     $USER_DATA_ROOT"
echo "  country cache: $COUNTRY_CACHE"
echo "  clear targets:"
for t in "${TARGETS[@]}"; do echo "    $t"; done
echo "  jsonl out:     $OUT"
echo "================================================================"

if [ "$KEEP_CACHE" -eq 0 ]; then
    echo
    echo "--- pre-clear: removing target parquets if present ---"
    for t in "${TARGETS[@]}"; do
        if [ -e "$t" ]; then
            rm -v "$t"
        else
            echo "  (absent) $t"
        fi
    done
else
    echo
    echo "--- --keep-cache: leaving target parquets as-is ---"
fi

echo
echo "=== RUN 1 (cold-cache subprocess) ==="
PYTHONPATH="$DATA_REPO" "$PYTHON_BIN" "$SCRIPT_DIR/build_feature.py" \
    "$COUNTRY" "$FEATURE" --label run1_cold --json "$OUT"

echo
echo "=== RUN 2 (fresh subprocess, data_root populated by RUN 1) ==="
PYTHONPATH="$DATA_REPO" "$PYTHON_BIN" "$SCRIPT_DIR/build_feature.py" \
    "$COUNTRY" "$FEATURE" --label run2_warm_xproc --json "$OUT"

echo
echo "Done.  Aggregated records appended to: $OUT"
