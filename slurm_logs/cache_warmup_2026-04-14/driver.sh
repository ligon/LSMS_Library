#!/bin/bash
set -u
cd /global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
LOG_DIR=slurm_logs/cache_warmup_2026-04-14
export PYTHONPATH=.

run_feature() {
    local feat=$1
    local start=$(date +%s)
    PYTHONPATH=. nice -n 19 .venv/bin/python -u -c "
import os, warnings, sys
warnings.filterwarnings('ignore')
import lsms_library as ll
try:
    df = ll.Country('Uganda').$feat(market='Region')
    print(f'[$feat] OK shape={df.shape} index={list(df.index.names)} cols={list(df.columns)[:8]}', flush=True)
    print(f'[$feat] dtypes={dict(df.dtypes)}', flush=True)
except Exception as e:
    print(f'[$feat] FAIL {type(e).__name__}: {e}', flush=True)
    sys.exit(1)
" 2>&1
    local elapsed=$(( $(date +%s) - start ))
    echo "[$feat] wall=${elapsed}s"
}

echo "=== Phase A start: $(date +%H:%M:%S) ==="
run_feature earnings           > $LOG_DIR/earnings.log 2>&1 &
PID_A1=$!
run_feature enterprise_income  > $LOG_DIR/enterprise_income.log 2>&1 &
PID_A2=$!
run_feature shocks             > $LOG_DIR/shocks.log 2>&1 &
PID_A3=$!

# Wait for earnings + enterprise_income to finish (income depends on both)
wait $PID_A1; STATUS_A1=$?
wait $PID_A2; STATUS_A2=$?
echo "=== Phase A (earnings+EI) done: $(date +%H:%M:%S) earnings=$STATUS_A1 enterprise_income=$STATUS_A2 ==="

if [ $STATUS_A1 -eq 0 ] && [ $STATUS_A2 -eq 0 ]; then
    echo "=== Phase B (income) start: $(date +%H:%M:%S) ==="
    run_feature income         > $LOG_DIR/income.log 2>&1
    echo "=== Phase B done: $(date +%H:%M:%S) ==="
fi

# Wait for shocks to finish too
wait $PID_A3
echo "=== All done: $(date +%H:%M:%S) ==="
