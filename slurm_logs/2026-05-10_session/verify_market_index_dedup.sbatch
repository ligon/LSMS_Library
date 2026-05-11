#!/bin/bash
#SBATCH --job-name=verify-#266
#SBATCH --account=co_carleton
#SBATCH --partition=savio2_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=00:30:00
#SBATCH --output=slurm_logs/2026-05-10_session/verify_market_index_dedup_%j.out
#SBATCH --error=slurm_logs/2026-05-10_session/verify_market_index_dedup_%j.err

set -uo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

echo "=== sbatch prolog $(date) ==="
echo "node:    $(hostname)"
echo "jobid:   ${SLURM_JOB_ID:-unset}"
echo "git:     $(git log -1 --oneline)"
echo "branch:  $(git branch --show-current)"
echo

LOCAL_VENV=/local/job${SLURM_JOB_ID}/venv
mkdir -p "$LOCAL_VENV"
tar -C .venv -cf - . | tar -C "$LOCAL_VENV" -xf -
PT="$LOCAL_VENV/bin/pytest"
echo

echo "=== STEP 1: GH #266 regression test (must pass with fix applied) ==="
LSMS_NO_CACHE=1 "$PT" -v --no-header --tb=short \
    tests/test_add_market_index_dedup.py
RC1=$?
echo "=== exit: $RC1 ==="
echo

echo "=== STEP 2: smoke check existing market='Region' tests didn't regress ==="
LSMS_NO_CACHE=1 "$PT" --no-header --tb=short \
    tests/test_sample.py::TestUganda2009MarketFallback
RC2=$?
echo "=== exit: $RC2 ==="
echo

echo "=== STEP 3: explicit before/after shape on Uganda shocks ==="
"$LOCAL_VENV/bin/python" -W ignore -c "
import lsms_library as ll
c = ll.Country('Uganda')
bare = c.shocks()
m = c.shocks(market='Region')
print(f'  shocks()                   shape: {bare.shape}  dup: {bare.index.duplicated().sum()}')
print(f'  shocks(market=Region)      shape: {m.shape}  dup: {m.index.duplicated().sum()}')
print(f'  expected:                  m_rows <= bare_rows ({len(m)} <= {len(bare)}) -> {len(m) <= len(bare)}')
print(f'  pre-fix:                   m_rows = 27268 (1.88x inflation)')
print(f'  post-fix:                  m_rows = {len(m)}')
"
echo

echo "=== verdict ==="
if [ "$RC1" -eq 0 ] && [ "$RC2" -eq 0 ]; then
    echo "ALL GREEN -- #266 fix verified."
    exit 0
fi
exit 1
