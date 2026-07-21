#!/bin/bash
#SBATCH --job-name=cov-matrix-regress
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=01:30:00
#SBATCH --output=slurm_logs/2026-06-25_coverage_matrix/regress_%j.out
#SBATCH --error=slurm_logs/2026-06-25_coverage_matrix/regress_%j.err

set -uo pipefail
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"
SESS=slurm_logs/2026-06-25_coverage_matrix

echo "=== prolog ==="; echo "node: $(hostname)  jobid: ${SLURM_JOB_ID:-unset}  started: $(date)"; echo
bash bin/savio_venv.sh mount
PY="$REPO/.venv/bin/python"
"$PY" -c 'import sys,pandas; print("python",sys.version.split()[0],"pandas",pandas.__version__)'
echo

# --- Warm-cache check: build Uganda readiness twice; 2nd run must rebuild 0
#     cells (v0.8.0 content-hash) and be markedly faster.
echo "=== warm-cache check (Uganda readiness x2) ==="
t0=$(date +%s)
"$PY" bench/matrix.py --countries Uganda --snapshot "$SESS/wc1.csv" --html "$SESS/wc1.html" >/dev/null 2>&1
t1=$(date +%s)
"$PY" bench/matrix.py --countries Uganda --snapshot "$SESS/wc2.csv" --html "$SESS/wc2.html" >/dev/null 2>&1
t2=$(date +%s)
echo "  run1 (cold-ish): $((t1-t0))s   run2 (warm): $((t2-t1))s"
"$PY" - <<'PYEOF'
import pandas as pd
a = pd.read_csv("slurm_logs/2026-06-25_coverage_matrix/wc1.csv", dtype=str, keep_default_na=False)
b = pd.read_csv("slurm_logs/2026-06-25_coverage_matrix/wc2.csv", dtype=str, keep_default_na=False)
same = a.equals(b)
print(f"  identical tier tables across runs: {same}")
print("  run2 tiers:", dict(b.tier.value_counts()))
PYEOF
echo

# --- Full regression: the whole pytest suite on the warm cache. The additive
#     change is package-level (new module + one __init__ import); this is the
#     'no regressions' charter gate.
echo "=== full pytest (warm cache) ==="
NPROC="${SLURM_CPUS_ON_NODE:-4}"
"$PY" -m pytest -n "$NPROC" --dist=loadfile -q -p no:cacheprovider 2>&1 | tail -45

echo; echo "=== done $(date) ==="
