#!/bin/bash
#SBATCH --job-name=cov-matrix-validate
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=4
#SBATCH --mem=24G
#SBATCH --time=00:45:00
#SBATCH --output=slurm_logs/2026-06-25_coverage_matrix/validate_%j.out
#SBATCH --error=slurm_logs/2026-06-25_coverage_matrix/validate_%j.err

set -euo pipefail
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"
SESS=slurm_logs/2026-06-25_coverage_matrix

echo "=== prolog ==="; echo "node: $(hostname)  jobid: ${SLURM_JOB_ID:-unset}  started: $(date)"; echo

echo "=== mount venv (squashfs) ==="
bash bin/savio_venv.sh mount
PY="$REPO/.venv/bin/python"
"$PY" -c 'import sys,pandas; print("python",sys.version.split()[0],"pandas",pandas.__version__)'
echo

# Readiness validation on a diverse subset: Uganda (rich, expect sane),
# Iraq (the #532 dropped-class case), Malawi (housing/categorical), and a
# small EHCVM country (Niger). Warm cache where present; cold cells pull S3.
echo "=== readiness build (subset) ==="
"$PY" bench/matrix.py \
    --countries Uganda Iraq Malawi Niger \
    --snapshot "$SESS/validate_latest.csv" \
    --html "$SESS/validate_matrix.html"

echo
echo "=== tier distribution ==="
"$PY" - <<'PYEOF'
import pandas as pd
df = pd.read_csv("slurm_logs/2026-06-25_coverage_matrix/validate_latest.csv", dtype=str, keep_default_na=False)
print(df.groupby(["country","tier"]).size().unstack(fill_value=0))
print()
# Charter verification anchors:
def cell(c,f,w):
    s = df[(df.country==c)&(df.feature==f)&(df.wave==w)]
    return s.tier.iloc[0] if len(s) else "<none>"
print("Uganda food_expenditures 2013-14 ->", cell("Uganda","food_expenditures","2013-14"))
print("Uganda household_roster  2013-14 ->", cell("Uganda","household_roster","2013-14"))
print("any 'dropped' cells:")
print(df[df.tier=="dropped"][["country","feature","wave","detail"]].to_string(index=False) or "  none")
print("any 'broken' cells:")
print(df[df.tier=="broken"][["country","feature","wave","detail"]].head(20).to_string(index=False) or "  none")
PYEOF

echo; echo "=== done $(date) ==="
