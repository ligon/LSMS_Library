#!/bin/bash
#SBATCH --job-name=glss782b
#SBATCH --account=co_carleton
#SBATCH --partition=savio3
#SBATCH --qos=savio_lowprio
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --time=05:00:00
#SBATCH --requeue
#SBATCH --open-mode=append
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/tmp/782-verify/logs/phase2_%j.log
set -u
MAIN=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
WT=/global/scratch/fsa/fc_jevons/ligon/tmp/wt-glss-prices
V=/global/scratch/fsa/fc_jevons/ligon/tmp/782-verify

SQUASHFUSE=/usr/libexec/apptainer/bin/squashfuse_ll
mnt=/local/job$SLURM_JOB_ID/venv_img
shim=/local/job$SLURM_JOB_ID/.fuseshim
mkdir -p "$shim" "$mnt"; ln -sf /usr/bin/fusermount "$shim/fusermount3"
if [ -c /dev/fuse ] && [ -x "$SQUASHFUSE" ]; then
  PATH="$shim:$PATH" "$SQUASHFUSE" "$MAIN/.venv.sqfs" "$mnt" || exit 1
else
  unsquashfs -f -d "$mnt" "$MAIN/.venv.sqfs" >/dev/null || exit 1
fi
PY="$mnt/bin/python"
export PYTHONPATH="$WT"
export LSMS_MAKE_JOBS=8
"$PY" -c "import lsms_library; assert 'wt-glss-prices' in lsms_library.__file__, lsms_library.__file__; print('lib:', lsms_library.__file__)" || exit 1
echo "=== $(date) node=$(hostname) HEAD=$(cd $WT && git rev-parse --short HEAD) ==="

echo; echo "############## PHASE before ##############"
LSMS_COUNTRIES_ROOT="$MAIN/lsms_library/countries" LSMS_DATA_DIR="$V/data-before" \
  "$PY" "$WT/slurm_logs/782/phase2.py" before 2>&1 | grep -v NullReadWarning
echo "--- before rc=$? ---"

echo; echo "############## PHASE after ##############"
LSMS_COUNTRIES_ROOT="$WT/lsms_library/countries" LSMS_DATA_DIR="$V/data-after" \
  "$PY" "$WT/slurm_logs/782/phase2.py" after 2>&1 | grep -v NullReadWarning
echo "--- after rc=$? ---"

echo; echo "############## DATA-GATED GUARD ##############"
cd "$MAIN"
LSMS_COUNTRIES_ROOT="$WT/lsms_library/countries" LSMS_DATA_DIR="$V/data-after" \
  "$PY" -m pytest "$WT/tests/test_ghanalss_food_label_canonical.py" -q -p no:randomly 2>&1 | tail -20

echo; echo "############## EXISTING TESTS ##############"
LSMS_COUNTRIES_ROOT="$WT/lsms_library/countries" LSMS_DATA_DIR="$V/data-after" \
  "$PY" -m pytest "$WT/tests/test_schema_consistency.py" "$WT/tests/test_ghanalss_community_prices.py" \
  "$WT/tests/test_ghanalss_nutrition.py" "$WT/tests/test_food_labels.py" "$WT/tests/test_label_selection.py" \
  -q -p no:randomly 2>&1 | tail -25

echo "=== DONE $(date) ==="
fusermount -u "$mnt" 2>/dev/null
