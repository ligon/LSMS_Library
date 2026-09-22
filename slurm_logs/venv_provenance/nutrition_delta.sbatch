#!/bin/bash
# How much did Uganda var/nutrition.parquet actually move since its baseline?
#
# tests/test_uganda_invariance.py fails on content_hash while shape (23801x15),
# columns and index names all match -- so no household was gained or lost and
# the change is in VALUES.  The baseline stores only a hash, so the magnitude
# cannot be read off it; the old values have to be rebuilt.
#
# Builds Uganda nutrition at 8ec0bab4e (the commit that last wrote the
# baseline, 2026-09-07) into its own LSMS_DATA_DIR, then diffs against the
# current build.  L1 blobs are shared (content-addressed, so safe).
#SBATCH --job-name=lsms-nutrition-delta
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3
#SBATCH --qos=savio_normal
#SBATCH --nodes=1
#SBATCH --time=03:00:00
#SBATCH --chdir=/global/scratch/fsa/fc_jevons/ligon/venv_validate
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/venv_validate/%x_%j.out
set -uo pipefail
MIRROR=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
BASE=8ec0bab4e
CLONE="/local/job$SLURM_JOB_ID/old"; MNT="/local/job$SLURM_JOB_ID/venv_img"
LIVE=/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library
STAGE="/global/scratch/fsa/fc_jevons/ligon/venv_validate/nutrition_old_$SLURM_JOB_ID"

shim="/local/job$SLURM_JOB_ID/.fuseshim"; mkdir -p "$shim" "$MNT"
ln -sf /usr/bin/fusermount "$shim/fusermount3"
PATH="$shim:$PATH" /usr/libexec/apptainer/bin/squashfuse_ll "$MIRROR/.venv.sqfs" "$MNT" || exit 1
git clone --quiet "$MIRROR" "$CLONE" && git -C "$CLONE" checkout --quiet "$BASE" || exit 1
echo "old tree at $(git -C "$CLONE" rev-parse --short HEAD)"

mkdir -p "$STAGE"; ln -sfn "$LIVE/dvc-cache" "$STAGE/dvc-cache"
export PYTHONPATH="$CLONE" LSMS_DATA_DIR="$STAGE"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
cd "$CLONE"
"$MNT/bin/python" -c "import lsms_library,os;assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath('$CLONE')+os.sep)" || exit 1

echo "=== building Uganda nutrition at $BASE ==="
"$MNT/bin/python" -c "
from lsms_library import Country
Country('Uganda', verbose=False).nutrition()
print('built')
" || { echo "old build FAILED"; exit 1; }

echo "=== diff old vs current ==="
"$MNT/bin/python" - <<'PYX'
import pandas as pd, numpy as np, os
old = pd.read_parquet(os.environ['LSMS_DATA_DIR'] + '/Uganda/var/nutrition.parquet')
new = pd.read_parquet('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/Uganda/var/nutrition.parquet')
print(f"  old {old.shape}   new {new.shape}")
print(f"  index identical: {old.index.equals(new.index)}")
o, n = old.align(new, join='inner', axis=None)
cols = [c for c in old.columns if c in new.columns]
o, n = old[cols].sort_index(), new[cols].sort_index()
print(f"  aligned rows: {len(o)}")
diff = (o != n) & ~(o.isna() & n.isna())
print(f"  cells differing: {int(diff.values.sum())} of {o.size} ({100*diff.values.sum()/o.size:.2f}%)")
print(f"  rows with any change: {int(diff.any(axis=1).sum())} of {len(o)} ({100*diff.any(axis=1).mean():.2f}%)")
print("\n  per-column: total before -> after (pct change), and median |rel change| on changed cells")
for c in cols:
    a, b = pd.to_numeric(o[c], errors='coerce'), pd.to_numeric(n[c], errors='coerce')
    m = diff[c] & a.notna() & b.notna()
    rel = ((b[m]-a[m]).abs()/a[m].abs().replace(0, np.nan)).median() if m.any() else float('nan')
    sa, sb = a.sum(), b.sum()
    pct = 100*(sb-sa)/sa if sa else float('nan')
    print(f"    {c:<14} {sa:>14.4g} -> {sb:>14.4g} ({pct:+7.2f}%)   changed={int(m.sum()):>6}  med|rel|={rel:.4f}")
PYX
fusermount -u "$MNT" 2>/dev/null
