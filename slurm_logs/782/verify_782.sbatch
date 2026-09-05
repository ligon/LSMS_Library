#!/bin/bash
#SBATCH --job-name=glss782
#SBATCH --account=co_carleton
#SBATCH --partition=savio3
#SBATCH --qos=savio_lowprio
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --time=05:00:00
#SBATCH --requeue
#SBATCH --open-mode=append
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/tmp/782-verify/logs/verify_%j.log
#
# GH #782 acceptance: cold-build GhanaLSS BEFORE (development config) and AFTER
# (fix branch config) in two private LSMS_DATA_DIRs, and diff the j axis.
# savio_lowprio is FREE and PRE-EMPTIBLE; --requeue is safe because each phase
# rebuilds its own private data dir from scratch.
set -u
MAIN=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
WT=/global/scratch/fsa/fc_jevons/ligon/tmp/wt-glss-prices
V=/global/scratch/fsa/fc_jevons/ligon/tmp/782-verify

SQUASHFUSE=/usr/libexec/apptainer/bin/squashfuse_ll
mnt=/local/job$SLURM_JOB_ID/venv_img
shim=/local/job$SLURM_JOB_ID/.fuseshim
mkdir -p "$shim" "$mnt"; ln -sf /usr/bin/fusermount "$shim/fusermount3"
if [ -c /dev/fuse ] && [ -x "$SQUASHFUSE" ]; then
  PATH="$shim:$PATH" "$SQUASHFUSE" "$MAIN/.venv.sqfs" "$mnt" || { echo "squashfuse failed"; exit 1; }
else
  unsquashfs -f -d "$mnt" "$MAIN/.venv.sqfs" >/dev/null || { echo "unsquashfs failed"; exit 1; }
fi
PY="$mnt/bin/python"

export PYTHONPATH="$WT"
export LSMS_MAKE_JOBS=8
"$PY" -c "import lsms_library; assert 'wt-glss-prices' in lsms_library.__file__, lsms_library.__file__; print('lib:', lsms_library.__file__)" || exit 1

echo "=== $(date) node=$(hostname) HEAD=$(cd $WT && git rev-parse --short HEAD) ==="

run_phase () {   # $1 = tag, $2 = countries root, $3 = data dir
  echo; echo "############## PHASE $1 ##############"
  export LSMS_COUNTRIES_ROOT="$2"
  export LSMS_DATA_DIR="$3"
  "$PY" - "$1" <<'PYEOF'
import sys, time, warnings
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root, data_root

tag = sys.argv[1]
print('countries_root:', countries_root())
print('data_root     :', data_root())
c = ll.Country('GhanaLSS')

t0 = time.time()
fa = c.food_acquired()
print(f'[{tag}] food_acquired: rows={len(fa):,}  {time.time()-t0:.0f}s')
fa.to_parquet(f'{data_root()}/../fa_{tag}.parquet')

for name in ('food_expenditures', 'food_quantities', 'food_prices'):
    t0 = time.time()
    try:
        d = getattr(c, name)()
        print(f'[{tag}] {name}: rows={len(d):,}  {time.time()-t0:.0f}s')
    except Exception as e:
        print(f'[{tag}] {name}: FAILED {type(e).__name__}: {e}')

t0 = time.time()
try:
    cp = c.community_prices()
    print(f'[{tag}] community_prices: rows={len(cp):,}  {time.time()-t0:.0f}s')
    cp.to_parquet(f'{data_root()}/../cp_{tag}.parquet')
except Exception as e:
    print(f'[{tag}] community_prices: FAILED {type(e).__name__}: {e}')

# nutrition coverage: share of food MASS carrying an FCT code
try:
    sys.path.insert(0, str(countries_root() / 'GhanaLSS' / '_'))
    import importlib, nutrition
    importlib.reload(nutrition)
    bw, bl = nutrition._load_food_codes(countries_root())
    q = nutrition._quantities(countries_root())
    d = q.rename('Quantity').reset_index()
    d = d.merge(bw[['t', 'j', 'FCT Code']], on=['t', 'j'], how='left')
    fb = d['j'].map(bl)
    d['FCT Code'] = d['FCT Code'].where(d['FCT Code'].notna(), fb).fillna('')
    tot = d['Quantity'].sum()
    cov = d.loc[d['FCT Code'] != '', 'Quantity'].sum()
    print(f'[{tag}] NUTRITION coverage: {100*cov/tot:.2f}% of mass '
          f'({cov:,.0f} / {tot:,.0f} kg)')
    miss = (d[d['FCT Code'] == ''].groupby('j')['Quantity'].sum()
            .sort_values(ascending=False).head(10))
    print(f'[{tag}] top unmapped-by-mass:')
    for j, v in miss.items():
        print(f'       {j!r:34} {v:,.0f}')
except Exception as e:
    import traceback; traceback.print_exc()
    print(f'[{tag}] nutrition coverage: FAILED {type(e).__name__}: {e}')

t0 = time.time()
try:
    f = ll.Feature('food_acquired')(['GhanaLSS'])
    print(f'[{tag}] Feature(food_acquired)([GhanaLSS]): rows={len(f):,} '
          f'levels={list(f.index.names)}  {time.time()-t0:.0f}s')
except Exception as e:
    print(f'[{tag}] Feature: FAILED {type(e).__name__}: {e}')
PYEOF
  echo "--- phase $1 rc=$? ---"
}

run_phase before "$MAIN/lsms_library/countries" "$V/data-before"
run_phase after  "$WT/lsms_library/countries"   "$V/data-after"

echo; echo "############## CENSUS ##############"
for tag in before after; do
  "$PY" "$V/census.py" "$V/fa_${tag}.parquet" "${tag^^}" || echo "census $tag failed"
done

echo; echo "############## DATA-GATED GUARD (after config) ##############"
export LSMS_COUNTRIES_ROOT="$WT/lsms_library/countries"
export LSMS_DATA_DIR="$V/data-after"
cd "$MAIN" && "$PY" -m pytest "$WT/tests/test_ghanalss_food_label_canonical.py" \
    -q -p no:randomly 2>&1 | tail -25

echo "=== DONE $(date) ==="
fusermount -u "$mnt" 2>/dev/null
