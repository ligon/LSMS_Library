#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 3:00:00
#SBATCH --exclude=n0035.savio4
#SBATCH -J sc-sweep
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections/logs/sweep-%j.out
# Tests outside the filename-filtered set: country-parametrized files under -k, kg/food files whole.
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections
ARM=${ARM:?}; SHA=${SHA:?}; RES=$OUT/results/sweep; mkdir -p $RES
J=/local/job$SLURM_JOB_ID; mkdir -p $J/tmp; export TMPDIR=$J/tmp
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_DATA_DIR
P=$J/priv; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
# private roots exported BEFORE the venv mount's import check
export PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log
echo "node $(hostname) start $(date -Is) ARM=$ARM SHA=$SHA"
git clone -q --no-hardlinks /global/home/users/ligon/mirrors/LSMS_Library $J/clone || exit 2
git -C $J/clone checkout -q --detach $SHA || exit 2
echo "HEAD $(git -C $J/clone rev-parse HEAD)"
( cd $J/clone && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python; [ -x $PY ] || PY=$J/clone/.venv/bin/python
export PYTHONPATH=$J/clone
cd $J/clone
$PY - <<PYEOF || exit 4
import os, subprocess, lsms_library, lsms_library.paths as p
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
assert os.path.realpath(str(p.data_root())).startswith(os.path.realpath('$P') + os.sep)
print('IMPORT OK', lsms_library.__file__, p.data_root())
PYEOF
SET=$OUT/results/tests/pytest_files_cand.txt
BROAD=$(for f in $(grep -lE "'(Mali|Senegal|Niger|Uganda)'|\"(Mali|Senegal|Niger|Uganda)\"" tests/test_*.py); do grep -qx "$f" $SET || echo $f; done | tr '\n' ' ')
WHOLE="tests/test_food_kg_size_pool.py tests/test_gh1452_older_rows.py tests/test_gh1487_kg_subset_is_a_slice.py tests/test_food_kg_inference.py tests/test_own_production_valuation.py tests/test_food_prices_dtype.py"
echo "$BROAD" | tr ' ' '\n' > $RES/broad_files_$ARM.txt
$PY -m pytest -n 8 --dist=loadfile -q -p no:cacheprovider -rfEs -k "Mali or Senegal or Niger or Uganda or mali or senegal or niger or uganda" $BROAD > $RES/pytest_broad_$ARM.txt 2>&1; echo "pytest broad rc=$?"
$PY -m pytest -n 8 --dist=loadfile -q -p no:cacheprovider -rfEs $WHOLE > $RES/pytest_whole_$ARM.txt 2>&1; echo "pytest whole rc=$?"
if [ "${HEADCHECK:-0}" = 1 ]; then
  $PY -m pytest -n 8 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_gh1707_served_corrections.py tests/test_derivations.py > $RES/pytest_head_$ARM.txt 2>&1; echo "pytest head rc=$?"
  P2=$J/priv-hash; mkdir -p $P2/data $P2/xdg $P2/tmp $P2/mpl
  env PRIVATE_ROOT=$P2 LSMS_DATA_DIR=$P2/data XDG_CACHE_HOME=$P2/xdg TMPDIR=$P2/tmp MPLCONFIGDIR=$P2/mpl LSMS_ISSUES_LOG=$P2/issues.log \
      $PY $OUT/scripts/hashprobe.py $J/clone $ARM $RES/hash_$ARM.json > $RES/hash_$ARM.log 2>&1; echo "hashprobe rc=$?"
fi
for f in $RES/pytest_*_$ARM.txt; do tail -1 $f; done
echo "in-tree parquets: $(find $J/clone/lsms_library/countries -name '*.parquet' | wc -l)"
git -C $J/clone status --porcelain | head
echo "SWEEP DONE $(date -Is)"
