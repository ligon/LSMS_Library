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
#SBATCH -J sc-tests
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections/logs/tests-%j.out
# Targeted pytest on one full SHA, cold private root.  ARM names the result.
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections
TAG=${TAG:?}; ARM=${ARM:?}; SHA=${SHA:?}
RES=$OUT/results/$TAG; mkdir -p $RES
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
J=/local/job$SLURM_JOB_ID
mkdir -p $J/tmp
export TMPDIR=$J/tmp
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_DATA_DIR
echo "node $(hostname) start $(date -Is) TAG=$TAG ARM=$ARM SHA=$SHA"
git clone -q --no-hardlinks $MIRROR $J/clone || exit 2
git -C $J/clone checkout -q --detach $SHA || exit 2
HEAD=$(git -C $J/clone rev-parse HEAD); echo "HEAD $HEAD"
( cd $J/clone && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
[ -x $PY ] || PY=$J/clone/.venv/bin/python
P=$J/priv; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
export PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg MPLCONFIGDIR=$P/mpl \
       LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/clone
cd $J/clone
$PY - <<EOF || exit 4
import os, subprocess, lsms_library, lsms_library.paths as p
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
dr = os.path.realpath(str(p.data_root())); assert dr.startswith(os.path.realpath('$P') + os.sep), dr
print('IMPORT OK', lsms_library.__file__, dr)
EOF
FILES=$(ls tests/ | grep -E '^test_.*(mali|niger|senegal|uganda|ehcvm).*\.py$' | grep -v nigeria | sed 's#^#tests/#' | tr '\n' ' ')
FILES="$FILES tests/test_derivations.py tests/test_build_transform_hash.py tests/test_no_build_input_in_contents_org.py tests/test_failure_is_not_absence.py tests/test_gh812_ehcvm_strata.py tests/test_gh1578_derivation_duplicates.py tests/test_schema_consistency.py tests/test_categorical_mapping_resolution.py tests/test_gh689_uganda_ea_psu.py tests/test_gh1459_uganda_early_ea.py"
[ -f tests/test_gh1707_served_corrections.py ] && FILES="$FILES tests/test_gh1707_served_corrections.py"
FILES="$FILES tests/test_gh1493_near_zero_screen.py"
[ -n "${ONLY:-}" ] && FILES="$ONLY"
FILES=$(echo $FILES | tr ' ' '\n' | sort -u | tr '\n' ' ')
echo "$FILES" | tr ' ' '\n' > $RES/pytest_files_$ARM.txt
$PY -m pytest -n 8 --dist=loadfile -q -p no:cacheprovider -rfEs \
    --junitxml=$RES/junit_$ARM.xml $FILES > $RES/pytest_$ARM.txt 2>&1
echo "pytest rc=$?"
tail -5 $RES/pytest_$ARM.txt
if [ "${HASHPROBE:-0}" = 1 ]; then
  P2=$J/priv-hash; mkdir -p $P2/data $P2/xdg $P2/tmp $P2/mpl
  env PRIVATE_ROOT=$P2 LSMS_DATA_DIR=$P2/data XDG_CACHE_HOME=$P2/xdg TMPDIR=$P2/tmp MPLCONFIGDIR=$P2/mpl \
      LSMS_ISSUES_LOG=$P2/issues.log PYTHONPATH=$J/clone $PY $OUT/scripts/hashprobe.py $J/clone $ARM $RES/hash_$ARM.json > $RES/hash_$ARM.log 2>&1
  echo "hashprobe rc=$?"
fi
echo "in-tree parquets: $(find $J/clone/lsms_library/countries -name '*.parquet' | wc -l)"
echo "issues log: $(wc -l < $P/issues.log 2>/dev/null || echo 0)"
git -C $J/clone status --porcelain | head
echo "TESTS DONE $(date -Is)"
