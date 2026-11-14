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
#SBATCH -J fx1604-code
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-final/logs/job1-%j.out
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-final
SCR=$OUT/scripts; RES=$OUT/results; FR=$OUT/frames; LOG=$OUT/logs
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
OLD=7735489cceb0fd68acf273eff850a5974d71cc58
NEW=${NEW_SHA:?}
J=/local/job$SLURM_JOB_ID
mkdir -p $RES $FR $LOG
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_DATA_DIR
echo "node $(hostname) start $(date -Is) NEW=$NEW"

for arm in base old new; do
  sha=$BASE; [ $arm = old ] && sha=$OLD; [ $arm = new ] && sha=$NEW
  git clone -q --no-hardlinks $MIRROR $J/$arm || exit 2
  git -C $J/$arm checkout -q --detach $sha || exit 2
  echo "$arm HEAD $(git -C $J/$arm rev-parse HEAD)"
done
( cd $J/new && bin/savio_venv.sh mount ) || exit 3
( cd $J/old && bin/savio_venv.sh mount ) || exit 3
( cd $J/base && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python

envp() {  # envp <arm> <subroot> cmd...
  local arm=$1 P=$J/$2; shift 2; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$arm "$@"
}

# A. hash footprint (config/sidecar/source hashes only)
for arm in base old new; do
  ( cd $J/$arm && envp $arm priv-hash-$arm $PY $SCR/hashprobe.py $J/$arm $arm $RES/hash_$arm.json > $LOG/hash_$arm.log 2>&1 )
  echo "hash $arm rc=$?"
done
$PY $SCR/hashcompare.py $RES/hash_base.json $RES/hash_old.json $RES/hash_new.json > $RES/hash_compare.json 2> $LOG/hashcompare.err
echo "hashcompare rc=$?"

# B. cold builds, three arms in parallel, each in its own private root
for arm in base old new; do
  ( cd $J/$arm && envp $arm priv-$arm $PY $SCR/build.py $J/$arm $arm $FR > $LOG/build_$arm.log 2>&1; echo "build $arm rc=$?" ) &
done
wait
echo "builds done $(date -Is)"

# C. new-arm probes and three-arm compare
( cd $J/new && envp new priv-new $PY $SCR/probe_new.py $J/new $FR $RES/probe_new.json > $LOG/probe_new.log 2>&1 ); echo "probe rc=$?"
$PY $SCR/compare.py $FR $RES/compare.json > $LOG/compare.log 2>&1; echo "compare rc=$?"

# D. make panel-ids byte identity on the new clone
( cd $J/new/lsms_library/countries/Malawi/_ && envp new priv-panel bash -c "PATH=$J/venv_img/bin:\$PATH; python -c 'import lsms_library,os; print(os.path.realpath(lsms_library.__file__))'; make panel-ids" > $LOG/make_panel_ids.log 2>&1 ); echo "make panel-ids rc=$?"
{ git -C $J/new status --porcelain; git -C $J/new diff --exit-code --quiet -- lsms_library/countries/Malawi/_/panel_ids.json lsms_library/countries/Malawi/_/updated_ids.json; echo "panel-ids json diff exit=$?";
  sha256sum $J/new/lsms_library/countries/Malawi/_/panel_ids.json $J/new/lsms_library/countries/Malawi/_/updated_ids.json;
  git -C $J/new show $NEW:lsms_library/countries/Malawi/_/panel_ids.json | sha256sum; git -C $J/new show $NEW:lsms_library/countries/Malawi/_/updated_ids.json | sha256sum; } > $RES/make_panel_ids.txt 2>&1
cat $RES/make_panel_ids.txt

# E. targeted tests on the new clone (warm new-arm root)
cd $J/new
FILES="$(ls tests/test_*malawi*.py) tests/test_panel_ids_persistence.py tests/test_id_walk.py $(ls tests/test_gh970*.py) $(ls tests/test_recall*.py) tests/test_build_transform_hash.py tests/test_derivations.py tests/test_no_build_input_in_contents_org.py tests/test_gh1578_derivation_duplicates.py tests/test_notes.py tests/test_gh1190_selected_farm_carrier.py tests/test_gh1210_invalid_food_waves.py tests/test_failure_is_not_absence.py tests/test_schema_consistency.py tests/test_categorical_mapping_resolution.py"
echo "$FILES" | tr ' ' '\n' > $RES/pytest_files.txt
envp new priv-new $PY -m pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -p no:cacheprovider -rfEs $FILES > $RES/pytest_targeted.txt 2>&1; echo "pytest targeted rc=$?"
envp new priv-new $PY -m pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_sample.py -k Malawi > $RES/pytest_sample_malawi.txt 2>&1; echo "pytest sample rc=$?"
tail -3 $RES/pytest_targeted.txt $RES/pytest_sample_malawi.txt

for arm in base old new; do
  echo "in-tree parquets $arm: $(find $J/$arm/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $arm:"; git -C $J/$arm status --porcelain | head -20
done
du -sh $J/priv-* 2>/dev/null
echo "JOB1 DONE $(date -Is)"
