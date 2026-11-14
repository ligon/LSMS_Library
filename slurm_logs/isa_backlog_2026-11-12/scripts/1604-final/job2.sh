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
#SBATCH -J fx1604-final
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-final/logs/job2-%j.out
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-final
SCR=$OUT/scripts; RES=$OUT/results/final; FR=$OUT/frames; LOG=$OUT/logs/final
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
FINAL=${FINAL_SHA:?}
J=/local/job$SLURM_JOB_ID
mkdir -p $RES $LOG
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_DATA_DIR
echo "node $(hostname) start $(date -Is) FINAL=$FINAL"
for arm in base final; do
  sha=$BASE; [ $arm = final ] && sha=$FINAL
  git clone -q --no-hardlinks $MIRROR $J/$arm || exit 2
  git -C $J/$arm checkout -q --detach $sha || exit 2
  echo "$arm HEAD $(git -C $J/$arm rev-parse HEAD)"
done
( cd $J/final && bin/savio_venv.sh mount ) || exit 3
( cd $J/base && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
envp() { local arm=$1 P=$J/$2; shift 2; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$arm "$@"; }

for arm in base final; do
  ( cd $J/$arm && envp $arm priv-hash-$arm $PY $SCR/hashprobe.py $J/$arm $arm $RES/hash_$arm.json > $LOG/hash_$arm.log 2>&1 ); echo "hash $arm rc=$?"
done
$PY $SCR/hashcompare.py $RES/hash_base.json $RES/hash_final.json > $RES/hash_compare.json 2> $LOG/hashcompare.err; echo "hashcompare rc=$?"
$PY $SCR/hashcompare.py $OUT/results/hash_new.json $RES/hash_final.json > $RES/hash_code_vs_final.json 2>> $LOG/hashcompare.err; echo "hash code-vs-final rc=$?"

( cd $J/final/lsms_library/countries/Malawi/_ && envp final priv-panel bash -c "PATH=$J/venv_img/bin:\$PATH; python -c 'import lsms_library,os; print(os.path.realpath(lsms_library.__file__))'; make panel-ids" > $LOG/make_panel_ids.log 2>&1 ); echo "make panel-ids rc=$?"
{ git -C $J/final status --porcelain; git -C $J/final diff --exit-code --quiet -- lsms_library/countries/Malawi/_/panel_ids.json lsms_library/countries/Malawi/_/updated_ids.json; echo "panel-ids json diff exit=$?"; } > $RES/make_panel_ids.txt 2>&1
cat $RES/make_panel_ids.txt

( cd $J/final && envp final priv-labels $PY $SCR/labels.py $J/final $FR $RES/labels.json > $LOG/labels.log 2>&1 ); echo "labels rc=$?"

cd $J/final
FILES="tests/test_notes.py tests/test_no_build_input_in_contents_org.py tests/test_derivations.py tests/test_gh1578_derivation_duplicates.py tests/test_malawi_food_borrowing.py tests/test_malawi_panel_id_guard.py tests/test_gh1237_malawi_make_prereqs.py tests/test_build_transform_hash.py tests/test_panel_ids_persistence.py tests/test_recall_record.py tests/test_recall_period.py tests/test_gh970_no_json_make.py tests/test_gh1190_selected_farm_carrier.py tests/test_schema_consistency.py"
envp final priv-pytest $PY -m pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -p no:cacheprovider -rfEs $FILES > $RES/pytest_final.txt 2>&1; echo "pytest final rc=$?"
tail -3 $RES/pytest_final.txt
for arm in base final; do echo "in-tree parquets $arm: $(find $J/$arm/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $J/$arm status --porcelain | head; done
echo "JOB2 DONE $(date -Is)"
