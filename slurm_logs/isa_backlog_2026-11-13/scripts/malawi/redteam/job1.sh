#!/bin/bash
#SBATCH -J mwrt-1
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 16
#SBATCH --mem=96G
#SBATCH -t 3:00:00
#SBATCH --exclude=n0035.savio4,n0034.savio4,n0046.savio4
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi-redteam/logs/%x-%j.out
# Red team, malawi stream: cold base-vs-candidate builds, metadata scan, hash census,
# closure probe, changed tests.  Restartable: each step rewrites its own outputs.
CAND=af81784630ef1ad650a705f4f9c85dde98912797
BASE=f138a3d61c203f892148592675a7ef72e160a322
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/malawi-redteam
source $BATCH/tools/burst-env.sh malawi-redteam $CAND $BASE || exit 9
O=$OUT/job1; mkdir -p $O/frames $O/scan
SCANROOT=/local/job$SLURM_JOB_ID/private/data-scan; mkdir -p $SCANROOT
echo "cand $SRC_1 $DATA_1 | base $SRC_2 $DATA_2 | scan $SCANROOT | $(date -Is)"
arm() { local which=$1; shift; if [ $which = cand ]; then cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$@"; else cd $SRC_2 && env PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$@"; fi; }

( arm cand $PY $RT/scripts/build_arm.py $SRC_1 cand $O/frames > $O/frames/cand.out 2>&1; echo "build cand rc=$? $(date -Is)" ) &
( arm base $PY $RT/scripts/build_arm.py $SRC_2 base $O/frames > $O/frames/base.out 2>&1; echo "build base rc=$? $(date -Is)" ) &
( cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$SCANROOT $PY $RT/scripts/scan_meta.py $SRC_1 $O/scan > $O/scan/scan.out 2>&1; echo "scan rc=$? $(date -Is)" ) &
( arm cand $PY $BATCH/tools/hash_census.py $O/census-cand.json > $O/census-cand.log 2>&1; echo "census cand rc=$?"
  arm base $PY $BATCH/tools/hash_census.py $O/census-base.json > $O/census-base.log 2>&1; echo "census base rc=$?"
  $PY $BATCH/tools/hashdiff.py $O/census-base.json $O/census-cand.json $O/hashdiff.json; echo "hashdiff rc=$?"
  arm cand $PY $RT/scripts/fwimp_parts.py $SRC_1 $O/fwimp-cand.json > $O/fwimp-cand.log 2>&1; echo "fwimp cand rc=$?"
  arm base $PY $RT/scripts/fwimp_parts.py $SRC_2 $O/fwimp-base.json > $O/fwimp-base.log 2>&1; echo "fwimp base rc=$?" ) &
wait
echo "phase 1 done $(date -Is)"

# changed / relevant tests, each arm on its own (now warm) root
TF="tests/test_gh1228_malawi_cowpea_label.py tests/test_malawi_cassava_recode.py tests/test_gh1190_selected_farm_carrier.py tests/test_1111_malawi_applied_kg.py tests/test_malawi_sale_basis.py tests/test_build_transform_hash.py tests/test_derivations.py tests/test_schema_consistency.py"
( arm cand $PY -m pytest -n 6 --dist=loadfile -q -p no:cacheprovider -rfEs $TF > $O/pytest-cand.txt 2>&1; echo "pytest cand rc=$?" ) &
( arm base $PY -m pytest -n 6 --dist=loadfile -q -p no:cacheprovider -rfEs $TF > $O/pytest-base.txt 2>&1; echo "pytest base rc=$?" ) &
wait
( arm cand $PY -m pytest -n 4 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_table_structure.py -k Malawi > $O/pytest-ts-cand.txt 2>&1; echo "ts cand rc=$?" ) &
( arm base $PY -m pytest -n 4 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_table_structure.py -k Malawi > $O/pytest-ts-base.txt 2>&1; echo "ts base rc=$?" ) &
wait
for f in $O/pytest-cand.txt $O/pytest-base.txt $O/pytest-ts-cand.txt $O/pytest-ts-base.txt; do echo "== $f"; tail -n 3 $f; done
for s in $SRC_1 $SRC_2; do echo "in-tree parquets $s: $(find $s/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $s status --porcelain | head -5; done
echo "JOB DONE $(date -Is)"
