#!/bin/bash
#SBATCH -J mw-final
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 16
#SBATCH --mem=96G
#SBATCH -t 4:00:00
#SBATCH --exclude=n0035.savio4,n0034.savio4,n0046.savio4
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
# Final head: gate, the full test set, table structure, hash census against v5's.
CAND=${CAND:?}; TAG=${TAG:?}
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
REP=$BATCH/reports/malawi
source $BATCH/tools/burst-env.sh malawi $CAND || exit 9
V=$OUT/verify-$TAG; mkdir -p $V
V5=$OUT/verify-v5
echo "cand $SRC_1 $DATA_1 | $(date -Is)"
arm() { cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$@"; }
arm $PY -m pytest -q -p no:cacheprovider -m "not requires_s3" tests/test_gh1228_malawi_cowpea_label.py tests/test_malawi_sale_basis.py tests/test_gh1190_selected_farm_carrier.py tests/test_derivations.py tests/test_1111_malawi_applied_kg.py tests/test_gh1457_malawi_farm_level_sales.py tests/test_no_build_input_in_contents_org.py > $V/pytest-gate.txt 2>&1
GATE=$?; echo "gate rc=$GATE"; tail -1 $V/pytest-gate.txt
[ $GATE -eq 0 ] || { echo "GATE FAILED, stopping"; exit 1; }
FILES="$(cat $V5/pytest_files.txt | tr '\n' ' ')"
arm $PY -m pytest -n 7 --dist=loadfile -q -p no:cacheprovider -rfEs $FILES > $V/pytest-cand.txt 2>&1; echo "pytest cand rc=$?"; tail -1 $V/pytest-cand.txt
arm $PY -m pytest -n 4 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_table_structure.py -k Malawi > $V/pytest-table-structure-cand.txt 2>&1; echo "ts cand rc=$?"; tail -1 $V/pytest-table-structure-cand.txt
arm $PY $BATCH/tools/hash_census.py $V/census-cand.json > $V/census-cand.log 2>&1; echo "census cand rc=$?"
echo "vs 51a918684:"; $PY $BATCH/tools/hashdiff.py $V5/census-cand.json $V/census-cand.json $V/hashdiff-vs-v5.json
echo "vs f138a3d61:"; $PY $BATCH/tools/hashdiff.py $V5/census-base.json $V/census-cand.json $V/hashdiff-vs-base.json
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $SRC_1 status --porcelain | head -5
echo "JOB DONE $(date -Is)"
