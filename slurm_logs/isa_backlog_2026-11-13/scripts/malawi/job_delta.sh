#!/bin/bash
#SBATCH -J mw-delta
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113; REP=$BATCH/reports/malawi
source $BATCH/tools/burst-env.sh malawi 5093b49398424e953fe049cfe12b3f29eb991443 9ca3518a35e6c688577283121baeb49a939b9e0f || exit 9
V=$OUT/verify-delta; mkdir -p $V
arm() { local which=$1; shift; if [ $which = cand ]; then cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$@"; else cd $SRC_2 && env PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$@"; fi; }
arm cand $PY -m pytest -q -p no:cacheprovider -m "not requires_s3" tests/test_gh1190_selected_farm_carrier.py tests/test_derivations.py tests/test_gh1228_malawi_cowpea_label.py tests/test_no_build_input_in_contents_org.py > $V/pytest-gate.txt 2>&1; echo "gate rc=$?"; tail -1 $V/pytest-gate.txt
( arm cand $PY $REP/scripts/pi_frames.py $SRC_1 cand $V > $V/pi-cand.log 2>&1; echo "pi cand rc=$?" ) &
( arm base $PY $REP/scripts/pi_frames.py $SRC_2 base $V > $V/pi-base.log 2>&1; echo "pi base rc=$?" ) &
wait
arm cand $PY $REP/scripts/pi_cmp.py $V; echo "cmp rc=$?"
arm cand $PY -m pytest -n 4 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_gh1190_selected_farm_carrier.py tests/test_derivations.py tests/test_gh1228_malawi_cowpea_label.py tests/test_no_build_input_in_contents_org.py tests/test_malawi_sale_basis.py tests/test_gh1457_malawi_farm_level_sales.py > $V/pytest-cand.txt 2>&1; echo "pytest cand rc=$?"; tail -1 $V/pytest-cand.txt; grep -E "^FAILED|^SKIPPED" $V/pytest-cand.txt | head
arm cand $PY -m pytest -n 4 --dist=loadfile -q -p no:cacheprovider -rfEs tests/test_table_structure.py -k "Malawi and plot_inputs" > $V/pytest-ts.txt 2>&1; echo "ts rc=$?"; tail -1 $V/pytest-ts.txt
arm cand $PY $BATCH/tools/hash_census.py $V/census-cand.json > $V/census-cand.log 2>&1; echo "census rc=$?"
echo "vs 9ca3518a3:"; $PY $BATCH/tools/hashdiff.py $OUT/verify-final/census-cand.json $V/census-cand.json $V/hd-vs-prev.json
echo "vs f138a3d61:"; $PY $BATCH/tools/hashdiff.py $OUT/verify-v5/census-base.json $V/census-cand.json $V/hd-vs-base.json
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name "*.parquet" | wc -l)"
echo "JOB DONE $(date -Is)"
