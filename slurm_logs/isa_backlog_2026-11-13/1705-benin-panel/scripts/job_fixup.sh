#!/bin/bash
#SBATCH -J benin-fixup
# usage: job_fixup.sh <cand-sha> <base-sha>
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh benin $1 $2 || exit 1
O=$OUT/fixup-${1:0:12}; mkdir -p $O
S=$BATCH/reports/benin/scripts
(cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/fixup_facts.py > $O/facts.txt 2>&1; echo "exit $?" >> $O/facts.txt)
(cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/build_plot_inputs.py $O/pi-cand > $O/pi-cand.txt 2>&1; echo "exit $?" >> $O/pi-cand.txt)
(cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" $S/build_plot_inputs.py $O/pi-base > $O/pi-base.txt 2>&1; echo "exit $?" >> $O/pi-base.txt)
(cd $SRC_1 && PYTHONPATH=$SRC_1 "$PY" $S/compare.py $O/pi-base $O/pi-cand $SRC_1/lsms_library/countries/Benin/_/updated_ids.json $O/compare.json > $O/compare.txt 2>&1; echo "exit $?" >> $O/compare.txt)
# make panel-ids regenerates the committed JSON byte for byte
(cd $SRC_1/lsms_library/countries/Benin/_ && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 PATH=$(dirname $PY):$PATH make panel-ids > $O/make_panel_ids.txt 2>&1; echo "exit $?" >> $O/make_panel_ids.txt; git -C $SRC_1 status --short >> $O/make_panel_ids.txt; echo "status-end" >> $O/make_panel_ids.txt)
cd $SRC_1; export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
"$PY" -m pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -p no:cacheprovider -q -rfEs tests/test_benin_panel_ids.py tests/test_benin_2018_plot_inputs.py tests/test_panel_ids_persistence.py tests/test_gh323_benin_togo.py tests/test_ehcvm_plot_inputs_identity.py tests/test_schema_consistency.py tests/test_build_transform_hash.py tests/test_id_walk.py tests/test_api_discoverability.py tests/test_benin_2021_wave.py > $O/pytest.txt 2>&1; echo "exit $?" >> $O/pytest.txt
"$PY" -m pytest -n 4 --dist=loadfile -p no:cacheprovider -q -rfEs -m "not requires_s3" tests/test_sampling.py > $O/pytest-sampling.txt 2>&1; echo "exit $?" >> $O/pytest-sampling.txt
find lsms_library/countries -name '*.parquet' > $O/intree_parquets.txt; git status --short > $O/git_status.txt
(cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $O/census-cand.json) > $O/census.txt 2>&1
(cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $O/census-base.json) >> $O/census.txt 2>&1
$PY $BATCH/tools/hashdiff.py $O/census-base.json $O/census-cand.json $O/hashdiff.json >> $O/census.txt 2>&1; echo "exit $?" >> $O/census.txt
