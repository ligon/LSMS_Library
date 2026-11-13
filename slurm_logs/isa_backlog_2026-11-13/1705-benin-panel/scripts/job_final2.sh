#!/bin/bash
#SBATCH -J benin-final2
# usage: job_final2.sh <cand-sha> <base-sha>  -- census, Feature parity, regeneration check, new tests at the head
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh benin $1 $2 || exit 1
O=$OUT/final2-${1:0:12}; mkdir -p $O; S=$BATCH/reports/benin/scripts
(cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $O/census-cand.json) > $O/census.txt 2>&1
(cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $O/census-base.json) >> $O/census.txt 2>&1
$PY $BATCH/tools/hashdiff.py $O/census-base.json $O/census-cand.json $O/hashdiff.json >> $O/census.txt 2>&1; echo "exit $?" >> $O/census.txt
(cd $SRC_1/lsms_library/countries/Benin/_ && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 PATH=$(dirname $PY):$PATH make panel-ids > $O/make_panel_ids.txt 2>&1; echo "exit $?" >> $O/make_panel_ids.txt; git -C $SRC_1 status --short >> $O/make_panel_ids.txt; echo status-end >> $O/make_panel_ids.txt)
(cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/feature_parity.py $O/feature_parity.json > $O/feature_parity.txt 2>&1; echo "exit $?" >> $O/feature_parity.txt)
cd $SRC_1; export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
"$PY" -m pytest -n 4 --dist=loadfile -p no:cacheprovider -q -rfEs tests/test_benin_panel_ids.py tests/test_benin_2018_plot_inputs.py tests/test_panel_ids_persistence.py tests/test_derivations.py > $O/pytest.txt 2>&1; echo "exit $?" >> $O/pytest.txt
find lsms_library/countries -name '*.parquet' > $O/intree_parquets.txt; git status --short > $O/git_status.txt
