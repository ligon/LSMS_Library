#!/bin/bash
#SBATCH -J benin-followup
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
SHA=$1
source $BATCH/tools/burst-env.sh benin $SHA || exit 1
O=$OUT/followup-${SHA:0:12}; mkdir -p $O
cd "$SRC_1"; export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
"$PY" $BATCH/reports/benin/scripts/feature_parity.py $O/feature_parity.json > $O/feature_parity.txt 2>&1; echo "exit $?" >> $O/feature_parity.txt
"$PY" -m pytest -n 4 --dist=loadfile -p no:cacheprovider -q -rfEs tests/test_benin_panel_ids.py tests/test_benin_2018_plot_inputs.py tests/test_panel_ids_persistence.py > $O/pytest.txt 2>&1; echo "exit $?" >> $O/pytest.txt
find lsms_library/countries -name '*.parquet' > $O/intree_parquets.txt; git status --short > $O/git_status.txt
