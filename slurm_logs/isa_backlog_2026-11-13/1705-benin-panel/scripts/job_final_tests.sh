#!/bin/bash
#SBATCH -J benin-final-tests
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
SHA=$1
source $BATCH/tools/burst-env.sh benin $SHA || exit 1
O=$OUT/final-tests-${SHA:0:12}; mkdir -p $O
cd "$SRC_1"; export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
"$PY" -m pytest -n 8 --dist=loadfile -p no:cacheprovider -q -rfEs tests/test_sampling.py tests/test_benin_panel_ids.py tests/test_benin_2018_plot_inputs.py tests/test_panel_ids_persistence.py tests/test_id_walk.py tests/test_schema_consistency.py -m "not requires_s3" > $O/pytest-nos3.txt 2>&1; echo "exit $?" >> $O/pytest-nos3.txt
"$PY" -m pytest -n 4 --dist=loadfile -p no:cacheprovider -q -rfEs tests/test_benin_panel_ids.py tests/test_benin_2018_plot_inputs.py -m requires_s3 > $O/pytest-s3.txt 2>&1; echo "exit $?" >> $O/pytest-s3.txt
git status --short > $O/git_status.txt; find lsms_library/countries -name '*.parquet' > $O/intree_parquets.txt
