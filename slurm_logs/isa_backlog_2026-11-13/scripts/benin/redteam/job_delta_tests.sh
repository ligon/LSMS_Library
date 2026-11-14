#!/bin/bash
#SBATCH -J rt-benin-delta-tests
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
C2=2aa1e428736450e5190992019c1b5eaa0b71b5ba
source $BATCH/tools/burst-env.sh benin-redteam $C2 || exit 1
O=$OUT/delta_tests; mkdir -p $O
cd $SRC_1; export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
N=$SLURM_CPUS_ON_NODE
T="tests/test_benin_panel_ids.py tests/test_benin_2018_plot_inputs.py tests/test_benin_2021_wave.py
 tests/test_panel_ids_persistence.py tests/test_id_walk.py tests/test_derivations.py tests/test_schema_consistency.py
 tests/test_build_transform_hash.py tests/test_gh1575_benin1575-stratawiring_strata.py tests/test_962_unit_sentinel.py
 tests/test_gh323_benin_togo.py tests/test_gh1226_benin_community_prices.py tests/test_ehcvm_plot_inputs_identity.py"
$PY -m pytest -n $N --dist=loadfile -p no:cacheprovider -q -rfEs $T > $O/pytest-benin.txt 2>&1; echo "exit $?" >> $O/pytest-benin.txt
$PY -m pytest -n $N --dist=loadfile -p no:cacheprovider -q -rfEs -k Benin tests/test_sampling.py tests/test_sample.py tests/test_ehcvm_7day.py > $O/pytest-kBenin.txt 2>&1; echo "exit $?" >> $O/pytest-kBenin.txt
$PY -m pytest -n $N --dist=loadfile -p no:cacheprovider -q -rfE -m "not requires_s3" tests/ > $O/pytest-datafree.txt 2>&1; echo "exit $?" >> $O/pytest-datafree.txt
git status --short > $O/git_status.txt; find lsms_library/countries -name '*.parquet' > $O/intree.txt
echo ALLDONE > $O/ALLDONE
