#!/bin/bash
#SBATCH -J rt-benin-census
# Hash census base vs candidate, then the brief's tests (with data, private root) and the
# whole data-free suite at the candidate head.
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh benin-redteam $CAND $BASE || exit 1
O=$OUT/census_tests; mkdir -p $O
(cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $O/census-cand.json > $O/census-cand.log 2>&1) &
(cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $O/census-base.json > $O/census-base.log 2>&1) &
wait
$PY $BATCH/tools/hashdiff.py $O/census-base.json $O/census-cand.json $O/hashdiff.json > $O/hashdiff.txt 2>&1

cd $SRC_1
export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
N=$SLURM_CPUS_ON_NODE
BRIEF="tests/test_benin_panel_ids.py tests/test_benin_2018_plot_inputs.py tests/test_benin_2021_wave.py
 tests/test_panel_ids_persistence.py tests/test_id_walk.py tests/test_derivations.py tests/test_schema_consistency.py
 tests/test_build_transform_hash.py tests/test_ehcvm_plot_inputs_identity.py tests/test_962_unit_sentinel.py
 tests/test_gh1039_make_backend_no_country_rule.py tests/test_gh968_no_make_fallback.py tests/test_gh323_benin_togo.py
 tests/test_gh1226_benin_community_prices.py tests/test_gh1575_benin1575-stratawiring_strata.py"
$PY -m pytest -n $N --dist=loadfile -p no:cacheprovider -q -rfEs $BRIEF > $O/pytest-brief.txt 2>&1; echo "exit $?" >> $O/pytest-brief.txt
$PY -m pytest -n $N --dist=loadfile -p no:cacheprovider -q -rfEs -k Benin tests/test_sampling.py tests/test_sample.py tests/test_ehcvm_7day.py tests/test_feature_canonical_index.py tests/test_population.py > $O/pytest-kBenin.txt 2>&1; echo "exit $?" >> $O/pytest-kBenin.txt
$PY -m pytest -n $N --dist=loadfile -p no:cacheprovider -q -rfEs -m "not requires_s3" tests/ > $O/pytest-datafree.txt 2>&1; echo "exit $?" >> $O/pytest-datafree.txt
git status --short > $O/git_status.txt; find lsms_library/countries -name '*.parquet' > $O/intree_parquets.txt
echo ALLDONE > $O/ALLDONE
