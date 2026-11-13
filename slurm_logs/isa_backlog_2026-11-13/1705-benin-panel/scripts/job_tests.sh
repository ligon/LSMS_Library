#!/bin/bash
#SBATCH -J benin-tests
# usage: job_tests.sh <label> <sha>
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
LABEL=$1; SHA=$2
source $BATCH/tools/burst-env.sh benin $SHA || exit 1
O=$OUT/tests-$LABEL-${SHA:0:12}; mkdir -p $O
cd "$SRC_1"
export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
N=$SLURM_CPUS_ON_NODE
FULL="tests/test_panel_ids_persistence.py tests/test_id_walk.py tests/test_derivations.py tests/test_schema_consistency.py
 tests/test_build_transform_hash.py tests/test_gh1575_benin1575-stratawiring_strata.py tests/test_gh1226_benin_community_prices.py
 tests/test_gh323_benin_togo.py tests/test_gh323_benin_cluster_features.py tests/test_api_discoverability.py
 tests/test_ehcvm_plot_inputs_identity.py tests/test_gh1039_make_backend_no_country_rule.py tests/test_gh968_no_make_fallback.py
 tests/test_gh323_grain_contract.py tests/test_ehcvm_unit_size.py tests/test_metric_decimal_comma.py tests/test_wave_provenance.py
 tests/test_gh1093_declared_unit_consumers.py tests/test_gh1475_country_crop_transforms.py tests/test_gh1460_yield_land_route.py
 tests/test_population.py tests/test_benin_2021_wave.py"
for f in tests/test_benin_panel_ids.py tests/test_benin_2018_plot_inputs.py; do [ -f $f ] && FULL="$FULL $f"; done
"$PY" -m pytest -n $N --dist=loadfile -p no:cacheprovider -q -rfEs $FULL > $O/pytest-full.txt 2>&1; echo "exit $?" >> $O/pytest-full.txt
"$PY" -m pytest -n 4 --dist=loadfile -p no:cacheprovider -q -rfEs -m "not requires_s3" tests/test_sampling.py > $O/pytest-sampling-nos3.txt 2>&1; echo "exit $?" >> $O/pytest-sampling-nos3.txt
KB="tests/test_sampling.py tests/test_sample.py tests/test_feature_canonical_index.py tests/test_currency.py tests/test_gh1063_roster_order.py
 tests/test_ehcvm_7day.py tests/test_gh812_ehcvm_strata.py tests/test_863_transforms.py tests/test_feature_attrs_carriers.py
 tests/test_shocks_extension_food_effects.py tests/test_gh805_cp1252_reader.py tests/test_ehcvm_crop_production_condition.py"
"$PY" -m pytest -n $N --dist=loadfile -p no:cacheprovider -q -rfEs -k Benin $KB > $O/pytest-kBenin.txt 2>&1; echo "exit $?" >> $O/pytest-kBenin.txt
find lsms_library/countries -name '*.parquet' > $O/intree_parquets.txt
git status --short > $O/git_status.txt
