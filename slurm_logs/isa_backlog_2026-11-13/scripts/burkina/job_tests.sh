#!/bin/bash
# pytest on the candidate.  Usage: job_tests.sh <sha> <tag>
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina
source $B/tools/burst-env.sh burkina "$1" || exit 9
TAG=${2:-t1}
export PATH="$(dirname "$PY"):$PATH"
cd "$SRC_1"
# Warm the private root once, so test files do not race to build the same tables cold.
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" - > $S/logs/tests-$TAG-warmup-$SLURM_JOB_ID.log 2>&1 <<'PYEOF'
import warnings; warnings.simplefilter('ignore')
import lsms_library as ll
c = ll.Country('Burkina_Faso')
for t in sorted(c.data_scheme):
    if t in ('panel_ids', 'updated_ids'):
        continue
    try:
        print(t, len(getattr(ll.Country('Burkina_Faso'), t)()), flush=True)
    except Exception as e:
        print(t, 'ERR', type(e).__name__, str(e)[:300], flush=True)
PYEOF
FILES=$(ls tests/test_*burkina*.py | tr '\n' ' ')
FILES="$FILES tests/test_gh1045_parse_datetime.py tests/test_derivations.py tests/test_schema_consistency.py tests/test_build_transform_hash.py tests/test_gh824_mali_ehcvm_crop_production.py tests/test_gh1437_mali_harvest_total.py tests/test_ehcvm_crop_production_condition.py tests/test_gh1528_sold_condition.py tests/test_food_kg_size_pool.py tests/test_gh1452_older_rows.py tests/test_gh1509_1068_1129_food_unit_evidence.py tests/test_ehcvm_unit_weights.py tests/test_recall_period.py tests/test_unpriceable_price_drop.py tests/test_feature_canonical_index.py tests/test_ehcvm_plot_inputs_identity.py tests/test_u_sentinel_protection.py tests/test_gh1460_yield_land_route.py tests/test_currency.py tests/test_ehcvm_condition_helpers.py tests/test_gh812_ehcvm_strata.py tests/test_sampling.py"
echo "$FILES" > $OUT/pytest-$TAG-files.txt
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -p no:cacheprovider -rfE $FILES > $OUT/pytest-$TAG.txt 2>&1
echo "pytest exit $?"
tail -3 $OUT/pytest-$TAG.txt
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"
