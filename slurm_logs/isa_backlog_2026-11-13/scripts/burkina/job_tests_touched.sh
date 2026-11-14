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
FILES="tests/test_burkina_faso_2021_crop_production.py tests/test_derivations.py tests/test_gh824_burkina_unit_sold.py tests/test_ehcvm_crop_production_condition.py tests/test_gh1528_sold_condition.py tests/test_schema_consistency.py"
echo "$FILES" > $OUT/pytest-$TAG-files.txt
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -p no:cacheprovider -rfE $FILES > $OUT/pytest-$TAG.txt 2>&1
echo "pytest exit $?"
tail -3 $OUT/pytest-$TAG.txt
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"
