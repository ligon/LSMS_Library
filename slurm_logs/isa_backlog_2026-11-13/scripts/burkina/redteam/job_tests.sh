#!/bin/bash
# Red team, burkina stream: targeted tests at the candidate, private root warmed in ONE process first.
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina-redteam
source $B/tools/burst-env.sh burkina-redteam "$1" || exit 9
export PATH="$(dirname "$PY"):$PATH"
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" - > $S/logs/tests-warmup-$SLURM_JOB_ID.log 2>&1 <<'PYEOF'
import warnings; warnings.simplefilter('ignore')
from lsms_library.country import Country
for c, tabs in (('Burkina_Faso', ['sample', 'household_roster', 'cluster_features', 'plot_features',
                                  'crop_production', 'food_acquired', 'interview_date', 'plot_inputs']),
                ('Mali', ['sample', 'crop_production', 'plot_features'])):
    for t in tabs:
        try:
            df = getattr(Country(c), t)(); print(c, t, len(df), flush=True)
        except Exception as e:
            print(c, t, 'ERR', type(e).__name__, str(e)[:300], flush=True)
try:
    Country('Burkina_Faso').food_prices(units='unitvalue'); print('food_prices ok')
except Exception as e:
    print('food_prices ERR', e)
PYEOF
FILES="tests/test_burkina_faso_2021_crop_production.py tests/test_gh1045_burkina_interview_date.py
 tests/test_gh1442_burkina_section_c_values.py tests/test_burkina_faso_2014_passages.py
 tests/test_gh824_burkina_unit_sold.py tests/test_burkina_faso_plot_inputs_identity.py
 tests/test_burkina_faso_1483_header_adapter_20261110.py tests/test_burkina_faso_cs_interview_date.py
 tests/test_derivations.py tests/test_gh1045_parse_datetime.py tests/test_schema_consistency.py
 tests/test_build_transform_hash.py tests/test_gh824_mali_ehcvm_crop_production.py"
ls $FILES > /dev/null || echo "some files missing"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -p no:cacheprovider -rfEs $FILES > $OUT/pytest-cand.txt 2>&1
echo "pytest exit $?"
tail -5 $OUT/pytest-cand.txt
