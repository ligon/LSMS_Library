#!/bin/bash
# Red team GH #1559 delta: census, tests, real-data guard check, s7fq13d wiring.  sbatch ... jobG.sh CAND BASE
set -uo pipefail
CAND=$1; BASE=$2
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg-redteam
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh cropkg-redteam "$CAND" "$BASE" || exit 9
J=$SLURM_JOB_ID
run_c() { ( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$@" ); }
run_b() { ( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" "$@" ); }
echo "=== census"; run_c $BATCH/tools/hash_census.py $OUT/G-census-cand-$J.json; run_b $BATCH/tools/hash_census.py $OUT/G-census-base-$J.json
$PY $BATCH/tools/hashdiff.py $OUT/G-census-base-$J.json $OUT/G-census-cand-$J.json $OUT/G-hashdiff-$J.json
echo "=== wire"; run_c "$RT/scripts/rt_wire.py" "$OUT/G-wire-$J.json"; echo "exit $?"
echo "=== guard on real Niger"
run_c - <<'EOF' > $OUT/G-guard-$J.txt 2>&1
import warnings, lsms_library as ll
from lsms_library import quantity_audit as Q
from collections import Counter
for which in ('distributional_raises', 'price_raises'):
    Q._clear_quantity_reports()
    saved = (Q.audit_quantities, Q.audit_survey_kg_prices)
    def boom(*a, **k): raise ValueError('injected')
    if which == 'distributional_raises': Q.audit_quantities = boom
    else: Q.audit_survey_kg_prices = boom
    try:
        with warnings.catch_warnings(record=True) as c:
            warnings.simplefilter('always'); df = ll.Country('Niger').crop_production()
    finally:
        Q.audit_quantities, Q.audit_survey_kg_prices = saved
    reps = Q.quantity_reports(country='Niger', table='crop_production')
    print(which, len(df), sorted(Counter((r['site'], r['column']) for r in reps).items()),
          [w.category.__name__ for w in c if 'could not run' in str(w.message)])
    print(' warning text ok:', any('may be the answer; that is not established' in str(w.message) for w in c),
          'old text present:', any('has been the answer in this corpus' in str(w.message) for w in c))
EOF
cat $OUT/G-guard-$J.txt | grep -v Warning: | tail -6
echo "=== tests"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -rs -p no:cacheprovider \
   tests/test_gh1559_survey_kg_price_screen.py tests/test_quantity_screen.py tests/test_build_transform_hash.py \
   > $OUT/G-pytest-$J.txt 2>&1 ); echo "pytest exit $?"; tail -3 $OUT/G-pytest-$J.txt
echo "in-tree parquet: $(find "$SRC_1/lsms_library/countries" "$SRC_2/lsms_library/countries" -name '*.parquet' | wc -l)"
( cd "$SRC_1" && bin/savio_venv.sh umount ) || true
echo done
