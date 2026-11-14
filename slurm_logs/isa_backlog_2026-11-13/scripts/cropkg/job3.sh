#!/bin/bash
# Attribute issues.log lines written during the test step: suspect files, base vs candidate.
set -uo pipefail
CAND=$1; BASE=$2
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh cropkg "$CAND" "$BASE" || exit 9
J=${SLURM_JOB_ID}; JL=/local/job$J
FILES="tests/test_failure_is_not_absence.py tests/test_feature_attrs_carriers.py tests/test_null_read_guard.py tests/test_null_index_guard.py tests/test_gh1559_survey_kg_price_screen.py tests/test_quantity_screen.py"
for side in 1 2; do
  s=SRC_$side; d=DATA_$side; src=${!s}; data=${!d}
  for f in $FILES; do
    log=$JL/issues-$side-$(basename $f .py).log; rm -f $log
    ( cd "$src" && [ -f "$f" ] && PYTHONPATH=$src LSMS_DATA_DIR=$data LSMS_ISSUES_LOG=$log "$PY" -m pytest -q -p no:cacheprovider -m "not requires_s3" "$f" > $JL/p-$side-$(basename $f .py).txt 2>&1 )
    echo "side $side $(git -C $src rev-parse --short HEAD) $f: issues lines $(wc -l < $log 2>/dev/null || echo 0); $(tail -1 $JL/p-$side-$(basename $f .py).txt 2>/dev/null)"
  done
done
echo done
