#!/bin/bash
# Red team ci stream, follow-up: the worker's six test files on base, A, B and the
# A+B merge tree (to reproduce 164/193/157/186), and A's wrapper tests under the
# windows-smoke env knobs that can be imitated on Linux.
set -uo pipefail
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
A=72885852cfc9ab765759c044c6b23489f94f3d7a
B=41f1a44a170cda31a523d7fddb4b7b8ec3c5a6f7
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh ci-redteam $A $B $BASE || exit 9
export PATH="$(dirname "$PY"):$PATH"
unset EXPECT
J=$SLURM_JOB_ID; N=$SLURM_CPUS_ON_NODE
SIX="tests/test_private_ci_check.py tests/test_gh932_safe_ci_failure_locations.py
 tests/test_gh932_pr_data_validation.py tests/test_no_editable_install.py
 tests/test_docs_coverage_hook.py tests/test_api_discoverability.py"
run6() {  # run6 <dir> <data> <label>
  ( cd "$1" && PYTHONPATH=$1 LSMS_DATA_DIR=$2 "$PY" -m pytest -n "$N" --dist=loadfile -q -p no:cacheprovider -rfEsx \
      $SIX > $OUT/pytest6-$3-$J.txt 2>&1; echo "pytest $3 exit $?" >> $OUT/pytest6-$3-$J.txt )
  echo "== six files, $3 @ $(git -C "$1" rev-parse --short=12 HEAD)"; tail -2 $OUT/pytest6-$3-$J.txt
}
run6 "$SRC_3" "$DATA_3" base
run6 "$SRC_1" "$DATA_1" A
run6 "$SRC_2" "$DATA_2" B
C=$JOBLOCAL/combined
git clone --quiet --shared --no-checkout "$MIRROR" "$C"
TREE=$(git -C "$C" merge-tree --write-tree "$A" "$B" | head -1)
M=$(git -C "$C" -c user.name=redteam -c user.email=redteam@invalid commit-tree "$TREE" -p "$A" -p "$B" -m "redteam combined A+B (job-local)")
git -C "$C" checkout --quiet --detach "$M"
mkdir -p $JOBLOCAL/private/data-comb; ident "$C" $JOBLOCAL/private/data-comb
run6 "$C" $JOBLOCAL/private/data-comb combined
echo "== A wrapper tests under windows-smoke-like env"
for v in "PYTHONUNBUFFERED=1" "PYTHONUNBUFFERED=1 PYTHONIOENCODING=cp1252" "PYTHONUNBUFFERED=1 PYTHONUTF8=0 PYTHONIOENCODING=cp1252:strict"; do
  lab=$(echo "$v" | tr ' =:' '_--')
  ( cd "$SRC_1" && env $v PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$N" --dist=loadfile -q -p no:cacheprovider -rfEsx \
      tests/test_private_ci_check.py > $OUT/pytest-A-env-$lab-$J.txt 2>&1; echo "exit $?" >> $OUT/pytest-A-env-$lab-$J.txt )
  echo "-- $v: $(tail -2 $OUT/pytest-A-env-$lab-$J.txt | tr '\n' ' ')"
done
