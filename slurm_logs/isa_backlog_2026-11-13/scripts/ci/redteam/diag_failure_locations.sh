#!/bin/bash
# Red team: why does test_real_wrapper_optin_... fail on base, A and B alike in
# job 41275398?  Hypothesis: private_ci_check._git's 2 s timeout under load with
# a --shared clone whose objects live in the NFS mirror's alternates.
set -uo pipefail
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh ci-redteam $BASE || exit 9
export PATH="$(dirname "$PY"):$PATH"
cd "$SRC_1"
echo "alternates: $(cat .git/objects/info/alternates 2>/dev/null)"
for k in 1 2 3; do
  /usr/bin/time -f "ls-tree %e s" git --no-optional-locks -c core.fsmonitor=false ls-tree -z HEAD -- tests/test_private_ci_check.py > /dev/null
  b=$(git ls-tree HEAD -- tests/test_private_ci_check.py | awk '{print $3}')
  /usr/bin/time -f "cat-file %e s" git cat-file blob "$b" > /dev/null
done
T=tests/test_gh932_safe_ci_failure_locations.py::test_real_wrapper_optin_preserves_stream_privacy_cleanup_status_and_static_names
for k in 1 2 3; do
  PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -q -p no:cacheprovider "$T" 2>&1 | tail -1
done
echo "-- with the git timeout raised to 30 s (job-local edit of this throwaway clone only)"
sed -i 's/timeout=2, check=False/timeout=30, check=False/' scripts/private_ci_check.py
grep -c 'timeout=30, check=False' scripts/private_ci_check.py
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -q -p no:cacheprovider "$T" 2>&1 | tail -1
git checkout --quiet -- scripts/private_ci_check.py
