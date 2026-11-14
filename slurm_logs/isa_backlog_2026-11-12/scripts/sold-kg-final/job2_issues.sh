#!/bin/bash
#SBATCH -J soldkg-issues
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-final/logs/%x-%j.out
# Attribute job 41252145's 12 issues.log lines: run test_gh1493_near_zero_screen.py
# (not requires_s3) alone with the issues log on persistent private scratch, and
# report only entry headers and error class names.
# Usage: sbatch --chdir=<out dir> job2_issues.sh NEW_SHA
set -uo pipefail
NEW=$1; [ ${#NEW} -eq 40 ] || { echo "need full SHA"; exit 2; }
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-final
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/sold-kg-final/job-${SLURM_JOB_ID}
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
JOBLOCAL=/local/job${SLURM_JOB_ID}
mkdir -p "$JOBLOCAL" "$PRIV/data"
export TMPDIR=$JOBLOCAL/tmp XDG_CACHE_HOME=$JOBLOCAL/xdg MPLCONFIGDIR=$JOBLOCAL/mpl
export LSMS_ISSUES_LOG=$PRIV/issues.log LSMS_DATA_DIR=$PRIV/data
mkdir -p "$TMPDIR" "$XDG_CACHE_HOME" "$MPLCONFIGDIR"
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_QUANTITY_STRICT || true
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
CLONE=$JOBLOCAL/new
git clone --quiet --shared --no-checkout "$MIRROR" "$CLONE"
git -C "$CLONE" checkout --quiet --detach "$NEW"
[ "$(git -C "$CLONE" rev-parse HEAD)" = "$NEW" ] || exit 3
cd "$CLONE"
bin/savio_venv.sh mount || exit 4
PY=$CLONE/.venv/bin/python
export PYTHONPATH=$CLONE
"$PY" - <<'EOF' || exit 5
import lsms_library, os, subprocess
from lsms_library import paths
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
r = os.path.realpath(str(paths.data_root()))
assert '/private/sold-kg-final/' in r, r
print('identity ok', lsms_library.__file__, r)
EOF
"$PY" -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider -m "not requires_s3" \
  tests/test_gh1493_near_zero_screen.py > "$PRIV/pytest-gh1493.txt" 2>&1
echo "pytest exit $?"; tail -1 "$PRIV/pytest-gh1493.txt"
echo "issues log lines: $(wc -l < "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
echo "entries: $(grep -c '^## ' "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
grep '^## ' "$LSMS_ISSUES_LOG" 2>/dev/null | cut -d' ' -f4- | sort | uniq -c
grep -o 'Error: `[A-Za-z_.]*' "$LSMS_ISSUES_LOG" 2>/dev/null | sort | uniq -c
echo "in-tree parquet: $(find "$CLONE/lsms_library/countries" -name '*.parquet' | wc -l)"
bin/savio_venv.sh umount || true
echo done
