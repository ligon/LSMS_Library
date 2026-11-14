#!/bin/bash
# Red team: the real-wrapper failure-location test fails on base/A/B burst-env clones
# under xdist but passes alone and in a merge-tree clone.  Is it _git's 2 s timeout?
set -uo pipefail
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh ci-redteam $BASE || exit 9
export PATH="$(dirname "$PY"):$PATH"
cd "$SRC_1"; N=$SLURM_CPUS_ON_NODE
F="tests/test_private_ci_check.py tests/test_gh932_safe_ci_failure_locations.py tests/test_gh932_pr_data_validation.py"
for k in 1 2; do
  echo "unpatched xdist run $k: $(PYTHONPATH=$SRC_1 "$PY" -m pytest -n $N --dist=loadfile -q -p no:cacheprovider $F 2>&1 | tail -1)"
done
echo "serial, whole failure-locations file: $(PYTHONPATH=$SRC_1 "$PY" -m pytest -q -p no:cacheprovider tests/test_gh932_safe_ci_failure_locations.py 2>&1 | tail -1)"
# Instrument (throwaway clone only): log each _git call's argv, rc and wall time.
"$PY" -I - scripts/private_ci_check.py <<'EOF'
import sys
p = sys.argv[1]; s = open(p).read()
old = '''        return result.stdout if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None'''
new = '''        import time as _t; open(os.environ["GITLOG"], "a").write(f"rc={result.returncode} {args[:2]}\\n")
        return result.stdout if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired) as _e:
        open(os.environ["GITLOG"], "a").write(f"EXC {type(_e).__name__} {args[:2]}\\n")
        return None'''
assert s.count(old) == 1; open(p, "w").write(s.replace(old, new, 1))
EOF
export GITLOG=$JOBLOCAL/gitlog.txt; : > $GITLOG
echo "instrumented xdist run: $(PYTHONPATH=$SRC_1 "$PY" -m pytest -n $N --dist=loadfile -q -p no:cacheprovider $F 2>&1 | tail -1)"
echo "git calls by outcome:"; sort $GITLOG | uniq -c | sort -rn | head -20
git checkout --quiet -- scripts/private_ci_check.py
echo "-- status of the clone:"; git status --porcelain | head; git config --list --show-origin | grep -v '^file:/global/home/users/ligon/.gitconfig' | head -20
