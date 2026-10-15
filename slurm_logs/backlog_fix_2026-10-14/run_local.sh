#!/bin/bash
# Local runner: one element of the follow-up array on THIS node (no Slurm array).
# Usage: run_local.sh <task-index> <cores>   -- same body as fix_array.sbatch.
set -uo pipefail
IDX=$1; CORES=${2:-6}
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
LIVE=/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library
ROOT=/global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_fix_local_$SLURM_JOB_ID
ELEMENTS=$MIRROR/slurm_logs/backlog_fix_2026-10-14/elements.txt
LINE=$(sed -n "$((IDX+1))p" "$ELEMENTS"); read -r KEY COUNTRIES BASE <<< "$LINE"; BASE=${BASE:-development}
[ -n "$KEY" ] && [ -f "$MIRROR/slurm_logs/backlog_fix_2026-10-14/briefs/$KEY.md" ] || { echo "NO ELEMENT/BRIEF for $IDX"; exit 1; }
STAGE=$ROOT/$KEY; DATA=$STAGE/data; EL=/local/job$SLURM_JOB_ID/elements/$KEY; CLONE=$EL/clone; MNT=/local/job$SLURM_JOB_ID/venv_img
mkdir -p "$STAGE" "$DATA" "$EL/tmp"
echo "element=$KEY countries=$COUNTRIES base=$BASE host=$(hostname) cores=$CORES start=$(date -Is)"
git clone --quiet --branch "$BASE" "$MIRROR" "$CLONE" || { echo "CLONE FAILED"; exit 1; }
git -C "$CLONE" config user.name "Sue the Coder"; git -C "$CLONE" config user.email "coder@sucoder.dev"
ln -sfn "$MNT" "$CLONE/.venv"
ln -sfn "$LIVE/dvc-cache" "$DATA/dvc-cache"
if [ "$COUNTRIES" != "-" ]; then for c in ${COUNTRIES//,/ }; do [ -d "$LIVE/$c" ] && cp -r "$LIVE/$c" "$DATA/$c"; done; fi
export PYTHONPATH="$CLONE" LSMS_DATA_DIR="$DATA" GH_REPO=ligon/LSMS_Library TMPDIR=$EL/tmp
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
export SLURM_CPUS_ON_NODE=$CORES   # the briefs size pytest -n to this; six agents share one 40-core node
unset PYTHONSAFEPATH
cd "$CLONE"
PYTHONSAFEPATH=1 "$MNT/bin/python" -c "import lsms_library,os,sys; p=os.path.realpath(lsms_library.__file__); sys.exit(0 if p.startswith(os.path.realpath('$CLONE')+os.sep) else 'imported '+p)" || { echo "IDENTITY FAILED"; exit 1; }
echo "clone at $(git rev-parse --short HEAD); identity OK; data root seeded: $(ls $DATA | tr '\n' ' ')"
git switch -c "fix/$KEY" --quiet
export STAGE
BRIEF=$MIRROR/slurm_logs/backlog_fix_2026-10-14/briefs/$KEY.md
cp "$BRIEF" "$STAGE/brief.md"
timeout 5h claude -p "$(cat "$BRIEF")" --output-format json --permission-mode bypassPermissions --max-turns 400 > "$STAGE/claude.json" 2> "$STAGE/claude.err"
RC=$?
echo "claude exit=$RC at $(date -Is)"
"$MNT/bin/python" -c "import json,sys; d=json.load(open('$STAGE/claude.json')); open('$STAGE/REPORT.md','w').write(d.get('result','')); print('turns', d.get('num_turns'), 'cost', d.get('total_cost_usd'), 'reason', d.get('terminal_reason'))" 2>/dev/null || echo "no parsable claude.json"
git status --short > "$STAGE/git_status_before_salvage.txt"
if [ -s "$STAGE/git_status_before_salvage.txt" ]; then
  git add -A && git commit -q -m "WIP salvaged by the harness: left uncommitted when the agent's turn ended ($KEY)

$(cat "$STAGE/git_status_before_salvage.txt")

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" && echo "salvaged uncommitted work into a WIP commit"
fi
git status --short > "$STAGE/git_status.txt"; git log --oneline "$BASE"..HEAD > "$STAGE/commits.txt"
if [ -s "$STAGE/commits.txt" ]; then git push --quiet origin "fix/$KEY" 2>&1 | tail -2; echo "pushed fix/$KEY: $(wc -l < $STAGE/commits.txt) commit(s)"; else echo "no commits on fix/$KEY"; fi
git diff --stat "$BASE"..HEAD > "$STAGE/diffstat.txt"
echo "done element=$KEY end=$(date -Is)"
