#!/bin/bash
#SBATCH --account=fc_jevons
#SBATCH --partition=savio4_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=06:00:00
#SBATCH --job-name=backlog-fix
#SBATCH --array=0-6
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_fix_%A/slurm_%a.out
# One element per dispatch-plan step that has no human gate (triage of 2026-09-24).
# Each element: private clone of development, hand-mounted venv image, private
# LSMS_DATA_DIR seeded with its countries' warm parquets, headless claude on a brief.
set -uo pipefail
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
LIVE=/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library
ROOT=/global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_fix_$SLURM_ARRAY_JOB_ID
ELEMENTS=$MIRROR/slurm_logs/backlog_fix_2026-09-24/elements.txt
LINE=$(sed -n "$((SLURM_ARRAY_TASK_ID+1))p" "$ELEMENTS"); KEY=${LINE%% *}; COUNTRIES=${LINE#* }
[ -n "$KEY" ] && [ -f "$MIRROR/slurm_logs/backlog_fix_2026-09-24/briefs/$KEY.md" ] || { echo "NO ELEMENT/BRIEF for task $SLURM_ARRAY_TASK_ID (elements.txt or brief missing on the mirror)"; exit 1; }
STAGE=$ROOT/$KEY; DATA=$STAGE/data; CLONE=/local/job$SLURM_JOB_ID/clone; MNT=/local/job$SLURM_JOB_ID/venv_img
mkdir -p "$STAGE" "$DATA" "$MNT" /local/job$SLURM_JOB_ID/tmp "/local/job$SLURM_JOB_ID/.fuseshim"
echo "element=$KEY countries=$COUNTRIES host=$(hostname) cpus=$SLURM_CPUS_ON_NODE job=$SLURM_JOB_ID start=$(date -Is)"
ln -sf /usr/bin/fusermount "/local/job$SLURM_JOB_ID/.fuseshim/fusermount3"
PATH="/local/job$SLURM_JOB_ID/.fuseshim:$PATH" /usr/libexec/apptainer/bin/squashfuse_ll "$MIRROR/.venv.sqfs" "$MNT" || { echo "MOUNT FAILED"; exit 1; }
git clone --quiet --branch development "$MIRROR" "$CLONE" || { echo "CLONE FAILED"; exit 1; }
git -C "$CLONE" config user.name "Sue the Coder"; git -C "$CLONE" config user.email "coder@sucoder.dev"
ln -sfn "$MNT" "$CLONE/.venv"
ln -sfn "$LIVE/dvc-cache" "$DATA/dvc-cache"
if [ "$COUNTRIES" != "-" ]; then for c in ${COUNTRIES//,/ }; do [ -d "$LIVE/$c" ] && cp -r "$LIVE/$c" "$DATA/$c"; done; fi
export PYTHONPATH="$CLONE" LSMS_DATA_DIR="$DATA" GH_REPO=ligon/LSMS_Library TMPDIR=/local/job$SLURM_JOB_ID/tmp
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
cd "$CLONE"
PYTHONSAFEPATH=1 "$MNT/bin/python" -c "import lsms_library,os,sys; p=os.path.realpath(lsms_library.__file__); sys.exit(0 if p.startswith(os.path.realpath('$CLONE')+os.sep) else 'imported '+p)" || { echo "IDENTITY FAILED"; exit 1; }
echo "clone at $(git rev-parse --short HEAD); identity OK; data root seeded: $(ls $DATA | tr '\n' ' ')"
git switch -c "fix/$KEY" --quiet
export STAGE
BRIEF=$MIRROR/slurm_logs/backlog_fix_2026-09-24/briefs/$KEY.md
cp "$BRIEF" "$STAGE/brief.md"
timeout 5h claude -p "$(cat "$BRIEF")" --output-format json --permission-mode bypassPermissions --max-turns 400 > "$STAGE/claude.json" 2> "$STAGE/claude.err"
RC=$?
echo "claude exit=$RC at $(date -Is)"
"$MNT/bin/python" -c "import json,sys; d=json.load(open('$STAGE/claude.json')); open('$STAGE/REPORT.md','w').write(d.get('result','')); print('turns', d.get('num_turns'), 'cost', d.get('total_cost_usd'), 'reason', d.get('terminal_reason'))" 2>/dev/null || echo "no parsable claude.json"
# Safety net 1: anything the agent left uncommitted (a turn can end with work pending) is committed
# on the element's branch rather than destroyed with /local.  .gitignore still applies.
git status --short > "$STAGE/git_status_before_salvage.txt"
if [ -s "$STAGE/git_status_before_salvage.txt" ]; then
  git add -A && git commit -q -m "WIP salvaged by the harness: left uncommitted when the agent's turn ended ($KEY)

$(cat "$STAGE/git_status_before_salvage.txt")

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" && echo "salvaged uncommitted work into a WIP commit"
fi
# Safety net 2: publish the element's branch even if the agent forgot, and record the tree state.
git status --short > "$STAGE/git_status.txt"; git log --oneline development..HEAD > "$STAGE/commits.txt"
if [ -s "$STAGE/commits.txt" ]; then git push --quiet origin "fix/$KEY" 2>&1 | tail -2; echo "pushed fix/$KEY: $(wc -l < $STAGE/commits.txt) commit(s)"; else echo "no commits on fix/$KEY"; fi
git diff --stat development..HEAD > "$STAGE/diffstat.txt"
echo "done element=$KEY end=$(date -Is)"
