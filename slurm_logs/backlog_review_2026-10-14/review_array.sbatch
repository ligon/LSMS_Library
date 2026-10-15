#!/bin/bash
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=40
#SBATCH --mem=0
#SBATCH --time=05:00:00
#SBATCH --job-name=backlog-review2
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_review2_%A/slurm_%a.out
# Red-team review, one element per fix branch.  Read-only w.r.t. the branch: no commit, no push.
set -uo pipefail
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
LIVE=/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library
ROOT=/global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_review_$SLURM_ARRAY_JOB_ID
ELEMENTS=$MIRROR/slurm_logs/backlog_review_2026-10-14/elements.txt
LINE=$(sed -n "$((SLURM_ARRAY_TASK_ID+1))p" "$ELEMENTS"); read -r KEY BRANCH COUNTRIES <<< "$LINE"
BRIEF=$MIRROR/slurm_logs/backlog_review_2026-10-14/briefs/$KEY.md
[ -n "${KEY:-}" ] && [ -f "$BRIEF" ] || { echo "NO ELEMENT/BRIEF for task $SLURM_ARRAY_TASK_ID"; exit 1; }
STAGE=$ROOT/$KEY; DATA=$STAGE/data; CLONE=/local/job$SLURM_JOB_ID/clone; MNT=/local/job$SLURM_JOB_ID/venv_img
mkdir -p "$STAGE" "$DATA" "$MNT" /local/job$SLURM_JOB_ID/tmp "/local/job$SLURM_JOB_ID/.fuseshim"
echo "element=$KEY branch=$BRANCH countries=$COUNTRIES host=$(hostname) cpus=$SLURM_CPUS_ON_NODE job=$SLURM_JOB_ID start=$(date -Is)"
ln -sf /usr/bin/fusermount "/local/job$SLURM_JOB_ID/.fuseshim/fusermount3"
PATH="/local/job$SLURM_JOB_ID/.fuseshim:$PATH" /usr/libexec/apptainer/bin/squashfuse_ll "$MIRROR/.venv.sqfs" "$MNT" || { echo "MOUNT FAILED"; exit 1; }
git clone --quiet --branch "$BRANCH" "$MIRROR" "$CLONE" || { echo "CLONE FAILED"; exit 1; }
git -C "$CLONE" fetch --quiet origin development
ln -sfn "$MNT" "$CLONE/.venv"
ln -sfn "$LIVE/dvc-cache" "$DATA/dvc-cache"
if [ "$COUNTRIES" != "-" ]; then for c in ${COUNTRIES//,/ }; do [ -d "$LIVE/$c" ] && cp -r "$LIVE/$c" "$DATA/$c"; done; fi
export PYTHONPATH="$CLONE" LSMS_DATA_DIR="$DATA" GH_REPO=ligon/LSMS_Library TMPDIR=/local/job$SLURM_JOB_ID/tmp STAGE
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
cd "$CLONE"
PYTHONSAFEPATH=1 "$MNT/bin/python" -c "import lsms_library,os,sys; p=os.path.realpath(lsms_library.__file__); sys.exit(0 if p.startswith(os.path.realpath('$CLONE')+os.sep) else 'imported '+p)" || { echo "IDENTITY FAILED"; exit 1; }
echo "clone at $(git rev-parse --short HEAD) on $BRANCH; identity OK; seeded: $(ls $DATA | tr '\n' ' ')"
cp "$BRIEF" "$STAGE/brief.md"
timeout 4h claude -p "$(cat "$BRIEF")" --output-format json --permission-mode bypassPermissions --max-turns 400 > "$STAGE/claude.json" 2> "$STAGE/claude.err"
echo "claude exit=$? at $(date -Is)"
"$MNT/bin/python" -c "import json; d=json.load(open('$STAGE/claude.json')); open('$STAGE/final_message.md','w').write(d.get('result','')); print('turns', d.get('num_turns'), 'cost', d.get('total_cost_usd'), 'reason', d.get('terminal_reason'))" 2>/dev/null || echo "no parsable claude.json"
[ -f "$STAGE/VERDICT.md" ] && echo "verdict: $(head -1 $STAGE/VERDICT.md)" || { echo "NO VERDICT.md; final message head: $(head -c 200 $STAGE/final_message.md 2>/dev/null)"; }
git status --short > "$STAGE/git_status.txt"; git log --oneline -1 > "$STAGE/head.txt"
echo "done element=$KEY end=$(date -Is)"
