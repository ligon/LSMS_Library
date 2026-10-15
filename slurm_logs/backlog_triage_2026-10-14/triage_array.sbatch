#!/bin/bash
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=40
#SBATCH --mem=0
#SBATCH --time=04:00:00
#SBATCH --job-name=backlog-triage2
#SBATCH --array=0-5
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_triage_%A/slurm_%a.out
# Read-only triage of the 74 open issues the 2026-09-24 triage did not cover (6 groups, one whole node each).
set -uo pipefail
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
LIVE=/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library
ROOT=/global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_triage_$SLURM_ARRAY_JOB_ID
TD=$MIRROR/slurm_logs/backlog_triage_2026-10-14
KEY=$(sed -n "$((SLURM_ARRAY_TASK_ID+1))p" "$TD/elements.txt")
[ -n "$KEY" ] && [ -f "$TD/briefs/$KEY.md" ] || { echo "NO ELEMENT/BRIEF for task $SLURM_ARRAY_TASK_ID"; exit 1; }
STAGE=$ROOT/$KEY; DATA=$STAGE/data; CLONE=/local/job$SLURM_JOB_ID/clone; MNT=/local/job$SLURM_JOB_ID/venv_img
mkdir -p "$STAGE" "$DATA" "$MNT" /local/job$SLURM_JOB_ID/tmp "/local/job$SLURM_JOB_ID/.fuseshim"
cp "$TD/merged_prs_since_0918.tsv" "$TD/open_issues.tsv" "$ROOT/" 2>/dev/null
echo "element=$KEY host=$(hostname) cpus=$SLURM_CPUS_ON_NODE job=$SLURM_JOB_ID start=$(date -Is)"
ln -sf /usr/bin/fusermount "/local/job$SLURM_JOB_ID/.fuseshim/fusermount3"
PATH="/local/job$SLURM_JOB_ID/.fuseshim:$PATH" /usr/libexec/apptainer/bin/squashfuse_ll "$MIRROR/.venv.sqfs" "$MNT" || { echo "MOUNT FAILED"; exit 1; }
git clone --quiet --branch development "$MIRROR" "$CLONE" || { echo "CLONE FAILED"; exit 1; }
ln -sfn "$MNT" "$CLONE/.venv"; ln -sfn "$LIVE/dvc-cache" "$DATA/dvc-cache"
export PYTHONPATH="$CLONE" LSMS_DATA_DIR="$DATA" GH_REPO=ligon/LSMS_Library TMPDIR=/local/job$SLURM_JOB_ID/tmp STAGE
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
cd "$CLONE"
PYTHONSAFEPATH=1 "$MNT/bin/python" -c "import lsms_library,os,sys; p=os.path.realpath(lsms_library.__file__); sys.exit(0 if p.startswith(os.path.realpath('$CLONE')+os.sep) else 'imported '+p)" || { echo "IDENTITY FAILED"; exit 1; }
echo "clone at $(git rev-parse --short HEAD); identity OK"
cp "$TD/briefs/$KEY.md" "$STAGE/brief.md"
timeout 3h claude -p "$(cat "$STAGE/brief.md")" --output-format json --permission-mode bypassPermissions --max-turns 300 > "$STAGE/claude.json" 2> "$STAGE/claude.err"
echo "claude exit=$? at $(date -Is)"
"$MNT/bin/python" -c "import json; d=json.load(open('$STAGE/claude.json')); open('$STAGE/REPORT_final_message.md','w').write(d.get('result','')); print('turns', d.get('num_turns'), 'cost', d.get('total_cost_usd'), 'reason', d.get('terminal_reason'))" 2>/dev/null || echo "no parsable claude.json"
git status --short > "$STAGE/git_status.txt"; [ -s "$STAGE/git_status.txt" ] && echo "WARNING: read-only agent left tree changes (not published)"
ls "$STAGE"; echo "done element=$KEY end=$(date -Is)"
