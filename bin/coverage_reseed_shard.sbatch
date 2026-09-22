#!/bin/bash
#SBATCH --job-name=cov-reseed-shard
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=01:30:00
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%A_%a.out
#SBATCH --error=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%A_%a.err
#
# One country of the MONTHLY authoritative reseed: clear that country's cache,
# then COLD-build from the clone's config -> a shard CSV. Submitted as an array
# by bin/coverage_reseed.sbatch. Per-task node-local venv (tar-pipe) because the
# squashfuse mount races under array concurrency (see the cube run, 2026-06-25).
set -u
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
WD="${COV_WORKDIR:?set COV_WORKDIR}"
SHARDS="${COV_SHARDROOT:?set COV_SHARDROOT}/shards"

mapfile -t COUNTRIES < "${COV_SHARDROOT}/countries.txt"
C="${COUNTRIES[$SLURM_ARRAY_TASK_ID]}"
echo "=== reseed shard: '$C'  node=$(hostname)  $(date) ==="

LV="/local/job${SLURM_JOB_ID}/venv"
mkdir -p "$LV"
tar -C "$REPO/.venv.lustre" -cf - . 2>/dev/null | tar -C "$LV" -xf - 2>/dev/null
PY="$LV/bin/python"
# The venv carries no editable install of this project (GH #957), so the import
# must be told which checkout to use.  Without this the bare import below fails
# outright -- it used to pass only via the .pth, which named the MIRROR, so this
# job graded the clone's config against the mirror's library code.
# PYTHONSAFEPATH makes PYTHONPATH authoritative: without it `python -m` and
# `python <script>.py` put cwd / the script dir at sys.path[0], so the
# `-m lsms_library.cli cache clear` below loaded the MIRROR's library while
# the build two lines later loaded the clone's.
export PYTHONSAFEPATH=1 PYTHONPATH="$WD"
LSMS_COV_CODE_ROOT="$WD" "$PY" -c '
import lsms_library, yaml, pandas, os, sys
want = os.path.realpath(os.environ["LSMS_COV_CODE_ROOT"]).rstrip(os.sep) + os.sep
got = os.path.realpath(lsms_library.__file__)
if not got.startswith(want):
    sys.exit("imported %s, expected under %s" % (got, want))
' || { echo "venv provisioning FAILED"; exit 1; }

echo "--- cache clear '$C' (authoritative) ---"
# Same LSMS_COUNTRIES_ROOT as the build below: without it this line resolved
# countries_root() from a different tree than the build it is clearing for.
LSMS_COUNTRIES_ROOT="$WD/lsms_library/countries" \
    "$PY" -m lsms_library.cli cache clear --country "$C" || echo "  (clear noop/failed)"
echo "--- cold build '$C' ---"
LSMS_COUNTRIES_ROOT="$WD/lsms_library/countries" \
    "$PY" "$WD/bench/matrix.py" --countries "$C" \
          --snapshot "$SHARDS/$C.csv" --no-html
echo "=== done '$C' rc=$? $(date) ==="
