# Sourced by every integration sbatch script.  Wraps the batch's burst-env.sh
# (private roots BEFORE any import, clones at full SHAs, venv mount, identity).
#   usage (in a job):  LABEL=<label>; source .../common.sh <sha1> [<sha2> ...]
# After it: SRC_i / DATA_i per SHA, PY, JOUT (this job's own output dir).
set -uo pipefail
case "$(hostname -s)" in n0035|n0034|n0046) echo "refusing to run on $(hostname -s)"; exit 4;; esac
RD=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/integration
S=$RD/scripts
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
BASE_SHA=c79f543da18f0d888e4e7998102db81feed798c9
source $BATCH/tools/burst-env.sh integration "$@" || { echo "burst-env failed rc=$?"; exit 5; }
JOUT=$RD/out/${LABEL:?LABEL}-$SLURM_JOB_ID
mkdir -p $JOUT
unset LSMS_ALLOW_INTREE_SOURCES LSMS_BUILD_BACKEND LSMS_COVERAGE_SNAPSHOT PYTHONPATH || true
# Make recipes and subprocesses call bare `python`: put the venv first on PATH.
export PATH=$(dirname $PY):$PATH
echo "python on PATH: $(command -v python)"
echo "host $(hostname -s) job $SLURM_JOB_ID label $LABEL start $(date -Is) shas $*"; uptime
[ -r "$HOME/.config/lsms_library/config.yml" ] && echo "config readable" || echo "CONFIG NOT READABLE"

hygiene() {  # hygiene REPO
    echo "in-tree parquet $1: $(find $1/lsms_library/countries -name '*.parquet' | wc -l)"
    echo "git status $1: $(git -C $1 status --porcelain | grep -v -e '^?? .venv' | wc -l) lines"
}
