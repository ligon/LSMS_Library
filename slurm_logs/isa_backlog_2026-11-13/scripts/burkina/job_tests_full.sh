#!/bin/bash
# Whole suite on the candidate, Makefile recipe.  Usage: job_tests_full.sh <sha> <tag>
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina
source $B/tools/burst-env.sh burkina "$1" || exit 9
TAG=${2:-full}
export PATH="$(dirname "$PY"):$PATH"
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -p no:cacheprovider -rfE tests > $OUT/pytest-$TAG.txt 2>&1
echo "pytest exit $?"; tail -3 $OUT/pytest-$TAG.txt
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"
