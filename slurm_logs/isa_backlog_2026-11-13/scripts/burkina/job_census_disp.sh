#!/bin/bash
# Dispositions census (by code) (burkina stream).  Usage: sbatch ... job_census.sh <base-sha>
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina
source $B/tools/burst-env.sh burkina "$1" || exit 9
cd "$SRC_1"
echo "python is: $(command -v python)  PY=$PY"
export PATH="$(dirname "$PY"):$PATH"
echo "python now: $(command -v python)"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/census_disp.py $OUT/census_disp.json
echo "census exit $?"
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"
