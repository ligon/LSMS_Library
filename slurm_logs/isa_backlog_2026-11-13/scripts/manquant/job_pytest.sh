#!/bin/bash
#SBATCH -J manquant-pytest
# Usage: job_pytest.sh <sha> <label> <test files...>
set -uo pipefail
SHA=${1:?sha}; LABEL=${2:?label}; shift 2
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $SHA || exit 1
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider -q -rfEs "$@" > "$OUT/pytest-$LABEL.txt" 2>&1
echo "pytest exit $?" | tee -a "$OUT/pytest-$LABEL.txt"
find "$SRC_1/lsms_library/countries" -name '*.parquet' | head
