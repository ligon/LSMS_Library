#!/bin/bash
#SBATCH -J manquant-probe
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
BASE=f138a3d61c203f892148592675a7ef72e160a322
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $BASE || exit 1
cd "$SRC_1"
export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1
"$PY" "$R/scripts/probe_raw.py" "$OUT/probe_raw_base.json" > "$OUT/probe_raw_base.log" 2>&1
echo "probe exit $?"
"$PY" "$R/scripts/build_tables.py" "$OUT/base-probe" Togo,Senegal,CotedIvoire > "$OUT/build_base_probe.log" 2>&1
echo "build exit $?"
find "$SRC_1/lsms_library/countries" -name '*.parquet' | head
