#!/bin/bash
#SBATCH -J manquant-final
# Usage: job_final.sh <cand-sha>: census + Togo rebuild at the final head.
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
T=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools
CAND=${1:?cand sha}
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $CAND || exit 1
S=${CAND:0:12}
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $T/hash_census.py $OUT/census-cand-$S.json ) > "$OUT/census_cand_$S.log" 2>&1
echo "census exit $?"
"$PY" $T/hashdiff.py $OUT/census-base.json $OUT/census-cand-$S.json $OUT/hashdiff-$S.json > "$OUT/hashdiff_$S.txt" 2>&1
"$PY" $T/hashdiff.py $OUT/census-cand-f001cfbd0aac.json $OUT/census-cand-$S.json $OUT/hashdiff-f001-to-$S.json > "$OUT/hashdiff_f001_to_$S.txt" 2>&1
echo "hashdiff exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$R/scripts/build_tables.py" "$OUT/cand-$S" Togo ) > "$OUT/build_cand_$S.log" 2>&1
echo "build exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" -c "
import pandas as pd
a=pd.read_parquet('$OUT/cand-f001cfbd0aac/Togo__plot_inputs.parquet'); b=pd.read_parquet('$OUT/cand-$S/Togo__plot_inputs.parquet')
pd.testing.assert_frame_equal(a,b); print('Togo plot_inputs f001cfbd0 == $S')
" ) >> "$OUT/build_cand_$S.log" 2>&1
echo "eq exit $?"
