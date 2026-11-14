#!/bin/bash
#SBATCH -J manquant-f2
# Usage: job_f2.sh <cand-sha>: census at the head + the tests that pin the edited Senegal files.
set -uo pipefail
T=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools
CAND=${1:?cand sha}
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $CAND || exit 1
S=${CAND:0:12}
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $T/hash_census.py $OUT/census-cand-$S.json ) > "$OUT/census_cand_$S.log" 2>&1
"$PY" $T/hashdiff.py $OUT/census-base.json $OUT/census-cand-$S.json $OUT/hashdiff-$S.json > "$OUT/hashdiff_$S.txt" 2>&1
"$PY" $T/hashdiff.py $OUT/census-cand-fc40960d27b8.json $OUT/census-cand-$S.json $OUT/hashdiff-fc40960-to-$S.json > "$OUT/hashdiff_fc40960_to_$S.txt" 2>&1
echo "census exit $?"; cat "$OUT/hashdiff_$S.txt" "$OUT/hashdiff_fc40960_to_$S.txt"
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider -q -rfEs \
  tests/test_gh1500_senegal_missing_food_unit.py tests/test_senegal_1483_s16b.py \
  tests/test_ehcvm_plot_inputs_identity.py tests/test_senegal_seed_crop_labels.py \
  tests/test_schema_consistency.py > "$OUT/pytest-f2-$S.txt" 2>&1
echo "pytest exit $?"; tail -3 "$OUT/pytest-f2-$S.txt"
echo "in-tree parquets:"; find "$SRC_1/lsms_library/countries" -name '*.parquet' | head
echo end
