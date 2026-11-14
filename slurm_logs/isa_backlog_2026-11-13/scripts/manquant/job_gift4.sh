#!/bin/bash
#SBATCH -J manquant-gift4
# Usage: job_gift4.sh <cand-sha> <prev-sha>
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
T=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools
CAND=${1:?cand}; PREV=${2:?prev}
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $CAND $PREV || exit 1
S=${CAND:0:12}; P=${PREV:0:12}
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$R/scripts/build_tables.py" "$OUT/gift4-cand-$S" Senegal,CotedIvoire,Niger ) > "$OUT/build_gift4_cand_$S.log" 2>&1
echo "build cand exit $?"
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" "$R/scripts/build_tables.py" "$OUT/gift4-prev-$P" Senegal,CotedIvoire,Niger ) > "$OUT/build_gift4_prev_$P.log" 2>&1
echo "build prev exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" "$R/scripts/compare_gift.py" "$OUT/gift4-prev-$P" "$OUT/gift4-cand-$S" "$OUT/compare_gift4_$S.json" ) > "$OUT/compare_gift4_$S.log" 2>&1
echo "compare exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $T/hash_census.py $OUT/census-cand-$S.json ) > "$OUT/census_cand_$S.log" 2>&1
"$PY" $T/hashdiff.py $OUT/census-base.json $OUT/census-cand-$S.json $OUT/hashdiff-$S.json > "$OUT/hashdiff_$S.txt" 2>&1
"$PY" $T/hashdiff.py $OUT/census-cand-$P.json $OUT/census-cand-$S.json $OUT/hashdiff-$P-to-$S.json > "$OUT/hashdiff_${P}_to_$S.txt" 2>&1
echo "census exit $?"; cat "$OUT/hashdiff_$S.txt" "$OUT/hashdiff_${P}_to_$S.txt"
cd "$SRC_1"
NIGER_TESTS=$(ls tests/test_niger*.py tests/test_867_nitrogen_content.py tests/test_gh1288_niger_plot_input_source_literal.py tests/test_gh866_stale_country_parquet_not_laundered.py tests/test_gh1469_farm_level_occurrence.py 2>/dev/null | tr '\n' ' ')
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider -q -rfEs \
  tests/test_ehcvm_plot_inputs_identity.py tests/test_senegal_1483_s16b.py tests/test_gh1500_senegal_missing_food_unit.py \
  tests/test_senegal_seed_crop_labels.py tests/test_senegal_plot_concatenator_refusal.py tests/test_cotedivoire_plot_inputs_unit_sentinel.py \
  tests/test_civ_2021_wave.py tests/test_togo_2018_plot_inputs_acquisition.py tests/test_962_unit_sentinel.py \
  tests/test_schema_consistency.py tests/test_build_transform_hash.py tests/test_derivations.py tests/test_feature_canonical_index.py \
  $NIGER_TESTS > "$OUT/pytest-gift4-$S.txt" 2>&1
echo "pytest exit $?"; grep -E "^(FAILED|ERROR)" "$OUT/pytest-gift4-$S.txt" | head -20; tail -2 "$OUT/pytest-gift4-$S.txt"
echo "in-tree parquets:"; find "$SRC_1/lsms_library/countries" "$SRC_2/lsms_library/countries" -name '*.parquet' | head
echo end
