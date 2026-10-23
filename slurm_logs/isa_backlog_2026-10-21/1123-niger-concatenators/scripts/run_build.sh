# census + cold DEFAULT-backend build of every Niger table + focused tests
source "$JOBDIR/scripts/common.sh"
git -C "$CLONE" log --oneline -1 > "$OUT/head.txt"
"$PY" "$S/hash_census.py" "$CLONE" "$OUT/hash.json" > "$OUT/hash.log" 2>&1; echo "census rc=$?"
unset LSMS_BUILD_BACKEND LSMS_NO_CACHE
"$PY" "$S/build_all.py" "$CLONE" "$LABEL" "$OUT/build-default.json" "$JOBDIR/private-$LABEL/default" \
    > "$OUT/build-default.log" 2> "$JOBDIR/private-$LABEL-build-default.err"; echo "build rc=$?"
find "$CLONE/lsms_library/countries" -name '*.parquet' | wc -l > "$OUT/intree_parquets_after_build.txt"
if [ "$LABEL" = base ]; then cp "$S/test_niger_concatenator_refusal.py" "$CLONE/tests/"; fi
"$PY" -m pytest -n "$NCPU" --dist=loadfile -p no:cacheprovider -q -rfE \
    tests/test_niger_concatenator_refusal.py tests/test_ehcvm_plot_inputs_identity.py \
    tests/test_niger_u_sentinel.py tests/test_niger_sold_unit.py tests/test_niger_labor_hours.py \
    tests/test_niger_2014_15_extension_key.py tests/test_niger_value_sentinels.py \
    tests/test_niger_food_label_drift.py tests/test_niger_region_labels.py \
    tests/test_niger_cluster_features_geo.py tests/test_gh1091_niger_community_builder.py \
    tests/test_gh823_niger_s05_community_prices.py \
    > "$OUT/pytest.txt" 2>&1; echo "pytest rc=$?"
tail -3 "$OUT/pytest.txt"
