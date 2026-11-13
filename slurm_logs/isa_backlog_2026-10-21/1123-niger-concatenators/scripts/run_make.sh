# cold MAKE-backend build of the five concatenated tables (fresh DATA)
source "$JOBDIR/scripts/common.sh"
unset LSMS_NO_CACHE
export LSMS_BUILD_BACKEND=make
"$PY" "$S/build_all.py" "$CLONE" "$LABEL" "$OUT/build-make.json" "$JOBDIR/private-$LABEL/make" \
    plot_inputs,crop_production,livestock,plot_labor,people_last7days \
    > "$OUT/build-make.log" 2> "$JOBDIR/private-$LABEL-build-make.err"; echo "build rc=$?"
find "$CLONE/lsms_library/countries" -name '*.parquet' | wc -l > "$OUT/intree_parquets_after_make.txt"
