mkdir -p "$JOBDIR/out-compare"
"$PY" "$JOBDIR/scripts/compare.py" "$JOBDIR" "$JOBDIR/out-compare/compare.json" > "$JOBDIR/out-compare/compare.log" 2>&1; echo "compare rc=$?"
