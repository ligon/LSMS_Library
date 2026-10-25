mkdir -p "$JOBDIR/out-oracle"
for L in base branch; do
  "$PY" "$JOBDIR/scripts/oracle.py" "$JOBDIR/data-$L-make" "$JOBDIR/out-oracle/oracle-$L.json" > "$JOBDIR/out-oracle/oracle-$L.log" 2>&1; echo "oracle $L rc=$?"
done
