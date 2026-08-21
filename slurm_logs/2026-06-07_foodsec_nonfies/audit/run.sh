set -u
D=slurm_logs/2026-06-07_foodsec_nonfies/audit
run_one() {
  local c="$1"; local safe=$(echo "$c" | tr ' /' '__')
  timeout 1800 .venv/bin/python "$D/../audit_one_country.py" "$c" "$D/$safe.json" \
     > "$D/$safe.log" 2>&1 || echo "TIMEOUT/ERR: $c (exit $?)" >> "$D/_driver.log"
}
export -f run_one; export D
tr '\0' '\n' < "$D/countries.nul" | xargs -d '\n' -P 14 -I{} bash -c 'run_one "$@"' _ {}
echo "ALL DONE $(date +%H:%M:%S)" >> "$D/_driver.log"
