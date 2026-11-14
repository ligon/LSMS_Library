# Sourced after scf_env.sh.  run_tests <arm> <label>: the targeted selection from THAT tree.
run_tests() {
  local arm=$1 label=$2 C=$J/$1
  local SEL
  SEL=$(git -C "$C" grep -l -P "\b(Mali|Senegal|Niger|Uganda)\b|ehcvm" -- 'tests/test_*.py' | tr '\n' ' ')
  for f in tests/test_derivations.py tests/test_build_transform_hash.py tests/test_no_build_input_in_contents_org.py \
           tests/test_failure_is_not_absence.py tests/test_schema_consistency.py \
           tests/test_categorical_mapping_resolution.py tests/test_gh1578_derivation_duplicates.py \
           tests/test_gh1707_served_corrections.py; do
    [ -f "$C/$f" ] && SEL="$SEL $f"
  done
  echo "$SEL" | tr ' ' '\n' | grep . | sort -u > "$OUT/results/pytest_${label}_selection.txt"
  echo "selected files ($label): $(wc -l < "$OUT/results/pytest_${label}_selection.txt")"
  mkdir -p "$PRIV/pytest"; chmod 700 "$PRIV/pytest"
  run_in "$arm" "tests-$label" timeout -s INT 6600 "$PY" -m pytest -n 8 --dist=loadfile -p no:cacheprovider -q -rfEs \
      $(cat "$OUT/results/pytest_${label}_selection.txt") > "$PRIV/pytest/pytest_${label}.txt" 2>&1
  echo "pytest $label rc=$?"
  # aggregates only into results: the summary line and failing node ids
  { tail -1 "$PRIV/pytest/pytest_${label}.txt"; grep -E '^(FAILED|ERROR) ' "$PRIV/pytest/pytest_${label}.txt" | cut -d' ' -f2 | sort -u; } \
      > "$OUT/results/pytest_${label}_summary.txt"
  cat "$OUT/results/pytest_${label}_summary.txt"
}
