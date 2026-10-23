#!/bin/bash
# Executed forms. Use NEW physical roots and exact root-owned metadata links.
# Existing evidence must not be overwritten; prepare.py refuses populated roots.
set -euC
P=/local/job40255479/control/isa-backlog-20261021/review-1058-Burkina_Faso
bash "$P/run.sh" candidate "$P/protect.py" before > "$P/protect-before-extant.log" 2>&1
bash "$P/run.sh" candidate "$P/prepare.py" > "$P/preparation-tier-correct.log" 2>&1
# Successful complete preparation gated every API probe.
bash "$P/run.sh" baseline "$P/build_verify.py" > "$P/baseline-cold.log" 2>&1
bash "$P/run.sh" baseline "$P/build_verify.py" warm > "$P/baseline-warm.log" 2>&1
bash "$P/run.sh" candidate "$P/build_verify.py" > "$P/candidate-cold.log" 2>&1
bash "$P/run.sh" candidate "$P/build_verify.py" warm > "$P/candidate-warm.log" 2>&1
if bash "$P/run.sh" baseline -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider --basetemp="$P/pytest-baseline" --junitxml="$P/baseline-tests.xml" "$P/test_burkina_faso_plot_gps.py" > "$P/baseline-tests.log" 2>&1; then
  exit 1
else
  review_baseline_status=$?
  [ "$review_baseline_status" -eq 1 ] || exit "$review_baseline_status"
fi
rg -q '^12 failed, 3 passed' "$P/baseline-tests.log"
bash "$P/run.sh" candidate -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider --basetemp="$P/pytest-candidate" --junitxml="$P/candidate-tests.xml" tests/test_burkina_faso_plot_gps.py tests/test_plot_id_canonical.py tests/test_burkina_faso_cluster_gps.py::test_2018_19_geo_subdf_maps_the_coordonnes_gps_columns tests/test_burkina_faso_cluster_gps.py::test_2014_declares_no_geo_subdf tests/test_burkina_faso_cluster_gps.py::test_no_wave_asks_for_lat_dd_mod tests/test_burkina_faso_cluster_gps.py::test_optional_matches_the_waves_that_lack_gps "$P/test_independent_edges.py" > "$P/candidate-tests.log" 2>&1
bash "$P/run.sh" baseline "$P/hashes.py" > "$P/baseline-hashes.log" 2>&1
bash "$P/run.sh" baseline bench/feature_audit/scan.py --features plot_features --countries Burkina_Faso Senegal --phases 1,2 --jobs 1 --build-timeout 120 --out "$P/baseline-findings-private.jsonl" > "$P/baseline-scanner.log" 2>&1
bash "$P/run.sh" candidate "$P/source_oracle.py" > "$P/source-oracle.log" 2>&1
bash "$P/run.sh" candidate "$P/boundary.py" > "$P/boundary.log" 2>&1
bash "$P/run.sh" candidate "$P/hashes.py" > "$P/candidate-hashes.log" 2>&1
bash "$P/run.sh" candidate bench/feature_audit/scan.py --features plot_features --countries Burkina_Faso Senegal --phases 1,2 --jobs 1 --build-timeout 120 --out "$P/candidate-findings-private.jsonl" > "$P/candidate-scanner.log" 2>&1
bash "$P/run.sh" candidate scripts/api_surface_audit.py > "$P/api-audit.log" 2>&1
bash "$P/run.sh" candidate "$P/finish_checks.py" > "$P/finish-checks.log" 2>&1
bash "$P/run.sh" candidate "$P/protect.py" after > "$P/protect-after.log" 2>&1
bash "$P/run.sh" candidate "$P/release_review.py" > "$P/release-review.log" 2>&1
