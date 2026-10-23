#!/bin/bash
# Executed command forms, pinned to immutable source heads.
# Preparation is one-shot and refuses existing input files. Use a NEW private
# evidence root and approved metadata target for a new review; never overwrite
# these frozen results or run a historic writer launcher.
set -eu
review_root=/local/job40255479/control/isa-backlog-20261021/integration/burkina-fifteen-review
bash "$review_root/run.sh" candidate "$review_root/protect.py" before
bash "$review_root/run.sh" candidate "$review_root/prepare.py"
bash "$review_root/run.sh" candidate "$review_root/boundary.py"
bash "$review_root/run.sh" baseline "$review_root/build_verify.py"
bash "$review_root/run.sh" candidate "$review_root/build_verify.py"
bash "$review_root/run.sh" baseline "$review_root/build_verify.py" warm
bash "$review_root/run.sh" candidate "$review_root/build_verify.py" warm
bash "$review_root/run.sh" candidate "$review_root/source_oracle.py"
bash "$review_root/run.sh" candidate "$review_root/source_inputs.py"
bash "$review_root/run.sh" candidate "$review_root/provenance.py"
for phase in baseline candidate; do
 bash "$review_root/run.sh" "$phase" bench/feature_audit/scan.py --features plot_features --countries Burkina_Faso Senegal --phases 1,2 --jobs 1 --build-timeout 120 --out "$review_root/$phase-GPS-findings-private.jsonl"
 bash "$review_root/run.sh" "$phase" bench/feature_audit/scan.py --features plot_inputs --countries Burkina_Faso Benin Togo --phases 1,2 --jobs 1 --build-timeout 120 --out "$review_root/$phase-input-findings-private.jsonl"
done
bash "$review_root/run.sh" candidate -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider --basetemp="$review_root/pytest-GPS" --junitxml="$review_root/candidate-GPS-tests-private.xml" tests/test_burkina_faso_plot_gps.py tests/test_plot_id_canonical.py tests/test_burkina_faso_cluster_gps.py::test_2018_19_geo_subdf_maps_the_coordonnes_gps_columns tests/test_burkina_faso_cluster_gps.py::test_2014_declares_no_geo_subdf tests/test_burkina_faso_cluster_gps.py::test_no_wave_asks_for_lat_dd_mod tests/test_burkina_faso_cluster_gps.py::test_optional_matches_the_waves_that_lack_gps "$review_root/test_independent_edges.py"
bash "$review_root/run.sh" candidate -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider --basetemp="$review_root/pytest-inputs" --junitxml="$review_root/candidate-input-tests-private.xml" tests/test_burkina_faso_plot_inputs_identity.py tests/test_gh323_benin_togo.py -k 'not recovers_the_discarded_line_item'
# Expected negative control exit 1: 12 fail, 3 pass. All other commands exit 0.
set +e
bash "$review_root/run.sh" baseline -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider --basetemp="$review_root/pytest-baseline" --junitxml="$review_root/baseline-GPS-tests-private.xml" "$review_root/test_burkina_faso_plot_gps.py"
negative_status=$?
set -e
test "$negative_status" -eq 1
bash "$review_root/run.sh" candidate -m pytest --collect-only -q -p no:cacheprovider tests/test_burkina_faso_plot_inputs_identity.py tests/test_gh323_benin_togo.py -k 'not recovers_the_discarded_line_item'
bash "$review_root/run.sh" candidate scripts/api_surface_audit.py
bash "$review_root/run.sh" baseline "$review_root/cache_metadata.py"
bash "$review_root/run.sh" candidate "$review_root/cache_metadata.py"
bash "$review_root/run.sh" candidate "$review_root/finish_checks.py"
bash "$review_root/run.sh" candidate "$review_root/protect.py" after
# After every source/API/test/scanner consumer has exited:
/local/job40255479/venv_img/bin/python "$review_root/release_review.py"
