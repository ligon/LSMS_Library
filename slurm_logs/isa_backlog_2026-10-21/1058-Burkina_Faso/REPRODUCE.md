# Reproduction and independent-review commands

Use the mounted image and a fresh physical private data root. Never copy
the baseline's Burkina derived parquets into a cold candidate. Preparation
must verify all 15 held L1 MD5/SHA receipts and six fresh Senegal companion
parquets before dependent probes. The writer's held prerequisites, full
frames and logs remain private; the scripts below are evidence helpers,
not production patches. They assert checkout/config identity and enforce
offline/write guards. Their SHA receipts are in validation.json.

The immutable author baseline is the ledger-only commit
`b5ced85c571bc4d5b430371d1f92ace336c24a3f`; its production/config is exact
`f8d236694c26fe9e709c5548f02c70f74ea72c33`. The runtime candidate is
`7eee549b15cacbe1e86b60a925b0899dc85d49e6`.

These are the executed command forms, from the assigned writer. Baseline
commands ran before runtime edits, with the new test file present. The
baseline pytest exit is intentionally nonzero (12 failures / three passes).
Do not rerun preparation against an existing evidence root or switch a
frozen checkout. An independent review must create its own physical copies
and guard metadata target.

```bash
cd /local/job40255479/worktrees/isa-Burkina_Faso-1058-fix
task_evidence=/local/job40255479/control/isa-backlog-20261021/Burkina_Faso/1058-fix

# Baseline phase, executed before runtime edits.
bash "$task_evidence/run.sh" "$task_evidence/prepare.py" > "$task_evidence/preparation-private.log" 2>&1
bash "$task_evidence/run.sh" "$task_evidence/build_verify.py" > "$task_evidence/baseline-cold-private.log" 2>&1
bash "$task_evidence/run.sh" "$task_evidence/build_verify.py" warm > "$task_evidence/baseline-warm-private.log" 2>&1
bash "$task_evidence/run.sh" -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider --junitxml="$task_evidence/baseline-tests-private.xml" tests/test_burkina_faso_plot_gps.py > "$task_evidence/baseline-tests-private.log" 2>&1
bash "$task_evidence/run.sh" "$task_evidence/hashes.py" > "$task_evidence/baseline-hashes-private.log" 2>&1
bash "$task_evidence/run.sh" bench/feature_audit/scan.py --features plot_features --countries Burkina_Faso Senegal --phases 1,2 --jobs 1 --build-timeout 120 --out "$task_evidence/baseline-findings-private.jsonl" > "$task_evidence/baseline-scanner-private.log" 2>&1

# Final candidate: this new root starts with no Burkina L2 parquets.
export TASK_PHASE=candidate
export TASK_DATA_ROOT="$task_evidence/data-candidate-verified"
bash "$task_evidence/run.sh" "$task_evidence/prepare_verified_candidate.py" > "$task_evidence/preparation-verified-candidate-private.log" 2>&1
bash "$task_evidence/run.sh" "$task_evidence/build_verify.py" > "$task_evidence/candidate-cold-private.log" 2>&1
bash "$task_evidence/run.sh" "$task_evidence/build_verify.py" warm > "$task_evidence/candidate-warm-private.log" 2>&1
bash "$task_evidence/run.sh" -m pytest -n 1 --dist=loadfile -q -p no:cacheprovider --junitxml="$task_evidence/candidate-tests-private.xml" tests/test_burkina_faso_plot_gps.py tests/test_plot_id_canonical.py tests/test_burkina_faso_cluster_gps.py::test_2018_19_geo_subdf_maps_the_coordonnes_gps_columns tests/test_burkina_faso_cluster_gps.py::test_2014_declares_no_geo_subdf tests/test_burkina_faso_cluster_gps.py::test_no_wave_asks_for_lat_dd_mod tests/test_burkina_faso_cluster_gps.py::test_optional_matches_the_waves_that_lack_gps > "$task_evidence/candidate-tests-private.log" 2>&1
bash "$task_evidence/run.sh" "$task_evidence/hashes.py" > "$task_evidence/candidate-hashes-private.log" 2>&1
bash "$task_evidence/run.sh" bench/feature_audit/scan.py --features plot_features --countries Burkina_Faso Senegal --phases 1,2 --jobs 1 --build-timeout 120 --out "$task_evidence/candidate-findings-private.jsonl" > "$task_evidence/candidate-scanner-private.log" 2>&1
bash "$task_evidence/run.sh" scripts/api_surface_audit.py > "$task_evidence/api-audit-private.log" 2>&1
bash "$task_evidence/run.sh" "$task_evidence/verify_final.py" > "$task_evidence/verification-private.log" 2>&1
```

Each cold/warm process compares all 13 full frames, with logical attribute
equality separate from pickle storage. Full warning and scanner multisets,
source copies, comparator bytes/modes/mtimes, tracked-tree snapshots and
549/39 inventories are checked by the finisher. Metadata-only preparation
of the ignored DVC tmp symlink is recorded in root-metadata-preparation.json;
no other checkout writes or data bypass are permitted by the guard.

Safe aggregate results are in REPORT.md and validation.json. The final
private freeze-proof.json records the exact final HEAD and quiescence.

Sue
