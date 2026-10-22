# Reproduction and exact checker source pins

These are the executed bounded stages. Do not replay into this frozen root.
For reproduction, make fresh physical private roots, adapt only the explicit root/checkout paths, prepare the two ignored DVC metadata targets, and re-run the terminal source/copy/current-hash gates before dependent stages. Never reuse or stamp an old cache.

`run.sh` fixes the mounted interpreter, cwd/PYTHONPATH/countries/config/data identities, private paths, all numeric/build/Make threads1 and `unshare -rn`. `common.py` asserts the active guards and exact identities before pickle/API use. `diagnostic.py` saves full failures privately and exposes only class/source locations. Actual pytest selection/arguments are in `tests.py`; it uses `-n 2 --dist=loadfile`.

```bash
P=/local/job40255479/control/isa-backlog-20261021/integration/mali-eighteen-review
cd "$P"
# Independent metadata inventories BEFORE preparation/build.
./run.sh base hash-base "$P/diagnostic.py" "$P/hash_inventory.py"
./run.sh candidate hash-candidate "$P/diagnostic.py" "$P/hash_inventory.py"
# Guarded physical preparation; see preserved initial filename stop.
./run.sh candidate preparation "$P/diagnostic.py" "$P/prepare.py"
./run.sh base base-niger "$P/diagnostic.py" "$P/niger_control.py" cold
./run.sh base base-niger "$P/diagnostic.py" "$P/niger_control.py" warm
./run.sh base oracle-niger "$P/diagnostic.py" "$P/source_oracle_niger.py"
./run.sh candidate comparator-copy "$P/diagnostic.py" "$P/copy_current_niger.py"
# The Mali source oracle's original signature stop is preserved. Its source
# assertions/serialized control completed; resume_mali_oracle is artifact-only.
./run.sh candidate oracle-mali "$P/diagnostic.py" "$P/source_oracle_mali.py"
./run.sh candidate oracle-mali-resume "$P/diagnostic.py" "$P/resume_mali_oracle.py"
./run.sh candidate tests-candidate "$P/diagnostic.py" "$P/tests.py"
./run.sh base tests-base "$P/diagnostic.py" "$P/tests.py"  # expected24FAIL/4PASS
./run.sh candidate audit "$P/diagnostic.py" "$P/api_audit_entry.py"
./run.sh candidate candidate-default-all "$P/diagnostic.py" "$P/probe.py" cold
bash candidate_matrix.sh
bash base_matrix.sh
./run.sh candidate population "$P/diagnostic.py" "$P/populate.py"
bash populated_matrix.sh
./run.sh base base-default-all "$P/diagnostic.py" "$P/covered_empty.py"
./run.sh candidate candidate-default-all "$P/diagnostic.py" "$P/covered_empty.py"
./run.sh base base-default-all "$P/diagnostic.py" "$P/features.py"
./run.sh candidate candidate-default-all "$P/diagnostic.py" "$P/features.py"
./run.sh candidate candidate-default-all "$P/diagnostic.py" "$P/held_public.py"
bash cli_matrix.sh
./run.sh base base-default-all "$P/diagnostic.py" "$P/scanner.py"
./run.sh candidate candidate-default-all "$P/diagnostic.py" "$P/scanner.py"
./run.sh candidate checker "$P/diagnostic.py" "$P/check_artifacts.py"
./run.sh candidate accounting-final "$P/diagnostic.py" "$P/warnings_scanner_check.py"
./run.sh candidate freeze-check "$P/diagnostic.py" "$P/freeze_checks.py"
```

`hash_inventory.py` writes by SIDE; those illustrative metadata-only scenario labels do not alter the base/candidate inventory filenames. The private source/copy receipts pin all exact inputs and existing source references. The oracle command records the historical stage: its stopped script snapshot is retained under `stops/Mali-oracle-comparator-call/`. `source_oracle_mali.py` now has the corrected comparator label for fresh reproduction; it was not re-decoded after the preserved signature stop in this review. A fresh successful oracle run needs no resume; `resume_mali_oracle.py` checks the original TypeError receipt and consumed only its saved artifacts in this review.

`exact.py`, `check_artifacts.py`, `warnings_scanner_check.py`, `source_oracle_mali.py`, `source_oracle_niger.py`, `freeze_checks.py`, all launchers and source snapshots are private and pinned by `checker-sources.json` and the final full manifest. Full child logs and traceback bodies must not be exported. The safe export allowlist contains only REVIEW.org, proof.json, verdict.json, REPRODUCE.md and checker-sources.json.

Sue
