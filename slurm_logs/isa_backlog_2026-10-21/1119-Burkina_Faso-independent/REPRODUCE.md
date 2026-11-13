# Independent BF1119 reproduction — Sue

Candidate3dc9d28510dd783c648413987011832669f8bcf7 versus actual parentcfd0782766068da6dc7e7605334e957f988a4ab1. Source/config/test trees and completed proof root are immutable. Replay requires NEW private roots/exact ignored DVC metadata preparation, adjusted constants, physical copies and terminal gates; never rerun sealed roots or author evidence.

```bash
P=/local/job40255479/control/isa-backlog-20261021/review-Burkina_Faso-1119
cd "$P"
bash run.sh candidate prepare "$P/bootstrap.py"
bash run.sh base hash-base "$P/diagnostic.py" "$P/hash_inventory.py"
bash run.sh candidate hash-candidate "$P/diagnostic.py" "$P/hash_inventory.py"
# First preparation manifest-shape stop preserved; corrected private reader:
bash run.sh candidate prepare "$P/diagnostic.py" "$P/prepare.py"
# Only terminal SUCCESS permits dependent decoding/APIs/pickles.
bash run.sh candidate tests-candidate "$P/diagnostic.py" "$P/test_launch.py"
bash run.sh base tests-base "$P/diagnostic.py" "$P/test_launch.py"  # expected1:5fail/4pass
bash run.sh candidate oracle "$P/diagnostic.py" "$P/source_oracle.py"
for side in candidate base; do
 bash run.sh "$side" "$side-default" "$P/diagnostic.py" "$P/build.py" cold
done
# Original candidate first warm sample/TypeError saved, checked artifact-only.
bash run.sh candidate candidate-default "$P/diagnostic.py" "$P/preserve_warm_stop.py"
bash run.sh candidate candidate-default "$P/diagnostic.py" "$P/build.py" warm --resume-first-saved
bash run.sh base base-default "$P/diagnostic.py" "$P/build.py" warm
for side in base candidate; do
 bash run.sh "$side" "$side-make" "$P/diagnostic.py" "$P/build.py" cold
 bash run.sh "$side" "$side-make" "$P/diagnostic.py" "$P/build.py" warm
 for backend in default make; do bash run.sh "$side" "$side-$backend" "$P/diagnostic.py" "$P/cli_check.py"; done
 bash run.sh "$side" "$side-default" "$P/diagnostic.py" "$P/scanner.py"
done
bash run.sh candidate audit "$P/diagnostic.py" "$P/api_audit.py"
# Original negative-axis setup and native/readtime unit expectation stops retained.
bash run.sh candidate prepare "$P/diagnostic.py" "$P/oracle_axis_diagnostic.py"
bash run.sh candidate prepare "$P/diagnostic.py" "$P/oracle_projection_v2.py"
bash run.sh candidate prepare "$P/diagnostic.py" "$P/check_artifacts.py"
bash run.sh candidate prepare "$P/diagnostic.py" "$P/warnings_check.py"
# First wrong-source footer expectation/metadata diagnostic preserved.
bash run.sh candidate prepare "$P/diagnostic.py" "$P/freeze_verify.py"
bash run.sh candidate prepare "$P/diagnostic.py" "$P/package_reports.py"
bash run.sh candidate prepare "$P/package_seal.py"
bash run.sh candidate prepare "$P/finish_seal.py"
```

These are recorded stage commands, not instructions to replay strict-stop prefixes in frozen roots. Unique original scripts/logs/operands/diagnostics remain under stops/. The source/API work was not replayed for the artifact, unit-layer or footer corrections. Existing source body assertions run only on default cold held APIs; Make comparators/pooled Features are intentionally excluded. `build.py` pins each exact call, backend, arguments and endpoint receipt; `test_launch.py` pins full pytest argv,131 candidate outcomes and isolated nine-case parent counterfactual. Two actual data tests are deselected from pytest and directly run separately, not counted as credential-free data passes.

run.sh uses mounted interpreter, exact cwd/PYTHONPATH/package/countries/config/data, private XDG/temp/DVC/config, absent credentials, numeric/build/Make1, socket/write guards and unshare-rn. common.py verifies those identities before frames/APIs. Data caches have one consumer per root. Exact.py checks explicit zero-tolerance values, all axis internals and recursively typed full attrs/_FIELDS. Source oracle v2 reads only saved sanctioned source frames and actual local Org BUILD dictionaries; no candidate helper/finalizer generates its expected rows. Warnings checker asserts all76 complete pair booleans and all scanner occurrences; original literal files remain private. Freeze verifier rechecks source/copy/author receipts and side-specific prebuild hashes without builds/stamps.

All full records/IDs/logs/warnings/tracebacks are private. Safe exports and their hashes/checker sources are allowlisted in FINAL-READY. Complete payload path/receipt manifest, clean source pins and terminal zero-consumer seal govern release, not a pre-seal draft report.
