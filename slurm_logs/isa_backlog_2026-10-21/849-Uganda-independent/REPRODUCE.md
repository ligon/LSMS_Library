# Independent Uganda849 reproduction (private evidence; readonly sources)

The exact executed sources and SHA256 pins are in checker-sources.json. Run under the supplied run.sh; it sets the mounted interpreter, exact checkout/countries/private config/data, guard+unshare-rn,1 numerical/build/Make thread and no acquisition/bypass. Never execute old author launchers or write into a frozen root. A fresh authorized review root and exact metadata preparation are prerequisites.

```bash
P=/local/job40255479/control/isa-backlog-20261021/review-Uganda-849
bash "$P/run.sh" candidate metadata "$P/preflight.py"
bash "$P/run.sh" base metadata "$P/hash-inventory-v2.py"
bash "$P/run.sh" candidate metadata "$P/hash-inventory-v2.py"
# Wait for BOTH inventory terminal exits before the scope gate.
bash "$P/run.sh" candidate metadata "$P/hash-scope-own.py"
bash "$P/run.sh" candidate metadata "$P/prepare.py"
# Wait for preparation terminal exit before decode/API.
bash "$P/run.sh" candidate metadata "$P/source-terminal-check.py"
bash "$P/run.sh" candidate default "$P/confirm-source-fields.py"
# Each source stage below was launched once, per side/backend, with unique logs.
for side in base candidate; do
  for backend in default make; do
    bash "$P/run.sh" "$side" "$backend" "$P/build-cold.py" cold
    bash "$P/run.sh" "$side" "$backend" "$P/warm.py"
    bash "$P/run.sh" "$side" "$backend" "$P/warm-native-artifacts.py"
    bash "$P/run.sh" "$side" "$backend" "$P/subsets-own.py"
  done
  bash "$P/run.sh" "$side" default "$P/features.py" initial
  bash "$P/run.sh" "$side" default "$P/features.py" warm
  bash "$P/run.sh" "$side" default "$P/cli-check.py"
  bash "$P/run.sh" "$side" default "$P/scanner.py"
done
bash "$P/run.sh" candidate default "$P/prepare-tests.py"
bash "$P/run.sh" candidate tests "$P/focused-tests.py"
bash "$P/run.sh" base negative "$P/parent-negative.py" # expected exit1:20FAIL4PASS
bash "$P/run.sh" candidate default "$P/source-factor-oracle-v2.py"
bash "$P/run.sh" candidate default "$P/derived-v2.py"
# Artifact-only controls: no Country/Feature/source build replay.
bash "$P/run.sh" candidate metadata "$P/negative-diagnostic.py"
bash "$P/run.sh" candidate metadata "$P/tiny-exact-diagnostic.py"
bash "$P/run.sh" candidate metadata "$P/own-artifacts-v4.py"
bash "$P/run.sh" candidate metadata "$P/strict-artifact-sweep-v3.py"
bash "$P/run.sh" candidate metadata "$P/feature-compare-strict-v2.py"
bash "$P/run.sh" candidate metadata "$P/warning-account-own-v2.py"
bash "$P/run.sh" candidate metadata "$P/test-account-own-v3.py"
bash "$P/run.sh" candidate metadata "$P/final-account.py"
bash "$P/run.sh" candidate metadata "$P/protected-final-v2.py"
```

Capture each invocation into previously absent, uniquely named private stdout/stderr files; never pipe pytest output or reuse a name. The original run retains those separate logs under the manifest.

This is the dependency order, not permission to rerun inside the frozen root. Actual observed scheduling used independent roots concurrently, with at most2 pytest loadfile workers TOTAL. Every launch had a unique stdout/stderr filename; per-call full warnings/endpoints and all stopped versions survive. The actual pytest selection and basetemp/JUnit arguments are in focused-tests.py and parent-negative.py. Final seal/readback commands are recorded by seal.py and FINAL-READY.json.

Sue
