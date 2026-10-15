SCOPE DEVIATIONS: none. Test isolation only; no production, country build configuration, scientific assertion, fixture, dependency, skip, or timeout changes.

# GH #1010: a simulated rebuild leaked into later tests

The collect-only regression helper left `LSMS_NO_CACHE=1` after teardown when the variable was initially absent. `MonkeyPatch.delenv(raising=False)` records no undo entry for an absent variable; the actual `pytest_configure` then directly assigns `1` for a simulated rebuild. The following inert-flag test also restores that leaked value. Existing values `0` and `1` were already restored correctly.

The fix registers the original state with `monkeypatch.setenv("LSMS_NO_CACHE", "")` before the existing deletion. Production configuration still sees the variable absent on entry and still honors explicit rebuild requests. Twelve regression cases cover absent / `0` / `1`, both rebuild options, and normal / exceptional teardown. Against the old helper, four absent cases fail and eight existing-value cases pass. With the fix, the complete file passes all 18 tests. Existing purge spies prevent real cache deletion.

Original integrated source: `cf1840e02317bac052b99e1e6c9fa066a8ca45bc`. Frozen proposed fix: `22a841ed8940c618680c5b4363fcb60e455b4a14`. Its ancestry includes held #967 and must not be merged wholesale into development. The narrow development port is `f00f329b3`, [PR #1011](https://github.com/ligon/LSMS_Library/pull/1011), containing only the ledger and helper test changes from commits `54b0b47d2` and `22a841ed8`.

## Discriminating reproduction

All Ghana builds used physical copies of the same quiescent GhanaLSS cache: 88 files, 60,298,616 bytes, equal SHA256 values and distinct inodes. The original source was rechecked unchanged. Only the shared content-addressed DVC blob cache was linked; writable L2 parquets were never linked. Package/config identity, private root, CPUs 16–23, build-workers 1, and BLAS/OMP/MKL 1 were pinned.

The original Ghana test, run alone, passed in 111.94 seconds with zero Ghana wave-build subprocesses. The ordered replay runs the actual helper file through `pytest.main`, then the same four API calls and exact three row-count assertions as that Ghana test.

| Call | Fixed ordered replay | Rows |
|---|---:|---:|
| `_country` → `food_acquired` | 24.20 s | 5,340,606 |
| `food_prices(units='unitvalue')` | 31.01 s | 1,676,093 |
| `food_prices(units='unitprice')` | 24.98 s | 189,656 |
| `food_quantities(units='units')` | 29.57 s | 1,694,623 |

Fixed helper execution leaves the variable absent. All four calls pass with **zero Ghana build subprocesses**. A separate exact sequential pytest invocation—complete helper file followed by the original Ghana test—passes **19 tests, 31 warnings, in 112.26 seconds**, also with zero Ghana builds.

The old helper's six tests pass but leave `NO_CACHE=1`. The first food call takes 329.27 seconds and rebuilds all seven waves; the next API starts another seven-wave rebuild. The old full replay remains in progress at this archive checkpoint; `comparison.json` states its completion boundary explicitly. No incomplete run is counted as a pass.

At every captured API/subprocess boundary, the expected and stored food hash both remain `c5f41b5cd6651509b55c11430793a07b26ba38654c1f7dc043308f0f1bd3c904`, the manifest contains all seven waves, and each private root is stable. The observer asserts that installing the external subprocess observer leaves this cache identity unchanged. This is forced bypass, not a demonstrated root, manifest, or fingerprint defect.

Ghana's relevant idiosyncrasy is that `u='Value'` records expenditure rather than physical mass. CONTENTS's older heading says **“food_prices returns ONE wave, and the other six are CORRECTLY empty”** (line 2025). Its newer farmgate section says **“It is now derived: Expenditure = Quantity * Price”** (lines 1306–1309), covering GLSS4–GLSS7. The old coverage statement predates that addition; this repair changes neither policy nor the original test counts. Prior art: #719 collect-only purity, ledger 780 fingerprint stability, #891 manifest/cache bypass, #1003 private-root fixture isolation.

## CI limits and retained comparisons

[Controlled data job 38688613005](https://github.com/ligon/LSMS_Library/actions/runs/38688613005/job/116269105325) stopped at the 600-second timeout during Ghana unitprice, waiting for a wave subprocess. It has no final pytest outcome counts. Its recorded ordering runs the leaking helper before Ghana. Local reproduction establishes the bypass mechanism; it does not prove the complete runner-level timeout cause. Corrected integrated run [38697245468](https://github.com/ligon/LSMS_Library/actions/runs/38697245468), source `9d5bf200f170f322076626386689b498bebb1c06`, supplies the next comparison. The timeout and scientific assertions remain unchanged.

After reproducing the leak, the parent cancelled old runs 38681185279 and 38683117467; their completed partial logs were then fetched and preserved. The earlier direct in-progress log request returned HTTP 404. They reached 66% and 34%, respectively, after Ghana currency and EHCVM tests; neither has a final summary or an F/E marker in captured file progress. This is incomplete evidence, not a full pass or proof of a stall. `cancelled-comparison.json` records exact source/job identities, links, log hashes and final progress. The separately completed native Windows success remains valid despite cancellation of its workflow's data job. No #941 cause or resolution is inferred; the Uganda historical nutrition fixture hold remains intact.

## Reproduction and evidence

`probe_environment.py` invokes the actual helper tests with purge spies only; it makes no survey/cache build calls. Run with `LSMS_SKIP_AUTH=1`, an empty `LSMS_CONFIG_DIR`, private `LSMS_DATA_DIR`, and `PYTHONPATH=<target-checkout>`, from that checkout. Its absent/0/1 results are in `environment-before.json`.

`ordered_replay.py` and `observe_ghana.py` reproduce the API comparison. Set `PYTHONPATH=<checkout>:<directory-containing-observe_ghana.py>`, `LSMS_COUNTRIES_ROOT=<checkout>/lsms_library/countries`, `LSMS_DATA_DIR=<physical-private-copy>`, and `GHANA_EVENTS=<private-output.jsonl>`. Use the resource pins above. Run the replay from the target checkout; for the exact lifecycle test run:

```sh
python -m pytest -p observe_ghana \
  tests/test_collectonly_no_purge.py \
  tests/test_currency_denominated_units.py::test_ghanalss_unitvalue_and_units_modes_are_unaffected \
  --timeout=600 --timeout-method=thread --durations=10 -q >private-pytest.log 2>&1
```

The repository profiling skill was read. These probes record API and subprocess timing, not a comprehensive CPU profile, and do not replace hashed production functions. `comparison.json`, `copy-identity.json`, and `private-evidence-hashes.json` contain aggregate evidence only. Raw pytest warning logs and full event streams remain private; no survey rows, household identifiers, or credentials are included here.
