SCOPE DEVIATIONS

- No production repair was warranted: the current four-wave Malawi `plot_inputs` population has no served missing-unit residual. This is a measured no-patch result, not a replay of historical counts.
- `make api-audit` could not find Poetry (exit 2). Its exact Python audit script ran successfully with the approved mounted interpreter; no dependency was installed.
- `unshare -n` was unavailable (Operation not permitted). Source actors instead inherited a Python audit hook refusing DNS and socket connections, including native producer subprocesses; the final verification calibrates the DNS refusal. This is Python network isolation, not a kernel network namespace.
- One serial test process briefly overlapped a census process before explicit CPU affinity was added. Both had thread pools/build/make limited to one. Subsequent Python actors were explicitly pinned to one CPU. No new job or runtime was created.
- No unsupported 2004 `plot_inputs` request was made. The standard all-wave `sample()` prerequisite required its declared 2004 cover source, alongside the six later cover sources.

RESULT

At pinned public development `d96a8152b35e2abd3f3f6fd1f9d68b37d0587f81`, normal Country and Feature calls serve:

| Wave | Rows | Unknown | Other nonblank units | Null / float NaN / blank / stringified missing |
|---|---:|---:|---:|---:|
| 2010-11 | 46,605 | 158 | 46,447 | 0 / 0 / 0 / 0 |
| 2013-14 | 16,209 | 12 | 16,197 | 0 / 0 / 0 / 0 |
| 2016-17 | 52,268 | 57 | 52,211 | 0 / 0 / 0 / 0 |
| 2019-20 | 64,835 | 13 | 64,822 | 0 / 0 / 0 / 0 |
| Total | 179,917 | 240 | 179,677 | 0 / 0 / 0 / 0 |

“Other nonblank” above is a count category, not the reported `Other (Specify)` label. Each wave's complete unit histogram is in `census-<wave>-cold.json`. Reported `Other (Specify)` and source-qualified organic units remain separate from Unknown. Plain float NaN was checked explicitly as well as through pandas null masks. No canonical `u` is NA; the served unit levels use nullable string dtype.

PRECEDENTS AND IDIOSYNCRASIES

Read Malawi CONTENTS first, including its TODO entries (no live LOGBOOK/WAITING blocks were found), then AGENTS/CLAUDE, the prior-art ledger, cross-country feature skill and Feature audit README. Used git/ripgrep in place of unavailable GitNexus. `api-audit.txt` records the on-disk skill inventory. The per-task ledger cites the standing baseline rather than duplicating it.

Live #1118 and PR #1251 explicitly say the old 28,724 and 6,075 counts are historical. #1251 already handles string/pd.NA missing labels, but explicitly leaves plain NaN outside its repair. No generic conversion was recreated. Current Malawi native assembly casts `u` to nullable string, and the existing map_index/finalizer path handles its missing values.

CONTENTS' old statement that the #1111 C4 gate “remains for 2016-17 ... and 2013-14” is explicitly superseded by its later 2026-10-23 entry: “The C4 gate ... is now closed by questionnaires”. The latter explains that the selected fertilizer amounts are enumerator kilogram totals, while e/j are timing. Current drivers label those totals Kilogramme. Neither historical count nor the earlier gate was silently treated as current. This census does not attempt a retrospective numerical decomposition of all intervening changes.

#1323 preserves six Module G sources' original observation identities, including original positions preceding nine authorized 2010 full-record repeat removals. D retains its reducers. H purchase attachment requires one known-plot G observation of the native crop code/source half; unknown/all-NA siblings veto ownership. This idiosyncrasy was preserved. Live #1190 confirms H09/H40 transport, H10 purchase value and H38 free receipts in the cross-sectional files. That finding does not decide a new measurement representation or authorize changes to ownership/eligibility.

METHOD AND BINDINGS

Source/config/runtime pin: d96, retained fast-forward throughout; the later PR1333 fertilizer_rate work was not adopted and is outside this census. `project-runtime.json` in the dispatch parent binds the approved mounted interpreter and private config/data/DVC locations. `process-identities.jsonl` records process identities, including wave scripts. `scripts/sitecustomize.py` enforces package, exact country/data/cache/interpreter/config identities and checkout-local cwd; `scripts/env.sh` limits threads and uses private HOME/config/cache/tmp bindings.

Saved #1190 operands were considered first. All four Malawi drivers, malawi.py and feature.py matched their published code SHA256s, but country.py did not. They were therefore not accepted as current API operands. No historical failed receipt was converted into passing evidence.

Started with cached-source 2013-14; then expanded to 2010-11, 2016-17 and 2019-20. The shared L1 was read-only. Only 25 unique declared D/G/H and required sample source blobs were staged as ordinary private copies after availability and exact sidecar MD5 checks; destination MD5 was verified too. `sources-<wave>.json` provides source path/MD5/size/SHA256 receipts. No provider acquisition, DVC CLI, credential inspection or shared cache clearing occurred. Standard coverage-union rebuilding caused earlier waves to rebuild when a new wave was requested. Every cold call finished below 300 seconds (largest about 102 seconds), and warm calls below 60 seconds.

`code-config-test-bindings.json` binds framework Python/schema files, Malawi build/config inputs and selected test files by SHA256. `witness-manifest.json` binds new private Country/Feature pickles, held only under campaign sibling `private-witnesses/95c34db5277843d2b00fa5a597b6eda5` with private modes. They are not in Git or this evidence bundle. `scripts/census.py`, `scripts/exact.py`, `scripts/final_verify.py` and their saved initial versions make the measurements reviewable. Evidence checksums are in SHA256SUMS.

VERIFICATION AND PRESERVED FAILURES

- Each single-wave Country/Feature cold and fresh-process warm run agrees after Feature's documented country/season addition, crop-to-j alias and index reordering. Feature drops entirely null Improved on 2010/2013 subset calls; comparisons restore only columns independently checked entirely null. This is an explicit API shape difference, not a claim of literal unaligned frame equality.
- Strict comparisons check axes/order, dtypes, null masks and exact nonmissing NumPy values without coercion/tolerance. Final checks also assert extension-array types/equality. Fourteen negative controls reject tiny and nextafter numeric changes in Float64 and float64, plus mask/dtype/order/index/attrs mutations. Combined-wave results are checked against each saved single-wave result. Warm calls compare attrs exactly; Country/Feature attrs agree after only the documented canonical_alignment addition. Across differently scoped wave subsets, payload comparisons exclude population/recall attrs whose wave coverage legitimately differs.
- Warm parquet mtimes are unchanged. No production bytes changed: source observations/input_obs, nonblank units, quantities/values, purchase ownership, eligibility, other keys, dtypes and attrs were not repaired or rewritten.
- Target tests: **50 passed, 17 requires_s3 deselected**, pytest serial `-n 0 --dist=loadfile`, in `tests.txt` / `tests.xml`. Includes #1251's intentionally preserved plain-NaN behavior and #1323's observation/ownership cases. No new synthetic test was needed for a no-production-change result.
- Initial 2013 comparison failed because it assumed Feature retained an all-null Improved column. Original script and failure log are retained; the corrected comparison explicitly models the documented behavior. Two private plot_inputs parquets from that attempt were removed to make the corrected 2013 run cold; shared caches were untouched.
- The first combined-wave receipt writer failed after comparisons on `.sum()` of an ArrowExtensionArray. Original script/log are retained. The scalar count was corrected to use a Series and the entire final verification rerun. This was evidence code, not a library defect.
- Existing categorical/sample warnings, single-wave all-null Improved warnings, frame-fragmentation warnings and mixed-population warnings remain visible in logs. Nothing was suppressed or patched to make them disappear.

PATCH, COST AND NEXT STEP

Only this report, the per-task ledger and a new Malawi CONTENTS census entry are committed. No runtime/config/test/schema change, no shared-machinery affected-country scope and no added hash invalidation cost. CONTENTS is explicitly excluded from build-input hashing; report/ledger are not build inputs. No corpus hash census or Make-backend acceptance is claimed.

Root should independently review the receipts and reconcile the Malawi `plot_inputs` remainder of #1118 as having no current served missing-unit residual at d96. Keep the umbrella open for its other country scopes; do not extrapolate this result to Malawi food_acquired or crop_production. #1190 purchase measurement representation, #1174 eligibility, source-unit decoding and fertilizer scientific questions remain for their existing human-review lanes. No GitHub write, public PR, merge or issue closure was performed here. Coordinator acceptance remains pending.

ROOT INTEGRATION REVIEW — 2026-10-30

Root and an independent Uganda reviewer support the no-production-patch conclusion for these four Malawi plot_inputs waves. Root authenticated all 57 manifest artifacts and 135 code/config/test bindings; the country documentation was already edited during final verification and matches the final documentation commit, while production bindings match d96. The independent review checked full unit histograms, sorted Country/Feature alignment, the all-null Improved adjustment and retained failure qualifications. This does not assert raw cross-surface row-order equality; warm within-surface order and attrs are checked. Warm cache evidence establishes unchanged parquet mtimes, not an instrumented no-write proof. #1118 remains open for Nigeria and non-ISA scope.
