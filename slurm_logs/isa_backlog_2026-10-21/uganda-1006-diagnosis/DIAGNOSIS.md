GH #1006 is confirmed test debt at `f8d236694c26fe9e709c5548f02c70f74ea72c33`. No tracked edit or production change has been made. The concrete [three-file proposal](PROPOSAL.md) and [ledger draft](ledger-draft.md) are ready for root boundary approval.

The live [issue #1006](https://github.com/ligon/LSMS_Library/issues/1006) was read with its comments (none at inspection). Uganda CONTENTS was read first for relevant ID/unit/coverage constraints and live TODO/WAITING/LOGBOOK caveats. Actual source readiness is food_acquired for food derivations, household_roster for household characteristics, sample for cluster/household geography, registered income for cross-table attrition and cluster_features for Region fallback. All six checked prerequisite caches are f8-hash-fresh and cover all eight waves. The private reproduction physically copied 170 Uganda prerequisite parquets from the pinned 1,519-file release manifest and 581 verified held raw L1 blobs; integration-head L2 caches were not trusted. Nutrition/FCT were excluded.

| Check | Result |
|---|---|
| Unmodified three files, warm prerequisites/no derived parquets, credentials available | 5 passed; 10 skipped; zero failures |
| Assertion-line tracing of passing dtype tests | Only nonempty checks execute; zero dtype assertions |
| Old missing/object/string controls, both API and synthetic cache-read surfaces | All 12 faults incorrectly pass |
| Required canonical-column + existing float predicate controls | Six distinct bad payloads rejected; float64 and Float64 accepted for both columns |
| Existing real attrition test bodies after bypassing stale decorators | Both pass; no threshold-50 violation; all adjacent overlaps/diagonals positive |
| Existing attrition assertion fault controls | Six rejected without changing bodies |
| Default/Region key, duplication and measurement fault controls | Six rejected |
| Source absence and credential absence controls | Each skips 15 original cases before data acquisition; empty root has no payload files |
| Present/absent LSMS_DATA_DIR × normal/injected-exception restoration | Four controls pass; outer/private source bytes and mtimes unchanged |

The core defects have concrete anchors:

- `tests/test_food_prices_dtype.py:42` and :54 list old wide columns. Its API outputs actually have canonical Price (Float64) or Quantity (float64), so :110/:124 never reach their conditional dtype assertions. The direct-cache tests at :67/:84 look for derived files that normal API never persists.
- `tests/test_panel_attrition.py:45` and :55 guard on household_characteristics.parquet, even though current dispatch derives it from household_roster. The body assertions themselves pass and discriminate six injected faults; keep them unchanged. The "monotonically non-decreasing" comment at :74 is unimplemented and contradicted by both actual diagonals; correct the comment rather than inventing that population promise.
- `tests/test_uganda_tables.py:13` guards on derived outputs; :64 requires default m; :24 uses non-strict migration xfails. Source-ready direct execution exposes four m assertion failures. Current Country defaults to market=None at country.py:6150, preserving v, and explicit Region adds m and drops v via :3221. The HC m case at :91 is separately guarded out; it should explicitly request Region. The legacy fallback-named test at :113 actually returns through runtime derivation, not the wave fallback; its nonempty/i/j checks can be folded into the stronger food-expenditure default case, while actual Wave recursion tests remain in test_wave_getattr.py.

Observed successful default outputs reproduce the issue's reviewed contracts: food_prices 348,181 rows/Price Float64; food_quantities 355,487/Quantity float64; food_expenditures 247,049/Expenditure Float64; household characteristics 24,364 rows, 14 integer demographic columns and floating log HSize. These are evidence counts, not proposed exact fixture pins. Income is 14,740 rows. No derived cache parquet exists before or after these normal calls.

Explicit Region calls retain exact values/dtypes and every non-geographic key for retained rows, while dropping 36 price, 37 quantity, 31 expenditure and 67 HC rows with unresolved market lookup. Their row counts are therefore 348,145 / 355,450 / 247,018 / 24,297. All known household Region assignments match sample exactly, and fallback rows remain explicit. The existing 2009 Region-HC 2,975-household pin holds. No new attrition or retained-value failure was found. Do not add an unjustified equal-row-count assertion, replace NA Regions or repin coverage to conceal failures.

Two reported qualifications are outside the proposed test repair. Global data_info.yml:56 still says HC `(t,v,i,m)`, contradicting the actual default Country contract; no production schema edit is proposed. HC's Region merge also changes the column-axis name `sex_age` to None; retained measurements, labels and payload dtypes are exact, but complete axis metadata is not. This was detected by an initial strict comparison and reported, not hidden as a value-preservation claim.

Relevant Uganda idiosyncrasies are substantive. v spans incompatible EA renderings, synthetic GPS bins and later parish identities; it must not be assumed stable across waves. Some source v/Region values are absent. CONTENTS:253's blanket "complete for every household" statement has its own :270 qualification of three missing 2019-20 Regions. Rotation/refresh design intent is qualified by the DDI's "This same sample was used" statement for 2015-16; attrition diagonals need not increase. Unit handling keeps reported-quantity Unknown distinct from expenditure-only Value and accepted raw-code residuals; no unit conversion decision changes. Live tasks include phantom outer-merge rows, missing GPS, null-key crop drops, producer-unconfirmed sales correction, panel weights and item-specific recall gaps. The separately reviewed #1077 build-versus-query unit qualification and #1062 sibling routing remain untouched. Nutrition/FCT/fixture decisions are not reopened.

Reuse is documented in the draft ledger: production derived dictionaries, data_root, pandas is_float_dtype, panel_attrition, existing market/sample/wave regressions, import-free requires_s3 markers and existing MonkeyPatch/cache_clear restoration patterns. The tests/conftest.py documentation explicitly prohibits both ambiguous conftest import spellings. No new shared helper package or global marker-policy/fixture change is proposed.

Executed private commands use the mounted image Python through run.sh, which pins exact f8 cwd/package/config, private data root, unshare -rn, active socket-method guards, numerical/build/make threads=1 and private logs. The trace run is serial. No source was acquired and no shared cache was written/cleared. Commands/artifacts:

```sh
python prepare.py
bash run.sh run_current.py
bash run.sh probe_contracts.py
bash run.sh probe_controls_cleanup.py
bash run.sh run_skip_control.py empty
bash run.sh run_skip_control.py credentialless
bash run.sh final_proof.py
```

Here python is `/local/job40255479/venv_img/bin/python`; scripts are under this private evidence directory, and cwd is `/local/job40255479/worktrees/isa-Uganda-1006`. Preparation is setup-only; copies/control roots are retained as evidence. Logs, full warnings, private outputs and keys remain private. See current-tests.json, contract-probe.json, discriminating-controls-cleanup.json, the two skip-control JSON files and final-proof.json for aggregate evidence. The strict first Region probe and initial overly strict empty-directory postcondition are preserved in private logs; completing those probes retained the HC metadata finding and permitted harmless empty directories, without weakening a coverage/value assertion or manufacturing a cache file.

The full post-approval test matrix, exact boundaries and normal/exceptional absence-cleanup plan are in PROPOSAL.md. This task stops here as requested; implementation, commits and GitHub writes await root approval.

-- Sue
