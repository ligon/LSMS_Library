**SCOPE DEVIATIONS**
- `lsms_library/countries/Nigeria/_/data_scheme.yml` — not named by the task, but the column must be declared `optional: true` there or Site B and the required-column guard have nothing to exempt. Not on the stop-list.
- `tests/test_nigeria_sold_unit.py` — new test file, not named by the task. Not on the stop-list.
- `lsms_library/countries/Nigeria/_/CONTENTS.org` — the task explicitly asks for idiosyncrasies to be recorded there.

No stop-list file was touched. Nothing is left uncommitted.

**1. Commits and branch**

Branch `fix/nigeria-824`, pushed to `origin` (the shared mirror) and confirmed at `origin/fix/nigeria-824`:
- `8cc7eed2b` the fix (nigeria.py, crop_production.py, data_scheme.yml, transformations.py, CONTENTS.org, new test)
- `d568ab428` CONTENTS.org follow-up with the API-frame and #867 measurements

I amended the first commit once after pushing, which made the branch non-fast-forward. Force-push is forbidden, so I reset my own clone back to the pushed commit and added the amendment as the second commit instead.

**2. Verification**

Package identity assertion passed before any edit, and `find lsms_library/countries -name '*.parquet'` stayed at 0 throughout.

Pytest summaries, verbatim:
- `tests/test_nigeria_sold_unit.py` alone: `15 passed in 10.25s`
- Regression selection (new file plus `test_nigeria_kg_factor`, `test_863_transforms`, `test_schema_consistency`, `test_feature_canonical_index`, `test_uganda_sold_unit`, `test_build_transform_hash`, `test_derivations`): `496 passed, 8 skipped, 43 warnings in 476.21s (0:07:56)`. The skips are Malawi tables not being warm in my private root, unrelated to this change.

Before/after numbers, all measured on the isolated root:

| claim | before | after |
|---|---|---|
| `crop_production` rows | 62,844 | 62,844 (index and all 9 pre-existing columns byte-identical) |
| `Unit_sold` non-null, 2011Q1 / 2013Q1 / W3–W5 | column absent | 3,486 / 3,430 / 0 |
| raw-code agreement 2011Q1 (CONTENTS.org figure) | 3,438 of 3,548 (96.9%) | reproduced exactly |
| raw-code agreement 2013Q1 | 3,316 of 3,478 (95.3%) | reproduced exactly |
| served size-collapsed agreement, raw files | — | 3,466 (97.7%) / 3,349 (96.3%) |
| agreement on the built table, parquet and API frame identical | — | 3,374 of 3,451 (97.8%) / 3,283 of 3,411 (96.2%) |
| #867: `Other Inorganic Fertilizer` rows unmatched in `nitrogen_kg(Nigeria plot_inputs)` | 170 | 0 |
| #867: matched rows / plots / total kg N | 10,014 / 4,359 / 173,672 | 10,184 / 4,472 / 178,074 |

The framework read (`Country('Nigeria').crop_production()`) served `Unit_sold` after the stale parquet was evicted, `make` failed on the rule-less target with three deliberate `No rule to make target` lines, and the country script ran. The sold-unit column is identified by value-label vocabulary: `sa3q11b`'s Stata value-label dict is identical to the harvest unit's in both waves (36 and 48 labels), pinned by test.

**3. Cache cost (stated, not paid on the shared cache)**

`nigeria.py` and `data_scheme.yml` are in every Nigeria table's L2-country hash and, via the `binp:` glob, every Nigeria L2-wave hash. All 24 Nigeria tables re-warm cold from L1; only `crop_production` changes content. `transformations.py` reaches no build fingerprint, so the #867 rider costs nothing. Only my private root was rebuilt.

**4. Surprises and CONTENTS.org**

Idiosyncrasies that bore on the task, from CONTENTS.org: `t` is the post-harvest quarter, not the wave directory; the sale has its own unit code while `u` is the harvest unit; W3–W5 sales sit in `secta3ii` at household-crop grain with no plot linkage (#843), so they carry no sold unit; `crop_production` is one of five rule-less Makefile targets (#866); and `_canon_unit` discards the Size axis. Nothing contradicted CONTENTS.org. The raw figures at lines 976–1000 reproduced to the row.

Surprises, both recorded in CONTENTS.org:
- The L2-country parquet stores `Quantity_sold` and `Value_sold` as `string` dtype, before and after this change. The API read coerces them to `Float64`, so nothing served is wrong, but anyone reading the cache parquet directly needs `pd.to_numeric`. Not fixed, out of scope.
- W1's variable label for `sa3q11b` reads "(11a).Quantity sold through buyer (unit)", a Stata varlabel slip. The value labels identify it.
- Served agreement is higher than the raw-code figure because the size-agnostic collapse folds big and medium sacks into one label, so those disagreements are invisible to the `Unit_sold != u` flag.
- `Olodo` appears in `Unit_sold` but never in `u` (W2 sold vocabulary only).

**5. Decisions**

- The #867 alias points `other inorganic fertilizer` at the existing `inorganic fertilizer` share of 0.20, not at `other fertilizer`'s 0.0. The label says inorganic, so the class share applies. This is a choice of which existing entry to alias, not a new number, and the comment says so. The NPS, D Compound and organic content decisions remain open on #867.
- No `Condition_sold` for Nigeria: the W1/W2 A3 files carry no sold-condition column.
- `Unit_sold` is a plain NA-bearing string column, never sentinel-filled, per the canonical note.