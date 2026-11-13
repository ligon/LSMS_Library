# Prior-Art Ledger — #820 Ethiopia §12 tree/fruit/root-crop sales into `crop_production`

**Search tier used:** ripgrep + git floor (the `gitnexus` MCP server failed to
connect this session; no index was read or refreshed).
**Baseline:** development `9a0ed5f3c`.

## §1 Task, restated

Ethiopia's `crop_production` (script path, `materialize: make`; one row per
§9 field-crop harvest, `(t, i, plot_id, j, u)`) attaches `Quantity_sold` /
`Value_sold` from the post-harvest §11 disposition module only.  In ESS1-ESS3
§11 excludes tree/fruit/vegetable/root crops by its own header; their
disposition is §12 (`sect12_ph_w{1,2,3}.dta`), which was unread.  Wire §12 as a
second SALE source in W1-W3 at the declared grain, without adding, dropping or
changing any served row other than filling empty sale cells; verify (not assume)
that W4/W5 §11 already carries those crops.

## §2 Existing machinery

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `crop_production_for_wave` | `lsms_library/countries/Ethiopia/_/ethiopia.py` (def) | §9 rows + §4 planting month + §11 sale attach (SALES GRAIN CAVEAT) | via build | extend: `sale12=`, `sale12_unit_labels=` kwargs |
| SALES GRAIN CAVEAT | `Ethiopia/_/ethiopia.py` comment block above `PERENNIAL_CROP_CODES` | holder-crop sale attached only where the pair is on exactly one plot | yes (implicitly) | reuse the rule for §12 |
| `_clean_unit_label` | `Ethiopia/_/ethiopia.py` | survey unit label -> `u` table label | `tests/test_ethiopia_shipped_factors.py` | reuse for W3 `ph_s12q0b` |
| `_eth_crop_label_map` / `harmonize_crop` | `Ethiopia/_/ethiopia.py`, `Ethiopia/_/categorical_mapping.org` | crop code -> `j` | yes | extend (+103, +105) |
| Malawi `_sale_block` / `assemble_crop_production` | `Malawi/_/malawi.py:1300`, `:1907` | Module I + Module Q (perennial) sales at household-crop grain, single-plot attach, "attach nothing and COUNT IT" | yes | precedent for counting unattached sales |
| Tanzania `_sales_block` | `Tanzania/_/tanzania.py:1222` | 5A + 7A + 7B sales, SUMMED across modules (`min_count=1`) | yes | precedent NOT followed for cross-module sum (brief forbids silent cross-module aggregation) |
| Burkina pre-write guard | `Burkina_Faso/2018-19/_/plot_inputs.py:137` (PR #1189) | `assert` declared index unique before `to_parquet` | yes | adapted: Ethiopia's index is already non-unique on development (GrainCollapseWarning 2/2/7/17), so the guard is structural (row count + row keys unchanged, attach frame unique) |

## §3 Definitions & conventions in force

- Derived values: `CLAUDE.md` "Derived Values" (the fit rule -- "a value served under a name implying a different window -- the function is the identity and the registry is the point"); field contract `lsms_library/derivations.py`; `Derivation` declared `str`, `optional: true` under `Columns: crop_production` in `lsms_library/data_info.yml` (verified), never in a country `data_scheme.yml`.

- Grain, `plot_id`, `i` per wave: `Ethiopia/_/CONTENTS.org` "plot_features" and
  "Household id emitted as i"; `holder_id` must be in every join key.
- `crop_production` columns: `lsms_library/data_info.yml` Columns.crop_production;
  `Ethiopia/_/data_scheme.yml` crop_production (all REPORTED fields).
- No silent aggregation in core; collapse selects the most complete row (GH #871):
  `CLAUDE.md` "Grain Collapse".
- Derivations: attachment of a reported holder-crop value to its unique plot is the
  §11 precedent and is not registered there.  Round 1 concluded "no value here is
  constructed"; round 2 REFUTED that as incomplete: the number is §12's answer, but
  its WINDOW (last completed production period / 12 months) is not the season of the
  row it is served on, so it is registered (`sect12-sale-period`) and stamped.

## §4 Invariants & assumptions

- §12 is keyed (household, holder, crop); no parcel/field (measured, all three waves).
- W1 §9 EXCLUDES tree/root crops; W2/W3 §9 INCLUDE them (questionnaire headers);
  W2/W3: every §12 pair has a §9 row; W1: 6,134 of 6,999 coded pairs do not.
- W2/W3 §12 repeat (holder, crop) lines (113 / 17 groups) that disagree.
- Crop code 19 = CACTUS in W1/W2 labels, RED KIDNEY BEANS from W3 (pre-existing
  `harmonize_crop` drift; not changed).
- `harmonize_crop` folds 52/56/69 -> 'Leafy Greens': a fill on one folded row moves
  which row the core collapse serves (measured: one 2015-16 `harvest_month`).

## §5 Reuse decision

| quantity | decision (reuse / extend / new) | reason |
|----------|--------------------------------|--------|
| §12 sale attach | extend (`_attach_sect12_sales`) | same rule as §11; §11 path left byte-identical |
| §11/§12 collision | new rule: §11 wins, counted | keeps every pre-existing value; no precedent forbids or decides; never summed |
| repeated §12 pair | new rule: ambiguous, counted | Malawi "attach nothing and COUNT IT"; a sum is a construction |
| Q6=No + blank | reuse §11 reading (0) | survey answer "did not sell" |
| W1 §12 harvest (Q3) | not served; open decision | needs a plot key §12 lacks; no precedent |
| W4/W5 | no change | §11 verified to carry the crops; their 12-month permanent-crop window is unmarked -- GH #1256, not stamped here |
| §12 window (round 2) | registered derivation `Ethiopia::crop_production::sect12-sale-period` | CLAUDE.md "Derived Values" fit rule: a value served under a name implying a different window; identity function `derive_sect12_sale`, inputs `inputs_sect12_sale` (`Ethiopia/_/derivations.yml`) |
| attached-then-dropped (round 2) | count as class 7; filter unchanged | ruling: report, do not change the drop filter in this PR |

## §6 Open questions for the human

- W1: serve §12 Q3 tree-crop harvest (6,134 pairs; 3,740 on a unique §4 field)? Blocks W1 tree-crop harvest and 1,679 W1 sales.
- Collisions: should §12 win where §11 says Q1 sold = No but §12 records a positive sale?  W2 12, W3 7 non-repeated pairs -- every one a §11 "No", none a blank §11 line (the earlier wording here, "§11 all-blank", was wrong; round-2 review).
- The empty-row filter in `crop_production_for_wave` ignores `Value_sold`: it drops §12 class-7 lines (W2 299 / 22 positive / 3,909 Birr; W3 62 / 7 / 7,430) and, on development, §11 rows with a positive value (W1-W5: 0 / 1 / 2 / 27 / 32; 1,500 / 7,750 / 41,442 / 33,482 Birr).  Left unchanged by ruling; a fix moves served rows.
- Wave-keyed `harmonize_crop` for codes 19 / 86 (moves served `j`).
- §11 path's bare `.sum()` / "No" overwrite / blank-unit-as-Kg (46/9/0/9/8 zero groups).

---
### Phase 3 — verification

- `_attach_sect12_sales` — OK (anchored on §2 SALES GRAIN CAVEAT, §4 grain): cold API diff on development vs branch: identical index and non-sale columns; every development non-null sale cell identical; grain / null-read / null-index / quantity reports identical.
- `crop_production_for_wave(sale12=...)` — OK (§5): `sale12=None` reproduces development exactly (W4/W5 untouched; synthetic test).
- `harmonize_crop` 103/105 — OK (§4): no §9/§4 row in any wave uses either code.
- Not a REINVENTION of Tanzania's `_sales_block`: that sums across modules, which this deliberately does not.
- `derive_sect12_sale` / `inputs_sect12_sale` / `Ethiopia/_/derivations.yml` — OK (anchored on CLAUDE.md "Derived Values" and the GhanaLSS 9B / EHCVM precedents): loader accepts the entry; both callables resolve; `tests/test_derivations.py` drift test passes in both directions; the stamp equals the §12-filled cells row for row (9,502 served rows) and the served values are identical to `f7f567c82` except the new column.
- `_report_sect12_sales` — OK (§4): asserts the eight classes partition the lines read; class 7 = W2 299, W3 62 lines, matching the independent oracle's "attached_lines_no_served_row".
- `median_price_valuation` default pools across waves (GH #1254): not touched (framework).
