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

- Grain, `plot_id`, `i` per wave: `Ethiopia/_/CONTENTS.org` "plot_features" and
  "Household id emitted as i"; `holder_id` must be in every join key.
- `crop_production` columns: `lsms_library/data_info.yml` Columns.crop_production;
  `Ethiopia/_/data_scheme.yml` crop_production (all REPORTED fields).
- No silent aggregation in core; collapse selects the most complete row (GH #871):
  `CLAUDE.md` "Grain Collapse".
- Derivations: attachment of a reported holder-crop value to its unique plot is the
  §11 precedent and is not registered there; no value here is constructed.

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
| W4/W5 | no change | §11 verified to carry the crops |

## §6 Open questions for the human

- W1: serve §12 Q3 tree-crop harvest (6,134 pairs; 3,740 on a unique §4 field)? Blocks W1 tree-crop harvest and 1,679 W1 sales.
- Collisions: should §12 win where §11 is all-blank (W2 13, W3 7 pairs with a positive §12 value only)?
- Wave-keyed `harmonize_crop` for codes 19 / 86 (moves served `j`).
- §11 path's bare `.sum()` / "No" overwrite / blank-unit-as-Kg (46/9/0/9/8 zero groups).

---
### Phase 3 — verification

- `_attach_sect12_sales` — OK (anchored on §2 SALES GRAIN CAVEAT, §4 grain): cold API diff on development vs branch: identical index and non-sale columns; every development non-null sale cell identical; grain / null-read / null-index / quantity reports identical.
- `crop_production_for_wave(sale12=...)` — OK (§5): `sale12=None` reproduces development exactly (W4/W5 untouched; synthetic test).
- `harmonize_crop` 103/105 — OK (§4): no §9/§4 row in any wave uses either code.
- Not a REINVENTION of Tanzania's `_sales_block`: that sums across modules, which this deliberately does not.
