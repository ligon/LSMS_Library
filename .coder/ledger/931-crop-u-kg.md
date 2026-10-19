# Prior-Art Ledger — GH #931: the crop kg ladder reads the declared `u_kg`

**Search tier used:** ripgrep+git floor (the gitnexus MCP server did not connect).

## §1 Task, restated
`transformations.harvest_kg_factors` (the CROP kg ladder behind `harvest_kg` /
`yield_kg`) was blind to a country's declared unit masses — the merged `u` +
`u_kg` categorical tables that `Country._unit_kg_factors` composes and that the
FOOD ladder (`food_kg_factors`) receives as `unit_kg` (GH #919).  Ruling
(@ligon, 2026-10-18): the crop ladder reads it; Ethiopia's quintal check on the
WB shipped table is re-expressed against an external answer key in the same
change.  The two ladders stay separate (`KG_FACTOR_LAYERS` vs
`FOOD_KG_FACTOR_LAYERS`); nothing is stored — read-time only.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country._unit_kg_factors` | `lsms_library/country.py` (~2292) | raw/canonical `u` label → kg from merged `u` + `u_kg` | `tests/test_gh919_country_qualified_units.py` | **reuse** — the crop record is exactly its output |
| `_seeded_kg_factors(unit_kg=)` | `transformations.py` (~1692) | declared factors outrank KNOWN_METRIC + label parser | `test_food_kg_inference.py` | **reuse the precedence**, not the call (per-row overlay; equivalence pinned) |
| `harvest_kg_factors` | `transformations.py` | crop ladder: reported > shipped > survey_median > metric > inferred > none | `test_crop_kg_factor.py`, `test_shipped_factors.py` | **extend** (`unit_kg=`, `kg_declared`, `declared` count) |
| `df.attrs['country']` | `country.py` `_finalize_result` | country identity on a Country frame; read by `conversion.convert` | `test_conversion*` | **reuse** to pick the table on a single-country frame |
| `feature._ATTRS_CARRIERS` | `feature.py` (~107) | re-attach per-country attrs records after the concat | `test_feature_attrs_carriers.py` | **extend** — one entry |
| `recall.merge_attrs` | `recall.py:396` | union of `{country: ...}` records | yes | **pattern** for `merge_unit_kg_attrs` |

## §3 Definitions & conventions in force
- `u_kg`: canonical unit → kg, cross-country default + per-country override,
  row-unioned (`_ADDITIVE_CATEGORICAL_TABLES`); `lsms_library/categorical_mapping/u.org` "Unit -> kilograms".
- A missing unit is `U_UNKNOWN`; the kg ladder serves it `none` (#1041 ruling).
- attrs survive an op only when every input agrees (CLAUDE.md "Panel ID Transitive Chains").
- `KgFactor` is REPORTED, never constructed; the constructed factor is `kg_per_unit` (`data_info.yml`, crop_production).

## §4 Invariants & assumptions
- `resolved` reads `inferred_arr`, which must carry the read seeds with
  precedence — so the declaration is laid over BOTH `metric` and `inferred_arr`
  (`harvest_kg_factors`, GH #929 comment).
- Layer order of `KG_FACTOR_LAYERS` must not change; the declaration folds into
  `metric` (as the food ladder folds it into its read layer).
- `_finalize_result` is in `_EXCLUDED_CALLABLES` and no `transformations`
  symbol may reach a build fingerprint (`test_no_transformations_symbol_reaches_any_fingerprint`).
- Crop frames carry no `Expenditure`, so `_get_kg_factors` there is the seeds
  alone; per-row overlay ≡ threading `unit_kg` into the seeds
  (`test_crop_declared_overlay_equals_food_seeding`).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| declared kg per unit | reuse `_unit_kg_factors` | one composition for both ladders |
| per-row declared factor | new `_declared_kg_factors` | a pooled `Feature` frame needs one table per country; `_seeded_kg_factors` takes one dict per frame |
| carrying it to the transform | new attrs record `unit_kg` `{country: {u: kg}}` | `harvest_kg` is analyst-called with no Country; importing Country into `transformations` is the hazard ledger `harvest-kg-shipped-factors.md` §6 names |

## §6 Open questions for the human
- `plot_inputs` consumers (`nitrogen_kg`, `seed_kg`) and `median_price_valuation`'s
  sold-kg path still call `_kg_factor_series` blind to `u_kg` (residual).
- A frame that loses its attrs (a `merge` with a frame whose attrs differ) goes
  blind silently except for `attrs['kg_factor_declared_from'] is None`.

---
### Phase 3 — verification
- `_declared_kg_factors` — OK (§5, §4): precedence equals `_seeded_kg_factors(unit_kg=)`, pinned.
- `harvest_kg_factors(unit_kg=)` — OK (§4): layers unchanged; 15 of 16 crop countries byte-identical in `kg_per_unit`; Ethiopia 13,977 Quintal rows `none` → `metric`.
- `Country._finalize_result` stamp — OK (§4): 0 of 251 probed hashes moved.
- `feature._ATTRS_CARRIERS['unit_kg']` — OK (§2): delegates to `transformations.merge_unit_kg_attrs`.
