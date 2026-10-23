# Prior-Art Ledger — GH #824 (Mali half) + GH #1267: the SOLD unit, and the tree-crop window, on Mali `crop_production`

**Search tier used:** ripgrep + git (floor).  The `gitnexus` MCP server failed
to connect this session (`CONNECTION_CLOSED`); per CLAUDE.md "Code
intelligence: GitNexus is OPTIONAL" the declared substitute (`rg` over call
sites) was used, and the blast radius is reported below.
**Inherits:** `.coder/ledger/STANDING.md` §2/§3/§4 -- cited, not restated.
**Base commit:** `9a0ed5f3c` (development), branch
`fix/824-mali-unit-sold-20261023`.
**Measurement environment:** worktree `824-ml` and a detached base worktree
at `9a0ed5f3c`, each with its own fresh `LSMS_DATA_DIR` holding only a
`dvc-cache` symlink to the shared L1; `PYTHONPATH` pinned to the checkout and
the import asserted by real path.

---

## §1 Task, restated

Mali `crop_production` is a script-path table (`materialize: make`) built by
two EAC-I wave scripts (`2014-15/_/crop_production.py`,
`2017-18/_/crop_production.py`) through `mali.crop_production_finalize`, and
concatenated by the country script.  Index `(t, i, plot_id, crop)`; `u` is a
COLUMN (the harvest unit).  Two defects, one Mali re-warm:

1. **#824 (Mali).**  Every EAC-I sale block records the sold quantity in its
   OWN unit variable (2014-15 `s3aq23b` / `s3bq13b`; 2017-18 `s7gq21b` /
   `s11fq14b`), which the scripts never read.  Carry it as the REPORTED,
   NA-bearing column `Unit_sold`, canonicalised at BUILD time through the same
   `u` table as the harvest unit.
2. **#1267.**  The 2014-15 tree rows (module 3B) serve `Quantity = s3bq09`,
   the NUMBER OF HARVEST MONTHS.  3B does record a harvest quantity --
   `s3bq10a`, the AVERAGE PER HARVEST MONTH (3B.10), unit `s3bq10b` -- so
   serve it.  Its window (per harvest month) differs from the field-crop
   rows' campaign total, as does `Quantity_sold` (`s3bq13a`, 3B.13, monthly),
   while `Value_sold` (`s3bq14`, 3B.14 "montant total") is a TOTAL.  2017-18
   trees (11F) already serve the monthly harvest `s11fq11a`, with a monthly
   sale pair (`s11fq14a`, `s11fq15`).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `crop_production_finalize` | `Mali/_/mali.py:338` | label maps, dtype coercion, the script's own `(t,i,plot_id,crop)` duplicate collapse | via the built table | **extend** -- carry `Unit_sold` / `months_harvested`; one-sold-unit gate in the collapse |
| `_unit_labels` | `Mali/_/mali.py:314` | decoded label -> `u` table Preferred Label | via the built table | **reuse** -- the SAME call canonicalises `Unit_sold` |
| `u` table | `Mali/_/categorical_mapping.org` (`#+name: u`) | Code (raw label) -> Preferred Label | `test_categorical_mapping_resolution.py` | **extend** by one spelling (`Chargement de camion`, 2017-18 11F) |
| Niger 2021-22 sale key | `Niger/2021-22/_/crop_production.py:89-107` | sale summed over `(i, crop, Unit_sold)`; attributed to a single-plot crop sold in ONE unit | `tests/test_niger_sold_unit.py` | **reuse the pattern** for 2017-18 `s7g` (household x crop grain) |
| Niger `_finish_crop_production` | `Niger/_/niger.py:537-563` | `Unit_sold` NA-bearing, never sentinel-filled | as above | **reuse the rule** |
| `Unit_sold` canonical note | `lsms_library/data_info.yml` (`Columns > crop_production > Unit_sold`) | contract: REPORTED, NA-bearing, disagreement is a FLAG | `test_schema_consistency.py` | **reuse** (no edit) |
| `Derivation` + registry | `lsms_library/derivations.py`; `data_info.yml` `Columns > crop_production > Derivation` (optional) | per-row key of a construction; identity derivations for a window rename are in-scope by CLAUDE.md "Derived Values -- the fit rule" | `tests/test_derivations.py` | **reuse** -- two keys, one per wave |
| `GhanaLSS::crop_production::9b-sale-amount-per-unit` | `GhanaLSS/_/derivations.yml:213` | the one registered crop-sale derivation (per-unit amount -> total) | `test_ghanalss_*` | **precedent only** -- shows a crop-sale window/denominator fact is registered, not hidden |
| `Mali/2018-19/_/mapping.py:inputs_food_acquired` | `:19` | an `inputs` callable template | `test_derivations.py` | **template** for the two new `inputs_*` callables |

**Did NOT reinvent:** a unit table (the `u` table is reused); a sale-unit
reducer (the Niger key + gate is copied, not generalised); a price rule
(`median_price_valuation`'s `Unit_sold` rule is a decision -- out of scope).

## §3 Definitions & conventions in force (cited)

- **`Unit_sold`**: `data_info.yml` `Columns > crop_production > Unit_sold` --
  the unit the SALE was reported in, a column beside the harvest `u`;
  NA-bearing; a disagreement with `u` is a FLAG, not an identification.
- **Build-time canonicalisation**: GH #1188's point -- read-time categorical
  auto-apply touches only the column/level named `u`, so `Unit_sold` must be
  canonicalised in the build (`Niger/_/CONTENTS.org`, "crop_production.u is
  the HARVEST unit"; `Niger/_/niger.py:553-563`).
- **Derived values**: CLAUDE.md "Derived Values" -- "a value served under a
  name implying a different window ... the function is the identity and the
  registry is the point"; "a rule that genuinely differs by wave is a
  different name"; `Derivation` is declared canonically and must NOT appear in
  a `data_scheme.yml` (`test_nothing_in_a_data_scheme_names_a_derivation`).
- **`optional: true`**: exempts a column from Site B (CLAUDE.md "The Silent
  All-Null Read") -- right for `Unit_sold` (NA on every unsold row) and
  `months_harvested` (NA on every field-crop row).

## §4 Invariants & assumptions

- **Mali idiosyncrasies (Mali/_/CONTENTS.org)**: 2014-15 and 2017-18 are
  EAC-I, not EHCVM; the 2017-18 household key is `exploitation`
  (`ménage` in the 11F tree file, renamed in the script); `u` is a COLUMN,
  not an index level, so a missing harvest unit is NA, not `Unknown`.
- **Rows must not move.**  Only `Quantity` on 2014-15 tree rows changes value;
  every other pre-existing column and every row is identical, except the two
  2017-18 tree rows whose `u` was NA only because `Chargement de camion` was
  missing from the `u` table.
- **2017-18 `s7g` is household x crop** (0 duplicate `(grappe, exploitation,
  s7gq01)`): the sale attaches only to a single-plot crop; the Niger
  `(i, crop, Unit_sold)` key and one-unit gate are inert today and kept as
  the grain they name.
- **Codes are not comparable across the two 2017-18 unit label sets**: code
  10 is `Autre` on the harvest set (`s7fq13c`) and `Tonne` on the sold set
  (`s7gq21b`; code 11 is `Autre`).  Units are decoded from each variable's
  OWN value labels and compared by LABEL.
- **Every Mali table's cache hash moves** (edits to `mali.py`,
  `data_scheme.yml`, `categorical_mapping.org`); no other country's should.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| `Unit_sold` | reuse `_unit_labels` + Niger key/gate | the contract and the precedent already exist |
| 2014-15 tree `Quantity` | `s3bq10a` (recorded) | 3B records a harvest quantity; serve it, with its unit `s3bq10b` (#1267 "if it does, serve it") |
| tree `Quantity_sold` | recorded monthly value (`s3bq13a` / `s11fq14a`) | serve what was recorded; the period is stated by a registered identity derivation per wave, never by a fabricated 12-month total (brief) |
| `months_harvested` | new optional column (`s3bq09` / `s11fq10`) | the one number that converts a per-month figure to a 12-month one; named away from `harvest_month` (month-of-year) |
| window disclosure | two registry keys, `3b-per-harvest-month` (2014-15) and `11f-per-harvest-month` (2017-18) | the column sets on the window differ by wave (2014-15 `Value_sold` is a total; 2017-18's is monthly) |

## §6 Open questions for the human

- Whether to serve 12-month tree quantities (`Quantity * months_harvested`)
  as a registered construction (precedent: GhanaLSS `9b-sale-amount-per-unit`;
  the WB `MLI_EACI2.do:242/313/353` does exactly this for 2017-18) -- the brief
  forbids it here.
- `median_price_valuation`'s `Unit_sold` rule, and whether it should skip rows
  keyed `3b-per-harvest-month` (2014-15 tree `Value_sold / Quantity_sold` is
  NOT a price) -- a decision, out of scope.
- A `KgFactor_sold` column from `s3aq23c` / `s3bq13c` / `s7gq21d` /
  `s11fq14c` -- a decision; coverage measured in the evidence README.
- Harvest `u = 'Autre'` in 2017-18 is tonne-sized on most rows (survey factor
  1000 kg) -- re-labelling it is a decision.

---
### Phase 3 — verification (fill at task end)
