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

- `crop_production_finalize` (`Unit_sold` canonicalisation, one-unit gate) --
  OK (anchored on §2 `_unit_labels`, §3 build-time canonicalisation, §2 Niger
  key/gate): the same `_unit_labels` call as `u`; the gate mirrors
  `Niger/2021-22/_/crop_production.py:93-105` and is inert (0 groups).
- 2017-18 `s7g` sale block -- OK (§2 Niger 2021-22 pattern; §4 rows must not
  move): keyed `(i, crop, Unit_sold)`, `sum` kept at `min_count=0` so no
  `Quantity_sold` / `Value_sold` value moves (the NA->0 fill it preserves is
  reported, not fixed).
- `derive_3b_per_harvest_month` / `derive_11f_per_harvest_month` + registry
  keys -- OK (§3 derived values: identity for a window rename; one name per
  wave rule): no 12-month construction; `Derivation` not in `data_scheme.yml`.
- `months_harvested` -- OK (§5): reported, declared optional, NA on field rows.
- `u` table row `Chargement de camion` -- OK (§2 extend): 2 harvest rows gain a
  label they always had.
- 9999.9 sentinel -- OK (§4, explained diff): extends the function's own
  99/9999 rule; 1 `Quantity` (NA before and after) and 1 `Quantity_sold`.
- No REINVENTION found: no new unit table, no new reducer, no price rule.
- Cache: hash census Mali-only (24/24 country, 96/96 wave, 0/40 btf).

---
## Round 2 — maintainer rulings on PR #1272 (@ligon, 2026-10-23)

Supersedes §5 rows 2-3 and §5's "window disclosure", and closes §6 bullet 1.

### §5' Reuse decision (round 2)

| quantity | decision | reason |
|---|---|---|
| tree `Quantity`, `Quantity_sold` | per-month answer x `months_harvested`, both waves, one key `Mali::crop_production::tree-twelve-month-total` | ruling 1; precedent GhanaLSS `9b-sale-amount-per-unit` (a registered construction under the table's own name, two waves one key) and `MLI_EACI2.do:242/313`; CLAUDE.md "Derived Values" (a derived value is first-class; same rule, same name) |
| tree `Value_sold` | as recorded, both waves | ruling 1: `s3bq14` is the total; for 2017-18 the instrument decides, and 11F.15 is printed word for word 3B.14 ("montant total de cette vente") -- the Stata label "mensuelle moyenne" disagrees (recorded in the registry and CONTENTS) |
| edge rows | missing codes masked before the product; 0 a month is 0; months NA -> NA; months 0 beside a positive answer -> NA | ruling 1 "decide from the data": a 0 quantity beside a recorded sale value is a manufactured zero |
| 2017-18 `s7g` sale | `sum(min_count=1)` (cython), 0 only on `s7gq20 == 'Non'` | ruling 2; #1250 / #1253 / #1263; CLAUDE.md #871 (no per-group callable) |

### Phase 3' — verification (round 2)

- `derive_tree_twelve_month_total` / `tree_crop_rows` -- OK (§5'): raw oracle
  358/358 (2014-15) and 270/270 (2017-18) on Quantity, Quantity_sold,
  Value_sold and months_harvested, written independently of `mali.py`.
- `crop_production_finalize` sentinel rule now field-rows only -- OK: tree
  per-month answers are masked before the product; 0 tree totals equal 99 /
  9999 today, and a test pins that a tree 99 survives.
- 2017-18 `s7g` `sum(min_count=1)` -- OK: raw oracle 24,449/24,449 on both
  sale columns; served zeros 10,460 -> 8,807 / 10,468 -> 8,806.
- The 1,607 'Oui'-without-quantity rows: all Leger (`typequest`); Leger's s7g
  carries no 7G.21 / 7G.22 at all.  Recorded in CONTENTS (both the
  crop_production and the sampling sections).
- NOT corroborated, recorded as WAITING: within crop x unit cells,
  value / per-month quantity sold does not rise with the month count (slope
  -0.05 se 0.30 in 2014-15; -0.52 se 0.47 in 2017-18), which rejects the
  mixed-window pairing the ruling serves; it cannot separate both-monthly from
  both-total.
- Cache: hash census vs development Mali-only (see the evidence README).

---
## Round 3 — tree `Value_sold` scaled (@ligon, 2026-10-23)

Supersedes §5' row "tree `Value_sold` | as recorded".

| quantity | decision | reason |
|---|---|---|
| tree `Value_sold` | per-month value x `months_harvested`, both waves; added to `tree-twelve-month-total`'s `columns` | ruling round 3, on an order-of-magnitude check (cashew 275 / 500 FCFA/kg matches published farm-gate prices only on this reading; multi-month rows at 2-3% of retail and bananas at 0.4 kg/plant/year on the total reading); the slope test bounds total-reporters at ~55% / ~40% |
| edges on the value | same rule as the quantities: 0 -> 0; months 0 or NA beside a positive value -> NA | ruling round 3 |
| 999999 on the value | masked before the product, tree rows only | 2014-15's six-digit field maximum (nothing exceeds it; 7 of 12 field rows carrying it sold 0), so a code; x 2 it would read 1,999,998. Not in the ruling: reported as a deviation |

Phase 3'' -- oracle 358/358, 270/270 on `Value_sold`; price == per-month
ratio on 190/190, 127/127; warnings identical to e95040028; hash census
Mali only.  The printed-form conflict ("montant total de cette vente") stays
recorded in CONTENTS with the sources that would settle it.

---
## Round 4 — the tree sale served as recorded (revised ruling @ligon 2026-10-23)

Supersedes round 2's §5' row "tree `Quantity`, `Quantity_sold`" for the sold
quantity, and round 3's row "tree `Value_sold`".  Base: development
`57d255585` (round 3 merged), branch
`fix/mali-tree-sales-as-recorded-20261023`.

### §2'' Prior art reused

| symbol | path | reuse |
|---|---|---|
| `Ethiopia::crop_production::sect12-sale-period` (#1257 / #820) | `Ethiopia/_/derivations.yml` | **the precedent** -- identity derivation naming a window; structure of the entry (window rule, questionnaire + modelling-choice assumptions, "nothing rescales it" validation) |
| `Ethiopia::crop_production::permanent-crop-12-month-window` (#1274 / #1256) | same | the identity function shape (`derive_permanent_crop_window` returns its inputs as a frame) |
| `Ethiopia/_/ethiopia.py:_no_sale_reading` | | the edge rule: an answer beside a contradicting flag is kept as reported -- applied to a sale beside a 0 / missing month count |
| `derivations.MULTI_SEP` / `union_keys` / `_key_counts` | `lsms_library/derivations.py` | two keys on one row: `'+'`-joined, sorted; `_key_counts` already splits it, so `attrs` counts each key.  `datetime_parse.assign_datetime` already writes the same joined form; `rg Derivation lsms_library/*.py` finds no consumer that compares the column to a single key |

### §5'' Reuse decision

| quantity | decision | reason |
|---|---|---|
| tree `Quantity` | unchanged: per-month harvest x `months_harvested`, `tree-twelve-month-total` (columns now `[Quantity]`) | ruling: the form and the 2017-18 manual ask the harvest per month |
| tree `Quantity_sold`, `Value_sold` | AS RECORDED (codes masked: 99 / 9999 / 9999.9 on the quantity, 999999 on the value), new identity key `Mali::crop_production::tree-sale-window` on tree rows with a positive sale | revised ruling; manuals say "total" for the value, the price levels say the value sits on the per-month quantity's footing; the instrument cannot settle month vs season |
| a sale beside a 0 / missing month count | served as recorded (6 rows, 2014-15) | the month count no longer enters the sale; Ethiopia `_no_sale_reading` precedent |
| `inputs` callables | split: `inputs_tree_twelve_month_total` returns the harvest variables, `inputs_tree_sale_window` the sale variables (shared reader `_tree_roster_inputs`); `raw_variables` partition the roster's measured answers | each key returns what it derives from |
| the stamp | `mali.stamp_tree_derivations(df)`, called by both wave scripts after the collapse | one implementation of the two-key rule |

### Phase 3'''' — verification

- Cold builds (fresh `LSMS_DATA_DIR` each), development `57d255585` vs the
  branch: index identical; only tree `Quantity_sold` (137 / 100 rows),
  `Value_sold` (131 / 99) and `Derivation` (331) move; `Quantity`, units,
  `months_harvested` and every field row identical.
- Raw oracle (`out/r4/oracle_r4.txt`, written from the raw rosters): every
  tree column and the `Derivation` value 357 / 357 and 270 / 270; development
  fails it on 137 / 131 and 100 / 99 rows of the sale columns.
- Price: identical on all 190 / 127 rows priced on both sides; 2014-15
  priced tree rows 190 -> 196.  **2014-15 cashew per-Kg median moves 275
  (n=12) -> 300 (n=13)**: one returning edge row is a cashew sale in Kg.  No
  price moved; the median of a larger set did.
- Downstream: `harvest_kg` / `yield_kg` frame-equal; `gross_crop_revenue`
  2014-15 1.127e8 -> 7.738e7, 2017-18 2.332e8 -> 1.619e8 (173 households, 171
  down, 2 up).  Site Q offender lists byte-identical.
- Hash census vs development: Mali only.
- Tests: the two files fail 14 + 5 errors on development, pass on the branch.

### §6'' Open

- `median_price_valuation` / `gross_crop_revenue` read tree `Value_sold` as a
  campaign sale; under the revised ruling it is the recorded (month-or-season)
  value.  Whether the framework should rescale or skip `tree-sale-window`
  rows is a framework decision, out of scope.

---
## Round 5 — red-team follow-up on round 4 (2026-10-23)

Red team on `9fbfdae86`: fit for a PR with fixes.  Base still development
`57d255585`.  All cold builds, tests, mutants and the hash census ran on Slurm
(burst runner, job dir `jobs/mali-sales-r5/`), per AGENT_RULES.

### §2''' Prior art reused

| symbol | path | reuse |
|---|---|---|
| `_LIVESTOCK_SENTINELS`, `_LABOR_SENTINELS` | `Mali/_/mali.py` | **in-country precedent**: 999999 already masked on Mali's other FCFA columns (livestock `SalesValue`, plot-labour wage) |
| WB `MLI_EACI1.do:645` | reference copy | same wave masks the seed value at `>= 999998` |
| instrument convention | AGR P1 p. 8 (`9998` NSP / `9999` Non declare); MEN P1 pp. 18, 22, P2 p. 11 (`Si NSP INSCRIRE 999998`) | the reserved values of a field |
| red-team `mutate.py`, `idleak.py`, `build_all.py`, `compare.py` | `jobs/mali-sales-review/scripts/` | adapted into `scripts/r5/` (anchors updated, mutants I-L added) |
| AGENT_RULES count-only asserts | `burst/AGENT_RULES.md` | every TestServed assert compares a local count |

### §5''' Decisions

| quantity | decision | reason |
|---|---|---|
| 2014-15 field `Value_sold` = 999999 (12 rows) | masked (NA) | the questionnaire convention + data (7 of 12 beside a zero sale) + the two precedents above; coordinator: "if the questionnaire convention shows ... mask it there as well" |
| tree / field `Value_sold` = 999998 (1 tree row) | masked (NA) — **scope deviation, reported** | the instrument's NSP value for a 6-digit amount; the row (cashew, 55 Kg, missing month count) prices at 18,182 FCFA/kg against a cell median of 275, and it alone moved the 2014-15 cashew per-Kg median 275 -> 300 and `median_price_valuation`'s Kg-only prices of 71 rows |
| window mismatch on the row | disclosed (registry rule, CONTENTS, a pinned test), not changed | ruling: harvest x months, sale as recorded |
| harvest 0 beside a positive sale (6 + 1 rows) | documented, not changed | coordinator item 7 |
| test hygiene | count-only asserts AND a quiet-repr fixture wrapper | the asserts alone did not stop the leak: pytest prints a failing test's arguments (the fixture frames) — measured on the mutants |

### Phase 3''''' — verification (numbers in the evidence README, round 5)

- All 23 Mali tables cold, base vs branch: 22 identical (values, index,
  warnings text, attrs); `crop_production` moves only tree `Quantity_sold`
  (137 / 100), `Value_sold` (tree 130 / 99, field 12) and `Derivation` (331).
- Prices: 2014-15 cashew per-Kg back to 275 (n = 12); `median_price_valuation`
  moves 0 rows in both modes (71 at `9fbfdae86`).
- Hash census: Mali only (24 / 24, 96 / 96, btf 0 / 40).

### §6''' Open (still)

- test_derivations' `TestDelivered` builds GhanaLSS and its Site Q warning
  (framework text, by design) prints household ids into pytest's warnings
  summary of a PASSING run; framework, out of scope here.
