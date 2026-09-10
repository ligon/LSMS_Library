# Prior-Art Ledger — GH #824 (Uganda half): carry the SOLD unit on `crop_production`

**Search tier used:** ripgrep + git (floor).  The `gitnexus` MCP server failed to
connect this session (`CONNECTION_CLOSED`), and this mirror has no `.gitnexus/`
index; per CLAUDE.md "Code intelligence: GitNexus is OPTIONAL" the declared
substitute (`rg` over call sites, blast radius reported) was used instead.
**Inherits:** `.coder/ledger/STANDING.md` §2/§3/§4 — cited, not restated.
**Base commit:** `a73437fd`, branch `feat/824-uganda-sold-unit`.
**Measurement environment:** worktree `tmp/wt-824-uganda`, isolated
`LSMS_DATA_DIR=<worktree>/.data` with only `dvc-cache` symlinked to the shared
L1, `LSMS_COUNTRIES_ROOT` and `PYTHONPATH` pinned to the worktree.  The shared
cache root was never written and `lsms-library cache clear` was never run.

---

## §1 Task, restated

Uganda's `crop_production` is a script-path table (`materialize: make`) built by
`uganda.crop_production_for_wave` at the declared grain
`(t, i, plot, j, u, condition, season)`.  `u` is the HARVEST unit (UNPS q6c).
The UNPS post-harvest module asks question 7 as a compound question — "how much
of the harvest was SOLD, in what CONDITION and in what UNIT" — so `Quantity_sold`
is denominated in q7c and `Value_sold / Quantity_sold` is a price per q7c, not
per `u`.  `CROP_COLMAPS` read neither q7c nor q7b.  This task wires both as
REPORTED columns (`Unit_sold`, `Condition_sold`), declares them in Uganda's
`_/data_scheme.yml` and in the canonical `lsms_library/data_info.yml`
`crop_production` block, and changes no row and no existing column.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `crop_production_for_wave` | `Uganda/_/uganda.py:1194` | builds the wave table from AGSEC5A/5B + AGSEC4A | `test_uganda_crop_condition.py`, `test_uganda_99999.py` | **extend in place** — two more columns off the row that is already read; no new function, no new call site |
| `CROP_COLMAPS` | `Uganda/_/uganda.py` | per-wave source column map | as above | **extend** — `unit_sold` / `condition_sold` keys beside the existing `unit` / `condition` |
| `_require` / `CropColmapError` | `Uganda/_/uganda.py:1163` | a NAMED column that does not resolve raises instead of silently falling back | yes | **reuse** — both new keys go through it |
| `_harvest_unit_map` / `_harvest_condition_map` | `Uganda/_/uganda.py:1142` | `harvest_units` (46 codes) / `harvest_conditions` (20 codes) org tables | via the built table | **reuse** — the sold unit is canonicalised through the SAME table as `u`, the sold condition through the SAME table as `condition` |
| `reduce_to_agreed` | `lsms_library/build_transforms.py:422` | "lossless or loud" collapse over a duplicated index: agreed value kept, conflicted cell set NA + `GrainConflictWarning`, escalated to a raise by `LSMS_GRAIN_STRICT=1` | yes (GH #323) | **reuse** — a first draft of this task hand-rolled exactly this; caught in review and replaced.  Uganda's own `cluster_features` hook (`uganda.py:226`) is the sibling call site |
| `_sale_block` / `assemble_crop_production` | `Malawi/_/malawi.py` (commit `85947ff5`) | the Malawi half of #824 | yes | **DO NOT transfer** — see §5 |
| `harvest_kg_factors` | `transformations.py:2179` | per-row kg factor + provenance | yes | **not used in shipped code** — consulted for the magnitude measurement, then set aside (see §6) |

**Did NOT reinvent:** a bespoke agreement-wise reducer (`reduce_to_agreed`
exists); a unit-label kg parser in library code (nothing here converts); a
`KgFactor_sold` layer (out of scope, §6).

## §3 Definitions & conventions in force (cited)

- **Canonical schema** = `lsms_library/data_info.yml`, `Columns:` section
  (STANDING §3, CLAUDE.md "Canonical Schema").  It had no sold-unit column
  before this task; declaring `Unit_sold` / `Condition_sold` there is part of
  the work, in the register of the existing `KgFactor` note.
- **`u` sentinel rule**: `data_info.yml` Columns preamble — "A survey that
  records an amount and no unit must put `Unknown` on `u`, NOT a null…  a NaN
  on a declared index level is a DEFERRED SILENT DELETION".  The rule is
  explicitly about DECLARED INDEX LEVELS.
- **`condition` vocabulary**: `data_info.yml` `Columns > crop_production >
  condition > spellings` — 21 tokens, seeded from Uganda's UNPS scheme
  (#323/#637).
- **`KgFactor` register**: `data_info.yml` `Columns > crop_production >
  KgFactor` — "REPORTED, NEVER CONSTRUCTED", plus an explicit consumer rule.
  The two new notes follow it.
- **Unit / condition identification rule**: `Uganda/_/CONTENTS.org` §"Which
  source column is the unit and which is the condition" — decided by the
  VALUE-LABEL vocabulary and the VALUE RANGE, never by the variable label.
- **`optional: true`**: exempts a column from the Site-B null-content guard
  (CLAUDE.md "The Silent All-Null Read"), read through
  `_required_scheme_columns`.

## §4 Invariants & assumptions

- **Rows must not move.** #842 verified against a rows-unchanged invariant and
  #861 changed the row count; this task must change neither.  Verified: 130,606
  rows before and after, index identical, every pre-existing column equal
  (`Quantity` 7,309,085.96; `Quantity_sold` 18,134,251.40; `Value_sold`
  10,709,007,434.04; `KgFactor` 958,552.32 — all byte-identical), `u=='Unknown'`
  counts unchanged in every `(t, season)`.
- **`u` and `condition` are INDEX levels; `Unit_sold` and `Condition_sold` are
  COLUMNS.**  The `Unknown` / `unknown_condition` sentinels exist only because
  `groupby(dropna=True)` deletes a null index key.  A column has no such
  hazard, so these two are NA-bearing — and NA carries information a category
  cannot ("this row records no sale").  (`uganda.py`, Returns docstring.)
- **The de-duplication block sums `Quantity_sold`.**  Summing across two
  different sold units yields a number in no unit; the label is therefore
  blanked, not guessed (§5).  11 groups of 130,606 corpus-wide.
- **2018-19 season A has no HARVEST unit at all** (#842) — so its agreement
  share is 0% by construction and is excluded from the agreement table and
  reported separately.
- **2015-16 `Value_sold` carries a ×100 fix applied in the wave script**
  (#829); the built table is at the right scale, so the price measurements
  need no wave exclusion.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| sold unit | **new column `Unit_sold`**, canonicalised through the existing `harvest_units` map | the sale's unit is a per-row attribute of the same source row; a second index level would split every row and duplicate the harvest, and overwriting `u` would relabel the harvest |
| sold condition | **new column `Condition_sold`**, vocabulary a YAML **alias** of `condition`'s `spellings` (`&crop_condition_vocabulary`) | one source of truth; a hand-copied 21-entry list is a second source that drifts.  Every reader of `data_info.yml` is `yaml.safe_load` (verified by `rg`), and nothing writes the file back |
| the collapse | **reuse `reduce_to_agreed(on_conflict='na')`** | tested "lossless or loud" machinery with the exact contract wanted, and it inherits the `LSMS_GRAIN_STRICT` lever |
| Malawi's merge-on-`u` pattern | **NOT transferred** | Malawi's sale lives in a SEPARATE module at household-crop grain, so it had to be joined onto the harvest row and the honest fix was to join on `u`, making the ratio a price per `u` by construction — which is why Malawi declared no `Unit_sold`.  Uganda's sale is on the harvest row; there is nothing to merge, and the second unit is a fact about the row.  Both shapes are correct; the canonical `Unit_sold` note documents both so a future country picks the right one |
| a sold kg factor (`a5?q7d`) | **deferred** | ADDS a column and needs its own before/after, exactly as `kg_factor` did (#849).  Natural name `KgFactor_sold`; recorded in `uganda.py` and CONTENTS.org |

## §6 Open questions for the human

- **`KgFactor_sold` (`a5?q7d`, "conversion factor into KG of quantity/unit
  sold")** is shipped from 2011-12 on and is a NON-CIRCULAR kg basis for the
  sold unit — the one thing that would let a per-kg farmgate price be formed
  for 2018-19 season A without going through a unit label.  Wire it next?
- **`harvest_kg_factors` does not resolve most Uganda unit labels to kg**: of
  1,617 rows where the sold and harvest units differ, only 26 got a factor for
  BOTH labels through that path.  The magnitude figures below therefore use a
  documentation-only nominal weight read out of the label text itself
  ("Sack (100 kgs)" → 100), which resolves 1,078 of the 1,617.  That gap is a
  separate finding about the kg machinery's coverage on crop units, not
  something this task changes.
- **`reduce_to_agreed`'s warning does not survive the cache.**  It files no
  grain report and nothing stamps it into the wave parquet, so the 11
  conflicted groups are announced on the cold build and are silent on every
  warm read — the GH #323 "the signal must survive the cache" lesson,
  unaddressed.  This is a property of `reduce_to_agreed` (Uganda's
  `cluster_features` hook has the same gap), not of this change.  Worth its
  own issue?  Related: under `LSMS_GRAIN_STRICT=1` those 11 groups make the
  Uganda build fatal — correct behaviour, and nothing in `tests/` builds
  Uganda `crop_production` with the lever set (checked).
- **2010-11 AGSEC5A `a5aq7c` is capped at code 20** in the shipped extract
  (evidence in §Phase 3).  Worth reporting upstream alongside the 2015-16 ×100
  defect (#829)?

---

### Measurements (isolated cold build, 2026-09-09)

**Coverage** — `Unit_sold` non-null as a share of sale rows (`Quantity_sold > 0`):

| wave | A | B | | wave | A | B |
|---|---|---|---|---|---|---|
| 2009-10 | 95.1% | 97.5% | | 2015-16 | 98.9% | 99.1% |
| 2010-11 | **57.2%** | 97.9% | | 2018-19 | 93.0% | 91.9% |
| 2011-12 | 99.8% | 99.7% | | 2019-20 | 96.4% | 95.3% |
| 2013-14 | 94.1% | 99.1% | | **all** | **93.9%** | |

`Condition_sold` is 98.1–100% on every cell except 2011-12 (0% — no q7b column
in the extract; asked on the questionnaire).  2010-11 season A's 57.2% is the
capped column, and its nullity is not random.

**Agreement** — `Unit_sold == u`, rows where both are known (`u != 'Unknown'`):
**96.0%** panel-wide (40,183 rows), by wave-season 92.0–98.0%; i.e. **4.0%
disagree**, close to the 2.0–10.4% CONTENTS.org recorded from the raw files.
Nigeria's 95–97% in #824 was Nigeria; Uganda's number is 96.0%.
2018-19 season A is excluded by construction (`u == 'Unknown'` on all 7,153
rows) and reported separately: its `Unit_sold` is non-null on 2,734 rows, so a
farmgate price for that wave-season is now reachable where it was not.

**Top sold units** (43 distinct): Kg 27.8%, Sack (100 kgs) 25.1%, Bunch
(Medium) 11.2%, Plastic Basin (15 lts) 10.8%, Tin (Debe) (20 lts) 6.1%, Sack
(120 kgs) 5.2%, Bunch (Big) 3.5%, Bunch (Small) 3.0%, Sack (50 kgs) 1.9%, Sack
(unspecified) 0.9%.

**Farmgate price implication** — median `Value_sold / Quantity_sold`, per crop,
over rows whose `Unit_sold` is the crop's dominant sold unit versus rows whose
`u` is that same unit; ten highest-`Value_sold` crops, `Unknown`/NA excluded
(2,734 priced rows have NA `Unit_sold`, 3,297 have `u == 'Unknown'`, of 45,519
priced rows):

| crop | dominant sold unit | n(Unit_sold) | median UGX / Unit_sold | n(u) | median UGX / u | ratio |
|---|---|---|---|---|---|---|
| Matoke | Bunch (Medium) | 4,130 | 6,000 | 4,182 | 5,333 | 0.889 |
| Coffee | Kg | 1,915 | 1,200 | 1,717 | 1,120 | 0.934 |
| Maize | Sack (100 kgs) | 3,175 | 50,000 | 3,106 | 50,000 | 1.000 |
| Beans | Kg | 2,934 | 1,000 | 2,722 | 1,000 | 1.000 |
| Sugarcane | Bundle (Unspecified) | 76 | 2,750 | 111 | 3,000 | 1.091 |
| Cassava | Sack (100 kgs) | 1,071 | 40,000 | 1,025 | 40,000 | 1.000 |
| Rice | Sack (100 kgs) | 258 | 120,000 | 245 | 110,000 | 0.917 |
| Ground Nuts | Sack (100 kgs) | 820 | 70,000 | 822 | 70,000 | 1.000 |
| Tomatoes | Plastic Basin (15 lts) | 226 | 10,628 | 239 | 10,000 | 0.941 |
| Soybean | Kg | 803 | 1,200 | 731 | 1,200 | 1.000 |

**1 of 10** moves by more than 10% (Sugarcane, 9.1% — nothing does).  **That
aggregate stability is the wrong place to look, and stating it alone would be
misleading.**  The medians barely move because only 3.5% of priced rows are
affected; on those rows the error is severe.  Of the 1,571 affected priced
rows, 1,057 carry a nominal weight in BOTH unit labels, and for those the
implied per-kg price was off by a factor with **median 4.0, p75 40, p90 100,
max 240** — **73.1% off by ≥ 2×, 26.7% by ≥ 100×**.  The biggest single cell is
`u = Sack (100 kgs)` with `Unit_sold = Kg` (169 rows, a 100× error), which is
Malawi's factor-of-50 pathology in Ugandan clothing.

**Grain conflicts** (rows sharing the whole declared index but reporting
different sold labels; blanked and warned, nothing dropped): 11 of 130,606 —
2009-10 6 `Unit_sold`; 2011-12 1 `Unit_sold`; 2013-14 2 `Unit_sold` + 1
`Condition_sold`; 2015-16 1 `Condition_sold`.

---
### Phase 3 — verification

- `crop_production_for_wave` (extended) — **OK (anchored on §2, §4)**: two
  columns added off the row already read; rows, index and every pre-existing
  column verified byte-identical against the pre-change build.
- `CROP_COLMAPS` (15 `unit_sold` + 15 `condition_sold` entries) — **OK (§3)**:
  every column choice re-derived from the value-label vocabulary and value
  range; 2011-12's `condition_sold: None` is corroborated by the questionnaire
  (C4), not by the extract's silence.
- the label collapse — **was REINVENTION, now OK (§2, §5)**: the hand-rolled
  agreement-wise reducer was replaced by `reduce_to_agreed(on_conflict='na')`.
- `Condition_sold.spellings` — **OK (§5)**: YAML alias, not a copy;
  `test_sold_condition_shares_the_condition_vocabulary` pins the identity.
- NA rather than a sentinel — **OK (§4)**: `data_info.yml`'s sentinel rule is
  scoped to declared index levels, and both new fields are columns.
- **Cache impact**: `uganda.py` is in every Uganda table's input hash, so all
  ~28 Uganda tables invalidate — a full Uganda re-warm is required after merge.
- **Blast radius (rg, GitNexus unavailable)**: `crop_production_for_wave` is
  called only from the seven `Uganda/{wave}/_/crop_production.py` scripts;
  `CROP_COLMAPS` only from those scripts and `tests/test_uganda_*`.  No
  cross-country consumer reads Uganda `crop_production` columns by name
  outside `transformations.harvest_kg*`, which touches `Quantity`, `u`,
  `condition` and `KgFactor` and is unaffected (`tests/test_crop_kg_factor.py`
  green, 55 tests with the two sibling Uganda files).
