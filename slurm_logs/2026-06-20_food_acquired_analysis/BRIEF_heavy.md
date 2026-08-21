# BRIEF: canonicalize + register food_acquired — GhanaSPS & Panama (Phase 3 of #218, heavy pair)

Shared design artifact. Implement against THIS spec. Author: Sue (scrum master), 2026-06-21.
Base branch: **development**. Read also the pilot BRIEF for the shared pattern,
verification recipe (LSMS_COUNTRIES_ROOT!), stop-list, and report format:
`/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/2026-06-20_food_acquired_analysis/BRIEF_pilot.md`
Precedent to copy: Cambodia #561 (wave script → canonical (t,i,j,u,s); country concat;
`materialize: make`; derived tables auto-surface; no legacy derived script).

Canonical target: `food_acquired` index `(t, i, j, u, s)` (v omitted — joined from
sample at API time), columns `[Quantity, Expenditure]` (+ optional `Price`),
`materialize: make`. i=household, j=harmonized food item, u=unit,
s∈{purchased,produced,inkind,other} (`transformations.S_VALUES`). Both countries
have the legacy **i/j SWAP** (j=household, i=item) — FIX to i=household, j=item.
Both have NO `data_scheme.yml` — create from scratch. Both have a dead
`_/food_prices_quantities_and_expenditures.py` — DELETE it.

================================================================
## GhanaSPS  (waves 2009-10, 2013-14, 2017-18) — SANS-SAMPLE
================================================================
**Bespoke 3-source melt — ALL THREE sources carry value (Expenditure).** Unlike
the stock `food_acquired_to_canonical` (which NaNs produced Expenditure), GhanaSPS
records purchased+produced+inkind quantity AND value. Emit one row per source with
that source's Quantity AND Expenditure. s map: purchased→purchased,
produced→produced, received-gift→inkind.

Source per wave (i=household, j=item harmonized via `_/food_items.org` per-wave col):
- 2009-10 `2009-10/Data/S11A.dta`: i=`hhno`, j=`itname`, u=`s11a_f` (numeric →
  `units.org` `#+name: unit09`). qty: purchased `s11a_ci`, produced `s11a_bi`,
  inkind `s11a_di`. value (cedis): purchased `s11a_cii + s11a_ciii/100`, produced
  `s11a_bii + s11a_biii/100`, inkind `s11a_dii + s11a_diii/100`.
- 2013-14 `2013-14/Data/11a_foodcomsumption_prod_purch.dta`: i=`FPrimary`,
  j=`foodlongname`, u=`unit` (text → `units.org` `#+name: harmonizedunit`).
  qty: `purchasedquant`/`producedquant`/`receivedgiftquant`. value:
  `purchasedcedis`/`producedcedis`/`receivedgiftcedis` (VERIFY these exact column
  names against the .dta; report if different).
- 2017-18 `2017-18/Data/11a_foodconsumption_prod_purch.dta`: i=`FPrimary`,
  j=`foodname`, u=`unitname`. qty/value columns as 2013-14 (VERIFY names).

GhanaSPS specifics:
- **No `sample` table** (cluster identity unavailable: no cluster var in food files,
  2013-14 has no EAs.dta, EAs.dta is EA-level only). So food_acquired index is
  `(t,i,j,u,s)` with **no v**. This is ACCEPTED (data gap; sample is a separate
  follow-up). The build must still be canonical + sane.
- **Expected**: `Country('GhanaSPS').food_acquired()` index `(t,i,j,u,s)` (NO v),
  cols `[Quantity,Expenditure]`, `is_this_feature_sane().ok is True` (the
  index_levels_match_scheme warn for the missing/optional v is ALLOWED).
- **Expected & BY DESIGN**: in the full `Feature('food_acquired')()` GhanaSPS is
  MODAL-EXCLUDED (it lacks v while the 15 others have it). That is NOT a defect —
  it is the documented consequence of shipping sans-sample. Verify GhanaSPS DOES
  appear in `Feature('food_acquired')(['GhanaSPS'])` (subset → it is the modal
  shape). A sample/v follow-up will fold it into the default assembly later.
- data_scheme.yml: create with `Country: GhanaSPS` + a `Data Scheme:` with ONLY
  the `food_acquired` block (index (t,i,j,u,s), Quantity/Expenditure float,
  materialize: make). Do not invent other features.

GhanaSPS acceptance bar:
1. food_acquired canonical `(t,i,j,u,s)` (no v), cols ⊇ [Quantity,Expenditure],
   sane.ok True.
2. food_{expenditures,prices,quantities}() non-empty + canonical (they auto-derive;
   expenditures here includes produced+inkind value since GhanaSPS carries it).
3. i=household ≫ j=item (NOT swapped). All 3 waves present in `t`.
4. **Source reconciliation** per wave: summed purchased+produced+inkind value from
   canonical food_acquired == raw source totals (the three *cedis columns); distinct
   household + item counts match source. Paste numbers per wave.
5. `Feature('food_acquired')(['GhanaSPS'])` returns GhanaSPS non-empty.

================================================================
## Panama  (waves 1997, 2003, 2008) — WITH SAMPLE (v from upm)
================================================================
i/j SWAP fix: current j=household / i=item → canonical i=household / j=item.
Source per wave (j=item harmonized via `_/food_items.org` per-wave col;
u via `_/units.json`):
- 1997 `1997/Data/GAST-A.DTA`: i=`form`, j=`ga100`(item code, numeric→food_items '1997'),
  u=`unitcode`. purchased qty `ga106a`, value `ga106c`; produced qty `ga110a`
  (no value); inkind qty `ga109a` (no value). v=`upm` (in this file).
- 2003 `2003/Data/E03GA10B.DTA`: i=`form`, j=`gai00`, purchased qty `gai06a`,
  value `gai06c`; produced qty `gai10a`. v=`upm` — NOT in the food file; MERGE
  `2003/Data/E03BASE.DTA` on `form` to attach `upm`.
- 2008 `2008/Data/05alimentos.dta`: i=`hogar`, j=`producto`, purchased qty `s11a6a`,
  value `s11a6c`; produced qty `s11a10a`. v=`upm` (in this file).
- VERIFY all column names against the .dta and the existing wave scripts; report
  any deviation. Watch for 1997 inkind (`ga109a`) — include it as s=inkind if present.

s map: purchased→purchased (qty+Expenditure), produced/obtained→produced
(qty, Expenditure NaN), 1997 ga109a→inkind (qty, Expenditure NaN).

Panama also build a minimal `sample` table (so food_acquired gets v and is a full
Feature() member):
- `sample` index `(i, t)`, column `v` = `upm` (string). If a household weight var
  exists in source (look for `factor`/`peso`/`fexp`/weight), include `weight`;
  else omit (a minimal v-only sample is acceptable for the join). Declare `sample`
  in data_scheme.yml. The framework's `_join_v_from_sample` will add v to
  food_acquired at API time.

Panama acceptance bar:
1. food_acquired canonical `(t,v,i,j,u,s)` (v joined from sample), cols ⊇
   [Quantity,Expenditure], sane.ok True (only the framework-joined-v warn).
2. food_{expenditures,prices,quantities}() non-empty + canonical.
3. i=household ≫ j=item (NOT swapped). All 3 waves present.
4. **Source reconciliation** per wave: purchased value total == raw `ga106c`/`gai06c`/
   `s11a6c` sum; distinct household + item counts match source. Paste numbers.
5. **Panama IS in the full `Feature('food_acquired')()`** (it has v → modal shape,
   NOT excluded). Verify Panama appears in `Feature('food_acquired')(['Panama','Uganda'])`.

================================================================
## Both: deliverable, branch, report
================================================================
- Rewrite each `{wave}/_/food_acquired.py` → canonical; rewrite `_/food_acquired.py`
  → wave concat; create `_/data_scheme.yml`; (Panama) add `_/sample.py` +
  `{wave}` sample wiring; DELETE `_/food_prices_quantities_and_expenditures.py`;
  fix `_/Makefile` (drop the dead derived rules).
- Branch `feat/218-{country-lower}-food-acquired-canonical` cut from origin/development;
  commit; **push --force-with-lease** (no PR, no merge).
- Verify with the pilot BRIEF's recipe (PY=.venv/bin/python, LSMS_COUNTRIES_ROOT=
  YOUR worktree countries, LSMS_NO_CACHE=1). Every acceptance-bar item must pass.
- STOP-LIST + mandatory SCOPE-DEVIATIONS report format: see BRIEF_pilot.md. Only
  edit files under `lsms_library/countries/{your country}/`.
- green=true ONLY if you actually ran verification and every bar item passed.
