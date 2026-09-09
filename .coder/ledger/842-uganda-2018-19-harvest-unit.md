# Prior-Art Ledger — GH #842 (Uganda half): 2018-19 harvest UNIT wiring

> Per-task ledger. Inherits `.coder/ledger/STANDING.md`. Scope is the Uganda
> half of #842 only — the Niger NULL-`u` half, and the SOLD unit (#824), are
> other tasks.

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED); per CLAUDE.md "Code intelligence:
GitNexus is OPTIONAL, and its substitutes are named", impact was assessed with
`rg` over `lsms_library/` and `lsms_library/countries/*/_/*.py` and the blast
radius is reported in §5.

## §1 Task, restated

`Country('Uganda').crop_production()` serves `u='Unknown'` on **every** row of
wave `2018-19` — 14,194 of 133,683 corpus rows (season A 7,153, season B
7,041) — because `uganda.CROP_COLMAPS['2018-19']` declares `unit: None` for
both seasons. `None` is the library's *declared, auditable* way to say "this
wave records no such column" (`uganda._require` docstring,
`countries/Uganda/_/uganda.py:1163`), so writing it is a claim about the
survey. For season B that claim is **false**: `AGSEC5B.dta` ships `a5bq6b`,
variable-labelled "6b. Unit", populated on all 7,041 rows. This task tests the
claim by VALUE RANGE for both visits and wires whatever the values prove.

No new table, no new column, no framework change: this is a one-key config
correction inside a country module, plus its documentation and regression
tests.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `CROP_COLMAPS` | `countries/Uganda/_/uganda.py:1459` | per-wave, per-season source-column map for `crop_production` | yes — `test_uganda_crop_condition.py::test_crop_colmap_columns_resolve_in_source` | **extend** (one key) |
| `crop_production_for_wave` | `countries/Uganda/_/uganda.py:1194` | builds the canonical `(t,i,plot,j,u,condition,season)` frame; maps the unit code through `unit_map` and fills `'Unknown'` where NA (`:1386`) | yes (same module) | **reuse unchanged** |
| `_require` / `CropColmapError` | `countries/Uganda/_/uganda.py:1163` / `:1148` | a NAMED column that is absent RAISES rather than silently falling back | yes | **reuse** — it is what makes wiring a name safe to attempt |
| `_harvest_unit_map()` | `countries/Uganda/_/uganda.py` (via `_harmonized_codes('harvest_units')`) | `{Code: Preferred Label}` from `categorical_mapping.org` `#+name: harvest_units` (46 codes) | indirectly | **reuse** |
| `_harvest_condition_map()` | same, `harvest_conditions` (20 codes) | the rival vocabulary the value-range test discriminates against | yes | **reuse as the discriminator** |
| `transformations.harvest_kg` | `lsms_library/transformations.py:1668` | `Quantity` × unit→kg factor, summed to `(t,i,plot,j)`; reads the unit LABEL only, via `_kg_factor_series` (`:1646`) | yes | **reuse unchanged** — it is the downstream that the fix unlocks |
| `_get_kg_factors` | `lsms_library/transformations.py:1004` | unit→kg factors; **prefers a per-row `Quantity_kg` where present** (`:1137`) — the Nigeria/Malawi `food_acquired` precedent | yes | **NOT reused** — see §6 |
| `tests/test_uganda_crop_condition.py` | `tests/` | the existing `crop_production` regression module: `crop_production` fixture, `_aws_creds_available()` gate, `_source_path()` | — | **extend** (add unit tests here, don't start a new module) |

## §3 Definitions & conventions in force

- `unit: None` in `CROP_COLMAPS` = "this wave records no such column", a survey
  fact, auditable; a name that does not resolve is a config bug and RAISES —
  `countries/Uganda/_/uganda.py:1148,1163` (`CropColmapError`).
- Which source column is the unit and which the condition is decided by the
  **value vocabulary, never the variable label** — `Uganda/_/CONTENTS.org`
  §"Which source column is the unit and which is the condition", and the
  `CROP_COLMAPS` comment block. 2013-14 AGSEC5A is the standing proof: its
  variable labels are swapped.
- `u='Unknown'` is a deliberate string sentinel, never `pd.NA`, because the
  de-duplication `groupby(level=...)` would silently delete a null index key —
  `countries/Uganda/_/uganda.py:1380-1386`.
- Canonical `crop_production` schema: `lsms_library/data_info.yml`
  `Columns.crop_production` (declares `condition`; declares **no** `Quantity_kg`).
- Cache: this table is script-path (`materialize: make`), so its L2-wave
  parquets are written hashless and must be evicted physically before a cold
  verification — CLAUDE.md §"Cache Behavior", limitation (b).
- Per STANDING.md §3 / §4 for the repo-wide grain, `v`-join and IO rules; none
  of them change here.

## §4 Invariants & assumptions

- **Row count must not move.** Wiring a unit relabels `u`; it must not add or
  drop rows. `crop_production_for_wave` collapses exact-duplicate index tuples
  by summing (`uganda.py:1394`), and `u` is an index level — so a unit that
  *splits* a previously-merged tuple WOULD change row counts. Measured
  before/after is therefore the invariant, not an assumption.
- **Only 2018-19 season B may move.** Every other `(t, season)` cell is
  byte-identical or the change is wrong.
- 2018-19 is a WIDE-vintage wave: parallel `_1`/`_2` slots, not one row per
  condition (`CONTENTS.org` §"The two vintages encode the same vocabulary").
  `CROP_COLMAPS['2018-19']` wires slot 1 only.
- The 2018-19 files mix naming vintages **within one file**: slot 1 uses the
  legacy `a5?q6{b,c,d}` names while slot 2 uses WB `s5bq06{b,c,d}_2`. Do not
  assume a single prefix.
- pytest deletes Uganda's cache at session start (`conftest.py::_purge_country_caches`,
  documented in `CONTENTS.org` §"Uganda is the ONE country pytest deletes the
  cache of") — run the suite with `LSMS_DATA_DIR` pointed at scratch.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| 2018-19 season-B harvest unit | **extend** `CROP_COLMAPS['2018-19']['B']['unit']` | the column exists and the values prove it is the unit; one key, no new machinery |
| 2018-19 season-A harvest unit | **no change** (`unit: None` stands) | measured: the file ships no unit column at all — a survey fact, now with numbers behind it instead of an assertion |
| unit→label mapping | **reuse** `_harvest_unit_map()` | already the single source of truth |
| kg conversion | **reuse** `harvest_kg` unchanged | the fix reaches it through `u`; no transform edit |
| survey-reported kg factor (`a5?q6d`) | **new — DEFERRED, see §6** | no canonical home exists and `harvest_kg` cannot consume one |

**Blast radius (rg floor).** `CROP_COLMAPS` is referenced only by
`countries/Uganda/_/uganda.py`, the seven `Uganda/{wave}/_/crop_production.py`
wave scripts, and `tests/test_uganda_crop_condition.py`. `u` on
`crop_production` is consumed by `transformations.harvest_kg` / `yield_kg`
(via `_kg_factor_series`). `crop_production` is **not** registered in
`lsms_library/data_info.yml` `index_info`, so it is already excluded from
`Feature('crop_production')` (`CONTENTS.org`) — no cross-country blast radius.
Risk: MEDIUM (moves data within one wave-season; guarded by a measured
before/after and two new tests).

## §6 Open questions for the human

1. **The survey-reported kg factor `a5?q6d` has no home, and this is the one
   place the fix stops short.** Both 2018-19 files carry "6d. Conversion factor
   into kg?" populated on 7,123/7,153 (5A) and 7,026/7,041 (5B) rows, so 5A's
   harvest weight is recoverable as `Quantity × a5aq6d` even though its unit
   label is not. But `lsms_library/data_info.yml` declares no `Quantity_kg`
   column for `crop_production`, and `harvest_kg` → `_kg_factor_series` reads
   the unit LABEL only — it never consults a per-row factor. The precedent is
   `food_acquired`, where Nigeria/Malawi ship `Quantity_kg` and
   `_get_kg_factors` prefers it (`transformations.py:1137`). Doing the same for
   `crop_production` needs a `transformations.py` change, which is out of this
   task's scope. **Recommendation: do it, as its own issue** — it is the only
   route to a kg figure for 2018-19 season A. **Caveat that must not be lost:**
   the raw factor needs a sanity filter first (`2018` appears as a factor —
   a year leak; a Kg row at 260,000; zeros; implied max 2.16e7 kg).
2. **The factor does NOT pin 5A's missing unit** — measured, so do not propose
   reconstructing it that way. Factor 20 is consistent with Tin (Debe) (20 lts),
   Basket (20 kg) and "Others specify"; factor 10 with Basket (10 kg) and
   Jerrican (10 lts); and within-unit dispersion on 5B is wide (Sack (100 kgs):
   688 rows at 100, 122 at 80, 117 at 120, 80 at 110). 2018-19 season A is
   `asked-not-distributed`, not recoverable by inference.
3. **The 5B `_2` slot is a genuine second CONDITION slot and is unwired** —
   evidence in `CONTENTS.org`; wiring it ADDS ~1,435 rows, which breaks this
   task's "rows unchanged" invariant, so it is deliberately left for its own
   change.

---
### Phase 3 — verification (filled at task end)

- `CROP_COLMAPS['2018-19']['B']['unit'] = 'a5bq6b'` — **OK (anchored on §2, §3,
  §5)**: reuses `_require` + `_harvest_unit_map()`; no new machinery; the value
  evidence satisfies the §3 "vocabulary, never the label" rule.
- `CROP_COLMAPS['2018-19']['A']['unit']` left `None` — **OK (anchored on §3)**:
  the `None` claim is now measured rather than asserted.
- No `lsms_library/*.py` edit — **OK (anchored on §5, §6)**: the kg-factor route
  that would have required one is deferred with its counts recorded.
