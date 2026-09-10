# Prior-Art Ledger — `visualizations.lorenz_curve`

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md` — cite it,
> `CLAUDE.md`, and `lsms_library/data_info.yml` rather than re-copying.
> Design doc: `slurm_logs/viz_lorenz/DESIGN.org`.

**Search tier used:** ripgrep + git floor (gitnexus MCP failed to connect this
session; no `.gitnexus/` index on this mirror).

## §1 Task, restated
Add `lorenz_curve()` to `lsms_library/visualizations.py` (exported at the
package top level like `population_pyramid` / `coordinate_map`): the
cumulative share of a welfare measure against the cumulative share of the
population, poorest first, for one survey wave of one `Country` (or a
household-grain frame). The welfare measure is the derived
`food_expenditures` table summed to household grain, expressed per person
using household size from the derived `household_characteristics`, and
weighted by the household's `sample()` weight times its size. The Gini
coefficient is the companion statistic and is reported on the chart. Every
choice that moves the curve (basis, per-person vs per-household, weighting,
households drawn at zero) is stated on the chart, in the module's existing
voice.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `visualizations.PALETTE`, `_FONT_STACK`, `_require_pyplot` | `visualizations.py:46-73` | module palette (ink/rule/bar/alt), font stack, lazy pyplot | yes | **reuse** |
| `visualizations._resolve_roster` | `visualizations.py:76` | Country / name / frame → `(frame, label, country_name)`, most-recent-wave-with-warning | yes | **extend**: generalise the wave-selection half for a non-roster table (or write a sibling `_resolve_table`) |
| `visualizations._attach` | `visualizations.py:117` | joins `weight` + `by=` column from `sample()` at `(i, t)`; names the weighting basis | yes | **reuse** (household grain is exactly its join key) |
| `visualizations._universe` | `visualizations.py:170` | "Represents: …" caption from `Country.population` | yes | **reuse** |
| `Country.food_expenditures(waves=, basis=)` | `country.py:4334-4419`; transform `transformations.food_expenditures_from_acquired:1147` | `(i,t,v,j,s) × Expenditure`; `basis='purchased'` (cash outlay, default) or `'total'` (all recorded acquisition value; equals purchased where no imputed value is recorded). **Zero-expenditure rows are dropped** (`:1200-1206`) | yes | **reuse** — sum over `(j, s)` to household grain; never re-derive from `food_acquired` |
| `Country.household_characteristics()` / `roster_to_characteristics` | `transformations.py:452` | `(t,v,i) × … log HSize`; household size = `exp(log HSize)`, **resident-filtered** (MonthsSpent/Away) | yes | **reuse** for household size; do not count roster rows |
| `Country.sample()` | `CLAUDE.md` §"sample() and Cluster Identity" | `(i,t) × v, weight, panel_weight, strata, Rural`; weights normalised to within-wave mean 1 over households | yes | **reuse** for weights, `by=` columns, and the frame of interviewed households |
| `Country.population` / `PopulationRecord` | `population.py`; `docs/guide/population.md` | per-wave universe tag + source + confidence | yes | **reuse** via `_universe` |
| Gini / Lorenz / cumulative-share computation | none — `rg -i 'lorenz|gini|cumulative share'` over `lsms_library/`, `docs/`, `SkunkWorks/`, `tests/` finds nothing | — | — | **new** (small, pure numpy; trapezoid Gini exact for the drawn polygon) |

## §3 Definitions & conventions in force
- **Derived tables** are runtime-derived, not registered: `food_expenditures`,
  `household_characteristics` — `STANDING.md §3`, `CLAUDE.md` §"Derived Tables".
- **`basis=`** on `food_expenditures`: `'purchased'` = cash outlay only;
  `'total'` = all recorded acquisition value, no imputation —
  `transformations.py:1147-1180` (GH #575).
- **Weights** are normalised to within-wave mean 1 *over households*; a
  person-level total is `sum(size_h * w_h)`, which is not the headcount —
  `visualizations.py:20-28`, `CLAUDE.md` §"Weights are normalised".
- **`v` is joined at API time** from `sample()`; do not bake or re-join it —
  `STANDING.md §2/§4`.
- **Population record**: `universe_tag` never travels without `source_type` +
  `confidence`; the chart reads it only through `_universe` — `CLAUDE.md`
  §"The Population Record".
- **Module design rules** (`visualizations.py:1-33`): colour encodes the
  conditioning variable, never a fixed attribute; a chart states the
  population it represents; the weighting basis is stated, never implied;
  one wave per chart unless the overlay is deliberate and labelled.
- **Pandas 3.0 targets** — `CLAUDE.md` §"Pandas 3.0 Targets".

## §4 Invariants & assumptions
- `food_expenditures_from_acquired` **drops zero/NaN Expenditure rows**
  (`transformations.py:1200-1206`), so a household with no cash food purchase
  is *absent* from `food_expenditures()`, not present at zero. Measured,
  Uganda 2013-14: 3,119 households in `sample()`, 3,089 in
  `food_expenditures(basis='purchased')`, 3,119 in `basis='total'` — the 30
  missing are true cash zeros (they have own-production rows). A Lorenz curve
  must decide about them explicitly and say what it did.
- `household_characteristics` is resident-filtered; its household set can
  differ from `food_expenditures`' (Uganda 2013-14: 2 households with
  expenditure but no characteristics row). Count and disclose, never drop
  silently.
- The Lorenz curve is **scale-invariant**: currency, deflators, recall-period
  scaling and `numeraire=` do not move it within a wave. Item coverage
  (`j` set) and the universe *do* differ across waves, so a multi-wave
  overlay is legitimate only as labelled separate curves, never a pool.
- `basis` moves the answer a lot: Uganda 2013-14 per-person person-weighted
  Gini is **0.497 purchased vs 0.343 total** — rural households produce their
  own food, so cash-only overstates consumption inequality. Weighting choice
  also moves it (per-person hh-weighted 0.559; hh-total unweighted 0.478).
  None of these is visible in the picture; the subtitle must carry them.
- `nonfood_expenditures` is **not one table** (GH #817, filed 2026-09-08): Togo is
  long `(t, i, j) × Expenditure`; Uganda `(i, t, v) × 41 items` and Nigeria
  `(i, t, v, m) × 96 items` are wide pivots made by their country-level scripts,
  with no schema in `data_info.yml` to catch it. `value=` accepts only the long
  shape and raises on a wide one, pointing at #817 — the viz must not launder
  the defect.
- `weights=True` must fall back to unweighted with a warning where a country
  has no weights (module convention, `visualizations.py:146-153`).
- Multiple inputs in a `merge` drop `attrs` unless they agree — irrelevant
  here (nothing downstream reads `attrs` off the chart frame), but the
  population caption is read from `Country.population`, not from `attrs`.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| household food expenditure | reuse `Country.food_expenditures(waves=, basis=)` summed over `(j, s)` | §2; the basis semantics and zero-drop live there and are tested |
| household size | reuse `household_characteristics()['log HSize']` | resident-filter semantics already decided (§3); counting roster rows would contradict them |
| sampling weight, `by=` column | reuse `_attach` (or its join, at household grain) | same `(i, t)` key, same fallback-with-warning contract |
| universe caption | reuse `_universe` | one reader of the population record in the module |
| wave selection | extend `_resolve_roster`'s wave logic (or sibling) | identical rule, different table |
| Lorenz ordinates + Gini | **new** | nothing in the repo computes them; ~15 lines of numpy; trapezoid Gini is exact for the piecewise-linear curve drawn, so the number and the geometry agree by construction |

## §6 Open questions for the human
Resolved with @ligon, 2026-09-08 (see `slurm_logs/viz_lorenz/DESIGN.org`):
- Default `basis` follows `food_expenditures()` (`'purchased'`); alternatives
  must be easy to reach — hence `value=` accepts another table name or a
  `(i, t)` Series, and `basis='total'` passes through.
- Households in `sample()` with no expenditure row are **omitted and counted**
  by default (`zeros=None` → `'drop'`); `zeros='include'` draws them at zero.
- Multi-wave overlay via `wave=[...]` (≤ 5) is in scope, as an ordered ramp.

---
### Phase 3 — verification (filled 2026-09-08)
Anchored check by the implementer, re-verified by an independent read-only
red-team agent (200 randomised Gini trials across three formulas; all four
docstring Ginis reproduced; merge keys unique on both sides; sibling messages
byte-identical) and by the coordinator (diff read, four renders inspected,
Nigeria/Uganda probes). Commits: see `git log --oneline` on
`feat/viz-lorenz-curve` (first commit `93750330`; review fixes follow).

- `lorenz_curve` — OK (§1, §5): assembles from `food_expenditures(waves=, basis=)`,
  `household_characteristics()['log HSize']`, `sample()`; reads neither
  `food_acquired` nor the roster; every drop is a counted subtitle bit.
- `_lorenz` — OK (§2 "new", §5): trapezoid Gini exact for the drawn polygon;
  ties merged; zero weights dropped; negative / all-zero / non-finite raise.
  Hand case `[1,2,3,4]` → 0.25 exactly.
- `_sum_to_households` — OK (§5): sums over every level but `(i, t)`.
- `_country_measure` — OK (§5 reuse; §4 GH #817): long shape only; a table
  without `Expenditure` raises citing #817; `basis='total'` in-kind check reads
  the `s` level already returned (no second API call).
- `_household_sizes` — OK (§2, §3): `exp(log HSize)`; size ≤ 0 counted and
  dropped, never divided by.
- `_select_waves` — OK after review fix (§5 "extend `_resolve_roster`'s wave
  logic"): the default is the most recent wave *the measure holds*, as the
  sibling reads its table's own `t`; validating against `Country.waves` alone
  picked Nigeria's empty `2024Q1`.
- `_by_groups` — OK (design §by; §2 `_attach` lookup): >2 values raise naming
  them; tuple form picks two; ledger silent on the tuple form (design choice).
- `_ramp` — design §Colour; no ledger anchor claimed (pure presentation). Cap
  0.50 chosen by the validator, documented in code.
- `_attach(noun=, …)`, `_require_pyplot(caller)`, `_MISSING_MPL` — OK (§2 reuse,
  generalised): pyramid behaviour and messages unchanged; the stale
  "test dependency group" claim corrected (matplotlib is a main dep, v0.11.0).
- No REINVENTION: `rg -i 'lorenz|gini'` still finds nothing outside the module
  and its tests. No CONTRADICTION found against §3/§4.
