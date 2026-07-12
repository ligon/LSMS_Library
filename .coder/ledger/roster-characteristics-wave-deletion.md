# Prior-Art Ledger — roster-characteristics-wave-deletion

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`. Living
> snapshot; git history is the journal.

**Search tier used:** ripgrep + git floor (gitnexus not consulted this session),
plus an empirical census: `Country(c).household_characteristics()` per-wave row
counts for all 40 countries that declare `household_roster`, warm cache.

## §1 Task, restated

`household_characteristics` is a **derived** table (`Country._ROSTER_DERIVED`,
`STANDING.md §2`): `country.py` aggregates `household_roster` across *all* waves
into one frame and hands it to `transformations.roster_to_characteristics()`,
which drops non-resident members using whichever residence-duration column is
present (`MonthsSpent` / `MonthsAway` / `WeeksAway`, per `CLAUDE.md`
§"MonthsSpent / MonthsAway / WeeksAway"). Because the frame handed to the
transform is the **country-level concat**, a residence column contributed by
*one* wave is unioned onto *every* wave. Where the resolved months series is
unusable for a wave, the keep-mask is all-False **for that whole wave** and
`household_characteristics` silently returns nothing for it, even though
`household_roster` is fully populated. 8 wave-cells are currently destroyed.
The task fixes the resolution + the guard, and repairs one wave whose
`MonthsSpent` is an untranslated French label — in **config**, not in code.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `roster_to_characteristics` | `lsms_library/transformations.py:177` | roster → household sex×age counts + `log HSize`; owns the residence filter (L251-265) | `tests/test_roster_to_characteristics_movers.py`, `tests/test_age_intervals.py`, `tests/test_uganda_api_vs_replication.py` | **extend** (the residence-resolution block only) |
| `Country._ROSTER_DERIVED` + dispatch | `lsms_library/country.py:3325`, `3515-3530` | resolves the transform by *name* at call time and passes the **all-waves** roster with `final_index` = the roster's own index levels | integration surface | reuse unchanged |
| YAML `mapping:` on a `myvars` entry | e.g. `Mali/2017-18/_/data_info.yml:76`, `Mali/2018-19/_/data_info.yml:163` (`MonthsSpent: [s1q13, {mapping: {Oui: 12, Non: 0}}]`) | maps a categorical source label to the canonical numeric months value **at config level** | per-country builds | **reuse** — this is the sanctioned pattern for D3 |
| `mover_sentinel` / NaN-in-`final_index` handling | `transformations.py:287-337` | GH #197 / #268: keeps movers rather than silently dropping them | `tests/test_roster_to_characteristics_movers.py` | untouched (same *class* of silent-drop bug, one layer down) |

Searched by concept — "months present", "residence", "drop departed", "keep
mask" — not just by identifier. No other implementation of a residence filter
exists in the library; `roster_to_characteristics` is the only site.

## §3 Definitions & conventions in force

- **MonthsSpent / MonthsAway / WeeksAway semantics** — `CLAUDE.md`
  §"MonthsSpent / MonthsAway / WeeksAway" (cited, not restated): `MonthsSpent`
  = months present 0–12; `MonthsAway` → `12 - value`; `WeeksAway` →
  `12 - weeks/(52/12)`. Filter excludes NaN (question not asked) and 0 months,
  **except** infants (age < 1). **"Countries without any residence column are
  unaffected — the old count-everyone behavior continues."** That sentence is
  the definition the D2 guard restores at *wave* granularity.
- **Ethiopia W4–W5 switched months→weeks** — same section: "`WeeksAway`: …
  Used by Ethiopia W4–W5 and Cambodia, where the questionnaire switched from
  months to weeks." So Ethiopia legitimately carries **both** columns at country
  level, disjointly populated. The `elif` chain contradicts this documented fact.
- **EHCVM binary → 12/0** — same section: "The binary `s01q12` … is mapped to
  0/12", done in the wave `data_info.yml` `mapping:` block. D3's fix follows
  this pattern verbatim (Burkina Faso 2014 EMC `B3A`, a 6-month binary).
- **Derived tables are not registered** — `STANDING.md §3`; the fix must stay in
  `transformations.py` + config, with no `data_scheme.yml` change.

## §4 Invariants & assumptions

- **The 1315-HH Uganda drift is the reason the filter exists** (`CLAUDE.md`,
  same section). The all-NaN fallback is *exactly* the old count-everyone
  behaviour, so it must fire **only** where the wave has no usable residence
  datum at all — never for a wave where the filter currently works. Enforced by
  the per-`t` (not global) guard + the before/after census.
- The roster handed to the transform is a **multi-wave** frame with `t` in the
  index (`country.py:3523` derives `final_index` from `['t','v','i','m']`).
  Any per-wave logic must group on `t` — and must not assume `t` is present
  (the transform is also called directly in tests with a `(i, pid)` index).
- Residence columns arrive with mixed dtypes across waves (BF: `'6 mois ou
  plus'`, `'12'`, `12.0`) — the country concat unions object + float. Resolution
  must stay `pd.to_numeric(..., errors='coerce')`-based.
- `STANDING.md §4` repo-wide invariants unchanged: no `inplace=`, `pd.NA` for
  string/categorical missing, config resolved via `countries_root()`.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| months-present series | **extend** `roster_to_characteristics` | only implementation; the bug is in its resolution step (per-column `elif` → per-row coalesce) |
| per-wave "no usable residence data" guard | **new** (inside the same function) | nothing in the library expresses "this column is unioned-in from another wave"; it is a property of the country-level concat, invisible at wave level. Kept ~10 lines, local to the transform. |
| Burkina Faso 2014 French labels → 12/0 | **reuse** the YAML `mapping:` pattern (EHCVM `Oui`/`Non`) | `CLAUDE.md` prescribes it; keeps language-specific labels out of `transformations.py` |

## §6 Open questions for the human

- BF 2014 `B3A` is a **6-month binary**, not a months count (`6 mois ou plus`
  77,268 / `moins de 6 mois` 943 / NaN 332). Mapping it to 12/0 matches the
  EHCVM convention (and drops the 943 short-stay members), but it is coarser
  than Uganda's true months count. Flagged, not decided — if the project would
  rather keep short-stay members in BF 2014, change the mapping to `12 / 6`.
- CotedIvoire 1985–89 and Mali 2021-22 now count **everyone** in the roster
  (no residence question in those waves). That is the documented behaviour for
  a country with no residence column, applied per-wave. It does mean those
  waves are not filter-comparable with their EHCVM siblings.

---
### Phase 3 — verification (fill at task end)

- `roster_to_characteristics` (residence-resolution block) — **OK (anchored on
  §3)**: per-row coalesce `MonthsSpent → MonthsAway → WeeksAway` matches the
  documented per-column semantics and is the only reading consistent with
  "Ethiopia W4–W5 switched … to weeks" while W1–W3 use months.
- `roster_to_characteristics` (per-`t` all-NaN guard) — **OK (anchored on §3/§4)**:
  restores "countries without any residence column are unaffected — the old
  count-everyone behavior continues" at wave granularity; the census shows it
  fires for exactly the 7 wave-cells with 0 non-null residence data and for no
  other wave (Uganda's 8 waves byte-identical).
- Burkina Faso 2014 `MonthsSpent` mapping — **OK (anchored on §3, reuse §2)**:
  same config-level `mapping:` idiom as the EHCVM `Oui`/`Non` → 12/0 waves; no
  French in library code.
- No **REINVENTION**: no other residence/months-present filter exists in the
  library (ripgrep over `months`, `spent`, `away`, `resident`).
- No **CONTRADICTION**: the filter still fires wherever it fires today; the only
  waves whose counts move are the 8 that currently return nothing.
