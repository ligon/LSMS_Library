# Prior-Art Ledger — `nonfood_expenditures` ships in two shapes (GH #817)

> Per-task ledger. Inherits `.coder/ledger/STANDING.md`, `CLAUDE.md` and
> `lsms_library/data_info.yml` — cited, not re-copied.

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED), so the CLAUDE.md-sanctioned
substitutes were used: `rg` for the call-site sweep, `gh api` for the issue
(the `gh issue view` GraphQL path 400s on this repo's classic Projects).

## §1 Task, restated

`nonfood_expenditures` is declared by three countries and delivered in two
incompatible shapes. Togo (PR #773) is canonical long `(t, i, j) x
{Expenditure, RecallWindow}`; Uganda and Nigeria deliver a WIDE
item-by-household matrix (41–96 item columns) with `m` baked into the index,
produced by a pivot in the *country-level* `_/nonfood_expenditures.py` — not by
the surveys. `lsms_library/data_info.yml` has no `Columns:` block for the table
and no `Index Info > index_info` entry, so `_assert_built_required_columns`,
the `dfs:` guard and the coverage grader have nothing to grade against, and
`Feature('nonfood_expenditures')` cannot stack the three frames.

End state: every declarer serves long `(t, i, j) x Expenditure`; the canonical
schema exists in `data_info.yml`; `Feature('nonfood_expenditures')` assembles.
The design is the issue's "Proposed fix", implemented, not redesigned.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Togo/2018/_/nonfood_expenditures.py` | `countries/Togo/2018/_/nonfood_expenditures.py:1` | builds long `(t,i,j) x {Expenditure, RecallWindow}`; asserts label injectivity and `(t,i,j)` uniqueness | `tests/test_togo_nonfood_expenditures.py` | **reference shape** — copied in spirit (assertions), not in code |
| `Togo/_/data_scheme.yml` nonfood block | `countries/Togo/_/data_scheme.yml:235` | `index: (t, i, j)`, `Expenditure: float`, `RecallWindow: str` | via schema tests | **the model** for the `data_info.yml` block and for Uganda/Nigeria's declarations |
| `Uganda/_/food_acquired.py` | `countries/Uganda/_/food_acquired.py:1` | country-level concat of already-long wave parquets + `id_walk`, then `to_parquet('../var/…')` | via `Feature()` audit | **reuse the pattern verbatim** for the nonfood concatenator |
| `Uganda/2013-14/_/food_acquired.py` | `countries/Uganda/2013-14/_/food_acquired.py:9` | stamps `round` in the **wave script** (`d['t'] = round`), then sets the canonical index | — | **reuse**: `t` is stamped by the wave script, not the helper |
| `uganda.food_acquired_to_canonical` | `countries/Uganda/_/uganda.py:416` | wide→long melt; `sum(axis=1, min_count=1)` so all-NaN stays NaN | — | **reuse the min_count=1 convention** (§3 sparsity) |
| `uganda.nonfood_expenditures` | `countries/Uganda/_/uganda.py:533` | one `.dta` → `groupby(['HHID','itmcd']).sum().unstack()` + `fillna(0)`; returns WIDE, with index/column names *swapped* (`index.name='j'` is the household, `columns.name='i'` are the items) | no | **extend** — same reads/label map, long output |
| `uganda.id_walk` | `countries/Uganda/_/uganda.py:603` | per-wave household-id remap; falls back to the `j` level and renames it `i` when `i` is absent | `tests/test_id_walk.py` (core twin) | **reuse**; the fallback+warning stops firing once `i` is the household level |
| `Nigeria/*/_/nonfood_expenditures.py` | `countries/Nigeria/2010-11/_/nonfood_expenditures.py:1` (+3 siblings) | reads 8–10 CSVs per wave, renames `hhid→j` / `item_cd→i` (**both backwards**), `set_index(['j','t','m','i'])`, `['value'].unstack('i')` | no | **extend** — same reads, long output, canonical names |
| `Nigeria/_/nonfood_expenditures.py` | `countries/Nigeria/_/nonfood_expenditures.py:1` | concat 4 wave parquets, `replace(inf, nan)`, `.T.groupby('i').sum().T`; dead commented `aggregate_items.json` rename | no | **extend** — keep `replace(inf, nan)`, drop the pivot and the dead code |
| `Country._join_v_from_sample` | `country.py:2891` | joins `v` at API time for household tables whose canonical index carries `v` | yes | **reuse** — never emit `v` |
| `Country._audit_index_collapse` | `country.py` | GH #323 grain audit at the declared-index collapse | yes | reuse (verify no new `GrainCollapseWarning`) |
| `local_tools.to_parquet` | `local_tools.py` | `_resolve_data_path` redirect to `data_root()` | yes | **reuse** — never write in-tree (GH #803) |
| `transformations.food_expenditures_from_acquired` | `transformations.py:1200` | the **sparsity convention**: an unreported item has NO ROW, never a fabricated 0 | yes | **cite** (§3) — the rule this task restores for nonfood |
| `local_tools.harmonized_food_labels` / `uganda.harmonized_food_labels` | `countries/Uganda/_/uganda.py:329` | `Code -> Preferred Label` from `nonfood_items.org` | — | **reuse unchanged** — `j` stays the Preferred Label |

## §3 Definitions & conventions in force

- **Sparsity**: "a household that did not report an item has no row" —
  `transformations.py:1200` (`food_expenditures_from_acquired`), quoted by GH
  #817. `fillna(0)` conflates *not reported* with *reported zero*.
- **`min_count=1`**: the repo's way of keeping that distinction through a sum
  (`uganda.py:501-507`, `_finalize_canonical_food_acquired`).
- **`v` is never emitted by a feature**; it is joined from `sample()` at API
  time — `CLAUDE.md` §"`sample()` and Cluster Identity"; declaring the
  canonical index *with* `v` in `index_info` is what arms the join.
- **`m` must not be baked into a cached parquet** — `CLAUDE.md` §"Gotchas with
  Teeth" (`other_features` is obsolete; `m` is added on demand by
  `_add_market_index()` when the caller passes `market=`).
- **Core never aggregates**; a wave *script* may reduce to its declared grain.
  `CLAUDE.md` §"Grain Collapse"; duplicates on a declared index mean the
  identifier is broken, so assert uniqueness in the script (Togo does).
- **Nigeria is post-planting / post-harvest**: each wave dir holds TWO rounds
  needing DISTINCT `t` (`2010Q3`/`2011Q1`, …) —
  `.claude/skills/add-feature/pp-ph/SKILL.md`, `Nigeria/_/CONTENTS.org`. `t` is
  already per-round in these scripts and must stay so; rounds are never pooled.
- **REPORTED values only**; annualising a recall window is the analyst's
  transformation (`Togo/_/data_scheme.yml:255` "WINDOW DECISION: NOT ANNUALISED").
- **Sanctioned IO**: `get_dataframe` / `to_parquet`; never the `dvc` CLI.
  `CLAUDE.md` §"Data Access".
- **`data_info.yml` `Columns:`**: `required: true` is the only key the loader
  and `tests/test_schema_consistency.py` act on
  (`test_schema_consistency.py:35`); `monetary: true` is consumed by
  `currency.currency_for()`. A country whose `data_scheme.yml` declares the
  table as a **dict** must then list every required column — which is why the
  bare `nonfood_expenditures: !make` entries have to become dicts.

## §4 Invariants & assumptions

- **Totals are the invariant.** The pivot only re-shaped; per-wave
  `sum(Expenditure)` must be byte-identical before and after. Measured BEFORE
  (private data dir, unmodified worktree): Uganda 4,002,642,071.19 over 8
  waves; Nigeria 1,566,586,338.07 over 8 rounds; Togo 716,006,884.
- **The shared cache's Uganda `var/nonfood_expenditures.parquet` is STALE** —
  it holds 2013-14 only (3,115x41). The issue's "3,115 x 41" was measured off
  it. A cold build gives 8 waves / 23,992x62. Use the cold build as BEFORE.
- **Uganda `uganda.nonfood_expenditures` names are swapped**: the returned
  frame's `index.name` is `'j'` but holds HOUSEHOLDS, and `columns.name` is
  `'i'` but holds ITEMS. Every consumer downstream compensates. Fixing the melt
  means fixing the names, and `2015-16/_/nonfood_expenditures.py`'s hhid
  fix-up (`replace({'j': ids}).set_index('j')`) is keyed on the wrong one.
- **Uganda zeros are almost all fabricated**: with `min_count=1`, 2005-06 has
  33,857 (household, item) groups of which 40 are all-NaN and **0** are a
  genuine reported zero; 2019-20 has 187,026 groups, 150,627 all-NaN and 76
  reported zeros. The wide parquet's 87,941–150,703 zeros per wave are the
  `fillna(0)`. Dropping non-positive rows costs 0 money in every wave.
- **Nigeria has no blank-label leak**: 13 codes map to `''` in
  `nonfood_items.json` for 2010Q3..2013Q1, but none of them occurs in the data
  (no `''` column in any wave parquet), so the long melt invents no `j=''` key.
- **Dropping `m` from Nigeria is safe, measured**: 0 households carry more than
  one `m` in any wave, and `(t, i, j)` has 0 duplicates after the melt.
- **Nigeria wave values**: 79 exact zeros (2010-11: 56, 2012-13: 23), 0
  negatives, 0 infinities; the rest is NaN (the unstack's fill) or positive.
- **`data_scheme.yml` is hashed by name into `Country._table_cache_hash`**, so
  editing it cold-rebuilds EVERY Uganda and Nigeria table. Expected, not a
  defect (`CLAUDE.md` §"Cache Behavior"; same note as the Togo ledger).
- **`Nigeria/_/aggregate_items.json`'s `Aggregated Label` is a FOOD map**
  (Eggs, Apples, Avocado pear, …). The commented-out
  `x.rename(columns=lbl['Aggregated Label'])` in the nonfood country script
  points at the wrong file entirely; it is not scaffolding to revive.
- **`id_walk` can collide two old ids onto one new id** within a wave, which in
  long form makes `(t, i, j)` non-unique where the wide form merely overwrote a
  column. Assert after the walk.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| canonical schema block | **new** (modelled on Togo) | `data_info.yml` has none; Togo's `data_scheme.yml:281` is the shape the issue prescribes |
| `index_info` entry | **new** | `(t, v, i, j)`; `v` present ⇒ the existing `_join_v_from_sample` fires, no `skip_extra` entry needed |
| Uganda wave melt | **extend** `uganda.nonfood_expenditures` | same source reads and label map; only the reshape and the sparsity rule change |
| `t` stamping | **reuse** `food_acquired`'s wave-script pattern | `d['t'] = round` in the wave script, exactly as `2013-14/_/food_acquired.py:9` |
| Uganda country concat | **reuse** `Uganda/_/food_acquired.py` | concat + `id_walk` + `to_parquet`; the sibling is the tested pattern |
| Nigeria wave melt | **extend** the four wave scripts | same reads; `unstack` → long, names un-swapped, `m` dropped |
| aggregation to `Aggregate Label` | **not built** | out of scope; `labels=` is core machinery (see §6) |

## §6 Open questions for the human

- `labels='Aggregate'` on a non-food table: `Uganda/_/nonfood_items.org` has an
  `Aggregate Label` column, but the `labels=` machinery resolves against
  `food_items` / `harmonize_food`. Extending it is a **core** change
  (`country.py`), explicitly out of scope here — reported as a follow-up.
- Nigeria's `sect8a..e` / `sect11a..e` source files are *different recall
  windows* (the scripts' comments say "past 7 days" once and "past month" four
  times, copy-pasted). Nigeria populates no `RecallWindow`, so its
  `Expenditure` pools windows. Out of scope for #817 (which is about shape),
  but it is the same defect class Togo's `RecallWindow` exists to prevent.

---
### Phase 3 — verification

Anchored on this ledger's own entries; anything not tied to one is out of scope
and is said so rather than padded.

- `lsms_library/data_info.yml` `Columns.nonfood_expenditures` +
  `Index Info.index_info.nonfood_expenditures` — **OK (§5 new, §2 Togo row)**.
  Modelled on `Togo/_/data_scheme.yml:281`, the only existing correct
  declaration; not a new convention. `required`/`monetary` are the two keys
  the loader and `test_schema_consistency.py:35` actually act on (§3).
- `uganda.nonfood_expenditures` — **OK (§5 extend, §3 sparsity/min_count)**.
  Same reads, same label map, same arithmetic; only the reshape and the
  zero-handling changed. `sum(min_count=1)` is the convention already in force
  at `uganda.py:501` (§2), not a new one. Not a REINVENTION of
  `food_expenditures_from_acquired`: that derives food expenditure from
  `food_acquired`'s `(t,i,j,u,s)` grain; this reads a non-food module that has
  no `u` and no `s`. Only the sparsity RULE is shared, and it is cited.
- The eight `Uganda/*/_/nonfood_expenditures.py` — **OK (§5 reuse)**. `t` is
  stamped by the wave script, verbatim the `2013-14/_/food_acquired.py:9`
  pattern (§2). The `round =` name shadows a builtin; that is the sibling's
  existing spelling and was kept rather than diverged from.
- `Uganda/2015-16/_/nonfood_expenditures.py` hhid fix-up — **OK (§4 swapped
  names)**. Re-keyed from `j` to `i` because `i` is now the household. Added
  the unmapped-id assertion the ledger's §4 warned about (`replace` leaves a
  miss in place silently); it passes with 0 unmapped.
- `Uganda/_/nonfood_expenditures.py` — **OK (§5 reuse)**. Now structurally the
  same file as the sibling `Uganda/_/food_acquired.py` (§2): concat wave
  parquets → `id_walk` → `to_parquet`. The post-`id_walk` duplicate assertion
  is the §4 collision landmine, made loud; it passes (0 duplicates).
- The four `Nigeria/*/_/nonfood_expenditures.py` — **OK (§5 extend, §3 pp-ph)**.
  `t` stays per-round, so no round is pooled. `m` dropped on the §4 measurement
  (0 households with >1 zone; 0 `(t,i,j)` duplicates), and the assertion in
  each script is what keeps that measured rather than assumed.
- `Nigeria/_/nonfood_expenditures.py` — **OK (§5 extend, §4 aggregate_items)**.
  `replace(inf, nan)` kept, per §2. The commented-out
  `aggregate_items.json` rename removed on the §4 finding that its
  `Aggregated Label` is a FOOD map — i.e. deleting it prevents a future
  REINVENTION-by-revival, it is not itself one.
- Both `_/data_scheme.yml` — **OK (§3 dict-declaration rule)**. A bare `!make`
  is a dict to the schema test, so it would have failed
  `test_required_columns_present` the moment `Expenditure` became required;
  they now declare `index: (t, i, j)` + `Expenditure: float`.
- `tests/test_nonfood_expenditures_schema.py` — **OK (§4 totals invariant)**.
  Pins the shape, the no-fabricated-zeros rule and the seventeen per-wave
  totals §4 records. Declarers are DISCOVERED from disk, not listed, so a
  fourth country meets the contract on arrival.
- `tests/test_visualizations.py::test_uganda_wide_nonfood_table_raises_citing_gh817`
  — **CONTRADICTION, resolved (§1)**. It asserted Uganda's table IS wide, i.e.
  it pinned the bug this task removes. Split rather than deleted: the guard is
  now exercised on a synthetic wide frame (so it survives any corpus change)
  and Uganda is asserted long-and-drawing. Neither half is weaker than the
  original — the original tested one thing, the pair tests two.

**CORRECTION to §6, made during verification.** The first draft of the Uganda
`CONTENTS.org` and `data_scheme.yml` comments asserted that Uganda's module
"asks one amount per item over a single window, so there is nothing to
disambiguate". That is an *unevidenced closing negative* of exactly the kind
`CLAUDE.md` §"Adjudicating `absent` cells" forbids (the Albania mistake), and
it is also **false**: the `.dta` variable labels read via
`pyreadstat.read_dta(..., metadataonly=True)` carry a CAPI fill — `h15cq5`
= *"How much came from purchases in the past [RECALL]? VALUE"* (2013-14 and
2011-12, identical) — so the recall period **varies by item**, as in Togo's
section 9, and this table sums across it. No wave ships a recall variable and
`nonfood_items.org` has no recall column, so it is a real gap needing the
questionnaire. Both files now say so; the claim was corrected before the fix
commit was final. Nigeria's equivalent note was hedged from the start and
stands.

**Not verified here, stated as such:** the *values* themselves are unchanged by
construction (the pivot summed the same numbers and every per-wave total
matches), but no cell of Uganda's or Nigeria's `nonfood_expenditures` has been
read against its questionnaire. Neither country is `blessed`, and this task
does not bless one.
