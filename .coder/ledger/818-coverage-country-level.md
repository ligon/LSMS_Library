# Prior-Art Ledger — #818 coverage grader: country-level-vs-per-wave by declaration, not by name

> Per-task ledger (copy of `TEMPLATE.md`). Living, git-tracked snapshot of the
> machinery, definitions, and conventions that bear on THIS task. Edit in place;
> git history is the journal. Inherits the repo §0 baseline in `STANDING.md` —
> cites it, `CLAUDE.md`, and `lsms_library/data_info.yml` rather than re-copying.
> Sibling ledger for the same module: `.coder/ledger/coverage-matrix.md`.

**Search tier used:** ripgrep + git floor. GitNexus MCP did not connect this
session (`CONNECTION_CLOSED`) and this mirror has no `.gitnexus/` index, so the
substitutes named in `CLAUDE.md` §"Code intelligence" were used and the blast
radius below was found with `rg` + a config-only sweep over all 40 countries.

## §1 Task, restated
`Country('EthiopiaRHS').community_prices()` returns 2,691 rows at `(t, v, j, u)`
covering six of the country's eight waves, yet `.coder/coverage/latest.csv`
grades all eight `absent` ("source not declared for wave"). The table is built by
a **country-level** `EthiopiaRHS/_/community_prices.py` (`materialize: make` in
the country `data_scheme.yml`) and appears in **no** wave's `data_info.yml`, so
`wave_available_features` — which lifts `Wave.data_scheme` by the derived map —
never sees it. The grader already knows this shape, but only by a hard-coded
**feature-name** allowlist (`COUNTRY_LEVEL_ONLY`), onto which `nutrition` was
added by hand. Replace the name key with the **declaration test**: country-level
for a `(country, feature)` iff `_has_country_level_feature()` and the feature is
in no wave's `data_scheme`. Whether a table is country-level is a property of the
`(country, feature)` declaration, not of the feature name — `community_prices` is
per-wave in seven countries and country-level in one.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_has_country_level_feature` | `lsms_library/coverage_matrix.py:480` | `_/{feature}.py` or `_/{feature}.json` exists under the country dir | no direct test | **reuse** — this is the declaration test the issue points at; already used by the `country_level_only` loop in `build_matrix:812` |
| `wave_available_features` | `coverage_matrix.py:455` | `set(Wave.data_scheme)` lifted by `_DERIVED_SOURCE` | yes (unit fakes) | **reuse** — the other half of the test (feature in NO wave's set) |
| `_env()["COUNTRY_LEVEL_ONLY"]` | `coverage_matrix.py:436` | `frozenset(JSON_CACHE_METHODS) \| {"nutrition"}` — the name allowlist | no | **narrow** to `frozenset(JSON_CACHE_METHODS)` |
| `country.JSON_CACHE_METHODS` | `lsms_library/country.py:75` | `{panel_ids, updated_ids}` — `@property` dicts, no `t` axis, not DataFrames | — | **reuse unchanged**; these genuinely cannot be graded per wave |
| `grade_feature` | `coverage_matrix.py:508` | builds the country table once, slices on `t`, grades each wave with `is_this_feature_sane` | yes | **extend** with a `country_level=` kwarg that seeds `covered` |
| `grade_country_level` | `coverage_matrix.py:666` | one `wave=None` cell; **never calls `is_this_feature_sane`** — non-empty ⇒ `sane` | no direct test | **keep**, but only `JSON_CACHE_METHODS` routes here now |
| `build_matrix` | `coverage_matrix.py:720` | owns `avail_by_wave` (computed once per country) and `countries_root` | yes (integration) | **extend** — classify there and pass the flag down |
| `_absent_tier` | `coverage_matrix.py:227` | the single place `absent` / `undeclared` is decided, incl. verdicts | yes | **reuse untouched** |
| `diagnostics.is_this_feature_sane` | `lsms_library/diagnostics.py:591` | 16-check `SanityReport` on any frame incl. a wave slice | yes | **reuse untouched** |
| `save_snapshot` | `coverage_matrix.py:834` | upsert on `(country, feature, wave)`; deletes stale rows only for fully-graded countries | yes | **reuse untouched** — but see §4: the key change for `nutrition` |

## §3 Definitions & conventions in force
- **`absent`** = "feature applies to the country but its source is **not declared
  for this wave**", the *live, un-adjudicated* queue — `coverage_matrix.py:33-36`
  and `docs/guide/coverage.md:34`. Using it for a wave that a country-level build
  actually serves is a category error, which is the whole bug.
- **`n/a`** = "no per-wave readiness applies" — `docs/guide/coverage.md:33`. Per
  the module docstring it "says *don't look*", so it must not cover a cell that
  has per-wave content.
- **A regrade is not a fix** — module docstring, `coverage_matrix.py:56-58`:
  a change that alters how cells are GRADED must first inventory what the old
  grading was catching by accident (GH #591, #592). §4 and the Phase-3 note below
  carry that inventory.
- **`Wave.data_scheme` omits derived tables**; the per-wave coverage oracle is
  `wave_available_features`, not `Country.data_scheme` — per
  `.coder/ledger/coverage-matrix.md` §2/§4.
- **Script path (`materialize: make`)** is the scar tissue of a survey
  idiosyncrasy — `CLAUDE.md` §"Two Build Paths". A *country-level* script is the
  outer case: one script, all waves, no wave declaration at all.

## §4 Invariants & assumptions
- Access `Country(c).waves` **before** `Country(c)[w]` (`wave_folder_map` is a
  side effect) — `.coder/ledger/coverage-matrix.md` §4. `build_matrix` already does.
- `grade_feature`'s `env` is a plain dict, and `tests/test_coverage_matrix.py::_env`
  builds a **3-key fake** (`DERIVED_SOURCE`, `load_feature`, `is_this_feature_sane`).
  A new `env["countries_root"]` lookup inside `grade_feature` would `KeyError`
  every existing unit test ⇒ classify in `build_matrix`, pass a **bool kwarg**.
- `covered` is computed **before** the build, so a country-level feature's true
  per-wave coverage is unknowable until `t_values` exists. Seeding `covered`
  all-True and *refining* it from `t_values` is what makes the failure modes come
  out right (broken build stays visible; waves outside `t` fall back to `absent`,
  never `dropped` — the issue prescribes exactly this).
- **EthiopiaRHS is the only country promised warm in this session's cache.**
  Nigeria/Uganda/Guyana blast radius is established from config + a *read-only*
  peek at already-materialized `var/*.parquet`; nothing is rebuilt.
- **`nutrition` has a `t` axis.** Measured 2026-09-08: `Uganda/Ethiopia/GhanaLSS`
  nutrition are all indexed `(i, t, v)` with `t` = the country's waves. It is
  therefore a *same-shape sibling* of EthiopiaRHS `community_prices`, and the
  declaration test necessarily moves it to the per-wave path. Its `wave=''` row
  is replaced by per-wave rows — a **key change**, which `save_snapshot`'s #724
  deletion handles for a country graded without a `--features` filter but which a
  feature-scoped run leaves behind (it is deliberately non-authoritative).

## §5 Reuse decision

| quantity | decision (reuse / extend / new) | reason |
|----------|--------------------------------|--------|
| "is this `(country, feature)` country-level?" | **new**, 3-line `_is_country_level_only()` composing two existing helpers | `_has_country_level_feature` alone is not the test — it is true for six Nigeria features that also want per-wave grading; the "in no wave's `data_scheme`" half comes from the existing `avail_by_wave` |
| per-wave grade of a country-level table | **reuse** `grade_feature`'s existing `t`-slice loop verbatim | the issue says so explicitly: it "already builds the country-level table once and grades each wave by slicing on `t`" |
| sanity boundary | **reuse** `is_this_feature_sane` / `SanityReport.ok` | never re-implement checks (`coverage-matrix.md` §2) |
| the `absent` / `undeclared` / `blocked` decisions | **reuse untouched** | `_absent_tier` is "the single place `absent` is decided"; #724 and the blocked path must stay byte-identical |
| `panel_ids` / `updated_ids` routing | **reuse unchanged** name set | they return dicts with no `t` axis; `grade_country_level` is the only thing that can grade them |

## §6 Open questions for the human
- **`nutrition`'s control row changes, and cannot not change.** The brief asked
  for `Uganda,nutrition` to be unchanged; the issue's design makes it a per-wave
  cell. Kept faithful to the issue (no name list). Two facts for the decision:
  (a) nutrition's index is `(i, t, v)` — measured, not assumed; (b)
  `grade_country_level` **never runs a sanity check**, so today's
  `Uganda,nutrition,,sane` means only "non-empty". Per-wave grading is the first
  time nutrition is actually checked. Re-adding the name is a one-line revert if
  the dispatcher prefers the old row.
- The corpus-wide `latest.csv` is **not** regenerated here (a coordinator
  decision). The rows that would move are inventoried in the Phase-3 note.
- **Found while inventorying the regrade, not asked for:** Guatemala and Panama
  ship a `_/nutrition.py` they declare nowhere. The old allowlist built it
  regardless of `data_scheme`, so `load_feature` raised `AttributeError` and the
  matrix filed a `broken` cell on the *same* `(country, feature, wave)` key the
  #724 loop had already filled with `undeclared` -- two contradictory rows on one
  key, and 2 of the corpus's 8 `broken` cells. The declaration test removes both
  phantoms. Pinned as a key-uniqueness test, not a tier assertion.

---
### Phase 3 — verification (fill at task end)

- `_is_country_level_only` (new, `coverage_matrix.py:497`) — **OK (anchored on
  §2/§5)**. Composes the two existing oracles (`_has_country_level_feature`,
  `wave_available_features` via `avail_by_wave`); no new notion of coverage, and
  no third place where `absent` is decided (§3 — `_absent_tier` untouched).
- `grade_feature(country_level=)` (extended, `:541`) — **OK (anchored on §4)**.
  Reuses the existing `t`-slice loop verbatim; the only new statement is the
  `covered` seed and its refinement from `t_values`. A bool kwarg, not an `env`
  key, exactly as §4 requires (the unit-test fake env has three keys).
- `COUNTRY_LEVEL_ONLY` (narrowed to `frozenset(JSON_CACHE_METHODS)`, `:439`) —
  **OK (anchored on §2/§5)**. The remaining names are the two `@property` dicts
  with no wave axis; nothing else is keyed by name.
- **No REINVENTION**: `is_this_feature_sane`, `_absent_tier`, `load_blessed`,
  `load_verdicts`, `load_blocked`, `save_snapshot` and the whole #724
  `undeclared` loop are untouched. The `blocked` path is byte-identical: it is
  evaluated per wave inside the same loop and never consults `country_level`.
- **No CONTRADICTION**: a wave outside the built `t` reads `absent`, never
  `dropped` (§3 — `dropped` presupposes a declaration); a country-level build
  that raises stays `broken` rather than being quietly downgraded to `absent`
  (§3 — `n/a`/`absent` must not say "don't look" about a live defect).

**Regrade inventory** (the module docstring's "a regrade is not a fix" rule).
Cells that move from per-wave `absent` to a graded tier, corpus-wide, from the
declaration test over all 40 countries (config sweep) + the waves each built
table's `t` actually names:

| country | feature | waves moving | source of the wave list |
|---|---|---|---|
| EthiopiaRHS | `community_prices` | 6 of 8 (1994a, 1994b, 1995, 1997, 2004, 2009) | **measured** — graded in this branch, all 6 `sane`, 2,691 rows |
| Nigeria | `crop_production`, `anthropometry` | 5 of 10 each (the `Q1` post-harvest rounds) | cached `var/*.parquet` `t` values (read-only peek; not rebuilt) |
| Nigeria | `livestock`, `plot_inputs` | 5 of 10 each (the `Q3` post-planting rounds) | same |
| Nigeria | `people_last7days` | 5 of 10 | same |
| Nigeria | `plot_labor` | 4 of 10 | same |
| Uganda | `income` | ≤ 8, unmeasured (no cached parquet; not built here) | config only |
| Guyana | `assets` | 1 of 1 | cached parquet exists, `(t, i, j)` |

The PP/PH split is the reason Nigeria moves only half its cells: each wave dir
holds two rounds with distinct `t`, and a given table is fielded in one of them
(`CLAUDE.md` §"A script is a complication"). The other half stays `absent`,
correctly.

Cells that do **not** move, and why the second half of the test is load-bearing:
Nepal (`cluster_features`, `food_acquired`, the four food derivations,
`household_characteristics`, `household_roster`, `individual_education`,
`interview_date`), Peru (six features) and Uganda `fct` are all declared by no
wave **and** have no country-level script — genuinely un-written config. They
stay `absent`, which is the live queue where they belong.

Key changes rather than tier changes: `nutrition` for Uganda / Ethiopia /
GhanaLSS moves from one `wave=''` row to per-wave rows. Uganda 8 and Ethiopia 5
are fully covered (`t` names every wave); **GhanaLSS is 5 of 7** — its `t` omits
1987-88 and 1988-89, so the regrade ADDS two `absent` cells there. That is the
one place this change grows the live queue rather than shrinking it, and it is
an adjudication question for `absent_verdicts.csv`, not a config bug: the same
two waves are where `GhanaLSS/food_acquired`'s `Price` is 100% null (the
`NullReadWarning` in the test log). Same structure as EthiopiaRHS 1989/1999,
which the issue anticipated. Measured on Uganda: 8 × `sane`, "all checks pass",
n_rows summing to 23,801 — exactly the country total the single row carried. This is the first time nutrition is
sanity-checked at all: `grade_country_level` never calls
`is_this_feature_sane`, it returns `sane` for any non-empty result.

**Regeneration hazard for whoever refreshes `latest.csv`:** the `wave=''` →
per-wave move is a KEY change, and `save_snapshot`'s #724 stale-row deletion
fires only for a country graded WITHOUT a `--features` filter. A scoped
`--features nutrition` refresh would leave the old `Uganda,nutrition,,sane` row
sitting beside the 8 new ones. Regenerate per country, unscoped.

**`blocked` path: code unchanged, reachability slightly widened.** In the
broken-build branch `not covered[w]` is tested BEFORE `_blocked_cell(w)`, so a
newly-country-level cell now reaches the blocked check where it used to
short-circuit to `absent`. That is the direction the module demands (a blocked
source must stay visible), and there are zero live instances: every
declared-by-no-wave Nepal feature has `_has_country_level_feature = False`.

**Surprise, and the one thing a reviewer should look at:** the fix is a wider
regrade than the issue implies — 36 measured wave-cell lifts (6 EthiopiaRHS +
29 Nigeria + 1 Guyana) plus Uganda `income` (≤ 8, unbuilt), the 3 `nutrition`
key changes, and 2 phantom `broken` cells removed (corpus `broken` 8 → 6) — not
the 8 EthiopiaRHS cells the issue names. Every mover was verified to have both
halves of the test, and no cell moves to a *worse* tier: the new path can only
lift `absent`, never manufacture `dropped`. The two new GhanaLSS `absent` cells
above are the sole addition to the queue.
