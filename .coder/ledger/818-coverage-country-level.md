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

---
### Phase 3 — verification (fill at task end)
- *(filled at task end)*
