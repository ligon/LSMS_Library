# Prior-Art Ledger — GH #842 (Niger half): `Unknown` as the canonical `u` sentinel

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`; cites
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (`CONNECTION_CLOSED`) and this mirror carries no
`.gitnexus/` index, so the substitutes named in CLAUDE.md §"Code intelligence"
were used: `rg` over `lsms_library/` and `lsms_library/countries/*/_/` for every
symbol touched, plus the test suite for the touched area. Declared, not skipped.

## §1 Task, restated

Niger serves a genuine NULL on the declared index level `u` — 441 rows of
`crop_production` and 12 of `food_acquired` (#842, re-verified against the live
API 2026-09-08). The row is *served*, so no build-time guard fires; it is
deleted later by whichever `groupby` the consumer reaches first, because pandas
`groupby(dropna=True)` drops NaN keys. That is the deferred silent deletion #847
describes at the framework level. This task is the **country-level** half for
Niger: fill a missing unit with a non-null sentinel at build time, in every
Niger table whose declared index carries `u` — `crop_production` (t,i,plot,crop,u),
`food_acquired` (t,v,i,j,u,s), `plot_inputs` (t,i,input,crop,u), and
`community_prices` (t,v,j,u) — and settle the sentinel's *name* as `Unknown`,
the corpus-canonical spelling.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_finish_plot_inputs` | `countries/Niger/_/niger.py:576` | the pattern this task generalises: `crop` filled with `CROP_NA`, `u` filled with `'Manquant'`, both for the NaN-index-key reason | no direct test | **extend** — factor the `u` fill into one helper |
| `CROP_NA` | `countries/Niger/_/niger.py:573` | `'(not crop-specific)'`; its comment block *is* the design rationale for #842 | no | cite, don't touch |
| `_finish_crop_production` | `countries/Niger/_/niger.py:462` | common tail for all four waves' `crop_production`; sets the `u` index without filling | no | **extend** — one call site covers 4 waves |
| `_unit_labels` | `countries/Niger/_/niger.py:445` | maps a raw unit column through the country `u` table; NA in → NA out | no | reuse unchanged |
| `_finish_community_prices` | `countries/Niger/_/niger.py:1227` | already *requires* `u` non-null (`df['u'].notna()` filter) | no | **verify only** |
| `_COMMUNITY_MISSING_UNITS` | `countries/Niger/_/niger.py:1183` | `{'Manquant'}` — drops CS07 "produit absent" rows, keyed on the **Preferred Label** | no | **must move with the label** (silent break otherwise) |
| `food_acquired_to_canonical` | `lsms_library/transformations.py` | Phase-3 s-axis reshape; Niger EHCVM waves alias/wrap it in `mapping.py` | yes | **out of scope to edit** — wrap in the wave `mapping.py` instead |
| `uganda.py` crop `u` fill | `countries/Uganda/_/uganda.py:1386` | `df['u'].where(df['u'].notna(), 'Unknown')` — the corpus precedent for the *name* | `tests/test_uganda_crop_condition.py` | **reuse the name** |
| `crop_production.condition` | `data_info.yml:452-505` | an INDEX level declared under `Columns:` with a `spellings` vocabulary + `unknown_condition` sentinel; its note states the same NaN-index-key rule | `tests/test_uganda_crop_condition.py` | **precedent for where to declare** |
| `_apply_categorical_mappings` | `country.py:2602` (called `country.py:2950`) | read-path auto-mapper: an index level named `u` is relabelled through the country's `u` table | yes | **the read-path relabel mechanism** |
| `_enforce_canonical_spellings` | `country.py:4956` (called `country.py:2980`) | read-path; applies `data_info.yml` `spellings` to columns AND MultiIndex levels | `tests/test_declared_spellings.py` | **disqualified** — see §4 |
| `_row_union_categorical` / `_ADDITIVE_CATEGORICAL_TABLES` | `country.py:299`, `country.py:289` | global `categorical_mapping/u.org` is row-unioned into the country table, **country row wins on key collision** | yes | **disqualifies the global-u.org route** — see §4 |
| `_check_declared_spellings` | `diagnostics.py:447` | treats `spellings.keys()` as a CLOSED vocabulary and `fail`s any value outside it, on index levels too | `tests/test_declared_spellings.py` | **the blocker** — see §4 |

## §3 Definitions & conventions in force

- Repo-wide baseline: `STANDING.md` §3/§4; IO sanctions and cache tiers per
  `CLAUDE.md`.
- `Unknown` as a non-ordinal sentinel: `lsms_library/data_info.yml:294-297`
  (`individual_education.Educational Attainment` note — "`Unknown` is the
  non-ordinal sentinel for don't-know / refused / unmappable").
- A null index key is deleted, not preserved: `CLAUDE.md` §"Grain Collapse"
  §3b, and `data_info.yml:490-494` (`condition`: "it is a real value, never NA,
  because a null index key is silently dropped by the duplicate collapse").
- Cache-hash inputs (which edits invalidate what): `CLAUDE.md` §"Cache
  Behavior" — country `_/*.org` and `_/{table}.py` are hashed; the canonical
  `lsms_library/data_info.yml` and `lsms_library/categorical_mapping/*.org`
  are **not**, and `_finalize_result` / categorical mappings / spellings are in
  `_build_registry._EXCLUDED_CALLABLES`.
- Niger idiosyncrasies: `countries/Niger/_/CONTENTS.org` — EHCVM `v = grappe`,
  `i = (grappe, menage)`; `crop_production.u` is the HARVEST unit and the SOLD
  unit is a different, undropped column; CS07 `Quantity` is a weight;
  `food_acquired` and `community_prices` never share a wave.

## §4 Invariants & assumptions

- **The `u` vocabulary is OPEN, corpus-wide** (GH #223 defers cross-country
  unit harmonization; `categorical_mapping/u.org` harmonizes `Kg` only). So a
  `spellings:` block on `u` in `data_info.yml` is **not** an option: any
  `spellings` block is read by `diagnostics._load_declared_vocabularies` as a
  *closed* vocabulary and `_check_declared_spellings` `fail`s every value
  outside it — which would be every real unit, in every food/crop country.
  Measured, not assumed (see the Phase 3 note).
- **A global `categorical_mapping/u.org` row cannot relabel Niger.**
  `_row_union_categorical` keeps the COUNTRY row on a source-label collision,
  and `Niger/_/categorical_mapping.org` already declares
  `Manquant -> Manquant`. A global `Manquant -> Unknown` row is a no-op there.
- **Niger's `u` table is read on BOTH paths**: build-time by
  `local_tools.get_categorical_mapping` (country file only, no global merge)
  and read-time by `Country._apply_categorical_mappings` (global + country).
  One table, two paths — so it cannot yield `Manquant` at build and `Unknown`
  at read. Changing its Preferred Label therefore *both* makes the parquet
  canonical and relabels any legacy parquet on read.
- `_COMMUNITY_MISSING_UNITS` keys on the **Preferred Label**, so it must be
  updated in the same commit as the label, or Niger silently stops dropping
  CS07 "produit absent" rows.
- `_RESERVED_U_SENTINELS = {'kg', 'Value'}` (`country.py:96`) — the
  `protect_u_sentinels` guard for `food_prices`/`food_quantities` does not
  cover `Unknown`, and does not need to: `Unknown` is not an `Original Label`
  in any `u` table, so the mapper passes it through unchanged.
- Fill only where the raw unit is missing on a row the script KEEPS. Do not
  resurrect rows already dropped for having no reported amount.
- Row counts before == after, for every table. The sentinel changes a value,
  never a row.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| sentinel NAME | **reuse** `Unknown` | already the corpus sentinel (`data_info.yml:296` education; `uganda.py:1386` writes it into `crop_production.u`) |
| the fill itself | **extend** `_finish_plot_inputs`'s pattern into one `niger.py` helper | four tables, one rule; four copies is how the spellings drift |
| API-time relabel | **reuse** the categorical mapper, at Niger's own `u` table | the corpus's actual read-path mechanism for `u`; the two alternatives are disqualified in §4 |
| build-time output | **write `Unknown` directly** | forced by §4 (one table serves both paths) and matched to Uganda's precedent; internal consistency is achieved by using ONE spelling everywhere in Niger, which is the point of the change |
| declaration of the canon | **new**, docs-only | a `note`/comment beside `u: unit` in `data_info.yml`'s index vocabulary — the machine-readable `spellings` route is disqualified |

## §6 Open questions for the human

- `community_prices` drops rows whose unit is the CS07 "produit absent"
  missing-marker. Those rows are genuinely price-less, so the drop is right —
  but it means `Unknown` is *both* "unit not recorded" (kept, elsewhere) and
  the marker for a dropped row (community_prices only). If CS07 ever carries a
  real price with an unrecorded unit, the two meanings collide. Measured for
  this branch; flagged, not redesigned.
- #847 (the framework-side NaN-index-key report) remains the general fix. This
  ledger's scope is Niger only.

---
### Phase 3 — verification (fill at task end)
