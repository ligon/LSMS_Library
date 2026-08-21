# Prior-Art Ledger — GH #323 (Nigeria)

**Search tier used:** ripgrep + git floor (the GitNexus MCP server is not mounted
in this subagent; blast radius established by direct call-site enumeration
instead — `_normalize_dataframe_index` has 4 call sites, all internal to
`country.py`).

## §1 Task, restated

`_normalize_dataframe_index` collapses a non-unique DECLARED index with
`groupby().first()`, silently discarding the dropped rows. For Nigeria the audit
reports 10 affected cells: `cluster_features` in all 8 W1–W4 quarters, and
`assets` in 2012Q3 + 2013Q1. The task is to fix the *cause* in Nigeria's config,
not to paper over the symptom, and to leave the collapse either impossible or
explicitly DECLARED.

Two independent root causes, both `INDEX_INCOMPLETE`:

1. **`v = ea` is not a cluster id.** In the GHS-Panel, `ea` is a serial unique
   only *within* an LGA. W1's design is 500 EAs × 10 HH = 5,000 households, but
   `nunique(ea) = 411`; `nunique(state, lga, ea) = 500`. So the key MERGES
   distinct clusters, and the household→cluster collapse then stamps one
   arbitrary EA's Region/District/Rural onto every household in the merged group
   (890/5000 wrong District in W1 alone; 1283/5263 by W4). Class-1, silently
   WRONG — and it leaks everywhere, because `sample()` publishes the same key and
   `_join_v_from_sample()` stamps it onto every household table.
2. **`assets` W2 drops `item_seq`.** `sect5b_plantingw2` is a PER-UNIT roster
   (one row per individual unit owned, each with its own age and resale value);
   `dup(hhid,item_cd) = 14,493`, `dup(hhid,item_cd,item_seq) = 0`. `.first()`
   kept unit #1 and discarded the rest: **₦147,297,485 (25.6%)** of reported
   asset value, in each of the wave's two quarters.

Two defects sat on top of (1): the `dfs:` block merged two HOUSEHOLD-level frames
on `v` (a cartesian product within each EA — W2's `cluster_features` was 62,538
rows from 4,859 households), and the geo sub-df was silently dropped in 3 of 4
waves (wrong column casing in W1/W3; W4's geovars file has no `ea` column at
all), so Latitude/Longitude were simply MISSING.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4100` | reorders/drops index levels, then collapses duplicates with `.first()` (or `sum` for `_ADDITIVE_MEASURE_COLUMNS`) | yes | **leave unchanged** (D1: core does not aggregate). The composite key makes its collapse lossless for Region/District/Rural |
| `aggregation:` key | `Albania/_/data_scheme.yml:83` + 7 others | **DEAD**: appears only in the `_skip` meta-key sets (`country.py:2387`, `diagnostics.py:230`); parsed and discarded, never enforced | no | **do NOT use** — D1 rejects making it real; it stays dead config, so Nigeria declares none |
| `Wave.cluster_features` | `country.py:1168` | GH #161: when `i` is in the index, collapses HH→cluster with `.first()` / **`.mean()` for Lat/Lon** | partly | **leave unchanged** — this is #323 Site 2, owned centrally (PR #617), not per-country |
| `_join_v_from_sample` | `country.py:~1620` | left-merges `sample().v` onto every household table | yes | reuse unchanged (but see §4) |
| `community_prices_for_wave` | `nigeria.py:~1990` | builds `v = format_id(ea)` from the community questionnaire | yes | **extend** — same keyspace or the join dies |
| `df_data_grabber` "Trickier" form | `local_tools.py:1034` | `{newvar: ([cols], fn)}` → row-wise transform; auto-bound by NAME (Benin's `i()`) | yes | **reuse** — one `nigeria.v()` serves sample + cluster_features |
| `converted_categoricals:` | `country.py:933` | per-sub-df flag; when set, `get_dataframe(convert_categoricals=False)` → raw CODES | yes (EthiopiaRHS, Albania) | **reuse** — needed for the code-space key |

## §3 Definitions & conventions in force

- `sample` is the single source of truth for household→cluster; `v` lives there
  and is joined at API time. Per `CLAUDE.md` "`sample()` and Cluster Identity".
- Canonical `assets` grain is **`(t, i, j)`** — `lsms_library/data_info.yml`,
  `Index Info > index_info`. A 4-level Nigeria would break `Feature('assets')`
  assembly against every 3-level country (the same hazard CLAUDE.md documents for
  `food_expenditures`' `s` level).
- Canonical `cluster_features` grain is `(t, v)`; `cluster_features` OWNS `v`.
- class-2 (silently MISSING) is strictly safer than class-1 (silently WRONG).

## §4 Invariants & assumptions

- **A NaN `v` is NOT a safe "missing" value.** `_join_v_from_sample` left-merges,
  so rows survive — but the guard at `country.py:~1272` documents that the
  downstream `groupby()` in `roster_to_characteristics` / the food-derivation
  pipeline **drops NaN keys**, silently swallowing those households from every
  derived table. So the `ea == 0` "Moved" households cannot be given `v = NA`;
  that would trade one silent-data-loss bug for another. They get an explicit
  singleton (`moved-{hhid}`) instead — present, never pooled, never dropped.
- **The keyspace must be built from CODES, not labels.** Measured: in W2 the
  community questionnaire's `state`/`lga` carry no Stata value labels while the
  household file's do, so a label-built composite gives **0%** overlap between
  `community_prices.v` and `sample.v`; a code-built one gives **100%**. A
  label-built key would have silently severed community prices from their
  households.
- `_finalize_result` does `df.dropna(how='all')` (`country.py:2217`): a row whose
  every column the `unique` reducer NA's out disappears entirely. Harmless with a
  correct key (Region/District are functions of the key), but it is why a
  half-migrated state showed W2 at 377 instead of 410 groups.
- `@build_transform()` (`_build_registry.py:69`) folds the decorated function's
  SOURCE into every table's cache fingerprint. Editing `_normalize_dataframe_index`
  invalidates **all** cached parquets → the next build rebuilds from source. Any
  before/after comparison must therefore rebuild both sides from source, or it
  compares fresh output against a stale cache and reports phantom regressions.

## §5 Reuse decision

| quantity | decision | why |
|---|---|---|
| cluster id `v` | **new** (`nigeria.cluster_id` / `nigeria.v`) | no existing composite-key helper; auto-bound by name so `sample`, `cluster_features` and `community_prices` cannot drift apart |
| HH→cluster collapse | **leave to core** | Ethan's D1 (2026-07-13): core does NOT aggregate on declaration. The composite key makes the existing `.first()` collapse *provably lossless* for Region/District/Rural, which is the whole point — fix the KEY, don't declare a reducer over a broken one |
| `assets` item_seq | **not fixed here** — reported instead | needs two CORE edits (canonical `index_info` + `feature.py`); see §7 |
| Lat/Lon reducer | **leave to core (Site 2)** | `mean()` is retained; auditing the HH→cluster collapse is owned centrally, not per-country |

## §6 What was built  *(config only — `lsms_library/*.py` is UNTOUCHED)*

This branch is the **config-only port** of `fix/323-nigeria`, off `origin/development`.
The original branch's +139-line `country.py` patch (`_aggregation_policy`,
`_apply_aggregation_policy`, `_unique_or_na`, and a `Wave.cluster_features` hunk)
was **stripped** under D1, together with both `aggregation:` keys it fed
(`cluster_features`, `assets`) — with core stripped those are dead config, since
`aggregation:` lives only in core's `_skip` meta-key set.

- **`nigeria.py`** — `cluster_id(state, lga, ea, hhid)` + the auto-bound `v(row)`;
  `community_prices_for_wave` builds `v` through the same helper.
- **wave YAMLs (all 5)** — `v: [state, lga, ea, hhid]` via a `converted_categoricals`
  `df_key` sub-df (raw codes) alongside the labelled `df_main`; geo joined on `i`
  (not `v`) — un-cartesianing the merge; per-wave geo column casing corrected.
  W5's `sample` is fixed too: it has no `cluster_features`, but its `sample.v` is
  stamped on every household table and was equally broken.
- **`data_scheme.yml`** — no `aggregation:` key. Comments record *why* the collapse
  is now sound (the key, not a reducer), and carry the `assets` open defect.

### Measured, per round (`LSMS_NO_CACHE=1`, vs. a real `development` baseline build)

`v` nunique in `sample`, **development → this branch**:

| wave | pp quarter | ph quarter |
|---|---|---|
| 2010-11 | 2010Q3: 411 → **500** | 2011Q1: 411 → **500** |
| 2012-13 | 2012Q3: 408 → 647 | 2013Q1: 408 → 647 |
| 2015-16 | 2015Q3: 402 → 792 | 2016Q1: 402 → 792 |
| 2018-19 | 2018Q3: 405 → 685 | 2019Q1: 405 → 685 |
| 2023-24 | 2023Q3: 404 → 911 | 2024Q1: 404 → 911 |

W1's 500-EA design is recovered **in both rounds**. (Post-W1 counts include the
`moved-{hhid}` singletons: e.g. W3 = 486 real clusters + 306 movers = 792.)

`cluster_features` Latitude present, **development → this branch**: 2010Q3 0 → 500,
2012Q3 409 → 645, 2015Q3 0 → 784, 2018Q3 0 → 671 (and identically in each wave's
ph quarter). Coordinates recovered in 3 of the 4 waves that declare the table.

### Round safety (the pp/ph question)

- **`v` is round-invariant**: 0 households change cluster between the pp and ph
  quarter, in all 5 waves. It holds *by construction* — a wave folder has ONE
  `data_info.yml` and both `Wave(t)`s read the SAME post-planting cover page, so
  there is no second geo-coding to drift from.
- Against the *raw* cover pages the only pp/ph disagreement is the `Moved`
  sentinel: 0 / 17 / 41 / 66 / 64 households (W1…W5) are in a real EA at planting
  and coded `ea == 0` at harvest, i.e. they relocated between the two visits.
  **Zero** real-EA→real-EA disagreements — so no geo-recode, no label/code drift,
  no `.0` float-suffix artefact. Using the planting frame's cluster for the whole
  wave is the right call: `v` is the *sampling* cluster, fixed by the frame.
- The `Moved` sentinel fires when it should and **only** when it should: 0 times in
  W1 (nobody can have moved out of a frame they were just drawn from), and
  152/306/166/393 times in W2–W5.
- `cluster_features` carries **both** quarters with identical cluster sets and zero
  attribute mismatches — neither round overwrites the other. 0 duplicate `(t,v)`.
- `community_prices` is correctly post-harvest-only. Structurally verified: across
  all 5 waves there are **11 `sectc8*` files and every one is `_harvest`** — no
  `sectc8*_planting*` exists. The planting community files that *do* carry
  `item_cd` (`sectc2_plantingw1`, `sectc2a/b_plantingw3`) have **no price column**;
  C2 is an availability module. No planting-round price data is being dropped.
- `sample` grain is unchanged by the rekey: 49,770 rows on `development` and on this
  branch — one row per (household, round), 0 duplicate `(i,t)`.

## §7 Residuals / honest gaps

- **`assets` item_seq is NOT fixed and is still broken.** W2's per-unit roster
  (`sect5b_plantingw2`, `item_seq` 1..15) is collapsed by `groupby().first()`,
  keeping unit #1: true ₦576,299,043 → kept ₦429,001,558 → **₦147,297,485 (25.6%)
  destroyed**, in *each* of 2012Q3 and 2013Q1. The original branch fixed this with
  an `aggregation:` reducer, which D1 rejects. Declaring `item_seq` in Nigeria's
  index does **not** close it either — *verified*: the canonical assets grain is
  `(t,i,j)`, so `Feature('assets')` calls `_harmonize_country_frame`, drops the
  extra level, and `_collapse_duplicate_index` re-reduces with `first()` (assets is
  not in `_ADDITIVE_MEASURE_COLUMNS`). A 4-level Nigeria would fix
  `Country.assets()`, leave `Feature('assets')` destroying the identical ₦147M, and
  make Nigeria the library's only non-canonical assets grain. The real fix is two
  CORE edits and belongs on its own PR:
  1. `lsms_library/data_info.yml` → `index_info: assets: (t, i, j, item_seq)`
  2. `lsms_library/feature.py` → `_ADDITIVE_MEASURE_COLUMNS['assets'] = ('Value',)`
     (`Quantity` must stay `first` — it comes from the clean `sect5a` hh×item grid
     and is merely repeated across the `item_seq` rows; `Age` wants `mean`.)
  Recorded as a KNOWN OPEN DEFECT block in `Nigeria/_/data_scheme.yml`.
- **Lat/Lon still collapse with `.mean()`** (core's Site-2 default). 5 / 24 / 50
  clusters in W2 / W3 / W4 span more than one coordinate — the panel keeps a moved
  household's ORIGINAL `ea` in the frame while the geovariables record its CURRENT
  dwelling, so households up to ~890 km apart get a centroid in neither place. The
  original branch NA'd these via a `unique` reducer; under D1 that is core's Site 2
  to own, not Nigeria's. Behaviour here is the historical one, not a regression.
- **`moved-` singletons inflate `cluster_features` row counts** (e.g. W3: 792 rows =
  486 real clusters + 306 one-household "clusters"). Deliberate: a NaN `v` would be
  dropped by the downstream `groupby()` and delete those households from every
  derived table. Analysts computing cluster-level means should be aware.
- **`v` is wave-local, not panel-stable.** LGA codes are recoded between waves, so
  the composite is comparable within a wave, not across. The previous `v = ea` was
  numerically comparable across waves but *meaningless* (it conflated clusters), so
  no real cross-wave linkage is lost.
- **~0.2–0.4% of `community_prices` clusters do not match a `sample` cluster** in
  W2/W3 (join rate 99.6% / 99.8%; 100% in W1/W4/W5) — community questionnaires
  administered in EAs where no sampled household resolved. Left visible rather than
  force-matched.
- **W5 declares no `cluster_features`**, so `community_prices` at 2024Q1 joins
  `sample.v` at 100% but `cluster_features` at 0%. Pre-existing gap, not a
  regression from this port.
- W1 `assets` still has no `Value`/`Age` (the W1 YAML never extracted them; the
  source section has no such columns). Pre-existing, unrelated to #323.
