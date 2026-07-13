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
| `_normalize_dataframe_index` | `country.py:4100` | reorders/drops index levels, then collapses duplicates with `.first()` (or `sum` for `_ADDITIVE_MEASURE_COLUMNS`) | yes | **extend** — teach it the declared `aggregation:` policy |
| `aggregation:` key | `Albania/_/data_scheme.yml:83` + 7 others | **DEAD**: appears only in the `_skip` meta-key sets (`country.py:2387`, `diagnostics.py:230`); parsed and discarded, never enforced | no | **make real** (prose → enforcement) |
| `Wave.cluster_features` | `country.py:1168` | GH #161: when `i` is in the index, collapses HH→cluster with `.first()` / **`.mean()` for Lat/Lon** | partly | **extend** — a declared policy must win |
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
| HH→cluster collapse | **extend** existing `aggregation:` key | the declaration already existed and was dead; adding a second mechanism would leave the first still lying |
| `assets` item_seq | **extend** `aggregation:` with a per-column reducer | canonical `(t,i,j)` grain must hold for `Feature('assets')`; a blanket sum would multiply `Quantity` by the unit count |
| Lat/Lon reducer | **new** `unique` reducer | `first` picks arbitrarily and `mean` invents a point between households up to 890 km apart; both are class-1 |

## §6 What was built

- **`country.py`** — `aggregation:` is now ENFORCED. `_aggregation_policy()` parses
  it; `_apply_aggregation_policy()` applies a per-column reducer
  (`sum`/`first`/`last`/`mean`/`min`/`max`/`unique`). `sum` uses `min_count=1`
  ("not reported" must not become a reported 0). `unique` is invariance-checked:
  constant → keep, disagreement → `<NA>` **plus a warning naming the column and
  the group count**. A column with no declared reducer is reported, not silently
  `first`-ed. `Wave.cluster_features` now lets a declared policy win over its
  `first`/`mean` defaults.
- **`nigeria.py`** — `cluster_id(state, lga, ea, hhid)` + the auto-bound `v(row)`;
  `community_prices_for_wave` builds `v` through the same helper.
- **wave YAMLs (all 5)** — `v: [state, lga, ea, hhid]` via a `converted_categoricals`
  `df_key` sub-df (raw codes) alongside the labelled `df_main`; geo joined on `i`
  (not `v`); per-wave geo column casing corrected. W5's `sample` is fixed too: it
  has no `cluster_features`, but its `sample.v` is stamped on every household
  table and was equally broken (404 bare-`ea` codes for 719 real clusters).
- **`data_scheme.yml`** — declared `aggregation:` for `cluster_features`
  (all-`unique`) and `assets` (`Value: sum`, `Quantity: first`, `Age: mean`).

## §7 Residuals / honest gaps

- **5 / 24 / 50 clusters in W2 / W3 / W4 get `<NA>` coordinates** (out of
  495/486/519). Their households sit up to ~890 km apart — the panel keeps a
  moved household's ORIGINAL `ea` in the sampling frame while the geovariables
  record its CURRENT dwelling. There is no defensible single coordinate, so the
  reducer refuses to pick one. W1 (where the data is clean) keeps all 500.
- **`v` is wave-local, not panel-stable.** LGA codes are recoded between waves, so
  the composite is comparable within a wave, not across. The previous `v = ea` was
  numerically comparable across waves but *meaningless* (it conflated clusters),
  so no real cross-wave linkage is lost.
- **3 of 2,501 `community_prices` clusters (0.1%) do not match a `sample` cluster** —
  community questionnaires administered in EAs where no sampled household resolved.
  Left visible rather than force-matched.
- W1 `assets` still has no `Value`/`Age` (the W1 YAML never extracted them; the
  source section has no such columns). Pre-existing, unrelated to #323.
