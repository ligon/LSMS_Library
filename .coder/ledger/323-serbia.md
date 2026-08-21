# Prior-Art Ledger — GH #323 (Serbia instance)

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`.

**Search tier used:** ripgrep + git (gitnexus not consulted; the change is
config-only — no library symbol is edited, so there is no call-graph blast
radius to assess).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py:4100`) collapses a
non-unique **declared** index with `groupby().first()`, silently discarding the
losing rows. Serbia/2007 is one instance: `cluster_features` declares index
`(t, v)` with `v: popkrug`, but `popkrug` is a zero-padded serial number **local
to a municipality** (`opstina`), not a global cluster id. The census-district
key is the pair `(opstina, popkrug)`. `enumeration_district.dta` holds 510
districts but only 328 distinct `popkrug`, so 182 districts were being collapsed
away — and because the colliding districts *disagree* on their payload, the
survivors carried arbitrary Region/Rural. This is **class-1 (silently WRONG)**,
not class-2 (silently missing).

Fix: make `v` the composite settlement key `(opstina, popkrug)`, in
`cluster_features.idxvars` **and** `sample.df_hh.myvars` **together** (sample.v
is the join key onto `cluster_features(t, v)`), and drop the dead-and-wrong
`v: popkrug` from `household_roster.idxvars`.

Serbia is **one instance, not the class**. #323 stays open.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/drops index levels; `groupby().first()` collapse + #323 warning on a non-unique declared index | yes (`tests/test_normalize_index_j_preserved.py`) | **unchanged** — the defect is the Serbia config feeding it a non-unique key, not the collapse code |
| `_join_v_from_sample` | `lsms_library/country.py:1633` | joins `v` onto household tables from `sample()` | yes (`tests/test_join_v_silent_skip_warn.py`, `test_no_v_join_declarative.py`) | reuse — it is why roster needs no `v` of its own |
| `mapping.py:i()` | `countries/Serbia/2007/_/mapping.py` | composite household id `opstina-popkrug-dom` via `format_id` | no | **extend** — factored to `_composite_id`, reused by new `v()` |
| `column_mapping` / `map_formatting_function` | `lsms_library/country.py:727-790` | binds a `mapping.py` function to any idx/myvar of the same NAME | — | reuse — a function named `v` auto-binds on *both* sides of the join, which is exactly the lockstep we need |
| `df_data_grabber` grabber | `lsms_library/local_tools.py:1045` | list-valued var + function ⇒ `df[cols].apply(f, axis=1)` | yes | reuse — this is what makes a list-valued `v` legal |
| `sample` `dfs:`/`merge_on:`/`drop:` block | `countries/Serbia/2007/_/data_info.yml` | GH #500 joined df_hh↔df_ed on the full `(opstina, popkrug)` key | — | **left as-is** (see §5) |

## §3 Definitions & conventions in force

- `v` = sampling cluster / settlement id. "Do NOT put `v` in feature
  `data_scheme.yml` indexes other than `cluster_features` (which owns it)" —
  `CLAUDE.md`, *`sample()` and Cluster Identity*.
- `sample` is "the single source of truth for mapping households to their
  sampling cluster" — ibid. `_join_v_from_sample` propagates it.
- Canonical `household_roster` index is `(t, v, i, pid)` —
  `lsms_library/data_info.yml:17` (`Index Info > index_info`). Roster's `v` is
  therefore *supposed* to arrive from `sample()`, not from its own idxvars.
- `format_id` is auto-applied to `idxvars` but NOT to `myvars` — `CLAUDE.md`,
  *Gotchas with Teeth*. A named `mapping.py` function, by contrast, binds to
  **both**; that is what keeps `cluster_features.v` (an idxvar) and
  `sample.df_hh.v` (a myvar) in the same key space.

## §4 Invariants & assumptions

- **`cluster_features.idxvars.v` and `sample.df_hh.myvars.v` must be the same
  key.** `sample.v` is the join key onto `cluster_features(t, v)`. Changing one
  alone either leaves the 182-row collapse (fix sample only) or breaks the join
  so every household gets NaN Region/Rural (fix cluster_features only). Pinned
  by `tests/test_serbia_cluster_key.py::test_no_household_has_nan_cluster_attributes`
  and `::test_sample_v_matches_cluster_features_v`.
- `(opstina, popkrug)` is unique in `enumeration_district.dta`: 510/510.
  `popkrug` alone: 328/510. `naselje` alone: 319/510 — **no single column
  works**; the composite is required.
- Source `opstina`/`popkrug`/`dom` are zero-padded **strings** in every Serbia
  `.dta`. `_composite_id` normalizes each part through `int`, so `v` is a strict
  prefix of `i` (`i = '{opstina}-{popkrug}-{dom}'`, `v = '{opstina}-{popkrug}'`).
- A `mapping.py` function whose name collides with a **data_scheme table name**
  is treated as a df_edit hook, not a cell formatter (`country.py:742`, GH #476).
  `v` is not a table name, so the binding is safe.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| composite `v` key | **extend** `mapping.py` | `i()` already does exactly this join-with-`format_id`; factored the shared part into `_composite_id` rather than writing a second copy (and added the NaN guard `i()` lacked) |
| roster `v` | **delete** | reuse `_join_v_from_sample` — the framework already supplies it; the local `v: popkrug` was dead (roster's declared index is `(t, i, pid)`, so the level was dropped anyway) *and* wrong |
| `sample` merge scaffolding | **no change** | GH #500's `merge_on: [opstina, popkrug]` + `drop:` already joins df_hh↔df_ed 1:1 on the correct full key. Re-plumbing it to merge on the composite `v` is a cosmetic simplification with a real regression surface (the `dfs:` merge is `how='outer'`; an ED with no households would fabricate a NaN-`i` row). Deferred deliberately — noted in the data_info.yml comments. |
| the `groupby().first()` collapse itself | **out of scope** | fixing the *class* is #323 proper; this ledger covers the Serbia *instance* only |

## §6 Open questions for the human

- The `sample` `dfs:`/`merge_on:`/`drop:` scaffolding now carries
  `opstina`/`popkrug` as plain columns purely as merge keys, while `v` is built
  from those same two columns. It could collapse to `merge_on: [v]`. Left alone
  on purpose (§5). Worth doing when someone next touches that block.
- Serbia is a single-wave country, so the `v` value change (`'0017'` →
  `'70017-17'`) breaks no panel linkage. Any *downstream* consumer that
  hardcoded a bare-popkrug `v` literal would need updating — none found in-repo.

---
### Phase 3 — verification

- `mapping.py:_composite_id` — **OK (anchored on §2/§5)**: extracted from the
  existing `i()`; adds only the `pd.isna` guard, so a missing key part yields
  `None` instead of `ValueError: int(nan)`.
- `mapping.py:v` — **OK (anchored on §3/§4)**: binds by name to both the
  `cluster_features` idxvar and the `sample.df_hh` myvar, which is what enforces
  the §4 lockstep invariant in *code* rather than in prose.
- `data_info.yml` (3 sites) — **OK (anchored on §4)**: the two `v` declarations
  moved together in one commit; the roster `v` deletion is anchored on §3
  (`cluster_features` owns `v`; the framework joins it).
- No library symbol changed ⇒ no `CONTRADICTION` / `REINVENTION` surface in
  `country.py` / `local_tools.py` / `transformations.py`.
