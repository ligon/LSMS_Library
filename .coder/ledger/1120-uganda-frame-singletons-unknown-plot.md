# Prior-Art Ledger — GH #1120 D1/D2/D3 + GH #1119 (Uganda)

**Search tier used:** ripgrep+git floor (the gitnexus MCP server failed to connect).

## §1 Task, restated
Implement @ligon's 2026-10-23 rulings for Uganda.
- **D1** — `sample` (YAML path, every wave) takes a household the survey
  recorded in ANOTHER cover from that cover, with that cover's weight. Uganda
  2015-16: AGSEC1 (agriculture cover) holds 72 households gsec1 lacks, with
  `parish_code` (2015-16's `v`) and `hwgt_W5`.
- **D2** — a household that no file locates gets a singleton `v` per household,
  Nigeria's `moved-{hhid}`; never a shared sentinel, never NaN.
- **D3** — a plot-grain row whose plot id is blank in the source gets
  `plot_id = 'unknown_plot'` (registered in `data_info.yml`, commit `1865cf884`).
- **#1119** — `plot_inputs` `j` marker `n/a` → `(not crop-specific)`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `nigeria.cluster_id` | `countries/Nigeria/_/nigeria.py:109-125` | `f'moved-{format_id(hhid)}'`, `None` without an id | `tests/test_nigeria_cluster_identity.py` | **precedent, copied** (`uganda.singleton_v`) |
| `uganda.cluster_features` | `countries/Uganda/_/uganda.py` (df_edit hook) | projects GSEC1 rows to `(t, v)` via `reduce_to_agreed`, drops NaN `v` with a count | `test_uganda_v_grain_invariants.py` | **extended**: alternate-cover adoption + singleton for cover rows |
| `Wave.grab_data` `dfs:` path | `lsms_library/country.py:~1950-2140` | merges sub-dfs on `merge_on`, `derived:`, `drop:`, then the df_edit hook | yes | reused (no framework edit) |
| `Wave._merge_subframes` | `country.py:~1650` | refuses a many-to-many merge (GH #323 site 4) | yes | constraint: one multi-row extension sub-df per block (2009-10 uses a 2-file `file:` list) |
| `build_transforms.fill_v_with_coord_bin` | `lsms_library/build_transforms.py:~229` | 2009-10 `@lat,lon` bin for blank `comm` | yes | reused unchanged; singleton applies AFTER it |
| `country._join_v_from_sample` | `country.py` | left-merges `sample.v` onto every household table; `format_id` on `v` | yes | unchanged — the reason `sample` must hold every served household |
| `transformations.roster_to_characteristics` | `transformations.py:640` | fills NaN `v` with the SHARED `'Mover'` bucket (GH #268) | yes | not touched; D2 empties it for Uganda |
| `CROP_NA = '(not crop-specific)'` | `Niger/_/niger.py:668`, `Mali/_/mali.py:715`, EHCVM wave scripts | the canonical `j` marker | Mali/Niger tests | **reused spelling** (`uganda.CROP_NA`) |
| `_CONDITION_UNKNOWN = 'unknown_condition'` | `uganda.py` | an epistemic index sentinel | yes | naming precedent for `UNKNOWN_PLOT` |

## §3 Definitions & conventions in force
- `unknown_plot` (EPISTEMIC) vs `Farm-level` (STRUCTURAL); consumer rule
  "not a plot … two `unknown_plot` rows of one household are not one plot":
  `lsms_library/data_info.yml` `Single Index: plot_id:` (commit `1865cf884`).
- `(not crop-specific)` is the one canonical plot_inputs `j` marker:
  `data_info.yml` Index Info plot_inputs comment (same commit).
- `sample` owns `v`; `_join_v_from_sample` stamps it on every household table:
  CLAUDE.md "`sample()` and Cluster Identity".
- Weights normalised to within-wave mean 1 at read time (`_normalise_sample_weights`):
  CLAUDE.md, same section.
- Pre-refreshment waves (and Uganda 2019-20) put one source weight in both
  `weight` and `panel_weight`: CLAUDE.md "Two weight types"; Uganda
  `2009-10`..`2011-12`, `2019-20` `data_info.yml`.

## §4 Invariants & assumptions
- `sample` and `cluster_features` publish one `v` keyspace
  (`Uganda/_/CONTENTS.org` "Hybrid `v` in 2009-10"); Nigeria serves its
  singletons from both (`Nigeria/_/CONTENTS.org`, "The `Moved` sentinel").
- `cluster_features` 2005-06 receives 368 geovar-only rows (2009-10 split-offs)
  with NaN `v` and no cover attribute (CONTENTS.org TODO). A blanket
  NaN-`v` → singleton in the `cluster_features` hook would mint 368 phantom
  clusters, so the hook gives singletons only to rows carrying a cover
  attribute (`Region`/`Rural`/`District`).
- A `dfs:` merge may repeat `i` in only ONE of the two frames
  (`_merge_subframes`), so a plot/person-grain extension sub-df must be the only
  multi-row sub-df in its block; the `sample` hook proves the repeats identical
  before collapsing them (lossless or loud).
- `uganda.py` and every wave `data_info.yml` are in every Uganda table's hash:
  a Uganda-only re-warm. No framework file is edited, so no other country moves.
- pandas 3 `astype(str)` keeps NaN as NaN, so a blank parcel/plot made the
  composite plot id NaN (measured), which is what D3 replaces.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| singleton `v` | reuse Nigeria's spelling (`moved-{format_id(hhid)}`) | the ruling; flagged as an open question because Uganda's households are not movers |
| D1 frame extension | extend: a `df_ag` sub-df + `_cover` marker + `uganda._adopt_alternate_cover` | no framework mechanism for "rows only the second cover has"; a new `derived:` kind would move every country's fingerprint |
| D2 frame extension | extend: `_frame`-marked sub-dfs naming the file that records the households | YAML-declared so the source sidecars enter the hash |
| D1 weight | the cover's single weight in both columns | Uganda's single-weight precedent (pre-refresh waves, 2019-20) |
| `unknown_plot` | new helper `uganda._plot_key` used by all three builders | one definition; only the served string changes, lookups unaffected |
| `(not crop-specific)` | reuse the Niger/Mali constant name `CROP_NA` | ruling #1119 |

## §6 Open questions for the human
- The singleton prefix: `moved-` is Nigeria's word; Uganda's unlocated
  households are AGSEC1-only / GSEC2-only / split-off-id households and
  blank-cluster-field cover households, not (all) movers.
- 6 `plot_inputs` seed rows carry the marker because their crop code is
  unmapped (5 × code 790 "Watermelon" per the 2013-14/2015-16 Stata labels,
  absent from `harmonize_crop`; 1 × code 0). Pre-existing; the rename is a
  spelling swap.

---
### Phase 3 — verification
- `uganda.singleton_v` — OK (anchored on §2 `nigeria.cluster_id`): same
  `f'{prefix}-{format_id(hhid)}'`, `None` without an id; `format_id`-stable.
- `uganda.sample` (hook) — OK (§4 dfs-merge constraint, lossless-or-loud): the
  frame-extension repeats are proved identical before collapse; a disagreeing
  repeat raises (synthetic test).
- `uganda._adopt_alternate_cover` — OK (§3 weight conventions): row-level, never
  fills a gsec1 gap; numeric columns stay numeric (the first cold build caught a
  string-typed cached weight; fixed in 4924a6c62, pinned by a test).
- `uganda.cluster_features` (hook) — OK (§4 2005-06 geovar orphans): singleton
  only for rows with a cover attribute; 2005-06's 368 still dropped.
- `uganda._plot_key` — OK (§3 `unknown_plot`): bare sentinel; lookups on the
  components unchanged.
- `uganda.CROP_NA` — OK (§2 Niger/Mali `CROP_NA`).
- Not anchored, disclosed: the 6 seed rows with an unmapped crop code that the
  marker now labels (§6), and the 11 2009-10 `plot_inputs` Seed keys that fold
  two plot-less lines each (no conflicting reported value).
