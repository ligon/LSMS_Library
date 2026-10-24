# Prior-Art Ledger — GH #1120 D1 / #1261 / #1119 / #637 (Nigeria)

**Search tier used:** ripgrep + git floor (the GitNexus MCP server did not
connect this session; call sites enumerated with `rg`).

## §1 Task, restated

Four maintainer rulings for Nigeria (@ligon, 2026-10-23), one branch:

1. **#1120 D1** -- `sample()` takes each ROUND's households from that round's
   own household cover page with that cover's weight: the post-planting cover
   (`secta_planting{wN}`) for the `YYYYQ3` quarter, the post-harvest cover
   (`secta_harvest{wN}`) for `YYYYQ1`.  Until now one `data_info.yml` block per
   wave folder read the PP cover for both rounds (both quarters map to the
   folder via `nigeria.wave_folder_map`).
2. **#1261** folds in: W2's `cluster_features` outer `dfs:` merge admitted 39
   geo-only households with a null `v`.  They are PH-cover households, so (1)
   serves them in 2013Q1; the geo join becomes LEFT onto each round's cover.
3. **#1119** -- `plot_inputs.crop` marker `n/a` -> `(not crop-specific)`.
4. **#637 W1/W2** -- `community_prices` gets `variety` = source item code
   (Tanzania #840 precedent); the W1/W2 groupby-first goes.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `cluster_id` / `v` | `Nigeria/_/nigeria.py:108` / `:127` | composite `state/lga/ea` key from raw codes; `moved-{hhid}` singleton for `ea == 0` (GH #323) | `tests/test_nigeria_cluster_identity.py` | reuse |
| `wave_folder_map`, `PP_QUARTER`, `PH_QUARTER` | `Nigeria/_/nigeria.py:23`, `:~345` | quarter <-> folder; round quarters | yes | reuse |
| `Wave.grab_data` script path + `t == self.year` filter | `lsms_library/country.py:2196-2264` | runs `make ../<wave>/_/<table>.parquet`, keeps the requested round | every Nigeria wave script | reuse (the pp/ph pattern) |
| `load_from_waves.build_wave` t-filter (GH #535) | `lsms_library/country.py:5433-5452` | same filter on the legacy route | yes | reuse |
| `Wave.cluster_features` -> `_collapse_to_cluster_grain` | `lsms_library/country.py:2318`, `:8410` | household-grain -> `(t, v)` with the Site 2 audit | yes | reuse (script writes `(t, i, v)`) |
| `df_data_grabber` | `lsms_library/local_tools.py:2120` | the YAML reader; reused in-script so the covers are read exactly as the YAML did | yes | reuse |
| Tanzania `2008-15/_/sample.py` | `Tanzania/2008-15/_/sample.py` | one script, four rounds, `v` a per-ROUND fact | yes | precedent |
| Ethiopia `<wave>/_/sample.py` + Makefile `../%/_/sample.parquet` | `Ethiopia/_/Makefile:63` | wave-level `sample` script pattern | yes | precedent |
| `column_mapping` per-file `t:` override | `lsms_library/country.py:1549` | a YAML per-file `t` override; ZERO users in the repo | no | **not used** (advisor: do not make a ruled data change its first exerciser) |
| Tanzania `community_prices_for_wave` `variety` | `Tanzania/_/tanzania.py:2141-2223` | source item code as a string level finer than `j` (GH #840) | `tests/test_community_prices_variety.py` | precedent |
| Nigeria `obs` (GH #637 defect C) | `Nigeria/_/nigeria.py` `community_prices_for_wave` | C8 reading slot | `tests/test_gh637_nigeria_w3_obs.py` | extend (key gains `variety`) |
| Mali / EHCVM `(not crop-specific)` | `Mali/_/mali.py` `CROP_NA` | the canonical marker | `tests/test_mali_plot_inputs_crop_marker.py` | precedent |

## §3 Definitions & conventions in force

- `v`, `weight`, `panel_weight`, the v-join: CLAUDE.md "`sample()` and Cluster
  Identity"; `sample` is the single source of truth for household -> cluster.
- pp/ph: `.claude/skills/add-feature/pp-ph/SKILL.md` -- two source files per
  wave with distinct `t` => script with `materialize: make`.
- `community_prices` canonical index `(t, v, j, u, variety, visit, obs)` and
  sentinels: `lsms_library/data_info.yml` Index Info (GH #840).
- `(not crop-specific)` / `Farm-level` / `unknown_plot`: `data_info.yml`
  (commit 1865cf884).
- D1 general form (Ethiopia: "the agriculture cover, with that cover's
  weight"): GH #1120 rulings comment.

## §4 Invariants & assumptions

- **Within a wave the two rounds are different covers.**  Measured: PH-only 0 /
  44 / 0 / 0 / 0, PP-only 61 / 49 / 104 / 238 / 296 households; movers between
  the visits 0 / 17 / 41 / 66 / 64 (W2 also 2 the other way); weight and
  panel_weight identical on every common household; no real EA changes LGA
  inside a wave.  CONTENTS.org's "the same households are visited in both
  rounds" was false.
- `sample.v` and `cluster_features.v` must be ONE keyspace per round
  (`test_sample_and_cluster_features_share_one_keyspace`), so both tables move
  together.
- A function in `nigeria.py` named like a table is a df_edit hook
  (`Wave.column_mapping`); the new helpers are `*_for_wave` / `round_covers`.
- `nigeria.py` and every wave `.py` are in every Nigeria table's input hash:
  a full Nigeria re-warm is the cost regardless (CLAUDE.md "Derived Values").
- `panel_ids` / `updated_ids` still read the PP cover (`panel_ids:` YAML
  blocks) -- out of scope (#1264).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| per-round sample | new `sample_for_wave` + thin wave scripts (reuse `df_data_grabber`, `cluster_id`) | YAML cannot read a different file per round except through an unused override |
| W2 PP agriculture cover (21 hh) | extend (`pp_supplement`) | D1 application on the Ethiopia precedent ("the agriculture cover, with that cover's weight"; NA here, the cover has none); the 63 plot_inputs NaN-`v` rows are at 2012Q3.  Kept (dispatcher decision, 2026-10-23) |
| a round's own roster (2016Q1: 2 hh) | extend (`pp_roster` / `ph_roster`, last in precedence) | D1: the roster records each household's state/lga/ea on every member row, so these households are located, not D2 cases; NA weights; the cover's label for the roster's codes |
| household no file locates | new: `unlocated-{hhid}` singleton (D2) | D2 ruling ("a singleton per household, never a shared sentinel", following `moved-{hhid}`); spelled apart from `moved-`, which records the survey's ea == 0 sentinel.  Fires on no household today |
| geo join | LEFT onto the round's cover | #1261; Ethiopia 2013-14 `merge_how: left` precedent |
| `variety` | reuse #840's definition (code as string) | ruling |
| crop marker | reuse the canonical constant | ruling |

## §6 Open questions for the human

- None open.  Settled by the dispatcher (2026-10-23): keep the W2 PP
  agriculture cover (D1, Ethiopia precedent); apply D2 (`unlocated-{hhid}`)
  to households no file locates; document the 2019Q1 community-price
  cluster with no 2019Q1 household.
- Found, not fixed: `individual_education` serves each PH record at the PP
  quarter too (~57,000 duplicated rows; CONTENTS.org TODO).

---
### Phase 3 — verification

Filled in the report (cold base-vs-branch builds on Slurm, raw-cover oracle,
hash census): `slurm`-side evidence under
`/global/scratch/fsa/fc_jevons/ligon/burst/jobs/nigeria/`.
