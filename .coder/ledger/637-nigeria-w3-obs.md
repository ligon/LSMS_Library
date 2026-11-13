# Prior-Art Ledger — GH #637 defect C: Nigeria W3 `community_prices` keeps only location 1

**Search tier used:** ripgrep+git floor.  The gitnexus MCP server failed to
connect this session ("Connection closed"); call-site sweeps were done with `rg`.

## §1 Task, restated
Nigeria's `community_prices` (cluster-grain, script path, `materialize: make`)
is built per wave by `nigeria.community_prices_for_wave`, which collapsed
`(t, v, j, u)` with `Price: 'first'`.  In W3 (2015-16, PH round 2016Q1) the C8
food-price module is fielded at TWO locations per community, one file each,
so the collapse kept location 1 in every cell both priced and served
location-2-only rows as if they came from location 1.  The fix adds the GH #840
canonical `obs` level (the form's reading slot) natively to Nigeria's declared
index: `obs = 1` for `sectc8a`, `obs = 2` for `sectc8b`, `obs = 1` in every
other wave.  W1/W2's own collapse (size variants, lumped items) is out of scope.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `community_prices_for_wave` | `countries/Nigeria/_/nigeria.py:2331` | builds one wave's frame from C8 source frames | `tests/test_nigeria_community_units.py` (synthetic), `tests/test_nigeria_cluster_identity.py` (built) | **extend**: per-frame `obs`, `obs` in key and index, codes-mode uniqueness assert |
| `_C8_SOURCE`, `_c8_raw_inputs` | `countries/Nigeria/_/nigeria.py:2818`, `:2835` | `inputs` frames for `c8-metric-size` / `c8-unrecorded-size-drop` | indirectly (registry drift tests) | **extend**: carry `obs` so inputs join the served grain |
| Niger `_community_price_triples` | `countries/Niger/_/niger.py:1383` | the #840 duplicate-entry rule (content key incl. `slot`) | `tests/test_community_prices_variety.py::TestNigerDuplicateEntries` | **precedent** (rule applied, fires 0 times here) |
| GhanaLSS `glss_prices` | `countries/GhanaLSS/_/glss_prices.py` | native `obs` = vendor slot, `(t, v, j, u, obs)` | `tests/test_ghanalss_community_prices.py` | **precedent** for a country-declared `(t, v, j, u, obs)` |
| `Feature` sentinels | `lsms_library/feature.py:_harmonize_country_frame` | fills `variety`/`visit`/`obs` for countries lacking them | `tests/test_community_prices_variety.py::TestAssembly` | reuse unchanged (Nigeria now fills only `variety`/`visit`) |

## §3 Definitions & conventions in force
- `obs`: "the observation within (variety, visit): the form's reading slot … A
  second record's slot that only REPEATS a served row is a duplicate entry and
  is not served (Niger: 134 rows)" — `lsms_library/data_info.yml`, Index Info
  `community_prices` comment (GH #840).
- Missing-level sentinels `variety: unknown_variety`, `visit: 1`, `obs: 1` —
  `data_info.yml` `missing_level_sentinels.community_prices`.
- "Core SELECTS nothing silently" / #840 "ONE ROW PER SURVEY-RECORDED PRICE";
  a duplicate key in a #840 builder is an error, not a cell to select from.
- Derived values: `obs` is an index level reading the form's slot, not a
  construction — no `Derivation` registration (CLAUDE.md "Derived Values":
  exempt as an index value visible in the frame).

## §4 Invariants & assumptions
- Nigeria is PP/PH; C8 is post-harvest only (`CONTENTS.org` "C8 is
  POST-HARVEST ONLY"), so `t` is the PH quarter and `obs` never meets a PP round.
- `v = cluster_id(state, lga, ea)` composite (GH #323) — unchanged.
- The `i == v` shim before `j` (map_index) — unchanged; `obs` appended after `u`.
- **Measured, 2026-10-23**: each W3 file is `(ea, item_cd)`-unique (0
  within-file duplicates), both cover all 487 EAs, `c8q1 == 1` and `c8q3 > 0`
  on every row; the shipped files carry NO location metadata (no name / type /
  date / GPS).  Enforced going forward by the codes-mode assert in
  `community_prices_for_wave` (W3–W5 must be collision-free on
  `(t, v, j, u, obs)`).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| W3 location slot | reuse the canonical `obs` level | #840 contract; GhanaLSS/Niger emit it natively |
| duplicate-entry drop | reuse Niger's rule exactly → **0 rows** | the rule's content key includes the slot; W3 has no second record of any slot.  Evidence C8b is not a re-entry of C8a: max per-EA all-field identity 88% (Niger's re-shipped blocks 99.8%); within-EA same-unit identical-price share 64.6% vs 48.6% for location 1 in two different EAs of one LGA |
| W1/W2 collapse | unchanged (`first`, now on `(t, v, j, u, obs=1)`) | out of scope; decision for @ligon |

## §6 Open questions for the human
- **W1/W2 collisions remain** (decision for @ligon): size variants of one base
  item (1,770 W1 / 1,921 W2 groups) and distinct items lumped under one label
  (798 / 1,030), Price conflicts in 2,303 / 2,605 groups — still collapsed to
  the first price.  Sketched fix: a `variety` level = source item code (#840's
  Tanzania precedent).
- The brief framed "a location-2 reading that only repeats location 1's row"
  as a duplicate entry; under the precedent's own rule it is a second slot and
  is served (1,608 such cells).  If the intent was otherwise, that is a ruling
  that would contradict Niger's slot-in-key rule and GhanaLSS's equal vendors.
- Pre-existing, not fixed: W3's `inputs_community_prices` returns 341 rows but
  the derivation stamps 1 — the other 340 are on unit "other specify" with a
  quantity in free-text `c8q2b`, served undivided (988 W3 rows on that unit).

---
### Phase 3 — verification
- `community_prices_for_wave` — OK (anchored on §3 `obs`, §5): `obs` is the
  frame's slot; no row selected in codes mode (asserted).  Raw-source oracle
  23,153 / 23,153 rows exact on key, `Price`, `Derivation`.
- `_C8_SOURCE` / `_c8_raw_inputs` — OK (§3 derived values): inputs gain `obs`;
  `(v, obs, item_cd)` unique in both W3 frames; dropped rows 14,603 all matched.
- `Nigeria/_/data_scheme.yml` index — OK (§2 GhanaLSS precedent).
- Pin moved: `tests/test_nigeria_community_units.py::test_recorded_c8_unit_survives_actual_builder`
  index-names pin `['t','v','i','j','u']` → `[..., 'obs']`, plus a new
  `obs == [1]` assertion (commented).  Not a weakening.
- Tests (`-n 3 --dist=loadfile`, same warm state, identical skip lists):
  development 458 passed / 11 skipped; branch 469 passed / 11 skipped (the
  11 new tests).  The new file fails 11/11 on development, for missing `obs`.
- Hash census (development vs branch): `Country._table_cache_hash` 24 / 549
  moved, all Nigeria (24 / 24); `Wave._input_hash` 240 / 2,109, all Nigeria;
  `build_transforms_fingerprint` 0 / 40.  A Nigeria-only re-warm.
- Evidence: `slurm_logs/isa_backlog_2026-10-21/637-nigeria-w3-obs/`.
