# Prior-Art Ledger — GH #1063 (EHCVM `household_roster` row order differs between builds)

**Search tier used:** ripgrep + git floor (the gitnexus MCP server failed to
connect this session; `rg` call-site sweeps substituted, per `CLAUDE.md`
§"Code intelligence").

## §1 Task, restated
Two builds of the same code served the EHCVM `household_roster` (CotedIvoire,
Mali, Togo on 2026-10-14; Benin, Guinea-Bissau on 2026-10-19) with identical
values, index set and dtypes but different row order -- at the L2-wave tier
(`{wave}/_/household_roster.parquet`), not only the country tier.  Find the
unordered step, make the order deterministic at the build site, and pin it with a
test that builds twice and compares with order.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave.grab_data` (`dfs:` branch) | `lsms_library/country.py:1919-2000` (fix at `:1940`) | extracts each `dfs:` sub-frame, then merges them on `merge_on` | `tests/test_gh323_site4_dfs_merge.py`, `tests/test_gh675_degraded_build.py` | **fix site** (one line) |
| `Wave._merge_subframes` | `lsms_library/country.py:1622` | cartesian guard (GH #323 site 4) + `pd.merge(left, right, on=merge_on, how=how)` | same | unchanged |
| `_parallel_waves.prebuild` / `relay` | `lsms_library/_parallel_waves.py` | fans the wave build out; the post stage runs in wave order | `tests/test_parallel_waves.py` | ruled out (measured: workers 1/2/4 identical) |
| `country.most_complete_row` | `lsms_library/country.py:8394` | GH #871 selector; preserves order | `tests/test_gh871_most_complete_row.py` | ruled out: builds under the same seed are order-identical, and the fixed build is order-identical across seeds, so no other unordered step is live here |
| synthetic-country fixture | `tests/test_gh675_degraded_build.py::synth` | `LSMS_COUNTRIES_ROOT` + `LSMS_ALLOW_INTREE_SOURCES=1` tree driving `grab_data` with no microdata | yes | **reused** pattern in the new test |
| `test_fingerprint_stable_across_pythonhashseed` | `tests/test_build_transform_hash.py:407` | subprocess-per-seed pattern for hash-seed invariance | yes | **reused** pattern |

## §3 Definitions & conventions in force
- EHCVM identity: `v: grappe`, `i: [grappe, menage]`, no vague level (`CLAUDE.md`
  §"Gotchas with Teeth"; e.g. `Benin/_/CONTENTS.org:36-38`).  Every EHCVM roster
  is a two-sub-frame `dfs:` block (`df_main` = section 1, `df_cover` = section 0
  for the interview date) with `merge_on: [v, i]`, `final_index: [v, i, pid]`.
- `merge_how:` defaults to `outer` (GH #323; `country.py` comment beside the fix).
- `pd.merge(how='outer')` sorts the result lexicographically by the join keys
  in the order of `on`; `how='left'` preserves the left frame's order.

## §4 Invariants & assumptions
- **`t` is constant inside one `dfs:` build** -- no sub-df in the corpus maps a
  `t` idxvar or myvar (census, 90 blocks); `check_adding_t` stamps
  `self.year`.  So only blocks with >= 2 non-`t` keys AND `outer` can depend
  on key order: 14 blocks in 10 `(country, table)` cells (8 EHCVM rosters,
  Nigeria 2012-13 `assets` `[i, j]`, Serbia 2007 `sample` `[opstina, popkrug]`).
  The other 76 are single-key (`(t,k)` and `(k,t)` sort identically) or
  `merge_how: left` (GhanaSPS, 6 blocks).
- Every `merge_on` in the corpus is a YAML list (census), so `[*merge_on]` and
  the old `set(...).union(merge_on)` see the same keys.
- `Wave.grab_data` is `@build_transform()` (ALL-scoped): any edit moves every
  table's `build_transforms_fingerprint`, hence every L2 hash (`CLAUDE.md`
  §"Grain Collapse", "The re-warm is a COLD one").

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| merge-key order | extend (one line) | `list(dict.fromkeys(['t', *merge_on]))`: `t` first, then the declared order -- the index prefix `(t, v, i)` for the EHCVM rosters.  No blanket `sort_index`: blocks that are already deterministic keep their order. |
| test harness | reuse | synthetic country from #675's fixture; subprocess-per-seed from `test_build_transform_hash.py` |

## §6 Open questions for the human
- None blocking.  The served order of the 9 seed-dependent cells is now the one
  development produced under a "declared-order" seed (e.g. `PYTHONHASHSEED=1`);
  a warm cache written under the other draw will rebuild once anyway, because the
  fix moves every hash.

---
### Phase 3 — verification

- `Wave.grab_data` merge-key line — OK (anchored on §4): fixed order, same key
  set; values unchanged (sorted frames equal to development in all 10 cells).
- `tests/test_gh1063_roster_order.py` — OK (anchored on §2): reuses the #675
  synthetic-country and #407 per-seed subprocess patterns; no new machinery.
- Evidence: `slurm_logs/isa_backlog_2026-10-21/1063-roster-order/README.org`.
