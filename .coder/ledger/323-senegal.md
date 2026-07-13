# Prior-Art Ledger — GH #323 (Senegal)

**Search tier used:** ripgrep + git (gitnexus MCP tools not reachable from this
worktree agent; call graph established by reading `country.py` end-to-end).

## §1 Task, restated

`_normalize_dataframe_index` (`country.py`) collapses a non-unique DECLARED
index with `groupby().first()`, silently discarding the dropped rows. GH #323
lists four Senegal cells. I reproduced all four counts exactly, and found the
audit's *classification* right about the mechanism but **wrong about the damage
on `food_acquired`** — see §7. Senegal has two genuinely distinct root causes:

- **A. INTENDED_AGGREGATION with a destructive reducer** — `plot_inputs`
  2018-19 (15 groups / 30 rows). A many-to-one label merge is deliberate; the
  *reducer* is the bug.
- **B. EXTRACTION_BUG (wrong grain), value-lossless** — `cluster_features`
  both waves. Declared at `(t, v)` but extracted from the household cover file.

`food_acquired` 2021-22 turned out to be a **non-finding** (already correct).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4100` | reorders/drops index levels; collapses duplicates via `first()`, warning once | yes (`test_normalize_index_j_preserved.py`) | **extend** |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py:101` | `{'food_acquired': ('Quantity','Expenditure')}` → sum, no warning (GH #501) | indirectly | **reuse as fallback** (untouched) |
| `_collapse_duplicate_index` | `feature.py:106` | same additive-vs-first logic, cross-country side | — | left alone (Feature path) |
| `Wave.cluster_features` | `country.py:1168` | collapses an `i` level to cluster grain, `first()`/`mean()` (GH #161) | `test_uganda_v_grain_invariants.py` | **not touched** — Senegal's `i` is already gone by then |
| `Wave.formatting_functions` / `df_edit` | `country.py:801,934,981,1053` | per-table Python hook from the wave module, applied on BOTH the single-file and `dfs:` merge paths | yes (via `food_acquired`) | **reuse** as the extension point |
| `aggregation:` key | 9 countries' `data_scheme.yml` | **parsed and IGNORED** — listed only in the `skip` sets at `country.py:2387`, `diagnostics.py:174` | no | **make it enforceable** |

## §3 Definitions & conventions in force

- "No aggregation in core": `SkunkWorks/grain_aggregation_policy.org` — the
  access path must never *implicitly* reduce grain. An INTENDED reduction must
  therefore be **declared**, which is exactly what this task adds.
- EHCVM `v: grappe`, `i: [grappe, menage]`, each grappe in exactly one vague —
  per `CLAUDE.md` "Gotchas with Teeth". Confirmed for both Senegal waves.
- Cache tiers: L2-**wave** parquet is written pre-collapse (the truth); L2-
  **country** is written post-collapse. Per `CLAUDE.md` "Cache Behavior".
- `aggregation: {visit: first}` (interview_date, 9 countries) is a **level**-keyed
  downstream hint, not a column-reducer map. It must stay inert.

## §4 Invariants & assumptions

- **The #323 warning only fires on a COLD build.** Warm, the collapse is already
  baked into L2-country, so the warning never re-fires: *the bug hides behind
  the cache it poisoned.* Every measurement below is therefore taken on a cold
  build into a private `LSMS_DATA_DIR` (L1 blob cache symlinked in, so no
  re-download and no `dvc pull`).
- Rows can only collide when the native unit `u` is IDENTICAL — `u` is in both
  declared indexes. So Quantity/Expenditure are extensive and `sum` is defined.
- Senegal cluster attributes are single-valued within grappe: **0 of 598** and
  **0 of 596** grappes conflict on Region/Rural/Latitude/Longitude (verified).
  The old `first()` was correct only by that accident; it is now *enforced*.
- `.pth` trap: `PYTHONPATH=<worktree>` does redirect `import lsms_library` when
  the entry point is a **script file** (sys.path[0] = script dir), but NOT under
  `python -c` (sys.path[0] = cwd = main repo). Every measurement asserts
  `'worktrees' in lsms_library.__file__`.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| duplicate-index reducer | **extend** `_normalize_dataframe_index` with a declared per-column policy | the hardcoded `_ADDITIVE_MEASURE_COLUMNS` can't express `Purchased: any`, and a country shouldn't need a library edit to declare intent |
| `on_duplicate_index` sub-key | **new** (under the existing `aggregation:`) | a *column*-keyed map cannot share a namespace with the existing *level*-keyed `visit: first` without ambiguity; nesting keeps all 9 existing declarations inert |
| cluster-grain collapse | **new** `transformations.collapse_to_cluster_grain` | no existing helper *enforces* the single-valued invariant; `Wave.cluster_features` assumes it. Reusable by the other 68 country-waves in this class |
| `plot_inputs` reducer | `sum` / `any` / `sum` | see §7 — verified against the source, not assumed |

## §6 Open questions for the human

- **The class is much bigger than Senegal.** 69 country-waves have a non-unique
  `cluster_features` (t,v), and in **13 countries a non-GPS attribute genuinely
  VARIES within a cluster** (Albania, Burkina_Faso, CotedIvoire, Guatemala,
  Guyana, Kazakhstan, Liberia, Malawi, Niger, Nigeria, Pakistan, Serbia,
  Tanzania) — there `first()` is silently picking one Region/Rural/District.
  That is class-1 *silently wrong*, not merely redundant. Senegal is clean, so
  I did **not** make the invariant raise globally: doing so would break those 13
  countries and every parallel fix-agent. Flipping the undeclared-collapse
  default from `warn+first` to `raise` is the remaining systemic step and needs
  to be a coordinated call once the countries have declared their policies.
- Should `_ADDITIVE_MEASURE_COLUMNS` (`feature.py:101`) eventually be *replaced*
  by per-country `on_duplicate_index` declarations? It is now the fallback.

---
### Phase 3 — verification

- `_duplicate_index_policy` — OK (anchored on §2, §3): reads only the new
  `on_duplicate_index` sub-block, so the 9 existing level-keyed `visit: first`
  declarations keep falling through to the legacy path (pinned by
  `test_level_keyed_aggregation_is_not_consumed`).
- `_reduce_duplicate_index` — OK (anchored on §4): `sum` uses `min_count=1` and
  `any`/`all` re-mask empty groups, so an all-NA group stays NA — "never asked"
  cannot become a confident 0/False. Raises on an uncovered column rather than
  falling back to `first()` (the §1 defect); vectorised per reducer, so the
  220k-row `food_acquired` path stays fast.
- `collapse_to_cluster_grain` — OK (anchored on §4): `nunique(dropna=False)`
  treats NaN as a value, so a cluster mixing NaN with a value is a conflict.
- **NOT a reinvention of** `_collapse_duplicate_index` (`feature.py:106`): that
  one serves the cross-country `Feature` assembly path and has no declared
  policy. Left untouched; the Country path now prefers the declaration.
- **`food_acquired` — NON-FINDING (contradicts the audit's stated damage).**
  See §7.

### §7 The audit's `food_acquired` damage claim is false — and why it matters

The triage stated that `first()` on the 218 collisions "DISCARDS 46.5% of
Expenditure (44,870 CFA erased)" and called it class-1 SILENTLY WRONG. It is
not. `food_acquired` is already in `_ADDITIVE_MEASURE_COLUMNS`, so those
collisions have been **summed** since GH #501. Measured by conservation of mass
on a cold build:

    2021-22 food_acquired   wave parquet -> API
      Quantity      1,137,283.9  ->  1,137,283.9   destroyed 0.0 (0.00%)
      Expenditure 185,854,825.0  -> 185,854,825.0  destroyed 0.0 (0.00%)

and the BASE cold build emits **3** #323 warnings, not 4 — the additive path
does not warn because it does not drop. The 46.5% figure is what `first()`
*would* do on that data, computed from the source rather than observed at the
API. I kept the declaration anyway (it pins the intent and removes Senegal's
reliance on a hardcoded library-side map) but it is **behaviour-preserving**,
and `rows_recovered` for Senegal is **15, not 233**.

This is the same failure mode the brief warns about: a number derived from the
code path you *assume* is running rather than the one that *is*. My first merge-
based instrument also returned a false zero (it joined on `u`, which the API
remaps `139. Sachet` -> `Sachet`); conservation of mass is join-free and caught
it.
