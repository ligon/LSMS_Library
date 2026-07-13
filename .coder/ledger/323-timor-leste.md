# Prior-Art Ledger — GH #323 (Timor-Leste)

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`.

**Search tier used:** ripgrep + git (gitnexus MCP tools not exposed in this agent's
tool set; blast radius established by hand — `_normalize_dataframe_index` is
module-private with exactly 4 call sites, all in `country.py`).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py`) collapses a non-unique
DECLARED index with `groupby().first()`, silently discarding the dropped rows.
For **Timor-Leste** the affected cell is `2007-08 / cluster_features`: 4477 rows
in, 300 out, **4177 duplicate index tuples**.

Unlike Mali (32,026 people vaporised) this collapse is *correct*. The 2007-08
wave extracts `cluster_features` from `../Data/basicvars.dta`, the **household
cover file** (4477 rows, one per `hh_id`; only 300 distinct `cluster`s), so each
household merely carries a redundant copy of its cluster's attributes. The task
is therefore **not** to recover rows — there are none to recover — but to make an
intended de-duplication *declared and machine-checked* rather than silently
performed by the same code path that vaporises Mali's roster.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/drops index levels; collapses duplicates via `.first()` (or SUM for additive tables) | `tests/test_normalize_index_j_preserved.py` (level restore only; **no duplicate-collapse test existed**) | **extend** |
| `_ADDITIVE_MEASURE_COLUMNS` | `lsms_library/feature.py` | the ONLY existing duplicate-collapse policy: SUM additive columns for `food_acquired` | yes | reuse as-is (untouched) |
| `aggregation:` key | `Albania/_/data_scheme.yml:83`, `Malawi/_/data_scheme.yml:94`, +5 more | `{level: reducer}` grain map, e.g. `visit: first` | **NO** | see §4 — **inert** |
| `SkunkWorks/grain_aggregation_policy.org:187` | design doc | proposes the `aggregation:` block; "expose as a `Feature` method / `grain` kwarg" — **future work** | n/a | forward-compatible with §5 |
| 2001 `cluster_features` | `Timor-Leste/2001/_/data_info.yml` | reads `WEIGHT.DTA`, already one row per cluster (`id4`) → index unique **by construction** | n/a | the honest counter-example |

## §3 Definitions & conventions in force

- `cluster_features`: index `(t, v)`; the one table that legitimately OWNS `v`
  (`CLAUDE.md`, "`sample()` and Cluster Identity"). Households live in `sample`
  (index `(i, t)`, `v` as a column) — **unaffected by this cell**, which is why 0
  rows of real data are at stake.
- class-1 = silently WRONG; class-2 = silently MISSING / loud. **class-2 is
  strictly safer** — brief, "THE STANDARD".
- INTENDED_AGGREGATION: the declared-index duplicates are a *redundant* encoding,
  not distinct observations.

## §4 Invariants & assumptions (the landmines)

- **The `aggregation:` key was RESERVED BUT INERT.** It appears in exactly three
  places, all of which *ignore* it: `country.py:2387`, `diagnostics.py:174`,
  `tests/test_table_structure.py:103` (`_skip = {"index","materialize","backend",
  "aggregation"}`). **No code reads it.** Albania's and Malawi's
  `aggregation: {visit: first}` are therefore **pure prose** — declared and never
  enforced. Consequence for this task: *merely adding an `aggregation:` block to
  Timor-Leste's YAML would have been a no-op* — more unenforced prose, the exact
  failure mode the brief names ("Prose is not enforcement"). The reducer had to be
  **implemented**, not just declared.
- **The bug hides behind the cache it poisoned.** The #323 warning fires only on a
  COLD build (`LSMS_NO_CACHE=1`); warm reads serve the already-collapsed
  L2-country parquet. Every measurement here is cold.
- **The instrument trap.** `{data_root}/{C}/var/{table}.parquet` is written
  POST-collapse and returns false zeros. Only the L2-**wave** parquet
  (`{C}/{wave}/_/{table}.parquet`) holds the truth. My scanner was validated
  against the known positives *before* any Timor number was trusted:
  Mali/2014-15/household_roster → 32,026 ✓ ; Guyana/1992/housing → 311 ✓.
- **The `.pth` trap is real and it caught me.** `PYTHONPATH=<worktree>` does **not**
  redirect `import lsms_library` — `.venv/.../lsms_library.pth` pins the MAIN
  checkout, and a bare `python -c` with `PYTHONPATH` set still imported
  `/mirrors/LSMS_Library/lsms_library/country.py`. Two things that DO work:
  `sys.path.insert(0, WT)` inside the script, and `pytest` (a root `conftest.py`
  + `tests/__init__.py` make pytest insert the worktree root at `sys.path[0]`).
  Both were asserted, not assumed — every verification script carries
  `assert 'worktrees' in lsms_library.country.__file__`.
- `cluster` is a **globally-unique superkey** in basicvars.dta: adding `district`,
  `posto`, `suco`, `region` or `strata` to the group key splits **nothing**
  (300 → 300). So this is NOT the Guyana INDEX_INCOMPLETE pathology, and no
  distinct real clusters are being merged. Payload constant in **0/300** clusters;
  zero NaN in `cluster`/`region`/`urban`/`hh_id`.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| duplicate-collapse policy | **extend** `_normalize_dataframe_index` | the additive-SUM policy already lives there; a second policy belongs beside it, not in a new layer |
| the reducer itself | **new**: `aggregation: {on_duplicate_index: unique}` | no existing reducer *verifies* an invariant; `first` (silently discards) and `sum` (would double-count 15 identical households) are both wrong here — see §6 |
| `{level: reducer}` grain map | **untouched** | Albania/Malawi's `visit: first` stays inert exactly as before; `on_duplicate_index` is a reserved key that can never collide with a level name, so a future implementer of level-collapse is unobstructed |
| structural fix (`drop_duplicates` at extraction, mirroring 2001) | **rejected** | it would make the index unique by construction, so the invariant check would never run — converting a class-1 risk into a *different* silent class-1 (a future inconstant payload would just yield 2 rows and get `.first()`-collapsed downstream anyway). Keeping the 4477 rows flowing and having the declared reducer verify them on **every cold build** is strictly stronger. |

### Why `unique` and not `first` / `sum`

`first()` gives the right answer here **today, by luck, not by construction** — it
happens to pick from 15 identical copies. `sum` would be nonsense (it would add
15 redundant copies of the same cluster). The invariant that actually makes
`cluster_features` meaningful is *"the payload is constant within a cluster"*, and
that invariant was asserted **only in a YAML comment** ("300 clusters, constant
within cluster") — which is precisely what let this slip past review. `unique`
**RAISES** if a group is not constant. It costs nothing today (0/300 conflicts,
verified) and converts a future class-1 failure (a new wave where Region genuinely
varies within a cluster → silently WRONG) into class-2 (loud). *That is the entire
point of declaring it.*

## §6 Open questions for the human

- **The `aggregation:` mechanism now exists but only Timor-Leste uses it.**
  "`cluster_features` derived from a household cover page" is a **recurring
  sub-pattern** of the #323 class, distinct from Mali/Guyana's missing-level
  pathology. Every such country should be swept and declared. Until then, the
  systemic fix **must not** make a non-unique declared index raise *without* this
  escape hatch — that would break exactly these countries. This PR lands the
  hatch; the sweep is not in scope for a single-country fix agent.
- Albania's and Malawi's `aggregation: {visit: first}` remain **inert prose**. They
  are untouched here (deliberately — activating them would change their output),
  but they are live examples of the same "declared and never enforced" failure and
  should either be implemented or deleted.

---
### Phase 3 — verification

- `_normalize_dataframe_index` — **OK (anchored on §2, §5)**: extended in place
  beside the existing additive-SUM policy; the pre-existing paths are reached via
  `elif`, which is semantically identical when no reducer is declared (`reducer is
  None`). Empirically: 36-country cold BEFORE/AFTER sweep, all non-Timor cells
  byte-identical, #323 warning counts unchanged.
- `aggregation: {on_duplicate_index: unique}` — **OK (anchored on §4)**: not a
  REINVENTION of the `{level: reducer}` map — that map is inert, keyed by index
  level, and expresses grain collapse, not invariant verification. Reserved key
  cannot collide with a level name.
- Timor-Leste `cluster_features` declaration — **OK (anchored on §5)**: 4477→300
  collapse now declared + verified on every cold build; fault injection confirms a
  conflicting payload RAISES instead of silently resolving (pre-fix it silently
  picked `R2` over `R3` for cluster 201).
