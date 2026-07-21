# Prior-Art Ledger — GH #323 (Uganda): silent duplicate-index collapse

**Search tier used:** ripgrep + git (gitnexus not consulted; the blast radius was
established empirically instead — BEFORE/AFTER table hashes across all 40
countries, see Phase 3).

## §1 Task, restated

`_normalize_dataframe_index` (`country.py`) collapses a **non-unique DECLARED
index** with `groupby().first()`. Uganda is hit two independent ways:

* **`people_last7days` (2018-19, 2019-20)** — INDEX_INCOMPLETE, silently
  **wrong**. Source `GSEC15A.dta` is LONG from 2018-19 on: two rows per `hhid`
  keyed by `CEA01` ∈ {`Household members`, `Visitors`}, `CEA01A-D` being the
  counts *for the selected category*. The declared index is `(i, t)` and `CEA01`
  was left undeclared (the YAML commented it out), so both rows collide and
  `first()` keeps whichever the file happens to list first — a coin flip.
* **`cluster_features` (all 8 waves)** — the household→cluster reduction is
  *intended* (`final_index: [t, v]`, GH #161) but **undeclared**, so it ran
  through the same `first()`. Three real defects rode on it: `v` is not a
  cluster key in 2018-19/2019-20; 2009-10's 565 out-of-frame households get no
  cluster; `Rural` is not a parish attribute.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4100` | reorders/drops index levels; collapses dups via `first()` (or `sum` for `_ADDITIVE_MEASURE_COLUMNS`) | partly | **extend** — add a declared-policy branch |
| `aggregation:` block | `data_scheme.yml` (Albania, Benin, CotedIvoire, BurkinaFaso, Guinea-Bissau, Malawi, Niger, Senegal, Togo) | **documentation only — NOTHING READ IT** | no | **extend** — make it load-bearing |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py` | the one existing collapse policy (sum for `food_acquired`) | yes | precedent for a per-table policy |
| `Wave.cluster_features` | `country.py:1167` | **its own** `first()`/`mean()` collapse when `i` is an index level | no | **fix** — 2nd instance of the same bug |
| `fill_v_with_coord_bin` | `build_transforms.py:212` | blank `v` → synthetic `@lat,lon` bin | yes | **reuse** verbatim for 2009-10 |
| `derived:` / `apply_derived` | `build_transforms.py:305` | YAML transformer dispatch | yes | reuse (but runs *after* `set_index`, so unusable when `v` is an index level → used a `df_edit` hook instead) |
| `df_data_grabber` | `local_tools.py:1018` | column extraction; **no row-filter primitive** | yes | **extend** — add `where:` |
| `uganda.v` | `Uganda/_/uganda.py:34` | country-level scalar `v` = `format_id`; auto-applied to every Uganda `v` | no | **override** per-wave (a list-valued `v` would hand it a whole row) |

## §3 Definitions & conventions in force

- `sample()` is the single source of truth for a household's cluster; `v` is
  joined from it at API time — `CLAUDE.md` "`sample()` and Cluster Identity",
  `_join_v_from_sample` (`country.py`). ⇒ **`sample.v` and `cluster_features.v`
  must be the same key**, or the join matches nothing.
- `cluster_features` owns `v`; index `(t, v)` — `Uganda/_/data_scheme.yml`.
- "NO AGGREGATION IN CORE" — `SkunkWorks/grain_aggregation_policy.org`. This
  task does not violate it: `cluster_features`' reduction is *declared by the
  country's own* `final_index`, not imposed by the composition path.
- `format_id` is auto-applied to `idxvars`, not `myvars` — `CLAUDE.md`.

## §4 Invariants & assumptions

- **The #323 warning fires only on a COLD build.** The collapse is baked into
  the L2-country cache, so the bug hides behind the cache it poisoned. Every
  measurement here ran under `LSMS_NO_CACHE=1`.
- **`.pth`-pinned imports** (`CLAUDE.md`): `PYTHONPATH` alone does not redirect
  `lsms_library` to a worktree — and `sys.path[0]` (cwd/script dir) shadows it
  too. Verified `'worktrees' in lsms_library.__file__` before trusting any run.
- `comm` (2005-06 … 2011-12) is the **2005-06 EA of origin**, an 8-digit
  structured code — verified 1:1 with District in the frame year (0 collisions
  across 322 EAs). Multi-district `comm` groups in later waves are panel
  **movers**, NOT a code collision.
- Parish **names** are not unique in Uganda (`CENTRAL` occurs in 10 districts).
- `astype(str)` on `pd.NA` yields `'<NA>'`, not `'nan'` — a blank-`v` scan that
  only tests `'nan'` under-reports (this cost one wrong intermediate reading).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| duplicate-collapse policy | **extend** `_normalize_dataframe_index` + make the existing `aggregation:` block load-bearing | the block already exists in 9 countries as prose; a parallel mechanism would fork the vocabulary |
| row filter for `CEA01` | **new** `where:` in `df_data_grabber` | no row-filter primitive existed; the alternative (`materialize: make` script) is far heavier and loses the YAML path |
| 2009-10 synthetic cluster | **reuse** `fill_v_with_coord_bin` | `sample` already builds exactly this label; a second implementation would drift and the two `v`s **must** agree |
| cluster-attribute reducer | **new** `unique` (agree → value; disagree → `<NA>` + warn) | `first` = arbitrary (class-1). `mode` was **tried and rejected on evidence** — see §6 |
| GPS reducer | `median` | a centroid is a defined statistic, not a guess; no-op on clean clusters (p50 spread = 0.0 km), and robust to the outlier coords that would drag a `mean` ~100 km |
| cluster key 2018-19/19-20 | `(district, parish)` | eliminates all 20 name collisions; **not** finer — see §6 |

## §6 Decisions that went against the brief (with the evidence)

1. **`comm` was NOT re-keyed.** The brief prescribed splitting `comm`
   (2010-11/11-12) by district, same as `parish_name`. Evidence says they are
   different diseases: `comm` is a structured code with **zero** district
   collisions in its frame year, and its multi-district groups show a
   *dispersal* signature (median majority-district share 0.86/0.82; movers only
   16%), whereas `parish_name`'s `CENTRAL` has a 24% majority across 10
   districts — a *collision* signature. Splitting `comm` by district would
   fracture real EAs, desynchronise `v` from `sample` and from the 2005-06
   baseline, and manufacture "clusters" out of movers.
2. **`mode` rejected as the categorical reducer.** Validated against the only
   independent ground truth (the 2005-06 frame, where comm→district is 1:1):
   `mode` recovered the right district for 78% of the *ambiguous* groups — but
   only **84% of the unambiguous control** groups, because Uganda split its
   districts between 2005 and 2011. An estimator that misses 16% of the cases
   whose answer is already known cannot certify the cases whose answer is not.
   ⇒ `<NA>`, loudly (class-2 beats class-1).
3. **Key is `(district, parish)`, not the finest available.** Adding
   `subcounty` splits 10 further groups, but *every* one is a spelling artefact
   of a single subcounty — `NYENGA` / `NYENGA DIVISION` (centroids 0.0 km
   apart), `LUBAGA` / `RUBAGA DIVISION` (5.3 km), `'KAGADI  TOWN COUNCIL'` /
   `'KAGADI TOWN COUNCIL'` (a doubled space). Keying finer would fragment real
   parishes on data-entry noise — a new bug traded for the old one.
4. **2005-06's 368 phantom `outer`-merge rows left to GH #606.** They carry
   `v = NaN` and are already dropped by `groupby(dropna=True)`, so they never
   reach the API; changing `how='outer'` at `country.py:1032` is library-wide
   and would risk 40 countries for zero Uganda gain. Documented, not touched.

---
### Phase 3 — verification

- `_apply_where` / `df_data_grabber(where=)` — **OK (anchored §2, §5)**: new
  primitive; none existed. `where=None` is a strict no-op, and no country
  declares a myvar named `where`, so every other country is unaffected.
- `_declared_aggregation` / `_REDUCERS` / `_reduce_unique` — **OK (anchored §2,
  §5)**: makes the *existing* `aggregation:` vocabulary load-bearing rather than
  forking it. The 9 other countries all declare `visit: first`, where `visit` is
  an **index level, never a column**, so `covered == []` and they keep the legacy
  path verbatim — confirmed by BEFORE/AFTER hash equality.
- `Wave.cluster_features` — **OK (anchored §2)**: was a second, independent
  instance of the same `first()` bug, and its comment asserted an invariant
  ("Region/Rural/District are invariant within a cluster by construction") that
  the data violates (122 Rural disagreements in 2019-20; 93 District in
  2010-11). Now defers to the declared policy; undeclared countries byte-identical.
- `Uganda/2009-10/_/mapping.py:cluster_features` — **OK (anchored §5)**: reuses
  `fill_v_with_coord_bin` with the same source columns and defaults, so its
  labels are identical to `sample`'s by construction rather than by coincidence.
- `Uganda/{2018-19,2019-20}/_/mapping.py:v` — **OK (anchored §2, §4)**: overrides
  the country-level scalar `uganda.v`; declared as a list in **both** `sample`
  and `cluster_features` so the two keys cannot drift.
- No REINVENTION or CONTRADICTION found.
