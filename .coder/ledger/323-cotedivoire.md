# Prior-Art Ledger — GH #323 (CotedIvoire)

**Search tier used:** ripgrep + git floor (gitnexus not consulted; the work is
config-scoped to one country and the framework symbol was read directly).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py:4100`) reorders a table's
index to the levels declared in the country's `_/data_scheme.yml`, and when the
result is NOT unique it collapses it with `groupby().first()` — silently
discarding the dropped rows. CotedIvoire trips this in **three structurally
different ways**, so one fix does not close it:

* **A — cluster_features, all 5 waves (17,896 of 17,909 dropped rows, 99.9%).**
  An **extraction bug**. Every wave's `data_info.yml` points `cluster_features`
  at a **household-grain** source — `../Data/WEIGHT{85,86,87,88}.DAT` and
  `Menage/s00_me_CIV2018.dta`, the very files `sample` reads with
  `i: [CLUST, NH]` / `[grappe, menage]` — while declaring only `v` as the index.
  So the extraction emits one row per *household* into a table whose grain is
  one row per *cluster* (1588/1600/1600/1600/12992 rows for 100/100/100/100/1084
  clusters), and the framework collapses the surplus. `(t, v)` is the CORRECT
  grain (cluster_features owns `v` — CLAUDE.md); the rows are redundant repeats
  of a cluster-constant attribute, not lost information. **Row count already
  right (1484); correctness-by-construction was not.**
* **B — plot_inputs, 2018-19 (9 rows).** `harmonize_seed_crop` in
  `_/categorical_mapping.org` was **non-injective**: four distinct reported seed
  labels shared one `Autre crop` bucket, and `harmonize_input` maps all of them
  to `Seed`, so two different reported line-items landed on the identical
  `(i, 'Seed', 'Autre crop', u)` key. Real reported quantities were discarded.
* **C — household_roster (2) + individual_education (2), 1988-89.** The RAW
  source repeats person keys: `SEC01A.DAT` has 4 rows duplicated on
  `(CLUST, NH, PID)`, all in CLUST 122 (NH 19 and NH 21). A source data-quality
  defect, not an extraction bug.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders to declared index; collapses non-unique index via `groupby().first()`, warning only on a cold build | indirectly | **untouched** — fixed upstream in config, per country |
| `_ADDITIVE_MEASURE_COLUMNS` | `lsms_library/feature.py` | the ONE existing declared collapse policy (sum, for `food_acquired`) | yes | not applicable — none of A/B/C is an additive measure |
| `df_edit` hook | `country.py:802` (`final_mapping['df_edit'] = formatting_functions.get(request)`) | a function in `{country}.py` / wave `mapping.py` whose NAME matches a declared table post-processes that table's extracted frame; fires on BOTH the single-file and `dfs:` merge paths | yes (used by `shocks`, `interview_date`) | **reused** — this is the fix vehicle for A and C |
| `cotedivoire.shocks` / `.interview_date` | `CotedIvoire/_/cotedivoire.py` | existing `df_edit` hooks — the pattern to follow | yes | pattern reused |
| `CROP_NA` sentinel | `CotedIvoire/2018-19/_/plot_inputs.py:31` | non-null token so a NaN index level isn't dropped by the collapse | — | **precedent, and a cautionary tale** (see §4) |
| `get_categorical_mapping` | `local_tools.py` | loads `Original Label -> Preferred Label` from `_/categorical_mapping.org` | yes | reused (the B fix edits the org table it reads) |

## §3 Definitions & conventions in force

- **cluster_features owns `v`; do NOT put `v` in other feature indexes** —
  `CLAUDE.md` ("`sample()` and Cluster Identity"). This is why the A fix
  projects to `(t, v)` rather than adding `i` to the index.
- **Canonical index per table** — `lsms_library/data_info.yml` `index_info`:
  `cluster_features: (t, v)`; `household_roster` / `individual_education`:
  `(t, v, i, pid)`. Confirms `(t, v)` is the intended cluster grain.
- **`aggregation:` in `data_scheme.yml` is DEAD CONFIG** — see §4. Declaring
  one would be documentation, not enforcement.
- **Reported-only discipline** — the `data_scheme.yml` comments for CotedIvoire's
  item-level tables ("REPORTED values only; … are transformations, never columns
  here"). Drives the C decision: emit every reported record.

## §4 Invariants & assumptions

- **`aggregation:` in `data_scheme.yml` IS NOT IMPLEMENTED.** It appears only in
  *skip-these-meta-keys* sets (`country.py:2387`, `diagnostics.py:230`);
  `_normalize_dataframe_index` never reads it. CotedIvoire's existing
  `interview_date: aggregation: {visit: first}` is therefore **prose, not
  enforcement**. The brief's "declare it in `aggregation:`" option was
  **rejected for exactly this reason** — it would have shipped a comment and
  called it a policy. Fixes here are enforced by code + assertions instead.
  *(Worth a separate issue: either implement the key or delete it.)*
- **The API index is unique BY CONSTRUCTION after the collapse.** So a test that
  asserts "the returned index is unique" **passes with the bug fully present**.
  This burned a cycle here: 9 of 10 first-draft tests passed pre-fix. Tests must
  assert on the **pre-collapse wave extraction**, on **row counts**, and on the
  **specific rows being eaten** — never on post-collapse uniqueness.
- **The bug hides behind the cache it poisoned.** The #323 warning fires only on
  a cold build; warm runs read the already-collapsed L2-country parquet. All
  numbers here are from **cold builds into an isolated `LSMS_DATA_DIR`** and from
  **L2-wave** parquets — never from `var/`.
- **`git stash` is repo-global, not worktree-local.** A bare `git stash pop` in a
  worktree pops whatever is on top of the SHARED stack — I popped the Malawi
  agent's stash into this worktree (restored it via `git stash store <sha>`).
  Use explicit SHAs, or a temp commit, for before/after captures.
- Cluster-constancy (measured, all 5 waves): `REGION` constant in 100/100
  clusters × 4 waves; `s00q01` constant in 1,084/1,084 grappes; Lat/Lon constant.
  **One exception: grappe 648 — 11 households `Rural`, 1 `Urbain`.**

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| cluster_features projection to `(t, v)` | **reuse** the `df_edit` hook (new function `cotedivoire.cluster_features`) | the framework already dispatches a table-named function as that table's post-extraction hook, on both the single-file and `dfs:` paths; one country-level function covers all 5 waves. No library change, no new mechanism. |
| conflict reducer for a disputed cluster attribute | **new** (`_cluster_majority`) | no existing reducer fits: `first` is a row-order lottery (the defect), `sum` is meaningless for a categorical. Strict majority, `<NA>` on a tie, warn always. |
| seed→crop injectivity | **extend** the existing `harmonize_seed_crop` org table | the collision is in the data, not the machinery; splitting the catch-all is the minimal, source-faithful change. |
| plot_inputs grain guard | **new** assertion in the wave script | the `CROP_NA` comment proves a comment is not a guard; the runtime assert is. |
| 1988-89 person-key collisions | **new**, wave-scoped (`1988-89/_/mapping.py`) | a country-wide dedup rule would paper over 4 rows in 2 households and hide the next occurrence. The brief explicitly forbids extending it country-wide. |

## §6 Open questions for the human

- **CLUST 122 / NH 21 / PID 10 is genuinely undecidable from the source.** Two
  rows, same age (10) and same relationship (Son/Daughter), **different SEX** —
  so they cannot be one person's consistent record. Either the household has two
  10-year-olds who were given one PID (plausible: polygamous household with two
  spouses, and it already contains two same-age different-sex children as
  separate pids — 11=F,8 and 12=M,8), or it has one child whose sex was
  corrupted on one of two entries. I keep **both reported records**, re-keyed
  `10` / `10_2`: this invents no *measurement* (every Sex/Age/Relationship is as
  reported; only the key label is synthetic), whereas collapsing asserts a Sex
  with a coin-flip chance of being wrong AND deletes a reported observation.
  **`HHEXP88.DAT.HHSIZE8` = 13 for this household (= the roster ROW count) but it
  is NOT independent evidence** — it is itself built by counting rows of the same
  defective file, so it counts the duplicate either way. (It is not a mere row
  count — it differs from the row count in 369/1600 households — but that does
  not let it see through the duplicate.) **If the maintainer prefers class-2
  (drop the ambiguous person loudly) over preserving both records, this is the
  one line to change**, in `1988-89/_/mapping.py::_resolve_pid_collisions`.
- **CotedIvoire `assets` returns 2,169 duplicate index tuples at the API**
  (pre-existing, unchanged by this PR — present identically before and after).
  It is NOT a #323 row-loss (no rows are dropped; the declared `(t, i, j)` index
  simply is not unique because a household can report several items of one type).
  Out of scope here, but it wants a real grain decision. Flagging rather than
  silently leaving it.

---
### Phase 3 — verification

- `cotedivoire.cluster_features` — **OK (anchored on §2, §3)**: uses the existing
  `df_edit` dispatch; projects to the `data_info.yml`-canonical `(t, v)`; does not
  add `i`, so it does not contradict "cluster_features owns `v`" (§3).
- `cotedivoire._cluster_majority` — **OK (anchored on §5)**: new, justified —
  no existing reducer is correct for a disputed categorical cluster attribute.
- `harmonize_seed_crop` split — **OK (anchored on §1-B, §4)**: restores
  injectivity over the labels in use; the `Sésame` bucket is left non-injective
  **deliberately** (it folds a *typo* variant, `césame`, of the SAME crop — a
  legitimate spelling fold, and it produces zero collisions).
- `plot_inputs.py` uniqueness assert — **OK (anchored on §4)**: converts the
  `CROP_NA` comment's intent into enforcement.
- `1988-89/_/mapping.py` — **OK (anchored on §5)**: wave-scoped, warns on every
  collision; the residual ambiguity is escalated in §6 rather than buried.
- **No CONTRADICTION or REINVENTION found.** No library code changed; the diff is
  confined to `lsms_library/countries/CotedIvoire/` plus one new test file, so no
  other country's build can be affected.
