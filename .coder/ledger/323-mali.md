# Prior-Art Ledger — GH #323 (Mali)

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`.

**Search tier used:** ripgrep + git floor (gitnexus MCP not available in this
worktree session); cross-checked every claim against the SOURCE `.dta` via
`get_dataframe()` rather than against `var/` (see §4).

## §1 Task, restated

`_normalize_dataframe_index` (`country.py`) collapses a non-unique DECLARED index
with `groupby().first()`, silently dropping the losing rows. Mali has 11 affected
(wave, table) cells. Fix the cells AND the class, prove the recovered rows are
real, and prove nothing else moved.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4100` | reorders/drops index levels; collapses dups (sum for additive tables, else `first()` + #323 warning) | yes | reuse — do not touch |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py:101` | `food_acquired -> (Quantity, Expenditure)`; makes the dup-collapse SUM, not `first()` | yes | reuse — **already fixes food_acquired** |
| `Wave.column_mapping` | `country.py:704` | builds `{file: {idxvars, myvars}}`; supports **per-file overrides** in the `file:` list | partly | extend (add `{const: …}`) |
| `df_data_grabber.grabber` | `local_tools.py:1045` | resolves a var spec to a Series (str col / `(col, fn)` tuple) | yes | extend (add `{const: …}`) |
| `Wave.grab_data` `dfs:` merge | `country.py:997` | outer-merges sub-dfs on `merge_on` | yes | extend (reduce sub-df to declared grain) |
| canonical `index_info` | `data_info.yml` | `interview_date -> (t, v, i, visit)` — per-visit is canonical (GH #506) | yes | reuse (Mali was the outlier) |
| `mali.food_acquired_to_canonical` / 2014-15 `mapping.py` | `Mali/2014-15/_/mapping.py:50` | melts the 3 EACI acquisition streams | — | doc-only change |

## §3 Definitions & conventions in force

- `pid`: the PERSON's identifier. Mali EACI source `EACIIND_p1.dta`:
  `s01q00` = "Numero d'ordre" (the person's roster line);
  `s01q` = "Code du répondant" (the line of whoever ANSWERED the section).
  Stata variable labels read from the L1 blob — these are the authority.
- `interview_date` canonical index = `(t, v, i, visit)` — `lsms_library/data_info.yml`
  `Index Info > index_info` (GH #506: "keep per-visit"). Niger / Benin / Togo /
  CotedIvoire all declare `visit`; Mali declared `(t, v, i)` and was the outlier.
- `food_acquired` canonical index = `(t, v, i, j, u, s)` — no `visit` level; the
  table is an ADDITIVE-measure table per `_ADDITIVE_MEASURE_COLUMNS`.
- EHCVM `vague` IS a sample split (each grappe in exactly one vague) — per
  `CLAUDE.md` "Gotchas". EACI `passage` is NOT (see §4).

## §4 Invariants & assumptions

- **The L2-COUNTRY parquet (`var/`) is written POST-collapse.** Scanning it for
  this bug returns false zeros. The L2-WAVE parquet (`{wave}/_/{t}.parquet`)
  holds the truth. Instrument validated against two known positives
  (Mali roster 32,026; Guyana housing 311) before any zero was trusted.
- **Library-code edits do NOT invalidate the content-hash cache** (the hash covers
  config + wave/country modules, not `country.py`). Every measurement here ran
  with a CLEAN `LSMS_DATA_DIR` **and** `LSMS_NO_CACHE=1`. A mid-task run without
  it silently served a stale post-collapse parquet and "lost" two waves.
- **The `.pth` in site-packages pins `lsms_library` to the MAIN checkout**;
  `PYTHONPATH` does not win. Verification scripts `sys.path.insert(0, <tree>)`
  and assert the resolved `lsms_library.__file__` is under that tree.
- **The shared main checkout is not a stable baseline.** Mid-task it was on
  `development` @ `d572d8a9`; a concurrent agent moved it to *its own unmerged
  branch* `fix/602-spellings` @ `2a775f34`. A "BASE" run that imports from the
  main checkout therefore silently measured ANOTHER AGENT'S CODE — it manufactured
  a phantom 789-cell `Rural: Urban -> Urbain` regression (GH #602 adds the French
  spelling variants; my branch point predates it). The only sound baseline is this
  branch's own merge-base, materialized with `git archive d572d8a9` into a private
  tree. Both BASE and FIX sweeps here import from trees under my control.
- `aggregation:` in `data_scheme.yml` is **parsed and IGNORED** — it appears only
  in a `_skip` set (`country.py:2387`). Declaring a reducer there is prose, not
  enforcement. The only real reducer lever is `_ADDITIVE_MEASURE_COLUMNS`.
- EACI `passage` is a REPEATED MEASURE, not a sample split (proved from source:
  `EACIALI_p1`/`_p2` each cover all 3,804 households — p1-only 0, p2-only 0 —
  a median 124 days apart, each with its own 7-day recall).
- `groupby().first()` SKIPS NaN per column; `drop_duplicates()` keeps the first
  ROW including its nulls. Not interchangeable (this bit once — see Phase 3).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| roster/education `pid` (2014-15) | **fix config** | one-token: `s01q` -> `s01q00`. Makes the declared index exactly unique (37,175/37,175). |
| `interview_date` visit level | **reuse canonical** | declare `(t, v, i, visit)` — matches `data_info.yml` + 4 EHCVM peers. |
| 2014-15 `visit` value | **extend** (`{const: …}`) | `EACICONTROLE_p1/_p2` have identical columns and NO `passage`; the visit is in the FILENAME. Injected as a literal = ground truth. Rejected: ranking `Int_t` (the old hook) — a positional guess that mislabels the 66 households whose passage-2 date precedes passage-1. |
| `food_acquired` reducer | **reuse, no change** | `_ADDITIVE_MEASURE_COLUMNS` already SUMS Quantity/Expenditure (GH #501). Verified at the API: dup group returns 5.0/1000.0 (sum), not 1.0/0.0 (first). Nothing is lost; only the docstring's false premise needed correcting. |
| `cluster_features` grain | **built, then DEFERRED — see §6** | the fix works and is value-correct for Mali, but it perturbs 6 Malawi GPS cells by 1 float32 ULP for reasons I could not explain, so it is NOT in this PR. |

## §6 Open questions for the human

### DEFERRED (D): `cluster_features` sub-df grain — diagnosed, implemented, then pulled

Not shipped. Recording it in full so the next agent starts from the answer.

**The defect (real, and confirmed from source).** `cluster_features` is a `(t, v)`
cluster table, but its `dfs:` sub-frames are declared with a coarse index (`v`)
while the underlying files are household- or item-grain. `pd.merge(..., on=['t','v'])`
is therefore many-to-many, and `_normalize_dataframe_index` hides it with
`groupby().first()`. Mali 2021-22: `df_main` = `ehcvm_conso_mli2021.dta` (393,480
item rows) merged against `df_geo` = `s00_me_mli2021.dta` (the household cover
page, 6,143 rows, ~11.97/grappe) produces **4,718,148 rows to describe 513
clusters** — I reproduced that number exactly as `sum_over_v(n_main * n_geo)`.
All four waves: 2,815 / 7,437 / 366,088 / 4,717,635 rows collapsed.

**It is value-benign for Mali.** I verified every extracted column is constant
within grappe in all four waves (`region`/`milieu`/`s0q01`/`s0q04`, and
`GPS__Latitude`/`Longitude` in 2021: 0 of 513 grappes with >1 distinct value). So
`first()` returns the right cluster values *by accident*. Zero rows are recoverable
here; the cost is a 4.7M-row cartesian and the accidental correctness.

**The fix I built** (`Wave._reduce_subdf_to_declared_grain`, reverted in this
branch — recover it from this branch's history): reduce each sub-df to one row per
its OWN declared `idxvars` before the merge, using `groupby().first()` (NaN-skipping,
matching the old reducer) after casting unordered categoricals to `str`; warn loudly
when a carried column is NOT constant within that key. Mali result: `cluster_features`
fingerprint IDENTICAL, #323 warnings 4 -> 0, 2021-22 merge 4,718,148 -> 513 rows.

**Why it is not shipped.** A cross-country sweep of all 41 `dfs:` (country, table)
pairs, with the config tree held constant so only the code varied, showed 40/41
identical and **Malawi `cluster_features` moving 6 GPS cells by exactly one float32
ULP** (e.g. `-11.536399841308594` -> `-11.536398887634277`, ~10 cm). The source
column `lat_modified` is `float32` and provably constant within the grappe; both
paths produce the *identical exact float32 at wave level*; the divergence appears
between `grab_data` and `_finalize_result`'s input, where the column has become
`float64`. It is triggered by the ROW-COUNT change alone (not by the choice of
reducer — `drop_duplicates` and `groupby().first()` are bit-identical on float32 in
isolation), which points at something row-count-dependent in the wave->concat path.
I could not explain it, and an unexplained value change in another country fails the
"nothing else moved" bar, so I pulled it rather than ship it.

**Two live findings that fix surfaced and that outlast it:**
- **CotedIvoire `cluster_features.Rural` is NOT constant within grappe** (the
  warning fired on `df_main`, 12,992 rows -> 1,084 grappes). The current
  `groupby().first()` is therefore already picking an arbitrary value for those
  clusters — a genuine latent class-1 bug, independent of anything here.
- **`CotedIvoire/2018-19/Data/Menage/grappe_gps_CIV2018.dta` is untracked** — it
  exists as a workspace file in the main checkout but has no `.dvc` sidecar in git
  (43 files vs 39 sidecars). Any clean checkout silently loses CotedIvoire's
  Latitude/Longitude via the "optional sub-df" fallback. Worth its own issue.


- **food_acquired recall basis across waves.** The two EACI passages are summed,
  so 2014-15 / 2017-18 are TWO-recall-week totals while the EHCVM waves
  (2018-19, 2021-22) are single 7-day recalls. Summing is lossless and
  price-neutral (the factor cancels in `Expenditure/Quantity`), and the windows
  are disjoint for 3,738 of 3,804 households — but consumers comparing LEVELS
  across waves must account for it. Documented in the 2014-15 `mapping.py`
  docstring; not silently "corrected".
- **10 pre-existing duplicate index tuples in `food_acquired`** survive
  `_finalize_result` on BOTH base and fix (unchanged by this work). Out of scope
  here, but they are real and should be filed separately.
- The canonical-mapping / spelling enforcement silently no-ops on a pandas
  Categorical (`.replace()` doesn't add categories). It only worked for
  `cluster_features.Rural` because the dup-collapse incidentally cast to `str`.
  Worked around locally (§Phase 3); the general latent bug remains.

---
### Phase 3 — verification

- `data_info.yml` `pid: s01q -> s01q00` (2014-15 roster + individual_education) —
  **OK (anchored on §3)**: `uniq(grappe, menage, s01q00) == 37,175 == row count`;
  `s01q` gives 5,149, exactly the row count the API was returning.
- `interview_date index: (t, v, i, visit)` — **OK (anchored on §3)**: restores the
  canonical index; 2018-19/2021-22 are unaffected (vague = sample split, already
  unique on `(t, v, i)`), so one declaration serves all four waves.
- `{const: …}` in `grabber` / `column_mapping` — **OK (anchored on §2)**: extends the
  existing per-file-override mechanism (whose `t`-constant branch was dead code).
  Collision-free: zero bare-dict `idxvars`/`myvars` values exist in any country config.
- `_reduce_subdf_to_declared_grain` — **OK (anchored on §4)**. It reduces with
  `groupby().first()` (NaN-skipping), NOT `drop_duplicates()` (which keeps the
  first row's nulls); `.first()` is the same reducer the old collapse applied to
  the merged frame, so a deduped sub-df is value-identical. It also casts unordered
  categoricals to `str` — required because `groupby().first()` raises on them, and
  for dtype parity with the old collapse path. Measured: Mali `cluster_features`
  fingerprint IDENTICAL to base, #323 warnings 4 -> 0, and the 2021-22 merge drops
  from 4,718,148 rows to 513.
- Removal of the rank-based `visit` synthesis in 2014-15 `mapping.py` —
  **OK (anchored on §5)**: replaced by source `passage`; no positional guess.

**A note on a false lead, recorded because it nearly shipped.** An early cell-diff
showed 789 `cluster_features.Rural` cells flipping `Urban` -> `Urbain`, and I
"fixed" it by adding the categorical cast above. That was a *phantom*: the BASE
side of that comparison had imported a concurrent agent's unmerged
`fix/602-spellings` branch out of the shared main checkout (see §4). Against the
true merge-base the fingerprint is identical and the cast is a no-op for Mali
(`Rural` arrives as `object`, never a Categorical). The cast is retained on its own
merits (reason 1 above), and its code comment was rewritten to say what is actually
true rather than the story I first believed.
