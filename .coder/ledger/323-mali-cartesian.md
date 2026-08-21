# Prior-Art Ledger — GH #323 site 4 / #627: Mali `cluster_features` cartesian

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`; cites
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git floor (gitnexus not consulted; this is a
config-only change touching no symbol).

## §1 Task, restated

`Mali/2021-22/_/data_info.yml` declared `cluster_features` as a two-sub-frame
`dfs:` block merged on the cluster key `v`. Both sub-frames were finer-grained
than `v`, so `Wave.grab_data`'s `pd.merge(..., on=['v','t'], how='outer')` was a
many-to-many CARTESIAN PRODUCT: 393,480 × 6,143 → 4,718,148 rows, **4,324,668
of them phantoms** — 88% of the 4,907,774 phantom rows PR #627's 40-country
census found. `_normalize_dataframe_index` then collapsed the result to 513 rows
with `groupby().first()`, so nothing downstream ever saw it. Fix the merge (D1:
core never aggregates the explosion away afterwards). Config-only, no
`lsms_library/*.py`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave.grab_data` (`dfs:` region) | `lsms_library/country.py:989-1035` (merge at `:1033`) | outer-merges `dfs:` sub-frames on `merge_on ∪ {t}` | yes | **untouched** (core is out of scope) |
| `Wave._cartesian_keys` / `_merge_subframes` | `origin/fix/323-site4-dfs-merge` `country.py:~906-1000` | exact many-to-many detector + phantom count | 16 tests on that branch | **used as the measuring instrument only**; not merged here |
| `_normalize_dataframe_index` | `country.py:4540` | collapses non-unique declared index via `groupby().first()`, audited by `_audit_index_collapse` (`:4214`) | yes | untouched — it is the janitor, not the culprit |
| `_collapse_to_cluster_grain` (site 2, GH #161) | `country.py:4490` | projects household-grain `cluster_features` onto `(t, v)` | yes | **not reached**: Mali declares no `i` in `cluster_features` `idxvars` |
| `Mali/2021-22 sample.df_cover` | `Mali/2021-22/_/data_info.yml:85-97` | already reads `Rural` from `s00q04` of the SAME cover page | — | prior art proving `s00q04` is the milieu column |
| `Mali/2017-18 cluster_features` | `Mali/2017-18/_/data_info.yml:42-60` | reads `Region: s0q01`, `Rural: s0q04` from that wave's cover page | — | prior art for the cover-page-as-geography pattern |

## §3 Definitions & conventions in force

- **`dfs:` merges exist to be collapsed, not grown**: *"Do NOT use `dfs:` merge
  blocks just to join `v` from a cover page — collapse to a single-file
  extraction"* / *"Existing `dfs:` merges are grandfathered but should be
  collapsed when touched"* — `CLAUDE.md`, "`sample()` and Cluster Identity" and
  "Gotchas with Teeth".
- **D1, no aggregation in core**: *"Duplicates on a declared index mean the
  IDENTIFIER IS BROKEN or a LEVEL IS MISSING — fix the index; do not declare a
  reducer"*; `aggregation:` is dead config — `CLAUDE.md`, "Grain Collapse";
  `SkunkWorks/grain_aggregation_policy.org` §3a.
- **EHCVM cluster identity**: `v: grappe` (not `[vague, grappe]`),
  `i: [grappe, menage]` — `CLAUDE.md`, "EHCVM countries";
  `Mali/_/CONTENTS.org` "Sampling Design".
- **Cluster GPS is a displaced cluster fix stamped on every household**, not a
  household fix — `CLAUDE.md`, site-2 note (GH #161). Verified for Mali 2021-22
  (513/513 grappes carry exactly one distinct coordinate pair).

## §4 Invariants & assumptions

- **A merge key duplicated in BOTH sub-frames is a cartesian by construction** —
  `_cartesian_keys` docstring, `origin/fix/323-site4-dfs-merge`. Sound *and*
  complete; the row-count-ceiling heuristic is not.
- `ehcvm_conso_*.dta` is at **(household × food item)** grain — 767 rows per
  grappe in Mali 2021-22. Any `dfs:` sub-frame drawn from it and keyed only on
  `v` is finer than the merge key.
- `s00_me_*.dta` is the **household cover page** — 12 households per grappe.
  Also finer than `v`. Two finer-than-key frames ⇒ cartesian.
- `groupby().first()` **skips NA per column**, so a collapsed row can be a
  composite assembled from different source rows. Harmless here only because
  Region / Rural / Lat / Lon are each provably constant within grappe — which is
  now asserted in a test, not in a comment.
- **The warm cache hides this.** The L2-country parquet is written
  post-collapse; every before/after measurement here was made in an isolated
  `LSMS_DATA_DIR` with only `dvc-cache` symlinked in.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| `Region` (2021-22) | **repoint**: `ehcvm_conso.region` → `s00_me.s00q01` | same 9 labels, 513/513 grappes agree, constant within grappe; mirrors what 2017-18 already does |
| `Rural` (2021-22) | **repoint**: `ehcvm_conso.milieu` → `s00_me.s00q04` | ditto; `sample.df_cover` in this same file already uses `s00q04` |
| `Latitude` / `Longitude` | **reuse unchanged** (`s00_me.GPS__*`) | already came from the cover page; only the merge around it is deleted |
| the merge itself | **delete** | all four columns live in one file; a `dfs:` block with one source is not a join |
| a reducer / `aggregation:` key | **rejected** | D1 — a reducer on a cartesian "only puts a signature on the corpse" |
| `merge_on: [i]` (keep the merge, fix the key) | **rejected** | works, but leaves a `dfs:` block whose only purpose is to re-join a file to itself at the grain it already has; CLAUDE.md says collapse when touched |

## §6 Open questions for the human

- **`Rural` in `cluster_features` ships the raw French `Urbain`**, while
  `sample.Rural` in the same wave maps it to `Urban`. Pre-existing and
  corpus-wide (2014-15 shows `Urbain` too); deliberately NOT changed here
  because this PR must be value-preserving. Blocks nothing; adjacent to GH #602.
- **2018-19 merges the 366,639-row consumption file against a 549-row
  grappe-GPS file.** Not a cartesian (the right side is 1 row per grappe, so
  it is m:1) and not fixed here, but it reads 366k rows to produce 551. The
  cover page `s00_me_mli2018.dta` would supply Region/Rural at household grain;
  the GPS genuinely lives in a separate file there, so the block cannot be
  deleted, only slimmed. Deferred: it changes no value and is pure waste.

---
### Phase 3 — verification

- `Mali/2021-22/_/data_info.yml cluster_features` — **OK (anchored on §3, §5)**:
  single-file extraction, no `dfs:`, no `aggregation:`; the merge is cured, not
  laundered.
- `tests/test_gh323_mali_cartesian.py` — **OK (anchored on §4)**: asserts at the
  WAVE level because §4's cache/collapse note means a country-level assertion
  passes with the bug present. **Negative control run** (pre-fix YAML restored,
  fresh isolated `LSMS_DATA_DIR`): `2 failed, 9 passed` — the wave-level
  cartesian test reports `4718148 == 6143` and the structural test reports the
  `dfs:` block; the four country-level tests pass *with the bug present*, which
  is exactly the blindness the docstring warns about. After the fix: `11 passed`.
- No `lsms_library/*.py` touched — **OK (anchored on §1)**: the D1 core patches
  on `origin/fix/323-mali` were deliberately not reused; that branch is about
  `pid`, a different defect.
