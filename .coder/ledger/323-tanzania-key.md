# Prior-Art Ledger — GH #323 (Tanzania, Site 2): the `cluster_features` cluster key

**Search tier used:** ripgrep + git, plus
`slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org` (branch
`origin/docs/323-grain-collapse-sites`), the Uganda template
(`origin/fix/323-uganda-config` + `.coder/ledger/323-uganda.md`), and the
already-landed Tanzania config commit `3a387bca`.

**Every measurement below is a COLD build** against an **isolated
`LSMS_DATA_DIR`** whose `dvc-cache` is symlinked to the shared L1 blob cache, so
L2 is rebuilt honestly while raw blobs are reused. `LSMS_NO_CACHE=1` alone is
NOT sufficient — it is *soft* for script-path L2-wave parquets, and Tanzania
`2008-15/cluster_features` is exactly such a script. Config tree =
`LSMS_COUNTRIES_ROOT=<worktree>/lsms_library/countries`, asserted before any
number was trusted.

## §1 Task, restated

Tanzania is the worst Site-2 cell in the corpus: **2,104 of 7,167
cluster-attribute cells contested (29.4%)**, 65.4% of them in 2012-13. Fix the
**identifier**, not the symptom. CONFIG-ONLY: nothing under `lsms_library/*.py`;
no `aggregation:` keys (D1); no reducer-by-declaration and no blanking-to-NA as
*the* fix.

## §2 Existing machinery (this task's area)

| symbol | path | what it does | reuse / extend / new |
|--------|------|--------------|----------------------|
| `_collapse_to_cluster_grain` | `country.py:4490` | Site 2's audit-then-`.first()` | **do not touch** (core, PR #617) — but it is the *instrument*: monkeypatched to measure |
| `reduce_to_agreed` / `collapse_to_cluster_grain` | `build_transforms.py:422/514` | lossless-or-loud grain reducer | **considered, NOT used** — see §6.1 |
| `Wave.column_mapping` → `final_mapping['df_edit']` | `country.py:802` | dispatches a function named after the table as that table's frame hook | **reuse** — the sanctioned YAML-path extension point (2019-20 / 2020-21) |
| `Tanzania/2008-15/_/cluster_features.py` | config | script-path extraction for the 4-round folder | **extend** |
| commit `3a387bca` | config | named the grain (`j`→`i`), `''`→`pd.NA` | **build on** — it made the collapse *visible*; it did not fix the key |
| `origin/fix/323-tanzania` | branch | the REJECTED Design-A patch (+167 lines of `country.py`) | **do not salvage the code**; the diagnosis is already in `3a387bca` |
| `Uganda/_/uganda.py` composite `v` (PR #634) | config | `DISTRICT/PARISH` composite key | **pattern considered, does not apply** — see §6.2 |

## §3 Definitions & conventions in force

- `sample()` is the single source of truth for a household's cluster; `v` is
  joined at API time by `_join_v_from_sample` — `CLAUDE.md` §"`sample()` and
  Cluster Identity". ⇒ **`sample.v` and `cluster_features.v` must be the same
  key.** Verified per wave (§7).
- `cluster_features` owns `v`; index `(t, v)` — `Tanzania/_/data_scheme.yml`.
- "NO AGGREGATION IN CORE" — `SkunkWorks/grain_aggregation_policy.org`, D1 of
  the consolidation note.
- Multi-round folder semantics: `.claude/skills/multi-round-waves.md` — the
  `2008-15/` script emits all four rounds with `t` in the index; paths use
  `wave_folder`, not `year`.
- NPS panel design: `.claude/skills/tanzania-panel-design.md` — the 2014-15
  extended/refresh split, and **that the NPS TRACKS movers**, which is the whole
  root cause here.

## §4 Invariants & assumptions (the landmines)

- **The bug hides behind the cache it poisoned.** Site 2 writes the L2 parquet
  *post*-collapse. Every number here is from an isolated data dir (§ header).
- **The geocode scheme is a property of the WAVE, not of the string's length.**
  2019-20's 8-character `'11014002'` is `01-1-014-002` = DODOMA under its own
  9-digit scheme, and IRINGA under the 8-digit one. `cluster_region()` therefore
  *requires* the caller to name the scheme; pinned by test.
- **Residency must be evaluated per `(t, v)`, not per `v`.** A cluster can have
  residents in one round and none in the next. The first cut grouped on `v` and
  silently lost **12 cluster-waves**; caught by a test that pins per-round
  cluster counts.
- **Stata writes an unpopulated STRING variable as `''`, not as missing.** The
  first cut used `.notna()` on `sdd_cluster` and dropped 63 rows instead of 389.
  Caught only because the split-off exclusion produced no cluster-count change.
- `format_id` is auto-applied to `idxvars`, not `myvars` — `CLAUDE.md`.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| what a cluster's Region/District/Rural ARE | **new, but no new primitive**: a residency FILTER in config | the columns are household attributes in a tracking panel; the fix is to stop asking non-residents, not to reduce their answers |
| the region/district crosswalk | **new**, checked in as `TZ_REGION_BY_CODE` in `tanzania.py` | mined from the 2008-09 frame, cross-checked against 2019-20 `t0_region` and 2020-21 `hh_a01_1`; a runtime mine would be silently re-derived on every build and is untestable |
| the household→cluster projection | **leave in core** (`Wave.cluster_features`) | see §6.1 |
| 2020-21 `v` | **re-key to `y5_cluster`** | `sample` already uses it; `clusterid` matched nothing and is NaN for 11.6% of rows |
| 2019-20 geography | **re-source to `t0_region`/`t0_district`** | `hh_a01_1` is the MOVER question — null for 89.9% of the households that can describe the cluster |

## §6 Decisions, with the evidence

1. **The config does NOT collapse to `(t, v)` itself** — no `reduce_to_agreed`,
   no `collapse_to_cluster_grain` hook. Collapsing in the config would drive the
   #323 instrument to 0 *for free* and blind the framework's Site-2 audit to
   whatever remains. The frame is left at household grain and only the
   *non-resident rows* are removed, so the before/after numbers are measured
   with the same instrument and the residue is still announced by
   `GrainCollapseWarning`. A test pins that `i` survives into the projection.

2. **A composite key (the Uganda fix) is the WRONG fix here, and the data say
   so.** Uganda's `v` was ambiguous: a parish code unique only within a
   district. Tanzania's is not. In round 1 — the only round in which nobody has
   moved — **all 409 clusters agree on all three attributes, 0 contested**, and
   the cluster id's own region field maps 1:1 onto the region name (26/26 codes)
   with its district field equal to the reported district code for **100.0%** of
   rows. Making `v` a `(region, cluster)` composite *would* have zeroed the
   Region column, but tautologically: it merely re-labels the mover's row with
   the mover's region and calls the disagreement gone. Measured, and rejected on
   that basis:

   | 2012-13 key | clusters | Region | District | Rural |
   |---|---|---|---|---|
   | `clusterid` | 409 | 242 | 314 | 247 |
   | `(region, clusterid)` | 821 | **0** | 253 | 178 |
   | `(region, district, clusterid)` | 1,165 | **0** | **0** | 142 |

   The composite inflates 409 real clusters into 1,165 by splitting each one
   across the regions its movers went to. That is not a finer key; it is a
   fabricated one, and it would desynchronise `v` from `sample`.

3. **`strataid2` looked like the perfect cluster-invariant source and was
   rejected on measurement.** It is `"<REGION> - RURAL|URBAN"` and shows *zero*
   within-cluster variation in all four rounds — because it is **100% NULL in
   rounds 1-3**. The "invariance" was an artefact of missingness. Exactly the
   trap #323 exists to catch; recorded so nobody re-proposes it.

4. **Cluster attributes are NOT taken from the cluster's frame round and carried
   forward.** Tried: per `v`, use the round the cluster first appears in. Round 1
   (409 clusters) and 2014-15 (419 new) come out at 0 contested, but the 82
   clusters that first appear in 2012-13 come out at 172 — they are pre-2012
   clusters *renumbered* after the Simiyu/Geita/Njombe/Katavi region splits, so
   their "frame round" is not a frame round at all. Residency generalises
   correctly where first-appearance does not.

5. **`urban/rural` is not in the geocode.** Tested: the EA-code leading digit
   predicts `urb_rur` for only 75.3% of round-1 clusters. So `Rural` stays a
   reported attribute, and its 264 residual contested cells are real
   sub-cluster variation, not decodable away.

6. **2019-20 split-off rows are dropped, and that removes 100 clusters.** Their
   `clusterid` is not a sampling cluster: under a single DODOMA code they pool
   households whose own strata span DAR ES SALAAM, DODOMA and ZANZIBAR, and of
   the 269 (of 326) whose parent household IS in the frame, only **31** carry
   the parent's cluster. Emitting cluster rows
   built from them is fabrication (the parent's "18 fabricated rows"). The honest
   consequence — those 326 households' `sample.v` now points at a cluster the
   table does not contain — is recorded in §9 rather than papered over.

7. **Malformed ids are carried through, not repaired.** Three 2020-21
   `y5_cluster` values have the wrong field widths and one carries region code
   `27`. Guessing the intended value is unfalsifiable; the tests pin the count so
   it cannot grow.

## §7 Measured effect (cold build, isolated `LSMS_DATA_DIR`)

Contested cluster-attribute cells (`nunique(dropna=True) > 1` per column, on the
pre-collapse frame — the parent's instrument, reproduced to ±2):

| wave | before | after | clusters |
|------|--------|-------|----------|
| 2008-09 | 0 | 0 | 409 → 409 |
| 2010-11 | 329 | **78** | 409 → 409 |
| 2012-13 | 803 | **147** | 409 → 409 |
| 2014-15 | 160 | **41** | 498 → 498 |
| 2019-20 | 257 | **72** | 247 → **147** |
| 2020-21 | 557 | **47** | 418 → **515** |
| **total** | **2,106** | **385** | 2,389 → 2,387 rows |

By column: Region **706 → 3**, District 758 → 118, Rural 642 → 264.

Rows destroyed by the collapse (framework's own Site-2 audit): **10,982 →
2,687**; NaN-key deletions **545 → 0**.

`sample.v` ≡ `cluster_features.v`, `(t, v)` pairs in `cluster_features` unknown
to `sample`:

| wave | before | after |
|------|--------|-------|
| 2008-09 / 2010-11 / 2012-13 / 2019-20 | 0 | 0 |
| **2020-21** | **417 of 417** | **0** |
| 2014-15 | 5 | 5 (pre-existing, §9) |

## §8 Verification

- `tests/test_gh323_tanzania_cluster_key.py` — 28 tests, all config-level;
  exercises no core aggregation.
- **Negative control**: run against a pristine `origin/development` config tree
  (`git archive` into a separate `LSMS_COUNTRIES_ROOT`, separate data dir),
  **20 of 28 fail** — including every contested-cell ceiling, the
  `sample.v` ≡ `cluster_features.v` test, both 2020-21 key tests and the
  per-wave cluster counts.
- `tests/test_tanzania_grain_gh323.py` (19) and
  `tests/test_tanzania_community_cluster_xwalk.py` pass.
- `tests/test_schema_consistency.py` + `tests/test_gh323_benin_togo.py` pass
  (243 passed, 2 skipped in that slice).
- Pre-existing failure, NOT caused by this branch and NOT fixed by it (core is
  out of scope):
  `tests/test_gh323_explicit_reducers.py::test_core_does_not_dispatch_the_reducers`
  — PR #617's private `country._collapse_to_cluster_grain` contains PR #618's
  banned substring. Same failure is recorded in `.coder/ledger/323-uganda.md` §8.

## §9 Deferred

- **2019-20 `sample.v` for the 326 split-off households.** Their `clusterid` is
  not a sampling cluster (§6.6) and now dangles. Blanking it changes `v` on every
  2019-20 household table for 27.5% of the wave — a semantic change that belongs
  in its own PR, per the same reasoning D2 used to keep Burkina Faso's
  `dropna=False` out of a bug fix.
- **5 of 498 2014-15 clusters unknown to `sample()`.** Present in the wave-level
  `sample.parquet` (13/7/10/10/10 rows), lost between there and `sample()` —
  i.e. in `_finalize_result`'s `id_walk`, on the panel-id side. Pre-existing on
  `development`; unchanged here.
- **A `harmonize_district` table.** Most of the 118 residual District cells are
  two spellings of one district (`MBINGA (NYASA)` / `MBINGA(NYASA)` /
  `MBINGA NYASA` / `NYASA`) or a mid-panel district split (Chalinze/Bagamoyo,
  Kibiti/Rufiji, Ubungo/Kinondoni). Label harmonisation, not a key defect.
- **A `Rural` cluster classification.** 264 residual cells are EAs whose
  households are genuinely not all urban or all rural.
