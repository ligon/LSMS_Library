# Prior-Art Ledger — GH #323 (Malawi, Site 2): the cluster key

**Search tier used:** ripgrep + git, plus PR #634 / `.coder/ledger/323-uganda.md`
(the template for this task), the consolidation note
`slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org` (branch
`origin/docs/323-grain-collapse-sites`, Site 2 + D1/D2/D3), and PR #627's Site-4
cartesian census. Every number below is a **cold** build against an *isolated*
`LSMS_DATA_DIR` (fresh tree, `dvc-cache` symlinked to the shared L1) with
`LSMS_COUNTRIES_ROOT` pointed at this worktree and asserted before any run.
`LSMS_NO_CACHE=1` alone was **not** trusted — it is soft for script-path L2-wave
parquets, and the collapse is baked into the cache it poisoned.

## §1 Task, restated

Malawi's `cluster_features` is declared `(t, v)` in `_/data_scheme.yml` but every
one of the five waves extracts it from the household **cover page**, so the frame
arrives at household grain and is projected onto the cluster by
`Wave.cluster_features` → `country._collapse_to_cluster_grain` → `.first()`. Where
the households of one `v` disagree about the cluster's own attributes, one
arbitrary household's answer is served as the cluster's — and `.first()` skips NA
per column, so the row it returns can be a **composite existing in no source
record**. The task: find, empirically, what actually identifies a cluster in each
wave and fix the identifier. Config only (`countries/Malawi/**`); no
`lsms_library/*.py`, no `aggregation:` keys, no declared reducer, no blanking to
`<NA>`.

## §2 Existing machinery (this task's area)

| symbol | path | what it does | tested? | reuse / extend / new |
|--------|------|--------------|---------|----------------------|
| `_collapse_to_cluster_grain` | `country.py:4490` | Site 2's audit-then-`.first()`; emits `GrainCollapseWarning` | yes | **do not touch** (core, PR #617) — monkeypatched *read-only* to capture the pre-collapse frame |
| `_join_v_from_sample` | `country.py` | joins `cluster_features` onto household tables through `sample.v` | yes | the constraint: forces `sample.v` ≡ `cluster_features.v` |
| `_enforce_canonical_spellings` | `country.py` (`_finalize_result`) | `rural`/`RURAL` → `Rural` at API time | yes | **reuse** — the reason no `Rural` mapping is needed in YAML |
| auto-discovery categorical mapping | `country.py` (`_apply_categorical_mappings`) | column name ↔ `categorical_mapping.org` table name, case-insensitive | yes | **reuse**, and *also* apply the `region` table at extraction (§6.3) |
| `mappings: [table, from, to]` in `myvars` | `country.py:760` `map_formatting_function` | pulls a `categorical_mapping.org` table into a dict | yes (used by Malawi `strata` in all 5 waves) | **reuse** for 2016-17 `Region` |
| `psu` column | `Malawi/2004-05/Data/sec_a.dta` | the fully qualified 8-digit EA code, already in the file | — | **reuse** — no derivation, no composite, no new primitive |
| `cs_i` | `Malawi/2016-17/_/mapping.py`, `2019-20/_/mapping.py` | `'cs-17-' + format_id(...)`, keeps CS ids apart from panel `y{3,4}_hhid` | — | **reuse** — `df_geo` must build `i` the same way `df_main` does |
| `Rural: 0/1` inline `mapping:` blocks | all 5 waves | **already removed on `development`** by GH #602 | yes | nothing to do — do **not** re-add |
| `aggregation:` block | `Malawi/_/data_scheme.yml` (`interview_date`) | dead config; nothing reads it | n/a | **forbidden** to add more (D1); the existing one is out of scope |

## §3 Definitions & conventions in force

- `sample()` is the single source of truth for a household's cluster; `v` is
  joined from it at API time — `CLAUDE.md` "`sample()` and Cluster Identity".
  ⇒ **`sample.v` and `cluster_features.v` must be the same key.**
- `cluster_features` owns `v`; index `(t, v)` — `Malawi/_/data_scheme.yml`.
- Canonical `Rural` is `str` with spellings `Rural: [rural, RURAL, …]` —
  `lsms_library/data_info.yml`. **Not** a 0/1 indicator.
- "NO AGGREGATION IN CORE" — `SkunkWorks/grain_aggregation_policy.org`, D1 of the
  consolidation note.
- D2: NaN-key rows are deleted and reported, never retained. (Malawi has none:
  every wave's cover page populates its cluster column for every household.)
- D3: Sites 2 and 4 get their own PRs. Site 4 is PR #627 — see §6.5.

## §4 Invariants & assumptions

- **The Malawi EA code is structured**: `region(1) + district(2) + TA(2) + EA(3)`,
  8 digits. Verified by decoding: the leading digit reproduces the reported
  region for 11,280/11,280 households in 2004-05.
- **Frame vintage is not shared across the IHS2/IHS3 boundary.** 2004-05's 564
  EA codes overlap 2010-11's 768 in only **3** codes (1998 vs 2008 census frame).
  The *format* is common to all five waves; the *sets* are not. So a cross-wave
  join on `v` is meaningful from 2010-11 on and not before — and the 2004-05 fix
  does not, and cannot, create a spurious panel link.
- **The IHPS waves are TRACKED panels.** `ea_id` in 2013-14 / the panel halves of
  2016-17 and 2019-20 is the **IHS3 baseline EA**, and the panel EA sets nest
  cleanly: 2019-20-PN = 2016-17-PN = 102 ⊂ 2013-14 = 204 ⊂ 2010-11 = 768. So a
  cluster's households genuinely live in different districts. Dispersal, not
  collision — see §6.2.
- **The two halves of 2016-17 / 2019-20 do not collide.** `ea_id` is one national
  keyspace; the single code shared between the 2016-17 CS and panel halves
  (`30305580`) is the *same real EA* in the *same district*, sampled twice.
- `format_id` is auto-applied to `idxvars`, not `myvars` — `CLAUDE.md`. `psu` is
  already an 8-character string in the source, so `sample.myvars.v: psu` needs no
  formatter.
- **A cartesian merge cannot change a contested-cell count.** `nunique(dropna=True)`
  per (cluster, column) is invariant to row repetition. Assumed by PR #627's
  owner; **measured** here rather than assumed — §7.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| 2004-05 cluster key | **reuse the source's own `psu`** | Not a composite, not derived: the fully qualified code is already a column. 564 values, exactly 20 HH each, 0 contested cells. A `(dist, ta, ea)` composite would produce the identical partition (verified: 564 groups) with three columns instead of one, and would *not* be on the later waves' 8-digit keyspace. |
| 2013-14 / 2019-20 panel keys | **no change** | The contest is dispersal. Uganda §6.5 set the precedent (`comm` not re-keyed): a structured code with zero collisions in its frame year, whose multi-district groups show a dispersal signature, must not be split by current district. Splitting 2013-14 on `(district, ea_id)` yields 626 groups of **median 1 household** — 422 singleton pseudo-clusters — and would desynchronise `v` from the IHS3 baseline. |
| GPS reducer | **none** — no averaging, no median | Same argument that retired core's `.mean()` on 2026-07-13, and here it is decisive: the 2013-14 within-EA spread is *entirely* movers (§6.2), so a centroid would place a cluster in the middle of nowhere between an EA and the towns its emigrants moved to. |
| categorical reducer | **none declared** | D1. The key fix removes the conflicts that were fixable; the rest are reported. |
| `Rural` 0/1 mapping | **nothing** — already removed by GH #602 on `development` | A second implementation would fork the vocabulary. Canonical `spellings` already normalise the three cases. |
| 2016-17 `Region` | **extend**: apply the existing `region` table at extraction | The auto-discovery mapping already collapses `South`/`Southern` at API time, so this is output-neutral; it just stops the *collapse* from reading a spelling difference as a disagreement about the cluster. |
| 2016-17 `df_geo` merge key | **fix**: build `i` with `cs_i`, exactly as `df_main` does | Not a new mechanism — the transform already exists and `df_main` already uses it. The block simply failed to mirror it. |
| 2010-11 / 2019-20 cartesian `df_geo` merges | **leave, and document in place** | Site 4, PR #627 owns it (D3). The one-line config fix was implemented, measured, and then **reverted** so #627's cardinality guard keeps these two cells as evidence. §6.5. |

## §6 Decisions, with the evidence

1. **2004-05's `ea` is not a cluster id — it is an intra-TA sequence number.**
   110 distinct values for 564 EAs. Measured at source, contested cells by key:

   | key | groups | region | district | reside |
   |-----|--------|--------|----------|--------|
   | `ea` | 110 | 66 | 79 | 19 |
   | `(dist, ea)` | 447 | 0 | 0 | 8 |
   | `(dist, ta, ea)` | 564 | 0 | 0 | 0 |
   | **`psu`** | **564** | **0** | **0** | **0** |

   `psu` ≡ `(dist, ta, ea)` exactly (each is unique within the other), with 20
   households in every group. The `(dist, ea)` row is the instructive one: it is
   the "make `v` a district composite" reflex, and it is *wrong here* — it still
   merges EAs that share a district but sit in different TAs.

2. **2013-14 is dispersal, and the GPS proves it.** 188 of 204 EAs have
   households at different coordinates — the broken-key signature. But restrict
   to households that did not move (`dist_to_IHS3location` ≤ 1 km) and **all 204
   EAs collapse to exactly one coordinate**; `LAT_DD_MOD` is the EA-level
   displaced fix of the household's *current* location, not per-household jitter.
   Median within-EA spread 145 km, max 775 km — every kilometre of it a tracked
   mover. Corroborated: 588/4,000 households report a district different from the
   one encoded in their own `ea_id`, sitting a median **68 km** from their IHS3
   location, against **0.03 km** for the 3,412 that match.

   This is the finding that made the difference between fixing an identifier and
   fabricating one. The brief's premise — "at 75%, the key is broken" — is right
   about 2004-05 and wrong about 2013-14, and the reason is arithmetic: with ~20
   households per EA and 13% of them district-movers, `1 − 0.87²⁰ ≈ 94%` of EAs
   are *expected* to contain at least one mover. The high **cell** rate is a
   property of the metric on a tracked panel, not evidence of a merged key.

3. **The 2016-17 `Region` conflict was a spelling, not a collision.** EA
   `30305580` is in both halves; the CS file writes `Southern`, the panel file
   `South`. Same district, same real EA. Applying the `region` table at
   extraction removes the cell. 1 → 0.

4. **2016-17 had no GPS at all, and it was a merge key.** `df_main` builds
   Cross_Sectional `i` as `cs_i(case_id)`; `df_geo` declared the raw `case_id`,
   so the `how='outer'` join matched nothing — 12,447 orphan geo rows next to
   14,955 coordinate-less main rows, and **0 of 880 clusters with GPS**. The two
   files are exactly 1:1 on `case_id` (12,447 = 12,447, none on either side
   alone), so mirroring the transform makes the join exact: **779 of 880**
   clusters now carry coordinates (the other 101 are IHPS panel EAs, for which
   IHS4 publishes no geovariables). Honest cost: 7 of those 779 EAs have
   households whose `lat_modified` differs within the EA — IHS4's displacement is
   nearly but not exactly EA-constant — so `Latitude`/`Longitude` show 7
   contested cells where before there were none, *because before there was no
   coordinate*.

   This is **not** a Site-4 cardinality fix and 2016-17 is not on PR #627's
   cartesian list. It is a non-matching key, which produces orphans, not a
   product.

5. **The two real cartesians were fixed, measured, and then reverted.** 2010-11
   (196,083 rows from 12,271; 183,812 phantom) and 2019-20 (185,842 from 14,612;
   171,230 phantom) merge a household-grain geo file on the cluster key `v`.
   Collapsing both to `merge_on: [i]` was implemented and measured: contested
   cells **0 and 250 — identical either way**, confirming §4's invariance claim
   by measurement rather than by argument. The change was then reverted at the
   coordinator's direction so PR #627's guard retains these cells as evidence;
   the one-line fix is recorded in each wave's `data_info.yml` next to the
   declaration that causes it, and in `CONTENTS.org`. #627 owns the decision.

6. **`Rural` needed nothing.** The pre-#602 inline `mapping:` blocks were both
   live and inverted (`rural: 0` in one file, `RURAL: 1` in another, in the *same
   wave*), but GH #602 already removed all five on `development`. Verified
   against the pre-collapse frames: `Rural` arrives as the raw label and
   `_enforce_canonical_spellings` normalises it. Nothing added.

## §7 Measured effect (cold build, isolated `LSMS_DATA_DIR`)

Contested cells = clusters where `nunique(dropna=True) > 1` for that attribute,
counted on the frame captured *inside* `_collapse_to_cluster_grain` before it
runs — the only place the evidence still exists.

| wave | before | after | clusters | note |
|------|--------|-------|----------|------|
| 2004-05 | 164 / 330 (49.7%) | **0 / 1,692** | 110 → **564** | Region 66→0, District 79→0, Rural 19→0 |
| 2010-11 | 0 / 3,840 | 0 / 3,840 | 768 | already correct |
| 2013-14 | 765 / 1,020 (75.0%) | 765 / 1,020 | 204 | dispersal — §6.2 |
| 2016-17 | 78 / 4,405 | 89 / 4,400 | 880 | Region 1→**0**; Lat/Lon 1→7 *because GPS now exists*: 0 → **779** clusters with coordinates |
| 2019-20 | 250 / 4,095 (6.1%) | 250 / 4,095 | 819 | dispersal — panel half only |
| **total** | **1,257 / 13,690** | **1,104 / 15,047** | 2,782 → **3,235** | |

(The brief's baseline was 1,255; the 2-cell difference is in 2004-05 and does not
bear on anything. Its 424,607 source rows are 412,160 here — the 12,447 removed
are 2016-17's orphan geo rows, and ~355k of what remains is the known cartesian.)

At the API, what the 2004-05 fix restored: **20 → 26 districts**, and the
settlement strata from `{Rural}` — every one of the 110 mega-clusters came out
rural — back to `{Rural, Urban}`.

Two of the five waves (2004-05, 2010-11) now emit **no `GrainCollapseWarning` at
all**; before, four did.

## §8 Verification

- `sample.v` ≡ `cluster_features.v` in **every** wave: **0** `cluster_features`
  `(t, v)` pairs unknown to `sample`, **0** orphaned `sample` clusters.
- `tests/test_gh323_malawi_cluster_key.py` — 14 tests, all config-level;
  exercises no core aggregation. **Negative control run**: 7 of the 14 fail
  against a pristine `origin/development` config tree (exported with
  `git archive`, cold data dir), including every load-bearing one.
- Malawi-relevant slice (`-k "malawi or Malawi or 323 or schema or sample or
  cluster"`): **680 passed, 58 skipped, 4 xfailed, 1 failed**.
- The 1 failure is **pre-existing and untouched by this branch**:
  `tests/test_gh323_explicit_reducers.py::test_core_does_not_dispatch_the_reducers`
  greps `lsms_library/country.py` for the substring `collapse_to_cluster_grain`
  and finds PR #617's private `_collapse_to_cluster_grain`. This branch is
  config-only and does not touch `country.py`. Same failure is reported on PR
  #634.

## §9 Deferred (deliberately)

- **Site 4 / PR #627**: 2010-11 and 2019-20's cartesian `df_geo` merges. §6.5.
- **The `region`/`district` extraction-time harmonisation is applied only where
  it removed a measured conflict** (2016-17 `Region`). Doing it everywhere is
  output-neutral and tidier, but it is a diff across five waves and two tables
  for zero measured gain, so it is not in this PR.
- **2016-17's 101 panel clusters and 2019-20's 102 have no coordinates**, because
  IHS4/IHS5 publish geovariables for the cross-sectional half only. Pre-existing,
  unrelated to #323, recorded in `CONTENTS.org`.
- **IHPS-2016 mixes baseline and current geography in one file** (`district`
  baseline, `reside` current — 0 vs 277 households off their EA's mode). That is
  the source's inconsistency; documented, not papered over.

---
### Phase 3 — verification

- `Malawi/2004-05/_/data_info.yml` `sample.v` / `cluster_features.v` = `psu` —
  **OK (anchored on §3, §5)**: same key in both, as `_join_v_from_sample`
  requires; verified 0 unknown pairs.
- `Malawi/2016-17/_/data_info.yml` `df_geo.idxvars.i` — **OK (anchored on §2)**:
  reuses the existing `cs_i`, mirroring `df_main`; no new transform.
- `Malawi/2016-17/_/data_info.yml` `Region` `mappings:` — **OK (anchored on §2)**:
  same `mappings: [table, from, to]` form the wave's `strata` already uses; the
  `region` table is unchanged.
- 2013-14 / 2019-20 — **OK (anchored on §5, §6.2)**: no change is the decision,
  and the decision is pinned by a test that fails if the evidence for it changes.
- No `aggregation:` key added, no reducer declared, no value blanked to `<NA>`,
  no file under `lsms_library/*.py` touched — **OK (anchored on §3, D1)**.
