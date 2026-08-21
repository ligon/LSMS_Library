# Prior-Art Ledger — GH #323 (Benin): the residue after PR #615

**Search tier used:** ripgrep + git floor (gitnexus not consulted; the change is
config-scoped to one country/one wave and the framework symbols were read
directly). The decisive prior art was found with
`git log --all --grep=323 -- lsms_library/countries/Benin` and
`git show fc3be203` (CotedIvoire).

> **Companion ledger, not a replacement.** `.coder/ledger/323-benin-togo.md`
> owns Benin's `plot_inputs` defect and its fix, which **merged on 2026-07-19**
> (PR #615, commits `36170a57` + `ef45231e`). This ledger owns what was left
> over: the `cluster_features` extraction grain, the `food_acquired`
> non-defect, and the dead `aggregation:` key. Read that one first for the
> `harmonize_seed_crop` story.

## §1 Task, restated

The task as briefed was "fix GH #323 for Benin", listing two deliverables: the
`cluster_features` extraction hook salvaged from
`rescue/2026-07-21/323-benin`, and an injective `harmonize_seed_crop` for
`plot_inputs`.

**The second was already done.** `origin/development` at `53ef3a5d` already
carries the split (`Semences d'autres céréales → Autre céréale`,
`Plants/boutures de tubercules → Autre tubercule`), the uniqueness assertion in
`Benin/2018-19/_/plot_inputs.py`, the ledger, and `tests/test_gh323_benin_togo.py`
— all merged via PR #615. Re-deriving it would have produced a no-op diff and,
worse, an invented second account of the same numbers. **Verified rather than
assumed**: a cold rebuild returns 10,605 rows with 0 duplicate index tuples
(§Phase 3).

So this task reduces to three things, and they are three *different* things —
conflating them is exactly how #323 survived elsewhere:

| table | dup rows on the declared index | what it is | action |
|---|---|---|---|
| `plot_inputs` | 71 | broken identifier — **real loss** | none — merged in PR #615 |
| `cluster_features` | 7,342 | **wrong extraction grain** — no loss | fix the extraction + enforce the invariant |
| `food_acquired` | 1,382 | **intended bucketing** — no loss | none — core already SUMs; document it |

`cluster_features` declares `(t, v)`, but `df_main` in
`Benin/2018-19/_/data_info.yml` reads `Region`/`Rural` from the **household**-level
cover page `s00_me_ben2018.dta` while declaring only `v: grappe`. Each grappe's
attributes are broadcast across its 8,012 households and handed to
`_normalize_dataframe_index` as 8,012 rows on a 670-cluster grain.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py` (~L4550) | reorders to the declared index; `groupby().first()` when non-unique, `sum` for `_ADDITIVE_MEASURE_COLUMNS`; audits first (#614) | yes | **untouched** (D1: core is not this PR's business) |
| `_audit_index_collapse` | `lsms_library/country.py:4214` | pre-collapse audit; `GrainCollapseWarning` on destruction, **silent when lossless**, stamped into the parquet and replayed warm | yes | reuse as the oracle — its silence on Benin is a *result*, not an absence |
| `_collapse_to_cluster_grain` (Site 2) | `lsms_library/country.py:~4453` | the *other* household→cluster collapse, for countries declaring `i:` in `cluster_features` idxvars | yes | **N/A for Benin** — Benin declares only `v`, so Site 2 never fires here; the collapse lands on Site 1 |
| `_ADDITIVE_MEASURE_COLUMNS` | `lsms_library/country.py` / `feature.py` | the one surviving reduction: `food_acquired` measures are SUMMED | yes | reuse — it is why `food_acquired`'s 1,382 collisions lose nothing |
| df_edit hook dispatch | `lsms_library/country.py:1054` (`dfs:` branch) | a `mapping.py` function named after a declared table runs on the merged, **already-indexed** frame | — | **reuse** — the hook sees `(t, v)`, which is what the salvaged code assumes |
| `Wave.formatting_functions` | `lsms_library/country.py:692` | loads `{wave_folder}.py` + `mapping.py` into the hook namespace | — | reuse (also the test's handle on the hook) |
| CotedIvoire case A | `fix/323-cotedivoire` `fc3be203`, `CotedIvoire/_/cotedivoire.py` | same defect: household-grain source, `v`-only declaration; projects in the extraction | `tests/test_gh323_cotedivoire.py` | **the pattern followed here** |
| `.coder/ledger/323-benin-togo.md` | — | Benin/Togo `plot_inputs` | `tests/test_gh323_benin_togo.py` | **cite, do not re-derive** |

## §3 Definitions & conventions in force

- **D1, "core does not aggregate"** — `slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org`,
  §"Decisions (Ethan, 2026-07-13)"; upheld in `SkunkWorks/grain_aggregation_policy.org`
  §3a and now summarised in `CLAUDE.md` §"Grain Collapse". Consequence for this
  PR: **no file under `lsms_library/*.py` is touched.**
- **`aggregation:` is dead config** — same doc, same section: the key survives
  only in core's "skip these meta-keys" sets, a test pins that the collapse does
  not honour it, and it now contradicts the policy it was written to serve.
  Cited in `CLAUDE.md` §"Grain Collapse".
- **Duplicates on a declared index mean the identifier is broken or a level is
  missing** — `CLAUDE.md` §"Grain Collapse"; the Mali `pid` case is the proof
  that no reducer can be correct in general.
- **EHCVM key convention**: each `grappe` is visited in exactly one `vague`, so
  `v: grappe` and `i: [grappe, menage]` — `CLAUDE.md` §"Gotchas with Teeth",
  `Benin/_/CONTENTS.org` §"Sampling Design".
- **`cluster_features` owns `v`** — `CLAUDE.md` §"`sample()` and Cluster
  Identity"; `Benin/_/data_scheme.yml` declares `index: (t, v)`.

## §4 Invariants & assumptions

- **The collapse hides behind the cache it poisoned.** The L2-country parquet is
  written *post*-collapse, so a warm read shows a clean unique index. Every
  number in this ledger is a cold build: `LSMS_NO_CACHE=1` **plus** an isolated
  `LSMS_DATA_DIR` (with `dvc-cache` symlinked to the shared L1 so no blob is
  re-fetched). `LSMS_NO_CACHE` alone is **not** sufficient — it is *soft* for
  script-path L2-wave parquets, and a stale pre-PR-#615 `plot_inputs.parquet`
  in the shared cache did in fact report 71 duplicates on the first attempt.
- **Config-tree identity.** `LSMS_COUNTRIES_ROOT=<worktree>/lsms_library/countries`,
  plus `PYTHONPATH=<worktree>` and an `assert 'worktrees' in lsms_library.__file__`
  in every measurement script (`CLAUDE.md` scrum-master addendum 3).
- **`groupby().first()` skips NA per column**, so a conflicting group collapses
  to a *composite* row assembled from different source rows — a cluster that
  exists nowhere in the survey. This is why "one household is missing Region"
  must count as a straddle, not as a tolerable gap; a test pins it.
- **`nunique(dropna=False)` counts NaN as a value**, so a cluster whose
  attribute is *uniformly* missing is one distinct value, not two. Also pinned.
- **Benin's `cluster_features` collapse is value-lossless today** (0 of 670
  grappes straddle; GPS is already grappe-level at 670/670) — so the #614 audit
  is correctly silent and **this fix recovers zero rows**. Saying otherwise
  would be a false claim of recovered data.
- **No `dvc` CLI.** All source reads go through `get_dataframe()` /
  `_ensure_dvc_pulled()` (lock-free direct-S3); this PR needs no write path.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| `plot_inputs` injectivity | **reuse (already merged)** | PR #615 landed it on `development`; re-deriving would fork the account of the same numbers |
| `cluster_features` grain | **reuse the CotedIvoire pattern** (project in the extraction) | same defect on a sibling EHCVM country; keeps the family consistent |
| the straddle policy | **new — RAISE**, not CIV's strict-majority-with-warning | CIV *had* a conflict (grappe 648: 11 Rural vs 1 Urbain) and so needed a resolution rule. Benin has **zero**, so a resolution rule would be dead code that quietly licenses a future guess. Raising is the strictly safer no-op. Noted as a deliberate divergence in §6. |
| duplicate-collapse policy | **reuse core as-is** | D1 — core is PR #614's; a country may not patch it |
| `food_acquired` collisions | **reuse `_ADDITIVE_MEASURE_COLUMNS`** | already lossless (totals verified identical); the only deliverable is prose so the next reader does not mistake a sum for a drop |
| `aggregation: {visit: first}` on `interview_date` | **delete** | dead config that contradicts D1, and nonsense on its own terms (`visit` is an index *level*, not a measure) |

## §6 Decisions worth flagging

1. **The hook RAISES; CotedIvoire's warns and takes a strict majority.** A
   deliberate divergence, argued in §5. If Benin ever acquires a straddling
   grappe the build stops with the offending clusters named — which is the
   outcome we want, because we would then have to decide *what the right answer
   is*, and the framework must not decide it for us in the meantime.
2. **`food_acquired` was left alone.** It is tempting to "fix" 1,382 colliding
   rows by splitting the residual food buckets the way `plot_inputs`' seed
   bucket was split. That would be wrong: `Autres poissons fumés` is the
   harmonized taxonomy's *own* residual category, the pooling is intended, and
   `u` and `s` are both in the index so the sum is commensurable. The two cases
   look identical at the level of "duplicates on a declared index" and are
   opposite at the level of what the data means. Hence the table in §1.
3. **A pre-existing `aggregation:` key was deleted, slightly widening the diff.**
   It was in the file this PR edits, it is dead, and leaving it would have left
   a Benin-shaped counterexample to the very policy the rest of the diff
   documents.

## §7 Open questions for the human

- None blocking. One observation for whoever owns Site 4: Benin's
  `cluster_features` uses a `dfs:` **outer** merge (`df_main` ⋈ `df_geo` on
  `v`) with no cardinality guard — the Site-4 pattern. Here it is benign
  (`df_geo` is 670/670 unique on `v`, so the merge cannot fan out) and the
  hook now runs downstream of it regardless, but it is the same construct.

---
### Phase 3 — verification

Cold builds, `LSMS_NO_CACHE=1` + isolated `LSMS_DATA_DIR` (L1 shared via a
`dvc-cache` symlink), `LSMS_COUNTRIES_ROOT` + `PYTHONPATH` pinned to the
worktree and asserted in-process.

| measure | before | after |
|---|---|---|
| `cluster_features` wave-frame rows | 8,012 | **670** |
| `cluster_features` duplicate `(t, v)` tuples | 7,342 | **0** |
| `cluster_features` API rows | 670 | 670 (frames byte-identical) |
| `food_acquired` wave rows → API rows | 190,618 → 189,236 | unchanged; `Quantity` 576,293.81 and `Expenditure` 87,287,926.96 identical on both tiers |
| `plot_inputs` wave rows / dups / API rows | 10,605 / 0 / 10,605 | unchanged (PR #615 already landed) |

**Regression re-verification of PR #615** (cold, counterfactually re-widening
the split buckets back to one `Autre crop`): the landed fix holds — 10,605 API
rows, 0 duplicate index tuples. The counterfactual destroys **71** rows across
**64** conflicting groups, carrying **11,445.25** units of reported `Quantity`
and flipping **4** households' `Purchased` True→False. The issue text's **68**
is the same defect counted conservatively: it drops the 3 pairs whose *measured*
payload is coincidentally equal, where `first()` loses no measurement — but
those two rows still name two *different* crops, so 71 is the honest figure.
Both accountings go to 0.
| `GrainCollapseWarning`s across all 21 Benin tables | 0 | **0** |

- `Benin/2018-19/_/mapping.py::cluster_features` — **OK (anchored on §2, §5)**:
  it is the CotedIvoire case-A pattern (project in the extraction), running in
  the documented df_edit slot, with the straddle policy divergence recorded in
  §6.1.
- `Benin/_/data_scheme.yml` + `Benin/_/CONTENTS.org` prose — **OK (anchored on
  §3)**: documents the three grain cases and cites the policy; declares no
  `aggregation:` key and says why.
- Deletion of `interview_date`'s `aggregation:` — **OK (anchored on §3, §5)**:
  the key is dead by the cited authority; `interview_date` returns 24,035 rows
  with zero grain reports either way.
- No `lsms_library/*.py` in the diff — **OK (anchored on §3, D1)**.
- `tests/test_gh323_benin_cluster_features.py` — **OK (anchored on §4)**: 6 of
  its 8 tests fail/error on the pre-fix tree; the 2 that pass are the ones that
  *should* (the invariant holds in the raw data, and the API row count was
  already right) — which is the honest instrument, per the CotedIvoire note
  that a post-collapse-uniqueness assertion passes with the bug fully present.
- **No REINVENTION found** against `.coder/ledger/323-benin-togo.md`: this PR
  contains no `harmonize_seed_crop` change. That ledger's account of
  `plot_inputs` stands unmodified and is cited, not restated.
