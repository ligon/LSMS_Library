# Prior-Art Ledger — GH #323 (Togo, `cluster_features`)

**Search tier used:** ripgrep + git floor. The decisive prior art was found by
`git show fc3be203` (`fix/323-cotedivoire`), `git show 36170a57` (the landed
Benin/Togo `plot_inputs` fix) and `git show origin/docs/323-grain-collapse-sites:slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org`.

> **Supersedes an earlier draft.** A rescued WIP snapshot
> (`rescue/2026-07-21/323-togo`) carried a ledger arguing for **Design A** — teach
> the core to read an `aggregation:` key from `data_scheme.yml` and reduce with a
> declared reducer, plus a `LSMS_CACHE_SCHEMA` bump and a
> `tests/test_grain_aggregation.py` ratchet. **Design A was rejected** (Ethan,
> 2026-07-13, decision D1). That draft's §5 "make the `aggregation:` block real"
> row is wrong and is not carried forward; its §1 *diagnosis* is, corrected.
> `aggregation:` remains dead config and this PR adds none.

## §1 Task, restated

Togo trips the framework's `groupby().first()` grain collapse in two places. The
first — **`plot_inputs`**, 165 destroyed rows from a non-injective
`harmonize_seed_crop` — was fixed and landed on `development` in `36170a57`, and
has its own ledger at `.coder/ledger/323-benin-togo.md`. **This ledger covers the
second: `cluster_features`.**

`cluster_features` is declared `index: (t, v)` — one row per grappe. Its
`df_main` source is the EHCVM **household** cover page
`2018/Data1/s00_me_tgo2018.dta`. Measured (`LSMS_NO_CACHE=1`, cold):

| frame | rows | distinct `(t, v)` |
|---|---|---|
| `s00_me_tgo2018.dta` (df_main) | 6,171 households | 540 grappes |
| `grappe_gps_tgo2018.dta` (df_geo) | 540 | 540 |
| `Wave.grab_data('cluster_features')` **pre-fix** | **6,171** | 540 |
| `Country('Togo').cluster_features()` | 540 | 540 |

So the wave frame carried **5,631 rows on a duplicated `(t, v)` tuple** for the
framework to reduce. Every one of those duplicates came from `df_main`; `df_geo`
is already at cluster grain.

**This destroys no VALUE.** All 540 grappes carry exactly one distinct Region
(`s00q01`) and one distinct Rural (`s00q04`) — 0/540 violations, measured from
the source, not assumed. So `.first()` landed on the right answer, and post-PR
#614 the core is (correctly) **silent** about a lossless collapse: zero
`GrainCollapseWarning`s fire for this cell on `development`.

> The rescued draft claimed this cell "fired a spurious #323 warning that
> camouflaged the real one from `plot_inputs`". That was true against the
> **pre-#614** core, whose guard was `not df.index.is_unique` and could not tell
> a lossless de-dup from a destructive one. It is **not** true of `development`
> today. Corrected here rather than repeated.

## §2 Existing machinery (this task's area)

| symbol | path | what it does | tested? | reuse / extend / new |
|--------|------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py` | reorders to the declared index; `groupby().first()` when non-unique (Site 1) | PR #614 | **untouched** |
| `_audit_index_collapse` | `lsms_library/country.py` | audits BEFORE the collapse; warns only on *destruction*; stamps the finding into the parquet and replays it on warm reads | PR #614 | **untouched** — and it is *why* nothing warns here |
| `_collapse_to_cluster_grain` | `lsms_library/country.py` | Site 2: projects a household-grain `cluster_features` onto `(t, v)`, audited | PR (Site 2) | **not reached** — fires only when `i` is in `df.index.names`; Togo's YAML declares `v: grappe` only |
| `Wave.column_mapping` → `final_mapping['df_edit']` | `lsms_library/country.py:802`, applied at `:1054` for the `dfs:` path | binds a `mapping.py` / `{country}.py` function whose **name matches the table** as a whole-frame post-extraction hook | — | **reuse** — this is the extension point |
| `cotedivoire.cluster_features` | `CotedIvoire/_/cotedivoire.py` (`fc3be203`) | same EHCVM cover-page defect; projects to cluster grain **in the extraction** | `tests/test_gh323_cotedivoire.py` | **the pattern followed here** |
| `Togo/2018/_/mapping.py` | wave module | already hosts `shocks`, `food_security`, `household_roster` df_edit hooks | — | **extend** — add `cluster_features` |

**Prior art is decisive.** CotedIvoire 2018-19 is EHCVM and has the *identical*
cell reading the *identical* file (`Menage/s00_me_CIV2018.dta`, 12,992 households
into a 1,084-cluster table). Its fix projects in the extraction and resolves
conflicts loudly. Togo's is the same fix, one wave smaller and one degree
simpler: CIV needed strict-majority conflict resolution because grappe 648
genuinely disagreed; **Togo has no disagreeing grappe**, so it raises instead of
resolving. Raising is the stronger contract and is available precisely because
the data supports it.

## §3 Definitions & conventions in force

- **The #323 doctrine (D1).** Duplicates on a declared index mean **the
  identifier is broken or a level is missing** — *never* that a reducer should be
  declared. Core does not aggregate. See
  `slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org` and CLAUDE.md
  §"Grain Collapse". `aggregation:` in `data_scheme.yml` is **dead config**; a
  test pins that the core does not honour it.
- **A manufactured duplicate is fixed by not manufacturing it.** The collapse is
  not the bug here; feeding a 540-row table 6,171 rows is.
- `cluster_features` **owns `v`** and is the only feature allowed to declare it
  (CLAUDE.md §"`sample()` and Cluster Identity").
- EHCVM index conventions: `v: grappe`, `i: [grappe, menage]` — CLAUDE.md
  §"Gotchas with Teeth". Togo has one wave, `2018`.
- The v0.8.0 cache hash covers `mapping.py`, so adding this hook invalidates the
  L2-wave and L2-country parquets automatically; no `LSMS_CACHE_SCHEMA` bump is
  needed (the rescued draft's bump was for a *library-wide* reducer change that
  no longer exists).
- Everything else: `STANDING.md` §3.

## §4 Invariants & assumptions

- **Region and Rural are constant within a grappe.** Measured: 0/540 violations.
  This is the invariant `.first()` was silently *assuming*; the hook **checks**
  it on every build and `raise`s if it ever fails. Pinned by
  `test_region_and_rural_are_constant_within_a_grappe`.
- **"Lossless" is not the same as "safe".** The reason to fix a value-lossless
  collapse is that `.first()` on a cluster that stopped being constant returns a
  silently **wrong** Region — not a missing one. That is the Kazakhstan 1996
  `v=126` failure (178 Urban + 2 Rural households → the whole cluster reported
  `Rural`) and the CotedIvoire grappe-648 failure. Both were licensed by the same
  prose — *"invariant within a cluster by construction of the LSMS-ISA sampling
  design"* — and prose is not enforcement.
- **`groupby().first()` skips NA per column**, so a conflicting group collapses
  to a *composite* row assembled from each column's first non-null value — a
  cluster that exists nowhere in the survey. Another reason the guard raises
  rather than reduces.
- **Do NOT assert API-index uniqueness as the regression test.** The collapse
  makes the API index unique *by construction*, so such a test passes with the
  bug fully present (9 of 10 first-draft CIV tests did). The observable is the
  **pre-collapse wave frame**. Inherited instrument note; honoured in
  `tests/test_gh323_togo_cluster_features.py`.
- **`lsms_library/*.py` is off-limits.** Sites 1–2 are owned centrally (PR #614 +
  the Site-2 PR). This change is config only.
- Repo-wide landmines: `STANDING.md` §4.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| the collapse itself | **reuse, untouched** | owned centrally; a per-country core patch is the exact failure the consolidation doc exists to stop (17 rival patches to one file) |
| the projection | **new, but on the CIV pattern** — a `cluster_features(df)` df_edit hook | CIV's `cotedivoire.cluster_features` is the same fix for the same file; Togo's lives in the *wave* module because Togo has one wave and the defect is wave-scoped |
| conflict handling | **RAISE** (CIV resolves by strict majority) | Togo has 0 conflicting grappes, so raising costs nothing today and is a stronger contract tomorrow. Majority-resolution would be inventing a policy for a case that does not exist. |
| declaring `aggregation:` for `cluster_features` | **rejected** | dead config; contradicts D1 (§3) |
| adding `i` to the `cluster_features` index | **rejected** | `cluster_features` is a `(t, v)` table by definition; adding `i` would make it `sample()` with extra steps |
| `LSMS_CACHE_SCHEMA` bump | **rejected** | the v0.8.0 content hash already covers `mapping.py` (§3); a manual lever is for library-wide pre-write changes, which this is not |
| a `tests/test_grain_aggregation.py` ratchet | **rejected** | it tested the Design-A feature, which will not exist |

## §6 Open questions for the human

- **The household-cover-page-as-cluster-source shape is a corpus-wide pattern,
  not a Togo quirk.** CIV had it in all 5 waves; the Site-2 measurement found 17
  countries declaring `i:` in `cluster_features` idxvars, 15 of 30 household-grain
  cells destroying rows. Togo's variant (no `i` at all, so Site 2's audit is
  structurally blind and only Site 1 sees it) is a *third* shape. Fixed here for
  Togo only; the sweep is somebody's separate PR.
- **`interview_date` in `Togo/_/data_scheme.yml` still carries an
  `aggregation: {visit: first}` key.** It is dead config and contradicts D1. It
  came from `717e32f4` (`feat(#438)`, six EHCVM countries at once), **not** from
  the #323 sweep, so removing it is a six-country change and is deliberately out
  of scope here. Flagging it so it is not mistaken for something this PR endorses.

---
### Phase 3 — verification

- `cluster_features(df)` hook (`Togo/2018/_/mapping.py`) — **OK (anchored on §2, §5)**: reuses the framework's existing table-name df_edit dispatch (`country.py:802`/`:1054`); no new machinery. Follows CIV's extraction-side projection rather than re-deriving one.
- **raises rather than reduces** — **OK (§4)**: negative-tested with a synthetic conflicting frame; `ValueError` names the offending columns.
- **no `aggregation:` key, no reducer, no index level added** — **OK (§3)**: D1 honoured.
- **no `lsms_library/*.py` touched** — **OK (§4)**: `git diff --stat` confirms the change is confined to `lsms_library/countries/Togo/**`, `.coder/ledger/`, and one new test.
- **REINVENTION check** — the two things this task could have reinvented were (a) CIV's cluster-grain projection and (b) the rejected Design-A reducer. (a) was found first and followed; (b) was found in the rescued draft and *discarded*, which is the more useful half of this ledger.

**Result (2026-07-21, `LSMS_NO_CACHE=1`, `LSMS_COUNTRIES_ROOT` pinned to the worktree):**

| observable | before | after |
|---|---|---|
| `Wave.grab_data('cluster_features')` rows | 6,171 | **540** |
| rows on a duplicated `(t, v)` tuple | 5,631 | **0** |
| clusters whose duplicate rows disagree | 0 | 0 (n/a) |
| `Country('Togo').cluster_features()` rows | 540 | 540 |
| API frame values | — | **identical** (`assert_frame_equal` passes) |
| `GrainCollapseWarning`s for this cell | 0 | 0 |

Rows of value: **none lost, none recovered** — by design. What changed is that
the grain is now correct by construction and the invariant is enforced instead of
assumed. Tests: 2 of 4 new tests FAIL on pristine `development`, all 4 pass after.

**`plot_inputs`, for the record** (landed in `36170a57`, re-verified here against
the raw `s16b` roster with the counterfactual pre-fix map):

| observable | old (shared `Autre crop`) | current (injective) |
|---|---|---|
| wave rows | 13,733 | 13,733 |
| conflicting groups | 162 | **0** |
| rows destroyed by `first()` | **165** | **0** |
| groups repeating one raw `s16bq01` label | 0 / 162 | n/a |

The issue text's "156" is the same defect counted more conservatively (excluding
the 9 groups whose duplicate rows carry identical values). Both accountings go
to 0.
