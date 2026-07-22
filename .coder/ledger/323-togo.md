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

So the wave frame carried **5,631 EXCESS rows** for the framework to reduce.
(Precisely: `index.duplicated(keep=False).sum()` is **6,171** — every row shares
its `(t, v)` with at least one other, since the smallest grappe holds 3
households and the largest 12 — and `duplicated(keep='first').sum()` is
**5,631**. The PR body's original "5,631 rows on a duplicated tuple" was the
second number under the first number's name; corrected 2026-07-22.)

Every one of those duplicates came from `df_main`; `df_geo` is already at
cluster grain.

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
| `reduce_to_agreed` | `lsms_library/build_transforms.py:422` (re-exported from `lsms_library.transformations`) | **the shipped country-facing reducer** — collapses rows sharing an index tuple, keeping only values they AGREE on; raises `GrainConflict` (naming the offending groups) otherwise; NaN is ABSENCE by default | `tests/test_gh323_explicit_reducers.py`, `tests/test_gh323_grain_contract.py` | **reuse** |
| `collapse_to_cluster_grain` | `lsms_library/build_transforms.py:514` | the named `cluster_features` case of the above; **its docstring prescribes this exact call site** (`from lsms_library.transformations import collapse_to_cluster_grain as cluster_features`) | `…::test_collapse_to_cluster_grain_works_as_a_bare_df_edit_hook` | **reuse — aliased directly; NOT re-implemented** |
| `cotedivoire.cluster_features` | `CotedIvoire/_/cotedivoire.py` (`fc3be203`) | same EHCVM cover-page defect; projects to cluster grain **in the extraction** | `tests/test_gh323_cotedivoire.py` | **the diagnosis followed here** — but it *predates* the helper above and hand-rolls its own resolution; do not copy its code |
| `Togo/2018/_/mapping.py` | wave module | already hosts `shocks`, `food_security`, `household_roster` df_edit hooks, and already alias-imports `food_acquired_to_canonical as food_acquired` | — | **extend** — alias-import `collapse_to_cluster_grain as cluster_features` |

**Prior art is decisive, in two layers.** *Diagnostically*: CotedIvoire 2018-19
is EHCVM and has the *identical* cell reading the *identical* file
(`Menage/s00_me_CIV2018.dta`, 12,992 households into a 1,084-cluster table). Its
fix projects in the extraction and resolves conflicts loudly. Togo's is the same
diagnosis, one wave smaller and one degree simpler: CIV needed strict-majority
conflict resolution because grappe 648 genuinely disagreed; **Togo has no
disagreeing grappe**, so the default `on_conflict='raise'` costs nothing and is
the stronger contract. *Mechanically*: the CIV fix was **generalised into a
public helper after CIV landed** — `collapse_to_cluster_grain` (PR #618,
`48f4d08f`), merged on `development` before this branch was cut — so the correct
move is to alias that helper, not to re-derive CIV's projection. See §7: the
first draft of this PR got the diagnosis right and this half wrong.

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

- **Region and Rural are constant within a grappe.** Measured: 0/540 violations
  (and 0/540 for `Latitude`/`Longitude` too). This is the invariant `.first()`
  was silently *assuming*; `collapse_to_cluster_grain` **checks** it on every
  build and raises `GrainConflict` if it ever fails. Pinned by
  `test_region_and_rural_are_constant_within_a_grappe`.
- **NaN is ABSENCE, not contradiction.** A grappe where one household reports
  `Maritime` and another reports nothing collapses to `Maritime`: no observed
  value is discarded, so the completion is lossless. This is repo doctrine
  (`tests/test_gh323_grain_contract.py::test_p2_complementary_missingness_is_
  COMPLETION_not_fabrication`, `tests/test_gh323_explicit_reducers.py::
  test_nan_is_absence_not_contradiction`) and it is the default
  (`na_is_conflict=False`) of the shipped reducer. It is **not** what the first
  draft of this file implemented — see §7.
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
| the projection | **reuse the shipped helper**, alias-imported as the `cluster_features` df_edit hook in the *wave* module (Togo has one wave and the defect is wave-scoped) | `collapse_to_cluster_grain` (PR #618) exists precisely for this cell and its docstring names this call site. **Corrected in round 2**: this row originally read "new, but on the CIV pattern" and a 40-line copy was written. See §7. |
| conflict handling | **RAISE** — the reducer's default `on_conflict='raise'` (CIV resolves by strict majority) | Togo has 0 conflicting grappes, so raising costs nothing today and is a stronger contract tomorrow. Majority-resolution would be inventing a policy for a case that does not exist. |
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

- `cluster_features` hook (`Togo/2018/_/mapping.py`) — **OK (anchored on §2, §5)**: a one-line alias of `lsms_library.transformations.collapse_to_cluster_grain`, dispatched by the framework's existing table-name `df_edit` mechanism (`country.py:802`/`:1054`); no new machinery, no private copy.
- **raises rather than reduces** — **OK (§4)**: negative-tested with a synthetic conflicting frame; `GrainConflict` names the offending groups *and* the per-column conflict counts.
- **no `aggregation:` key, no reducer, no index level added** — **OK (§3)**: D1 honoured.
- **no `lsms_library/*.py` touched** — **OK (§4)**: `git diff --stat` confirms the change is confined to `lsms_library/countries/Togo/**`, `.coder/ledger/`, and one new test.
- **REINVENTION check** — **this is where round 1 failed, and it is recorded rather than quietly patched.** The round-1 ledger asserted the only two reinvention candidates were (a) CIV's cluster-grain projection and (b) the rejected Design-A reducer, and concluded the check was clean. It **missed (c)**: `collapse_to_cluster_grain` / `reduce_to_agreed`, merged on `development` in PR #618 *after* CIV, in the same sweep this PR belongs to. Round 1 therefore hand-rolled a 40-line copy of a shipped, tested helper whose docstring names this exact call site. Round 2 replaces it with the alias. Search-tier lesson: the round-1 search was a `git log`/`git show` walk over *sibling country fixes*; it never grepped `lsms_library/` for the shipped helper. **A prior-art search that only looks at peer call sites cannot find a helper that was extracted from them.**

**Result (2026-07-21, `LSMS_NO_CACHE=1`, `LSMS_COUNTRIES_ROOT` pinned to the worktree):**

| observable | before | after |
|---|---|---|
| `Wave.grab_data('cluster_features')` rows | 6,171 | **540** |
| EXCESS rows (`duplicated(keep='first')`) | 5,631 | **0** |
| rows sharing a `(t, v)` (`keep=False`) | 6,171 | **0** |
| clusters whose duplicate rows disagree | 0 | 0 (n/a) |
| `Country('Togo').cluster_features()` rows | 540 | 540 |
| API frame values | — | **identical** (`assert_frame_equal` passes) |
| `GrainCollapseWarning`s for this cell | 0 | 0 |

Rows of value: **none lost, none recovered** — by design. What changed is that
the grain is now correct by construction and the invariant is enforced instead of
assumed. Tests (round 2, six tests): **4 of 6 FAIL on pristine `development`**
(`…wave_frame_is_already_at_cluster_grain`, `…hook_is_the_shipped_reducer_not_a_
private_copy`, `…projection_hook_raises_rather_than_picking_a_row`,
`…nan_is_absence_not_contradiction`); all 6 pass after. The other two
(`…api_still_returns_every_cluster`, `…region_and_rural_are_constant_within_a_
grappe`) pass **both** with and without the change by design — they pin an
invariant and a source fact, not the fix — and each says so in its own
docstring. Against the *round-1* (hand-rolled) tree, 3 of the 6 fail: the two
that pin the alias and its NaN reading, plus the retargeted `GrainConflict`
assertion.

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

---
## §7 Round 2 — what the adversarial review found, and what I re-derived

PR #632 was reviewed and returned **FIX_FIRST**. The review confirmed every
headline number and confirmed the diagnosis; what it broke was the *reuse*
claim. Everything below was re-measured independently before being written down
(isolated `LSMS_DATA_DIR`, `dvc-cache` symlinked to the shared blob store,
`LSMS_COUNTRIES_ROOT` + `PYTHONPATH` pinned to the worktree with an in-process
`assert 'wt632' in lsms_library.__file__`; negative control by reverting
`mapping.py` to `origin/development`, **not** by stashing).

### Accepted — Finding 1: the hook re-implemented merged machinery

`collapse_to_cluster_grain` (`lsms_library/build_transforms.py:514`, PR #618,
`48f4d08f`, re-exported from `lsms_library.transformations`) is the shipped
helper for exactly this defect, and its docstring shows this exact call site.
Round 1 hand-rolled a 40-line equivalent. Swapped for the one-line alias and
rebuilt cold: the wave frame **and** the API frame are `assert_frame_equal`-
identical (540 rows, same values, same order, same dtypes) to the hand-rolled
build. The copy bought nothing and could only diverge. Fixed; §2/§5 corrected
above rather than silently rewritten.

### Accepted — Finding 2: the copy read `NaN` as contradiction

The copy's guard was `nunique(dropna=False).gt(1)`, i.e. `na_is_conflict=True`
hard-coded with no opt-out. Re-derived on a two-row frame
(`Region=['Maritime', NaN]`, `Latitude=[NaN, 6.1]` in one grappe):

| | result |
|---|---|
| round-1 hand-rolled hook | `ValueError: column(s) ['Region', 'Latitude'] vary WITHIN a cluster` — **build dies** |
| `collapse_to_cluster_grain` (shipped) | `{'Region': 'Maritime', 'Latitude': 6.1}` |

That contradicts §4's NaN doctrine, and it accused `Latitude` — a column that is
pure absence. **Latent, not live**: `s00q01`/`s00q04` are 0% null and the GPS
NaNs are constant within grappe (5 grappes, 56 household rows in the
pre-projection frame). But Togo's own geo source is the shape that would trip
it. Subsumed by the Finding-1 fix; now pinned by
`test_nan_is_absence_not_contradiction`, which fails against the round-1 tree.

### Accepted — Finding 3: weaker diagnostics

The copy raised a bare `ValueError` naming only the columns. `reduce_to_agreed`
raises `GrainConflict` (a `ValueError` subclass, so `except ValueError` callers
are unaffected) naming the offending **groups** and per-column counts, and
carries the `on_conflict='na'` escape hatch and `LSMS_GRAIN_STRICT` interaction.
Subsumed. `test_projection_hook_raises_rather_than_picking_a_row` retargeted
from `match='vary WITHIN a cluster'` to `GrainConflict` / `match='grain
conflict'`.

### Also fixed, not raised by the review

- **The tests' fixtures swallowed every exception into `pytest.skip`.** A broken
  build would have produced a green run — the end-to-end tests were vacuous
  exactly when they mattered. Removed; the module now carries `requires_s3` from
  `tests/conftest.py`, and `conftest`'s `pytest_runtest_makereport` hook converts
  a missing-credentials failure (and only that) into a skip.
- **"5,631 rows on a duplicated `(t, v)` tuple" was the wrong name for a right
  number.** `duplicated(keep=False).sum()` is **6,171** (no grappe holds fewer
  than 3 households, so every row shares its tuple); **5,631** is
  `duplicated(keep='first').sum()`, the EXCESS. Corrected in §1, the results
  table, `CONTENTS.org`, `data_scheme.yml` and `mapping.py`.
- **Two of the four round-1 tests passed with and without the fix** and the PR
  said so only in its body. Each now says so in its own docstring, naming the
  tests that *do* discriminate.

### Disputed — nothing

No finding was over-claimed. Two review remarks are worth keeping as
non-findings: the `household_roster` row-order difference the reviewer chased
was correctly attributed to its own harness, not to this PR; and the
"zero duplicates by splitting real entities" failure mode is genuinely ruled out
here — re-derived independently: households per grappe run 3–12 with **394 of
540 grappes at exactly 12** (the EHCVM design cell), `grappe` is nationally
unique (`drop_duplicates(['s00q01','grappe'])` = 540 = `nunique(grappe)`), and
`(grappe, menage)` has 0 duplicates.

### Still deferred (unchanged)

`Togo/_/data_scheme.yml`'s `interview_date` still carries a dead
`aggregation: {visit: first}` key from `717e32f4`; removing it is a six-country
change and stays out of scope. Separately noted for someone else:
`tests/conftest.py`'s docstring advises `from conftest import
aws_creds_available`, which **raises `ImportError`** — `tests/` is a package and
a project-level `conftest.py` shadows the name. The working import is
`from tests.conftest import ...`, which this module uses; the docstring is not
edited here.
