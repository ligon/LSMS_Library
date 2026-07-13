# Prior-Art Ledger — GH #323 (Tajikistan `cluster_features`)

**Search tier used:** ripgrep + git floor. GitNexus MCP tools were NOT available in
this environment (`ToolSearch` returned no `gitnexus_*` tools), so the CLAUDE.md
"run `gitnexus_impact` before editing a symbol" rule was satisfied by an explicit
call-graph grep instead — recorded in §2. Flagging this rather than silently
skipping the mandated step.

## §1 Task, restated

`Country('Tajikistan').cluster_features()` returns 767 rows while the underlying
L2-wave parquets hold 2000 / 4160 / 4644 / 1503 household rows. The task was to
determine whether ~11.5k rows are being silently DESTROYED (as in Mali's roster,
where 86% of people vanish) or losslessly DEDUPLICATED, and to make whichever is
true be *enforced* rather than assumed.

Answer: **Tajikistan is a FALSE POSITIVE for data loss and a TRUE POSITIVE for
"undeclared silent collapse."** `rows_recovered = 0`. All four waves read
`cluster_features` from the HOUSEHOLD cover file with a superfluous `i: <hhid>`
in `idxvars`, so the extraction lands at household grain for a table whose
canonical grain is the cluster `(t, v)`. The "duplicates" are exactly the
two-stage sampling take.

## §2 Existing machinery

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave.cluster_features` | `lsms_library/country.py:1168` | **THE ACTUAL COLLAPSE SITE.** Unguarded `groupby(keep_levels).agg({'first', GPS:'mean'})` dropping `i` | no | **extend (guarded)** |
| `_normalize_dataframe_index` | `country.py:~4100` | generic index reduce; GH #323 `RuntimeWarning` on `groupby().first()` | yes | extend (`aggregation:`) |
| `_declared_index_levels` | `country.py:3723` | parses `index: (t, v)` | yes | reuse |
| `Country._materialization_entry` | `country.py:1335` | raw `data_scheme.yml` entry (incl. `aggregation`) | yes | reuse |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py` | vetted sum-allowlist for `food_acquired` | yes | reuse (untouched) |
| `aggregation:` key | 9 countries' `data_scheme.yml` | **INERT** — see §4 | n/a | **make it enforcing** |

Call-graph impact (grep, in lieu of `gitnexus_impact`): `_normalize_dataframe_index`
has exactly **4 call sites**, all inside `country.py` (2115, 2715, 2929, 2983), all
passing `scheme_entry`. It runs for *every table of every country*, so the blast
radius is repo-wide — which is why the new branch is strictly additive (fires only
when `aggregation:` names an ELIMINATED level). Verified byte-identical output on
all 17 affected countries (§Phase 3).

## §3 Definitions & conventions in force

- `cluster_features` canonical index is `(t, v)` — `lsms_library/data_info.yml`
  (`Index Info > index_info`); also `CLAUDE.md` ("`cluster_features` owns `v`").
- `sample` is the single source of truth for the household→cluster map —
  `CLAUDE.md` §"`sample()` and Cluster Identity". Tajikistan's `sample` block
  carries `v` per household in all four waves, so `cluster_features` has no
  business re-deriving it.
- "NO AGGREGATION IN CORE" contract — `SkunkWorks/grain_aggregation_policy.org`.
  The composition path must not *implicitly* reduce grain. This fix does not add
  a reduction; it makes an existing, hidden one explicit and checked.
- class-1 (silently WRONG) vs class-2 (silently MISSING): when in doubt, fail
  loudly rather than guess — per the task standard.

## §4 Invariants & assumptions

- **PSU-constancy (the load-bearing invariant).** `Region` / `Rural` are
  PSU-level attributes replicated across the households of a cluster. Verified on
  the RAW NUMERIC CODES in the source `.dta` (so no code→label mapping collision
  can hide a conflict): **0 of all 767 cluster-waves** carry more than one
  distinct `(Region, Rural)`. Also 0 NaN in `hhid` and in the PSU var, which
  rules out PHANTOM_NAN_ROWS. The ambiguous set is EMPTY, so `.first()` provably
  picks the only value there is.
- **`aggregation:` was INERT.** Declared in Senegal, Niger, Togo, Benin,
  CotedIvoire, Guinea-Bissau, Burkina_Faso, Malawi, Albania — but a repo-wide grep
  for it in `*.py` returned only two hits (`country.py:2387`,
  `diagnostics.py:230`), both `_skip = {...}` sets that merely exclude it from
  COLUMN parsing. **Nothing read it as a policy.** So "just declare `aggregation:`"
  was, before this change, a NO-OP. Prose is not enforcement — and neither was
  this key.
- All nine pre-existing declarations are `visit: first` where `visit` is IN the
  declared index (e.g. `interview_date`, `(t, i, visit)`). Such a level is never
  eliminated, so `_declared_reducer` returns `None` for them and they stay
  no-ops. This is what makes the change safe for those nine countries.
- **GPS must be exempt.** `Latitude`/`Longitude` are household-level BY DESIGN and
  are averaged to a cluster centroid (`country.py:1177`), so they are *expected*
  to vary within a cluster. Checking them would false-positive every GPS country.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| household→cluster map | **reuse** `sample()` | already the single source of truth (§3); do NOT re-derive |
| collapse of `i` in `cluster_features` | **extend** `Wave.cluster_features` | that is where the collapse actually happens; the guard belongs at the collapse |
| policy vocabulary | **extend** `aggregation:` | the key already exists and is declared in 9 countries; make it read rather than invent a second mechanism |
| `unique` reducer | **new** | no existing reducer *enforces* losslessness; `first`/`sum` both decide silently |
| `sum` reducer | **NOT added** | `food_acquired` already sums via the vetted `_ADDITIVE_MEASURE_COLUMNS` allowlist; a generic `sum` would happily add region codes and sampling weights |

### Why `unique` and not `first`

`first` is provably correct for Tajikistan *today* (0/767 conflicting clusters), so
`unique` is a guaranteed no-op on the data. That is precisely the point: it converts
an unchecked assumption into an invariant **re-verified on every build**. If a future
wave, a recode, or a PSU straddling two oblasts ever breaks PSU-constancy, `unique`
FAILS LOUDLY instead of quietly keeping whichever household sorted first. The #323
warning goes quiet for Tajikistan for a *reason that is checked*, rather than because
a poisoned cache stopped asking.

### Why `i` STAYS in `idxvars` (a deliberate departure from the dispatch brief)

The brief's Part 1 proposed dropping `i: hhid` from all four wave `data_info.yml`s and
deduping at the wave level. I did **not** do that, for a reason worth recording: `i` is
what *names* the level being eliminated. Remove it and the row multiplicity becomes
**anonymous** — a bare 1875-row duplicate blob over `(t, v)` with no level to attach a
policy to, i.e. exactly the undeclarable shape of the bug. Keeping `i` is what makes the
collapse expressible as `aggregation: {i: unique}` and therefore enforceable. Dropping it
would also have removed the enforcement point while leaving the class untouched.

## §6 Open questions for the human

- **The class is much wider than Tajikistan.** 17 countries carry household grain in
  `cluster_features`; on a cold build the "invariant by construction" comment is
  actually **FALSE in ten of them** — Albania (18 clusters), Guatemala (8), Guyana
  (23), Kazakhstan (2), Liberia, Malawi, Niger, Pakistan, Tanzania, Uganda. This
  change makes all of them WARN (data unchanged). Each needs its own triage: is the
  conflict a genuine multi-region PSU, a `v` that is not actually a cluster id, or a
  bad mapping? Filed as follow-up, not silently "fixed" — guessing there is exactly
  the class-1 error this issue is about.

---
### Phase 3 — verification

- `Wave.cluster_features` — **OK (anchored on §2, §4)**: the collapse is now checked;
  GPS exempted per §4; data byte-identical on all 17 affected countries.
- `_declared_reducer` / `_apply_declared_reducer` — **OK (anchored on §4)**: reducer is
  consulted only for ELIMINATED levels, so the nine `visit: first` declarations remain
  no-ops (pinned by `test_reducer_ignored_for_a_level_that_survives_in_the_index`).
- `_nonconstant_groups` / `_assert_constant_within_groups` — **OK (anchored on §5)**: not a
  REINVENTION of `_ADDITIVE_MEASURE_COLUMNS` — that path *sums* a vetted column list for
  `food_acquired`; this one *proves a collapse is lossless* and refuses to guess otherwise.
- `aggregation: {i: unique}` (Tajikistan `data_scheme.yml`) — **OK (anchored on §4)**: no
  longer inert; `test_aggregation_key_is_read_not_inert` fails on a pre-fix tree.
- No CONTRADICTION with `SkunkWorks/grain_aggregation_policy.org` (§3): no new grain
  reduction is introduced — an existing hidden one is surfaced and enforced.
