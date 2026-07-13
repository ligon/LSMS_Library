# Prior-Art Ledger — GH #323 / Cambodia (`cluster_features`)

**Search tier used:** ripgrep + git (gitnexus not consulted; the surface is two
collapse sites in `country.py` plus one country's `data_scheme.yml`).

## §1 Task, restated

`Country('Cambodia').cluster_features()` returns 252 rows from a 1512-row
source. The source (`2019-20/Data/hh_sec_1.dta`, the household cover page,
extracted by the wave's `data_info.yml` with `idxvars {i: HHID, v: s01q01}`) is
HOUSEHOLD-grain; the country's `_/data_scheme.yml` declares the table at
CLUSTER grain, `index: (t, v)`. The framework closes that gap by dropping the
undeclared `i` level and collapsing the 1260 resulting duplicate `(t, v)` rows
with `groupby().first()` — silently.

Verified on the pre-collapse frame: this is **not** data loss. The 1260 rows are
exact repetitions of the cluster's attributes (0 / 252 clusters disagree on
`Region`, 0 / 252 on `Rural`), so `first()` had no choice to make and 0 rows of
information are discarded. Cambodia is a TRUE positive for *silent undeclared
aggregation* and a FALSE positive for *silently dropped data*. `recovers: 0`.

The defect to fix is therefore the **class**, not the instance: the projection is
correct but **undeclared and unverified**, and the identical code path is
silently WRONG (GH #323 class-1) wherever the invariance it assumes does not
hold. Fix = declare the aggregation, and *enforce* the declaration.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave.cluster_features` | `country.py:1168` | **The site where Cambodia's collapse actually fires.** `if 'i' in df.index.names: groupby(keep_levels).agg({'first', 'mean' for GPS})` (GH #161). Its comment *asserts* "Region/Rural/District are invariant within a cluster by construction" — prose, never checked. | no | **extend** (gate an assertion on the declared policy) |
| `_normalize_dataframe_index` | `country.py:4100` | Generic: reorder → drop undeclared levels → collapse duplicates (`sum` for `_ADDITIVE_MEASURE_COLUMNS`, else `.first()` + the #323 `RuntimeWarning`). | `test_normalize_index_j_preserved.py` | **extend** (same declared-dedup branch) |
| `aggregation:` scheme block | `Niger/_/data_scheme.yml:114`, `Togo/_/data_scheme.yml:91` (`visit: first`) | A `{level: reducer}` grain-aggregation policy. **Declared but consumed by nothing** — every reader (`diagnostics.py:174`, `country.py:2387`, `tests/test_table_structure.py:103`) merely *skips* the key. | no | **reuse the grammar, add the missing enforcement** |
| `SkunkWorks/grain_aggregation_policy.org` | design doc (2026-06-16) | Names both `.first()` collapses (`_normalize_dataframe_index`, `feature._harmonize_country_frame`) as "the GH #323 / #325 silent-data-loss footguns"; step 4 of its plan is the `aggregation:` block. | — | this task implements the `dedup` reducer of that block |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py` | The one collapse policy that *is* enforced today (sum, not first) for `food_acquired`. | yes | precedent for "policy decides the reducer"; untouched |

## §3 Definitions & conventions in force

- `cluster_features` canonical index — `(t, v)`; per `lsms_library/data_info.yml`
  (`Index Info > index_info:16`). It is also on the `Join v from sample >
  skip_extra` list (`data_info.yml:65`) — "keyed by `(t, v)` directly; has no
  `i`, so never joined".
- `v` (cluster) — `Single Index` in `lsms_library/data_info.yml:2`. In Cambodia
  `v = s01q01` is a **province-prefixed EA id** (12064 = Phnom Penh, 16001 =
  Ratanak Kiri), hence globally unique: no province/district level is missing
  from the index (rules out INDEX_INCOMPLETE — the Guyana failure mode).
- "Do NOT put `v` in feature `data_scheme.yml` indexes other than
  `cluster_features` (which owns it)" — `CLAUDE.md`, *`sample()` and Cluster
  Identity*.
- class-1 (silently WRONG) vs class-2 (silently MISSING): class-2 is strictly
  safer — the #323 house rule this fix is built on.

## §4 Invariants & assumptions

- **The L2-country parquet is written POST-collapse; the L2-wave parquet holds
  the truth.** (`{data_root}/Cambodia/2019-20/_/cluster_features.parquet` = 1512
  rows on `(t,i,v)`, unique; `var/cluster_features.parquet` = 252 rows.) Any
  verification must therefore run at **cold build**, on the pre-collapse frame —
  it can never be re-derived downstream. Confirmed by scanning both tiers.
- **The #323 warning never fires for `cluster_features` at all** — not even cold.
  `load_from_waves` calls `getattr(wave_obj, 'cluster_features')()`
  (`country.py:2673`), so `Wave.cluster_features` collapses the frame *before*
  `_normalize_dataframe_index` sees it; the frame arriving there is already
  unique. (Measured: 0 warnings on a cold Cambodia build, base code.) The
  original triage attributed the collapse to `_normalize_dataframe_index`; that
  is where the *generic* case lives, but **not** where Cambodia's fires.
- `groupby().first()` **skips nulls** — a group holding `{NA, 'Urban'}` collapses
  to `'Urban'` and loses nothing. So a null is not a contract violation; only a
  disagreement between two *observed* values is.
- Adding a key to a country's `_/data_scheme.yml` changes the v0.8.0 content hash
  (`CLAUDE.md`, *Automatic content-hash staleness*), so landing the declaration
  invalidates Cambodia's caches and forces exactly one cold, contract-verifying
  rebuild. The check re-runs whenever any hashed input changes — which is the
  only time its result could change.
- Unordered categoricals break `groupby()` (`CLAUDE.md`, *Gotchas with Teeth*) —
  the invariance check must stringify them first, as the collapse below it does.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| declaration syntax | **reuse** `aggregation: {level: reducer}` | already in the repo (Niger/Togo) and in the design doc; inventing a second grammar would fork the policy |
| `dedup` reducer | **new** (within that grammar) | `first` = "keep one, discard the rest" (the footgun itself); `sum` would double-count non-additive attributes (Region is not additive, and the 6 rows are *repetitions*, not parts). `dedup` = "these rows are one entity; collapsing is lossless" — the only reducer that states a *checkable* claim, and the only one that matches Cambodia's data. |
| where to enforce | **both** collapse sites (`Wave.cluster_features`, `_normalize_dataframe_index`) | Cambodia fires the first; the generic path fires the second. Enforcing only one would leave the class open. |
| violation policy | **raise** | class-2 > class-1: a loud stop naming the offending clusters beats returning one of two Regions at random. |
| repo-wide declaration for `cluster_features` | **rejected** | see §6 — the scan proves the invariance is FALSE in ~10 countries; blanket-declaring `dedup` would either break them or (worse) bless them. |

## §6 Open questions for the human

- **The same idiom is unverified — and demonstrably violated — elsewhere.**
  Scanning every country's L2-wave `cluster_features` frame (instrument validated
  against the known positives: Mali roster 32,026 dup rows, Guyana housing 311),
  the household→cluster collapse merges rows that **disagree on a retained
  column** in:

  | country / wave | conflicting groups (of total clusters) |
  |---|---|
  | Uganda 2019-20 | `Rural` 125/794, `District` 23/794, `Region` 11/794 |
  | Tanzania 2019-20 | `District` 99/247, `Rural` 97/247, `Region` 61/247 |
  | Pakistan 1991 | `Language` 141/301, `Region` 75/301 |
  | Serbia 2007 | `Region` 111/328, `Rural` 69/328 |
  | Burkina Faso 2014 | `District` 94/900 |
  | + Albania, CotedIvoire, Ethiopia, Kazakhstan, Liberia, Malawi, Nigeria, Guyana (smaller counts) |

  Each is a latent **class-1** (`first()` keeps one value at random, silently).
  They look like *incomplete indexes* — a cluster code unique only within a
  district, i.e. Guyana's ED/HH conflation — not like Cambodia. They need
  per-country diagnosis (is `v` really globally unique there?), which is out of
  scope for this PR; the enforcement mechanism landing here is the tool that
  makes each of them a one-line declaration + a loud failure if it is a lie.
  Filing this as a follow-up is the decision I'd like confirmed.
- Countries with per-household GPS (`Latitude`/`Longitude`, e.g. Malawi, Nigeria,
  Uganda) legitimately *average* to a cluster centroid — `mean`, not `dedup`.
  Should the policy grammar grow a per-column reducer (`aggregation: {i: {default:
  dedup, Latitude: mean, Longitude: mean}}`) so those countries can declare too?
  Not needed for Cambodia (no GPS in `hh_sec_1.dta`), so not built.

---
### Phase 3 — verification

- `_aggregation_policy` (`country.py`) — **OK (anchored on §2/§5)**: reads the
  existing `aggregation:` block; returns `{}` for the malformed/absent case, so a
  collapse can never *accidentally* claim to be declared.
- `_assert_dedup_lossless` (`country.py`) — **OK (anchored on §4)**: runs on the
  pre-collapse frame (the only place the evidence exists), stringifies unordered
  categoricals before `groupby`, and treats nulls as non-conflicts — matching
  `.first()`'s own null-skipping semantics. Not a REINVENTION of
  `_ADDITIVE_MEASURE_COLUMNS`: that picks a *reducer*, this *proves a claim*.
- `Wave.cluster_features` gate — **OK (anchored on §2/§4)**: assertion only; the
  `agg({'first'|'mean'})` call is untouched, so a country that has not declared
  the policy is byte-identical. Under a verified `dedup` contract `mean == first
  == the single value`, so the collapse itself needs no branch.
- `_normalize_dataframe_index` declared-dedup branch — **OK (anchored on §5)**:
  fires only when *every* dropped level is declared `dedup`; otherwise falls
  through to the additive-sum branch or the loud undeclared-collapse warning,
  both unchanged. A `first` declaration (Niger/Togo `visit: first`) is explicitly
  **not** enforcement and does not suppress the warning — pinned by
  `test_non_dedup_reducer_is_not_enforced`.
- Cambodia `_/data_scheme.yml` — **OK (anchored on §1/§3)**: declares
  `aggregation: {i: dedup}`; the index stays `(t, v)`, no column added, no row
  recovered (there were none to recover).
