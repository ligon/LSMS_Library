# GH #323 — Pakistan 1991 `cluster_features` silent collapse

## §1 Task

`_normalize_dataframe_index` silently collapsed Pakistan 1991 `cluster_features`
from **4,957 rows → 301** via `groupby(['t','v']).first()`, discarding **4,656 rows**.

Class: **EXTRACTION_BUG** (not a missing index level). The declared index `(t, v)` is
correct — canonical `lsms_library/data_info.yml` defines `cluster_features: (t, v)`.
The *extraction* was wrong in two compounding ways.

## §2 Findings (all verified against source, not taken on faith)

Instrument note: the L2-**country** parquet (`var/`) is written POST-collapse and is
useless as evidence. All counts below come from the L2-**wave** parquet and the raw
`.DTA`. Scanner was calibrated on the known positives before use:
Mali/2014-15/household_roster → 32,026 dups ✓, Guyana/1992/housing → 311 dups ✓.

### (1) Wrong granularity → the collapse
`1991/_/data_info.yml` built `cluster_features` from **F00A.DTA, the household COVER
SHEET**, with `idxvars: {v: clust, i: hid}`.

| fact | value |
|---|---|
| F00A.DTA rows | 4,957 |
| distinct `hid` | 4,957 (household grain) |
| distinct `clust` | **301** (cluster grain) |
| L2-wave parquet index | `['t','v','i']`, 4,957 rows |
| after the collapse | 301 rows — **4,656 dropped** |

The surplus `i` level is what manufactures the household rows and hands them to a
silent reducer.

### (1a) CORRECTION to the diagnosis: WHERE it collapses (I traced it; it is not #323's site)
The dispatch brief — and my own first draft of this ledger — said the collapse happens in
`_normalize_dataframe_index` via `droplevel('i')` + `groupby(['t','v']).first()`, and that
its GH #323 `RuntimeWarning` fires on a cold build. **For `cluster_features` that is false,
and I verified it by tracing.** On a cold, cache-cleared, pristine-`development` build:

```
>> _normalize_dataframe_index called: wave='1991' rows=301 idx=['t','v'] unique=True
```

It receives a frame that is **already 301 rows and already unique** — so the #323 warning
**never fires, warm or cold** (a full cold build emits zero duplicate-tuple warnings).

The real collapse is **earlier and completely silent**, in `Wave.cluster_features()`
(`country.py` ~1168, added for **GH #161**):

```python
if 'i' in df.index.names:
    keep_levels = [lvl for lvl in df.index.names if lvl != 'i']
    agg = {c: ('mean' if c in ('Latitude','Longitude') else 'first') for c in df.columns}
    df = df.groupby(level=keep_levels).agg(agg)      # no warning, ever
```

Measured layer by layer (pristine base): `grab_data` → **4,957** rows `(t,v,i)`;
`Wave.cluster_features()` → **301** rows `(t,v)`. That is the 4,656.

That branch justifies `.first()` on the premise that *"Region/Rural/District are invariant
within a cluster by construction of the LSMS-ISA sampling design"* — **a precondition it
never checks.** Pakistan violates it outright (§2(2)). So this instance was strictly
*worse* than the #323 class as briefed: not "silent unless cold", but **silent always**.

This finding is what forced a test redesign — see §5.

### (2) Wrong columns → class-1 silently WRONG (the serious part)
- `Region: religion` — the household's **religion code**, wired to a geography column.
  Crosstab religion × province shows they are unrelated (religion is ~96% code-1 in
  *all four* provinces). The API's `Region` was literally `"1.0"` for 291/301 clusters.
- `Language: langint` — language **of the interview**; a household/interview attribute.
- Neither is cluster-level. They **vary within cluster**: religion in 75/301 clusters,
  langint in **141/301 (47%)**. So `first()` was not deduplicating identical rows — it
  elected one arbitrary household as spokesperson for its whole cluster.
  **Measured: 252 households' religion + 849 households' langint disagree with the value
  `first()` assigned to their cluster = 1,101 silently-wrong attributions.**
- `Rural` was declared `optional: true` (canonically `required: true`) and never populated.

### (3) The right source was already in the repo
**REGIONS.DTA** (`hhcode`, `province`, `urbrural`, `newprov`) — already used by `sample`.
Both `province` and `urbrural` are **perfectly constant within cluster: 0 of 300 clusters
vary** — i.e. they are *genuine* cluster-level features, so cluster-level reduction is
EXACT and lossless.

### (4) Labels CONFIRMED from the codebook — not inferred
The diagnosis flagged the labels as inferred from distributions and unconfirmed. I pulled
`Data/REGIONS.TXT` (lock-free, via `_ensure_dvc_pulled`; **never** `dvc pull`) and it *is*
the codebook:

```
code     province     newprov           urbrural
1        Punjab       North Punjab       urban
2        Sind         South Punjab       rural
3        NWFP         Sind
4        Balochistan  NWFP
5                     Balochistan
```

So `province` 1=Punjab, 2=Sind, 3=NWFP, 4=Balochistan; `urbrural` 1=urban, 2=rural
(matching `sample`'s existing mapping). Labels are now **documentary, not guessed**.
Note the codebook spells it **"Sind"** (not "Sindh") — I use the codebook's own spelling.
Independent cross-check: `newprov` nests inside `province` exactly as the codebook
predicts (Punjab→{North,South Punjab}, Sind→Sind, NWFP→NWFP, Balochistan→Balochistan).

### (5) The one genuinely ambiguous cluster — where I did NOT guess
The diagnosis said "204 households / 1 cluster absent from REGIONS.DTA". Refined:
- 204 cover households have **no** REGIONS row, spread across **115** clusters.
- Because province/urbrural are cluster-constant, those 114 clusters still resolve
  **exactly** from their *other* households. No loss.
- **Exactly one cluster — `2202029`, which has exactly ONE household — is entirely absent
  from REGIONS.DTA.** It has no region data at all.

`clust`'s leading digit encodes province with a **zero off-diagonal** crosstab, so
province is *recoverable* for `2202029`. **I deliberately did not use it as a data source.**
That inference is validated precisely on the 300 clusters where province is *already known*
(free) and is untestable on the *one* cluster where it is actually needed — which is exactly
the Guyana failure mode this issue's standard calls out ("validate where validation is
NEEDED, not where it is free"). And `urbrural` is not recoverable from `clust` at all.

So cluster `2202029` gets `Region = <NA>`, `Rural = <NA>` — **class-2 (honestly missing)**,
never class-1 (silently wrong) — and the script emits an explicit `RuntimeWarning` naming
the cluster. The prefix→province rule is instead used as a **build-time GUARD** (assert
`clust` leading digit == `province` for all 300 resolvable clusters), not as a data source.
Precedent: `sample` already carries `Rural = NaN` for these same 204 households.

**Consequence, measured (not predicted):** the script writes 301 rows to the L2-wave
parquet (300 + the all-`<NA>` orphan), but the **API returns 300**. `_finalize_result`
runs `df.dropna(how='all')` (`country.py` ~2217), a documented "hollow row" safety net
that drops any row whose every non-index column is NaN. The orphan is exactly such a row.
I let it: omitting the row and carrying an all-NA row are *informationally identical* to a
consumer (a `sample`→`cluster_features` join yields NaN geography for that household
either way), and fighting a documented framework policy to keep a row with zero
information would buy nothing. Both are class-2. The parquet retains the row as the honest
record; the API applies the standard policy. `cluster_features` therefore covers 300 of
301 clusters, and cluster `2202029` (1 household) is *absent by design, because the survey
never recorded its region* — asserted in the regression test so nobody later "fixes" it by
imputing.

## §3 Prior art / definitions in force (cited, not duplicated)

- `lsms_library/data_info.yml` — `index_info: cluster_features: (t, v)`; `Region` and
  `Rural` both `required: true`; canonical `Rural`/`Urban` spellings.
- `CLAUDE.md` — "Do NOT put `v` in feature `data_scheme.yml` indexes other than
  `cluster_features` (which owns it)"; `format_id` auto-applied to `idxvars` not `myvars`;
  never `dvc pull` from CLI.
- `country.py::run_make_target` — with no Makefile present it executes
  `{wave}/_/{table}.py` directly (this is how Pakistan's existing `interview_date` builds).
- `country.py::_normalize_dataframe_index` — the collapse site.

## §4 Design decision: why a script, not a declared `aggregation:`

I checked whether declaring `aggregation:` in `data_scheme.yml` would drive a reducer.
**It would not.** `country.py:2387` only lists `aggregation` in
`_skip = {"index","materialize","backend","aggregation"}` so it isn't parsed as a *column*;
**no code path reads it as a policy.** The only real reducer is the hardcoded
`_ADDITIVE_MEASURE_COLUMNS` map in `feature.py`. Declaring `aggregation: first` would have
been **prose, not enforcement** — the exact failure mode this issue warns about three times.

Therefore the fix makes the extraction **unique by construction**, so *no reducer runs at
all*. YAML alone cannot do this (any household-grain source yields duplicate `v` and gets
silently collapsed), so `cluster_features` moves to the script path — the documented
rule-of-thumb case ("cross-file join" + a grain reduction that must be guarded).

## §5 The fix

1. `1991/_/cluster_features.py` (**new**) — joins F00A (cluster universe, 301) with
   REGIONS (geography), **asserts** cluster-constancy of `province`/`urbrural` and the
   `clust`-prefix↔`province` invariant, reduces to one row per cluster, and asserts the
   result is unique on `(t, v)`. Assertions are *enforcement*; the build fails loudly if
   any invariant ever breaks.
2. `1991/_/data_info.yml` — drop the `cluster_features:` YAML block (superseded).
3. `_/data_scheme.yml` — `cluster_features`: add `materialize: make`; **remove `Language`**
   (not a cluster attribute — a modal reducer would *invent* an attribute the survey never
   measured); flip `Rural` from `optional: true` to required.
4. `tests/test_pakistan_cluster_features.py` (**new**) — regression net.
5. `_/CONTENTS.org` — documents the rewiring, the codebook labels, and cluster `2202029`.

### Test design — two tempting guards here are VACUOUS, and I removed one after writing it
My first draft asserted "no GH #323 collapse warning fires on a cold build". It **passed on
the unfixed tree** — because, per §2(1a), that warning never fires for this table. That is a
provably vacuous guard, the exact failure this issue's standard calls out. Likewise,
asserting the API index `is_unique` is vacuous: *post*-collapse it is always unique — which
is precisely why the defect survived so long.

The honest detector had to sit at the **extraction**, upstream of the silent reducer: assert
the wave-level artifact is already cluster-grain (no `i` level; 301 rows, not 4,957). That,
plus the semantic content of the columns, is what the tests assert.

| test | pre-fix | post-fix |
|---|---|---|
| `test_extraction_is_cluster_grain` | **FAIL** (4,957 rows, idx `(t,v,i)`) | pass |
| `test_index_is_cluster_grain` | **FAIL** | pass |
| `test_region_is_province_not_religion` | **FAIL** (values `"1.0"`,`"2.0"`,`"3.0"`) | pass |
| `test_rural_is_populated` | **FAIL** (never populated) | pass |
| `test_language_is_gone` | **FAIL** (`Language` present) | pass |
| `test_cluster_without_region_is_absent_not_imputed` | **FAIL** | pass |
| `test_geography_is_constant_within_cluster` | pass (source invariant — a guard, not a detector) | pass |
| `test_v_aligns_with_sample` | pass (guard) | pass |

**6 fail pre-fix / 8 pass post-fix.** The two that pass in both are honest guards on source
invariants, and are labelled as such rather than counted as detectors.

`religion` / `langint` are legitimate *household* attributes; they are not relocated to a
new household-level table in this change (scope), and no correct information is lost by
dropping them — every value they exposed was an arbitrary household elected as its
cluster's spokesperson. Noted in CONTENTS.org for a future household-level table.

## §6 Result

| | before | after |
|---|---|---|
| L2-wave rows | 4,957 (household grain) | **301** (cluster grain) |
| duplicate `(t,v)` rows in L2-wave | **4,656** | **0** |
| API rows | 301 | 300 (+1 hollow row dropped by policy) |
| API `Region` | religion code (`"1.0"` ×291) | Punjab 154 / Sind 83 / NWFP 42 / Balochistan 21 |
| API `Rural` | never populated | Urban 150 / Rural 150 |
| silent `first()` elections | 1,101 wrong attributions | **0** |
| GH #323 collapse RuntimeWarning | fires on cold build | **does not fire** |

The row count does not "recover" to 4,957 — and it **must not**. `cluster_features` is a
cluster-level table; 301 *is* the correct cardinality. What is recovered is the 4,656 rows'
worth of *information* that was being destroyed: the table now carries real, cluster-constant
geography instead of an arbitrarily-elected household's religion.

## §7 CLASS-level defect — NOT fixed here, must not be forgotten

Two distinct class-level defects, and the second is the one this instance actually exposes.

### (a) The briefed class (static, config-vs-config)
A wave's `idxvars` may declare index levels FINER than the table's declared index in
`data_scheme.yml`, and **nothing checks the two against each other**; the surplus level is
silently swallowed downstream. This needs no data and no cache — it is a pure config
mismatch checkable at load time.

### (b) The class this instance actually exposes: an UNCHECKED PRECONDITION (the sharper one)
`Wave.cluster_features()` (`country.py` ~1168, GH #161) silently reduces any household-grain
`cluster_features` with `groupby().first()`, on the stated premise that the columns are
"invariant within a cluster by construction of the LSMS-ISA sampling design". **It never
checks that premise.** When the premise holds, `.first()` is harmless dedup — which is why
this has survived. When it fails, `.first()` elects an arbitrary household as spokesperson
for its cluster and the output is **class-1 silently wrong**, with *no warning on any code
path, warm or cold*. That is exactly what Pakistan was.

I scanned the config tree for the trigger (`cluster_features` idxvars declaring `i`):

> **39 wave-cells across 17 countries** — Albania, Burkina_Faso, Cambodia, China, Guatemala,
> Guyana, Kazakhstan, Kosovo, Liberia, Malawi, Niger, **Pakistan**, Serbia and Montenegro,
> South Africa, Tajikistan, Tanzania, Uganda.

Every one routes through that unchecked `.first()`. For most, the columns genuinely *are*
cluster-invariant, so it is lossless. **But nothing verifies that, per country, ever** — so
any future (or existing) miswiring of a household-level column into `cluster_features` is
silently wrong, undetectably.

**Recommended central fix** (one library change, catches all 17 countries): in that GH #161
branch, verify the premise before reducing — for each non-GPS column, check
`groupby(level=keep_levels)[col].nunique() <= 1` and `warnings.warn` (or raise) naming the
column and the offending clusters when it fails. Cheap, data-driven, and turns a silent
assumption into enforcement. Pakistan's own script now does exactly this locally
(INVARIANT 1), so Pakistan is protected regardless.

### Why I did not land (a) or (b) here
My mandate for this task is explicitly config-only ("No `country.py` change needed"), and
~13 sibling agents are concurrently fixing the other affected countries in their own
worktrees. A repo-wide hard guard would fail their currently-red cells, and an allowlist
file would collide across 14 concurrent PRs. Both guards should land **once, centrally,
after** the per-country fixes — not from a single-country branch.

**GH #323 must NOT be closed on this PR.** Pakistan is one instance. Closing the issue on a
single-instance fix is literally how #323 got closed the first time.
