# Prior-Art Ledger — GH #323 (GhanaLSS)

**Search tier used:** ripgrep + git (gitnexus MCP tools were not available in this
environment; call-graph established manually — `_normalize_dataframe_index` has
exactly four call sites, all in `country.py`: 2115, 2715, 2929, 2983).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py`) reduces a wave/country
frame to its DECLARED index (`data_scheme.yml` → `index:`). When that index is
non-unique it collapses the duplicates with `groupby().first()`, keeping one row
per tuple and silently discarding the rest. GhanaLSS was reported with 7
affected cells (~982k duplicate rows). The task: fix GhanaLSS's instances
*without* leaving the class alive, and without moving any other country.

GhanaLSS turns out to exhibit **both** failure modes the same line of code
enables — which is why one uniform fix would have been wrong:

* **(A) benign-but-silent** — `cluster_features`, 5 waves. The source is
  household-grain; the projected columns are cluster-invariant. The collapse is
  a correct de-duplication, arrived at silently.
* **(B) silently WRONG** — `cluster_features`, 1987-88 + 1988-89. `Region` is
  wired to a person-level **region of birth**. In 1988-89 `.first()` fabricates a
  cluster's region from its first-listed person. In 1987-88 the wave's own
  `df_edit` hook fabricates it from the modal birthplace of the cluster's
  under-12s instead — same wrong column, different fabrication. (Corrected
  2026-07-21; this line previously attributed `.first()` to both waves. See
  "Corrections after adversarial review", C2.)
* **(C) phantom NaN keys** — `food_security`, 2016-17. 110 households with
  NaN `clust`/`nh` collapse onto one NaN tuple and are dropped.

## §2 Existing machinery

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4100` | reorders/reduces to declared index; `groupby().first()` on duplicates | partly | **extend** (checked reducer) |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py:101` | `food_acquired` → sum, not first (GH #501) | yes | precedent for a per-table policy |
| `aggregation:` block | 9 × `data_scheme.yml` | declared `{visit: first}` | **no** | **was INERT — never read by any code** |
| `_finalize_result` `dropna(how='all')` | `country.py:2217` | drops all-NA rows | yes | explains why (C) recovers 0 API rows |
| `get_categorical_mapping` | `local_tools.py:1138` | org-table → dict | yes | returns `{}` for 1988-89 (separate bug) |

**Key prior-art finding:** `aggregation:` was already *declared* by 9 countries
and *skipped* as a scheme meta-key (`country.py:2387`, `diagnostics.py:230`) —
but **grep for its consumption returns zero**. It was pure prose. Adding
`aggregation:` to GhanaLSS without implementing it would have been precisely the
"prose is not enforcement" failure. See `SkunkWorks/grain_aggregation_policy.org`
(item #4, "not yet built").

## §3 Definitions & conventions in force

- Design contract, `SkunkWorks/grain_aggregation_policy.org`: *"the
  composition/access path — `country.py` (`_normalize_dataframe_index`) — NEVER
  reduces grain."* Item #3 of its implementation order names this exact line:
  *"Both core `.first()` collapses are the GH #323 / #325 silent-data-loss
  footguns."* The full union+sentinel rewrite is a framework-wide change and is
  **not** attempted here (it would move every country); this task implements the
  narrower "declared or fatal" half.
- `cluster_features` owns `v`; declared index `(t, v)` — `CLAUDE.md`, canonical
  `lsms_library/data_info.yml`.
- class-1 (silently WRONG) vs class-2 (silently MISSING): class-2 is strictly
  safer. Drop loudly rather than guess.

## §4 Invariants & assumptions

- `groupby(level=…)` defaults to `dropna=True` → **NaN-keyed rows are dropped
  outright, not merged into a phantom**. Verified: `food_security` 2016-17 loses
  all 110.
- `_finalize_result`'s `dropna(how='all')` (`country.py:2217`) drops any row
  whose every non-index column is NA. This is why **rows recovered = 0** (§6).
- The `.pth` trap is real: `PYTHONPATH` does **not** redirect `lsms_library` to a
  worktree — `sys.path[0]` (cwd) wins. Verified empirically; defeated with an
  explicit `sys.path.insert(0, WT)` + assert.

## §5 Reuse decision

Extend the existing, already-declared-but-inert `aggregation:` block rather than
invent a parallel key. New reducer `invariant`, keyed on **columns** (per the
design doc's `{column: reducer}` shape). All 9 existing blocks are
`{visit: first}` — `visit` is an index *level*, never a column, and `first` ≠
`invariant`, so they are doubly unaffected. Proven: §6.

## §6 Evidence

**Instrument validated first** (the brief's trap): scanner reproduced Mali
`household_roster` 2014-15 dup = **32,026** and Guyana `housing` 1992 dup =
**311** exactly, then all 7 reported GhanaLSS cells exactly.

### (B) `Region` is birthplace — five independent strands

1. `mapping.py`'s `Region()` and `Birthplace()` are **byte-identical function
   bodies** over the same `region_dict`; both waves map `REGION` as the roster's
   `Birthplace`.
2. The waves' own `categorical_mapping.org` `region` code list runs 1..17 and
   includes **11=Nigeria, 12=Ivory Coast, 13=Togo, 14=Burkina Faso, 15=Mali,
   16=Other Africa**. A Ghanaian enumeration area cannot be *located* in Nigeria.
3. `REGION` varies **within a single household** — 1,019/3,192 HH (1988-89) and
   1,033/3,136 (1987-88), up to **6 distinct values in one household**.
4. It varies within a cluster in 167/170 (1988-89) and 174/176 (1987-88).
5. `Y01A.DCT` order: MAR, SCOHAB, SID, **REGION, NAT**(ionality), LODGE — the
   classic birthplace→nationality pair.

**Averted fabrication** (decoding REGION as the repaired `region_dict` would):

| wave | clusters labelled **"Nigeria"** | disagree with modal birthplace |
|------|-------------------------------:|-------------------------------:|
| 1987-88 | **7** of 176 | 49 of 176 |
| 1988-89 | **9** of 170 | 53 of 170 |

**The landmine.** `Region` is currently **100 % NaN** (0/14,924) because
`get_categorical_mapping` returns `{}` for 1988-89 — an *unrelated* defect. So
the class-1 fabrication is **armed behind a class-2 bug**: repair `region_dict`
(a plausible future fix, since it also breaks `Birthplace`) and the fabrication
goes live. Dropping `Region` costs **zero** information today and defuses it.

### (A) cluster-invariance — verified, all 5 waves

`region` / `loc2` / `ez`: max distinct value per cluster = **1**; clusters with
>1 = **0**. So `.first()` returns the right answer; it was silent, not wrong.

| wave | source | rows | clusters |
|------|--------|-----:|---------:|
| 1991-92 | POV_GH.DTA | 4,523 | 365 |
| 1998-99 | SEC0A.DTA | 5,998 | 300 |
| 2005-06 | aggregates/pov_gh5 | 8,687 | 580 |
| 2012-13 | PARTA/g6loc_edt | 16,772 | 1,200 |
| 2016-17 | g7sec8h.dta | 934,584 | 1,000 |

### (C) `food_security` 2016-17

110 tail rows (13,899–14,008): `clust`=NaN, `nh`=NaN, **all 8 FIES items NaN**,
but a valid `hid`. `hid == f"{clust}/{nh:02d}"` with **100.00 %** fidelity on the
13,899 good rows, and is byte-identical to the id the framework builds from
`[clust, nh]`. The 110 parse to **110 distinct households, disjoint** from the
answered set, and **all 110 are present in `sample()`**. `FIES_score()` already
returns `pd.NA` when all 8 items are missing, so recovery does **not** fabricate
a food-secure 0.

### The guard actually bites — validated where validation is NEEDED

Testing the guard against the *live* 1988-89 tree is **vacuous** (Region is
all-NA → `nunique` = 0 → passes trivially). That would be validating where it is
free. Fed the **real birth-region values** (the world where `region_dict` is
repaired), it RAISES:

```
1987-88: 'Region' non-constant in 174 of 176 group(s)  -> ValueError
1988-89: 'Region' non-constant in 167 of 170 group(s)  -> ValueError
```

### Nothing else moved

757 real (country × wave × table) cells pushed through **both** the BASE and FIX
`_normalize_dataframe_index` on real cached data: **0 differ**, 0 non-GhanaLSS
deltas. The change is data-inert everywhere else; it alters only *warnings*.

## §7 Honest accounting — rows recovered = 0

The API row count does **not** move, and saying otherwise would be a vanity
metric:

- (A) one row per cluster **is** the correct grain — nothing to recover.
- (B) 1988-89's `cluster_features` was already contributing **0 API rows**
  (all-NA → `dropna(how='all')`). Removing it is a 0-row change.
- (C) the 110 recovered households are all-NA in every declared column, so
  `_finalize_result`'s `dropna(how='all')` drops them — exactly as it already
  drops 7 all-NA households that *did* have `clust`/`nh` populated.

The value is **correctness**: a fabricated cluster `Region` deleted, a silent
967k-row collapse made declared-and-checked, and a 110-household phantom killed.

## §8 Left undone (deliberately, not fixed)

1. **`sample` 2 duplicate `(i, t)` tuples** — pre-existing, fires identically on
   pristine base. Root cause is **different**: `panel_ids` is many-to-one —
   1988-89 HH `204922` **and** `204932` both map to `1987-88,101332`; `255718`
   **and** `255728` both map to `1987-88,114008`. Two *distinct* households
   collapse onto one id. My `invariant` reducer cannot apply (weight/strata
   genuinely differ between two different households), and choosing the "true"
   match is a guess I refuse to make. **Reported, not guessed.**
2. **`get_categorical_mapping` returns `{}` for GhanaLSS 1988-89 *and* 1987-88**
   (1987-88 added 2026-07-21 — measured, `len(region_dict) == 0` in both) — both
   `region_dict` and `relationship_dict` are empty, so `Birthplace` *and*
   `Relationship` are all-NA for that wave. Separate defect; **not fixed here on
   purpose** — fixing it would *arm* the (B) fabrication, which is precisely why
   (B) is being removed first. 1998-99's `cluster_features` Region/Rural are
   likewise all-NA (0/5,998), so that wave contributes 0 API rows today.
3. The framework-wide union+sentinel rewrite (`grain_aggregation_policy.org`
   item #3). Out of scope for a single-country fix; the `invariant` reducer is
   the "declared or fatal" half and is available to every country now.

---

## Stripped to config-only (2026-07-13, GH #323 consolidation)

This branch (`fix/323-ghanalss-config`, off `origin/development`) carries the
country work from `fix/323-ghanalss` and **nothing else**. **PR #609 is left
open and untouched** — this is a separate branch, not a rewrite of it.

Per `slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org`:

* **Stripped: 85 lines of `lsms_library/country.py`** (the Design-A `invariant`
  reducer) and `tests/test_declared_aggregation_gh323.py` (which exercised it).
  **D1: core does not aggregate.**
* **Stripped: the `aggregation: {Region/Rural/Ecological_zone: invariant}` block.**
  Comments in the 1987-88 / 1988-89 `data_info.yml` that cited it as *enforcement*
  ("re-adding either column here RAISES at build time") have been corrected — that
  claim is no longer true, and the drop is now pinned by a test instead.
* **Kept, and it stands alone:** the 1987-88 / 1988-89 `cluster_features` deletions
  and the 2016-17 `food_security` rekey. Both are config facts about the *source
  columns*, independent of any collapse mechanism. Also kept: the `Ecological_zone`
  declaration (2012-13 already emits `ez`; it was simply never declared).

**Note on where the fix is observable.** Neither change moves the *country-level*
API numbers, so both had to be verified at the **wave** level. Cold build
(`LSMS_NO_CACHE=1`), branch vs. `origin/development`:

* `1988-89` `cluster_features`: development builds **14,924 person-rows** carrying a
  `Region` column — that column is Y01A.DAT's `REGION`, the person's region of
  **BIRTH** (its code list runs to 11=Nigeria, 12=Ivory Coast, 13=Togo; a Ghanaian
  EA cannot be in Nigeria). This branch builds **0 rows, no columns** — deliberately
  unwired. Two precisions added 2026-07-21 after adversarial review: (a) those
  14,924 `Region` values are **all NA** (`notna() == 0`) because `region_dict` is
  `{}` — the fabrication is armed, not firing; (b) **`1987-88` is NOT "likewise"**.
  Development already builds **0 rows** for 1987-88 — `(0, 2)`, columns
  `['Region', 'Rural']` — because that wave routes through `mapping.py`'s
  `cluster_features(df)` `df_edit` hook, whose `groupby(['t','v','Region'])` drops
  every row when `Region` is all-NA. Only 1988-89 has the 14,924→0 delta.
* `2016-17` `food_security` at wave level: development yields **13,899 distinct `i`
  + 110 NaN-keyed rows** (the 110 collapse onto one phantom `(t, NaN)` tuple and are
  deleted by groupby's dropna). This branch yields **14,009 distinct `i`, 0 NaN** —
  all 110 recovered as the distinct households they are. The country-level count is
  unchanged (13,892) because those 110 are all-NA and are dropped by
  `_finalize_result`'s `dropna(how='all')` — exactly as the original branch predicted.
  **The fix removes the phantom at the source; it is not expected to move the API
  row count.**

---

## Corrections after adversarial review (2026-07-21)

The review reproduced every *data* number in this PR exactly and did not move the
code. What it broke were three **claims**. All three are corrected here and in the
tracked YAML comments; the corrections are comment-only.

### C1 (was HIGH) — "no cluster-invariant region source exists" was FALSE

`1988-89/_/data_info.yml` asserted, as permanent justification for the drop, that
*"a sweep of all 93 .DAT files in this wave's Data/ found no cluster-invariant
region-like column"*. That is the Albania-precedent anti-pattern CLAUDE.md bans by
name, and worse than unevidenced — it is contradicted. Re-ran the sweep myself:
`REGION` appears in **three** of the 93 files, not one.

| measurement | value |
|---|---|
| files in 1988-89 `Data/` with a `REGION` column | `Y01A.DAT` (roster), **`HEALTH.DAT`**, **`DRUG.DAT`** |
| `HEALTH.DAT` / `DRUG.DAT` shape | (231, 404) / (169, 191); both carry `REGION` + `CL1..CL5` |
| offset that resolves `CL*` to `CLUST` | **`+2000`** — 166/168 (HEALTH), 167/169 (DRUG) land in the roster's 170-cluster set; offsets `0` and `+1000` land **0** |
| clusters covered by HEALTH ∪ DRUG | **168 of 170** |
| clusters with a cross-facility disagreement | **14** |
| HEALTH vs DRUG agreement on shared clusters | **154 of 165** |
| `REGION` range | HEALTH/DRUG **0..9**; roster **1..11** → 0-indexed against the wave's 1-indexed `region` table |
| `REGION + 1` == cluster's modal birth-region | **160 of 166** |
| mean / median share of a cluster's residents BORN in its HEALTH region | **0.793 / 0.855** |

That last row is the cleanest confirmation of the PR's own thesis: the ~20% gap
between "born here" and "lives here" *is* migration, which is exactly why
birthplace cannot stand in for location. But the categorical negative had to go.
The comment now says what is true: **a candidate exists, unvalidated, not wired —
a `todo`, not a closure.** Not wiring it is still right: 14 ambiguous clusters, 2
uncovered, and the `CL + 2000` mapping is inferred from value ranges, not
documented. The `.DCT` files carry positions and names only (verified: no variable
labels), and per `GhanaLSS/_/CONTENTS.org` the **GLSS1/GLSS2 questionnaires are
scanned-image PDFs with no text layer** — so check C4 (the instrument) is
*unrunnable* for these two waves. That is precisely why a closing verdict here
would have been unfalsifiable.

1987-88's weaker version was corrected too. Directly, the review's charge does
*not* stick there: a sweep of all **77** `.DAT` files in that wave (not 93 — that
is 1988-89's count) finds `REGION` in exactly one file, the roster. Indirectly it
does: `1988-89/Data/CLYR1YR2.DAT` bridges the two waves' clusters — **85** linked
`(CLYR1, CLYR2)` pairs, all 85 `CLYR1` values in 1987-88's 176 clusters and all 85
`CLYR2` values in 1988-89's 170; **84 of the 85** reach a HEALTH/DRUG region, and
that bridged region equals the 1987-88 cluster's modal birth-region in 77 of 84.
Partial (85 of 176 clusters = 48%) and inheriting every ambiguity above, so also a
`todo`.

### C2 (was MEDIUM) — the `Age: AGEY` characterization was FALSE

The 1987-88 comment said `AGEY` *"made a cluster's 'Age' the age of its
first-listed person (range 17-80)"*. That never happened. `1987-88/_/mapping.py`
defines a **table-level `cluster_features(df)` hook**, dispatched by `country.py`
as the table's `df_edit`; `AGEY` is its **filter input** (`df.query("Age<12")`) and
the frame it returns has columns `['Region', 'Rural']` — no `Age`, ever. Verified
cold on the pre-fix config tree: `Country('GhanaLSS')['1987-88']
.grab_data('cluster_features')` → **`(0, 2)`, `['Region','Rural']`**.

The real 1987-88 defect is therefore **modal birthplace of a cluster's under-12s**
— the very guess the 1988-89 comment says it refuses to make. It deserved naming,
and it is a better argument for the drop than the one that was given. The hook
survives this PR as dead code (nothing declares the table any more), so a landmine
note now sits on its docstring: re-adding the block re-arms it.

Root cause of the zero rows, in both waves: `get_categorical_mapping` returns `{}`
for **1987-88 as well as 1988-89** (measured; §8 item 2 previously named only
1988-89). `Region` is all-NA, and the hook's `groupby(['t','v','Region'])` drops
every row.

### C3 (was LOW ×2) — wrong wave's numbers

* *"development builds 14,924 rows … `1987-88` likewise"* — no. Development
  already builds **0** rows for 1987-88 (C2). Only 1988-89 has the delta. Fixed
  above and in the PR body.
* *"Y00A.DAT's LOC … varies within cluster in 3 of 170 clusters"* in the
  **1987-88** comment — 3/170 is **1988-89's** figure. 1987-88 is **5 of 176**.
  Both re-measured. Also worth recording, and stronger than the original claim:
  `LOC` is **1 for 3,142 of 3,147** households in 1987-88 (and 1 for 3,191 of
  3,194 in 1988-89), i.e. near-constant, and its `.DCT` gives no label — so it is
  not a "plausible Rural indicator" at all. Still not wired.

### Not disputed, and re-derived independently

Every data number in the PR reproduces. Re-measured from source: 1,019/3,192 and
1,033/3,136 households with within-household `REGION` variation (max 6 distinct);
167/170 and 174/176 clusters; `g7sec9c.dta` 14,009 rows, 110 with `clust`&`nh` NaN
and all eight FIES items NaN, 110 distinct `hid`, **0** overlap with the 13,899
answered ids, and `hid == f"{clust}/{nh:02d}"` on **13,899 of 13,899**.

One addition, answering the PR thread's *"an EA could be in Nigeria if we're
following movers"*: code 11 is populated (236 persons in 1988-89) but spread over
**87 of 170 clusters**, and **no cluster is more than 17.5% foreign-coded** —
**zero** clusters are 100%. If `REGION` were the EA's location, an EA coded 11
would have *all* its residents coded 11. Recorded as strand 3 in the YAML.

### Follow-up, deliberately not done here

Wiring / adjudicating `HEALTH.DAT` + `DRUG.DAT` `REGION` as 1988-89's
`cluster_features.Region` (and bridging it to 1987-88 via `CLYR1YR2.DAT`) needs
the four `absent`-cell checks, and C4 is blocked on the image-only GLSS1/2
questionnaires. Out of scope for this PR; noted, not guessed.
