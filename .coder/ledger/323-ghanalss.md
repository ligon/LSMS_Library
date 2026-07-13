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
  wired to a person-level **region of birth**. `.first()` fabricates a cluster's
  region from its first-listed person.
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
2. **`get_categorical_mapping` returns `{}` for GhanaLSS 1988-89** — both
   `region_dict` and `relationship_dict` are empty, so `Birthplace` *and*
   `Relationship` are all-NA for that wave. Separate defect; **not fixed here on
   purpose** — fixing it would *arm* the (B) fabrication, which is precisely why
   (B) is being removed first. 1998-99's `cluster_features` Region/Rural are
   likewise all-NA (0/5,998), so that wave contributes 0 API rows today.
3. The framework-wide union+sentinel rewrite (`grain_aggregation_policy.org`
   item #3). Out of scope for a single-country fix; the `invariant` reducer is
   the "declared or fatal" half and is available to every country now.
