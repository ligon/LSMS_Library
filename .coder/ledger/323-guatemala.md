# Prior-Art Ledger — GH #323 (Guatemala)

**Search tier used:** ripgrep + git floor (gitnexus not consulted; the work is
config-tree + one country module, and the blast radius was established directly
by rebuilding every Guatemala table before/after).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py:4175`) collapses a
non-unique **declared** index with `groupby().first()`. For Guatemala the
affected cell is `2000 / cluster_features`: the wave frame carried 7,276 rows
(one per household) on a declared `(t, v)` index, and 7,268 of them were
discarded to reach 8 rows.

The declared cluster key was `v: region` — 8 Guatemalan regions. That is
**coarser than the geography that determines the declared `Rural` column**: all
8 regions contain both urban and rural households. So `Rural` was not a function
of `v`, and the `.first()` collapse was not a dedup but an **arbitrary pick by
row order** — it stamped 7 regions "Urban" and `suroccidente` "Rural", leaving
**3,591 of 7,276 households (49.4%)** in a cluster whose `Rural` flag
contradicted their own. This is **class-1 silently WRONG**, not class-2 missing.

A co-defect: `cluster_features`' idxvars also carried `i: hogar`, which is not
part of the declared `(t, v)` index — the proximate reason the frame was
household-level for a cluster-level table.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/drops index levels; collapses duplicates via `groupby().first()` (or `sum` for `_ADDITIVE_MEASURE_COLUMNS`), warning "GH #323" | yes (indirectly) | **untouched** — see §6 |
| `_join_v_from_sample` | `lsms_library/country.py:2134` | joins `sample.v` into household-level tables at API time | yes | reuse (constrains the fix: `sample.v` and `cluster_features.v` must move together) |
| `df_data_grabber` | `lsms_library/local_tools.py:1018` | "Trickier" form `{newvar: (list_of_cols, fn)}` builds a composite key from several columns | yes | **reuse** — this is how the composite `v` is built |
| `benin.i()` | `countries/Benin/_/benin.py:6` | precedent: composite household id from a list value in `data_info.yml` | yes | **reuse the pattern** for `guatemala.v()` |
| `guatemala.individual_education(df)` | `countries/Guatemala/_/guatemala.py` | precedent: a `df_edit` hook named after a declared table | yes | **reuse the pattern** for `cluster_features(df)` |
| `Country.column_mapping` | `lsms_library/country.py:704` | binds a country-module function to a var when the names match and the var isn't a declared table | — | reuse (makes `v: [depto, mupio, sector, segmento]` work in both `sample.myvars` and `cluster_features.idxvars`) |

## §3 Definitions & conventions in force

- `sample` is the single source of truth for household→cluster mapping;
  `v` belongs in `cluster_features`' index and nowhere else — per `CLAUDE.md`
  §"`sample()` and Cluster Identity".
- `cluster_features` canonical index is `(t, v)` — `countries/Guatemala/_/data_scheme.yml`.
- Categorical columns from `.dta` come back as pandas categoricals; YAML mapping
  keys must be *string* keys when the raw labels are strings (`'urbana': Urban`)
  — `CLAUDE.md` §"Gotchas with Teeth". (`area` is exactly such a column.)
- `format_id` is auto-applied to `idxvars` but **not** to `myvars` — but the
  named-function branch in `column_mapping` takes precedence for a *list* value,
  so `guatemala.v()` runs in both positions.

## §4 Invariants & assumptions

- **`sample.v` and `cluster_features.v` must change together.**
  `_join_v_from_sample()` propagates `sample`'s `v` into every household-level
  table; a one-sided edit silently breaks the v-join. (`country.py:2134`)
- **A cluster-level column must be a function of `v`.** If it is not, collapsing
  to one row per `v` cannot be lossless — which is the whole #323 defect. Now
  *enforced*, not assumed: `guatemala.cluster_features()` raises if any payload
  column varies within `(t, v)`.
- **`upm` in `CONSUMO5.DTA` is float32-corrupted and must NOT be used** (§5).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| cluster key `v` | **new** (`guatemala.v()`), built on the existing Benin composite-key pattern | ENCOVI 2000 *does* have a PSU — see below. No existing helper composes it. |
| `cluster_features` dedup | **new** df_edit hook, guarded | The framework's generic `groupby().first()` is exactly what must not run silently here. |
| `strata` | **unchanged** (`region`) | Could not be determined; see §6. Not guessed. |

### The PSU: found, and the trap inside it

`CLAUDE.md` asserted *"Guatemala | No PSU/cluster variable in data"*, and the
independent diagnosis for this task concluded *"NO FINER PSU EXISTS"*, proposing
a `region × area` (16-cell) pseudo-cluster. **Both are wrong.** They checked
`HOGARES` / `ECV02H01` / `ECV03H01` / `ECV04H02` / `PERSONAS` / `ECV40P18`, which
carry only `region` + `area`.

`CONSUMO5.DTA` is household-level (7,276 rows; its `hogar` set is *identical* to
`ECV01H01`, so it joins 1:1) and carries `upm` — *Unidad Primaria de Muestreo* —
plus the full hierarchy `depto` / `mupio` / `sector` / `segmento` and `estrato`.

**But the raw `upm` column must not be used.** Stata stored it as a `float`
(IEEE single precision) and every value lies in ~1.0e8–2.2e9, far above float32's
exact-integer limit of 2^24 = 16,777,216. The float32 ULP there is 8–256, which
rounds away the low-order digits encoding `segmento`:

- 1,065 real geographic cells → only **847** distinct stored `upm` values;
- **201** `upm` values conflate ≥2 genuinely different PSUs (**2,128 households**);
- e.g. `segmento` 16 and 32 in sacatepequez/mupio 1/sector 9 both store as `301100928`.

Reading `upm` back would therefore silently *merge* clusters — a smaller instance
of the very bug being fixed. The faithful reconstruction is the composite
`depto-mupio-sector-segmento` (components are small ints that float32 holds
exactly). Verified on the 7,276-household frame:

| property | composite | raw `upm` |
|---|---|---|
| clusters | **1,065** | 847 (218 identities destroyed) |
| cells spanning >1 region | **0** | 0 |
| cells containing both urban+rural | **0** | 0 |
| cells with >1 distinct design weight | **0** | **17** |

The last row is the decisive independent check: in a two-stage design the design
weight is constant within a PSU. It is **exactly constant within all 1,065**
composite cells, and is *not* for the corrupt `upm`. The composite also strictly
refines `upm` (no composite cell spans two `upm` values). Median 6, max 16
households per cluster — the shape of a real enumeration area.

Re-pointing `sample` / `cluster_features` from `ECV01H01.DTA` to `CONSUMO5.DTA`
changes **no existing value**: the two files agree exactly on `hogar`, `factor`,
`region`, `area` (max abs diff 0.0). It only *adds* the geography.

## §6 Open questions for the human

- **`strata` is still `region`, and that is very likely incomplete.**
  `CONSUMO5.DTA` carries `estrato` (6 levels, constant within every PSU).
  Within-stratum design-weight dispersion (as a fraction of total weight
  variance): `region` 0.725 · `estrato` 0.946 · `region × area` 0.623 ·
  `region × estrato` **0.399** · the PSU itself 0.000. So `region × estrato` is
  clearly *closer* to the design stratum than `region` — but **no** candidate
  makes the weight constant within-stratum, so the true stratum cannot be
  identified from the data alone. Left as `region` and flagged in the YAML rather
  than replaced with an unverifiable guess (non-negotiable #4: do not guess
  quietly). Resolving it needs the ENCOVI 2000 sample-design documentation.
  `strata` is not part of the declared index and did not cause the #323 collapse.

- **The CLASS fix is deliberately NOT in this branch.** `_normalize_dataframe_index`
  should refuse to collapse a non-unique *declared* index with `.first()` when any
  non-index column is non-constant within a group (Guatemala is the textbook case:
  `Rural` varied within `v` in 8/8 groups). But that is shared framework code, and
  ~14 countries still carry the bug: making it raise today converts their silent
  wrongness into hard failures all at once. That must land as one coordinated
  change *after* the per-country cells are fixed, not unilaterally from a
  Guatemala worktree. Flagged here so it is not lost — closing the instance while
  leaving the class is how #323 got closed the first time.

---
### Phase 3 — verification

- `guatemala.v()` — **OK (anchored on §2/§5)**: reuses the Benin composite-key
  pattern via `df_data_grabber`'s "Trickier" form; no existing helper composed a
  PSU from geographic components.
- `guatemala.cluster_features()` — **OK (anchored on §4)**: does not reinvent the
  framework collapse — it *forbids* the unguarded one, raising if any payload
  column is non-constant within `(t, v)` instead of picking arbitrarily.
- `2000/_/data_info.yml` (`sample`, `cluster_features`) — **OK (anchored on §4)**:
  both `v`s changed together, as `_join_v_from_sample` requires; `i: hogar`
  removed from `cluster_features` idxvars.
- `_/data_scheme.yml` — **OK**: `Region` is no longer `optional` (it is now a real,
  well-defined column); stale "region is used as the v-index itself" note removed.
- `CLAUDE.md` — **CONTRADICTION corrected (§5)**: the "No PSU/cluster variable in
  data" row was false and is precisely why nobody looked in `CONSUMO5.DTA`.
