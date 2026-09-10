# Prior-Art Ledger — harvest-kg-shipped-factors (WORKPLAN Phase 2 prerequisite; #852 / #853 / #854)

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED) and this mirror carries no
`.gitnexus/` index; per `CLAUDE.md` §"Code intelligence: GitNexus is OPTIONAL"
the substitute for `gitnexus_impact` was a `rg` sweep of every reference to
`harvest_kg_factors` / `KG_FACTOR_LAYERS` / `KgFactorSource` /
`_screen_reported_factors` / `kg_factor_sources` across `*.py`, `*.org`,
`*.md`, `*.yml`.  Declared here rather than skipped.  Result: **no live code
outside `lsms_library/transformations.py` and `tests/test_crop_kg_factor.py`**
— every other hit is prose (`AGENTS.md`, two prior ledgers, `slurm_logs/`,
`Uganda/_/CONTENTS.org:991,1134,1234,1836`).  No docs page enumerates the
layer list, so adding a fifth layer breaks no rendered document; the Uganda
`CONTENTS.org` lines describe the *reported* screen and do not list layers.

## §1 Task, restated

Add the shared mechanism Phase 2 of
`slurm_logs/2026-09-09_epar_curation/WORKPLAN.org` needs before the Ethiopia
(#852 `Crop_CF_Wave{2..5}`, #853 area units) and Malawi (#854 IHS5 crop CFs)
loaders can land: a **shipped conversion-factor layer** in the
`harvest_kg` / `harvest_kg_factors` family in
`lsms_library/transformations.py`.

`harvest_kg` is an **analyst-callable methodology transform**, not a registered
or derived table: it is absent from `Country._FOOD_DERIVED` /
`_ROSTER_DERIVED` (`CLAUDE.md` §"Derived Tables") and from every country's
`data_scheme.yml`.  So this change adds no feature, moves no cache hash, and
touches no country config.  What it adds is one optional kwarg,
`shipped_factors=`, taking a DataFrame of externally shipped kg-per-unit
factors that the caller has already loaded and de-duplicated; the transform
joins it onto each `crop_production` row and serves it as a new layer ranked
**after `reported` and before `survey_median`**.

Explicitly NOT in scope: any country loader (that is #852/#853/#854), any
auto-discovery or registry (§6), Malawi's shelled/unshelled cross-condition
ratio (LEARNINGS L9 — a *different*, later layer), and plot **area**
conversion (#853 is an area axis, not a weight axis; it consumes no part of
this mechanism).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `harvest_kg_factors` | `transformations.py:1913` | resolves each row's kg-per-unit through the layer stack and discloses which layer served it | yes, `tests/test_crop_kg_factor.py` | **extend** (one new layer + kwarg) |
| `harvest_kg` | `transformations.py:2102` | `Quantity x kg_per_unit`, summed to `(t,i,plot,j)`; re-stashes the tallies after the `groupby` | yes | **extend** (kwarg passthrough) |
| `KG_FACTOR_LAYERS` | `transformations.py:1826` | `('reported','survey_median','inferred','none')`; the counts over these **partition** the frame and sum to `len(df)` | yes (`test_provenance_counts_sum_to_the_input_row_count`) | **extend** (insert `'shipped'` at rank 2) |
| `KgFactorSource` | `transformations.py:~1899` (column minted in `harvest_kg_factors`) | per-row layer provenance | yes | **extend** (new value `'shipped'`) |
| `_screen_reported_factors` | `transformations.py:1714` | the plausibility screen: kg-unit must weigh 1, nothing over `KG_FACTOR_MAX=250`, no integral calendar year on a non-kg unit; **counts, never clips** | yes (four tests) | **reuse verbatim** — the brief forbids a second screen, and the function is already layer-agnostic (it takes `(df, values)` and returns `(screened, rejected_mask)`) |
| `_survey_median_factors` | `transformations.py:1856` | median of *reported* factors within `(country, t, u, condition)` → `(country, t, u)`, gated at `min_reports`; `dropna=False`; sentinel-excluded on both sides | yes | **untouched** — shipped factors must never enter a median of *reported* values |
| `_level_or_column` | `transformations.py:1839` | reads a name from the index **or** the columns | yes (indirectly) | **reuse** — the join must not care which side of the frame a key lives on |
| `_valid_factor` | `transformations.py:1846` | NaN unless finite and `> 0` | yes | **reuse** for the shipped table's own values |
| `_unit_sentinel_mask` / `U_UNKNOWN` / `_U_SENTINELS` | `transformations.py:1789` / `:1774` / `:1786` | rows whose `u` records **no unit at all** (`'Unknown'`, `'Manquant'`, NaN) | yes (four tests) | **reuse** — see §4 |
| `_disagreement` | `transformations.py:2082` | `{both, disagree, share}` for a layer pair against the reported reference | yes | **reuse** for a third pair, `reported_vs_shipped` |
| `_get_kg_factors` / `_kg_factor_series` | `transformations.py:782` / `:1658` | the `inferred` layer (metric tokens, label parser, price-ratio) | yes | **untouched** |
| a join/merge helper for an external lookup table | — | **none exists.** `rg 'shipped\|conversion_factor\|conv_fact' lsms_library/*.py` returns nothing on the crop side; the only external-table reader is `Malawi/2019-20/_/food_acquired.py:17` (a wave script, food side) | — | so `new` is not reinvention |
| `median_price_valuation` | `transformations.py:2918` | the sibling analyst-callable transform; `weight_col=` landed the same day | yes | **precedent only** — same "optional kwarg, default path byte-identical" shape, same cache-probe recipe (`.coder/ledger/median-price-weight-col.md`) |

## §3 Definitions & conventions in force

- **`KgFactor` is REPORTED, NEVER CONSTRUCTED** — `lsms_library/data_info.yml:545-588`
  (`Columns.crop_production.KgFactor`, `optional: true`, `type: float`).  The
  note reads: "Only a number the survey (**or a survey-supplied conversion
  table**) actually wrote down belongs in this column.  Nothing inferred goes
  here … construction happens at read time".  **Reconciliation with this
  task** (the parenthetical is the live tension): a WB-shipped table like
  Ethiopia's `Crop_CF_Wave{2..5}` is keyed on `crop x unit x region`, not on
  the row — putting it in the stored column would require the loader to make
  the de-duplication decision (#852: crop 74 / unit 62, 4.34 vs 6.125) and the
  region join at **build** time, bake both into a parquet, and lose the
  ability to audit the table against the instrument's own factor.  So this
  design keeps it **out** of the column and serves it as a read-time layer,
  which is what the note's own last clause asks for.  See §6.
- **`KgFactor` is kg per ONE unit of the row's `u`** (`data_info.yml:584-588`:
  "CONSUMERS MUST NEVER TREAT THIS AS A QUANTITY … the only sanctioned use is
  `Quantity * KgFactor` on the SAME row").  `shipped_factors.KgFactor` carries
  the identical meaning, deliberately reusing the name.
- **Count, never clip** — `_screen_reported_factors` docstring
  (`transformations.py:1714-1740`): "A rejected value is NEVER clipped or
  rescaled — inventing a weight is the failure this whole design exists to
  avoid."  Also `AGENTS.md:353` and `.coder/ledger/857-crop-quantity-screen.md`
  for the same contract on `Quantity`.
- **Core never aggregates; analyst-callable transforms may** —
  `SkunkWorks/grain_aggregation_policy.org` §3a; `transformations.py:1580` ff.
  module header.  `harvest_kg` sits on the analyst side of that line, which is
  why an explicit kwarg is the right lever and a config entry is not (§6).
- **The missing-unit sentinel is not a unit** — `U_UNKNOWN` docstring
  (`transformations.py:1774-1788`) and `harvest_kg`'s own Notes
  (`:2190-2199`): "a median over them is a number about nothing, and filling
  other such rows with it would fabricate weights."
- **Pandas 3.0 targets** (`CLAUDE.md`): no `inplace=`, `pd.isna`/`pd.notna`,
  `.iloc[0]`, `groupby(..., dropna=False)` wherever an NA key must survive.
- **Derived tables are runtime-derived, not registered** — `CLAUDE.md`
  §"Derived Tables"; per STANDING.md §3.

## §4 Invariants & assumptions

- **Default byte-identical.**  `shipped_factors=None` must take the exact
  prior expressions.  Pinned structurally by
  `test_frame_without_kgfactor_reproduces_the_old_algorithm` /
  `test_screen_is_inert_without_a_kgfactor_column` (which compare against
  `_legacy_harvest_kg`) and empirically by
  `test_uganda_harvest_kg_baseline` (130 606 rows in, 36 095 out,
  `Harvest_kg` sum `10 868 272.24500081`, `reported 14 050 / survey_median 57 /
  inferred 28 147 / none 88 352 / reported_implausible 99`).  **Those numbers
  must not move.**
- **The layer counts PARTITION the frame** — `sum(counts[l] for l in
  KG_FACTOR_LAYERS) == len(df)`, pinned twice
  (`test_provenance_counts_sum_to_the_input_row_count`,
  `test_the_screen_count_rides_beside_a_partition_that_still_sums`).
  Consequence, and it is forced, not chosen: `'shipped'` joins
  `KG_FACTOR_LAYERS`, so `counts['shipped']` must be **present on every call**,
  `0` when no table is passed.  Three existing tests and the Uganda baseline
  assert the counts dict by **exact equality**, so each gains two
  zero-valued keys (`shipped`, `shipped_implausible`).  **No number moves.**
- **`_screen_reported_factors` reads the row's own `u` from `df`**, not from
  the factor table — so reusing it for the shipped layer screens a shipped
  factor against the unit it is being *applied to*.  That is the correct
  referent (a shipped factor of 300 on a `Kg` row is as wrong as a reported
  one).
- **`crop_production.j` is the DECODED crop label**, not the WB crop code
  (Uganda/Mali/Niger/Tanzania/Togo all run `j` through
  `categorical_mapping.org`).  A shipped table read straight from
  `Crop_CF_Wave4.dta` carries `crop_code` **74**, which will not match
  `'Enset'` and — after string normalisation — `74.0` will not match `'74'`
  either.  **The loader must decode its codes through the same mapping the
  wave uses before handing the table over.**  This transform will silently
  match nothing otherwise; the `shipped: 0` count is the tell.
- **`region` is spelled `region` here, lower case.**  `cluster_features` ships
  **`Region`** (capital R), so a caller resolving region onto
  `crop_production` must rename.  The transform matches exactly
  (`_level_or_column`) and does **not** accept both spellings — silently
  accepting either would hide a mis-keyed join.
- **`region` must be resolved by the CALLER.**  The transform never re-enters
  `sample()` or `cluster_features()`; `CLAUDE.md` §`sample()` makes the v-join
  a `_finalize_result` concern, and a transform that reached back into
  `Country` would couple an analyst-callable function to the config tree.
- **A duplicate key is REFUSED, never reduced.**  `groupby().first()` on an
  ambiguous factor table is exactly the grain collapse `CLAUDE.md`
  §"Grain Collapse" forbids ("Duplicates on a declared index mean the
  IDENTIFIER IS BROKEN … fix the index; do not declare a reducer").  #852's
  own "What it must NOT do" says the same: "Never average the duplicate row
  (crop 74 / unit 62, 4.34 vs 6.125)."  De-duplication is the loader's
  deliberate act.
- **The sentinel excludes the shipped layer too.**  A shipped table is a
  kg-*per-unit* table; a row that recorded no unit has nothing for it to be
  per.  A loader keying a factor on `u='Unknown'`, or a table with no `u` key
  at all, would otherwise fabricate weights for unit-less rows — the exact
  failure `U_UNKNOWN`'s docstring exists to prevent.  Applied **before** the
  screen, so `shipped_implausible` counts only rows that would otherwise have
  been served.
- **`pd.merge` matches null keys** (`CLAUDE.md` §Gotchas, `dfs:` cartesian
  bullet).  Join keys are normalised (`str.strip().str.lower()`, NA mapped to
  a private sentinel) on **both** sides **before** the duplicate check, so a
  casefold collision is *refused*, not silently resolved.
- **Attrs**: `groupby` drops `attrs`, which is why `harvest_kg` re-stashes the
  tallies (`transformations.py:2222-2225`).  The new keys ride on the same
  two dicts and need no new mechanism.  (`CLAUDE.md` §"Panel ID Transitive
  Chains and the `attrs` Flag`" for the general rule.)

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| plausibility screen for shipped factors | **reuse** `_screen_reported_factors` | the brief forbids a second screen; the function is already layer-agnostic and its referent (the row's `u`) is the right one |
| layer rank / provenance / counts | **extend** `KG_FACTOR_LAYERS`, `KgFactorSource`, `kg_factor_sources` | one machine-readable list already drives all three |
| layer-pair audit | **reuse** `_disagreement` for a new `reported_vs_shipped` pair | the question "does the WB table agree with what the enumerator wrote down?" is precisely #852's audit, and the helper already answers the analogous one for `inferred` |
| key resolution (index level vs column) | **reuse** `_level_or_column` | `u` is a level in Uganda and a column in Tanzania; `condition`/`region` vary too |
| value validity | **reuse** `_valid_factor` | 0 / negative / non-finite in a shipped table are discarded on the same rule as a report |
| unit-less rows | **reuse** `_unit_sentinel_mask` | §4 |
| the join itself | **new** (`_shipped_factor_lookup`) | nothing in the library joins an external factor table onto an item feature; a `pd.merge` would have to fight `crop_production`'s duplicate-label MultiIndex, so the lookup is a dict over normalised key tuples on a fresh positional axis |
| `country` as an allowed join key | **new, and beyond the brief's enumerated `(t, j, u, condition, region)`** | a `Feature('crop_production')` frame carries a `country` level, and a table loaded for one country would otherwise be applied to **every** country's rows.  Allowing `country` lets a loader fence its own table; it costs nothing when absent |
| de-duplication of an ambiguous table | **refused, not implemented** | §4; it is a per-country decision (#852 names Ethiopia's, whose answer — keep 4.34 — comes from W3 data, not from a rule) |
| shelled/unshelled cross-condition ratio | **out of scope** | LEARNINGS L9 / #854 "What it must NOT do"; a *later, separate* layer with its own N threshold and no crop allowlist |
| `yield_kg` passthrough | **not implemented** | scope; see §6 |

## §6 Open questions for the human

1. **Auto-registry: should `shipped_factors` ever be discovered rather than
   passed?**  This PR implements **option (1)** only.  The three options and
   the axis they trade on:

   | option | shape | what it buys | what it costs |
   |---|---|---|---|
   | **(1) explicit only** *(this PR)* | `harvest_kg(cp, shipped_factors=ethiopia.crop_conversion_factors(...))` | the de-dup rule, the region resolution and the wave scope are all visible at the call site; the transform stays a pure function of its arguments and stays out of the config tree | every caller must know the country has a table.  `yield_kg` calls `harvest_kg` internally, so **an analyst asking for yields gets no shipped layer at all** unless a passthrough is added.  A cross-country `Feature()` frame can only be served one country's table at a time |
   | **(2) `shipped_factors='auto'`** | dispatch to a per-country loader — either `Country.crop_conversion_factors()` registered in `data_scheme.yml`, or a `_SHIPPED_FACTOR_LOADERS = {'Ethiopia': …}` dict in `country.py` | one call works everywhere, including inside `yield_kg`; a `Feature()` frame can be served per-country by grouping on the `country` level | an analyst-callable transform re-enters `Country` (import cycle, cache re-entry, and the "core never aggregates" line gets blurry); a de-dup decision and a region join are hidden behind a default; per-country dispatch on a pooled frame is a real chunk of machinery |
   | **(3) declarative** | a `Shipped factors:` block in `lsms_library/data_info.yml` naming, per country, the loader and the join keys | uniform, greppable, and the join keys become schema rather than convention | `data_info.yml` is the **canonical cross-country schema**, not a build registry; and a `.yml` sibling *is* cache-relevant when it is `data_scheme.yml` (`CLAUDE.md` §population config placement) — the placement question alone is a design call |

   **@ligon decides.**  Recommendation from this seat: keep (1) until the two
   loaders (#852, #854) exist and we can see whether their call shapes
   actually rhyme; the `yield_kg` gap is the one concrete cost and it is one
   kwarg away from being fixed under (1) as well.
2. **Should `data_info.yml:555-557`'s parenthetical — "or a survey-supplied
   conversion table" — be tightened** to route table-sourced factors through
   the `shipped` layer and keep the stored `KgFactor` column strictly per-row?
   As written, a future loader could read it as licence to bake Ethiopia's
   `Crop_CF` into the column, which would make the de-dup and region decisions
   invisible in a parquet.  **Not edited in this PR** (it is a canonical-schema
   change with its own blast radius); flagged so the Ethiopia loader agent
   does not have to guess.
3. **`yield_kg(..., shipped_factors=)` passthrough** — deliberately not added
   here (scope).  Worth adding in the Ethiopia PR, where there will be a real
   table to test it with.

---
### Phase 3 — verification (measured)

- `_shipped_factor_lookup` (`lsms_library/transformations.py:1978`) —
  **OK (anchored on §2, §4, §5)**: no existing external-table join to
  duplicate (§2, row 10); duplicate keys refused rather than reduced (§4);
  keys normalised before the duplicate check so a casefold collision raises;
  NA keys carried on a private sentinel rather than through `pd.merge`'s
  null-matching (§4).
- `harvest_kg_factors(shipped_factors=…)` — **OK (anchored on §4)**: the
  `None` path takes the prior expressions.  Pinned by the legacy-comparison
  tests and by `test_uganda_harvest_kg_baseline`, which **ran** (not skipped
  — S3 credentials are auto-unlocked here) with every number unchanged:
  130 606 rows in, 36 095 out, sum `10 868 272.24500081`, `reported 14 050 /
  survey_median 57 / inferred 28 147 / none 88 352 / reported_implausible 99`.
  Two zero-valued keys added to the expected counts dict, as §4 requires.
- Screen reuse — **OK (anchored on §5)**: `_screen_reported_factors` is
  called, not copied.
- Layer rank — **OK (anchored on §1, §3)**: `reported` > `shipped` >
  `survey_median` > `inferred` > `none`.
- Cache impact — **OK (anchored on §1)**: measured base `9e4c0867` vs the
  code commit, `build_transforms_fingerprint` for 9 tables +
  `Country('Uganda')._table_cache_hash` for 4 — **0 of 13 values moved**.
  *Recipe correction for the next agent*: `.coder/ledger/median-price-weight-col.md`
  writes `_table_cache_hash(table)`; the real signature needs the wave list,
  `c._table_cache_hash(table, c.waves)`.
- Tests — 31 new in `tests/test_shipped_factors.py`, 35 in
  `tests/test_crop_kg_factor.py`, 66 passed.

### Found while building (belongs in §4; recorded here so it is dated)

- **pandas 3.0.2 normalises BOTH `None` and `pd.NA` to NaN** when an object
  ndarray becomes a DataFrame column.  Measured directly; not in `CLAUDE.md`'s
  Pandas-3.0 list.  Consequence: `kg_shipped_source` must be read with
  `pd.isna`, never `is None` — the first version of its test asserted
  `is None` and failed.
- **The duplicate refusal is STRICTER than the core grain-collapse rule.**
  Core stays silent on a *lossless* de-dup (identical rows); this refuses
  those too.  Deliberate: a factor table is a small curated artefact, and a
  loader that has not looked at its own duplicates has not looked at its
  table.  A raw WB file with repeated rows will trip it —
  `drop_duplicates()` in the loader is the expected answer.
- **The `j`-vocabulary trap generalises to every key.**  Keys are compared as
  stripped, lower-cased text, so any type or vocabulary mismatch silently
  matches nothing: a WB crop code `74` against a decoded `'Enset'`, `74.0`
  against `'74'`, a `region` code `1` against `1.0`, `t` as an int against
  `'2019-20'`.

## §7 Red-team round (2026-09-09) — what changed

`slurm_logs/2026-09-09_epar_curation/REDTEAM_P2_shipped_factors.org` graded
the branch **mergeable** with three CONCERNs on join semantics and a
docstring FAIL.  All were taken on this branch rather than deferred, at the
coordinator's direction.  What moved:

1. **`shipped_matched`, and a warning on a zero match.**  The red-team's
   headline finding was that the layer can serve 0 rows with **no signal**,
   *and* that the docstring's proposed tell was wrong in the false-alarm
   direction — `counts['shipped']` is **post-rank** and reads 0 for a
   perfectly-keyed table that `reported` outranks on every row (probe
   `[P1g]`).  `shipped_matched` is therefore taken **pre-sentinel,
   pre-screen, pre-rank**: rows the table supplied a finite factor for.  It
   is the one count that separates *mis-keyed* from *outranked* from
   *sentinel-excluded* (which previously shared one signature — all `none`,
   all counts 0).  A non-empty table matching nothing now raises
   `ShippedFactorWarning`.  **The message distinguishes two causes**: a
   keying mismatch, versus a table whose `KgFactor` column carries no usable
   value at all — the red-team's `[E]` case, a `.dta` conversion column read
   back as text, which would otherwise have been sent to the wrong end of the
   file.
2. **`u` is enforced, not merely requested.**  The "nothing joinable" error
   already claimed "at minimum the unit `'u'`" and nothing checked it (probe
   `[P1a]`: a `j`-only table served 50 kg/unit to a `basket`).  Rule chosen
   over text-softening, per the coordinator: *a kg-per-unit factor without a
   unit is not a factor.*  Note the resulting **guard order**, pinned by test:
   the `u` rule fires before "nothing to join on", and a frame with no `u` at
   all raises even earlier in `_kg_factor_series` — so the "nothing to join
   on" branch is unreachable through `harvest_kg_factors` and is exercised
   directly as the defence-in-depth it is.
3. **A dropped key level now WARNS instead of going national in silence.**
   The asymmetry the red-team named: a *multi*-region table raises (the
   surviving keys are ambiguous) while a *single*-region table applied
   silently — so the more careless loader got the weaker signal.  It warns
   and still **joins**; it does not raise, because the Ethiopia region case
   legitimately needs this until `cluster_features` carries `region`.
4. **Docstring accuracy**: "four layers" → five (two sites), the
   `_screen_reported_factors` fall-through list gained `shipped`, `§` →
   "section" (the one non-ASCII character in the diff), the sentinel comment
   no longer overstates what `shipped_implausible` counts, and `harvest_kg`'s
   "bit for bit" is narrowed to the `Harvest_kg` values (the counts dict and
   the companion frame do gain keys/columns).
5. **`data_info.yml` KgFactor note tightened.**  The red-team's item 9 read
   the old parenthetical — "the survey (or a survey-supplied conversion
   table)" — plainly and concluded it **licenses** baking Ethiopia's
   `Crop_CF` into the stored column: a table lookup is transcription, not
   inference, so the note's prohibition did not reach it.  §6 Q2 is therefore
   **closed, not deferred**: the note now says a shipped table is applied at
   read time through `harvest_kg(shipped_factors=...)` and never written to
   the column, and says why (the de-dup rule and the region join would become
   invisible in a parquet, and `reported_vs_shipped` would be impossible
   because the shipped value would have *become* the reported one).

**Ledger correction the red-team earned.**  §4 credited the sentinel with
preventing "a table with no `u` key at all [fabricating] weights for
unit-less rows".  On the Tanzania shape (no `u` anywhere) that protection is
actually a **pre-existing** guard in `_kg_factor_series`, not the sentinel;
and on a `u`-bearing frame the case is now refused outright by rule 2 above.
The sentinel's real job is narrower and still load-bearing: a row whose `u`
*is* the recorded-nothing sentinel.

**Not taken here, by direction** (red-team items 6 and the `<=0` note):
`yield_kg(..., shipped_factors=, min_reports=)` pass-through stays for the
Ethiopia PR — it is two lines and wants a real table to test against; and a
shipped `0`/`-3` is discarded by `_valid_factor` before the screen, so it is
not counted `shipped_implausible`.  That is exactly what the `reported` layer
does, so changing it would mean changing `reported` too.

---
### Phase 3 — re-verified after the red-team round

- Tests: `tests/test_shipped_factors.py` **37** + `tests/test_crop_kg_factor.py`
  **35** = **72 passed**.  `test_uganda_harvest_kg_baseline` **RAN** (14 s,
  rebuilding L2-country from the wave parquets — the shared `Uganda/var/` was
  cleared by a concurrent agent, which is why the red-team had to deselect
  it) and every number is unchanged: 130 606 / 36 095 /
  `10 868 272.24500081` / `reported 14 050, survey_median 57, inferred
  28 147, none 88 352, reported_implausible 99`.  The counts dict now also
  carries `shipped: 0`, `shipped_implausible: 0`, `shipped_matched: 0`.
- Adjacent: `test_schema_consistency` + `test_api_discoverability` +
  `test_build_transform_hash` + `test_median_price_valuation` — **364 passed**
  (these read `data_info.yml`, so they cover the note edit).
- Cache: base `9e4c0867` vs final head, `build_transforms_fingerprint` for 9
  tables + `build_transforms_fingerprint(None)` +
  `Country('Uganda')._table_cache_hash(t, c.waves)` for 4 — **0 of 14 moved**.
- **Does `lsms_library/data_info.yml` enter any cache hash?  NO — measured.**
  A perturbed copy of the file (one appended comment) under an otherwise
  identical package tree moved **0 of 14**.  The reason is that
  `Wave._input_hash` hashes the **wave's** `data_info.yml`
  (`country.py:859`, `wave_dir / "data_info.yml"`), never the canonical
  cross-country schema file; and `_BUILD_INPUT_SUFFIXES` (`country.py:646`)
  does not include `.yml` at all.  So the note edit is free, and it ships in
  the same commit.
