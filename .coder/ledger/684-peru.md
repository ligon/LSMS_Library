# Prior-Art Ledger — GH #684 (Peru: make the country functional)

**Search tier used:** ripgrep + git + direct measurement against the raw `.DTA`
(L1 DVC blobs) + full-text extraction of the four shipped 1994 PDFs.  Every
number below was reproduced independently before any edit; none is inherited
from the task brief on trust — and two of the brief's framings were corrected
by measurement (see §6).

## §1 Task, restated

`Peru/_/` held a country-level **`data_info.yml`**; `Country.resources` reads
**`data_scheme.yml`**.  Peru was the only country in the corpus spelling it that
way, so `Country('Peru').data_scheme == []`, every table was unreachable, and
Peru contributed **zero cells** to the coverage matrix.  The rename alone turns
CI red in two places, so the task is: rename, add the composite-`i` formatter
the config always needed, establish the cluster key from the survey
documentation, and validate every mapping against source — because *this config
has never executed, so nothing in it has been validated* (the GH #676 lesson).

## §2 Existing machinery (this task's area)

| symbol | path | what it does | tested? | reuse / extend / new |
|--------|------|--------------|---------|----------------------|
| composite idxvar formatter | `country.py: map_formatting_function`; `local_tools.py: df_data_grabber.grabber` | a named formatting function bound to a *list* idxvar is applied row-wise, receiving a Series | yes (Guyana `i()`, Benin, Senegal) | **reuse** — `mapping.py:i()` hyphen-joins the three parts |
| `format_id(..., zeropadding=n)` | `local_tools.py` | canonical string id; preserves leading zeros, passes hyphenated strings through unchanged | yes | **reuse** |
| `df_edit` hook dispatch | `country.py: column_mapping` / `get_data` | a module-level function named after a declared `data_scheme` table runs on the grabbed-and-indexed frame *before* `_normalize_dataframe_index` | yes (Guyana, Albania, Ethiopia, China) | **reuse** — the sanctioned place to resolve duplicates explicitly |
| `code_label_map` | `local_tools.py` | `Code -> Label` from a country `categorical_mapping.org`, keyed by **both** the string and int form of each numeric code | yes | **reuse** — it exists precisely because the naive reader silently returns `{}` (GhanaLSS #372/#377/#348) |
| `_apply_categorical_mappings` + `_augment_numeric_code_keys` | `country.py` | API-time by-name decode of a column against a same-named org table; adds `'1'`/`'1.0'` variants of integer keys | yes | **partial reuse** — it does *not* generate `'01'`, and it runs after `_expand_kinship`; see §4 |
| `_expand_kinship` / `kinship.yml` | `country.py`, `categorical_mapping/kinship.yml` | `Relationship` → Generation/Distance/Affinity; tries `.str.title()` first | yes | **extend** — ten Peruvian PARENTESCO labels added |
| `roster_to_characteristics` | `transformations.py` | resolves `MonthsSpent` / `MonthsAway` / `WeeksAway` to months-present and filters | yes | **reuse untouched** |
| `_normalize_dataframe_index` | `country.py` | collapses a non-unique declared index with `groupby().first()` | yes | **do not touch** — the fix is config + hook, per the no-aggregation-in-core policy |

## §3 Definitions & conventions in force

Cited, not duplicated:

- `CLAUDE.md` §"Grain Collapse": duplicates on a declared index mean the
  identifier is broken or a level is missing — fix the index, never declare a
  reducer in core.  A *declared* reduction inside a country's own `df_edit`
  hook is the sanctioned escape (Guyana `cluster_features`).
- `CLAUDE.md` §"`sample()` and Cluster Identity": only `cluster_features`
  declares `v`; every other table gets it joined at API time **iff** the country
  has a `sample` table.  Peru has none, so `household_roster` is `(t, i, pid)`.
- `lsms_library/data_info.yml` `Columns.cluster_features`: `Region` and `Rural`
  required; `Rural`'s canonical vocabulary is `Rural`/`Urban`/`Informal` with
  `Urbano` an accepted variant (GH #602).
- `lsms_library/data_info.yml` `Columns.household_roster`: Generation /
  Distance / Affinity are `api_derived` but still `required`, so they must be
  *declared* even though nothing extracts them (`tests/test_schema_consistency`
  checks the declaration).
- `.coder/coverage/` tiering: `absent` = "not declared for this wave", an
  un-adjudicated gap; a country that declares nothing is *invisible*, which is
  the defect this issue reports.

## §4 Invariants & assumptions (all measured 2026-08-21 unless noted)

- **The survey's own household key is `(SEGMENTO, VIVIENDA, HOGAR)`** —
  `dicciona.pdf` makes those three, plus `RECTYPE`, the COMMON items of *every*
  record type.  Exactly unique in REG01: 3,623 rows, 3,623 groups, 0 duplicates.
- **`HOGAR` is a fraction, not a serial**: `m-encues.pdf` 7.2.3 says numerator =
  household's serial in the dwelling, denominator = number of households in it.
  Confirmed: values are exactly `{11,12,13,14,22,23,33}`, numerator never
  exceeds denominator, and the declared denominator matches the observed
  household count in **3,622 of 3,623** rows.
- **`v = segmento`, NOT a composite.**  `m-encues.pdf` 7.2.3 separates
  "Recuadro B: UBICACION MUESTRAL — Segmento Nº, Vivienda Nº" from
  "Recuadro A: UBICACION GEOGRAFICA — Departamento, Provincia, Distrito y Centro
  Poblado".  357 of 364 segmentos map to one department; the 7 `a01` and 10
  `a06` exceptions are a **single** minority household each (two in segmento
  125), with the rest of the geography identical and the departments
  non-adjacent.  **Zero mode ties** across all 364 segments.
- Corroboration: `(segmento, vivienda)` = **3,543 dwellings** against a
  documented plan of **3,544**; the community questionnaire `CCPP0.DTA` covers
  **111 segments = exactly the 111 rural ones**; its `departa` agrees with
  REG01's modal `a01` in **203/204** records.
- **The Guatemala float32 trap does not apply.**  REG01/REG02 ids are int16 /
  int8 and every geographic code is a string; no float column exceeds 99.
  `CCPP0.DTA` is float32 but tops out at 662.  (1985 / 1991 are float32
  throughout; their maxima are 362,931 and 526,192, both under 2^24.)
- **`a01` arrives zero-padded (`'06'`) while the org reader parses `Code` as
  int64.**  Neither the explicit `mappings:` list form nor
  `_augment_numeric_code_keys` bridges that: both would silently no-op.
- **`_expand_kinship` runs BEFORE `_apply_categorical_mappings`**, so an
  API-time `Relationship` decode arrives too late — measured: Generation /
  Distance / Affinity came back 100% null with an "unknown relationship labels
  `['0'..'9']`" warning until the decode moved to grab time.
- **`diagnostics._check_declared_spellings` grades the cached parquet**, not the
  API output, so a `Rural` decoded only at API time fails it.  Both decodes are
  therefore build-time.
- **One corrupt person id**: `(81, 2, 11)` numbers three people 1, 3, 3.
  REG05 (`e00`), REG07 (`g00`) and REG08 (`h00`) all number that household
  1, 2, 3, and REG05 marks its code-3 person as in school (the 15-year-old).
- **`b03` = 99 is non-response, not an age**: all six such people also have
  `b04a` = 99 (the dictionary's explicit sentinel), four are one household's
  un-enumerated "otro pariente" members, and ages 96/97/98 have 1/1/2 people.
  *This is a reading, not a statement of the source* — `dicciona.pdf` prints
  B03 as the plain range 00:99.
- **`b08` is months-absent**: reconstructing `m-encues.pdf`'s Pregunta Nº 9
  membership rule from `b08` and `b01` reproduces the survey's own `b09` flag
  for **19,098 of 19,285 people (99.03%)**.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| household id `i` | **reuse** the composite-idxvar formatter | Guyana's shape exactly; only the widths are Peru's |
| cluster id `v` | **no change** — `segmento` as declared | the documentation and the measurement both say the segment is the sample unit; a composite would split 7 real clusters |
| `cluster_features` reducer | **new hook**, mode with tie→NA | `first()` lets row order return the mis-keyed value; mode is unambiguous in all 364 segments |
| Region / Rural decode | **reuse** `code_label_map`, at build time | one source of truth (the org tables), dual keys, and it raises rather than returning `{}` |
| Sex / Relationship decode | **reuse** the YAML `mappings: [table, key, value]` form | same org tables; grab-time so kinship sees labels |
| the duplicate pid | **new hook**, repair-by-row-order with a checked precondition, else Guyana-style drop | three sibling modules make it reconcilable, and dropping would orphan two people's records in four modules |
| `MonthsAway` | **reuse** the framework residence filter | b08 is a documented months-absent variable; the filter is *not* ENNIV's rule and that gap is written down |
| `sample` table | **not this PR** | no weight variable found in REG01, sample documented self-weighting; wiring it is the natural follow-up |

## §6 Open questions for the human

- **The brief expected a composite cluster key, on the Guatemala precedent.  I
  did not build one**, because the discriminating fact differs: Guatemala's
  `segmento` was a *local* serial reused across departments, Peru's is a
  national serial with 357/364 departments unique.  A composite would have made
  the index unique by splitting seven real clusters on transcription typos —
  uniqueness without correctness, which is what the brief itself warned against.
- **The department labels are not provable from anything this repository
  ships.**  No Peru document contains a code list; the cited source
  (`DICCIONA.ASC`) is not here.  The list is the standard INEI alphabetical
  ubigeo ordering, and four internal checks are consistent with it, but a swap
  between two similar departments would survive all four.
- **`b03` = 99 → NA contradicts the dictionary read literally.**  The empirical
  case is strong; the tension is recorded rather than resolved.
- **`_/categorical_mapping.org`'s `LENGUA` table is wrong for 1994** — seven
  codes where `dicciona.pdf` declares nine, shifted from code 4 (it omits CAMPA
  and SHIPIBO).  Nothing reads it; left in place with a warning rather than
  silently "fixed", since its provenance is the missing 1991 dictionary.
- **`blessed.csv` was deliberately left empty.**  Every number here was checked
  against source by an agent, not read by a human, and an agent-written blessing
  would make `blessed` a synonym for `sane`.

---

## §7 Verification and landing (second agent, 2026-08-21)

The agent that wrote §1--§6 was interrupted before committing.  Its output was
rescued into commit `22008332` **verbatim, unreviewed**.  This section records
what a second agent actually verified, and is the only part of this file
written after the work was executed rather than alongside it.

### §7.1 The rescued measurements were read out of a WARM CACHE

The first thing checked, and the thing that nearly went wrong.  The L2 parquets
at `~/.local/share/lsms_library/Peru/` were stamped **19:22**, while the rescue
commit is **20:46** -- so the tables "built" on first re-run were the dead
agent's own cached output, not a build of the committed config.  A verification
run against them would have proved only that the parquet existed.

This is the GH #323 shape exactly: *the guard sits downstream of the thing that
fails.*  Cleared with `lsms-library cache clear --country Peru` (L2 only; L1 DVC
blobs untouched) and rebuilt cold.  **Every number below is from the cold
build**, and every one reproduced §4's claims.

### §7.2 What builds, cold

`Country('Peru').waves` -> `['1985', '1990', '1991', '1994']`;
`.data_scheme` -> 9 tables (5 declared + 4 auto-derived).

| table | result |
|---|---|
| `cluster_features` | 364 x 2, index `(t, v)`, unique; Region 25 labels 0 null; Rural {Urban 253, Rural 111} |
| `household_roster` | 19,285 x 7, index `(i, t, pid)`, unique; Sex {F 9883, M 9402}; Age 0--98, 6 null; Relationship 10 labels; MonthsAway 0--12, 3 null; Generation/Distance/Affinity 884 null each (see §8.2 -- this said "**0 null**" before review) |
| `household_characteristics` | 3,621 x 15, index `(t, i)`, unique |
| `individual_education`, `food_acquired`, `interview_date`, `food_expenditures`, `food_prices`, `food_quantities` | clean `RuntimeError` ("no wave-level build succeeded") -- declared, no wave implements them |

The six raising tables are the *intended* design, not a defect: they are
declared so the coverage matrix can grade them `absent` (the work queue) rather
than not see them at all.  Precedent confirmed -- `.coder/coverage/latest.csv`
already holds **21** country/feature pairs that are `absent` in every wave
(Ethiopia `nutrition`, Guyana `assets`, Nigeria `anthropometry`, all of Nepal).

Zero `GrainCollapseWarning` anywhere.  The `cluster_features` MODE hook and the
`household_roster` pid repair both fire as designed; the repair warns exactly
once, for `1994/081-02-11 (3->2)` -- the household §4 documents.

Framework-ordering check: the roster index comes back `['i','t','pid']` against
a declared `(t, i, pid)`.  That is the framework, not Peru -- Guyana and
Tajikistan both return `['i','t','v','pid']` from the same declaration.  Peru
correctly lacks `v` (no `sample` table).

### §7.3 Verdict on the two canonical files -- both KEPT

- **`lsms_library/data_info.yml` (Currency)** -- **required by the rename, not
  optional.**  `catalog._country_dirs()` gates on the presence of
  `_/data_scheme.yml`, so before the rename Peru was invisible to
  `test_every_country_has_currency`, which parametrizes over it.  The rename
  makes that test newly collectible for Peru, and it would fail without this
  entry.  The codes check out: the inti (PEI) ran Feb 1985 -> 1 Jul 1991 and the
  nuevo sol (PEN) from 1 Jul 1991, so 1985/1990 -> PEI and 1991 (fielded
  Sep--Nov) / 1994 -> PEN.  Schema matches the documented
  `{default, overrides}` form used by GhanaLSS / Tajikistan / Azerbaijan.
- **`categorical_mapping/kinship.yml`** -- additive, 9 new keys, no collision
  with the 483 existing ones (`Otro Pariente` deliberately not re-added; it
  already exists from Guatemala ENCOVI with the same tuple).  Proven load-
  bearing: every one of the ten PARENTESCO labels resolves, where before the
  addition nine of ten did not.  **This section originally cited "0 null over
  all 19,285 people" as evidence of correctness. That was wrong reasoning and
  is retracted -- see §8.2.**

One residual, **not** changed here: `Otro Pariente` -> `[0, 0, consanguineal]`
is Guatemala's pre-existing tuple, and it makes Peru's `Distance` identically 0
for all 19,285 people (884 of them "otro pariente", a category that is by
definition *not* lineal).  Editing a canonical entry that another country
depends on does not belong in a Peru PR; recorded for a kinship-specific issue.

### §7.4 Other rescued files -- all correct

`Age: float` replaces the old `Age: int`: the canonical schema declares
`Age: {type: float, required: true}`, so this is a correction (and it matters --
`age_handler` returns fractional years when DOB is present).  Dropping the
stale `Waves:` key is right (it named `1985-1986` against a `1985/` directory).
Dropping the country's `Index Info:` block is right (only the canonical
`data_info.yml` `Index Info` is read).  The `categorical_mapping.org` rework --
renaming `DPTO`->`Region` and `AREA`->`Rural` so the by-name auto-dispatch can
fire -- is confirmed working: labels appear in the built table, and `Urbano`
harmonises to canonical `Urban` at API time.

`LENGUA` is left knowingly wrong-for-1994 with a warning, as §6 says.  Accepted:
nothing extracts B06, the table name matches no canonical column so the
auto-dispatch cannot fire on it, and the note carries `dicciona.pdf`'s correct
nine codes for whoever wires it.

### §7.5 A guard was added, and proven to fail on the defect

`tests/test_schema_consistency.py::TestCountryConfigIsReachable` (data-free,
2.1 s, 38 cases):

- `test_no_country_level_data_info_yml` -- a **country**-level `_/data_info.yml`
  is never read and is always this bug.  After the rename **no country in the
  corpus has one**, so the invariant is clean.  Verified falsifiable: restoring
  `fd9efc1b`'s `Peru/_/data_info.yml` makes it fail with
  `AssertionError: ['Peru']`.
- `test_data_scheme_declares_at_least_one_table` -- an empty `Data Scheme:`
  makes a country invisible to the coverage matrix.

This is the guard the issue asked for, in its precise form.  Note the corpus
already had `coverage.unconfigured_countries()`, which reports a country dir
holding microdata with **no** `_/data_scheme.yml` -- and its unit test uses
*Peru* as the fixture.  It did not catch this because Peru's config was not
missing; it was misfiled, which is a different failure.

### §7.6 Tests

`tests/test_schema_consistency.py tests/test_currency.py
tests/test_table_structure.py` -> **2,575 passed, 2 failed, 4 skipped,
5 xfailed** (7m38s).  Both failures are
`CotedIvoire/cluster_features` (missing Latitude/Longitude) and are
**pre-existing** -- reproduced on the unmodified main checkout at `553ed2b0`,
and already filed as GH #592.  Nothing Peru touches can reach them.

The two cells the issue reported red are green:
`test_required_columns_present[Peru:household_roster]` and
`test_feature_is_sane[Peru/cluster_features]`, plus
`test_feature_is_sane[Peru/household_roster]` and
`test_every_country_has_currency[Peru]`.

**Not run:** the full suite, and `make matrix` (Peru's cells are predicted
`sane`/`builds` for 1994 and `absent` elsewhere, but that prediction is not
measured here).

### §7.7 An adjacent finding, deliberately NOT fixed here

**Afghanistan is invisible too, by a different route.**
`countries/Afghanistan/_/` holds `afghanistan.py`, `CONTENTS.org` and a
`Makefile` but **no `Data Scheme:` block in any file** -- so it is not the #684
defect (a registry in the wrong filename) but an unstarted country. It ships two
wave dirs (`2016-17/`, `2020/`) and a `var/`. It is out of scope for this PR and
is left for its own issue; the new guard does not fire on it, correctly, since
there is no misfiled registry to rename.

### §7.8 Still not blessed

`blessed.csv` remains untouched, per §6 and the repo rule: every number here was
checked against source by an agent, and an agent-written blessing would make
`blessed` a synonym for `sane`.

---

## §8 Red-team round (2026-08-22)

An independent red-team returned **FIX-THEN-MERGE** on PR #691
(`PATCHREVIEW_691.org`), using a blind ground-truth derivation
(`GROUNDTRUTH_684.org`) as an oracle.  It reproduced every measurement in §7
cold, confirmed the build sound, and decoded the three ENNIV PDFs (they are
LZW-compressed text, which is why every reader here had failed on them) to
check the documentary transcriptions verbatim.  All of them held.

What follows is what **changed** as a result, and — importantly — the three
places where my own measurement **contradicted what the review told me**.

### §8.1 F1 — the 1990 wave: I was wrong, and the review was partly wrong too

§7 / `CONTENTS.org` asserted the 1990 `.SSP` files were SPSS/PC+, that
`pyreadstat` could not read them, and that the wave needed **re-acquisition**.
All three false.  They are **SAS Transport (XPORT) V5** (SAS 6.06/6.07,
1992–93), on disk today.  This was the worst defect in the PR: an unevidenced
negative in `CONTENTS.org` prescribing months of work, which is precisely the
Albania shape `CLAUDE.md` warns about.  Corrected in full, as a visible
correction rather than a silent overwrite.

**But the review's prescribed remedy is itself a trap, and I caught it by
measuring instead of adopting.**  The review (and the coordinator relaying it)
said to use `pyreadstat.read_xport`.  Measured, both readers on the same bytes:

| file | cols | non-empty via `pyreadstat` | non-empty via `pandas` |
|---|---|---|---|
| `N00A.SSP` | 10 | 3 | 10 |
| `EXPEND.SSP` | 5 | 3 | 5 |
| `PANEL.SSP` | 4 | 2 | 4 |
| **total** | **19** | **8** | **19** |

`pyreadstat.read_xport` returns **100% NaN for 11 of 19 columns and raises
nothing**.  In `EXPEND.SSP` the loss alternates exactly (`HID` ok, `HHSIZE`
NaN, `PCFDEXP` ok, `WT` NaN, `TOTPCX` ok) — a field-width misalignment, not
empty data.  `pandas.read_sas(blob, format='xport')` reads all 19 from the
bare blob with no `.xpt` rename.  **Anyone following the review's advice would
ship 1990 with half its columns silently empty.**

Two consequences the review could not have reached:

- **`WT` is real** — 1,509 values, 0.5–1.0, mean 0.854, essentially
  two-valued, which is what the documented two-stratum 1990 design predicts
  (the 1,280-dwelling Lima panel plus the 260-dwelling urbano-marginal
  *ampliación*).  I was told `WT` was "apparently the only real sampling
  weight in the Peru series"; via `read_xport` it reads as **entirely NaN**,
  so that claim was true in substance but unsupported by the method offered
  for it.  It is now supported.
- **`PID85`/`PID90` are real** (3,326 rows each), which **resolves
  `GROUNDTRUTH_684.org` open Gap #2** ("genuinely empty variable, or an
  XPORT-reader artefact").  It is the artefact.  1990 has a usable person key
  and is panel-linkable to 1985.

Filed as **GH #699** with the reproduction.  Reader implemented: **no** — out
of scope for this PR, as instructed.

### §8.2 N1 — `Otro Pariente`, and the retraction of my own evidence

`m-encues.pdf` "Pregunta Nº 1" defines B01 code 7 as *"hermano, tío, primo,
consuegro, bisnieto, abuelo, cuñado, bisabuelo, sobrino"* — spanning
Generation −3..+3, Distance 0..2 and both affinities.  `[0, 0,
consanguineal]` is therefore **refuted by a document this repo ships**, not
merely uninformative.

Now `[null, null, null]`.  Verified this needs **no framework change**
(`_load_kinship_map` does `tuple(vals)`; `_component` returns `x[i]` for any
tuple; `pd.array(..., Int64Dtype())` maps `None` → `pd.NA`) and that **no
"unknown relationship labels" warning fires**, because the key still matches —
which is the point: removing the entry instead would produce a recurring
warning whose own text invites the next reader to supply a fresh guess.

Blast radius measured before and after, cold, on the shared key:

| | rows | `Otro Pariente` | Gen/Dist/Affinity nulls before → after |
|---|---|---|---|
| Peru | 19,285 | 884 (4.58%) | 0 → 884 |
| Guatemala | 37,771 | 596 (1.58%) | 0 → 596 |

No other value in either country changed; Guatemala's `Distance` still spans
{0, 1}.  `test_feature_is_sane[Guatemala/household_roster]` passes.  This is a
correctness gain for Guatemala too — the tuple was equally unjustified there —
so it is not a degradation and I did not stop.

**Retraction.**  §7 asserted "Generation/Distance/Affinity 0 null over all
19,285 people" three times *as evidence the mapping was correct*.  That was
bad reasoning: zero-null is a **coverage** measure, not a correctness one, and
it was asserted alongside the separate observation that `Distance` was
constant — which should have been the tell.  The mapping produced a non-null
value for 884 people by asserting something the source denies.  Corrected
throughout.

Half the degeneracy is *not* the mapping's fault and is worth keeping
straight: ENNIV's ten codes contain **no sibling or cousin category at all**,
so Peru's `Distance` would be degenerate under any mapping.  Filed as
**GH #698**, which also covers the canonical file's self-inconsistency
(`Other relative of head or spouse` = `[0,0,consanguineal]` at kinship.yml:282
vs `Other relative of head` = `[0,2,consanguineal]` at :631).

### §8.3 F2, N2, N3, N4/N5, and the Region admission

- **F2** — "most of the file" was wrong.  Re-measured independently, matching
  the review exactly: **60/364 segments (16.5%), 720/3,623 households (19.9%),
  3,862/19,285 people (20.0%)**.  Corrected.
- **N2** — the MODE hook reported nothing at build time; its counts lived in a
  docstring measured once.  It now warns on **every** build with the projected
  row count, the number of disagreeing (column, segment) pairs and the number
  of overruled values, plus the 2026-08-21 expectation, so drift is loud.
  Fires with exactly **17 pairs / 18 values**.  This matters because moving
  the collapse into a `df_edit` hook also moved it *above* the framework's
  own grain auditor.
- **N3 — `LENGUA` is FIXED, not merely flagged.**  The review confirmed the
  nine-code list from `dicciona.pdf`, so shipping a knowingly-wrong table was
  no longer defensible (the GH #676 bet).  The frequency distribution settles
  it independently: `b06` takes exactly 1..9, and 689 people sit at code 9
  (`No habla` — infants) against 2 at code 7 (`Ingles`).  The old table had no
  code 8 or 9 at all, silently dropping 706 people, and labelled the two
  English speakers "No habla".  Also **added `EST-CIVIL` code 9** — measured,
  `b05` takes {1..6, 9}, and the table omitted 9.
- **N4/N5** — recorded in the config comments where the next agent will hit
  them, each re-measured here: `individual_education (t,i,pid)` has **26**
  duplicate tuples (not additive → would land on `groupby().first()`);
  `food_acquired (t,i,j,s)` has **87** (benign, additive SUM); `ak03` reaches
  **99999.99** against a declared max of 1000 and the scrubber only fires
  above 1e99; `u` and `v` are dropped from the canonical `food_acquired`
  index, which matters to `Feature()`.
- **Region** — the review judged my "not provable" admission *pessimistic*,
  and it was right.  The INEI *ubigeo* ordering is externally documented and
  the shipped table is **strictly alphabetical** (verified programmatically,
  0 violations), so a pairwise swap would have to break alphabetical order,
  which is visible on inspection; check 1 (Callao at 07, 85/85 urban) pins the
  offset.  What survives is only a systematic re-lettering, which the four
  data checks would catch.  Softened accordingly — an under-claim wastes the
  next reader's time exactly as an over-claim does.

Withdrawn by the review and correctly **not** changed: the Pregunta 8-vs-9
citation.  Both are right; the "pregunta 9" inside the quoted passage is a
typo *in the manual*, reproduced verbatim as a quotation should be.

Withdrawn by the coordinator: the claim that the declared-but-unwired tables
are stale Nepal-template residue.  The review measured all three as
satisfiable from real 1994 columns.

### §8.4 Issues filed (the §7 promise, now kept)

The review's sharpest bookkeeping point: "a deferral with no issue is a
deletion."  §7 promised five follow-ups and filed none.  All five now exist:

| # | subject |
|---|---|
| **#698** | `Otro Pariente` undetermined; corpus-wide "other relative" policy |
| **#699** | `local_tools.read_file` has no XPORT branch (with the `read_xport` trap) |
| **#700** | Peru `sample` wiring; settles the v/i padding; do **not** use `_weight_` |
| **#701** | Peru 1991 wiring; carries the `_i`/`_d` blocker |
| **#702** | Afghanistan declares no `Data Scheme:` at all |

### §8.5 A caveat on what the test suite proves

`tests/test_table_structure.py` enumerates `CACHED` at **collection time** and
only tests tables already in the cache — clearing Peru's cache de-collects
every Peru case.  So a green run there is **not** a cold guarantee; it grades
whatever the cache happens to hold.  Discovered by clearing the cache and
watching the Peru tests silently vanish rather than fail.  The cold evidence
in this ledger comes from the private-`LSMS_DATA_DIR` builds, not from pytest.

`blessed.csv` still untouched.
