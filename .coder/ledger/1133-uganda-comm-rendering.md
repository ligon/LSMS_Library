# Prior-Art Ledger - GH #1133 Uganda early-wave cluster rendering

**Author:** Sue (GPT-6).  **Baseline:** development
`60aa81e9eda3b747e25a37c9e7354315b842b408`.
**Search tier used:** ripgrep + Git source/history; no dynamic census, project
imports, tests, provider bytes, or cache work.  This ledger is the first task
commit.  Phase 0 is source/metadata only; a production repair is not yet chosen.

## §1 Task, restated

Determine the source-backed rendering of Uganda's early enumeration-area
identifier `v`, preserving both household-to-cluster attachment and possible
community-questionnaire joins.  GH #1133 separates a formatting defect from
actual moves, absent `comm`, synthetic GPS bins, and genuinely ambiguous
household-derived pairs.  First establish the CSECTION1 key authority for
2005-06, 2009-10, 2010-11, and 2011-12.  The historical 322 pairs, two
non-bijective cases, 272 simple cases, and 50 district recodes are prior claims,
not measurements at this baseline and not a general digit-insertion rule.

## §2 Existing machinery (this task's area)

| symbol / source | path:line or section | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `v` | `lsms_library/countries/Uganda/_/uganda.py:34` | delegates the country formatter to `format_id` | generic format-id tests; Uganda grain tests | extend only if source evidence supports a rendering authority |
| wave `sample` / `cluster_features` | `Uganda/{2005-06,2009-10,2010-11,2011-12}/_/data_info.yml` | source `comm`; sample uses a mapped value, several cluster routes use scalar `idxvars` | `tests/test_uganda_v_grain_invariants.py`, `tests/test_sample.py` | reuse both routes; one formatter edit alone does not prove both routes change |
| `Wave.column_mapping` | `lsms_library/country.py:1370` | named formatter is bypassed for a scalar-string index mapping when `format_id_function=True`; explicit mappings have their own branch | general column-mapping and loader tests | reuse explicit mapping machinery if required; no framework redesign |
| `format_id` | `lsms_library/local_tools.py:3461` | generic identifier formatting, including numeric/missing handling | `tests/test_format_id.py` | reuse; no global Uganda-specific rule |
| `coalesce_coord_bin` | `lsms_library/transformations.py` and `Uganda/2009-10/_/data_info.yml` | fills absent `comm` with a synthetic coordinate-bin label | transformation and Uganda grain tests | preserve byte-level label convention and eligibility |
| `cluster_features` hook | `lsms_library/countries/Uganda/_/uganda.py:146` | projects household cover records to `(t,v)` using `reduce_to_agreed`; genuine conflict becomes missing with warnings | `tests/test_grain_collapse_cluster_features.py` | reuse; do not choose a district winner or average GPS |
| `_join_v_from_sample` | `lsms_library/country.py:3091` | attaches canonical sample `v` at the public read boundary by `(i,t)` | sample-join and Country tests | reuse; preserve non-sample table cache ownership |
| `Waves`, `id_walk`, `panel_ids` | `Uganda/_/uganda.py:269,679`, `Uganda/_/panel_ids.py`, `lsms_library/local_tools.py:3608` | declared household links and wave-specific stable household identity | `tests/test_panel_ids_dtype.py`, `tests/test_panel_ids_persistence.py` | reuse; never infer household identity from improved area matches |
| committed ID dictionaries | `Uganda/_/{panel_ids,updated_ids}.json` | source-of-truth household linkage, not ordinary Make outputs | persistence test and Makefile policy | reuse read-only; never regenerate during normal builds |
| cluster continuity declarations | `Uganda/_/sampling.yml` | early mixed/stable/wave-scoped boundaries; later kind changes | `tests/test_sampling.py` | update only a boundary whose measured continuity supports it |
| continuity warning / refusal | `lsms_library/sampling.py:824,912,994` | checks all crossed boundaries, outermost Country-only warning, strict refusal | `tests/test_sampling.py` | reuse; no new crosswalk or Feature warning policy |
| source and finalization contracts | `.coder/ledger/STANDING.md`, `CLAUDE.md`, `lsms_library/data_info.yml` | declared sources, redirected data writes, identity conversion, post-read `v` ownership | existing suite | reuse standing baseline |

## §3 Definitions & conventions in force

- Interface harmonization and cache/source boundaries: `CLAUDE.md`,
  `.coder/ledger/STANDING.md` §3/§4, `lsms_library/data_info.yml`.
- Country-specific meanings: complete `Uganda/_/CONTENTS.org` at the baseline,
  including all LOGBOOK/WAITING/TODO entries, read before this ledger.
  Early `comm` is an origin EA, not necessarily current location.  In 2009-10
  synthetic `@lat,lon` bins are not community-questionnaire clusters.  From
  2013-14 `v` changes kind to parish, then to a district/parish composite in
  2018-19.  These are not the early rendering problem.
- Accepted policy: GH #942 ruling (comment 6259384175, 2026-10-19) accepts
  DESIGN Q1-Q7 and files Q5 separately.  See
  `slurm_logs/2026-09-18_gh942_cluster_identity/DESIGN.org` §2.3a/Q5.
  The ruling does not authorize inventing ambiguous cluster correspondences.
- `sampling.yml` is an editorial source-backed design record; its measured
  continuity notes are historical evidence, not a new census.  See its header
  and `panel` entries.  The 2011-12 `wave-scoped` tag is explicitly reversible
  once source-backed rendering and actual continuity have been checked.
- Community-key spelling and width in the four CSECTION1 releases are still
  unverified.  The committed sidecars identify files, not their columns.

## §4 Invariants & assumptions

- Inherit `STANDING.md` §4.  No source rows, household IDs, survey eligibility,
  weights, GPS bins, scientific units, or table grain change under this task.
- Preserve the 2009-10 mixed boundary and synthetic labels; do not convert
  actual mover/split-off geography into a formatting correspondence.
- Normalize consistently at supported sample, cluster, and community read
  points.  `Wave.column_mapping`'s scalar-index branch means that changing
  `Uganda.v` alone is not sufficient evidence of consistency.
- A source absent from held providers is a coverage limitation, not evidence
  that its key can be guessed.  Distinguish absent records, missing `comm`,
  multi-valued linkage, non-bijective mapping, and genuine household moves.
- Preserve identifier strings and leading zeros.  No digit insertion,
  width guessing, district rewriting, or most-common-value selection without
  source authority.  Do not create a canonical API field or new grain.
- Full frame comparisons must retain complete axes, codes/levels, storage
  dtypes and typed attributes; narrower named-field equality is not full
  equality.  Retain all actual failures and warnings.
- The original documentation contains tensions that must be reported rather
  than silently resolved: claimed sample/Region completeness versus three
  missing Regions; one-third refresh each wave versus the same sample in
  Wave 5; and the 2005 cluster YAML's stable-centroid statement versus the
  country hook's documented origin-EA dispersal and no-GPS-averaging policy.
- Country source changes may invalidate Uganda tables beyond `sample` and
  `cluster_features`.  Fingerprint cost is unmeasured in phase 0; no claim of
  zero or country-only invalidation until a guarded complete metadata census.
- The current source includes the accepted #793/#942 design machinery and
  merged #1196/#1204/#1205/#1215 changes.  Do not revert their storage, routing,
  native-ID, or empty-axis contracts while addressing this rendering issue.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| canonical early EA rendering | pending source authority | CSECTION1 keys and the two ambiguous cases have not been read in this task |
| household linkage | reuse | committed dictionaries plus existing `id_walk`; geography is not linkage authority |
| sample-to-table `v` attachment | reuse | `_join_v_from_sample` owns it at read time |
| cluster reduction and missingness | reuse | established loud `reduce_to_agreed`, not an imputer |
| read-point formatting | extend existing explicit mappings if justified | existing loader machinery can cover scalar-index and value routes without a framework change |
| warning and sampling boundary | reuse / conditional metadata update | accepted Q5 policy; only measured continuity can justify changing 2011-12 |
| diagnostic evidence | new finite private records only | needed to distinguish source keys, linkage, ambiguity, bins, and current public routes; no production implementation in phase 0 |

## §6 Open questions for the human

These are evidence gates for root review, not requests for a new scientific
definition.  No canonical rendering decision is requested before the evidence.

- Which key columns and value domains do the four declared CSECTION1 releases
  actually use, and are their EAs contemporaneous or original-frame identities?
- Do the historical two non-bijective pairs persist at the merged baseline;
  are they caused by recorded moves, source recodes, inconsistent links, or
  conflicting source definitions?  Preserve them unresolved until witnessed.
- Is there an existing supported community-table join beyond the generic
  `v`/market attachment?  A declared raw file is not itself a registered table.
- What provider set is physically available for a finite, private source-backed
  diagnostic and bounded normal-route comparison?  Phase 0 permits metadata
  receipts only; root must review the exact later guard/copy budget.

---
### Phase 3 - verification (phase-0 checkpoint only)

- No production symbol, test, source mapping, or sampling declaration changed.
- Ledger/source map: OK (anchored on §2-§4).  Dynamic claims, canonical mapping,
  actual cache cost, and source-key evidence remain unmeasured and held.

---
### Phase 1 - measurement (2026-10-22, base `9a0ed5f3c`, private data root)

Raw keys via `get_dataframe` (+ `StataReader` labels via `get_data_file`):

| wave | GSEC1 `comm` | community file key(s) |
|---|---|---|
| 2005-06 | int32 8-digit, 322 EAs, "EA Code in 2005/06" | `csec1.comm` 7-char, 320, "2005/06 District Definitions (7 Digits)"; 0 literal overlap with GSEC1 |
| 2009-10 | 7-char, 317 EAs + 565 blank, "Current EA Code as in 2005/06 Data (7 Digits)" | `CSECTION1.comm` (05/06 defs) 315/317 overlap; `comcod` (2009/10 defs) 266 |
| 2010-11 | 7-char, 324 EAs, "Current EA Code as in 2005/06 Data" | `CSECTION1.comm` (05/06 defs) 273/324; `comcod` (2010/2011 defs) **321/324** |
| 2011-12 | int32 8-digit, 321 EAs | `CSEC1.comm` 8-digit 308/321; `comcod` 7-char "current" 0 |

No Uganda table reads any community file (`rg` over Uganda yml/py/Makefile).

- Three code families: 8-digit frame EA; 7-char 2005/06-definitions form =
  8-digit with its 4th digit dropped (all 317 2005-06 -> 2009-10 household
  pairs; every community `comm`; 0 collisions over 323 known 8-digit codes);
  7-char current-definitions form (2010-11 GSEC1, `comcod`).
- Household linkage = HHID identity in 2005-06 .. 2011-12 (`Waves` entries
  are `()`, `updated_ids.json` has no early-wave keys).
- 2010-11 -> 2011-12: 322 pairs, 272 insert-at-index-3, 50 district recode,
  region digit and serial preserved in all 322 -- the issue's numbers
  reproduce exactly.  The 2 "non-bijective" pairs are 8-digit EAs receiving
  two 7-codes: 10730027 <- 1070027 (1 hh) + 1150027 (8 hh); 30630002 <-
  3060002 (1 hh) + 3180002 (7 hh).  CSECTION1 (2009-10 and 2010-11) pairs
  comm 1070027 / comcod 1150027 and comm 3060002 / comcod 3180002 on one
  row; CSEC1 2011-12 pairs comcod 1150027 / comm 10730027 and comcod 3180002
  / comm 30630002.  One EA under two district definitions; 7 -> 8 remains a
  function.  Region: Central (10730027), Northern/KAABONG (30630002).
- 2009-10 real codes: 269 of 317 in the 2010-11 7-domain; the 48 missing are
  derived from 2005-06 -> 2009-10 (and 2009-10 -> 2011-12) linkage.  Union
  over four boundaries: 370 pairs, no 7-code with two 8-codes, every row
  corroborated by >= 1 community file.  Uncovered: 2010-11 `1530040` (1 hh).
- Canonical form: 8-digit confirmed (GSEC1 2005-06/2011-12, CSEC1 2011-12;
  changes 2 waves not 3).  Evidence against, reported: 3 of 4 community
  files key on the 7-char 2005/06 form; drop-4th-digit recovers that key.
- `mappings:` probe: the flat `[tbl, key, value]` form on Uganda `v` becomes
  `(uganda.v, dict)`, and `grabber`'s tuple branch applies only the first
  element, row-wise, so the build raises `ValueError`; an all-digit org key
  column parses to int64, so even a nested form would silently miss str
  `comm`.  Framework not edited; the string form `mappings: <function>`
  (Burkina_Faso/2014/_/data_info.yml:239, mapping.py:41) is used instead.

### Phase 2 - implementation (commit `113d53392`)

- `Uganda/_/categorical_mapping.org`: `#+key: comm7` / `#+name:
  comm_frame_ea` (370 rows; Form, four per-boundary household counts,
  community-file corroboration, Notes), evidence in the header comment.
  `#+key:` precedent: `Niger/_/categorical_mapping.org:90`.
- `Uganda/_/uganda.py`: `comm_frame_ea(value)` (format_id, lookup, else
  as-is) + `_comm_frame_ea_map()` read via `get_categorical_mapping` from
  `Path(__file__)` (precedent `harmonized_unit_labels`).
- 2009-10 `sample`/`cluster_features` myvar and 2010-11 `sample` myvar /
  `cluster_features` idxvar: `v: [comm, {mappings: comm_frame_ea}]`.
- `sampling.yml` 2011-12 `wave-scoped` -> `stable`; 2009-10 and 2010-11 notes
  re-measured (`mixed` kept).  CONTENTS.org item 1 rewritten; hybrid-`v`
  claim corrected ("exactly" was 315/317).

### Phase 3 - verification

- Continuity (linked households keeping `v`): 2005-06->2009-10 0/2607 ->
  2410/2607; 2009-10->2010-11 1819/2647 -> 2207/2647; 2010-11->2011-12
  0/2510 -> 2510/2510.  Every remaining non-keeper is a 2009-10 bin or NA.
- Distinct served `v`: 2009-10 656 -> 656 (541 bin rows / 339 bins / 24 NA
  unchanged), 2010-11 324 -> 322; cluster_features 4805 -> 4803 (2010-11
  320 -> 318); every other cluster row identical after relabel.
- `sample(waves=['2010-11','2011-12'])` ClusterIdentityWarning 1 -> 0.
- household_roster `v` == sample `v` for all early-wave households.
- Hash census: Uganda only -- 28/28 L2-country, 224/224 L2-wave, 0/40 btf.
- `tests/fixtures/uganda_baseline.json` `var/sample` and
  `var/cluster_features` entries move (out of scope here; values reported).

### Phase 4 - documentation corrections from independent review of `935ba72f9`

Prose and comments only; served `sample` / `cluster_features` /
`household_roster` re-built and `assert_frame_equal` to the previous head,
all waves.  Each claim re-measured before editing:

- `10730027` GPS: the 2005/06-form household's fix is 0.0 km from 8 of the 9
  `1150027` households; the 9th (District MUBENDE) is 34.66 km away, so
  `1150027` was already NA before the merge.  2010-11 GPS rows 188 -> 186.
  The lost fix is shared by 9 of the EA's 11 households (9 of 10 with a fix;
  the 2010-11 split-off has none) -- the review said 10 of 11.
- `1070027` is 2 raw 2010-11 households (10- and 13-character HHIDs; 1
  linked); `3060002` is 1.  The table's hh columns count linked households.
- CSEC1 pairs comcod 3010003 with comm 30130033; no GSEC1 household in any
  wave carries 30130033; 10 sit on 30130003 in 2005-06 and 2011-12.  GSEC1
  8-digit codes: 322 (the earlier 323 included this CSEC1-only code).
- 14 rows (13 drop-4th + 2220022 -> 20530022) have no CSEC1 pairing and no
  CSECTION1 -> CSEC1 chain; the chain agrees with drop-4th for 308 of 309.
- 2005-06 `v` vs `csec1.comm`: 0 of 3,123 literal, before and after.
- Hybrid bullet: drop-4th(`v`) in CSECTION1.comm for 2,399 of 2,410.
