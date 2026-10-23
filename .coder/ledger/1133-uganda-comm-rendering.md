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
