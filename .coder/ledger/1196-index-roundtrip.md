# Prior-Art Ledger - GH1196 cold/warm index dtype diagnosis

**Search tier used:** ripgrep + git; installed-source inspection is available.
The initial grounding is retained below; section7 records the root-accepted diagnosis.
No production fix has been accepted or applied.

## §1 Task, restated

Locate the precise serialization/read/finalization transition behind the two
independently measured GH1196 manifestations: Niger food_acquired u and
Uganda cluster_features v levels, both within complete public MultiIndex frames.
Both change pyarrow StringDtype missing sentinel between ordinary cold and
fresh-process warm returns. A shared cause was unproved at initial grounding;
section7 now binds both retained cases. Preserve the full interface, values and typed attrs.

## §2 Existing machinery

Anchors below are at release f8d236694c26fe9e709c5548f02c70f74ea72c33.

| symbol | path:line | existing role | tests / decision |
|--------|-----------|---------------|------------------|
| get_dataframe | lsms_library/local_tools.py:1686 | sanctioned reader; parquet dispatch at1837 uses pandas/pyarrow | reuse; test_cache_hash_invalidation.py:101 and test_gh645_to_parquet_null_coercion.py:119 |
| to_parquet | lsms_library/local_tools.py:3225 | sanctioned writer; reset/rebuild index and two writer branches | reuse; test_gh645_to_parquet_null_coercion.py tests literal null spellings/masks, test_cache_hash_invalidation.py tests metadata/atomicity |
| Country._read_l2_country_cache | lsms_library/country.py:4434 | current-hash/coverage read gate, returns pre-finalize frame | reuse; cache hash/coverage tests, including test_gh891_wave_subset_cache.py |
| Country._finalize_result | lsms_library/country.py:3815 | existing index augmentation/normalization/walk/v join and finalization | inspect actual call sequence; no replacement pipeline |
| _normalize_dataframe_index | lsms_library/country.py:8394 | shared declared-index normalization | inspect both observed axis classes |
| map_index | lsms_library/local_tools.py:3840 | existing parquet index mapping | inspect actual reachability; do not assume it is the cause |

The existing generic round-trip tests establish real null preservation and
literal-label survival; they do not establish GH1196's exact index dtype
contract. Find a discriminating minimal regression before proposing a fix.

## §3 Definitions and conventions in force

- Inherit STANDING.md §3/§4 and the exact canonical data_info.yml declarations.
- STANDING.md §4: cached parquets are pre-finalize; finalization runs on reads.
- STANDING.md §4: pandas3 string/ID/categorical missing values use pd.NA.
- Country CONTENTS owns release-specific identifiers and caveats. Read complete
  CURRENT Niger and Uganda files, including TODO/WAITING/LOGBOOK, before analysis.
- The observations concern Niger's u and Uganda's v levels; Uganda has4805
  rows/zero null v. The saved diagnostic's pandas.Index class describes the
  EXTRACTED v level (dtype-details.py:6-7), not the complete public frame index.
  Bind full-frame and level classes separately at every actual stage.
  The initial468d ledger overstated this distinction; its git history and
  original private brief remain preserved. Equal symptom does not prove cause.

## §4 Invariants and assumptions

- Preserve exact labels/IDs/null masks/order/multiplicity, full axis internals,
  dtypes and recursively typed attrs/_FIELDS; never hide a mismatch by casting,
  dropping attrs or weakening comparator tolerances.
- Preserve GH645 real-null/literal-null-spelling distinctions and writer atomicity.
- Current-hash/full-coverage normal cache routing remains in force; no bypass,
  cache stamp, promotion, shared mutation or source acquisition.
- Historical Niger and Uganda reports remain at their original pins. Diagnosis
  on releasef8 must establish actual source/cache compatibility independently.
- Existing CLI JSON/typed-record serialization and Make writes are separate
  observations. Do not label them fixed by an index-dtype explanation.

## §5 Reuse decision

| behavior | decision | reason |
|----------|----------|--------|
| source/parquet IO | reuse | existing sanctioned reader/writer and held immutable evidence |
| freshness/coverage | reuse | existing metadata gates, no manual re-stamping |
| country/index finalization | inspect/reuse | existing machinery is the object of diagnosis |
| strict stage comparator | reuse proven machinery with source pins | preserve full typed comparisons and negative controls |
| production repair | undecided | no exact cause or accepted minimal regression yet |

## §6 Open questions

The exact transition and whether the two cases share it remain technical
questions for diagnosis. No new scientific or identifier policy is authorized.
Report any genuine contract ambiguity before proposing a production change.

### Phase3 verification

Pending. Source-only preparation and initial grounding have no runtime verdict.


## Section7 Root-accepted read-only diagnosis, 2026-10-22

Source f8, READY-v4 08a2aa8392cd21b4b6d4308523334303ed7b43d42cf767ab1e1bf56b1958832a;
281-entry manifest368e4997fd99d0982c6db54992fa19e8f6bea82271b28a1e2ec91e2d1afcd24c.
Root independently verified every receipt, complete directory membership,
13 safe exports, actual checker pins,14880 tracked source endpoints, selected
original/dependency/copy receipts and actual8FAIL/7PASS JUnit causes.

Both retained cases first diverge in installed pyarrow25.0.1
`pandas_compat._extract_index_level`: unlike data-column reconstruction, its
`col.to_pandas(types_mapper=None)` ignores retained field numpy_type="string"
extension policy and selects pandas3's NaN string policy. Actual write-time
and read schema bytes match; neither map_index nor finalization first creates
the difference. This binds239877Niger/u and4805Uganda/v cases only, not every
string index or survey. Both complete public indices are MultiIndex.

Two new ordinary Default warm processes exactly match saved warm full frames
and typed attrs, preserving21 current/fullcoverage cache files per process.
No original native-prewrite operand or new source-cold/Make/Feature/scanner
proof was produced. Existing writer JSON attrs and pre-Arrow unused-level
pruning remain separate contracts.

Actual source routes show a Country L2-gate-only repair misses observed Uganda
Make: both entries lack materialize:make, and Make uses the YAML wave route.
Four immutable original Uganda1198 Make endpoints corroborate the4805row case;
actual Niger Make food remains source-only/unmeasured in this diagnosis.

Eight guard denials remain qualified:6relative cleanup removals and2pytest
cache-directory attempts, without PID/dir_fd/stack attribution. The94legacy
symlink-hash convention stop, original273-payload late-file membership stop
and release Path/string stop remain preserved. No source/evidence mutation,
API/test replay or zero-denial claim repaired that history.

Next candidate is a PRIVATE PATCH PROPOSAL at the generic parquet reader,
using each recorded physical index field's policy. Root must inspect the actual
patch before production edits. No dependency/schema/fingerprint exclusion,
writer coercion/JSON, country/source/scientific or missingness-policy change.
The549/39/1362 source-marker fingerprint cost is a prediction to measure on
the actual future source, not an approval to rebuild other-country outputs.

See slurm_logs/isa_backlog_2026-10-21/1196-index-roundtrip-diagnosis/ and
queued-1196-index-fix/STAGE1.json for exact limits and next-stage ownership.
