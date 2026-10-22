# Prior-Art Ledger - GH1196 cold/warm index dtype diagnosis

**Search tier used:** ripgrep + git; installed-source inspection is available.
This is initial grounding for a queued read-only diagnosis, not a cause or fix.

## §1 Task, restated

Locate the precise serialization/read/finalization transition behind the two
independently measured GH1196 manifestations: Niger food_acquired MultiIndex u
and Uganda cluster_features Index v. Both change pyarrow StringDtype missing
sentinel between ordinary cold and fresh-process warm returns. A shared cause
has not been established. Preserve the full interface, values and typed attrs.

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
- The two measured axes differ: Niger is a MultiIndex level u; Uganda is a plain
  Index v with4805 rows/zero null v. Equal symptom does not prove equal cause.

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
