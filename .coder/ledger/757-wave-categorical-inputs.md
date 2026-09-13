# Prior-Art Ledger - GH #757 wave categorical inputs

**Search tier used:** ripgrep + git floor; direct code and covering tests.

## §1 Task, restated
Extend Wave._input_hash so changes to inherited categorical/build Org inputs
invalidate L2-wave parquets before a country rebuild can restamp stale mappings.
No data acquisition, mapping edits, cache clearing, or post-read behavior changes.

## §2 Existing machinery

| symbol | path:line | behavior and coverage | decision |
|--------|-----------|-----------------------|----------|
| Wave._input_hash | lsms_library/country.py:837 | metadata-only wave hash; tests/test_cache_hash_invalidation.py | extend |
| Country._table_cache_hash | lsms_library/country.py:3249 | composes wave hashes; already hashes country Org | reuse |
| Country.categorical_mapping | lsms_library/country.py:1898 | global Org plus country categorical_mapping.org, falling back to parent | reuse resolution |
| Wave.categorical_mapping | lsms_library/country.py:1081 | inherits country dictionary, overlays wave tables | reuse |
| cached_file_hash, cache_freshness, stamp_parquet_hash | lsms_library/local_tools.py | content memoization and parquet freshness gates; cache tests | reuse |
| _ORG_HASH_SKIP | lsms_library/country.py:646 | excludes CONTENTS.org | reuse |

## §3 Definitions & conventions in force
- Cache and IO conventions: STANDING.md §§3-4 and CLAUDE.md Cache Behavior.
- Pre-finalize cached values can include categorical mappings consumed during
  extraction (country.py:1028); post-read application alone is not an input.
- Country.categorical_mapping reads package-global categorical_mapping/*.org,
  then country _/categorical_mapping.org or its parent fallback (:1912-1936).

## §4 Invariants & assumptions
- Preserve STANDING.md §4: use existing freshness gates, no source data hashing.
- CONTENTS.org remains excluded. Paths use Wave/Country.file_path for config;
  package-global mappings use the same package resource root as their reader.
- Existing coarse whole-file hashing deliberately over-invalidates within a
  country. Globals conservatively affect all tables/countries. New hash inputs
  necessarily change initial hashes; this is not a hash-neutral migration.
- Existing Country instances memoize mapping dictionaries. Same-instance config
  hot reload is outside this fix; fresh instances must rebuild correctly.

## §5 Reuse decision
Extend the existing Org-input loop with distinct country/global prefixes; hash
only the actual parent categorical fallback if the country file is absent.
Reuse cached_file_hash and existing wave stale-cache gates, not a new cache.
Tests use temporary metadata, synthetic source data and private parquet paths.

## §6 Open questions for the human
No implementation blocker: dispatcher authorizes conservative global coverage.
The deliberate cost is a one-time broad cache invalidation and future broad
invalidation on global Org edits. No full corpus rebuild will be run here.

### Phase 3 - verification
Pending targeted metadata and synthetic stale-cache tests.

- OK (sections 2-5): Wave._input_hash reuses cached_file_hash and existing
  _ORG_HASH_SKIP with distinct corg/gorg/parent_cmap inputs; no new cache gate.
- OK (sections 3-4): new synthetic CSV test builds Old, edits the inherited
  mapping, observes stale cache, rebuilds New, then serves New from fresh cache.
  Country instances are recreated; no hot-reload promise is made.
- OK (section 4): metadata tests cover country/global changes, deterministic
  hashes, CONTENTS exclusion, unrelated-country stability, and actual fallback.
- Targeted run: 22 passed, 16 existing Albania warnings in 75.36 seconds:
  `PYTHONPATH=$PWD LSMS_BUILD_WORKERS=1 taskset -c 16-23
  /local/job38734650/mirrors/LSMS_Library/.venv/bin/python -m pytest
  tests/test_cache_hash_invalidation.py --no-purge -q`.
  Output: /tmp/gh757-tests.log. New tests separately: 3 passed in 3.72 seconds.
- Initial invalidation census: executing HEAD's _input_hash and the new method
  against unchanged GhanaLSS/Albania/Nigeria metadata, two tables per wave,
  changed 46/46 hashes (2.637 seconds including country initialization). This
  confirms broad one-time invalidation, without clearing or rebuilding caches.
- `git diff --check` passed. Corpus-wide cache rebuild and same-instance mapping
  invalidation are outside this verification scope.
