# Prior-Art Ledger - GH #1210 invalid food-wave requests

Search tier: git + ripgrep; no GitNexus or memory tool is exposed.
Actual independent parent: f8d236694c26fe9e709c5548f02c70f74ea72c33.

## §1 Task, restated

Reject a request containing an invalid wave before cache maintenance or
derived-table pre-work can mutate L1 or L2. Preserve the existing invalid-key
error and valid selection, staleness, coverage and food semantics.

## §2 Existing machinery

| machinery | anchor | behavior and tests | decision |
|---|---|---|---|
| `Country.__getitem__` | `country.py:2964` | accepts declared wave identifiers; exact existing KeyError for an invalid identifier | reuse for invalid elements only |
| generated table method | `country.py:6150` | validates kwargs before derived pre-work; valuation calls `_valuation_geo` before aggregation | extend with entry guard after argument validation |
| `Country._aggregate_wave_data` | `country.py:4980` | handles internal and public cache/build routes | extend with entry guard before maintenance |
| `_valuation_geo` | `country.py:3169` | calls `cluster_features(waves=...)`, so invalid validation must precede it | preserve |
| `_evict_hashless_wave_caches` | `country.py:4709`, nested `load_from_waves:5249` | GH479 eviction before every rebuild descent; `test_cache_hash_invalidation.py` tests selective/stale eviction | preserve for valid requests |
| `_read_l2_country_cache`, `_union_waves`, `_restrict_to_waves` | `country.py:4311` onward | GH891 coverage grows monotonically; `test_gh891_wave_subset_cache.py` | preserve |
| `_has_country_builder` and Make route | `country.py`, `test_gh1039_make_backend_no_country_rule.py` | country-builder routing and no-rule fallback | preserve |

## §3 Definitions & conventions in force

- Inherit `STANDING.md` sections 3/4 and `CLAUDE.md` cache/derived-table rules.
- Canonical food columns/axes remain in `lsms_library/data_info.yml`.
- Native `(t,i,j,u,s)` food parquets and finalized API `(i,t,v,j,u,s)`
  frames are distinct; full actual axes/storage are comparison operands.
- Malawi notes document panel-only 2013, combined later XS/panel waves,
  label-derived IHS2 factors and region-keyed IHS3+ factors; preserve them.
- f8 notes say retired food aliases derive from food_acquired, but the scheme
  still contains two null aliases. GH1208 and GH1209 are independent fixes.

## §4 Invariants & assumptions

- An all-bad or valid-plus-invalid request must preserve the complete L1/L2
  pathset, bytes, size, mode, mtime and physical receipts.
- Reuse the first invalid element's precise `__getitem__` KeyError; do not
  construct valid Wave objects just to validate membership.
- Materialize explicit non-list iterables once; retain None, empty selections,
  order, duplicates and existing string-as-iterable behavior. No new string API.
- Keep existing argument-error precedence before the public wave guard.
- Valid absent/stale country rebuilds still evict hashless waves (GH479);
  covered warm requests remain reads, and GH891 union/coverage rules remain.
- No fingerprint exclusions, Make cleanup, alias removal, valid-subset fallback
  fix, string-policy change or scientific/unit work belongs here.
- Full 549 country / 39 framework / 1,362 wave hash census precedes data.
- All project/data actors use isolated mounted runtime and guarded private roots.

## §5 Reuse decision

| behavior | decision | reason |
|---|---|---|
| invalid-key formatting | reuse `self[bad]` | exact existing contract, one source of error wording |
| explicit iterable preparation | new small Country helper | no existing selection validator snapshots the input before maintenance |
| entry validation | extend public and internal entries | public valuation pre-work can precede the internal entry |
| cache/build behavior | reuse unchanged | only invalid entry ordering is defective |
| verification | extend focused tests and guarded held-source experiment | must distinguish actual parent side effects and preserve valid results |

## §6 Open questions for the human

None blocking. Root approved the helper and both entry guards before framework
edits; independent review and publication remain root-owned.

### Phase 3 - verification

- `OK` (sections 2/4): 28 candidate regressions pass. Correctly launched actual
  f8 control has 16 invalid/mixed failures and 11 passing ordinary controls.
  Its additional exhausted-iterator failure is a measured behavior change,
  not evidence of universal unchanged iterable compatibility.
- `OK` (section 4): held-source all-bad and mixed requests preserve the whole
  candidate L1/L2 physical state. Parent requests each delete five native wave
  files and alter five directories, with L1 and country parquet unchanged.
  Direct-food KeyError args and the single-KeyError chain remain exact.
- `OK` (sections 3/4): 56 strict pairs pass without normalization: 34 Country,
  12 Feature and ten native-parquet pairs. All complete axes, unused levels,
  codes, storage, null policies, typed attrs and numeric values are checked.
- `OK` (section 2): 21 selected GH479/GH891/GH1039 tests and API audit pass.
  The scoped scanners have 163 literally identical ordered records each,
  including two pre-existing food-price failures. Six valid warm cases write
  no parquet; stamped hashes and coverage are current.
- `OK` (section 4): all 549 country, 39 framework and 1,362 declared-wave
  fingerprints move. Public API order and complete declaration metadata are
  unchanged. No exclusions or corpus data rebuild were added.
- Diagnostic history is retained: the first metadata checker wrongly assumed
  order for a set-derived declaration list; only its full-multiset comparison
  was corrected. An unexpected 28-pass supposed-parent xdist run has no worker
  identity history and is diagnostic only. The corrected parent launch uses
  explicit base rootdir/importlib and per-worker pre-collection assertions.
- Controlled Default derived routes retain the existing KeyError args but
  fail earlier. The two old Make controls reach a deliberately blocked external
  builder; their underlying wrapped backend error was not measured there.
- Frozen GH1209 source and evidence remain read-only. Full physical/source/
  provider protection and immutable author handoff are recorded privately.

Sue
