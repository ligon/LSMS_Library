# Prior-Art Ledger -- #1118 Tanzania plot-input units

**Search tier used:** ripgrep + git floor; immutable release baseline
`f8d236694c26fe9e709c5548f02c70f74ea72c33`.

## §1 Task, restated

Normalize missing `u` in Tanzania's `plot_inputs_for_wave`, used only by
2019-20 and 2020-21 plot-input scripts, to the existing `U_UNKNOWN` sentinel.
Retain recorded units even when `Quantity` is absent. GH #1041's existing
maintainer ruling covers missing units with or without quantity. This is the
Tanzania slice of #1118, not a change to factors, decoding, source-use
predicates, identifiers, grain, curation, or other countries.

## §2 Existing machinery

| symbol | path | what it does | tested? | decision |
|--------|------|--------------|---------|----------|
| `U_UNKNOWN` | `lsms_library/transformations.py` | existing canonical `Unknown` unit sentinel | `tests/test_962_unit_sentinel.py` | reuse |
| `plot_inputs_for_wave` | `lsms_library/countries/Tanzania/_/tanzania.py` | binds seed and five plot-input channels, collapses duplicate declared keys | existing Feature/schema tests; add targeted synthetic tests | extend only unit normalization |
| `_SEED_UNIT_LABELS`, `_LIQUID_UNIT_LABELS` | same module | decode total-quantity unit codes; OTHER has no recoverable unit text in held extracts | held Stata dictionaries checked | unchanged |
| `id_walk` | `lsms_library/local_tools.py`, Tanzania `_/plot_inputs.py` | existing UPHI conversion | panel-ID tests and ledger `637-tanzania-keys.md` | unchanged |
| `Country._join_v_from_sample` | `lsms_library/country.py` | joins sampling cluster at API time | standing baseline §2 | unchanged |
| content hashes | `Country._table_cache_hash`, `build_transforms_fingerprint` | country-module edits invalidate country tables | cache tests | reuse; measure all 549 country cells / 39 framework tables |

## §3 Definitions & conventions in force

- Inherit `.coder/ledger/STANDING.md` §§3-4 and `CLAUDE.md` data-access,
  cache, identifier, and pandas conventions.
- Canonical table: `lsms_library/data_info.yml`, `plot_inputs`.
- Registered country grain and columns:
  `lsms_library/countries/Tanzania/_/data_scheme.yml`, `plot_inputs`.
- Source detail and unresolved key caveats:
  `lsms_library/countries/Tanzania/_/CONTENTS.org` and
  `.coder/ledger/637-tanzania-keys.md`. Preserve live `WAITING` #114.
- GH #1041 / live #1118: a missing or blank unit is existing `U_UNKNOWN`,
  including records without a quantity. No additional human policy choice.

## §4 Invariants & assumptions

- Preserve all 9,698 current Country rows, keys, non-unit fields and attrs;
  exactly 408 current `u` cells change (2019-20: 86; 2020-21: 322).
  Of these, 389 have quantity and 19 do not. Preserve all source-use rows.
- Delete the unsupported statement `u is meaningless where there is no
  quantity` and its quantity-dependent blanking. It contradicts #1041 and
  would erase a genuine recorded unit on future no-amount source records.
- Normalize actual null / whitespace-only units **after** the unchanged
  duplicate collapse. `first` must still find a later recorded unit when an
  earlier duplicate lacks one; early `Unknown` would displace it.
- Nonblank labels remain verbatim. Preserve all decoder maps, source
  predicates, reducers (`sum(min_count=1)`, first unit, max flags), IDs and
  factors. Report genuine conflicting-unit duplicate groups for #637.
- The held extract's `ag4a_10c_2` is a **purchase**-quantity unit, not the unit
  of total `Quantity`. Two 2020 records with missing/OTHER total units have
  known purchase units; do not borrow that separate channel.
- No unlisted nonnull raw unit codes in the four held modules. Seed OTHER
  (5) and liquid OTHER (4) have no unit-specification text in these extracts.
  The 2020 agriculture questionnaire blob is absent; do not acquire it.
- Only the modern long-rainy-season AG_SEC_3A/4A scripts are in scope.
  No 2008-15 edits or rebuilds. UPHI back-casting, disjoint 2014 subpanels,
  2019 Extended / 2020 Refresh plus urban booster remain as documented.
- All microdata, IDs, full logs and caches stay in physical private task
  storage; reads use sanctioned helpers, network is actively blocked.
  Use image Python, exact worktree import/config assertions and one numerical
  thread / build worker; at most four pytest loadfile workers.

## §5 Reuse decision

| behavior | decision | reason |
|----------|----------|--------|
| missing unit representation | reuse `U_UNKNOWN` | existing #1041 ruling and sentinel |
| duplicate aggregation | reuse unchanged | unit normalization cannot decide #637 grain |
| source decoding and row inclusion | reuse unchanged | metadata agrees with decoder; missing total units are not join loss |
| validation oracle | new private source binding and conservation checks | independent held-source expectations, with raw records kept private |

## §6 Open questions for the human

None for this scoped correction. Root approved the exact boundary on
2026-10-21. The 2020 questionnaire absence and conflicting-unit duplicate
census are evidence/residuals, not requests to decide source facts.

---
### Phase 3 -- verification

Pending implementation and private cold/warm, source-conservation,
Country/Feature, targeted-test and 549/39 hash checks.
