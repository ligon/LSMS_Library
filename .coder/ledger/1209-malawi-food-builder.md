# Prior-Art Ledger - GH #1209 Malawi food builder

Search tier: git + ripgrep (no GitNexus or memory server exposed).
Actual parent: d29a56783d08a8a647e2622150a4ee5fbdd7246a (PR #1208).

## §1 Task, restated

Retire Malawi's obsolete country food concatenator so that the Make backend
uses the same canonical native-wave assembly as the Default backend.
Preserve all wave extraction, source, label, unit, quantity and valuation rules.

## §2 Existing machinery

| machinery | anchor | behavior and coverage | decision |
|---|---|---|---|
| `Country._has_country_builder` | `lsms_library/country.py:4893` | probes the country script and Make var target under an empty root; `tests/test_gh1039_make_backend_no_country_rule.py` covers both routes | reuse |
| `Country._aggregate_wave_data` | `lsms_library/country.py:5819` | Make falls through to `load_from_waves` when no country builder exists; same tests | reuse |
| Malawi native wave scripts | `lsms_library/countries/Malawi/*/_/food_acquired.py` | five canonical `(t,i,j,u,s)` outputs, using `malawi.food_acquired_to_canonical`; unit cleanup and GH1042 tests | reuse unchanged |
| retired country script | `lsms_library/countries/Malawi/_/food_acquired.py:14` at parent | hardcodes five waves and expects retired wide aliases, then lowercases/reduces/replaces zeros; current Make fails at `units` | remove duplicate |
| native-wave Make rule | `lsms_library/countries/Malawi/_/Makefile:21` at parent | builds data-root wave parquets; no wave Makefiles exist | preserve |
| runtime food derivation | `lsms_library/country.py:6240`, `transformations.py:2932` | expenditures, prices and quantities derive from `food_acquired`; parent PR1208 removed raw registrations | reuse |
| Feature assembly | `lsms_library/feature.py`, `bench/feature_audit/README.md` | forwards food kwargs and carries metadata; scoped unmodified scanner | reuse |

## §3 Definitions & conventions in force

- Inherit `STANDING.md` §3 and `CLAUDE.md` "Derived Tables".
- Canonical food schema: `lsms_library/data_info.yml:58`, `:1022`.
- Malawi `CONTENTS.org` "derived food registrations" already records this
  Make failure; there is no contradictory current food-builder decision.
- Malawi notes distinguish panel-only 2013, combined XS/tracking later waves,
  IHS2 label-derived factors and IHS3+ region-keyed factors. Preserve them.
- The GH1042 rulings and accepted suffixed-unit residuals remain in force.

## §4 Invariants & assumptions

- Inherit `STANDING.md` §4: sanctioned I/O, pre-finalize caches, API `v` join,
  derived-table discovery, and `id_converted` propagation.
- No country food script or var Make rule may remain: either makes the GH1039
  predicate select the obsolete route.
- Direct `make food_acquired` builds native wave parquets only; the framework
  owns country assembly. It must not claim to write a var aggregate.
- A Makefile edit can move every Malawi table and wave hash. Measure the full
  country/framework/wave scope before data; do not claim food-only invalidation.
- GH1210 invalid-wave eviction and scientific/unit residuals are separate.

## §5 Reuse decision

| behavior | decision | reason |
|---|---|---|
| country food assembly | reuse GH1039 `load_from_waves` | already authoritative and tested; no framework change needed |
| wave extraction and transforms | reuse unchanged | canonical source detail is already available |
| direct Make entry point | extend local Makefile with phony wave target | makes the retained native rules accessible without duplicate country assembly |
| regression coverage | extend with Malawi route/request tests | must distinguish parent failures from candidate behavior |

## §6 Open questions for the human

None blocking. Root approved the local retirement scope and will own
independent review and publication. Protected private verification is mandatory.

### Phase 3 - verification

- `OK` (sections 2/4): the actual predicate is false on the candidate and
  true on d29; the direct Make dry run names each native wave exactly once.
- `OK` (sections 2/3): six candidate route/request/preservation tests pass.
  On the actual parent, four route tests fail and both Default assembly
  tests pass. Thirteen existing focused tests pass.
- `OK` (section 4): the complete pre-build inventory compares 549 country,
  39 framework and 1,362 declared-wave keys. Exactly 26 Malawi country and
  97 Malawi wave hashes move; every other key is unchanged.
- `OK` (sections 2/4): guarded held-source checks pass 56 strict Country
  pairs, 24 strict Feature pairs and ten native-parquet pairs, including
  complete axes, unused levels/codes, storage, null policies and typed attrs.
  No normalization is needed. All 16 parent Make Country endpoints fail;
  its 12 Feature requests degrade empty. Candidate Make restores ten
  nonempty Feature results and preserves the two expected no-Price empties.
- `OK` (section 4): direct Make retains the existing singleton wave and
  builds the other four native waves, without a country food parquet.
  The unmodified scoped scanners have 163 literally equal records each:
  158 warnings, three passes and two pre-existing price-unit failures.
- The first two Default all-wave runs remain diagnostic only: their
  wrapper omitted `LSMS_MAKE_JOBS`. Fresh L1-only v2 roots provide the
  conforming evidence. Both denied relative-output inventory attempts,
  early test-axis/fixture stops and two unguarded metadata-status utility
  calls are retained and disclosed in the private author report. There
  is no zero-denial or zero-outside-write claim.
- Full private evidence is under the owner-only
  `/local/job40255479/control/isa-backlog-20261021/Malawi/1209` prefix.

Sue
