# Prior-Art Ledger - GH1215 Uganda empty-column order

**Author:** Sue (GPT-6). **Phase:** static proposal only; no implementation is authorized.
**Source parent:** `a417b69d71c460ab33f6a494b4df5d67cd792de5`.
**Search tier:** direct source reads, ripgrep, git, and stdlib AST/text inspection.
No GitNexus or memory connector is available in this session.

## §1 Task, restated

Diagnose why the existing typed-empty country response can enumerate required
columns before optional columns while Uganda's native/covered-cache response
uses another order. Establish the existing contract and all affected callers,
then propose a bounded boundary and meaningful checks. This is a Tier C
schema-order proposal, not authority to change production, schema, or tests.

## §2 Existing machinery (source anchors at a417)

| machinery | source authority | role | coverage status | decision |
|---|---|---|---|---|
| Module-level `_empty_declared_frame` | `country.py:1050` | required-then-optional typed-empty constructor | GH1039 test431; only required x followed by optional y, so order policy is not discriminated | reuse/possibly extend |
| `_required_scheme_columns`, `_optional_scheme_columns` | `country.py:1020/1038` | existing ordered membership within each group | GH323 site4 test181; null-read optional test232 | reuse without changing membership/audit semantics |
| `absent_subset_frame`, `load_from_waves` | `country.py:5004/5249`; constructor calls5027/5403 | only two production constructor callers | GH1099 matrix310 and error/lazy-hit controls; GH1204 barren test146 | preserve routing/cache/error boundary |
| `_finalize_result`, `_restrict_to_waves`, cache reads | `country.py:3815/4357/4434/4637` | ordinary finalization; cache row restriction preserves column order | existing GH1099 strict one-field empty controls | preserve |
| Native crop projection | `countries/Uganda/_/uganda.py:1568/1645`; scheme127 | explicit eight-column order equals original declaration order | native producer source; existing GH1204 fixture is not the exact eight-column order oracle | reuse as independent order authority |
| `Feature.__call__`, `_harmonize_country_frame` | `feature.py:1076/695/1186` | skips empties, may drop nonempty all-NA columns, concatenates native columns | canonical-index/assembly tests | preserve; not a typed-empty schema oracle |
| Optional dict dtype enforcement | `country.py:7103/7141/7163`; `tests/test_table_structure.py:68/110` | country coercer skips dict type specs; global canonical types can rescue only known fields | existing tests document extended dict syntax; no new runtime here | separate actionable dtype proposal, not order implementation |

## §3 Definitions & conventions in force

- Inherit `STANDING.md` §3 and `CLAUDE.md`; canonical schema is
  `lsms_library/data_info.yml`. Do not invent a column-order contract.
- Uganda `CONTENTS.org`: "A shared axis NAME is still not a shared VOCABULARY."
  Crop `plot_id` contains native household/parcel/plot identity, while area IDs
  have a different vocabulary. Keep order diagnosis separate from ID repair.
- The native crop index retains `production_obs` after `season` (GH1198),
  condition, unit and actual source-season detail. A/B denote modules, not
  universally the calendar's first and second seasons.
- `KgFactor`, `Unit_sold`, and `Condition_sold` are optional ordinary columns;
  missing sold labels are NA-bearing rather than invented index sentinels.
- GH1204 documents 2005-06 as genuinely barren and the existing typed-empty
  response as preceding a country-cache rebuild/write.

## §4 Invariants & assumptions

- Inherit `STANDING.md` §4. No raw reads, package imports, API calls, tests,
  builds, data copies, acquisition, cache/venv operations, or runtime replay.
- Only this ledger and the assigned private prefix may be written.
- Preserve dtype/index names, optional membership, attrs, native grain and all
  pre-existing failure evidence. Stable column order alone is not full-frame
  equality or cold/warm equivalence.
- Quote contradictory documentation as evidence; do not silently replace it.
  The notes say Region is "complete for every household" and separately
  document three missing Regions in 2019-20. The notes also retain historical
  reducer/factor/cache statements and explicitly dated superseding sections.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| Column identity/order authority | reuse | actual Uganda declaration and explicit native projection agree; no universal native-order promise found |
| Empty dtype/index construction | reuse/possibly extend | recommend original entry iteration restricted to existing required/optional membership; root Tier C decision remains pending |
| Metadata/Feature verification | reuse | bounded same-request attrs, typed axes, optional-presence and synthetic healthy controls; no new public grain |
| Static census | new private analysis only | stdlib lexical inventory38 scheme files/448 entries;48 optional-bearing;33 order-sensitive across21 countries; no runtime reachability inference |

## §6 Open questions for root

- Choose narrow original-declaration order in the existing constructor, or a
  larger explicit public-empty normalization contract. The first aligns the
  motivating Uganda paths, but cannot promise every native/cached writer has
  the same order or optional membership.
- The constructor is reachable from the all-table tagged aggregator and is not
  fingerprint-excluded; actual549/39/1362 hash cost is unmeasured and must be
  assessed before any separately released data build.
- The concrete proposal and bounded actual-parent/healthy/metadata/Feature
  controls are in the private PHASE0.md. No implementation/runtime is released.
- Root's BF1188 author observation is separately qualified: four new optional
  fields are absent from actual parent3dc9d285. Read-only detached3d39096
  declarations/tests bind their names/types; no independent runtime result is
  claimed. Owned a417 static census finds101 optional dict fields,20 globally
  typed and81 without global type coverage. These are not81 observed failures.

### Phase 3 - verification

- OK (anchored on §2/§5): recommendation reuses ordered schema membership and
  existing typed-empty construction; no new constructor or schema hardcoding.
- OK (anchored on §3/§4): no production/schema/test/data change; all original
  author four strict failures remain failures. No package/numerical import,
  payload read/copy, API, test, build or acquisition was performed.
- CONTRADICTION retained (anchored on §3): historic2018-only factors versus
  dated newly-wired rounds, universal parcel-match claim versus held-area
  qualification, and source-season shorthand versus actual module chronology.
  They authorize no scientific/identity rewrite in an order task.
- Static census v1 stopped at the existing CLAUDE->AGENTS link before schema
  analysis; v2 explicitly binds that link and target and completes0. Both
  sources, original terminal1/stderr and corrected terminal0 are retained.
- Initial ledger commit467b3b59c preceded analysis; this final ledger records
  the bounded proposal only. Root owns the unresolved Tier C decision.
