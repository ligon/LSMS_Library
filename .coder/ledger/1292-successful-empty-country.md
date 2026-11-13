# Prior-art ledger -- successful empty YAML results (Nigeria 1292)

Sue / GPT-6, 2026-10-28. Ledger, root-approved implementation and finite
synthetic author verification; independent review remains pending.
Base: `d65e8b13683409ab0005f1a5590208cd7d94ae62` on the isolated
`fix/1292-successful-empty-country-20261028` branch.
Search tier: git + ripgrep; GitNexus is unavailable. Inherits `STANDING.md`,
current `AGENTS.md`/`CLAUDE.md`, and canonical `lsms_library/data_info.yml`.
Read current guidance changes, relevant prior ledgers/history and tests.
The root decision below supersedes the original phase-1 implementation hold.
No real-source/provider build or broad metadata census has been performed.

## §1 Task, restated

Preserve a successfully produced, valid zero-row YAML wave answer instead of
using its emptiness as grounds for a second legacy build. A strict subset that
is positively barren and has no country builder must return an honest typed
empty answer without replacing valid other-wave country-cache content. Keep
real failures, malformed results, script fallback, JSON, full-empty failures,
country-builder authority, unknown rule probes and nonempty union builds.

The motivating country repair is recovered Nigeria
`e569358f5d4481bb45ae4e90609a83df3b267f83`: five explicit source dates stop
copying selected PH education answers onto PP. Its own ledger and complete
2,754-line `_/CONTENTS.org` were read, including WAITING entries. The relevant
idiosyncrasy is one physical wave folder serving two rounds while its selected
education source is PH. W5 YAML selects `s2q9`, its legacy script `s2q3`.
The existing CONTENTS qualification explicitly preserves the possibility of
genuine PP education in W1/W2 instruments; no new PP availability claim follows.
Country-specific sources/sample closure remain with the Nigeria owner.

## §2 Existing machinery

All code locators below are on d65e8b1, unless a recovered ref is specified.
Its `country.py` is byte-identical to recovered e569358.

| Machinery | Source | Contract / existing controls | Decision |
|---|---|---|---|
| Actual Wave getters | `country.py:1194-1222`, `:2365-2449` | Dynamic getter calls grab_data; cluster and food getters perform additional processing; `test_wave_getattr.py`, GH1078 tests | Preserve actual getter, never bypass with a direct grab_data call |
| Wave declarations | `country.py:1262-1303` | Scripts plus YAML keys; other_features/cluster_features compatibility; declaration is not producer success | Reuse for eligibility, never sole proof |
| Private producer origin | `country.py:1873-1917`, `:2354`, `:2428-2437` | `_with_origin` reports actual memory/parquet/materialized/missing route without public attrs; GH1078 ledger/tests | Candidate extension, protocol unresolved below |
| Explicit source date | `country.py:1595-1596`, `:2317-2318` | Per-file t override and real requested-quarter filter; recovered GH1292 Wave tests | Reuse unchanged |
| Current legacy decision | `country.py:5499-5549` | None OR DataFrame.empty triggers fallback; GH535 then filters legacy t | Extend only proven-success empty handling |
| Early absent subset | `country.py:5203-5227` | GH1099 strict subset with no declarations and completed no-builder proof; cache hits remain earlier | Preserve; declared empty is a separate case |
| Post-wave assembly | `country.py:5584-5666` | Normalization, empty discard, nonempty concat; country_builder=False constructor; otherwise country fallback | Extend after outcome accounting |
| Parallel protocol | `_parallel_waves.py:478-505`, `:560-580`, `:600+` | Builder returns exactly (has_table, result); result is processed by worker/relay orchestration; `test_parallel_waves.py` | Preserve outer pair; audit every consumer before any carrier change |
| Required/optional fields | `country.py:1070-1097` | Shared authoritative membership | Reuse, no copied required-column list |
| Constructed empty and marker | `country.py:1100-1163` | Declaration-order typed payload; zero rows plus marker means non-cacheable answer | Reuse after original result validity is established |
| Country write guards | `country.py:5817-5821`, `:6021-6022` | Constructed answers skip both country parquet writes | Preserve both; no union coverage on an empty answer |
| Country builder probe | `country.py:5069-5173` | Completed absence only; unknown errors/timeouts fail safe, not memoized | Reuse; no rule regex or file-existence success inference |
| Real finalizer | `country.py:3958-4356` | Normalization, Relation alias, categorical mappings, kinship, v join, types and attrs | Reuse actual contract, no parallel schema interpreter |
| Required-column guard | `country.py:4993-5041` | Checks after finalization, allowing fields created there; GH323 Site4 tests and script-path tests | Reuse membership/placement principle; do not manufacture fields before checking |
| Alias handling | `country.py:8663+`, `:7104+`, `:7205+` | Normalizer undoes j-to-i when declaration requires j; canonical level aliases have their own consumers; rejected column spellings use metadata | Test actual supported semantics; do not invent alias equivalence |
| Related-table metadata | `country.py:3194-3207`, `:3533+`, `:4064-4069` | Even empty results may request sample/spatial data | Existing IO cost is material; no second finalizer pass |

Relevant prior ledgers: `1099-empty-subset.md`,
`1218-constructed-empty-explicit-types.md`,
`1218-current-development-integration.md`, `1281-wave-config-error.md`,
`1288-make-failed-recipe.md`, and `1078-food-producer-provenance.md`.
Their history and existing tests anchor the distinctions, not predicted counts.

## §3 Definitions and conventions in force

- STANDING sections 3-4 and current AGENTS govern schema, sanctioned IO,
  pre-finalize caches, sample-owned v and ID handling. Current AGENTS clarifies
  that L2-country i is already walked; preserve that current contract rather
  than repeating older baseline wording about all cache columns being raw.
- GH1099's absence means no declaration, not an exception or an empty file.
  This task adds a distinct success case and must preserve that distinction.
- GH1039 ruling, issue comment 6259386847: the framework assembles waves when
  no country builder exists. It does not authorize skipping a real builder.
- `_empty_declared_frame` is an answer constructor, not evidence a build was
  complete. Constructing declared columns before validation would conceal an
  actual missing required field.
- Zero rows means `len(df) == 0`. DataFrame.empty also includes a frame with
  rows but no columns; such a frame is not this task's empty answer.
- GH1078 ledger explicitly disallows public attrs sentinels, mutable globals
  and an instance last-origin field as substitutes for per-result provenance.
  An earlier scratch suggestion of a last-origin/receipt field therefore is
  withdrawn; no such field is approved by this ledger.
- Full finalization supplies fields that do not exist in a raw Wave result:
  Relation becomes Relationship, kinship creates Generation/Distance/Affinity,
  and sample may supply v. A blanket pre-finalize required-field test rejects
  valid supported inputs and is not an acceptable substitute.

## §4 Invariants and observed mechanism

1. Actual getter exceptions retain their existing handling. GH1281 KeyError
   remains a contextual RuntimeError with original cause; AttributeError keeps
   its existing compatibility path. No failure becomes absence.
2. A successful YAML zero-row result currently enters use_legacy at5524-5527.
   GH535 filters legacy t at5543-5548, so this source diagnosis does NOT prove
   the legacy W5 question is returned at PP. Extra IO and cold empty-request
   failure are supported; public question-substitution outcomes are unmeasured.
3. Empty results are discarded at5605-5606. Default DVC/manual descents pass the
   default country_builder=True; ordinary non-DVC YAML does too. With no rows
   they reach country fallback5658. Existing GH1099 cannot short-circuit
   Nigeria's declared education table.
4. Valid supported zero-row YAML cases must be fixed, including outputs whose
   supported aliases/finalizer complete their schema. Leaving them on legacy
   solely because a raw-shape predicate is easier preserves the defect.
5. A malformed empty, missing required payload, failed extraction, script
   missing-result or row-bearing zero-column frame must never be filled into
   success by the declared-empty constructor. Optional absence stays optional.
6. Keep JSON semantics, unknown/invalid waves, genuine/unknown country builders,
   full/all-wave-empty failures and valid populated cache unions unchanged.
7. A coverage-union rebuild may legitimately write populated union content
   while serving an empty requested subset. Do not impose blanket zero writes.
   A constructed answer alone must never replace bytes/coverage for other waves.
8. No worker-local closure mutable set may carry proof back to the parent.
   Serial and parallel paths need the same explicit outcome evidence. The
   outer two-value protocol is fixed; a nested carrier is not automatically
   transparent and must be audited through all frame consumers.
9. Keep warm gates before additional declaration/producer inspection. Broad
   build hash impact remains visible; no new exclusion or fake schema bump.

## §5 Reuse decision and concrete patch sketch for review

| Need | Decision | Reason |
|---|---|---|
| Valid empty answer type/order/no-write behavior | Reuse typed constructor and marker | Existing tested schema/serialization policy |
| No country builder | Reuse conservative probe | Actual rule authority, grouped targets and unknown cases already handled |
| Actual producer proof through getters | Extend existing private origin, subject to root protocol choice | Truthy resource config plus an empty frame is insufficient; direct grab_data bypass and mutable last-origin are ruled out |
| Schema completeness | Reuse real normalization/finalization and required membership | Aliases/kinship/sample-v must be honored, malformed schema must not be fabricated |
| Hash invalidation | Reuse tagged orchestrator/loader closure | Routing is content-producing and broad cost must be measured |

Proposed source edit sequence AFTER root chooses the unresolved mechanics:

1. Bind producer proof to the same invocation of the real Wave getter.
   Extend the existing private per-result origin return through actual getter
   contracts; default public getters and DataFrame attrs remain unchanged.
   Resolve YAML-cache origin versus script-parquet origin explicitly, and
   preserve cluster/food wrappers and aliases. This is not yet a concrete
   approved signature/carrier edit: `_with_origin` is currently a grab_data
   option consumed by food_acquired, not a generic Country getter protocol.
2. In build_wave, suppress the second legacy build only for a genuinely
   successful zero-row YAML outcome. Keep None, missing/script and exceptions
   on their existing paths. Propagate the original witness explicitly; do not
   replace it with a full declared schema before validity is checked.
3. Account for all actual build_waves outcomes, including union members. Keep
   populated results on existing concat/write/restrict routes. Only a fully
   proven barren strict subset with completed no-builder proof may become the
   existing non-cacheable declared answer; preserve full-empty failure.
4. Validate original zero-row witnesses through the actual supported schema
   contract before a constructor can supply missing columns. Finalize exactly
   once on the normal path. Placement relative to constructor and write guards
   is an unresolved design dependency, not permission to add a second pass or
   duplicate a schema-only interpretation of finalizer behavior.

### Phase-1 design gaps resolved by the root decision below

- **Producer transport:** the safe extension of `_with_origin` through generic
  and special actual getters, plus each parallel frame consumer, has not been
  selected. A mutable last-origin field contradicts GH1078; a tuple wrapper
  cannot be assumed transparent. No carrier implementation is authorized.
- **Validation placement:** existing required checking is post-finalize, but
  country writes occur before the finalizer and the declared constructor adds
  the very fields being checked. A design must retain the original schema
  witness until checked, keep it out of cache, support aliases/kinship/sample-v
  and avoid an extra finalizer/source-IO pass. The current sketch does not
  claim this ordering solved. A pre-finalize raw-field check or a second full
  finalizer is explicitly not the accepted shortcut.

## §6 Root review questions and finite verification proposal

Root owns the two design choices above; there is no newly inferred survey
policy question. This phase ends with ledger review, before implementation.
Nigeria owner owns the five-PH-source oracle and sample closure. No imports,
tests, census, source reads or cache operations are released by this ledger.

After source/protocol review, prepare identical synthetic counterfactual
carriers on d65 framework with recovered Nigeria config versus the candidate
framework with the SAME config. Preserve actual Country/Wave/getter/finalizer behavior;
stub only sanctioned source leaves and observable subprocess boundaries.

- Successful zero-row YAML must avoid legacy execution for dynamic, food and
  cluster getters, including supported alias and finalizer-dependent fields.
- Malformed shaped empty and missing required payload fail without constructor
  laundering. Cover absent optional fields, all-null-but-present payload,
  row-bearing zero-column frames and missing/script/None outcomes separately.
- Both backends: cold, full/partial/stale/bypassed country cache, no-stage and
  DVC-exception writes; PP-only, PH-only, mixed and all-wave requests; fresh
  Country instances and covered cache hits. Preserve full union when rebuilt;
  never stamp a constructed empty with populated union coverage.
- Actual source/config errors, unknown/missing Make probe, real/broken country
  builders, JSON and invalid requests retain original behavior and causes.
- Exercise serial and prebuild/relay paths, warning/ledger/attrs preservation,
  and ensure no producer receipt is reused across requests or wave instances.
- Reuse GH1039/1099/1218 tests, GH1281 tests, GH1288 Make failure controls,
  GH891 coverage tests, invalid-wave tests, GH1078 producer tests,
  test_wave_getattr.py, test_parallel_waves.py and recovered GH1292 Wave tests.
  Extend discriminating tests; do not rewrite scientific pins to match results.

The hash-impact plan reuses the reviewed metadata census: exact baseline and
candidate processes, complete ordered Country/framework/Wave maps, identical
keysets and per-map errors retained, empty private data roots, and exhaustive
tracked CSV plus DVC sidecar metadata permissions (including Data1/outside Data).
Raw/source payload/network access refuses. Verify source/runtime identities
before/after and keep guard refusals explicit. Previous 549/40/2109 counts are
historical, not this task's measured results. No full corpus build is implied.

### Phase 3 -- verification status

Implementation/test freeze: `52eb6c6ee8c9485680a9a4b98ecfa9d69700cff6`.
Only `country.py` and `tests/test_gh1292_successful_empty_country.py` change
scientific/runtime code. The tested working bytes were committed unchanged:
country SHA256 `6d322a026aa447b2916c06df38e6b0f034bdec51fb0199cee49b493036e31c6c`,
test SHA256 `fea8c8b6df9a23ea49a4ee0807b367dcad96b4582c76932e3d402e4992aa9472`.

- Reuse/definition checks (§§2-4): actual getters return private invocation
  origins; scheduler outer pairs are unchanged. Original normalized empty
  witnesses concatenate before exactly one normal finalizer and the shared
  required-membership guard. No declared payload is filled, and the existing
  zero-row no-write marker protects both country write sites.
- Root clarified aggregate-level membership: complementary original empty
  schemas may satisfy the same union contract as populated Country assembly.
  A single malformed witness or a union entirely missing a required payload
  fails. There is no new per-wave required-schema policy or extra finalizer.
- Strict-subset classification uses the original API request, even when a
  stale cache's manifest expands the internal rebuild union to all waves.
  A proven empty subset preserves that existing country cache; an original
  all-wave empty request retains its failure behavior.
- Guarded finite cohort: **133 passed** (11.05 s). This includes 35 new
  actual Country/Wave/extraction/finalizer/cache tests and focused existing
  Wave getter, GH1281 config-error, GH1078 producer, GH1039/GH1218 typed-empty
  and scheduler accounting/grouping controls. Capture/relay includes pickle
  transport, warnings and attrs; no actual multi-CPU pool was run.
- Same 35 new tests with the exact `f02936b0d` country module supplied by a
  private import loader: **21 expected failures, 14 passes** (10.64 s).
  Identical configurations/assertions were retained. This is an explicit
  single-module baseline counterfactual, not a claim of a separate complete
  baseline checkout or a hash-impact measurement.
- Both runs used the approved `/local/job40561868/venv_img/bin/python`, exact
  package/config/data identity assertions, private synthetic files, one CPU
  and numerical/build threads 1; source bytes matched before/after and guard
  denials were zero. Real source/cache/provider/network access and unapproved
  subprocesses refused. Only tracked metadata and new private synthetic
  parquet/CSV/config/temp files were permitted. Loader setup failures and
  fixture corrections are retained separately, not counted as production
  test evidence.

Durable evidence:
`/global/scratch/fsa/fc_jevons/ligon/burst/isa-country-owners-20261028/framework-empty-success-52eb6c6e/`.
It includes launchers, the frozen baseline module, exact selections, identity,
source hashes, JUnit, logs, guard results and report. The broader metadata
hash census, native Nigeria same-config public API carrier, actual fork-pool
execution, independent review and real-source scientific acceptance remain
subsequent coordinated stages. No hash exclusion or schema bump was added.

Sue

## Review correction -- witnessed empties on the Make backend

Root accepted the independent supported-route counterexample as an unmet
contract in this task, also present before 52eb. With `materialize: make`, an
existing country Makefile and unavailable `make`, the established helper
returns False under its executable-builder policy. The positive-empty gate
correctly refuses unknown rule absence, but `country_builder=False` then
constructs declaration columns over the original witness (country.py:5701).
Reviewer `review-framework1292-52eb/run-v2.log` records both valid-empty and
missing-required cases unexpectedly succeeding, with zero guard denials.

Reuse the existing successful-empty witness and failure machinery: when the
positive-empty proof does not return its original witness, the old no-builder
constructor must reject any retained positive witness. Do not change genuine
declaration absence, None/script/error behavior, the Make probe, finalization
count or aggregate schema membership. A positive-empty plus a failed declared
outcome must likewise not become a fabricated declaration answer.

Add supported Make-backend controls for unknown-builder valid/malformed empty,
completed no-builder valid/malformed empty, genuine declaration absence, and
positive-empty plus declared missing output. Retain the exact new tests against
the unchanged 52eb module as baseline, then verify the focused existing cohort
once. Root authorizes this narrow correction and finite synthetic checks.
After publication, run only the corrected candidate's full metadata census
under the reviewed v4 guard and compare with the verified original d65 maps;
preserve every original packet and failed dispatch roster unchanged.

## Root implementation decision -- 2026-10-28

The two source-design questions above are now resolved sufficiently for a
focused implementation and counterfactual tests. This supersedes the phase-1
hold; it does not authorize real-source reads or scientific acceptance.

- Extend the existing private `_with_origin` keyword through the actual
  dynamic Wave getter and the two explicit table getters (`food_acquired`,
  `cluster_features`). Keep default public returns and processing unchanged.
  The dynamic default must still call `grab_data(name)` without the private
  keyword, preserving existing overrides. Country requests the private return
  only from getters that explicitly support it; unsupported overrides retain
  the old conservative route. Bind proof to this same invocation, never a
  later cache-existence check or mutable last-origin field.
- A truthy YAML resource declaration together with that invocation's
  `memory`, `parquet` or `materialized` origin identifies YAML production;
  `missing`, script resources and unsupported overrides do not prove it.
  A successful zero-row result must avoid the second legacy build. Do not use
  `DataFrame.empty` to classify row-bearing, zero-column frames as zero rows.
- Keep `_parallel_waves`' outer `(has_table, result)` protocol. Its actual
  `_build_captured` and `relay` implementations only store and return the
  second value; neither interprets the frame. Carry per-result proof inside
  that second value, with tests of capture/relay and serial behavior. No
  production change to the scheduler module is needed.
- Retain the original normalized zero-row schema witness, rather than
  constructing declared payload columns. In a strict subset, only when all
  requested outcomes are accounted for as absent or successful YAML empty,
  at least one is successful, and the existing builder probe completes with
  no builder, return the original empty witness. Real/unknown builders,
  failed or script outcomes and all-wave empty requests retain prior behavior.
  Nonempty union assembly retains its existing path and write semantics.
- Reuse `_CONSTRUCTED_EMPTY_ATTR` as the existing zero-row answer/no-write
  marker, widening its private documentation to include an original empty
  witness. It is not producer provenance. This protects both pre-finalize
  cache writes without fabricating a schema. Capture the validation condition
  locally before finalization; finalize exactly once, then reuse the required
  membership check for this successful-empty answer. Missing required payload
  must raise, optional absence may pass, and supported finalizer-produced
  fields must pass. Extend the guard with an honest YAML diagnostic rather
  than claiming every error is a stale script parquet.

Implementation must include meaningful baseline-red/candidate-green actual
Country/Wave routing tests, getter default compatibility, aliases/kinship/v,
missing required versus optional fields, cache non-overwrite/union coverage,
full-empty failure, legacy/script and unknown-builder controls. Test only
synthetic fixtures under the approved interpreter with private runtime paths
and one CPU; real source/API acceptance and the broader metadata hash census
remain subsequent coordinated stages. Root will independently review the
implementation before publication.

Sue
