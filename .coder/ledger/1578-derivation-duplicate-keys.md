# GH1578 — refuse duplicate members only in derivation registries

Ledger-first isolatedfix/1578-derivation-duplicate-keys-20261108 at authoritative
GitHub728992c7697bdf25286e3d63a7ae70092829a843. Searchtiergit/ripgrep/AST;
inherits STANDING§§3–4,CLAUDE,canonicaldata_info and derivations contracts.
Fertilizer33c/06d0 and operators/CI41019974 remain immutable and monitored.

## §1 Task and authority

FreshGH1578/body/all0comments read. Registry union merges can leave duplicate
keys silently overwriting earlier evidence. Rootauthorizes registry-only explicit
duplicate refusal, actionablefile/key/location, nestedmappings included. No
scientificdefinition/registryrewrite/genericYAML/parser/globalcache/source change.

## §2 Reuse/priorart

- derivations._load,derivation_records/framework_records lru-caches,from_config
  record validation,recordimmutability,missingfile andcache_clear semantics.
- yaml_utils.SchemeLoader(yaml.SafeLoader),including existing!make constructor.
  Genericload_yaml remains unchanged; new validation opts in ONLYat _load.
- tests/test_no_editable_install._duplicate_workflow_keys walks raw YAML nodes
  BEFOREoverwrite. Reuse the idea, not its BaseLoader/textual-key comparison:
  registrykeys needactualSafeLoader scalar equality and aliases/cycles/merges.
- cli._OrderedLoader flattens mappings but does NOT reject duplicates; cannot
  reuse it as a duplicateguard. test_schema_consistency regex is DataScheme-only.
- Existingtest_derivations covers loader/records/cache/publicmetadata entrypoints;
  GH812 only pins one registry occurrence, not general duplicate prevention.
- docs/guide/derivations and module doc: one evidenced construction perkey; YAML
  metadata resolves callables lazily so registry enumeration must not import
  country producers or fetch/build data.

## §3 Actual compatibility ruling

15committed registry files include Uganda's legitimate inherited anchor plus
explicitcolumns override, Nigeria whole-record/raw-variable aliases. Root
CONFIRMS preserve all supported nonduplicateSchemeLoader behavior and intentional
merge inheritance/explicit overrides. Check raw MappingNode members BEFORE any
flattening; flattened duplicates are not proof of explicitduplicate declarations.
One merge-sequence may inherit overlapping keys according to existing precedence;
each raw source mapping must still have unique explicit members. Scalar aliases
and quoted/equivalentresolved keys that collide in a Python mapping are duplicates.
Actual countryrecordcontent is parsed as opaque registry schema, not interpreted
scientifically; no country-specific semantic/source fixture or producer admitted.

## §4 Invariants

Genericload_yaml/SchemeLoader constructors/resolvers/callers remain byteexact.
DerivationRecord/from_config/normalization/ordering and public signatures unchanged.
Country/framework/missingfile/falseyempty/malformed/cache_clear/exceptionretry
semantics remain, except explicit duplicate files now fail instead of overwrite.
No registry values/definition edits, callable resolution/native/CountryAPI/Make/
warm/DVC/dependency/shared-cache/GitHub writes. Errors carry filename,resolvedkey,
first and repeated source locations (aliasoccurrencemarks need care).

## §5 Minimal design/acceptance

Use a private SchemeLoader subclass for derivations._load ONLY. Validate raw
composed mapping graph before SafeLoader flattening, with visited-node tracking
foraliases/cycles; construct scalar keys using the same loader to detect true
resolved-key collisions. Keep merge directive distinct from quotedliteral<<.
Capture actual alias-key occurrence marks through composition if needed rather
than misreporting an anchor definition as the repeated use. Reuse existing
ConstructorError with source marks. Delegate normal construction unchanged.

Inventedcontrols top/nested/flow/quoted/aliased/resolved-equivalent keys, malformed/
unhashablekeys,anchors/mergesequences/overrides/cycles/!make/tags,missingfiles and
cache_clear/refusalretry,framework and combined metadataentrypoints; genericYAML
lastwins unchanged. Compare EVERYvalid committed registry record to exactbaseline
loader, including types/fields/order,without resolving/importing its callables.
Hashscope must be measuredmetadata-only, no read-path0/N assertion by intuition.
Use authenticatedclosedcurrentBFAmap if available, otherwise boundedfreshbaseline.

## §6 Executed evidence and remaining gates

44new controls are green:43passed13.14s in finalsource stage plus1corrected
publicentrypoint control5.90s.28existingkey/record/registrycontrols passed in
initialstage; no overlap is added as a claimed single-run count. Finalcorpus
2controls passed6.74s:ALL15committed files/81valid DerivationRecords, including
2framework records, match exact728baseline typedfields/recordtypes/order;
recordclass/allothercallable definitions and yaml_utils bytes exact. No registry
rewrites,producerimports orcallableresolution. The guide's old “currentlyempty”
framework claim contradicted these2records and is removed, not silently ignored.
StrictAPIaudit exit0 on finalproductionbytes. Named source/API/write guards
remain active0denials, private datafiles0; alltestactorsclosed beforefreeze.

Preserved attempts:initial28existingpass/37newsetuperrors imported public
ll.derivations function asmodule; documentedGH883 import_module escape hatch
corrects only the new fixture. Initialexternal census also failedcollection
for that import and is preserved. Corrected37newcases passed11.79s. Expanded
final43pass/1newfixturefail patchedll.countries, while actual table uses
catalog.countries. Correct actualdependency with identical assertions; corrected
casepasses. Production unchanged by those fixture fixes. Native merge-tag
handling follows installedSafeLoader tag-based semantics even for taggedcomplex
key shapes; explicitmembers are checked before flattening.

Root's authenticated728closedbaseline-map admission permits candidate-only
450/1895/35/186 metadata census, preservingoriginalmeasuredhead. Hashcost remains
UNRUN at runtimefreeze, never a guessed0/N. Rootpreflight REQUIRED before any
CI/census launch; existing acceptedoperators with only lane/namespace changes.
Independentreview/fullCI/GitHub/currentbase composition are separategates.
Allsource/native/API/Make/warm/shared-cache/GitHub campaigns remain unrun.


## §7 Phase 3 — measured composition and failed full-suite gate (2026-11-08)

This addendum supersedes only the runtime-freeze UNRUN statuses in §6. The
runtime source head85b07a0328cd8fedcf38c4864945bb317fcbbacd remains frozen.
The final two-control corpus run was6.74s; the old6.98s entry referred to the
preceding corpus attempt. TIMING-ADDENDUM.json preserves that correction.

Independent review CLOSED PASS:46 new distinct controls, no P1/P2 findings,
no skips/deselection, all15registry files/81typed ordered records exact baseline.
Six source/API/write guard receipts were active with0denials/private files0.
See reports/review-1578-duplicate-registry-20261108/{VERDICT.md,evidence.json}.

An ordinary composition with current fa76ee4364d3e27cdbe08f0bb1d2014b060a8f9b
froze c8fb35ca957432d4bd59e1beeed2bedc0b3c6d9a. Four owned paths and five
incoming paths were disjoint; all mode/type/blob identities were proven, and the
filesystem verification ref read back exact before actors launched. No rebase,
force push, registry rewrite or scientific definition change was involved.
Candidate-only source-free metadata census CLOSED0 in123.121457s: differences
0/450country,0/1895wave,0/35build,0/186import fingerprints;0errors/denials,
18None and public metadata surface unchanged. The authenticated closed baseline
map and original measured-header provenance were retained; no baseline replay.

The full offline CI gate is FAILED, not green: job41020419 FAILED1:0,21:10,
12030passed,1failed,5652skipped,4xfailed in1115.40s. JUnit contains17687cases,
1failure,0errors,5656skips including the4expected failures. All44new registry
controls passed. Exact source/operator/Make/test bytes and clean head receipts
are retained in reports/1578-current-verification-20261108, along with the
original logs/JUnit and FAILED-FULL-CI-RESULTS.json.

The sole failure was the original GH1171 inert GNU Make test
`test_undeclared_fct_target_refuses_without_running_nutrition`: the original
subprocess.run timeout15s expired before stdout/stderr were captured. Both the
entire test file and real Ethiopia Makefile are byte-identical between fa76 and
c8fb. The fixture constructs only invented scripts/paths and does not load a
derivation registry. This classifies the observed failure path; it does not prove
a system cause or establish a flake. The blocked operation/PID and ambient Make
settings were not captured by that failed actor. FAILURE-CLASSIFICATION.md
preserves those limits. No fixture, assertion, timeout or product code changed.

Root authorized preparation of one same-node fa76/c8fb counterfactual, exactly
once per head with the original15s timeout and all original assertions. Its
operator is separately frozen at reports/1578-fct-counterfactual-20261108;
ROOT preflight is pending and the diagnostic has NOT launched at this ledger
freeze. Targeted results cannot relabel the failed full suite; any replacement
full CI requires root review of the diagnostic evidence first.

All original runtime/review/verification trees and failed stage receipts remain
immutable. No source acquisition, Country API, raw decode, native/scientific
Make target, warm cache, dependency, shared-cache or GitHub write was performed
by this phase. Final GitHub checks/publication and a green full-suite gate remain
unmet; this is draft readiness with an explicit failed verification gate.


## §8 Final current-development verification (2026-11-08)

This addendum extends the historically correct §7 frozen state; the original
failed full suite and every failed fixture/operator attempt remain preserved.
No runtime, test, registry, configuration or generic YAML-loader change follows
this ledger-only finalization.

The bounded diagnostic's V2 job41021744 failed before pytest/Make: its harness
mistook legitimate empty cache directories for files. It executed zero original
tests and zero Make processes. A separately frozen V3 corrected only that
private-layout validation. Three source-free layout controls accepted the real
two empty directories and refused a seeded regular file and symlink L1.
V3 job41021945 completed0:0 on the original n0048.savio4 host. Baselinefa76
and candidatec8fb each ran the original unchanged15s test once and passed;
their single Make children returned expected refusal2 in0.290536/0.212962s.
All13 source/API/write guard bindings per case remained active with0denials.
This serial comparison did not reproduce the full-load timeout. Its historical
system cause remains UNPROVEN; the old fullCI41020419 remains FAILED1:0.

An ordinary composition a7119e19bb147e78487b3ee288cecb9de8f08aef joins final
ledger headc88db92e3e3266a60172e88897a8df36c2d7b858 with current development
b8346cf1f953ac82517b1b7473c715a5655ca950. All19113 mode/type/blob entries
match the disjoint union: four owned paths, twelve incoming paths, no overlap.
No manual runtime changes or rebase were used.

Fresh metadata-only actors measured actual b834 and a711 separately under
private roots, false source credentials and network isolation:0/450country,
0/1895wave,0/35build,0/186import fingerprints changed; all18None cells and
public553country/2313wave/39method surface entries match. Each actor closed0,
with0errors/denials/private data files, in123.952211/125.393419s. The guarded
strict API audit closed0 in9.612560s. These are metadata checks, not table data,
Country/Feature builds or source verification.

Fresh full offline CI41022082 COMPLETED0:0 in14:24 on n0055.savio4. The full
configured test directory ran with four loadfile workers, BLAS/build threads1,
a fresh mounted image, private nonsymlink L1/L2/config, false reader credentials
and unshare-rn:12108passed/5652skipped/4xfailed in693.80s. JUnit17764 cases
contains zero failures/errors; all44 new registry controls passed. The source
skips establish no real-source scientific acceptance.

All17687 old case IDs remain. The77 additional cases are exactly53 Nigeria
safe-harvest and24 market-refusal cases merged since c8fb. Thus12030 original
passes + the original timeout case now passing +77 incoming =12108 passes;
skips/xfails are unchanged. The44 owned registry cases are the sole test-file
addition relative to b834. The17720-case complement in candidate JUnit is not
claimed as a separately executed baseline collection.

The prior independent46-control PASS/noP1P2 review and exact15-registry/
81-record typed-order corpus parity remain applicable: all tested runtime and
test objects are unchanged. All verification actors are closed and frozen
operators remain exact. Final GitHub publication/checks and expected-head
merge are the remaining root-managed gates.

Durable packets (absolute root):
/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/reports/
- 1578-fct-counterfactual-20261108-v2 (zero-test failure and addendum)
- 1578-layout-control-20261108-v3 and1578-fct-counterfactual-20261108-v3
- 1578-b834-composition-proof-20261108
- 1578-b834-verification-20261108 (fullCI/JUnit/count reconciliation/API/maps)
- 1578-finalization-20261108 (root final-object and publication receipts)

No real source/getter/raw decode/scientific Make/native builder/warm campaign,
production-weight change, master release or shared cache re-warm is claimed.
