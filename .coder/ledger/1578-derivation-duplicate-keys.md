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

## §6 Gates

Rootexplicitmerge ruling resolves compatibility; no sciencechoice needed.
All tests privatefalsecreds/unshare/MAINvenv exactcwd/import/countries-root,
BLAS/build1/pytest2loadfile and namedsource/API/write refusals. Freeze tests,
validregistrycensus,hashcost and ledger before independentreview/publication.
