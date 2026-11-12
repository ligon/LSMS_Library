# Prior-Art Ledger — GH1454 served-unit build dependency

**Source:**11714b185fe2c90f1bc6c60164aa40d358acf0f3. Search tier: git/ripgrep/AST floor.
**State:** approved repair frozen/tested at f450039e813eca991f5845e9c3f55128c8fa3736; independent review/release pending. No source builds or census.

## §1 Task
Version the categorical mapper actually baked into EHCVM sized-unit parquets by
`carry_unit_size`→`served_unit_labels`, without changing its output or the
post-read mapper, global schema version, registry exclusions or other1454 policy.

## §2 Existing machinery
- `ehcvm.py:461–478`: served_unit_labels locally imports Country, creates its
  two-level probe and invokes `_apply_categorical_mappings`; `carry_unit_size`
  uses the result before writing. Existing sized-unit tests exercise this path.
- `_build_registry.py:_import_map/_closure_parts/_resolve_methods`: resolve
  explicit function imports and methods on classes defined in that function's
  own module. Importing an external Country class supplies no function code
  and misses its method; this is a static resolution gap, not a dynamic policy.
- `framework_imports_fingerprint`: existing selective script/mapping framework
  closure, used in both Wave._input_hash and Country._table_cache_hash.
- `country.py:Country._apply_categorical_mappings`: single existing mapper,
  numeric-key augmentation, reserved sentinels, label variants, attrs handling.
- `tests/test_build_transform_hash.py`: source-semantic mutation by monkeypatch
  inspect.getsource + clear_caches; no real output build required.
- `SkunkWorks/build_transform_cache_hash.org`: hash callable closure across
  modules; do not hash all generic country.py or tag every use globally.

## §3 Definitions
Inherit STANDING.md§3–4 and AGENTS.md Cache Behavior. Cached outputs are
pre-finalize; normal post-read-only transforms do not invalidate caches. The
AGENTS.md GH1224 exception and ehcvm.carry_unit_size docstring currently require
manual schema bump because this actual prewrite dependency is missed. Replace
that caveat only once the narrow automatic closure is proved.

## §4 Invariants
No changes to mapper, size policy, codebooks, native quantities, frames or
countries/config. Keep empty probe fastpath, output object-array and exact
Country(name) construction/call semantics. No new build_transform root, registry
mechanism/exclusion, schema bump, or generalized class resolution. Unrelated
build roots and scripts must not acquire this mapper dependency; actual complete
hash impact is unmeasured until separately reviewed census. No raw/API/cache IO.

## §5 Reuse
**Extend existing call structure** with a tiny country-module function calling
Country(name)._apply_categorical_mappings(frame), imported locally by the same
EHCVM caller. Existing own-module method resolution can then reach the mapper
and its own helpers. **Reuse** original mapper unchanged and original registry
unchanged. Pure tests will discriminate semantic mapper/helper changes from
post-read-only finalizer edits, plus delegated output/input conservation.

## §6 Decisions
No scientific/default decision needed. ROOT reviews proposed source/test diff
and anticipated scope before expensive full metadata census or release work.

## Phase3
- **OK (§2–§5):** country-module delegation reaches the unchanged mapper and
  `_augment_numeric_code_keys`/`_categorical_key_column` through existing resolver.
  Existing Country AST is exact after removing only the new helper; other EHCVM
  executable AST is exact after inverting only its local import/call.
- **OK (§4):** six new pure controls PASS/0skip at f450, 15.590s wrapper.
  Semantic mapper-source mutation moves the EHCVM import fingerprint; unrelated
  script closure and food/sample global fingerprints stay exact. Post-read
  finalizer-source mutation stays outside that EHCVM closure. Delegation/probe/
  sized-bare-null values, dtypes, attrs, empty behavior and inputs are preserved.
- **OK (§2/§4):**150 unchanged companions PASS/0skip in17.13s pytest,
  23.578s wrapper: selected EHCVM size/hook/wiring controls, hermetic key resolver
  and12 pure registry controls. Omitted derived API dispatch and real-Country
  hash-grid fixtures. This is not raw-source/API/build/census evidence.
- Both runs used MAIN/private roots/unshare/threads1/n2loadfile, exact package
  and countries-root identities; source clean/datafiles0. Import created a
  private config/s3_creds placeholder; configuration is not claimed empty.
- Registry/schema/config and all scientific/default rules unchanged. No new
  global transform tag/exclusion or schema bump. Complete actual hash impact
  UNMEASURED; existing whole-wave/country aggregation may broaden invalidation
  within countries importing the EHCVM path. ROOT owns further review/census.
- Receipts: reports/1454-served-unit-fingerprint-proposal-20261109 under the
  durable isa-sm-20261103 burst root, six-v1 and companions-v1. No failed runtime
  attempts; an initial source-edit assertion was corrected before execution
  without changing other Country imports or running tests on partial edits.
