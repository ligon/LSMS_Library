# Prior-Art Ledger — GH1454 served-unit build dependency

**Source:**11714b185fe2c90f1bc6c60164aa40d358acf0f3. Search tier: git/ripgrep/AST floor.
**State:** design/proposed source-only repair; no source builds or census.

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
Pending. Source-only design; no tests/imports/builds/census executed yet.
