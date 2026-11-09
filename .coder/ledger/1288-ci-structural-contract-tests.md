# Prior-Art Ledger — GH1288 CI structural/test-contract corrections

**Source:**c821054706c26dd838dc08b47b39882da823cff5; searchgit/ripgrep/AST.
**State:** test-only correction frozen/tested at87f054a466625a4fae61e411fa3f934639f4cda5; ROOT review/integration pending.

## §1 Task
Update six test modules for the reviewed isolated productive Make transaction.
Preserve exact output/coverage/refusal/no-write semantics; no product changes.

## §2 Machinery
- `country.py:prepare_make_waves/fresh_make_country`: selected native preparation
  precedes country compilation; genuine nonzero native failures propagate.
  Private temporary wave outputs are not promoted. Live Other inputs stay exact.
- `_materialize_make_yaml/_materialize_make_other`: reviewed dynamic-child build
  roots must appear in the exact pinned root set.
- `_read_l2_country_cache`: no stamp/write at read; fresh verified producer stamp
  is separate, inside fresh_make_country after validation/snapshot/verify.
- `load_dataframe_with_dvc` and `fresh_make_country`: exclusive rebuild descents,
  each reports once; whole-function source count conflates their definitions.
- Existing Niger recipe/compiler/positive controls, Uganda income[11,22] and
  coverage checks, subset refusal and GH809/GH920 runtime companions are reused.
- External static source/log report: reports/review-1288-ci-secondary-failures-20261109
  under durable isa-sm-20261103 burst root, manifest15215f90717d3f51a584eebb9e87668f485ad5e98cce974a94d33ad137b028e8.

## §3 Definitions
Inherit STANDING.md§3–4, AGENTS cache/build boundaries, accepted GH1288 native
producer/pre-finalize/current-input and live-Other conservation rulings.
No-return on native failure differs from the later wrapped country-Make error;
no change in scientific data/eligibility or cached payload follows.

## §4 Invariants
Own only test_build_transform_hash, test_niger_concatenator_refusal,
test_gh1062_uganda_sibling_writes, test_gh920_rebuild_warning,
test_make_country_wave_subset, test_gh809_hashless_country_parquet and this ledger.
Production/schema/config/exclusions remain exactc821. Keep all original exact
healthy payloads, nonzero failures, current hashes, declared coverage, no-fallback,
no selected live writes, and no stamp/write on warm cache hits. No broad catches,
wildcard root set or weakened tolerances. No real sources/cache/census/fullsuite.
Coordinate shared niger_tree fixture with1452 before changing it.

## §5 Reuse
Extend existing tests with precise route/stage structural assertions and private
invented-script trace/byte witnesses. Reuse all production methods unchanged.
Add a finite direct invented warm readgate spy only to close the specific
GH809 no-write-on-hit boundary, without real Country construction/data.

## §6 Open questions
None blocking ROOT-approved changes.1452 separately owns two authorOther and
nine strictrecipe cases; neither their source/tests/ledger is changed here.

## Phase3
- **OK (§2–§4):** exact2 child-dispatch entrypoint names added; exact-set equality retained.
- **OK (§4):** optional `niger_tree(..., trace=False)` preserves the SCRIPT
  constant and default native script bytes;1452 confirmed compatibility before
  their integration. Only four negative cases opt in. Original native
  CalledProcessError1/cmd and diagnostic survive; first2 prepared privately,
  third fails, no fourth/compiled result/live-wave or country writes.
  Other recipe-stop, compiler refusal/YAML/healthy controls remain exact and pass.
- **OK (§2–§4):** Uganda full income waves/[11,22] remain exact; saved live
  earnings/enterprise bytes/mtime/current hashes/one-wave manifests stay exact,
  while selected income has current all-wave hash/coverage.
- **OK (§4):** precise no-output RuntimeError text retained with no live target;
  both rebuild routes have one report before their respective build descent.
  Existing runtime warning/memo controls remain unchanged and pass.
- **OK (§2–§4):** GH809 reads actual normal/assumed-fresh gates, pins actual
  warm branches’ gate linkage/no write/stamp, and adds an invented direct
  twice-hit spy proving exact stored payload/bytes/mtime without stamp/write.
  Existing legacy→stale and wave legacy-stamp rules remain. The two real-cache
  delivered tests are unchanged and explicitly excluded from this pure run.
- **Actual focused run:**75PASS/0fail/error/skip,158.00s pytest/165.986s wrapper,
  outer0. MAIN/private fresh roots/unshare/threads1/n2loadfile;3 package/config
  identity records, source clean/private datafiles0. Import-created private
  config/s3_creds placeholder recorded; configuration is not claimed empty.
 44 warnings retained, including existing diagnostic/optional-module signals.
- All product/schema/config/exclusion mode/type/blob entries stay exactc821;
 19162 nonowned objects exact. Six tests+this new ledger are the only paths.
- Receipts: reports/1288-ci-structural-contract-tests-20261109 under durable
  isa-sm-20261103 burst root, focused-v1. No failed new runtime attempt,
  source acquisition/survey decoding/shared cache/census/fullsuite or1452
  owned strictrecipe/authorOther cohort replay. Original20CI failures remain
  preserved. ROOT owns further combined review/configuredCI/publication.
