# Prior-Art Ledger — GH1288 CI structural/test-contract corrections

**Source:**c821054706c26dd838dc08b47b39882da823cff5; searchgit/ripgrep/AST.
**State:** ROOT-approved test-only correction, before implementation/runtime.

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
Pending focused test-only verification. All original20CI failures preserved.
