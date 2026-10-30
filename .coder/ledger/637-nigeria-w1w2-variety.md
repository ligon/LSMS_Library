# Prior-Art Ledger — GH637 Nigeria W1/W2 native price variety

Search tier: git/ripgrep + source AST floor. Source-plan baseline development
26803b81050df527add7ed5908db6c827a65eed6. No product/data/test execution yet.

## §1 Task
Implement maintainer comment6368100768: W1/W2 community price `variety` is the
original item code, including its native size digit. Distinct source answers
must not be first-kept under one harmonized j. This is a settled identity rule,
not a new unit/size/eligibility or price-policy decision.

## §2 Existing machinery
- Nigeria `community_prices_for_wave`2817: one shared helper, mode names for
  W1/W2 and codes for W3–W5. Current3049–3068 groupby lacks variety and takes
  first Price. `_resolve_w1w2_price_item`2806 maps full code then code//10 to j/u;
  retain this mapping, but variety must keep the FULL original code.
- Drivers2010-11/2012-13 `_ /community_prices.py` (without the space): each
  reads one CSV and calls mode names; PH quarters2011Q1/2013Q1. Country compiler
  concatenates all five waves and sorts. Registry index currently t,v,j,u,obs.
- Tanzania `community_prices_for_wave`2157–2263: native item code as string
  variety; assert full source key unique, no first selection. Tests:
  test_community_prices_variety.py declaration/builder/assembly assertions.
- Niger `S05_VARIETY`1628 and `_s05_community_prices`: canonical
  unknown_variety on a wave that lacks a finer variety, inside a country whose
  other waves carry native varieties. This is the needed mixed-wave precedent.
- Feature `_missing_level_sentinels`231 and `_harmonize_country_frame`:
  canonical community_prices sentinel variety unknown_variety, visit1. Current
  W3–W5 Feature identity already uses this sentinel. Retain it explicitly when
  adding Nigeria's uniform native variety axis; retain W3 native obs1/2.
- Existing tests test_gh637_nigeria_w3_obs.py explicitly pin old W1/W2 first
  selection as out-of-scope then. Replace that obsolete pin deliberately; keep
  W3 uniqueness/location/price assertions. test_nigeria_community_units.py and
  cluster/metric size tests protect1095/834. PR1192 ledger preserves first grain;
  it does not implement this task.

## §3 Definitions
Inherit STANDING.md, CLAUDE.md, data_info.yml. Canonical community_prices index
is t,v,j,u,variety,visit,obs; variety is the SOURCE code as string. j remains
harmonized. obs is the location/form reading slot, not variety. C8 is PH-only,
cluster-grain with composite state/lga/EA v; i==v is only an internal shim to
protect j from map_index and must not reach the public index.

## §4 Invariants
Keep j/u lookup, Price conversion, positive-price/missing-cluster/item/unit
filters, all metric rescue/drop masks, Derivation payload/attrs, W3 physical
location obs, CSV text bytes and all source namespaces. W3–W5 source rows/Price/
Derivation/NA masks/order remain exact after removing ONLY the added constant
variety axis; their fully aligned Feature identities remain exact. No adoption
of their item codes as newly meaningful varieties. No invented variety when a
W1/W2 retained source item code is malformed; reject a nonintegral/nonfinite
identity rather than silently truncate, with no broad eligibility expansion.
Full native duplicate key must raise, even for equal Price; no new ordinal,
dedup rule, median, aggregation or unit fallback. Public APIs must retain
canonical named axes and strict exact comparator semantics. No household
identity inference or change to settled1357 exclusions.

## §5 Reuse/extend
Extend only shared price helper and country declared axis; reuse canonical
sentinel and Niger mixed-wave precedent, Tanzania native-code uniqueness,
existing reader/writer, compiler, metric normalization, map_index shim and
Feature alignment. Update stale driver/docs/declaration tests. No new framework
or shared schema definition is necessary. Source-only synthetic checks precede
any finite actual. Existing saved1192 witnesses need exact durable custody and
source compatibility before reuse; old local locators do not prove survival.

## §6 Open points
No unresolved human scientific choice for this ruled coordinate. Empirical
full-key duplicates/malformed retained codes, if discovered, must be reported,
not silently assigned an ordinal. Future actual admission still needs exact
current source custody/read matrix/operator and root release. No actual/data
work is authorized by this source plan.
