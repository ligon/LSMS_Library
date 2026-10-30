# Prior-Art Ledger — Mali 2021 price availability (GH #821)

Search tier: Git/ripgrep floor plus exact official World Bank catalog metadata.
Public base 88c7b540e30db924c20859343b519babf6a09016. CONTENTS read first, including
all TODO/WAITING/LOGBOOK matches (only the TODO keyword declaration is present).

## §1 Task
Reconcile remaining 2021 availability after merged #1356. No source acquisition,
cache probing, raw decode, API/build, new inputs or scientific policy change.

## §2 Existing machinery
Reuse 821-mali-community-prices.md and merged #1356/a06b7a62 for the completed 2018
implementation; do not duplicate it. Existing coverage row is unsure with
C1-partial/C4-notrun. Five authenticated prior ordinary files bind missing held
community blobs and measured regional-source schema. Official catalog 6275
F36-F40 now permits label inspection without those raw blobs; F48 corroborates
regional price keys. add-feature skill makes the World Bank dictionary the
primary source for module-variable location. STANDING.md governs IO/schema.

## §3 Definitions
EAC-I 2014/2017 differs from EHCVM 2018/2021. EHCVM grappe is sample.v; vague is
fielding period, not an invented cluster crosswalk. community_prices food/product
quotes are not the same concept as agricultural land rents/sales or wage rates.
Regional/milieu/time price data cannot be assigned to clusters by inference.

## §4 Invariants and contradictions
Preserve historical CONTENTS "2021-22 has no equivalent" and its later qualified
correction. Catalog schema evidence can advance the old unverified-label claim;
it cannot prove survey nonexistence or fielded/withheld/not-asked status.
s03 DOES contain land/labor amounts; never claim no price-like fields anywhere.
Keep unsure/C4-notrun; no loader, schema, unit factor or synthetic grappe.

## §5 Reuse decision
Documentation/coverage-reason update only. Existing 2018 code and all execution
remain untouched. No tests warranted for this reversible documentary update.

## §6 Remaining evidence
Obtain exact 2021 community/market instrument or provider clarification of any
cluster-price release. Catalog lists household questionnaires/BIDs only. Do not
rebuild 2018 or acquire all five raw community blobs just to rediscover known labels.

### Phase 3 verification
OK (§2/§4): six complete official variable lists contain exactly the catalog's
23/55/19/74/36/12 entries; archived HTML SHA256 and URL bindings are retained in
ordinary author evidence. Five selected prior ordinary files authenticate against
the closed SHA256SUMS manifest. The current five community sidecars match the
old declared MD5/size bindings. OK (§3/§4): s03 land/labor values explicitly
remain distinct; unsure/C4-notrun and all runtime/source boundaries are retained.
Whole-diff whitespace and exactly-one-coverage-row checks passed. No tests/data.
