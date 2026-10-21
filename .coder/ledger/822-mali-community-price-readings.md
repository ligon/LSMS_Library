# Prior-Art Ledger - GH822 Mali community-price readings 

Search tier: ripgrep/git floor, direct file and AST-aware source review.
The read-only diagnosis preceded the approved implementation brief.
Inherits `.coder/ledger/STANDING.md`, `CLAUDE.md` and canonical `lsms_library/data_info.yml`.

## §1 Task, restated

Restore 2014-15 Mali EAC-I community price readings1/2 beside current reading3,
with a native integer obs level while preserving variety code, passage,
Description, per-lot Price and lot-weight Quantity. Separate #821 owns2018-19.

## §2 Existing machinery

| Symbol | Anchor | Tested | Decision |
|---|---|---|---|
| community_prices_finalize | Mali/_/mali.py:826 | test_community_prices_variety.py declaration/static/built tiers | extend index only; reuse mapping/filter/shim |
| _crop_labels / _unit_labels | Mali/_/mali.py:305/:313 | exercised by real community builds | reuse unchanged label policy |
| COMMUNITY_PRICES_INDEX | Mali/_/mali.py:823 | declaration/static/built tests | extend by obs |
| wave script | Mali/2014-15/_/community_prices.py:44 | real built tests | extend explicit triples; decode missing unit labels |
| country concat | Mali/_/community_prices.py:21 | built tests | reuse concat, update index contract text |
| Feature canonical alignment | lsms_library/data_info.yml:166 | test_community_prices_variety.py::TestAssembly | reuse, no framework edit |
| source reading/writing | STANDING.md §2/§4 | IO/cache suites | reuse get_dataframe/get_data_file/to_parquet |

## §3 Definitions in force

- CONTENTS.org:721 identifies triples q03/04/05, q06/07/08 and q09/10/11.
- CONTENTS.org:766 records GH840: add the level, select nothing; numeric
  variety code is stable while Description quotes differ by passage.
- CONTENTS.org:801 records questionnaire Poids(KG), not NumberOfUnits.
- Canonical index/sentinels already include obs at data_info.yml:166/:288.
- Exact source metadata and aggregates are private `summary.json` and
  `decoded-summary.json`; PDF not newly verified, source definition reused.

## §4 Invariants and assumptions

- STANDING.md §4 sanctioned IO and runtime read-only config invariants apply.
- Preserve the existing i==v shim before j until finalization removes it.
- No selection or deduplication of separately reported form slots.
- All original reading3 rows and full dropped-key sets remain unchanged.
- Current unmapped-unit/item/nonpositive-price and Quantity-sentinel policy
  is unchanged. Positive price alone does not prove an unresolved-unit answer valid.
- Decode only p2 missing-label unit fields from the common community codebook;
  codes99/out-of-range/fractional remain unresolved. Never round unknown codes.
- No2018-19 wiring, canonical/framework edits, source fixtures or dependencies.

## §5 Reuse decision

Extend the native grain and slot assembly; reuse all existing finalization,
canonical alignment, IO and filtering machinery. A narrowly scoped source-unit
label restoration is needed because two distributed Stata columns lack their
dictionaries; no existing country mapping keys numeric community unit codes.

## §6 Approved boundary and residual

The dispatcher approved retention of the existing unknown-unit filter and
exact old reading3 keep/drop sets. The129classified entries remain excluded,
with a separate diagnosis-first residual drafted in the evidence directory.
Mali-only shared test expectation updates are authorized; Niger regions stay
unchanged. The implementation follows DESIGN.md in the same task evidence.
