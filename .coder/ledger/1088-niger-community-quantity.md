# Prior-Art Ledger - GH #1088: Niger CS07 quantity missing codes

Per-task ledger; inherits `STANDING.md` sections 3-4. Sue, 2026-10-21.
Search tier: ripgrep + git. No GitNexus server/index is available in this
lane. Approved design: campaign `1088-design.md`, recorded by the dispatcher
after the read-only held-source replay at baseline `f8d236694`.

## Section 1: Task, restated

Extend Niger's existing CS07 quantity mask to recognize exactly nominal
`9999.9` and its float32-promoted value `9999.900390625`. Preserve current
nonpositive, `9999` and `99999` handling. Preserve Price, all index keys,
the variety/visit/obs grain, and the existing duplicate-entry policy.

## Section 2: Existing machinery

| Symbol | Location | Existing behavior / test | Decision |
|---|---|---|---|
| `_community_price_triples` | `countries/Niger/_/niger.py:1376` | Reads quantity at 1454-1459; filters Price separately; drops exact duplicate entries after melting. `tests/test_community_prices_variety.py::TestNigerDuplicateEntries`. | Extend the existing quantity mask only. |
| `_COMMUNITY_PRICE_SENTINELS` | `countries/Niger/_/niger.py:1354` | Exact `9999.0`, `99999.0` codes, used for both existing quantity and Price handling. | Reuse unchanged for Price; extend a quantity-specific tuple. |
| `_finish_community_prices` | `countries/Niger/_/niger.py:1490` | Retains rows with missing quantity if Price and index keys remain usable; asserts unique grain. Existing declaration and built tests. | Reuse unchanged. |
| `_COMMUNITY_PRICE_CONTENT` | `countries/Niger/_/niger.py:1372` | Exact content key for second-record repeats, including Quantity and source slot. Existing duplicate-entry tests. | Reuse unchanged; verify normalization changes no held-source dedup outcome. |
| `Country._finalize_result` | `country.py:3815` | Applies the declared index and read-time harmonization. | Reuse unchanged in paired held-source replay. |
| `get_dataframe` / `get_data_file` | `local_tools.py:1686`, `data_access.py:2444` | Sanctioned source and metadata access, per `STANDING.md`. | Reuse, with copied held L1 and private writable data roots. |

## Section 3: Definitions and evidence in force

- `CONTENTS.org:1208`: "CS07's =Quantity= is a WEIGHT"; Price is the reported
  price for that quantity, not an additive measure or a per-unit price.
- `CONTENTS.org:1303` at baseline: "They stay =obs= 4-6 for now"; representing the 30
  nonrepeat second-entry observations is a decision for the maintainer.
- `CONTENTS.org:1307` at baseline and open GH #1088 already interpret nominal `9999.9`
  as a quantity missing code. This interpretation is an existing ruling,
  not new independent instrument confirmation in this task.
- Precision qualification: `float32(9999.0)` is exactly `9999.0`.
  `float32(9999.9)` promoted to double is `9999.900390625`. The held 2014
  quantity columns are declared Stata **double**, already holding the latter
  value; their earlier precision history is unproven. All three grids label
  the quantity columns "poids du produit"; no numeric value labels document
  a missing code. All four relevant questionnaire PDFs are absent from held
  shared L1 and were not acquired. The held World Bank `NER_ECVMA1.do` and
  `NER_ECVMA2.do` contain no CS07/comprix handling that confirms this code.
- API/index/IO conventions: `STANDING.md` sections 3-4, `CLAUDE.md`, and
  `lsms_library/data_info.yml` remain authoritative.

## Section 4: Invariants and approved boundary

- Exact quantity matches only: `(9999.0, 99999.0, 9999.9,
  float(np.float32(9999.9)))`. Normalize in the existing quantity mask inside
  `_community_price_triples`, after numeric conversion and before content
  duplicate detection. Never round, use a tolerance, clip a range, or coerce
  all quantities to float32.
- Price keeps its original `(9999.0, 99999.0)` policy. No unit/product-absence
  change; no grain/schema/framework edit; no decision about the nonrepeat tail.
- Read-only replay at clean `f8d236694`: three held raw grids have 23,409 /
  34,380 / 21,960 rows. They serve 46,043 rows (32,352 / 13,691 by wave).
  Exactly 76 served 2014 quantities are the encoded value: source slots
  43 / 32 / 1, 21 clusters, five varieties. Quantity-only normalization keeps
  every key, Price and other column identical, retains 15 + 15 `obs > 3`
  rows, and changes exactly those 76 quantities to NA.
- Negative baseline: private actual-helper regression fails because Quantity
  remains `9999.900390625` while the positive Price survives. Adjacent float32
  values `9999.8994140625` / `9999.9013671875` and ordinary nearby quantities
  must remain untouched. The held source itself has other quantities 9997
  and 9998, so broad all-nines/range logic would change unrelated values.
- Validation: baseline-red helper/finalizer cases; targeted existing tests;
  cold paired private builds and warm replay; duplicate-entry counts; scoped
  Feature audit with a healthy held warm comparator; measured hash impact.
  Shared caches and source acquisition remain outside this task.

## Section 5: Reuse decision

Extend the existing quantity-specific exact membership mask; add no new
normalizer or derivation. Reuse the synthetic CS07 harness and finalizer,
the current source reshape/dedup machinery, the sanctioned IO, and the
existing Feature audit. A separate quantity tuple prevents accidentally
expanding the Price filter.

## Section 6: Open questions for the human

- The existing 30 nonrepeat second-entry observations remain pending their
  representation decision. This task neither blocks on nor resolves it.
- Instrument-level independent confirmation of the nominal missing code is
  unavailable in the held sources; implementation follows the existing
  issue/CONTENTS ruling with this limitation explicit.

## Phase 3: Verification

- `_COMMUNITY_QUANTITY_SENTINELS` / quantity mask: OK (sections 2, 4-5).
  Extends existing exact membership; no new normalizer or Price-policy change.
- `_finish_community_prices` and `_COMMUNITY_PRICE_CONTENT`: OK (sections 3-4).
  Paired actual cold API builds and fresh-process warm replay retain 46,043
  rows, every key, all other columns, and 45,967 other Quantity values.
  Exactly 76 quantities become NA; duplicate-content drops remain 0 / 17 /
  117, and the 30 nonrepeat second-record rows remain.
- Cache blast radius: all 23 declared Niger table hashes move, because the
  country module is a shared build input. Controls: 0 of 24 Mali and 0 of
  25 Tanzania table hashes move. No shared cache is cleared or promoted.
- Feature assembly: OK (sections 3-4). A verified fresh Mali release-cache
  parquet, physically copied privately, contributes 40,218 rows beside
  Niger's 46,043; all 86,261 rows have unique canonical keys. Audit reports
  zero failures/errors and three existing contract warnings (cluster-level
  tables have no household `i`; Mali does not record `obs`).
- The source interpretation limitation and open second-record representation
  decision remain as stated in section 6. Full numeric/code and cold/warm
  evidence is summarized in `slurm_logs/isa_backlog_2026-10-21/Niger/1088/`;
  raw source copies and returned frames stay private.
