# Prior-Art Ledger — GH #823 Niger 2018-19 community prices

**Search tier used:** ripgrep and read-only git. This is diagnosis preparation at `60aa81e9e`; no source data were opened.

## §1 Task, restated

Determine whether the held `2018-19/Data/s05_co_ner2018.dta` can supply Niger's registered `community_prices` table, and identify the smallest native-source read needed to settle its grain, labels, price basis and cluster linkage before any wiring.

## §2 Existing machinery

| symbol | location | tested? | decision |
|---|---|---|---|
| `_community_prices_maps`, `_item_labels`, `_community_price_triples`, `_finish_community_prices` | `lsms_library/countries/Niger/_/niger.py:1330-1556` | `tests/test_community_prices_variety.py` | Reuse maps/finalizer only if the new source satisfies their contracts; CS07 triple reshaping is source-specific. |
| ECVMA wave scripts | `Niger/{2011-12,2014-15}/_/community_prices.py` | same test; `tests/test_gh1091_niger_community_builder.py` | Reuse independent wave build pattern, not their CS07 variables. |
| Framework country builder after GH #1091 | `lsms_library/country.py`, `Niger/_/Makefile:106-107` | `tests/test_gh1091_niger_community_builder.py` | Reuse; no country concatenator. |
| Canonical index and missing-level sentinels | `lsms_library/data_info.yml:175,298` | `tests/test_community_prices_variety.py` | Reuse seven-level shape; do not adopt issue's older four-level proposal. |
| Sanctioned IO and cache gates | `.coder/ledger/STANDING.md` §2/§4 | cache tests | Reuse in a later authorized runtime phase. |
| Mali PR #1180 | `Mali/2014-15/_/community_prices.py`; `.coder/ledger/822-mali-community-price-readings.md` | `tests/test_mali_community_price_readings.py` | 2014 source/slot precedent only; it did not wire Mali 2018-19. |

## §3 Definitions and conventions in force

- `Niger/_/CONTENTS.org:1185-1205` records the s05 module as present but unwired and warns that `releveID` units carry sizes that household `food_acquired.u` lacks. Its row/cluster counts are historical claims until remeasured here.
- `Niger/_/CONTENTS.org:1221-1315` and `Niger/_/data_scheme.yml:411-511` define the existing variety, visit and observation policy for CS07. A new instrument must be read before assigning its corresponding keys.
- `Niger/_/CONTENTS.org:1561-1614` records GH #1091 framework aggregation and explicitly preserves the 2018-19 gap.
- `Niger/_/CONTENTS.org:10-15` says EHCVM `v` is `grappe`; `sample` uses `grappe` in `2018-19/_/data_info.yml:4-14`.
- Canonical `community_prices.Price` is a surveyed price for one `u` (`lsms_library/data_info.yml:1125-1140`); Niger's current CS07 `Quantity` is a recorded weight (`Niger/_/CONTENTS.org:1207-1220`). The s05 basis must be established natively.
- Inherit `.coder/ledger/STANDING.md` §3, `AGENTS.md` and the canonical schema; these are not copied into this ledger.

## §4 Invariants and assumptions

- This phase reads text, Git objects, sidecars and exact endpoint metadata only. No product import, source payload read, test, build or cache mutation occurred.
- `s05_co_ner2018.dta.dvc` declares MD5 `586edb9feb45f4593058bdfb0a7b83b5`, size 1,020,560; a matching-size L1 path exists, but its digest and contents were not verified.
- A seven-level key, usable market unit and price basis cannot be inferred from the seven-column claim in the issue. Duplicate records, price sentinels and whether a separate variety/visit axis exists are open.
- Do not map a size-qualified `releveID` label to a bare household unit or fabricate `Quantity`, `variety`, `visit` or `obs`. Do not copy Mali's data decisions.
- `STANDING.md` §4 IO and runtime config invariants apply after root approves a native read; use the held source and private data/output paths.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| source read and wave parquet | reuse | `get_dataframe` / `to_parquet` exist and are tested (`STANDING.md` §2/§4). |
| seven-level table, framework aggregation | reuse | GH #840 and #1091 already settled those interfaces. |
| s05 native interpretation | defer | Only the sidecar and historical prose were checked; no source measurements yet. |
| unit-size bridge to household food | defer | `releveID` versus `food_acquired.u` requires source and instrument evidence. |

## §6 Open questions for root

- After the bounded source read, which native s05 fields genuinely identify product variety, visit and observation? If any axis is absent, root must decide how the canonical sentinel applies before wiring.
- Is each s05 price one size-qualified unit, and is any physical quantity reported? This determines how the required Niger `Quantity` column could honestly be represented.
- Which unit-size labels can join household food units without rescaling? Hold all scientific conversion and production wiring until evidence and review.

### Phase 3 — verification

Pending source read and root review. No new or changed production symbol exists to verify.
