# Prior-Art Ledger — #821 Mali EHCVM community prices

**Search tier:** ripgrep + git floor (no GitNexus tool available).

## §1 Task
Wire the held Mali 2018-19 s05 community module without losing either reported
FCFA quote; inspect 2021-22 availability without inventing cluster identity.

## §2 Existing machinery
| Symbol / source | Role and tests | Decision |
|---|---|---|
| Mali/2014-15/_/community_prices.py; mali.community_prices_finalize | Three EAC-I slots and weight; test_mali_community_price_readings.py | Keep EAC-I eligibility and weight policy unchanged; cannot apply that filter/sentinel policy to EHCVM |
| Niger/_/niger.py _s05_community_prices | Two-slot obs and source-row rank; test_gh823_niger_s05_community_prices.py | Reuse representation, not Niger-specific duplicate deletion or Quantity=1 derivation |
| lsms_library/data_info.yml community_prices | Canonical seven-level index and unknown_variety sentinel; test_community_prices_variety.py | Reuse |
| get_dataframe / to_parquet / format_id | Shared IO and identifier helpers | Reuse |
| Country wave aggregation / Feature | #1091 removed country concatenator; test_mali_community_prices_builder.py | Reuse without new country builder |
| b42c1c80 / fix/prices-mali | Available ancestor: documentation correction, not implementation | Already merged; do not repeat |

## §3 Definitions & conventions
Inherits STANDING.md §3 and CLAUDE.md; country-specific source notes are
Mali/_/CONTENTS.org Sampling Design and Prices. EHCVM v is grappe, not
(vague,grappe). Price is reported FCFA. Size-resolved source unit labels must
remain size-resolved. Canonical observation grain is (t,v,j,u,variety,visit,obs).

## §4 Invariants
Inherits STANDING.md §4 IO/cache rules, with the established cluster-table v
exception. Current local_tools.map_index protects j when v is present without i;
the new EHCVM loader must not inherit the unnecessary legacy i==v shim.
No aggregation, first-quote selection, inferred count or weight. Source missing
fields stay missing. Held s05 has no weight: seven fields only; the historical
EAC-I weight policy cannot be applied to this EHCVM module. Source inspection
must use independently verified sidecar MD5/size bindings.

## §5 Reuse decision
Two quote slots use the canonical obs level. Source labels can be retained
without a speculative household code bridge; source product identity must
survive any broader j harmonization. Quantity stays NA because none is reported;
there is no new numeric derivation and therefore no derivation registry entry.
The wave helper must preserve all rows, unlike EAC-I's existing positive-price
and resolvable-unit filter. Unknown or duplicate keys need explicit accounting.

## §6 Open questions for root review
The task premise says a weight exists; the held source contradicts it. Exact
schema evidence went to the shared QUESTIONS channel; independent root review
confirmed Quantity-NA is source-supported. Quantity=1 would require Mali
instrument evidence and a separate decision; it is not part of this change.
2021-22 has no s05 sidecar; held ehcvm_prix has no grappe. No wave declaration
without an established source/key contract.

### Phase 3 — verification
- `community_prices_s05` — OK (§2-5): both slots and every row retained;
  existing food map with pass-through; product code protects many-to-one j;
  full sized u retained; no invented quantity or derivation. No household shim.
- New wave script — OK (§2/4): shared IO, exact selected source, no country builder.
- Config/tests — OK (§2/3): same canonical index; 2018-19 now supported;
  2017-18/2021-22 unsupported; legacy EAC-I controls remain scoped and intact.
- 186 focused tests passed. Actual source independently verified at MD5/size,
  54,669 source rows -> 109,338 slots, no drops. Country and Feature preserve
  exact source Prices; all 549 price v values occur in sample.v (551 total).
- Existing Feature drops all-null Quantity; Country warns (strict mode raises).
  No country-wide optional relaxation. Fresh-process warm call exact against
  private dtype-preserving snapshot; cache bytes/mtime unchanged.
- Independent representation review received through dispatch steering:
  Quantity-NA/raw Price/full unit-size cleared subject to source conservation.
  All observed conservation checks passed; root integration review remains.
- 2021-22 s00-s04/DDI blobs not held at inspected L1; held regional price file
  has no grappe. `unsure` verdict records the incomplete label/instrument check.
- Verification controls initially misexpected one existing unit spelling,
  sample v location, Feature all-null pruning, and parquet witness dtype.
  Failures preserved, expected contracts corrected without normalizing actual
  frames. Final post-review native-shape build and fresh warm both passed.
