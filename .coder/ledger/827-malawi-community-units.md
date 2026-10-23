# Prior-Art Ledger — GH #827 Malawi community_prices: `u` / `NumberOfUnits` jointly blank

**Search tier used:** ripgrep + git floor (the gitnexus MCP server did not connect; no generated-index mutation).

## §1 Task, restated
Malawi `community_prices` (`(t, v, j, u)`, script path, `materialize: make`; waves 2010-11 IHS3, 2013-14 IHPS, 2019-20 IHS5 Cross_Sectional + Panel) served 38,506 of 82,635 rows whose `u` was null at build time.  Diagnose, per wave and half, why the Module CK unit is missing, and fix only what the source supports: an unmapped code family (extend with the instrument's own labels), a unit carried elsewhere (read it, following precedent), or no unit recorded (the canonical `Unknown` sentinel).  No price rescaling, no invented `NumberOfUnits`, no conversions, no framework or other-country edits.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_price_block` / `assemble_community_prices` | `Malawi/_/malawi.py` (community_prices block) | reshape Module CK -> `(t, v, j, u)` | no dedicated test before #827 | extend (fixed unit, sentinel) |
| `harmonize_price_unit` | `Malawi/_/categorical_mapping.org` | CK unit codes 1-21 -> Preferred Label | indirectly | reuse unchanged (comment only) |
| `_W1W2_PRICE_UNIT` | `Nigeria/_/nigeria.py` (community_prices header, "W1/W2 do NOT carry a unit column -- the questionnaire fixes one unit per item") | per-item fixed community-price unit | Nigeria tests | precedent for `_CK_ITEM_FIXED_UNIT` |
| `U_UNKNOWN` | `lsms_library/transformations.py:3744` | canonical missing-unit sentinel | `tests/test_962_unit_sentinel.py` | reuse |
| Malawi "codes on no instrument's list -> Unknown" | `Malawi/_/CONTENTS.org`, entry "food_acquired: the =u= table decodes IHS3 unit codes per the questionnaire (GH #1042 decode residual)" | rule for off-list unit codes | `tests/test_malawi_unit_cleanup.py` | reuse the rule |
| `local_tools.map_index` | `lsms_library/local_tools.py:3934-3935` | renames a null `u` to the literal `'unit'` on every read | — | NOT touched (framework); the reason the served gap was `'unit'`, not null |
| Niger `_community_price_triples` | `Niger/_/niger.py` | drops no-price / missing-unit community rows | Niger tests | considered, not followed: Malawi deliberately serves `Available` rows |
| Nigeria plot_inputs "no amount -> keep NA" | `Nigeria/_/nigeria.py` (`assemble_plot_inputs`) | NA `u` where no quantity | #1047 tests | considered, not followed: its premise is "`u` is a COLUMN"; here `u` is an index level |

## §3 Definitions & conventions in force
- `u` missing-unit sentinel `Unknown`; "a NaN on a declared index level is a DEFERRED SILENT DELETION"; consumer rule "`u == 'Unknown'` is NOT a unit": `lsms_library/data_info.yml` lines 25-56.
- community_prices `Price`: "the surveyed prevailing price of one unit `u` of item `j`": `data_info.yml`, `Columns > community_prices`.
- Malawi Module CK semantics (current price, `Price / NumberOfUnits` basis): `Malawi/_/CONTENTS.org`, "community_prices: what Module CK actually asks".
- Off-list unit codes -> `Unknown`; OTHER (SPECIFY) with absent text -> `Unknown` (@ligon ruling 2026-10-18): `Malawi/_/CONTENTS.org`, #1042 entries.
- Units skill: `_U_SENTINELS` is closed; write `U_UNKNOWN`: `.claude/skills/add-feature/food-acquired/units/SKILL.md`.

## §4 Invariants & assumptions
- `(t, v, j)` is unique per wave (one row per EA x item; 2019-20's two halves carry disjoint EAs) -- asserted in `compare.py`; `(t, v, j, u)` uniqueness asserted in `assemble_community_prices`.
- The 17 struck items never carry a unit code or count in any held file (0 of 28,631 served rows) -- now ENFORCED: `_price_block` raises if a struck item records a unit code.
- Rows whose every column is null (4,422: availability missing, no price) are dropped by `_finalize_result`; giving them a `u` does not change that (rows 82,635 before and after).
- 2013-14 (IHPS) ships no Module CK page in the repo (its community questionnaire ends at CJ) and no Stata value labels for `com_ck00b3`.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| `u` on the 17 struck items | new dict `_CK_ITEM_FIXED_UNIT`, following Nigeria `_W1W2_PRICE_UNIT` | the IHS3 CK form strikes out NUMBER OF UNITS / UNIT CODES on CK3, CK4, CK30-35, CK42, CK44-51 (page image, printed p. 20); the item text names the basis |
| `u` on unit-asked items with a blank or off-list code (incl. not-available rows) | reuse `U_UNKNOWN` | no unit recorded; `u` is a declared index level |
| `NumberOfUnits` on struck items | unchanged (NA) | the form struck that cell; the brief forbids inventing a count |
| 2013-14 codes 22 / 23 | `Unknown` (off the CK list), NOT decoded | no IHPS 2013 CK instrument; see §6 |
| `Other (Specify)` (2,876 rows) and the 2010-11 `com_ck00b3_os` text | unchanged | not blank; free-text interpretation is a scientific choice |
| 2019-20 `ckc` free text ("50g", "cup", "2 tablets"; 28 priced rows) | unchanged (`NumberOfUnits` NA, `u` as coded) | free-text interpretation |

## §6 Open questions for the human
1. **`Piece` for the ten count goods whose text names no unit** (Trousers, Battery, Watch, Hand hoe, Panga, Sickle, Axe, Bicycle, Foam mattress, Mosquito net; 16,960 rows).  An inference from the struck count cell, not a transcription.  `Malawi/_/CONTENTS.org` said before #827: "A blank unit is *unknown*, not 'one piece'"; that was written on the premise that "the unit is a free field, not pre-printed", which the form contradicts for these items.  Reversible: one dict.
2. **Serve `NumberOfUnits = 1` on the 17 struck items?**  The form fixes the count with the item ("1 day", "(bottle)") but records none; left NA.
3. **2013-14 codes 22 / 23 under the IHPS household list** (22 SATCHET/TUBE, 23 OTHER (SPECIFY)).  Evidence for: 2013-14 never uses code 20 (2010-11 479, 2019-20 937); 22 falls on items 2010-11 puts on code 20 (Paraffin, Charcoal, Clothes Soap, Toothpaste, Aspirin, Fansidar); 23 on items 2010-11 puts on 21 (Chitenje, Aspirin, Charcoal, Firewood); the IHPS 2013 household Module G and Market questionnaires print 22 / 23 so.  Against: no CK page; adopting the whole list would also send 2013-14's 12 (5 rows), 16 (3) and 21 (1) to `Unknown` as unprinted (`_IHPS13_UNPRINTED_CODES`).  Code 23 is `Unknown` under either reading; the contested set is 47 rows of code 22.
4. **`map_index`'s null-`u` -> `'unit'` rename** (framework) is undocumented and serves a spelling `_U_SENTINELS` does not know.  Malawi no longer reaches it for community_prices; other tables / countries may.
5. **CK42 label drift**: 2010-11's Stata value label says "(per 50kg)"; the printed IHS3 page and the IHS5 label say "(per 50kg per kilometer)".  The printed form governs (`50 kg per Kilometre`).

---
### Phase 3 — verification
- `_CK_ITEM_FIXED_UNIT` — OK (anchored on §2 Nigeria `_W1W2_PRICE_UNIT`, §5): labels reuse Malawi's `Piece` / `Packet` / `50 kg Bag`; `Bottle`, `Day`, `50 kg per Kilometre` are new transcriptions of printed item text (§6.1, §6.5).
- `_price_block` — OK (anchored on §3 sentinel rule, §4 struck-item invariant now enforced).  Price / NumberOfUnits / Available identical on all 82,635 rows (join on `(t, v, j)`).
- `assemble_community_prices` — OK (§4): docstring corrected; no logic change.
- Cache: Malawi only — 26 / 26 L2-country hashes, 130 / 130 L2-wave hashes, 0 / 40 `build_transforms_fingerprint` (`results/hash_census_diff.json`).
