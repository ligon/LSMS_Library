# GH #587 step 1: Nigeria PP C2 item and unit mapping, and each wave's price basis

I've built the PP item and unit crosswalk for all six C2 sources and committed it under `.coder/` (four commits; nothing is wired). I did not write `REPORT.md`: the harness refuses report files from subagents, so the full report is this message. Please save it to `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-mapping/REPORT.md`.

The headline finding: for W3–W5, all 454 proposed `j` labels equal what the post-harvest (PH) build serves for the same code today, so the existing mapping applies. W1 and W2 need the quarter-aware food resolver instead. The code-only lookup is wrong on 13 W1/W2 codes, and the PH W1/W2 item map is a different numbering.

## What was done, and the hard rules
- **Agent:** Claude Opus 5.5 (`claude-opus-5-5`). Worktree `/local/job41201840/worktrees/587-mapping`, HEAD `13956851f` asserted at the start.
- **Nothing edited:** no Nigeria `data_scheme.yml`, `data_info.yml`, `categorical_mapping*`, script or public call. No `dvc` CLI, no lock files touched, no parquet in the tree (0 found).
- **Private environment only:** every Python run used `LSMS_DATA_DIR=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-mapping/private/data`, asserted equal to `paths.data_root()`, plus private cache, tmp and issues-log paths. `lsms_library.__file__` was asserted to be under the worktree.
- **No data rows read:** every row count is copied from the sealed prior operand audit (`…/isa-sm-20261103/reports/587-nigeria-five-pp-community-docs-20261110/numeric-operands-v1/RESULT.json` and `CLOSED-STRATA-v2.json`).
- **Questionnaires:** nine community PDFs (five PP, four PH) fetched with `get_data_file`; all MD5s match their sidecars. Fetch record: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-mapping/private/docs/FETCH-RECORD.json`.
- **Tests:** `tests/test_gh587_nigeria_pp_price_reports.py` passes, 47 of 47 (`-n 2 --dist=loadfile`).

## 1. Nigeria idiosyncrasies that bear on PP community prices

**What I read of `CONTENTS.org` (4,237 lines):**
- In full: L1–810, 985–1012, 1590–1612, 1843–2177, 2106–2125, 2209–2232, 2804–2840, 2887–2916, 3161–3191 and 3803–4237.
- The rest only through the heading outline and a keyword sweep: L810–985, 1012–1590, 1612–1843, 2232–2804 and 2916–3803 (bar 3161–3191).
- The only status keywords are the file header and the nonfood recall-window WAITING at L3505. There are no LOGBOOK drawers.

**The idiosyncrasies:**
1. **PP and PH are distinct rounds with distinct `t`.** PP `t` is `PP_QUARTER` (2010Q3, 2012Q3, 2015Q3, 2018Q3, 2023Q3). Each round has its own cover and the memberships differ (L681–692).
2. **A contradiction, quoted, not resolved.** L2140–2152 says "C8 is POST-HARVEST ONLY … the post-planting community questionnaire carries sections C1-C4 only … Those cells are *correctly empty*." All five PP forms carry a price section; it is C2, not C8:
   - W1: PDF p.5, "Section 2: Food Prices".
   - W2: p.5, "Section 2: Food Prices".
   - W3: p.5 and p.11, "Food Prices - 1st / 2nd Location".
   - W4: p.5, "C2: FOOD PRICES".
   - W5: p.6, "SECTION C2. FOOD PRICES".
3. **W3 prices two physical locations** (`obs` 1 and 2, L1855–1920). The PP form has the same design. The two native item and unit cards are byte-identical (116 and 42 entries).
4. **The cluster key `v` is the composite `state/lga/ea`** (L985–1010). The "community" is a village or group of villages within the EA, not the EA population (L1595–1611; PP forms p.2).
5. **W1/W2 PH prices use the community's own item numbering**, mapped by name in `_W1W2_PRICE_ITEM` (L1984–1992). PP C2 does not use it (section 2).
6. **Codes collide across rounds.** Food item codes collide across quarters (GH #1144, L6–35) and unit codes across waves (GH #607, L219–260).
7. **Unit conventions already in force:**
   - The unit decoder is size-agnostic, and restoring size is an open maintainer decision (#834, L2084–2095). The same collapse appears in `crop_production`, where "Sack/Bag" spans 9 to 100 kg (L3161–3189).
   - An unplaced recorded label is kept as recorded (#1095 ruling, L2057–2062).
   - A blank unit is `Unknown` (L466–491).
   - W1 labels stay weightless (#1035, L493–555).
   - W4-PH "40. DERICA/TIN" and "120. SACHET" are served as derica and packet/sachet (#1033 ruling, L341–347).
   - `plot_inputs` serves "Stem" and "Seedling/Plant" as unregistered spellings (L2897–2910).
8. **PH evidence on g/cl:** "quantity of one" with a g or cl unit turned out to be a package price, at 1/189 and 1/42 of the household unit value (L2097–2103). PH divides by a coded metric size and drops rows with no recorded size (L2105–2125). Neither rule is authorized for PP.
9. **W3 and W5 native unit cards disagree with the printed cards.** Section 2 now explains the W3 case.
10. **The held W4 PP form is a mixed version.** The cover says W4 PP and the table-of-contents footer says "Wave 3 … Post-Harvest". Its printed item list also lacks the whole alcoholic-drinks block, 160–164.
11. **Unqualified "Pepper" is held** and is not a Preferred Label (#1537, L4059–4071).

## 2. (A) Is each PP card the same codebook as PH and the household food card?

**W1 (2010Q3)**
- **Items, native card:** this is the household consumption numbering. Against `harmonize_food[2010Q3]`:
  - 81 have the same code and label;
  - 8 have the same code with spelling differences (121 is truncated);
  - 1 is the Pepper case (76).
- **Items, vs the PH W1 numbering:** `_W1W2_PRICE_ITEM` and the PH printed p.14 are a different codebook. 63 code numbers are shared and only 30–37 mean the same thing; for example, 10 is guinea corn in PP and Biscuits in PH.
- **Items, vs the printed PP form:** the printed list (pp.5–8) is a community numbering too. 65 code numbers are shared and only 30–38 agree. No data row lies outside the native card, so no printed-only code (200 Bottled water, 900 Beer, …) occurs, and I infer the data follow the native numbering.
- **Units, codes 11–95:** these are the W1-PH food card SP10BQ2B (L504–521). Compared mechanically with `u[2010-11]`, 15 match (sack, basket, basin, pick-up). 18 differ only in spelling (bunch, tuber, bundle, wheel barrow, jerry can).
- **Units, codes 1–7 and 101–106:** the cups with stated weights, congo/mudu, tiya, and kg/g/l/ml/pieces/other are not the household 1–6 card. Household code 1 is Kg; PP code 1 is a tomato cup.
- **Printed units:** the form has an empty "Unit Codes" column and no key.

**W2 (2012Q3)**
- **Items:** native (95) equals printed (pp.5–7) plus code 0, "No more option". Against `harmonize_food[2012Q3]`: 90 identical, 2 spelling differences, 76 Pepper, 15 Semovita with no entry, and the 0 sentinel.
- **Items, vs PH:** not the PH W2 codebook. That rests on repo code only, since no W2 PH PDF is held.
- **Units:** native equals `u[2012-13]` code for code (10 is the typo "MEDIUM DERCA"). The printed card lacks 16 "pieces", which the data use on 234 rows.

**W3 (2015Q3, both locations)**
- **Items:** native equals the printed PP card and the PH W3 printed card code for code (601 is printed "60A"). Against `harmonize_food[2015Q3]`: 109 identical and 6 spelling differences.
- **Item 161:** native says "Palm oil"; printed PP, PH and household all say "Palm wine".
- **Units:** native equals `u[2015-16]` for 38 codes. The anomaly:
  - The native card has 22 "MUDU LARGE" and 32 "DERICA LARGE". These are on neither the PP printed card nor the household card; they are the PH W3 printed numbers (PH p.30).
  - The native card lacks 31 and 42, which the printed and household cards call Mudu Large and Derica Large. The data use them: 31 on 707 + 53 rows, 42 on 66 + 1. Code 22 appears on 2 rows and 32 on none.
- **The PP unit card is not the PH W3 card.**

**W4 (2018Q3)**
- **Items:** native (111) equals `harmonize_food[2018Q3]` on all 111 (107 identical, 4 spelling).
- **Items, vs the held printed lists:** the PP and PH printed lists have 55, 65, 131 and 140 (PH also 54); the native card instead has 148 and 160–164. Since 0 rows lie outside the native card, the data follow it.
- **Units and sizes:** the native cards (33 units, 19 sizes) equal the printed card (p.13) exactly. They are a subset of the PH W4 printed card, which adds units 270–330 and further size codes.

**W5 (2023Q3)**
- **Items:** native is byte-identical to W4. Only the printed PP list adds 55, 65 and 131; the PH W5 printed list has no codes beyond the native card.
- **Units:** the native label sets are named `c8aq7a_1` (42 units), `c8aq7b_1` (38 sizes) and `c8aq1`. These are PH C8-style names inside a C2 file.
  - The printed card is W4's (33/19), which is also the PH W5 printed card.
  - Against it, the native card adds 2, 4 "CENTLITRES", 125 and 270–330, lacks 14 and 250, and relabels 40 "DERICA/TIN" and 120 "SACHET".
  - It agrees with every PH W5 label `CONTENTS.org` quotes. Full equality with the PH headers is unverified.

## 3. Counts and tables

| source | items: identity / label-match / judgement / unmappable | units: identity / label-match / judgement / unmappable |
|---|---|---|
| W1 | 81 / 0 / 9 / 0 | 15 / 9 / 21 / 1 |
| W2 | 90 / 0 / 3 / 2 | 15 / 0 / 1 / 0 |
| W3, each location | 109 / 0 / 7 / 0 | 38 / 0 / 6 / 0 |
| W4 | 107 / 0 / 4 / 0 | 23 / 2 / 6 / 2 |
| W5 | 107 / 0 / 4 / 0 (inherits W4) | 21 / 11 / 8 / 2 |

Each unit table also has a missing-unit row mapped to `Unknown`. Rows carrying a judgement or unmappable unit code: W1 3,736 of 21,908; W2 68; W3 821 + 87; W4 1,604; W5 5,150 of 30,021.

**Normalisation disclosed.** "Same label" means equal after stripping the code prefix, the W1 category prefix, case and the word "etc". That rule lets two truncated W1 labels pass as identity: 105 and 140.

**Substantive item judgements:**
- W1/W2 76: Pepper or Fresh Pepper.
- W3 161: proposed Palm wine. The alternative, Palm oil, would collide with code 50.
- W2 15 Semovita: unmappable; the code-only lookup would serve "Bread".
- W2 0: unmappable, a questionnaire sentinel.

**Units: six verified errors in the existing unit decoder (`_canon_community_unit`) on PP labels:**

| native label | rows | decoder returns | proposed |
|---|---|---|---|
| millilitre (W1) | 460 | l | ml |
| 4. CENTLITRES (CL) (W5) | 36 | l | cl |
| 251. BAG OF 20 SACHETS (W4/W5) | 103 + 464 | Sack/Bag | Bag of 20 Sachets |
| congo/mudu (W1) | 1,841 | Congo | maintainer choice |
| MEDIUM DERCA (W2) | 68 | nothing | Derica |
| tomatoes cup (W1) | 233 | nothing | new weightless token, maintainer choice |

**Other unit judgements** are inherited lossy PH folds: tin to Derica, bin/basket to Basket, 250 to Packet/Sachet, 100 BUNCH/BUNDLE, W5 40/120/125, and W3 31/42/22/32.

**SIZE cards.** W4 has 19 codes (4 ordinal, 1 one-size, 14 metric); W5 has 38 (4 ordinal, 1 one-size, 33 metric). Priced rows:
- W4: ordinal 20,948, one-size 577, metric 3,156, blank 7,565.
- W5: ordinal 18,150, one-size 33, metric 4,619, blank 7,219.

## 4. (D) Price basis by wave, from each wave's own PP form

**W1: `computable-from-recorded-fields`.**
- Form: pp.5–8, columns "What is the price for the item? | Naira | Number of Units | Unit Codes". Variables `sc2q3a`, `sc2q3b`, `sc2q3c`. There is no quantity-of-one instruction.
- Number of units = 1 (15,014 rows): the price is of one unit as recorded.
- Number of units > 1 (7,198 rows): needs `sc2q3a / sc2q3b`, a registered derivation that needs a yes.
- Number of units missing (105 priced rows): `unresolved`.
- Unit missing (413 rows): `u = Unknown`.

**W2: `basis-established` for every unit except g/cl.**
- Form: pp.5–7, "PLEASE USE A QUANTITY OF ONE (1) FOR [ITEM] AND THE UNIT OF MEASURE YOU ARE USING? | What is the price for the item? NAIRA". Variables `c2q2`, `c2q3`; there is no quantity field.
- g/cl rows (209): `unresolved`; see Q4.

**W3: `basis-established` for local units and "other specify".**
- Form: the same wording at first-location questions 7/8/9 (pp.5–10) and second-location questions 1/2/3 (pp.11–16). Variables `c2q2`, `c2q3`; `c2q2b` is the other-unit text, not a size.
- Metric-unit rows with blank other-unit text are an open question (Q4), not a verdict: 12,725 (location 1) + 4,329 (location 2). This includes Kg/l, because `CONTENTS.org` itself drops W3 Kg/l rows as package prices (L2110–2116) while serving W4 blank-size kilograms as one kilogram (L2121–2125).

**W4: `basis-established` at (unit, size) grain.**
- Form: pp.5–9, "PLEASE USE A QUANTITY OF ONE (1) … UNIT | SIZE; What is the price for the item? NAIRA". Variables `c2q2`, `c2q2c`, `c2q3`. There are no g/cl unit codes.
- Whether that is "the price of one `u`" depends on Q1: under the base `u` I recommend, the size is not in `u`, for ordinal sizes as well as metric ones.
- A per-cl or per-kg price for the 3,156 metric-size rows would be a computation and needs a yes (Q5).

**W5: `unresolved`.**
- Form: pp.6–11, the same wording, with no quantity column. The PH W5 form, by contrast, asks "price for [ONE UNIT]" (PH p.19).
- The export carries `c2q2a` "Quantity" on 350 of 30,021 priced rows (19 equal to 1, 331 other) and is missing it on 29,671.
- `c2q2_cvn` is a conversion factor, not a price operand.
- Treating the missing-quantity rows as one unit is the default quantity of one the ruling withholds.

## 5. (E) Where the tables should live, and what each placement costs

Costs follow CLAUDE.md's hash rules; none was measured.
1. **Country `_/categorical_mapping.org`.**
   - Cost: a Nigeria-wide cold re-warm.
   - A table named like a column or index auto-applies at read time; adding rows to `u` would reach every Nigeria `u` level by code.
   - `nigeria.py` reads this file by path.
   - A `2023Q3` column would change `_food_item_labels`, which raises for W5 food on purpose.
2. **Country `_/categorical_mapping/` directory.** Same cost and auto-apply risk; the path readers in `nigeria.py` do not see it.
3. **Wave `_/`.** Same Nigeria-wide cost. Worse, read-time overlay reaches PH rows too, because a wave directory holds both rounds.
4. **Inside the PP adapter module.** Same Nigeria-wide cost; no auto-apply, no path-reader effect.
5. **Country `_/*.yml`.** Moves no hash, but an edit would never invalidate the `make` build that consumes it: a silent stale cache.
6. **Global `categorical_mapping/`.** Corpus-wide re-warm; wrong level.

**Recommendation: option 4.**
- `j`: W1/W2 via `harmonized_food_labels(t=<PP quarter>)`, W3–W5 via the code contract, plus explicit overrides for the judgement rows.
- `u`: `_canon_community_unit` plus the six overrides.
- Land it with the wiring change so Nigeria re-warms once.

## 6. Maintainer questions

**Q1. Unit axis.**
- (a) Size-agnostic base labels, as PH serves now, with the native size kept in a column.
- (b) Sized labels, as `food_acquired` uses.
- Base keeps PP and PH comparable but serves small and large heaps (and 9–100 kg sacks) under one `u`.
- Recommend (a) now, and decide the sized axis for PH and PP together.

**Q2. W1 number of units.**
- (a) Register `Price = sc2q3a / sc2q3b` (7,198 rows).
- (b) Serve only the rows where it equals 1.
- (c) Hold W1.
- Recommend (a): it is a recorded denominator. The 105 rows with no count stay unserved either way.

**Q3. W5 basis.**
- (a) Accept the paper's quantity of one. **This is the default quantity of one and is outside the ruling.**
- (b) Hold W5 until the fielded CAPI specification for `c2q2a` is found.
- (c) Serve only the 19 rows with quantity 1.
- Recommend (b).

**Q4. Metric units with no recorded package size** (g/cl: W2 209, W3 about 12.9k, W5 231; plus W3 Kg/l).
- (a) Hold them with a PP deletion entry. **This mirrors the PH drop rule and is outside the ruling.**
- (b) Serve them as recorded.
- (c) First test them against household unit values.
- Recommend (c), then decide.

**Q5. W4/W5 metric-size rows.**
- (a) Serve the per-container price under the container `u`.
- (b) Register the PH `c8-metric-size` division for PP. **This is the PH size rule and is outside the ruling.**
- (c) Hold.
- Recommend (b) if you agree, else (a).

**Q6. Item labels.**
- Pepper or Fresh Pepper for W1/W2 76 (PH W1/W2's Fresh Pepper contradicts #1144).
- Palm wine for W3 161.
- Semovita: unserved unless a `harmonize_food` row is approved.
- Drop W2 0.
- Recommend: Pepper, Palm wine, Semovita unserved.

**Q7. Unit labels.**
- W3 31/42 (and 22/32) to large mudu / large derica.
- W1 congo/mudu: Congo, Mudu, or a new combined token.
- W1 tomato cup: a new weightless token, or the label verbatim, which would put "(0.087 kg)" in `u`.
- tin: Derica or tin.
- 251 to Bag of 20 Sachets; millilitre to ml; CENTLITRES to cl.
- W5 40/120 per #1033; 125 to Packet/Sachet or Packet.
- Recommend the proposals in `MAPPING.org`.

## 7. Unverified limits
- I read no PH price headers and hold no W2 PH PDF, so the PH-side sameness claims rest on printed cards, repo code and `CONTENTS.org` quotes.
- Printed lists come from the PDF text layer (pypdf). I checked the membership differences by hand but viewed no page images.
- I had no per-code item counts, so "the data follow the native card" rests on the audit's 0 rows outside it.
- The g/cl and Kg/l questions rest on PH evidence only; I ran no PP value check.
- **Possible live PH defect:** PH W4/W5 may serve "251. BAG OF 20 SACHETS" as Sack/Bag and W5 "4. CENTLITRES (CL)" as litres, since the same decoder runs there. Not checked, and #1095's unplaced-label check would not catch a wrongly placed label. This probably deserves its own issue.
- No cache-hash footprint was measured.
- The fielded CAPI versions of all five instruments are unknown.
- Parts of `CONTENTS.org` were skimmed rather than read (ranges in section 1).
- I did not edit `CONTENTS.org`, which was outside the commit scope. An entry worth adding:
  - the PP C2 sections against L2140–2152;
  - the W1 printed-versus-native numbering;
  - W3 22/32 vs 31/42;
  - W3 161;
  - W4's missing 160–164;
  - the W5 `c8aq7*` label sets;
  - the six decoder errors.

## Commits (auto-published)
- `6059e38ecec1c17934a709b4612e324be49a19c0`: tables and generator.
- `b4fb98f8b052801baf12c1374fe32b6b5a88812b`: ledger.
- `3ed6afcc7eb0bcf79581db58106eeb02e26c9c70`: W1 units 11–95 compared mechanically, tables regenerated.
- `2b4a3632cdfce0daf0370a490827a4d73ac0551b`: ledger corrections.

Committed files are in `/local/job41201840/worktrees/587-mapping/.coder/ledger/587-nigeria-pp-mapping/` (CSV is gitignored, so the tables are committed as `.org`):
- MAPPING.org
- build_mapping.py
- printed_items.py
- printed_items.json
- occupancy.json
- COUNTS.json

The ledger is `/local/job41201840/worktrees/587-mapping/.coder/ledger/587-nigeria-pp-mapping.md`.

The full CSVs, with every comparator column, are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-mapping/tables/`:
- items.csv
- units.csv
- sizes.csv
- units_printed_only.csv
