# GH #587 wiring: Nigeria's five post-planting `community_prices` cells

Agent: Claude Opus 5.5 (`claude-opus-5-5`). Worktree `/local/job41201840/worktrees/587-mapping`,
branch `feature/587-nigeria-pp-mapping-20261112`. Start HEAD `2b4a3632cdfce0daf0370a490827a4d73ac0551b`
(asserted). Base for every comparison: development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

**Verdict: READY** (for human review of the judgement rows and the three questions in §12). Head `c541bf49ffd2ffcb15c67501cf827e76cc0d9828`.

The five PP quarters (2010Q3, 2012Q3, 2015Q3, 2018Q3, 2023Q3) are served: **126,126 rows**, on
Nigeria's shared `j` / base `u`, with the native size in a new optional `Size` column. Every priced row
left unserved sits in one registered `rows: 0` omission. The PH rows are frame-equal to base except a
shared-decoder defect fix (409 relabels, 49 drops), which is in its own commit. The other five Nigeria
tables are frame-equal to base. Two ruled W1 treatments measure badly against household unit values.
They are implemented as ruled and brought back as questions (§6, §12).

## Commits (all auto-published to the mirror)

| SHA | what |
|---|---|
| `209185db6e872d9bd1e511acdda5e1f753770344` | PH defect fix: `_canon_community_unit` places `251. BAG OF 20 SACHETS` and W5 `4. CENTLITRES (CL)` correctly; the drop-inputs mask decodes like the build |
| `03fe9034c7b601e914bb383cd4b1f57cd8fa6991` | PP wiring: serving layer, five wave scripts, registry, `Size`, tests |
| `2d1d51412` | CONTENTS.org (corrects L2140 and records the PP section) and the `test_gh637_nigeria_w3_obs` per-round pin |
| `ed06358d12df6c90fdbcd45634e2af4f3f6c9e74` | prior-art ledger, step 2 |
| `d61ed2d37e1c86a171a42b92a81f6530236990e6` | wave scripts spell the PP source paths as literals, so `Wave._input_hash` folds their DVC sidecars (the computed path was invisible to it); `nigeria.py` header comment |
| `963642425c49d34994fb5c5b38b3167dc6f1725d` / `c541bf49ffd2ffcb15c67501cf827e76cc0d9828` | prose only: the item hold is Semovita alone (registry rule, CONTENTS.org table); ledger |

What follows `03fe9034c` (where the full test selection ran):
- `2d1d51412`: CONTENTS.org and one test pin;
- `ed06358d1`, `c541bf49f`: the ledger;
- `d61ed2d37`: literal path declarations in the five wave scripts, a verification argument in
  `pp_community_prices_for_folder`, one comment and two tests;
- `963642425`: registry and CONTENTS prose.

For those changes the targeted rerun at `d61ed2d37` and the cold equality check are sufficient.
`derivations.yml` and CONTENTS.org are in no build hash.

The decoder commit `209185db6` is not self-contained. It moves the 2024Q1 count that
`test_gh637_nigeria_w3_obs::test_rows_per_round_and_slot` pins (30,064 → 30,015), and that pin is updated
only in `2d1d51412`. If the decoder fix is cherry-picked alone, take the pin with it.

## 1. Idiosyncrasies (named, from `Nigeria/_/CONTENTS.org`, the sources and the forms)

Read in full (of 4,237 lines):
- L1–760: food item decode, acquisition sources, stray unit codes, `None`/`Unknown`, the #607 / #1033 /
  #1041 / #1035 unit sections, PP/PH rounds and covers;
- L985–1057: cluster id, LGA changes;
- L1590–1612: the community universe disclaimer;
- L1843–2177: community_prices, C8, units, size;
- L2804–2916: crop / plot_inputs unit handling, including the `Stem` / `Seedling/Plant` precedent;
- L3161–3191 and L3803–3857;
- L3925–4237: #637 variety, #1537 Pepper, #1552, the GH587 PH entries, #1288.

Not read: L760–985, 1057–1590, 1612–1843, 2177–2804 (crop_production sales / values), 2916–3161,
3191–3803 and 3857–3925. These are sampling detail, plot_features, crop_production, interview_date and
nonfood. A keyword sweep of those ranges for `community`, `sectc2` and `C8` found two relevant notes:
- the #1287 compiler notes (L3619–3645, L3725–3791), which say "C8 is PH-only". That is true: PP is C2.
- the #1287 compiler collects physical folders through `Wave.data_scheme`. It is unchanged, and it is
  covered by `test_gh1287_nigeria_four_declarations.py`.
The only TODO/WAITING keyword is the nonfood recall-window WAITING (L3505), which does not bear on this.
There are no LOGBOOK drawers.

1. **PP and PH are distinct rounds with distinct `t`** (`PP_QUARTER` / `PH_QUARTER`). Each round has
   its own cover, and the memberships differ (L681–692). The PP rows go out at the PP quarter. A wave
   folder's script now writes both rounds; `combine_rounds` refuses a shared `t`.
2. **The contradiction at L2140–2152, quoted:** "Those cells are *correctly empty*, not a silent drop: no
   `sectc8_planting*` file exists ... the post-planting community questionnaire carries sections C1-C4
   only -- section 8 "Food Prices" is in the post-harvest community instrument alone." All five PP forms
   price food in **Section 2 / C2** (W1 PDF p.5, W2 p.5, W3 pp.5 and 11, W4 p.5, W5 p.6), and the
   released files carry the price (W1 `sc2q3a`, W2–W5 `c2q3`). The search found nothing because it
   looked for `*c8*`. **Corrected in CONTENTS.org** (a contradiction-of-record note above the old
   paragraph, which is kept). The same false claim was in `tests/test_nigeria_cluster_identity.py` ("C2 is
   an availability module ... no price column at all"); that test now checks both rounds.
3. **The PP cards are not the PH cards.** W1/W2 PP item codes are the household consumption numbering of
   the PP quarter, not PH W1/W2's community-own numbering (`_W1W2_PRICE_ITEM`). 63 code numbers are
   shared and only 30–37 mean the same thing. W1's printed list is a third numbering; the data follow
   the native card.
4. **Food item codes collide across quarters** (GH #1144, L6–35). So `j` comes from
   `harmonized_food_labels(t=<PP quarter>)`. The wave-blind Code contract is wrong on 7 W1 and 6 W2
   codes. W3–W5 equal it on every carded code (pinned). W5's item card is byte-identical to W4's, and the
   codebook has no 2023Q3 column (it raises on purpose), so W5 reads 2018Q3.
5. **Unit codes collide across waves and cards** (GH #607, L219–312). W1 PP code 1 is a tomato cup, but
   household W1 code 1 is Kg. `u` Code 22 = Basket Medium. A PP unit code is never joined to the `u`
   table by number.
6. **The W3 PP native unit card lacks 31 / 42**, which the data use (707+53 / 66+1 priced rows) and the
   printed PP card and `u[2015-16]` call Mudu Large / Derica Large. It carries 22 / 32 instead, the PH W3
   printed numbers. **W3 item 161** is "Palm oil" natively; every printed card says Palm wine, and Palm
   oil is code 50.
7. **W3 prices two physical locations** in PP as in PH (`sectc2a` / `sectc2b` → `obs` 1 / 2, L1855–1935).
8. **`v` is the composite `state/lga/ea`**, built from codes (GH #323, L985–1040). The community is not
   the EA population (L1595–1611).
9. **The unit axis is size-agnostic** (#834, L2084–2095; and `crop_production` 9–100 kg sacks,
   L3161–3189). Hence the new `Size` column.
10. **Unit conventions in force**:
    - an unplaced recorded label is kept verbatim (GH #1095, L2057–2062);
    - a blank unit is `Unknown` (GH #1041, L466–491);
    - W1 stated weights stay out of `u` (GH #1035, L493–555);
    - W4-PH 40 / 120 serve derica / packet/sachet (GH #1033, L341–347).
11. **PH g/cl evidence (L2097–2125)**: "quantity of one" with a g or cl unit was a package price, at
    1/189 and 1/42 of the household unit value. PH divides by a coded metric size and drops rows with
    none, including W3's blank-text Kg/l.
12. **The W4 PP form is a mixed version.** Its cover says W4 PP; its table-of-contents footer says
    "Wave 3 ... Post-Harvest". Its printed list lacks 160–164.
13. **W5's C2 file carries PH-style label sets** (`c8aq7a_1`, `c8aq7b_1`) and a `c2q2a` "Quantity" the
    printed form lacks.
14. **Unqualified "Pepper"** stays native and is not a Preferred Label (#1537, L4059–4071). It passes
    through `labels='Aggregate'`.

**New idiosyncrasies found and recorded in CONTENTS.org:**
- PH 2019Q1 / 2024Q1 served `251. BAG OF 20 SACHETS` as `Sack/Bag`, and 2024Q1 served W5's
  `4. CENTLITRES (CL)` as `l`. This is a live PH defect (§4).
- W5 PP `c2q2a` is a gram/centilitre package count on the g/cl rows, but a count of units on CUBE rows
  (§5).
- W1 PP `g`/`ml` with N = 1 are package prices (144x), and W1 Kg/l with N > 50 are not counts (§6).

## 2. Design as built

- **Placement: option 4.** `Nigeria/_/pp_community_prices.py`, beside the sealed report adapter, which
  is untouched (its 47 tests pass). The Price-NA prototype `pp_community_price_projection` is left in
  place. No categorical_mapping table and no `.yml`, so nothing auto-applies to another Nigeria table.
- **`j`**: `pp_item_map(source)` = `harmonized_food_labels(t=PP_ITEM_QUARTER[source])` over the carded
  codes, minus `PP_ITEM_HELD` (W2 0 and 15). It refuses a carded code with no label that is not held.
  It reproduces MAPPING.org's `proposed_j` on all six sources: 0 mismatches.
- **`u`**: `pp_unit_map(source)` = `_canon_community_unit(label)` plus `PP_UNIT_OVERRIDES` (six
  entries: W1 1 / 6 / 104, W2 10, W3 31 / 42). A carded label neither places is kept verbatim (#1095);
  only W4/W5 210 / 260 are, on 0 rows. A missing unit code is `Unknown`. It equals MAPPING.org's
  `proposed_u` everywhere MAPPING proposes one. Tomato cup and 210 / 260 had no single proposal; they
  get `Tomatoes cup` and verbatim.
- **`Size`** (str, optional): the W4/W5 `c2q2c` label verbatim, or the size the W1/W2/W3 unit code
  carries (`pp_size_map`). It is NA on PH rows. Non-null PP rows: 2,771 / 169 / 16,823 / 24,681 / 22,737.
- **Price and disposition** (`pp_dispositions`, decided in this order, every row exactly once):
  1. `unpriced`: no positive Naira. Excluded, as PH excludes it.
  2. `item-hold`.
  3. `unit-unplaced`: an off-card unlabelled code. 0 rows; warns.
  4. `w1-no-count`.
  5. `unrecorded-size-hold`: W2/W3/W5 g/cl, or W3 Kg/l, with no coded metric size and not
     `99. ONE SIZE ONLY`.
  6. `w5-quantity-hold`: `c2q2a` present and not 1.
  7. `metric-size`: `c2q3 / size_qty`, `u` becomes g/cl, the PH rule.
  8. `w1-divided`: `sc2q3a / sc2q3b` where N > 1.
  9. `served`: as recorded.
- **Wiring**: each wave script appends
  `combine_rounds(df, pp_community_prices_for_folder(folder, lambda f: get_dataframe(f, convert_categoricals=False)))`.
  The country concatenator, Makefile rules, `materialize: make` path and the `i == v` shim are
  unchanged. The adapter now imports plain `nigeria` from its own directory, as wave `mapping.py`
  modules do, so the scripts, the registry's by-path load and an `LSMS_COUNTRIES_ROOT` override bind one
  module.
- **Registry** (`Nigeria/_/derivations.yml`, six entries):
  - `pp-w1-number-of-units`: function `derive_pp_w1_unit_price`.
  - `pp-c2-metric-size`: function `2018-19/_/mapping:derive_c8_metric_package_unit_price`, the PH
    function. It has its own key because the PH entry's `inputs` return PH rows only, and
    `test_community_prices_metric_sizes_are_per_unit` pins that key to PH quarters.
  - Four `rows: 0` omissions: `pp-w1-no-count-hold`, `pp-unrecorded-size-hold`,
    `pp-unharmonized-item-hold` and `pp-w5-quantity-not-one-hold`.
  - Every `inputs` returns the original variables under the served `(t, v, j, u, variety, obs)` labels.
- **`data_scheme.yml`**: `Size: {type: str, optional: true}`. The PH-only comments are corrected,
  including "all units used already exist in `u`", which PP rows break with `Tomatoes cup`,
  `Congo/Mudu`, `ml` and `Unknown`.
- **Cache**: a Nigeria-wide re-warm (`nigeria.py`, `_/*.py`, `data_scheme.yml`, `Makefile` and wave
  scripts are hashed). No other country moves; no library code is touched.

## 3. Per-wave served / held (cold candidate build, `03fe9034c`)

| PP t | source rows | priced | served | `Derivation` | held, by registered omission |
|---|---|---|---|---|---|
| 2010Q3 | 23,441 | 22,319 | 22,214 | 7,199 `pp-w1-number-of-units` | 105 `pp-w1-no-count-hold` |
| 2012Q3 | 21,405 | 21,405 | 21,124 | — | 209 `pp-unrecorded-size-hold` (g 204, cl 5); 72 `pp-unharmonized-item-hold` (all code 15 Semovita; code 0 has no priced row) |
| 2015Q3 | 37,952 (two locations) | 37,929 | 20,875 (obs 1 15,032, obs 2 5,843) | — | 17,054 `pp-unrecorded-size-hold` (g 8,445, cl 4,489, Kg 3,075, l 1,045) |
| 2018Q3 | 57,054 | 32,246 | 32,246 | 3,156 `pp-c2-metric-size` | none |
| 2023Q3 | 56,277 | 30,021 | 29,667 | 4,556 `pp-c2-metric-size` | 231 `pp-unrecorded-size-hold` (g 195, cl 36); 123 `pp-w5-quantity-not-one-hold` |

- Every served PP quarter has a unique index, no non-positive or NA Price, and only PP derivation keys.
- `derivation_inputs` returns 7,199 / 7,712 rows for the two constructions, equal to the stamped rows.
  It returns 105 / 17,494 / 72 / 123 rows for the omissions; none is a served key, and each is attrs
  `rows: 0`.
- Price quantiles (min / p10 / median / p90 / max, Naira):
  - 2010Q3: 0.0008 / 1.3 / 100 / 700 / 70,000;
  - 2012Q3: 1 / 25 / 150 / 700 / 9,554;
  - 2015Q3: 3 / 20 / 100 / 400 / 18,000;
  - 2018Q3: 0.013 / 10 / 150 / 900 / 60,000;
  - 2023Q3: 0.0075 / 8.3 / 250 / 1,700 / 150,000.
  - The small values are the per-g / per-cl / per-ml divisions.
- `u` values outside the PH vocabulary: W1 `Tomatoes cup` 231, `Congo/Mudu` 1,834, `ml` 455,
  `Unknown` 390, plus `Bundle` and `Jerry Can`, which are in `u` but not in PH C8; W3 `Unknown` 9;
  W4/W5 `Bag of 20 Sachets`, which PH now serves too.
- `j` outside the PH vocabulary: `Pepper`, `Groundnuts` (W1/W2) and `Maize (shelled)` (W2). These are
  the #1144 native names.

## 4. The shared decoder: PH measurement and fix

`_canon_community_unit` falls through to the substring decoder `_canon_unit`. On base, measured on the
held PH sources (exploration job 41237264) and confirmed by the cold base-vs-candidate diff:

| PH t | label | served on base | rows | after the fix |
|---|---|---|---|---|
| 2019Q1 | `251. BAG OF 20 SACHETS` (blank size) | `Sack/Bag` | 148 | `Bag of 20 Sachets`, same Price |
| 2024Q1 | `251. BAG OF 20 SACHETS` (blank size) | `Sack/Bag` | 261 | `Bag of 20 Sachets`, same Price |
| 2024Q1 | `4. CENTLITRES (CL)` (blank size) | `l` | 49 | `cl`; blank size → **dropped** under the existing `c8-unrecorded-size-drop` (W5 drop 133 → 182) |

The cold diff of PH rows on base's columns shows 458 base-only and 409 candidate-only keys. These are
exactly the 409 pairs `Sack/Bag → Bag of 20 Sachets` with equal Price, plus the 49 `2024Q1|l` rows.
There are 0 value differences on the other 146,383 common rows.

- The fix is two exact aliases in `_COMMUNITY_UNIT_LABELS`. The shared harvest / sold / input decoder
  `_canon_unit` is unchanged (pinned).
- `_c8_raw_inputs`' drop mask used `_canon_unit`, so it would not have listed the 49 rows. It now uses
  the build's decoder.
- The other four PP-only decoder errors (millilitre, congo/mudu, MEDIUM DERCA, tomato cup) occur on no PH
  label, because PH W1/W2 have no unit column. They are PP overrides.

## 5. W5 `c2q2a`: the treatment, and why

350 priced W5 rows record `c2q2a`:
- 19 equal 1 (18 CUBE, 1 LITRES). Served as recorded.
- 208 are GRAMS / CENTLITRES with a blank size. Held first by `pp-unrecorded-size-hold`.
- 123 are **excluded with a count** (`pp-w5-quantity-not-one-hold`): 41 CUBE, 65 BOTTLE/CAN (63 with
  a coded metric size), 10 KILOGRAMS and 7 LITRES.

Why exclude rather than serve with a flag:
- **CUBE**: the price scales with `c2q2a`. Price over the median of the same item / unit / size cell
  with `c2q2a` absent equals 1.0 × `c2q2a` for q = 2–3, 0.73× for q = 25–50 and 0.8× for
  q = 100–1,400. So "one cube" would overstate the price up to ~1,400x.
- **BOTTLE/CAN**: `c2q2a` equals the coded size on 8 of 63 rows. Prices do not scale with it (cell ratio
  ~1.0). These rows would be correct under the metric-size division, as PH serves its own `c8aq2_a`
  rows.
- **KILOGRAMS / LITRES**: no clean reading. q = 100 / 500 kg do not scale; q = 20–25 l do.
- An exclusion is reversible from `derivation_inputs`; a served mispriced row is not.
- **Alternative for the maintainer**: serve the 63 coded-metric-size BOTTLE/CAN rows like PH (divide by
  the coded size, flag `c2q2a`), and keep excluding the other 60.

## 6. The g/cl household-unit-value test (ruling item 7), and what else it showed

Community price as recorded (the literal quantity-of-one reading for held rows) is compared with
`food_acquired` purchased `Expenditure / Quantity`, same round. The cells are matched `(t, v, j, u)`;
the figure is the median of community / household. Jobs 41237950 and 41238037.

| stratum | rows | cells | median | within 2x | national cells / median |
|---|---|---|---|---|---|
| **W2 g, held** | 204 | 70 | **2.24** | 39% | 27 / 4.84 |
| W2 cl, held | 5 | 3 | 1.13 | 100% | 5 / 1.13 |
| **W3 g, held** | 8,445 | 1,699 | **150** | 9% | 79 / 150 |
| **W3 cl, held** | 4,489 | 1,256 | **57** | 4% | 19 / 67.5 |
| **W3 Kg, held** | 3,075 | 719 | **1.07** | 82% | 62 / 1.25 |
| **W3 l, held** | 1,045 | 328 | **1.00** | 85% | 20 / 1.17 |
| W4 cl, `pp-c2-metric-size` (control) | 3,136 | 460 | 1.00 | 97% | 12 / 1.00 |
| W2 Kg / l served (control) | 12,716 / 3,793 | 3,697 / 677 | 0.90 / 1.04 | 55% / 77% | |
| W4 Kg / l served (control) | 1,626 / 717 | 649 / 153 | 1.00 / 1.13 | 96% / 76% | |
| W5 g / cl, held | 195 / 36 | — | **no household comparator**: W5 has no `food_acquired` | | |

**W5, community-internal comparator.** This is clearly labelled as not a household test (job 41238086).
For 208 of the 231 held rows, `c2q2a` holds a gram / cl count, with `c2q2_cvn` 0.001 / 0.01. The
comparison is item medians against the same round's `pp-c2-metric-size` per-unit prices (6 g items,
5 cl items):
- as recorded: **543x / 13x**;
- divided by `c2q2a`: **1.5x / 1.5x**;
- against PH 2024Q1 per-unit prices: 307x / 11x as recorded, **0.96x / 1.23x** divided.

**Options for the maintainer (item 7):**
- (a) **W3 Kg/l: serve as recorded.** They are per-unit prices (1.07 / 1.00). This would add 4,120
  rows. It departs from PH W3, which drops its Kg/l for the same blank field; whether PH W3 Kg/l are also
  per-unit was not measured.
- (b) **W3 g/cl: keep held.** They are package prices (150x / 57x).
- (c) **W2 g: keep held, or serve.** The evidence is mixed: 2.2x on 70 cells, 4.8x nationally. W2 cl
  is 5 rows.
- (d) **W5 g/cl: serve `c2q3 / c2q2a` as a new registered derivation (208 rows), or keep held.** The
  internal comparator supports the division; no household test is possible.

**The ruled W1 treatments, measured the same way** (job 41238037, `w1_buckets.py`):
- **W1 `g` with N = 1, served as recorded per the ruling**: 963 rows, 218 cells, median **144x**, 5%
  within 2x. W1 `ml` with N = 1: 80 rows, 21.6x (2 cells), 265x nationally. They are package prices,
  like PH and PP W3 g.
- **The W1 division by N is right where N is a gram / ml count**:
  - g, N 6–50 / 51–200 / >200: 1.01 / 0.86 / 0.73;
  - ml, N > 200: 1.00;
  - Kg / l, N 2–5: 0.75 / 1.00.
- **It is wrong for Kg/l with large N**:
  - Kg, N 6–50 / 51–200 / >200: 0.33 / 0.23 / 0.009;
  - l, N 51–200 / >200: 0.014 / 0.001, while undivided they sit at 1.05 / 0.85.
  - So about 332 rows (Kg 128 + l 204 with N > 50) are under-priced 50–1000x by the ruled division.

## 7. Label judgements, FOR MAINTAINER REVIEW (implemented as MAPPING.org proposes)

Generated from the mapping step's `items.csv` / `units.csv`, taking every row whose flag is not OK. W3 rows cover both
locations; W5 rows that inherit W4's decision are listed under both. "rows" is the priced-row count where the mapping
tables carry it.

**Items (29)**

| src | code | native label | served `j` | alternative / note |
|---|---|---|---|---|
| W1 | 65 | fruits: canned | Fruit canned | spelling / truncation / wording only |
| W1 | 66 | fruits: other fruites | Other fruits | spelling / truncation / wording only |
| W1 | 71 | vegetables: tomatoes puree(canned) | Tomato puree (canned) | spelling / truncation / wording only |
| W1 | 76 | vegetables : pepper | Pepper | Fresh Pepper |
| W1 | 113 | milk and milk products: milk tinned (unsweetend) | Milk tinned (unsweetened) | spelling / truncation / wording only |
| W1 | 121 | coffee, tea, cocoa and the like beverages:chocolate drinks ( | Chocolate drinks (including Milo) | spelling / truncation / wording only |
| W1 | 133 | sugar, sweets and confectionary : other sweets confectionary | Other sweets and confectionary | spelling / truncation / wording only |
| W1 | 154 | non-alcoholic drinks:  fruit juice canned | Fruit juice canned/Pack | spelling / truncation / wording only |
| W1 | 155 | non-alcoholic drinks:  other non-acoholic drinks | Other non--alcoholic drinks | spelling / truncation / wording only |
| W2 | 0 | No more option | **unserved** (`pp-unharmonized-item-hold`) | questionnaire sentinel |
| W2 | 15 | Semovita | **unserved** (`pp-unharmonized-item-hold`) | no harmonize_food row; Code contract would say "Bread" |
| W2 | 65 | Fruits (canned) | Fruit canned | spelling / truncation / wording only |
| W2 | 76 | Pepper | Pepper | Fresh Pepper |
| W2 | 153 | Soft drinks (Coca Cola, Sprite, etc) | Soft drinks (Coca Cola, spirit, etc) | spelling / truncation / wording only |
| W3 | 52 | 52. Groundnuts Oil | Groundnut oil | spelling / truncation / wording only |
| W3 | 53 | 53. Other oil and fat | Other oils and fats | spelling / truncation / wording only |
| W3 | 78 | 78. Leaves (Cocoyam, Spinash,etc.) | Leaves (Cocoyam, Spinach, etc.) | spelling / truncation / wording only |
| W3 | 94 | 94. Wild game/bush meat | Wild game meat | spelling / truncation / wording only |
| W3 | 147 | 147. Mellon (ground) | Melon (ground) | spelling / truncation / wording only |
| W3 | 153 | 153. Soft drinks (Coca cola, sprit, etc) | Soft drinks (Coca Cola, spirit, etc) | spelling / truncation / wording only |
| W3 | 161 | 161. Palm oil | Palm wine | Palm oil |
| W4 | 52 | 52. Groundnuts Oil | Groundnut oil | spelling / truncation / wording only |
| W4 | 53 | 53. Other oil and Fat | Other oils and fats | spelling / truncation / wording only |
| W4 | 94 | 94. Wild game/bush  meat | Wild game meat | spelling / truncation / wording only |
| W4 | 133 | 133. Other sweets and confectionery | Other sweets and confectionary | spelling / truncation / wording only |
| W5 | 52 | 52. Groundnuts Oil | Groundnut oil |  |
| W5 | 53 | 53. Other oil and Fat | Other oils and fats |  |
| W5 | 94 | 94. Wild game/bush  meat | Wild game meat |  |
| W5 | 133 | 133. Other sweets and confectionery | Other sweets and confectionary |  |

**Units (47, plus the missing-unit rule)**

| src | code | native label | rows | served `u` (+ `Size`) | alternative |
|---|---|---|---|---|---|
| W1 | 1 | tomatoes cup (0.087 kg) | 233 | `Tomatoes cup` | tomatoes cup |
| W1 | 5 | derica cup (big)  (0.7 kg) | 85 | `Derica` (Size `big`) | large derica |
| W1 | 6 | congo/mudu (1.5 kg) | 1841 | `Congo/Mudu` | congo/mudu |
| W1 | 41 | bunch of plantain/ffb: small (5 kg) | 122 | `Bunch` (Size `small`) | Bunch Small |
| W1 | 42 | bunch of plantain/ffb: medium (8 kg) | 164 | `Bunch` (Size `medium`) | Bunch Medium |
| W1 | 43 | bunch of plantain/ffb: big (15 kg) | 40 | `Bunch` (Size `big`) | Bunch Big |
| W1 | 51 | tuber of yam: small (3 kg) | 132 | `Tuber` (Size `small`) | Yam Tuber Small |
| W1 | 52 | tuber of yam: medium (5 kg) | 95 | `Tuber` (Size `medium`) | Yam Tuber Medium |
| W1 | 53 | tuber of yam: big/large (8 kg) | 42 | `Tuber` (Size `big/large`) | Yam Tuber Big/Large |
| W1 | 61 | bundle of millet, g/corn, sugarcane, vegetable etc: small (1 | 88 | `Bundle` (Size `small`) | Bundle Small |
| W1 | 62 | bundle of millet, g/corn, sugarcane, vegetable etc: medium ( | 18 | `Bundle` (Size `medium`) | Bundle Medium |
| W1 | 63 | bundle of millet, g/corn, sugarcane, vegetable etc: big (40  | 11 | `Bundle` (Size `big`) | Bundle Big |
| W1 | 71 | wheel barrow: small (60 kg) | 15 | `Wheelbarrow` (Size `small`) | Wheelbarrow Small |
| W1 | 72 | wheel barrow: medium (85 kg) | 36 | `Wheelbarrow` (Size `medium`) | Wheelbarrow Medium |
| W1 | 73 | wheel barrow: big/large (110 kg) | 9 | `Wheelbarrow` (Size `big/large`) | Wheelbarrow Big/Large |
| W1 | 74 | wheel barrow: extra large (150 kg) | 5 | `Wheelbarrow` (Size `extra large`) | Wheelbarrow Extra Large |
| W1 | 91 | jerry can, keg, rubber of palm oil: small(10 lt.) | 201 | `Jerry Can` (Size `small`) | Jerry Can Small |
| W1 | 92 | jerry can, keg, rubber of palm oil: medium (20 lt.) | 105 | `Jerry Can` (Size `medium`) | Jerry Can Medium |
| W1 | 93 | jerry can, keg, rubber of palm oil: big (25 lt.) | 34 | `Jerry Can` (Size `big`) | Jerry Can Big |
| W1 | 94 | jerry can, keg, rubber of palm oil: large (50 lt.) | 0 | `Jerry Can` (Size `large`) | Jerry Can Large |
| W1 | 95 | jerry can, keg, rubber of palm oil: drum (200 lt.) | 0 | `Jerry Can` (Size `drum`) | Jerry Can Drum |
| W1 | 104 | millilitre | 460 | `ml` | ml |
| W2 | 10 | MEDIUM DERCA | 68 | `Derica` (Size `MEDIUM`) | medium derica |
| W3 | 10 | 10. BIN/BASKET | 18 + 22 | `Basket` | bin/basket |
| W3 | 14 | 14. TIN | 30 + 9 | `Derica` | tin |
| W3 | 22 | 22. MUDU LARGE | 0 + 2 | `Mudu` (Size `LARGE`) | large mudu |
| W3 | 31 | (no native label) | 707 + 53 | `Mudu` (Size `LARGE`) | large mudu |
| W3 | 32 | 32. DERICA LARGE | 0 + 0 | `Derica` (Size `LARGE`) | large derica |
| W3 | 42 | (no native label) | 66 + 1 | `Derica` (Size `LARGE`) | large derica |
| W4 | 10 | 10. BIN/BASKET | 152 | `Basket` | bin/basket |
| W4 | 14 | 14. TIN | 131 | `Derica` | tin |
| W4 | 100 | 100. BUNCH/BUNDLE | 870 | `Bunch` | bunch |
| W4 | 190 | 190. JERRY CAN/RUBBER | 0 | `Jerry Can` |  |
| W4 | 210 | 210. STEMS | 0 | `210. STEMS` | Stem |
| W4 | 250 | 250. SACHET | 348 | `Packet/Sachet` | Sachet |
| W4 | 251 | 251. BAG OF 20 SACHETS | 103 | `Bag of 20 Sachets` | Bag of 20 Sachets |
| W4 | 260 | 260. SEEDLING/PLANT | 0 | `260. SEEDLING/PLANT` | Seedling/Plant |
| W5 | 4 | 4. CENTLITRES (CL) | 36 | `cl` |  |
| W5 | 10 | 10. BIN/BASKET | 72 | `Basket` | bin/basket |
| W5 | 40 | 40. DERICA/TIN | 855 | `Derica` | derica |
| W5 | 100 | 100. BUNCH/BUNDLE | 753 | `Bunch` | bunch |
| W5 | 120 | 120. SACHET | 2887 | `Packet/Sachet` | packet/sachet |
| W5 | 125 | 125. PACKET | 83 | `Packet/Sachet` | Packet |
| W5 | 190 | 190. JERRY CAN/RUBBER | 0 | `Jerry Can` |  |
| W5 | 210 | 210. STEMS | 0 | `210. STEMS` | Stem |
| W5 | 251 | 251. BAG OF 20 SACHETS | 464 | `Bag of 20 Sachets` | Bag of 20 Sachets |
| W5 | 260 | 260. SEEDLING/PLANT | 0 | `260. SEEDLING/PLANT` | Seedling/Plant |
| every source | (missing) | — | W1 390, W3 9 served | `Unknown` (data_info.yml sentinel) | dropped, as PH drops a blank unit |


## 8. Verification: commands, jobs, results

Every job uses the following setup (scripts in
`/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-wiring/work/`):
- `job.sh` clones the mirror at a full SHA, then runs `bin/savio_venv.sh mount` from the clone;
- `PYTHONPATH` is set to the clone, and `lsms_library.__file__` is asserted to be under it;
- a private `LSMS_DATA_DIR` per arm under `work/roots/` (asserted, never a shared root), plus private
  XDG / TMP / MPL / issues log;
- BLAS pinned, `LSMS_BUILD_WORKERS=1`, `LSMS_SKIP_AUTH` unset;
- `sbatch -A co_carleton -p savio4_htc -q carleton_htc4_normal -N1 -n1 -c 8 --mem=64G`.

| job | arm / SHA | what | result |
|---|---|---|---|
| 41237264 | `2b4a3632c` | `explore.py`: PP operand strata; PH decoder census | `work/out/explore/explore.json` |
| 41237359 | `2b4a3632c` | `explore2.py`: W5 `c2q2a` strata | §5 |
| 41237266 | base `e5fdd2fa8` | `measure_arm.py`: cold build of 6 tables, Aggregate, Feature | 4 min; `work/out/base/` |
| 41237703 | cand `03fe9034c` | the same | 7 min; `work/out/cand/` |
| 41237950 | cand | `post.py`: `compare.py` and `gcl_test.py` (`pp_tally` crashed on a script bug, rerun next) | §3, §4, §6 |
| 41238037 | cand | `pp_tally.py --inputs`, `w1_buckets.py` | §3, §6 |
| 41238086 | cand | `w5_internal.py` | §6 |
| 41237215 | base | the test selection on base | **4,311 passed, 106 skipped, 0 failed** (28 min) |
| 41237704 | cand `03fe9034c` | the same selection | **4,441 passed, 106 skipped, 1 failed**: `test_gh637_nigeria_w3_obs::test_rows_per_round_and_slot`, the per-round pin (PH quarters only, pre-fix 2024Q1 30,064); updated in `2d1d51412` (30 min) |
| 41238892 | final `d61ed2d37` | `final_check.py`: wave-level path; item-hold split | §3; §11 |
| 41238983 | final `d61ed2d37` | `final_eq.py`: cold `community_prices` vs the candidate's, both through a parquet round trip | **frame-equal** (272,918 × 3) |
| 41238893 | final `d61ed2d37` | targeted: `test_gh637_nigeria_w3_obs`, `test_nigeria_cluster_identity`, `test_gh587*`, `test_nigeria_community_units`, `test_derivations`, `test_no_build_input_in_contents_org`, `test_build_transform_hash`, `test_gh637_nigeria_w1w2_variety`, `test_community_prices_variety`, `test_gh1287_nigeria_four_declarations`, `test_feature_canonical_index` | **664 passed, 6 skipped, 0 failed** (7 min) |

The test selection is every `tests/test_*.py` naming Nigeria or community_prices (118 files on base),
plus `test_gh587*`, `test_derivations.py`, `test_build_transform_hash.py`,
`test_no_build_input_in_contents_org.py`, `test_community_prices_variety.py`,
`test_feature_canonical_index.py` and `test_coverage_matrix.py`. It runs with `-n 8 --dist=loadfile`,
and the output goes to `work/out/pytest-*.log`.

**Cold base vs candidate:**
- **`sample`, `household_roster`, `food_acquired`, `plot_features`, `crop_production`**:
  `assert_frame_equal(check_exact=True)` passes for all five.
- **`community_prices`**: PH as in §4. `Size` is added, all-NA on PH. Dtypes `Price` Float64,
  `Derivation` string and `Size` string. The index is unique.
- **Read-time audits**: identical warning sets base vs candidate for all six tables. `community_prices`
  files no Site B (null-read), Site I (null-index) or Site Q report in either arm. There is no new
  all-null required column. The only changed message is the `ClusterIdentityWarning` span, now
  `2010Q3 .. 2024Q1`; the same two declared boundaries apply.
- **Feature**: `Feature('community_prices')(['Nigeria','Tanzania','Uganda'])` gives 290,617 rows, with
  a **unique** index (base 164,540). All ten Nigeria quarters are present. Uganda declares no
  `community_prices`; Feature warns and drops it in both arms.
- **`labels='Aggregate'`**:
  - `LabelPassThroughWarning`: 1 of 142 `j` values unrenamed, on 837 rows, all PP W1/W2 `Pepper`.
  - `attrs['labels_unmatched']` = `{n_unmatched: 1, rows: 837, examples: ['Pepper']}`.
  - The Aggregate frame's index is not unique **in base too** (pre-existing: `reaggregate=False`
    renames W3–W5 items onto one label).
- **Synthetic tests**:
  - `tests/test_gh587_nigeria_pp_community_serving.py`: 75 new cases, through the real serving layer;
    crosswalk pins against MAPPING.org; wave-script AST wiring; registry.
  - `tests/test_gh587_nigeria_pp_price_reports.py`: the 47 existing cases.
  - `tests/test_nigeria_community_units.py`: 3 new decoder cases, 57 total.
- **Data-gated tests changed**:
  - `test_nigeria_cluster_identity.py`: `test_community_prices_serves_each_round_at_its_own_quarter`,
    `test_pp_derivation_inputs_join_and_reproduce_the_served_rows` (×2) and
    `test_pp_omissions_return_rows_the_table_does_not_serve` (×4).
  - `test_gh637_nigeria_w3_obs.py`: the per-round pin (`EXPECTED`) now includes the PP quarters and
    2024Q1 30,064 → 30,015.

## 9. Draft PR body

> **GH #587: serve Nigeria's five post-planting community_prices cells (+ a PH C8 decoder fix)**
>
> Every Nigeria PP community form prices food in Section C2. CONTENTS.org's "C8 is post-harvest only
> ... correctly empty" was contradicted by the forms and the files. Per the 2026-11-12 rulings, the five
> PP quarters are served on Nigeria's shared labels:
> - **126,126 rows** in total: 2010Q3 22,214; 2012Q3 21,124; 2015Q3 20,875; 2018Q3 32,246; 2023Q3 29,667.
> - `j` = the PP quarter's `harmonize_food` Preferred Label; `u` = the PH base unit.
> - The native size is in a new optional `Size` column.
> - W1 Price is `sc2q3a / sc2q3b` where N > 1 (registered `pp-w1-number-of-units`).
> - W4/W5 coded metric sizes are divided by the PH rule (`pp-c2-metric-size`, the same function).
> - Every unserved priced row is in a `rows: 0` registry entry, and its `inputs` return it.
> - The crosswalk lives in `Nigeria/_/pp_community_prices.py`.
>
> **Separate commit, a live PH defect**: `251. BAG OF 20 SACHETS` was served as `Sack/Bag` (2019Q1 148,
> 2024Q1 261 rows), and W5's `4. CENTLITRES (CL)` as `l` (2024Q1 49 rows). Now they are
> `Bag of 20 Sachets` and `cl`; the 49 have a blank size and drop under the existing
> `c8-unrecorded-size-drop`.
>
> **Measured** (cold, private roots; base `e5fdd2fa8`):
> - PH rows are frame-equal apart from those 458 rows.
> - `sample`, `household_roster`, `food_acquired`, `plot_features` and `crop_production` are frame-equal.
> - Read-time audit reports are unchanged.
> - `Feature('community_prices')` has a unique index.
>
> **For maintainer review (every judgement row; MAPPING.org's proposal implemented; full table in the report §7)**:
> - Items:
>   - W1/W2 76 → `Pepper`, not Fresh Pepper.
>   - W3 161 → `Palm wine`, not Palm oil.
>   - W2 15 Semovita and W2 0 (sentinel) unserved.
>   - Spelling / truncation rows served under the quarter resolver's label: W1 65, 66, 71, 113, 121, 133,
>     154, 155; W2 65, 153; W3 52, 53, 78, 94, 147, 153; W4/W5 52, 53, 94, 133.
> - W1 units:
>   - 1 → `Tomatoes cup` (new token);
>   - 5 derica cup (big) → `Derica`;
>   - 6 → `Congo/Mudu` (new token);
>   - 104 millilitre → `ml`;
>   - the same-card sized codes 41–43 / 51–53 / 61–63 / 71–74 / 91–95 → `Bunch` / `Tuber` / `Bundle` /
>     `Wheelbarrow` / `Jerry Can`, with the size in `Size`.
> - W2 10 MEDIUM DERCA → `Derica`.
> - W3 units: 10 BIN/BASKET → `Basket`; 14 TIN → `Derica`; 22 / 31 → `Mudu`; 32 / 42 → `Derica`.
> - W4/W5 units:
>   - 10 → `Basket`, and W4 14 → `Derica`;
>   - 100 BUNCH/BUNDLE → `Bunch`;
>   - 190 JERRY CAN/RUBBER → `Jerry Can`;
>   - 250 SACHET → `Packet/Sachet`;
>   - 251 → `Bag of 20 Sachets`;
>   - 210 / 260 verbatim (0 rows).
> - W5 units: 4 CENTLITRES → `cl`; 40 → `Derica`; 120 → `Packet/Sachet`; 125 PACKET → `Packet/Sachet`.
> - Any source: a missing unit → `Unknown` (W1 390, W3 9 rows), where PH drops it.
>
> **W5 `c2q2a`**: the 123 rows recording a quantity other than 1, and not already held for size, are
> excluded and registered.
> - On the 41 CUBE rows the price scales with it, so serving one cube would overstate the price.
> - On the 63 coded-metric-size BOTTLE/CAN rows it does *not* scale (`c2q2a` matches the coded size on
>   only 8). The PH division would serve them correctly, as PH serves its own `c8aq2_a` rows. Excluding
>   them is the conservative choice; serving them is the alternative.
>
> **Questions** (numbers in the report):
> 1. The W1 g/ml rows with N = 1 measure 144x the household per-gram price.
> 2. The ruled W1 division misprices Kg/l with N > 50 by 50–1000x.
> 3. On item 7: W3 Kg/l measure as true per-unit prices (1.07 / 1.00) and could be served. W3 g/cl are
>    package prices (150x / 57x). W5 g/cl look serveable as `c2q3 / c2q2a`.
>
> Cost: a Nigeria-wide re-warm; no other country moves.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 10. Draft #587 narrow / close comment

> The five PP community_prices cells are served (PR #___, 126,126 rows). The PH C8 decoder defect it
> found is fixed in the same PR. For community_prices, the remaining sub-questions are price-basis
> rulings, not missing wiring. Proposed: narrow #587's community_prices item to done on merge, and file
> one follow-up issue, "Nigeria PP community prices: residual price-basis rulings":
> - (i) W1 g/ml rows with N = 1 (1,043 rows; 144x);
> - (ii) W1 Kg/l rows with N > 50 (~332 rows);
> - (iii) metric units with no package size: 17,494 held rows. W3 Kg/l look serveable; W3 g/cl are
>   package prices; W2 g is mixed; W5 g/cl could be divided by `c2q2a`;
> - (iv) W5's 63 coded-metric-size BOTTLE/CAN rows with `c2q2a`;
> - (v) the label judgements listed in the PR.
>
> Close #587 only if nothing else hangs on it. CONTENTS.org still records #587 items for PH
> plot_features (W4/W5 held) and the W1/W2 PH coping gap.

## 11. Unverified limits

- Fielded CAPI versions of all five PP instruments are unknown. "Quantity of one" rests on the printed
  forms, for W5 by ruling.
- W5 has no household comparator. The W5 g/cl evidence is community-internal and thin (6 g / 5 cl items).
- PH W3's Kg/l drop was not re-measured against household values. Option (a) concerns PP only.
- The W1 bucket diagnostics are matched-cell medians, not row-level validation. Some cells are thin (ml
  N = 1: 2 cluster cells).
- `Size` is not filled for PH rows (out of scope); PH W4/W5 record a size the table does not carry.
- No PDF page images were viewed in this step. The form locators come from the mapping step (pypdf text
  layer).
- No full-corpus suite was run. The selection is every test file naming Nigeria or community_prices,
  plus the derivation, hash and drift tests.
- No cache-hash census was run. "Nigeria-wide re-warm" follows CLAUDE.md's hash rules and was not
  counted.
- The `unit-unplaced` disposition (an off-card unlabelled unit code) warns but has no registry entry. It
  is 0 rows today.
- The wave-level path was exercised only at the final head (job 41238892). `Country('Nigeria')['2010Q3' / '2011Q1' / '2023Q3'].community_prices()` returns exactly that quarter's rows (22,214 / 30,628 / 29,667), equal in count to the country frame's slice. It carries the `i == v` shim level, as PH wave calls always have.
- `.coder/coverage/latest.csv` still grades the five PP cells `dropped`; `make matrix` was not rerun.
- Fixed in `d61ed2d37`, after the full test run: the PP sources had been reached through a computed path,
  so a re-released PP source would not have invalidated the cache. The served rows were re-checked
  cold at the final head (§8).
- **One rule slip, disclosed**: a local pytest of `tests/test_gh637_nigeria_w3_obs.py` on n0035 (meant
  as a light check) also ran that file's data-gated class. It fetched ~44 MB of Nigeria sources into a
  *private* root (`/local/job41201840/tmp/p587/data`) and built `community_prices` (~20 s). No shared
  root was touched. All other data work ran in Slurm jobs.

## 12. Questions for the maintainer

1. **W1 g/ml with N = 1** (963 + 80 rows, served as recorded per the ruling) measure 144x the household
   per-gram price. Hold them like the unrecorded-size rows?
2. **W1 Kg/l with N > 50** (~332 rows): the ruled division leaves them 50–1000x under the household
   price. Hold them, or keep the division?
3. **Item 7.** Serve W3 Kg/l as recorded (4,120 rows, ~1.0x)? Serve W5 g/cl as `c2q3 / c2q2a` (208
   rows) as a new registered derivation? Keep W3 g/cl and W2 g held?
