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

### Phase 2 -- native measurement (2026-10-22)

Read-only. Nothing wired; no `data_scheme.yml`, `data_info.yml`, `niger.py` or
`categorical_mapping.org` edit; no unit converted; no `Quantity`/`variety`/
`visit`/`obs` fabricated. Every number below is in
`slurm_logs/isa_backlog_2026-10-21/823-niger-s05/measure_823.json` (re-run:
`measure_823.py` there, ~12 s, single process, private `LSMS_DATA_DIR`), with
`s05_unit_crosswalk.csv` and `s05_item_crosswalk.csv` (label metadata and
row counts) beside it; the row-level duplicate-key detail is written privately
to `$LSMS_DATA_DIR/823-niger-s05/s05_duplicate_keys.csv`, not committed. Sources: `s05_co_ner2018.dta` (read file
md5 = sidecar md5 `586edb9f…`), `s07b_me_ner2018.dta`, `s00_me_ner2018.dta`,
`Country('Niger').sample(waves=['2018-19'])`, the 2018-19 sidecars, the held
community questionnaire `.xls` files and the WB catalog 4296 DDI PDF (read
once with a scratch `pypdf`; pp. 4-5, 80, 875-882 quoted below).

**Answers to §6.**

1. *Identifiers.* A row is `(grappe, s05q01 product, s05q02 releveID unit-size)`
   with two price columns. There is **no** variety field, no market/seller/
   record field, and no visit beyond `vague` (each grappe is in exactly one
   vague: 0 of 504 in two; s05 `vague` equals the household cover's `vague`
   for 504/504). The only observation axis is the pair of columns
   `s05q03` "5.03. Relevé 1-Prix (en FCFA)" / `s05q04` "5.04. Relevé 2-Prix
   (en FCFA)". `Existe` is 1 on all 42,247 rows (pre-filtered).
2. *Price basis / quantity.* No physical quantity is shipped. At least 9.4% of
   the prices were produced by a DIVISION before distribution (see F3), which
   is consistent with Prix ÷ Quantité giving the price of ONE unit-size, but
   the Niger instrument that would confirm it is not held (F4). So the basis
   is *probably* one unit `u`, and that is not C4-validated.
3. *Units.* `releveID = 10 × s07bq03b unit code + s07bq03c size code` for all
   74 used codes. Only the 19 size-0 ("bare") codes, 9,199 rows (21.8%), can
   join `food_acquired.u` without rescaling. The 55 size-qualified codes
   (33,048 rows) join the household PAIR `(s07bq03b, s07bq03c)`, which
   `food_acquired.u` discards (F7).

**F1. Shape and label sets.** 42,247 × 7: `grappe` int16, `vague` int8,
`Existe` (`Existe_cereales`: Oui/Non), `s05q01` (`produitID`, 60 declared, 55
used; 22 Beignets, 24 chameau, 40 Marquereau fumé, 80 Tomate séchée, 112 Gari
declared with 0 rows), `s05q02` (`releveID`, 156 declared, 74 used), `s05q03`
/ `s05q04` float64. Vague 1: 249 grappes / 21,068 rows; vague 2: 255 / 21,179.
Products per grappe 14-55 (median 36); unit-sizes per (grappe, product) 1-8
(median 2).

**F2. Duplicate keys.** `(grappe, s05q01, s05q02)` repeats for 160 keys, each
exactly twice (320 rows, 137 grappes; adding `vague` changes nothing). 20 are
byte-identical rows (lossless drop, #840 class); 140 disagree on price. They
are scattered, not blocks (median row gap 7,726; cf. the contiguous 2011-12
CS07 double entry), and concentrated in `poisson séché` (96 rows),
`Maquereau` (78) and `Riz Importé non parfumé` (64). Nothing in the file says
whether they are collapsed varieties, two sellers, or entry errors. Melted to
slots, the 140 second records repeat the first record's same slot in 6
(slot 1) + 4 (slot 2) cases and carry 270 new prices.

**F3. Prices.** Both columns: 0 NA, 0 zero, 0 negative, 0 sentinel (999,
9999, 99999, 9999.9) -- already cleaned upstream. Range 2.34-90,000 (`s05q03`)
and 1.98-85,000 (`s05q04`) FCFA, median 500, p99 25,000.
- `s05q04 == s05q03` exactly in 29,783 rows (70.5%); per grappe the equal
  share is 0.27-1.00 (median 0.74), 0 grappes with none, 9 with all.
- Exact ratio 0.85: `s05q04 = 0.85·s05q03` in 1,296 rows (380 grappes, 40
  products, top-10 grappes hold 10.8%) and `s05q03 = 0.85·s05q04` in 767 (296
  grappes, 34 products). The neighbouring ratios 0.8 / 0.9 occur 433 / 571
  times, and the 0.85-side value is non-integer 32% / 43% of the time against
  ~10% overall. Over-represented and spread everywhere; origin undetermined.
- Non-integer FCFA: 3,973 `s05q03` (9.4%), 4,272 `s05q04` (10.1%), with clean
  denominators (`s05q03`: /2 1,999, /3 1,254, /4 445, /8 143, /6 54). A
  transaction amount cannot be 33.33 FCFA, so these were divided before
  distribution. The share rises for small sizes (bare 0.8%, petit 19.1%,
  quart 15.7%, moyen 7.3%, grand and demi 5.8%, entier 1.9%), as "n small
  heaps for a price" would; the size digit does NOT predict the denominator
  (`quart` mostly /2, not /4). Float32 noise is also present (`479.999977`).
- **The file is sorted on the two readings' ratio.** Rows 0-8,270 have
  `s05q04 < s05q03` (99.7%), 8,271-38,007 are equal, 38,008-42,246 have
  `s05q04 > s05q03` (99.1%); Spearman(ratio, row) 0.98 / 0.95 within the outer
  blocks. The contiguous "0.85 blocks" are an artefact of that sort, NOT
  evidence of separately processed subsets.
- Extreme pairs: ratio > 2 in 15 rows, < 0.5 in 26 (e.g. grappe 81, imported
  rice, Tiya entier, 1,200 vs 10,200 -- a keying slip). Site Q screens only
  `crop_production.Quantity`, so a wiring would serve these unflagged.

**F4. The held "Niger" community questionnaire is Chad's.**
`2018-19/Documentation/ner_ehcvm1_qnr_community_excel_vague{1,2}.xls` (md5
`7a72b806…`, `78545c3e…`) are the files the catalog 4296 DDI (p. 4-5) lists
as the Niger community questionnaires, but their cover reads `REPUBLIQUE DU
TCHAD`, "ENQUÊTE HARMONISEE … 2017/2018", citing Chadian statistics law, and
their price section lists `Riz de Laï` / `Riz de Bongor`. That template's
SEC5_PRIX has THREE relevés of (Quantité, Prix), numbered 5.03-5.08; Niger's
`.dta` has `5.03` = Relevé 1 Prix and `5.04` = Relevé 2 Prix and no quantity,
while the DDI file description (p. 80) says the section "provides three price
records". No Niger community instrument is held, so neither the price basis
nor the meaning of the two relevés can be checked against a questionnaire.

**F5. Coverage.** 504 s05 grappes = 504 `s00_me` grappes = 504
`sample(waves=['2018-19'])` `v` values (`format_id(grappe)`), exact equality.
Confirms the issue's 504/504.

**F6. Items.** Both files tag the item with a label set named `produitID`
(60 vs 138 labels) and every s05 code is a household code, but the strings
differ: s05 stores clean UTF-8 (`Thé vert`), s07b stores the double-encoded
form the 2018-19 `mapping.py` repairs (`ThÃ©`); code 23 is `Viande de
boe__uf` in s05 vs `Viande de bÅ__uf` in s07b, so `_LOST_PREFIX_REPLACEMENTS`
does not catch the s05 form. Through the production `harmonize_food` map
(`niger._community_prices_maps()`, resolved from the wave dir):
- label route: 39 codes hit an `Original Label` (30,106 rows); 41 codes /
  32,244 rows land on a `j` the household table serves; 14 codes / 10,003 rows
  do not;
- **code route** (same `produitID` code → the household's label for it →
  household pipeline): 55/55 codes, 42,247/42,247 rows land on a served `j`.
Of the 14 label-route misses, 11 are spelling/encoding only (`Mais`/`Maïs`,
`Pates`/`Pâtes`, case, `boe__uf`, `-`/`/`, `sillure fumée`/`Sillure fumé`);
**3 codes name a NARROWER product than the household code**: 67 `Pastèque`
vs `Pastèque, Melon` (904 rows), 77 `Courge/courgette` vs `Aubergine,
Courge/Courgette` (458), 130 `Thé vert` vs `Thé` (1,118).

**F7. Units.** For all 74 used codes the unit part (`code // 10`) is a
household `s07bq03b` code; its label matches after mojibake decode and accent/
case folding for 69, and the other 5 are spellings of the same unit
(`Canette`/`Cannette`, `Filet au kilogramme`/`Filet au Kg`, `Louche
(Djoni/Djini)`/`Louche`). The size digit matches the label suffix for 72;
**1252 is labelled `grand` and 1253 `moyen`**, the reverse of `s07bq03c`
(2 = Moyen, 3 = Grand) -- 79 rows, which is wrong is undetermined.
- 19 bare codes, 9,199 rows: via the code bridge 16 (9,096 rows) land on a
  `u` the household table serves (`Kg`, `Litre`, `Tiya`, `Sac (50 Kg)`, …);
  3 (`Filet au Kg` 51, `Sac (10 Kg)` 1, `Sac (5 kg)` 51) are household units
  no consumed household row uses. Via the label route and the `u` table: 14
  codes, 8,726 rows.
- 55 size-qualified codes, 33,048 rows (78.2%): no counterpart in
  `food_acquired.u`. But 65 of the 74 (unit, size) pairs, covering 42,014 rows
  (99.4%), are observed in household consumed rows' (`s07bq03a > 0`)
  `(s07bq03b, s07bq03c)`,
  and 91,125 of 107,216 household consumed rows (85%) carry a non-zero size
  that `food_acquired.u` drops.

**F8. Other waves and the stale comments.** 2021-22 ships `s00_co`-`s04_co`
only (no `s05_co*`), plus `ehcvm_prix` / `ehcvm_nsu` -- the issue is right.
2018-19 ships `s00a_co`, `s00b_co`, `s01_co`-`s05_co` and NO
`ehcvm_prix`/`ehcvm_nsu` sidecar. So `Niger/_/data_scheme.yml:472-481`
("only the two ECVMA waves carry an EA-level community price module"; "the
EHCVM waves (2018-19, 2021-22) replaced the community price module with …
ehcvm_prix") is false for 2018-19 on BOTH counts, beyond the bracketed
correction at :482-484; `niger.py:1293-1297` repeats the claim. Not edited
here (`data_scheme.yml` and `niger.py` are in every Niger cache hash).

**WIRING PROPOSAL** (for review; not implemented).

One wave script `2018-19/_/community_prices.py` on the independent-wave
pattern, aggregated by the GH #1091 framework builder (no country
concatenator), reusing `_community_prices_maps` and `_finish_community_prices`
(its duplicate-key assert and the `i == v` shim apply unchanged). It cannot
reuse `_community_price_triples`, whose `codes`/`cs07q02`/`numquest` contract
is CS07-specific; a small s05 melt is needed.

| level | native source | rule |
|---|---|---|
| `t` | -- | `'2018-19'` |
| `v` | `grappe` | `format_id`; equals `sample().v` 504/504 |
| `j` | `s05q01` code | code bridge: household `s07bq01` label of the same code → `harmonize_food` → decode (55/55); D3 |
| `u` | `s05q02` code | size 0: household unit label of `code // 10` through the household pipeline (`Kg`, `Litre`, `Tiya`, …); size > 0: keep the size-qualified label (`Tas, petit`), never collapse to `Tas`; D4 |
| `variety` | none | sentinel `unknown_variety` (size is a unit attribute, not a variety) |
| `visit` | `vague` | native 1/2 as in Niger 2018-19 `food_acquired` (`visit: vague`); a between-cluster fielding period, not a repeat visit; D6 |
| `obs` | column | `s05q03` → 1, `s05q04` → 2; a duplicated key's second record → 3/4 (slot + 2·rank, the CS07 slot + 3·rank convention); D1, D5 |

- `Price` = `s05q03` / `s05q04` as recorded (FCFA; no rounding of float32
  noise; nothing to filter -- 0 NA/zero/sentinel). Reads as "price of one unit
  `u`" if F3's division is Prix ÷ Quantité; that reading is not C4-validated.
- `Quantity` = NA. The file records none; 1 would be a claim about an
  unshipped field. Consequence: Niger's `data_scheme.yml` declares `Quantity:
  float` **required**, so Site B reports a required column 100% null in the
  2018-19 `t` slice (`NullReadWarning`, fatal under `LSMS_READ_STRICT=1`). D2.
- `Description` = the s05 product label as recorded (keeps `Thé vert` where
  `j` is the household's broader label).
- Byte-identical rows (20) dropped before the melt; second-record slots that
  repeat the first record's same slot (10) dropped after it -- both the
  existing #840 lossless class. Projected served rows: 42,227 records × 2 −
  10 = **84,444** (270 of them obs 3/4), all 504 grappes. No unit conversion,
  no rescaling.
- Cache: a new `.py` in `2018-19/_/` enters every 2018-19 table's input hash
  (`binp:` glob), i.e. one Niger re-warm, plus whatever `data_scheme.yml`
  comment edit is approved.

**Decisions for @ligon.**
- D1 *Both relevés as obs 1/2?* 70.5% of pairs are identical, 2,063 rows sit
  on an exact 0.85 ratio of undetermined origin, ~10% of values are
  producer-divided. Serve both as recorded (the #840 / Mali #822 "select
  nothing" rule), or flag the 0.85 rows?
- D2 *Quantity:* NA with the Site B report (visibility; `optional: true` is
  country-grain and would disarm the two ECVMA waves too), make it optional in
  the scheme, or rule that a per-unit price makes `Quantity = 1` a definition
  -- the last needs a Niger instrument first.
- D3 *j:* code bridge (55/55) vs label route (41/55); and whether the 3
  narrower products (`Pastèque`, `Courge/courgette`, `Thé vert`) take the
  household's broader `j` or their own Preferred Labels.
- D4 *u for sized units:* serve `Tas, petit` (joins nothing in
  `food_acquired.u`) or first extend food `u` to carry `s07bq03c` so 99.4% of
  community rows join by the pair; and the `Louche` 1252/1253 label-vs-digit
  swap.
- D5 *The 140 conflicting duplicate keys:* obs 3/4 (Niger's convention) or a
  `record` level -- the open #840 residual decision, same question.
- D6 *visit:* native `vague` (consistent with `food_acquired`) or sentinel 1.
- D7 *Instrument:* catalog 4296 ships Chad's community questionnaire under
  Niger's name; acquire Niger's (INS Niger) before any C4-grade claim about
  the price basis or the missing third relevé.

**Not determined.** Whether a price is per one unit-size or per an unshipped
quantity; where the 0.85 ratio comes from; whether identical pairs are two
agreeing sellers or a copied reading; what the 140 conflicting duplicate keys
are; which of the `Louche` label or code is right; why the DDI says three
records where two are shipped.

### Phase 3 — wiring and verification (2026-10-22)

Decisions D1-D7 were taken by @ligon on 2026-10-22, relayed by the
coordinator, and implemented on branch
`fix/823-niger-2018-community-prices-20261022` (development `9a0ed5f3c`
merged).  D7 resolved the instrument question: Niger's own CAPI price
questionnaire ("Qx Communautaire Prix- NIGER_V1_VF") is on the UEMOA regional
catalogue, entry 48 (`/download/199`, `/download/200`).  It records two
integer, multiple-of-5 prices per selected unit-size, with no quantity and no
variety.  The non-integer prices (F3) were therefore produced after collection.
The CAPI code list also prints `1252 = moyen`, `1253 = grand`, so the `.dta`
value label is the swapped one.

**What changed.**

| file | change |
|---|---|
| `Niger/2018-19/_/community_prices.py` | new wave script (s05 → seven-level table) |
| `Niger/_/niger.py` | `_s05_unit_label`, `_s05_code_maps`, `_s05_community_prices`, `_s05_raw_inputs`; `_community_prices_maps(dirs=None)`; corrected WAVE COVERAGE comment |
| `Niger/2018-19/_/mapping.py` | `derive_s05_one_item_per_price` and `inputs_community_prices`, the registry callables (the Nigeria #834/#900 wave-scoped placement) |
| `Niger/_/derivations.yml` | `Niger::community_prices::one-item-per-price` (waves 2018-19, columns Quantity, bases questionnaire + modelling-choice) |
| `Niger/_/data_scheme.yml` | comments only (coverage, 2018-19 levels, Quantity); no key added; `Derivation` is NOT declared there (`test_nothing_in_a_data_scheme_names_a_derivation`) |
| `Niger/_/CONTENTS.org` | new section "2018-19 community prices are wired (GH #823)"; stale passages annotated |
| tests | new `tests/test_gh823_niger_s05_community_prices.py`; Niger 2018-19 baselines in `test_community_prices_variety.py` (CS07-only tests scoped to the CS07 waves; `unknown_variety` exempt from the code check) and `test_niger_u_sentinel.py` (46,043 → 130,487 rows, 2018-19: 0 `Unknown`) |

The canonical `data_info.yml` was not edited: `Derivation` was already declared `optional`
under `community_prices` (Nigeria #834).  No precedent serves community `Quantity = 1` elsewhere.
Ethiopia reads a recorded quantity, and GhanaLSS and Malawi carry `NumberOfUnits`.

**Rules as built (§2-4 checked).**
- `u`: `releveID = 10 × s07bq03b + s07bq03c`.  A size-0 code takes the household unit label through
  the household pipeline (decode, then the `u` table).  A sized code becomes `<unit> (<Size>)` with
  the household's size word (Uganda's `Heap (Small)` form).  The size comes from the digit.  The two
  code→label maps must be one-to-one or the build raises.
- `j`: matched by code to the household `s07bq01` label, through `harmonize_food` and the decode.
  `Description` keeps the s05 label.
- `obs = slot + 2·rank` (rank in file order within the key).  20 byte-identical rows are dropped
  before ranking; 10 second-record slots that repeat the first record's same slot are dropped after
  the melt.  Both are the #840 lossless class, so `_finish_community_prices`'s duplicate-key
  assertion holds unchanged.
- `Quantity = 1.0` (Float64) and `Derivation` are stamped by the registered function after
  `_finish_community_prices`, i.e. after every drop.

**Verification.**
- Focused test `tests/test_gh823_niger_s05_community_prices.py`.  The 17 synthetic tests cover the
  unit rule (size 0, sized, Louche 1252/1253, unknown code), the code bridge (narrower products,
  collision), obs numbering with a duplicate key, the byte-identical copy and the repeated slot,
  `Existe`, and `Quantity`/`Derivation`/the registry entry.  On development (`9a0ed5f3c`) all 17
  fail at fixture setup (registry key absent, `_s05_*` absent).  On the branch all 17 pass, plus
  3 data-gated tests.
- `test_gh823…`, `test_community_prices_variety.py`, `test_gh1091_niger_community_builder.py`
  and `test_derivations.py`: **153 passed**, 0 skipped (data-gated tiers ran).  Neighbours
  `test_niger_u_sentinel.py`, `test_feature_canonical_index.py`, `test_962_unit_sentinel.py`
  and `test_no_c1_controls_in_curated_labels.py`: 187 passed, 3 skipped (warm-cache gates).  The 2
  failures were `test_niger_u_sentinel`'s pinned Niger community counts, updated as above; its
  `community_prices` cases then passed (4/4).
- Real data, cold, separate private data roots (`validate_823.py`; aggregates in
  `validate_823.json`):
  - 2011-12 / 2014-15 are **identical** to development in every column development serves
    (46,043 rows, `assert_frame_equal(check_exact=True)`).  The only new column is `Derivation`,
    which is NA on those rows.
  - 2018-19: **84,444 rows** (the Phase 2 projection) in 504 grappes, 55 `j` and 74 `u`.
    `obs` 1/2 = 42,087 each, 3 = 134, 4 = 136; `visit` 1 = 42,117, 2 = 42,327; `variety` all
    `unknown_variety`; `Quantity` all 1.0; `Derivation` all the key.  Top units:
    `Sachet (Petit)` 8,658, `Unité (Petit)` 6,046, `Tas (Petit)` 5,744, `Sachet (Moyen)` 5,180,
    `Kg` 5,076.
  - Shared labels with Niger 2018-19 `food_acquired`: all 55 `j`; 16 of 19 size-0 `u`
    (18,181 rows).  `Filet au Kg`, `Sac (10 Kg)` and `Sac (5 kg)` (206 rows) are household units
    no served food row uses.  0 of 55 sized labels (66,057 rows), as decided.
  - `Feature('community_prices')(['Niger'])`: 130,487 rows, canonical index plus `country` /
    `currency`; `attrs['derivations']` carries the key at 84,444 rows.
  - `derivation_inputs(key)`: 42,247 source rows with the original names.  42,087 of 42,087
    served `(t, v, j, u, visit)` keys are found, and every input key is served.  0 served rows
    are unmatched.  On 83,854 single-record obs 1-2 rows the served Price equals the source
    reading.
  - The branch frame also carries `attrs['recall']`.  This is pre-existing: `attach_recall`
    attaches Niger's 2018-19 FOOD recall record to any table covering that wave.  Metadata
    only; noted, not changed.
- Cache (`hash_census.py`, base vs branch; `hash_impact_823.json`): 23 of 549 country hashes
  (Niger only), 0 of 40 `build_transforms_fingerprint`, 92 of 2,109 wave hashes (Niger only,
  23 per wave).  One Niger re-warm.

**Precedents followed (standing guidance, @ligon 2026-10-22).**  For each sub-decision the
decisions did not settle, the precedent found in other countries (paths under
`lsms_library/`; lines at `9a0ed5f3c`).

| sub-decision | built | precedent |
|---|---|---|
| wave-script layout | `sys.path` + `niger` helpers, labels and codes read twice, `_finish_community_prices`, assert, `to_parquet` | `countries/Niger/2014-15/_/community_prices.py` (whole file); `countries/Mali/2014-15/_/community_prices.py:44-91` |
| `Description` where the source has no variety label | s05 product label as recorded, stripped | Tanzania `countries/Tanzania/_/tanzania.py:1988-1993` (source item label); GhanaLSS `countries/GhanaLSS/2012-13/_/community_prices.py:55` (item label) |
| `obs` = reading number | s05q03 → 1, s05q04 → 2 | Mali `countries/Mali/2014-15/_/community_prices.py:65,78` (`enumerate(_READINGS, 1)`) |
| second source record | `obs = slot + nslots·rank` (3/4), repeats dropped | Niger CS07 `countries/Niger/_/niger.py:1460,1487,1504`; GhanaLSS slot + 3 (`data_info.yml:166-167`) |
| rows with no usable price | dropped (none present) | `countries/Niger/_/niger.py:1494` |
| `visit` for EHCVM | `vague` (D6) | `visit: vague` in 14 EHCVM wave configs, e.g. `countries/Niger/2018-19/_/data_info.yml:42`, `countries/Mali/2018-19/_/data_info.yml:190`, `countries/Senegal/2018-19/_/data_info.yml:174` |
| sized-unit labels | `<unit> (<Size>)`, size word title-cased in the survey's language (D4) | Uganda `countries/Uganda/_/categorical_mapping.org:359-361` (`Heap (Big/Medium/Small)`); Malawi `countries/Malawi/_/categorical_mapping.org:291-293, 333-335` (`Pail (Small)`, `Heap (Large)`).  Anti-precedent: Nigeria folds ordinal sizes into the base unit and records that as a loss (`countries/Nigeria/_/nigeria.py:2380-2384`) |
| derivation function/inputs placement | wave `mapping.py` wrappers over country-module logic | Nigeria `countries/Nigeria/2018-19/_/mapping.py:34,50`; Niger `countries/Niger/2018-19/_/mapping.py:22` |
| `Derivation` dtype and stamping | nullable `string`, stamped after the last drop | Nigeria `countries/Nigeria/_/nigeria.py:2448`; CLAUDE.md "Derived Values" (stamp after collapse) |
| assumption basis for an instrument statement | `questionnaire` | Nigeria `countries/Nigeria/_/derivations.yml:45` |
| `variety` where the form names none | `unknown_variety` | **No native-frame precedent.**  Followed the canonical sentinel's definition (`data_info.yml:269`, value at `:299`).  The structural alternative, Tanzania's source item code as `variety` (`countries/Tanzania/_/tanzania.py:2000-2004`), applies only where several codes are lumped into one `j`; s05 codes are one-to-one with `j`.  **Flagged for review.** |
| `Quantity` on a per-one-unit price | `1` as a registered derivation (D2) | **No precedent serves a constant `Quantity`**, and none registers a constant as a derivation.  The per-one-unit precedent is to serve `Price` only: Nigeria `countries/Nigeria/_/data_scheme.yml:571-572`, Tanzania `:396-397`, EthiopiaRHS `:254-255`.  Niger's scheme requires `Quantity` for all its waves, and D2 decided 1 explicitly.  **The tension is flagged, not resolved here.** |

**Publication.**  Row-level outputs (built frames, `derivation_inputs`, duplicate-key detail, full
warning text) were written only under `/local/job40390255/data-private/lane-823/`.  The committed
JSON holds counts, labels and hashes summaries, and warning *classes* only.

**Not determined / residual.**
- What the 140 conflicting duplicate keys are (two enumerators of the same product is possible;
  the file has no enumerator id).
- The origin of the 0.85 ratios and of the non-integer prices.
- `.coder/coverage/latest.csv` still grades the cell `absent` until the next `make matrix`.
- `food_acquired.u` still drops `s07bq03c`, so sized community prices have no household `u` to
  join without rebuilding the pair.
