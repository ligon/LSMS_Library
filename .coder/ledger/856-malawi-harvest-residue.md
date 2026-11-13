# Prior-Art Ledger — #856 Malawi harvest other-specify residue

**Search tier:** ripgrep + git. Source and sidecar metadata only; no survey payload read.

## §1 Task

Measure, per buildable Malawi crop-production wave and source half, the raw Module G/P harvest rows carrying an other-specify unit or crop code and their own specify texts. Decide whether recovery merits a later design review. The #869 unmapped-crop warning is already implemented.

## §2 Existing machinery

| Symbol | Source | Role | Tested |
|---|---|---|---|
| `_harvest_block` | `Malawi/_/malawi.py:1210` | Maps raw G/P crop and unit codes; reads no specify text | `test_malawi_cassava_recode.py`, `test_malawi_shipped_factors.py` |
| `_sale_block` | `Malawi/_/malawi.py:1300` | Independently maps I/Q sale unit and crop codes | `test_malawi_sale_basis.py` |
| `assemble_crop_production` | `Malawi/_/malawi.py:1907` | Warns on unmapped crop drops at :2014; preserves declared grain | `test_malawi_cassava_recode.py` |
| `_clean_freetext_unit` | `Malawi/_/malawi.py:79` | Food-side specify-text cleaner, deliberately conservative | `test_malawi_unit_cleanup.py`, `test_gh1042_missing_unit_spellings.py` |
| `harmonize_crop_unit`, `harmonize_crop` | `Malawi/_/categorical_mapping.org:687`, `:610` | Code maps; unit 13 is Other (Specify), seasonal crop 48 and perennial 1018 remain generic Other | mapping/build tests |
| `get_dataframe` | `STANDING.md §2/§4` | Sanctioned native reader after source access is authorized | existing reader tests |

## §3 Definitions and conventions

- Crop harvest `Quantity` stays the survey's native amount; `u` is the harvest unit, while sale `Quantity_sold` uses its own unit (`CONTENTS.org:119`, `malawi.py:1300`).
- Module P uses the +1000 crop namespace; IHS4/IHS5 P recodes 100→1001 and 21→1018, while Q keeps the old sale codes (`CONTENTS.org:676`, `malawi.py:1139`).
- `condition` is a distinct reported form axis; P never asks it (`CONTENTS.org:719`, `STANDING.md §3`, `data_info.yml`).
  **Corrected in Phase 2:** P never asks the shelled/unshelled FORM, but IHS4/IHS5 P does ask a FRESH/DRY question (`ag_p09_1`); see Phase 2 finding 5.
- The repo-wide source and schema conventions are inherited from `STANDING.md §3/§4`, `CLAUDE.md`, and `lsms_library/data_info.yml`.

## §4 Invariants and assumptions

- 2010-11 reads Full_Sample only; 2013-14 is panel; 2016-17 and 2019-20 concatenate Cross_Sectional and Panel with different household-id handling (`CONTENTS.org` and four wave scripts).
- Seasonal crop 48 and perennial Other (18 early, 21 late) currently map to generic `Other`, so a generic label is not an unmapped-crop drop. Confirm these meanings against each source's native value labels before using them as measured categories.
- No specify-text column is named by the current harvest scripts. `unit_os` and `crop_code_os` occur in EPAR comparisons, not as verified column names in our held extracts. Exact names and presence remain unknown pending native metadata.
- All 12 declared G/P payloads are absent in-tree. All 12 exact L1 blobs are present through both the home-cache aliases and the configured scratch `data_dir`; these paths identify the same held files. No payload was opened in this phase; no new acquisition is needed for the next native-read diagnosis.

## §5 Reuse decision

| Quantity or behavior | Decision | Reason |
|---|---|---|
| Source read | Reuse `get_dataframe` later | `STANDING.md §2/§4`; no parallel reader |
| Unit cleanup | Consider reuse or harvest-specific extension after measurement | Food's code-23 vocabulary does not establish crop code-13 semantics |
| Crop identity recovery | Defer | Own-source texts and counts are unmeasured; never copy EPAR strings |
| Quantity magnitude | Preserve native | Any kg interpretation belongs in a factor layer, not stored `Quantity` |

## §6 Open questions for root

- The configured L1 root holds all 12 blobs. Root reviews the bounded native-read plan before that separate diagnosis phase.
- Does the raw source's own label metadata name the other-specify codes and text columns in each half? This is the first next read, before any recovery design.

### Phase 3 (placeholder from Phase 1)

Not applicable at Phase 1: no implementation or numerical diagnosis had occurred.
Superseded by the dated Phase 2 and Phase 3 sections below.

### Phase 2 -- native measurement (2026-10-22)

**What ran.** `slurm_logs/isa_backlog_2026-10-21/856-harvest-residue/measure_856.py`
(re-runnable; output `measure_856.json` + `measure_856_summary.csv` beside it).
Each of the 12 G/P files the four wave scripts pass to `_harvest_block` was read
with `get_data_file` -> `get_dataframe(convert_categoricals=False)`; Stata
variable and value labels were read with `StataReader` for metadata only.
Denominator = the rows `_harvest_block` keeps (its only row filter is a non-null
crop code). Read-only: no library, config or mapping file changed.

**Cross-checks against recorded numbers.** Module G builder rows sum to
**112,691** over the four waves, exactly `CONTENTS.org`'s "112,691 Module-G
harvest rows in all four waves" (#854 entry). Perennial code-21 rows in the four
IHS4/IHS5 P files sum to **385**, exactly `CONTENTS.org`'s "385 rows coded
=1021=" (#869 entry).

**Codes, confirmed from each file's own value labels.** Unit 13 = `OTHER
(SPECIFY)` in all 12 files. Seasonal crop 48 = `OTHER (SPECIFY)` in all six G
files. Perennial OTHER = 18 in 2010-11 and 2013-14, 21 in 2016-17 and 2019-20
(-> 1018 after `_PERENNIAL_RECODE`). Seasonal 10/16/26 are `OTHER TOBACCO /
GROUNDNUT / RICE (SPECIFY)`: the crop is named and only the variety is
unspecified, so they are **not** counted as generic Other (804 G rows in all;
their 2010-11 text is mostly `RICE` 74 / `GROUNDNUTS` 45, which adds nothing).

#### Per wave / half / module

| wave | half | mod | file rows | builder rows | unit 13 (share) | u13 w/ text | u13 distinct / k80 / k90 | generic-other crop code | other-crop rows (share) | w/ text | crop distinct / k80 / k90 | union share |
|---|---|---|---:|---:|---:|---:|---|---|---:|---:|---|---:|
| 2010-11 | Full Sample | G | 28,143 | 27,813 | 42 (0.15%) | 41 | 16 / 9 / 12 | 48 | 328 (1.18%) | 326 | 26 / 3 / 7 | 1.32% |
| 2010-11 | Full Sample | P | 12,531 | 5,797 | 205 (3.54%) | 205 | 9 / 1 / 2 | 18 | 36 (0.62%) | 22 | 16 / 12 / 14 | 4.14% |
| 2013-14 | Panel | G | 10,169 | 9,985 | 96 (0.96%) | not held | -- | 48 | 167 (1.67%) | not held | -- | 2.61% |
| 2013-14 | Panel | P | 4,505 | 2,714 | 122 (4.50%) | not held | -- | 18 | 43 (1.58%) | not held | -- | 6.08% |
| 2016-17 | Cross Sectional | G | 26,506 | 25,975 | 795 (3.06%) | 758 | 48 / 7 / 17 | 48 | 371 (1.43%) | no column | -- | 4.42% |
| 2016-17 | Cross Sectional | P | 4,251 | 3,959 | 130 (3.28%) | 130 | 25 / 8 / 14 | 21 | 39 (0.99%) | 39 | 19 / 12 / 16 | 4.22% |
| 2016-17 | Panel | G | 6,289 | 6,289 | 105 (1.67%) | 57 | 11 / 3 / 6 | 48 | 111 (1.76%) | no column | -- | 3.36% |
| 2016-17 | Panel | P | 1,064 | 964 | 25 (2.59%) | 25 | 7 / 3 / 5 | 21 | 1 (0.10%) | 1 | 1 / 1 / 1 | 2.70% |
| 2019-20 | Cross Sectional | G | 32,677 | 32,651 | 600 (1.84%) | 600 | 108 / 23 / 48 | 48 | 990 (3.03%) | no column | -- | 4.79% |
| 2019-20 | Cross Sectional | P | 7,893 | 7,883 | 137 (1.74%) | 137 | 55 / 28 / 42 | 21 | 312 (3.96%) | 311 | 111 / 49 / 80 | 5.45% |
| 2019-20 | Panel | G | 9,984 | 9,978 | 145 (1.45%) | 145 | 43 / 14 / 29 | 48 | 234 (2.35%) | no column | -- | 3.74% |
| 2019-20 | Panel | P | 1,601 | 1,586 | 14 (0.88%) | 14 | 12 / 10 / 11 | 21 | 33 (2.08%) | 33 | 17 / 11 / 14 | 2.96% |

"k80 / k90" = how many distinct normalised strings (strip, collapse spaces,
upper-case) cover 80% / 90% of the text-bearing rows. "union share" = unit 13
OR generic-other crop.

**Totals (all 135,594 builder rows).** Unit 13: **2,416 rows (1.78%; 1.91% of
rows with a positive Quantity)**. Generic-other crop: **2,665 rows (1.97%; 1.75%
of positive-Quantity rows)**. Either: **4,998 (3.69%)**. By wave, the union is
1.80% / 3.35% / 4.17% / 4.63%. Within-unit quantity weight (no kg conversion):
generic-other crop rows carry 0.0-2.2% of each cell's KILOGRAM-reported
Quantity, except 2013-14 P at 7.3% of a 59-row base. A cross-unit "quantity
share" of unit-13 rows is not computable: their unit is the unknown.

#### The real specify-text columns

| wave / half | G crop text | G harvest-unit text | P crop text | P harvest-unit text |
|---|---|---|---|---|
| 2010-11 Full_Sample | `ag_g0d_os` | `ag_g13b_os` | `ag_p0d_os` | `ag_p09b_os` |
| 2013-14 | none held | none held | none held | none held |
| 2016-17 Cross_Sectional | **none** (only variety `ag_g0e_1_oth`) | `ag_g13b_oth` | `ag_p0c_oth` | `ag_p09b_oth` |
| 2016-17 Panel | **none** (only variety `ag_g0e_oth`) | `ag_g13b_oth` | `ag_p0d_oth` | `ag_p09b_oth` |
| 2019-20 Cross_Sectional | **none** (only variety `ag_g0e_1_oth`) | `ag_g13b_oth` | `ag_p0c_oth` | `ag_p09b_oth` |
| 2019-20 Panel | **none** (only variety `ag_g0e_1_oth`) | `ag_g13b_oth` | `ag_p0d_oth` | `ag_p09b_oth` |

The P crop-text name does not follow the P crop-code name: Panel files code the
crop in `crop_code` but spell the text `ag_p0d_oth`. 2010-11 G also carries
`ag_g09a/b/b_os/c` (a second harvest question, populated only on visit-2 rows);
the builder reads `ag_g13*` only, and only 14 rows have `ag_g09a` without
`ag_g13a`. EPAR's `unit_os` / `crop_code_os` are not column names in any held
file.

#### Findings

1. **The unit residue is mostly two codes the IHS4/IHS5 Module G list
   dropped.** `ag_g13b`'s own value labels lack code 3 (90 KG BAG) from 2013-14
   and codes 3 and 11 (BASKET (DENGU)) from 2016-17; the 2016-17 and 2019-20 G
   data carry **zero** rows of either code, against 599 and 1,484 in 2010-11 G.
   Respondents moved them to 13 + text: of the 1,645 code-13 G rows in those
   two waves, **886 (54%)** restate one of the two (`90 KG BAG`/`90KG BAG`/
   `90KG` ... 687; `DENGU`/`BASKET`... 199 -- an indicative first-match regex
   bucket, `UNIT_TEXT_BUCKETS` in the script, not a mapping). Another 159 name
   another explicit kg size (`70 KG BAG`, `100 KG BAG`, ...), 337 a container
   word (basin / pail / bucket / tin / litre), 219 are unclassified. IHS4/IHS5 Module P
   (which kept codes 3 and 11) shows no such shift: its unit text is dominated
   by `MKOKO` (172 of 205 in 2010-11) and `CLUSTER(S)` / `TREE(S)` / `DOZEN` /
   `CASE` in IHS4/IHS5 -- self-described counts with no code. So 2016-17+ G
   `u = 'Other (Specify)'` hides most of what 2010-11 serves as `90 kg Bag` and
   `Basket (Dengu)`: a cross-wave break on the `u` axis, small in rows.
2. **2013-14 other-specify text is not held.** Neither `AG_MOD_G_13` nor
   `AG_MOD_P_13` has a specify column; both carry `occ`, labelled "Case Section
   Roster / Line Occurrence Sequence for O/S Merge", and none of the 104 held
   2013-14 files is an O/S file. Codes are countable (218 unit-13, 210
   generic-other rows); the strings cannot be measured. Whether the World Bank
   distributes a separate O/S file for IHPS 2013 was not determined.
3. **Seasonal crop-name text exists in Module G only in 2010-11** -- but
   Module D names the IHS4/IHS5 rows. `ag_mod_d` carries the plot's crop list
   (`ag_d20a..e`, same codes) and `ag_d20_oth` / `ag_d20b_oth` "Other Crop".
   Joining G code-48 rows on (household, `gardenid`, `plotid`): 2016-17 Panel
   111/111 matched with text (111 unambiguous); 2019-20 XS 990/990 (963 with
   exactly one other-crop text); 2019-20 Panel 234/234 (234). 2016-17 XS
   `ag_mod_d.dta` could not be read through `get_dataframe` (with or without
   `encoding='latin-1'`: "has a Stata header but cannot be decoded" -- the bad
   byte `CONTENTS.org` records for that file), so its 371 code-48 rows are
   undetermined. The variety text on code-48 rows is near-empty (2016-17 XS 1,
   Panel 7, 2019-20 0).
4. **Seasonal "Other" is mostly cowpea, a crop `harmonize_crop` has no code
   for.** Literal `COW PEA` spellings: 157 of 326 texts in 2010-11 G (plus
   local-name strings `NSEULA` 77 and `MSEWULA` 8, untranslated here); via
   Module D 65 of 111 (2016-17 Panel), 584 of 963 (2019-20 XS), 113 of 234
   (2019-20 Panel). **919 rows say cowpea literally** -- 56% of the 1,634
   seasonal code-48 rows with one unambiguous held text (1,661 have any text;
   27 2019-20 XS plots carry two "Other Crop" texts). The rest is a long tail
   (`MUCUNA`, `NKHUNGUDZU`, `CUCUMBER`, `SESAME`/`CHITOWE`, ...), plus crops that
   DO have codes (`CASSAVA` 29+3, `SORGHUM` 8, `PIGEON PEAS` 3, `TOBBACO` 4).
   Perennial "Other" (464 rows) is mostly sugar cane (a seasonal code, 39) and
   trees: 2019-20 XS `SUGARCANE` 106 (+ `SUGAR CANE` 6, `SUGARCANES` 4),
   `ACACIA` and misspellings ~37, `BLUEGUM` 20; 111 distinct strings, 89
   singletons.
5. **CONTRADICTION with CONTENTS.org: IHS4/IHS5 Module P does ask a condition
   question.** `CONTENTS.org` (#833 entry): "every Module P harvest row is on the
   harvest side, in every wave (P asks no condition question)"; (#854 entry):
   "every perennial Module-P row, which asks no condition question";
   `categorical_mapping.org`: "the perennial Module P, which asks no condition
   question at all". Measured: all four IHS4/IHS5 P files carry `ag_p09_1`
   "What was the condition of [TREE/PERM CROP]?" = 1 FRESH / 2 DRY / 3 NOT
   APPLICABLE, answered on 2,989 / 767 / 7,050 / 1,410 rows (FRESH 2,325 / 629 /
   6,218 / 1,177). It is a DRYNESS axis, not the shelled/unshelled FORM, so the
   S/U statements ("Module P never asks S/U") stay true and the sale-basis
   wildcard is unaffected. Absent in 2010-11 and 2013-14 P. Unread by the
   builder. Out of #856 scope; a follow-up-issue candidate (does the shipped
   IHS5 tree table, or any kg factor, need fresh vs dry?).
6. **A third stale claim.** `CONTENTS.org` (livestock): code 318 "Other
   (Specify)" -> NaN -> dropped from the index, "like `crop_production`'s
   "Other" crop". It is not: crop 48 / 1018 map to `Other` and are served
   (the 2,665 rows above). Sentence corrected in `CONTENTS.org`.
7. **Smaller things seen.** Off-scheme unit codes: 2010-11 G `50` (1 row);
   2013-14 G `3` (2, unlabelled there), `18` (1), `25` (1); 2013-14 P `0` (3).
   2016-17 XS P `ag_p09a` = 999999 on one row (a sentinel served as a
   quantity). 2013-14 P `ag_p09a` is stored as a Stata byte with max 99 (other
   waves' P max 700-12,000) -- not investigated. Text leakage is negligible:
   unit text on a non-13 row 4 (2010-11 G); crop text on a non-generic code 150
   in 2010-11 G (136 on 10/16/26) and 17 in 2010-11 P. 2010-11 `visit` 2 rows
   exist (G 7,273) but no household or (household, plot, crop) appears in both
   visits, so the denominator double-counts nothing.

#### Recommendation

**Material in structure, small in volume -- so a NARROW step 2, not the broad
EPAR-style cleaner.** By volume the residue is small: 1.8% of harvest rows carry
an other-specify unit and 2.0% a generic-other crop (3.7% either, 4.6% at most
in any wave); no cell exceeds 6.1%. A general free-text cleaner over every
specify string would map 16-108 distinct strings per cell (k90 up to 48 for
units, 80 for perennial crops), mostly singletons, for well under 2% of rows --
not worth it; document that tail. But two pieces are concentrated enough to
justify work, each against our own strings:
(a) **units** -- restoring the two codes the IHS4/IHS5 G instrument dropped
(90 kg bag, basket/dengu) to their existing `harmonize_crop_unit` labels covers
886 rows with a handful of patterns and repairs a cross-wave break in `u`;
anything with a magnitude other than those (70 / 100 kg bags, litre containers)
stays `Other (Specify)` or goes to a counted kg-factor layer, never into
`Quantity`;
(b) **crops** -- cowpea is >= 919 rows and the majority of the seasonal "Other"
wherever text is held, but recovering it needs a new Preferred Label and, for
IHS4/IHS5, a G <-> Module D plot join, which is a design decision rather than a
cleaner; it should be its own issue. 2013-14 (no text held) and 2016-17 XS
seasonal crop names (Module D unreadable via `get_dataframe`) are unrecoverable
from what we hold (540 of 2,201 seasonal code-48 rows), which caps (b) at
about three quarters of them (1,634 rows with one unambiguous held text).
Everything else -- the long tails, perennial tree names, P's count-style units --
is right-sized as the `CONTENTS.org` note added with this phase.


### Phase 3 -- unit recovery implemented, cowpea drafted (2026-10-22)

**Approval.** @ligon approved both Phase 2 recommendations (via the
coordinator): implement the 90 kg bag / dengu unit recovery; draft, not file,
the cowpea issue. Branch `fix/856-malawi-harvest-unit-text-20261022`, merged
with development `6ffeb3caf`.

**Reuse decision (Phase 1 §5 revisited).** `_clean_freetext_unit` was not
reused. It title-cases the TEXT into a new label; the harvest side needs the
opposite, recoding to an EXISTING integer code so the label comes from
`harmonize_crop_unit` and is byte-identical to a 2010-11 code-3 / code-11 row.
New: `_UNIT_TEXT_RECODE` (exact normalised strings), `_normalise_unit_text`,
`_recover_unit_code`, `_unit_codes` (raises on a mis-named text column), and
`_recovered_counts` / `_sum_recovered` (`attrs['unit_text_recovered']`). New
kwargs: `_harvest_block(unit_text=)` and `_sale_block(unit_sold_text=)`. All of
these are in `Malawi/_/malawi.py`. `_crop_pieces` (`derivation_inputs`) passes
the same columns, via `_UNIT_TEXT_COLS` in `_CROP_SALE_SOURCES`, so the inputs
still join the served rows.

**Seam: the integer code, before the label map.** This class is a label
harmonisation of the survey's own answer (`harmonize_crop_unit`,
`_PERENNIAL_RECODE`), not a construction, so it gets no `Derivation` key.

**Sale side was in scope after measurement.** Modules I and Q lose codes 3 and
11 in 2016-17 / 2019-20 exactly as G does (2013-14 I lacks both, Q lacks 3).
The attach keys on `u`. A cold build of the harvest-only commit (`8b637dee4`)
detached **158 sales / 16,826,900 MWK** that both sides had written as 13 +
"90 KG BAG". The sale commit (`d9fbe9c51`) applies the same helper. The
harvest-only state is measured in `diff_base_harvest_only.json`.

**Strings** (`strings_856.json`, raw lines over G/P/I/Q).  The tables and
numbers in this section are the state BEFORE the `BASKET (BAMBOO)` ruling;
the final state is in Phase 3b below.

| | recovered to |
|---|---|
| `90 KG BAG`, `90KG BAG`, `90KG`, `90KG BAGS`, `90 KG BAGS`, `90KGS`, `90 KGS`, `90 KG`, `90KGBAG`, `90 KGBAG`, `BAGS OF 90 KGS` | code 3 (947 lines) |
| `DENGU`, `BASKET`, `BASKETS`, `BUSKET`, `BASKET (DENGU)`, `BASKET(DENGU)`, `BASKET ( DENGU)`, `BASKET ( DENGU )`, `BASKET (DENGUE)`, `BASKET DENGU`, `BASKET-DENGU`, `BASKET/ DENGU`, `DENGU/BASKET`, `DENGU ( BASKET )`, `DENGU( BASKET )`, `DENGU/BUSKET`, `BUSKET/DENGU` | code 11 (293 lines) |

Every whitelist entry occurs in the data at least once.

Deliberately left as `Other (Specify)` (count, all four modules):

| string | lines |
|---|---:|
| `BASKET (UNSPECIFIED SIZE)` | 61 |
| `BASKET (LARGE)` | 13 |
| `BASKET (BAMBOO)` | 13 |
| `LARGE BASKET` | 11 |
| `WEAVING BASKET` | 3 |
| `BASKET (SMALL)`, `DENGU(GOGODA)`, `SMALL BASKET` | 2 each |
| `MEDIUM WEAVING BASKET`, `1 90 KG`, `90 KG BAD`, `90KG BAY`, `LARGE DENGU`, `DENGU(LARGE)`, `LARGE BASKETS`, `MEDIAM BASKET`, the two `BASKET OF ...` strings | 1 each |

Also left: every other bag size. Two judgement calls: plain `BASKET`
-> dengu, and a bare weight with no container word (`90KG`, `90 KG`,
`90KGS`, `90 KGS`; 21 lines) -> `90 kg Bag`. The kilograms are the same
either way; only the label says "Bag".

**The served-table requirement ("identical except `u` on exactly the
recovered rows") does not hold, and cannot.** The attach keys on `u`.
1,004 rows differ:
- 885 change `u` (876 change only `u`; 9 also lose a `BASKET (BAMBOO)` sale);
- 115 rows with unchanged `u` gain a sale;
- 4 rows with unchanged `u` lose a sale.

**Both commits or neither.** Reverting the sale commit alone restores the
158-sale regression, so `8b637dee4` alone is a known-regressing state. Merge
or squash the branch whole.

**Real data: cold builds, development vs branch head.** Each build used its
own fresh private data root; the files are `diff_base_final.json` and
`summary_*.json`.

- **Rows:** 135,078 both sides; the per-wave counts are identical. 885 rows
  change `u` only. By module and target:
  - G 2016-17: 475 -> `90 kg Bag`, 36 -> `Basket (Dengu)`.
  - G 2019-20: 208 and 146.
  - P: 11 + 1 (2016-17) and 6 + 2 (2019-20).
  - 2010-11 and 2013-14: untouched.
- **No grain collision:** 0 ambiguous pairings, so no plot-crop had both a code-3 row and a recovered row.
- **Sales:** 115 perennial harvest rows coded 3 / 11 gain a sale on the
  unknown-condition wildcard. Module Q dropped those codes, so a Q sale written
  13 + text could not meet them before. 13 sales detach (347,000 MWK):

  | detached sales | why |
  |---:|---|
  | 9 | `BASKET (BAMBOO)` sale against a `DENGU` / `BASKET` harvest, same households (7 confirmed in the raw text, 2 panel rows not looked up because the API walks their ids) |
  | 1 | OX-CART harvest vs `busket` sale (correct: different units) |
  | 1 | `90 KG BAD` harvest vs `90 KG BAG` sale |
  | 2 | groundnut, both sides unrecovered, previously `contradiction-price-sale`; now unresolved because their (crop, `Other (Specify)`) price cell lost the recovered rows |

  Net **+2,421,000 MWK** attached (2016-17 +768,600; 2019-20 +1,652,400).
  The pinned counts in `tests/test_malawi_sale_basis.py` were updated with
  this provenance:
  - wildcard: 449 -> 467 and 1,041 -> 1,137;
  - contradiction-price-sale: 47 -> 46 and 85 -> 84;
  - unresolved: 88 -> 89 (2016-17), 2019-20 count unchanged at 138 but its
    MWK moved.
- **kg** (`harvest_kg_factors`): no row whose `u` stayed put changed its factor.
  - **Default:** 700 `90 kg Bag` rows go `none` -> `metric` 90 kg; the 185 baskets stay `none`.
  - **Shipped IHS5 tables** (`with_region`): 640 rows -> `shipped` 90 kg bag (median 99.9, min 36), 60 -> `metric` 90, 157 baskets -> `shipped` (median 8.39, range 6.36-16.15), 28 baskets stay `none`.
  - **Harvest kg, default:** 2016-17 +2.4%, 2019-20 +0.4%.
  - **Harvest kg, with the shipped tables:** +2.5% and +0.4%.

**Tests.** `tests/test_malawi_unit_text_856.py` has 31 tests: synthetic
frames through the real `_harvest_block`, `_sale_block` and
`assemble_crop_production`. They cover:
- a mapped string and a deliberately unmapped one;
- text on a non-13 code is inert;
- `Quantity`, `Quantity_sold` and `Value_sold` are unchanged;
- code-3 / code-11 rows are unchanged;
- `unit_text=None` reproduces the old output;
- a mis-named column raises;
- every whitelist entry is in normal form;
- a symmetric 13 + "90 KG BAG" pair still attaches.

Results:
- Against development source (`base-856`): 30 failed, 1 passed (the kwargs
  and whitelist do not exist there).
- On the branch: 31 passed.
- Existing Malawi files on the branch (`test_malawi_unit_cleanup`,
  `test_malawi_cassava_recode`, `test_malawi_sale_basis`,
  `test_malawi_shipped_factors`, `test_gh637_malawi_first_sites`, plus the
  new file): **168 passed, 0 failed, 0 skipped**, data-gated tests included.
  That is after the pin update; before it, 4 failed, exactly the pins above.

**Cache** (`hash_diff.json`, 1218's `hash_census.py` on both checkouts):
- Malawi L2-country: 26 of 26 move.
- Malawi L2-wave: 130 of 130 move (all five waves; 2004-05 too, because
  `malawi.py` is in every Malawi table's input hash).
- Build-transform fingerprints: 0 of 40 move.
- No other country moves (549 / 2,109 / 40 in total).

**Cowpea.** Drafted, not filed:
`slurm_logs/isa_backlog_2026-10-21/856-harvest-unit-text/COWPEA-ISSUE-DRAFT.md`.
New fact found while drafting: the food side already has the label
`Cowpea (Khobwe)`. That gives in-repo support for the 2016-17 variety text
`NKHOBWE`, but not for `NSEULA` / `MSEWULA`.

**Open / not determined:**
- RESOLVED in Phase 3b (@ligon: yes). Was: whether `BASKET (BAMBOO)` should also map to dengu. It would re-attach the 9
  sales; same-household evidence suggests it is the same basket. Left for a
  decision.
- The 2 panel detached rows' raw texts (ids are walked at the API).
- Crop code 49, which is dropped with a loud #869 warning in 2019-20 (66
  rows). Seen in both builds; not #856's to fix.

Publication note (Sue, 2026-10-22): `diff_base_harvest_only.json`,
`diff_base_final.json` and `moved_sale_texts.json` are ROW-LEVEL (household
and plot ids, sale values, respondents' free text) and are NOT committed --
the repository is public and World Bank microdata may not be redistributed.
`commands.sh` now writes them to a private `$P/856-private/`; the warning
strings in `summary_*.json` were reduced to class, wave and counts for the same
reason.  The aggregate numbers above are unchanged.

### Phase 3b -- rulings, review fixes and final numbers (2026-10-22)

**Decisions taken on the clean branch.** The branch is
`fix/856-malawi-unit-text-20261022`, whose parent is development `9a0ed5f3c`.
1. **`BASKET (BAMBOO)` -> `Basket (Dengu)`.** Ruling by @ligon. It is the
   only bamboo-basket spelling on a code-13 row in our G/P/I/Q files: 13
   Module I lines in 2016-17, none on the harvest side.
2. **`BASKET (UNSPECIFIED SIZE)` -> `Basket (Dengu)`.** Decision by Sue,
   for maintainer review at the PR. "Unspecified size" names no size, so it
   is the plain basket. Precedent: Malawi's `u` table keeps plain `Heap`
   apart from the sized heaps. It has 61 lines: Q 2016-17 59, P 2016-17 2.
3. **`90 KG BAD`, `90KG BAY` -> `90 kg Bag`.** These are unambiguous bag
   typos, mapped for consistency with the basket typos. `09KG BAG` stays
   left, because it could mean 9 kg.
4. **Scope comment corrected.** Only the dropped codes 3 and 11 are
   recovered. Code 2 (50 kg) and the other listed codes still exist, and
   texts restating them are deliberately left and sized as a follow-up in
   `CONTENTS.org`:

   | listed unit | lines | reviewer's narrower count |
   |---|---:|---:|
   | `50 KG` bag | 19 | 16 |
   | PAIL | 66 | 38 |
   | BUNCH | 11 | 10 |
   | PLATE | 8 | 8 |
   | PIECE | 11 | 7 |
   | BALE | 6 | 1 |
   | OX-CART | 4 | 1 |

   My counts are token matches; every string is in `strings_856.json`.
5. **`derivations.yml`.** The four `_oth` columns are now in `raw_variables`
   of all 8 sale-basis entries; `ag_p09b_oth` appears only where the entry
   already lists Module P. `.yml` is not a build input.

**Strings** (`strings_856.json`): code 3 recovers 949 raw lines (947 + the
2 typos); code 11 recovers 367 (293 + 13 bamboo + 61 unspecified). The table
of size- and weave-qualified baskets still left is in `CONTENTS.org`; the
largest are `BASKET (LARGE)` 13 and `LARGE BASKET` 11.

**Real data.** Cold build of this head against the same development build
(`base-856`, `6ffeb3caf`; the only commits between it and `9a0ed5f3c` touch
Mali and `nitrogen_kg`):

| | Phase 3 (pre-ruling) | bamboo only (`bf29f212a`) | **this head** |
|---|---:|---:|---:|
| rows whose `u` changed | 885 | 885 | **888** |
| sales gained | 115 | 115 | **168** |
| sales lost | 13 / 347,000 | 7 / 1,072,000 | **6 / 1,062,000** |
| net MWK attached | +2,421,000 | +1,696,000 | **+2,539,200** |
| harvest kg (default), 2016-17 / 2019-20 | +2.4% / +0.4% | same | same (2019-20 +90 kg) |

At this head:
- **`u` changes:** 2016-17: 486 -> 90 kg Bag, 39 -> Basket (Dengu), 14 of
  them perennial. 2019-20: 215 and 148, 8 perennial.
- **Rows differing from development:** 1,062. 888 change `u` only; 168 gain
  a sale; 6 lose one.
- **Sales gained:** 53 of the 168 are Module Q `BASKET (UNSPECIFIED SIZE)`
  sales meeting perennial harvest rows coded 11 (+833,200 MWK).
- **Sales lost:**

  | sales | MWK | why |
  |---:|---:|---|
  | 3 | 875,000 | 2016-17 tomato: sale `BASKET (BAMBOO)` vs harvest `BASKET (SMALL)` or no text |
  | 1 | 70,000 | `OX-CART` harvest vs `BUSKET` sale |
  | 1 | 80,000 | 2019-20 groundnut: harvest `09KG BAG` vs sale `90KG BAY` |
  | 1 | 37,000 | 2016-17 groundnut decided by price, now unresolved |

  The cotton row (`90 KG BAD` harvest vs `90 KG BAG` sale) re-attaches
  (+10,000).
- **kg:** no unchanged-`u` row's factor moves. The kg for the typo rows:
  default 2019-20 +90 kg; with the shipped tables 2019-20 +52 kg.

**Pinned constants** (`tests/test_malawi_sale_basis.py`):
- 2016-17 wildcard: 467 -> 520.
- `VALUE_SOLD_AFTER`: 2016-17 409,967,725; 2019-20 529,435,948.
- `UNRESOLVED` 2019-20: (138, 6,324,030) -> (137, 6,244,030).
- `UNRESOLVED_GROUNDS['contradiction_no_separation']` stays **271**, now
  with a provenance comment. The reviewer was right that `bf29f212a` gives
  272 (2016-17 70 -> 71). At this head 2019-20 also moves 105 -> 104: the
  `90KG BAY` groundnut sale was unresolved at `bf29f212a`, and now that it is
  recovered it has no harvest row of its unit, so it leaves the ladder. The
  sum is back to 271. This was measured by a per-wave replay
  (`ladder_tallies.py`) on both `9a0ed5f3c` and this head, not assumed.
- **Possible strengthening, not done here:** the test at line ~764 compares
  only the keys of `UNRESOLVED_GROUNDS`, so a stale count cannot fail it.
  Comparing the summed values would have caught the 272.

**Quantity screen** (Site Q, recomputed on both builds):
- 2016-17 flags 7 -> 6. A Cassava `Other (Specify)` row drops out because
  its cell's p90 goes 14 -> 24.
- 2016-17's Nkwani row with `Quantity` 2016 (a year in the quantity field)
  stays flagged, now in the `Basket (Dengu)` cell (ratio 144 -> 106). With
  the shipped tables it contributes 22,643 kg, 11.1% of the 2016-17
  shipped-path increase (204,408 kg).
- 2019-20 stays at 13 flags. A Cabbage `Other (Specify)` row's ratio goes
  153 -> 103. The reviewer reported 104; that was presumably measured at
  `bf29f212a`.

**Tests.**
- `test_malawi_unit_text_856.py`: 33 passed.
- The six Malawi harvest/sale/unit files plus `test_derivations.py`:
  **214 passed, 0 failed, 0 skipped**, data-gated tests included.

**Cache.** `hash_census.py` on `9a0ed5f3c` against this head:
- Malawi L2-country: 26 of 26 move.
- Malawi L2-wave: 130 of 130 move.
- Build-transform fingerprints: 0 of 40 move.
- No other country moves.

**Publication.** Row-level outputs (the diffs, the raw texts, logs, the
ladder decision frames) live under the private `856-private/` directory. The
committed `ladder_tallies_*.json` and `strings_856.json` are aggregate
counts. `build_and_measure.py` cuts saved warnings before any `i=` listing.
