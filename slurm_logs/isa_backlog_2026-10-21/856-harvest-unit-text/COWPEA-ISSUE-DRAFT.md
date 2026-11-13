Malawi crop_production: seasonal crop "Other" is mostly cowpea, which harmonize_crop has no label for

<!-- DRAFT, not filed (2026-10-22).  Follow-up to #856; written by the #856 lane. -->

## Summary

Malawi's seasonal harvest module (Module G) has no cowpea code. Respondents who grew cowpea were coded 48 `OTHER (SPECIFY)`, which `harmonize_crop` serves as `crop = 'Other'`. Measured on our own files for #856, **at least 919 seasonal harvest rows say cowpea in plain text**: 56% of the 1,634 code-48 rows that carry one unambiguous text. That makes cowpea the single largest crop hidden inside `Other`. It cannot be found by `crop`, cannot be joined to `food_acquired`'s cowpea items, and cannot get a crop-specific kg factor.

## Evidence (all from our extracts; evidence in `slurm_logs/isa_backlog_2026-10-21/856-harvest-residue/` and the ledger `.coder/ledger/856-malawi-harvest-residue.md`, Phase 2)

| wave / half | code-48 rows | where the crop name is | rows naming it | literal `COW PEA` |
|---|---:|---|---:|---:|
| 2010-11 Full_Sample | 328 | Module G `ag_g0d_os` (on the row itself) | 326 | 157 |
| 2013-14 | 167 | **no specify text held** (only an `occ` "O/S Merge" key) | 0 | -- |
| 2016-17 Cross_Sectional | 371 | Module D `ag_d20_oth`, but the file **does not read** (see below) | -- | -- |
| 2016-17 Panel | 111 | Module D `ag_d20_oth` | 111 | 65 |
| 2019-20 Cross_Sectional | 990 | Module D `ag_d20_oth` / `ag_d20b_oth` | 963 unambiguous (990 with any) | 584 |
| 2019-20 Panel | 234 | Module D `ag_d20_oth` | 234 | 113 |

- **Module G has no crop-name specify column after 2013-14.** The IHS4/IHS5 G files carry only a variety-specify field (`ag_g0e_1_oth` / `ag_g0e_oth`), which is near-empty on code-48 rows (1, 7, 0, 0 rows). The name is in **Module D**: its plot-level crop list (`ag_d20a..e`, the same crop codes) and `ag_d20_oth` / `ag_d20b_oth` "Other Crop". Joined on (household, `gardenid`, `plotid`), it names every code-48 G row in every half it can read. 27 2019-20 cross-sectional plots carry two "Other Crop" texts, so they are ambiguous.
- **540 of the 2,201 seasonal code-48 rows cannot be named from what we hold:**
  - 167 in 2013-14 (no text at all);
  - 371 in 2016-17 cross-sectional, because `Cross_Sectional/ag_mod_d.dta` does not read through `get_dataframe`, with or without `encoding='latin-1'` ("has a Stata header but cannot be decoded"). This is the latin-1 bad byte `Malawi/_/CONTENTS.org` records under plot_features. That wave's `plot_features` script reads the file column-restricted through pyreadstat, which may or may not reach `ag_d20_oth`; untested.
  - 2 in 2010-11 with an empty text.
- **Local names are not translated here.** 2010-11 also has `NSEULA` (77) and `MSEWULA` (8), and 2016-17 Panel's variety text has `NKHOBWE` (5). None of them is in the 919.
  - **`NKHOBWE`** has in-repo support: the survey's own food item label is `Cowpea (khobwe)`.
  - **`NSEULA` / `MSEWULA`** have none. They need a citable source before anyone maps them.
- **The rest of the tail is not cowpea.** It includes `MUCUNA`, `NKHUNGUDZU`, `CUCUMBER`, `SESAME` / `CHITOWE` and `LABLAB`. It also includes crops that already have codes: `CASSAVA` (perennial code), `SORGHUM` (32), `PIGEON PEAS` (36) and `TOBBACO` (5-10).
- **Perennial "Other" (codes 18 / 21) is not cowpea.** It is mostly sugar cane and trees (acacia, bluegum). Out of scope.

## What a fix needs

1. **A cowpea Preferred Label** in `harmonize_crop` (`Malawi/_/categorical_mapping.org`). This is a new row keyed on a synthetic code, since the instrument has none. The food side already has the label `Cowpea (Khobwe)` (`categorical_mapping.org`, food item table). Reusing it is the "shared Preferred Label" rule `harmonize_crop`'s header states, and it lets `crop_production.crop` join `food_acquired`. Adding a row to a level's `.org` is a cache event for every Malawi table.
2. **Recovery from our own strings only:**
   - **2010-11:** read `ag_g0d_os` on the G row.
   - **2016-17 Panel and 2019-20:** a G-to-D join on (household, `gardenid`, `plotid`), used only where the plot's "Other Crop" text is unambiguous (exactly one).
   - **Matching:** exact normalised strings from a measured list, in the style of `malawi._UNIT_TEXT_RECODE` (#856).
3. **The same recovery on the sale side (Module I), or a measured argument why not.** The sale attach keys on the crop code, so a harvest row recovered to cowpea would detach a sale still coded 48. #856 measured this exact effect for units: 158 sales detached when only one side was recovered.
4. **Counted, not silent.** Report per-wave rows recovered, rows left `Other`, and the 540 that cannot be named, on `attrs` and in `CONTENTS.org`.
5. **A decision on the local names**, made only with a source that says what each one is (the instrument, a crop glossary), never by assumption:
   - **`NKHOBWE`:** the food label `Cowpea (khobwe)` is one such source.
   - **`NSEULA`, `MSEWULA`:** none found yet.
6. **Possibly:** a way to read `Cross_Sectional/ag_mod_d.dta`'s `ag_d20_oth` through a sanctioned reader, which would recover up to 371 more rows.

## What it must NOT do

- Do not copy EPAR's crop-name recode list (`EPAR_UW_Malawi_IHS_W1.do:1230-1248`); free-text spellings differ between extracts, and the strings above are ours.
- Do not map the long tail, or crops that already have a code, as a side effect. A text naming sorghum on a code-48 row is a different decision from naming cowpea. Leave it `Other` unless it is decided separately.
- Do not guess for 2013-14 or the unreadable 2016-17 cross-sectional rows. They stay `Other`, and they are counted.
- Do not translate local-language names without a citable source.
- Do not touch `Quantity`.

Refs #856 (measurement and the unit half). Related: #869 (perennial recode), #854 (conversion tables keyed on crop variety).
