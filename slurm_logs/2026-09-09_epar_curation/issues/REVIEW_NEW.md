# Review of NEW_01..NEW_12 issue drafts

Reviewer pass, 2026-09-09. Every `path:line` cited in the twelve drafts (ours
and EPAR's) was opened. EPAR clone commit
`abfbdc9301242527b7543606aad1ad6eed2e6530`
(`/global/scratch/fsa/fc_jevons/ligon/reference/epar/LSMS-Agricultural-Indicators-Code`),
per `reference/epar/NOTES.md`.

All twelve drafts were edited. NEW_01, NEW_02, NEW_03, NEW_04, NEW_05, NEW_06,
NEW_07, NEW_08, NEW_09, NEW_10 were rewritten or substantially corrected;
NEW_11 and NEW_12 were corrected in place.

## Global sweeps (all twelve, after editing)

| sweep | result |
|---|---|
| `grep -nP '[^\x00-\x7F]'` (ASCII only) | clean (NEW_01 had a section-sign, replaced with "section") |
| `grep -n 'NEW_[0-9]'` (cross-draft refs) | clean; all seven rewritten as prose ("the ... issue filed alongside this one") |
| hedges (`worth`, `genuine(ly)`, `confirming/confirms/confirmed`) | clean; 14 occurrences removed |
| `Labels:` vs `gh label list` | all labels used (`bug`, `cleanup`, `data-gap`, `diagnostics`, `enhancement`, `framework`) exist. NEW_02's `documentation` was dropped -- see below |
| issue numbers referenced (#820 #824 #841 #848 #849 #850 #439) | all present in `open_issues.tsv`; #167 correctly described as closed |
| title length | four titles were 125-141 chars; shortened to 103-122 |

## The systematic filename error (item 1)

`EPAR_UW_conversionfactors.do` is 493 lines and lives in
`Malawi IHS/Nonstandard Unit Conversion Factors/`. It is the BUILDER for
`Malawi_IHS_cf.dta` (hard-coded `input` blocks from the IHS3 food-conversion
PDF + the two IHS5 raw tables, then an interpolation ladder). It contains
none of the lines the drafts cited to it. Every cited line lives in
`Malawi IHS/Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do` (7,250 lines).
All were re-verified there and re-cited:

| draft | old citation | verified location | note |
|---|---|---|---|
| NEW_03 | `conversionfactors.do:1326` | `W1.do:1326` | merge key `region crop_code_long unit condition`, `keep(1 3)`; also at `:946`, `:2006`, and prescribed in the folder README's HOW TO USE |
| NEW_04 | `conversionfactors.do:1529` | `W1.do:1529` | `ren ag_r07 nb_ls_1yearago` |
| NEW_05 | `conversionfactors.do:1253-1268` | `W1.do:1253-1270` | "20 KG BAG" is `:1255`, not `:1253` (1253 is MG); pails `:1265-1266`; plates `:1268-1270` |
| NEW_05 | `conversionfactors.do:1240-1248` | `W1.do:1240-1248` | correct as listed; block opens at `:1230` |
| NEW_05 | `conversionfactors.do:1315` | `W1.do:1315` | `gen qmult = regexs(0) if(regexm(unit_os,"[0-9]+"))`; branches `:1316-1322` |
| NEW_12 | `conversionfactors.do:3339-3346` | `W1.do:3339-3346` | median ladder EA/TA/district/region/country, `obs_* >= 10` |
| NEW_12 | `conversionfactors.do:4525`, `:4529-4532`, `:4531`, `:4535` | `W1.do`, same numbers | all correct |
| NEW_12 | `conversionfactors.do:5959` | `W1.do:5959` | full `livestock_income` expression |

NEW_03's characterisation of what `EPAR_UW_conversionfactors.do` contains was
also wrong (it claimed a merge key). Replaced with an accurate description of
the file plus a citation to the folder README's DATA SOURCES / HOW TO USE.

## Per-file changelog

### NEW_01 (Ethiopia Crop_CF) -- rewritten

- **`:324` was misattributed.** That line is about
  `ET_local_area_unit_conversion.dta` (the LAND-area file), not `Crop_CF`. W5
  reads `Crop_CF_Wave5.dta` from the WB temp data at `:764`. Removed.
- **The draft contradicted itself**: it said `:324` "explains why our 2021-22
  wave has no sidecar" while its own bullet lists
  `2021-22/Data/crop_cf_wave5.dta.dvc`. Removed.
- **The de-dup claim was false.** See item 2 below. Replaced with the live
  citation and the twin in W4.
- **New measured fact added**: four sidecars, three distinct md5s;
  `Crop_CF_Wave4` (2018-19) and `crop_cf_wave5` (2021-22) are one
  byte-identical blob (`299c2700fe8d96822d8e803b202fa431`). All three blobs
  already in the local DVC cache -- the draft claimed only two were.
- `data_scheme.yml:120` -> `:120-121` (the quote spans two lines).
- `transformations.py:1920` verified correct; widened to `:1920-1948` and
  `KG_FACTOR_LAYERS` at `:1822` added.
- Added a "must not" about EPAR's post-`fillin` derived rows (`W5.do:794-797`)
  and a pointer to the `CONTENTS.org:1389-1403` re-cite.

### NEW_02 (Ethiopia local-area units) -- rewritten; premise was stale

- **The documentation half is already fixed.** Commit `ef633b12` (2026-09-09)
  added `Ethiopia/_/CONTENTS.org:1051-1062`, a "Correction" paragraph that
  already states the file is held, names the three distinct blobs, quotes
  `W5.do:324`, and ends "GH #TBD ... to wire it into `plot_features.Area`".
  Title, framing and the `documentation` label were removed accordingly; the
  issue is now the wiring, and "replace `GH #TBD` at `:1061`" is a fix step.
- `CONTENTS.org:1036-1039` -> `:1042-1049` (the heading is at 1042).
- **Blob count corrected** (item 3): four sidecars, THREE distinct md5s
  (2015-16 and 2018-19 share `f556b66e242e3c6d3f4584b686de2bcb`; 2011-12
  `ad8ca021...`; 2013-14 `1680c547...`), and all three are cached -- not "two
  of the four".
- The awkward ">1000 ha-style plausibility screens" bullet was rewritten to
  "Never resolve an implausible converted `Area` by clipping it ... count and
  name the rows".
- Added `W5.do:325-341` (the geography-key padding a loader will also need).

### NEW_03 (Malawi IHS5 crop CFs + `condition`) -- rewritten

- Filename error fixed (above).
- **"CONTENTS.org omits both crop-side files entirely" is false.**
  `Malawi/_/CONTENTS.org:659-662` already lists both, marked "2026-09-09:
  held, unwired; only the food file is read". Corrected, and the fix step
  changed from "add them" to "update the note when the loader lands".
- **"the dead parameter is the new fact" is false.**
  `Malawi/_/CONTENTS.org:201-207` already says "`ag_g13c` is passed to
  `_harvest_block` as `condition=` but the parameter is *accepted and never
  used*".
- **"every current caller passes `condition=None` today" is false.** All four
  wave scripts pass `condition='ag_g13c'`:
  `2010-11/_/crop_production.py:36`, `2013-14/:36`, `2016-17/:46`,
  `2019-20/:41`. The framing was inverted: the waves feed a real column into a
  parameter the module drops.
- `grep -nw condition malawi.py` returns `:915` only; the draft's `:1266` /
  `:2493` come from a substring grep and are "unconditionally" (draft was
  right about that, wording tightened).
- Added the README's borrow statement verbatim, and the
  `EPAR_UW_conversionfactors.do` ladder (`:399-432`, `:469-487`) and
  `shunsh_na` allowlist (`:436-444`) as `imputed`, not to be stored.

### NEW_04 (Malawi `ag_r07`) -- rewritten; scope was wrong (item 6)

- **The readme interpretation is wrong.** `Wave 1/readme.md:97` is "Known
  Issues: None" and carries no such text. `Wave 3/readme.md:97` and
  `Wave 4/README.md:98` do carry "Survey question on how many animals owned
  by household at start of the season are not available. Livestock holdings
  are valued using observed sales prices" -- but that is about a
  START-OF-SEASON question and about VALUATION, not about `ag_r07`.
- **`ag_r07` is read by EPAR in all four waves**: `W1.do:1529`, `W2.do:2481`,
  `W3.do:1555`, `W4.do:3012`.
- **`ag_r07` is present in all four of our waves' source files.** Read
  directly from the cached DVC blobs (`pd.read_stata` on
  `~/.local/share/lsms_library/dvc-cache/{md5}`): present in 2010-11
  (Full_Sample and Panel), 2013-14, 2016-17 (XS and Panel), 2019-20 (XS and
  Panel). Stata label: "How many [LIVESTOCK] did your household own exactly
  12 months ago?"
- So the draft's "Do NOT backfill for 2016-17/2019-20 -- the column genuinely
  does not exist" was false and is removed; scope is all four waves. Title
  changed.
- `malawi.py:1734` -> `:1734-1736` (signature spans three lines);
  `:1730-1732` -> `:1728-1732`.
- Replaced the removed "must not" with two real ones (do not derive a flow
  from `ag_r02 - ag_r07` at build time; count nulls before wiring).

### NEW_05 (Malawi free-text recovery) -- rewritten

- **"every caller is a `food_acquired.py`" is incomplete.** There are two call
  sites: `2010-11/_/food_acquired.py:115` and `malawi.py:185` inside
  `handling_unusual_units` (`:156`), which `2016-17/_/food_acquired.py:88` and
  `2019-20/_/food_acquired.py:72` call. Both are food-side, so the conclusion
  holds; the enumeration was wrong.
- Line-number fixes per the table above.
- **New finding**: the comment at `malawi.py:1114-1116` says the rows dropped
  at `:1117` are "logged by row count". There is no print, warning or counter
  anywhere in `assemble_crop_production` (`:1033`). The comment describes a
  log that does not exist; that is now part of the issue.
- Added a "must not" about EPAR's quantity rescaling (`:1256`, `:1319`,
  `:1322`), which would fold a recovered magnitude into reported `Quantity`.

### NEW_06 (Tanzania Quantity plausibility) -- corrected

- `transformations.py:1801` is `SURVEY_MEDIAN_MIN_REPORTS`, not
  `_screen_reported_factors`. That function is at `:1714`; `KG_FACTOR_MAX` at
  `:1690`; the "counted under `reported_implausible`" text at `:1926-1931`.
- `tanzania.py:1046-1054` -> `:1046-1051`, and noted it lives in
  `plot_features_for_wave` (`:983`), not in `crop_production_for_wave`.
- **`W5.do:254` was read backwards.** `:254` is COMMENTED OUT and is the
  unit-error reading; the rule EPAR applies is `:255`, a decimal rescale
  (`/10000`). Rewritten as "two competing readings, one applied".
- All parquet numbers re-measured and correct: 11,125 rows; max 7,500,000
  (`u='kg'`, Coconuts, `i=9640-001-99`, plot 1); mean 2,218.41; std 85,732.98;
  Timber 4,000,000; Other Fruits 2,400,000; 2,708 distinct `i`; EPAR's
  `0015-001-001` absent. Added the fourth outlier (Ripe Bananas 1,505,400).

### NEW_07 (Tanzania plot_inputs roster) -- corrected (item 7)

- `tanzania.py:1352` correct. `2020-21/_/plot_inputs.py:21` is WRONG -- the
  `assert df.index.is_unique` is at `:23`; `:22` is the call. Fixed.
- **No build-time merge confirmed**: `grep 'merge|\.join|concat|plot_features|crop_production'`
  over `tanzania.py:1352-1549` yields one hit, the block's own `pd.concat` at
  `:1491`. Cited.
- `W5.do:2473` opened: `merge 1:1 hhid plot_id season using ...
  input_quantities.dta, nogen keep(1 3)`, with BOTH inline comments ("11 plots
  have expenses but don't show up in the all_plots roster" and "MGM 5.18.2024:
  80 not matched, 10,527 matched"). `keep(1 3)` confirmed.
  **The draft asserted the 80 are dropped input rows; EPAR does not say which
  side they fall on.** Reworded to state the mechanics and the ambiguity;
  title softened from "EPAR loses 80 of 10,607".
- **The grep claim was wrong as stated.** Plain `grep -c` for `rent` returns 26
  (substring hits in "different", "current"...). With `-w`, all six terms are
  zero in `Tanzania/_/CONTENTS.org`; `open_issues.tsv` has one `plot_inputs`
  hit (#569, index_info registration). Also the file is 1,841 lines, not
  1,833. All corrected.

### NEW_08 (Nigeria per-row KgFactor) -- corrected

- `transformations.py:1920-1932` -> `:1923-1933`, with the exact text ("The
  row's own ``KgFactor`` column -- what the instrument wrote down").
- `W4.do:982` and `:595-597` verified verbatim. Added that `:982` fires
  immediately after the lookup merge at `:980`, which is what makes it a
  precedence statement.
- **`W4.do:577-585` is three of FOUR `*_conv` columns.** The coalesce runs
  `:575-592` and includes `sa3iq6d_conv` at `:592`. Corrected, and the "must
  not" about averaging sections updated.
- Our-side citations all verified (`crop_production.py:1-28` docstring, both
  greps zero, `grep -c KgFactor` zero in both files).
- Added a `data_scheme.yml` declaration step and a #824 sale-unit "must not".

### NEW_09 (Nigeria zone area factors) -- rewritten; the hedge removed and the
number corrected (item 4)

- `W4.do:391-397` opened. **The cited range is wrong on both counts.** The BID
  factor table is a comment block at `:364-372`; the "ALT observed from the
  data" alternative at `:374-382`; the standard-unit conversions applied at
  `:385-388`; the zone-specific ones applied at `:390-409` (heaps `:390-395`,
  ridges `:397-402`, stands `:404-409`). `:391-397` straddles two of those.
  `:388-390` for "plots/acres/sqm" is likewise wrong (`:385-388`).
- **"~23x" is one cell, not the disagreement.** Computed over all 18 cells,
  observed/BID ranges 0.16x to 71.6x, median 4.9x. 23.4x is heaps zone 1.
  Per-unit: heaps 0.16-71.6, ridges 0.35-2.2 except zone 6 at 50, stands
  3.8-69.2. Draft rewritten around the measured range.
- **"the disagreement ... is EPAR's own data-quality caveat" is false.** EPAR
  records both grids without commenting on the gap, and applies the BID one.
  The "ALT" label is a coder's initials (cf. `/*ALT 02.23.23*/` at `:411`),
  not a caveat.
- The hedge ("was not individually re-opened this pass") is gone.
- `W1.do:309-357` verified: same BID numbers, saved as a `landcf.dta`, and
  **no observed grid** -- the alternative appears only in the later wave file.
- `Nigeria/_/CONTENTS.org:731-743` -> `:738-750` (heading at 738). Quote and
  the 86.6% / per-wave shares verified verbatim.
- **New delta added**: our CONTENTS lists "plots" among the non-standard
  units, while EPAR converts it with an all-zone x0.0667 (`W4.do:386`). That
  one unit has no competing factor set and can be decided separately.

### NEW_10 (Uganda 99999 sentinel) -- corrected (item 8)

- `W1.do:483` and `:558` verified verbatim, including the inline count "1410
  quantities (99999) changed to missing". EPAR's UNPS W1 is 2009-10 (`:4`),
  matching our wave; citation added.
- `CONTENTS.org:2098-2102` -> `:2105-2109` (heading at 2105).
- The `99999` grep hits three files, not one: `CONTENTS.org` plus
  `fct_uganda.csv` (43 lines) and `fct_usda.csv` (7). Conclusion unchanged.
- Added `:556-557` (where `quantity_harv` is coalesced) so `:558` reads in
  context, and replaced the vague "worth carrying over" note with a concrete
  "do not adopt the narrow rule alone".

### NEW_11 (plot_cultivation schema) -- corrected (item 9)

- **All twelve "already open" file:line citations verified correct.**
  Uganda 2015-16 `plot_labor.py:25-26` / `plot_inputs.py:22-23`; Ethiopia
  2018-19 `plot_inputs.py:28`, `plot_labor.py:21`, `plot_features.py:20`;
  Tanzania 2019-20 `plot_inputs.py:21`, `plot_features.py:17`,
  `plot_labor.py:24`; 2020-21 `plot_inputs.py:17`, `plot_labor.py:21`,
  `plot_features.py:18`; and `2020-21/_/plot_labor.py:9-10` for the lowercase
  filename note.
- All 21 raw decision-maker variable names re-grepped across `lsms_library/`:
  zero hits each. `W5.do:298-302` confirms 2020-21's `ag3a_08b_*` spelling.
- EPAR side verified: Uganda `W5.do:413-424` (save at `:424`) with the column
  list confirmed against `Uganda_NPS_W5_person_ids.dta` built at `:271-279`;
  `:433-439` collapse (exact form `collapse (mean) female (firstnm)
  dm1_gender, by(hhid parcel_id plot_id season)`); Tanzania `W3.do:261`/`:263`;
  Ethiopia `W4.do:515` (widened to `:513-515`).
- `data_info.yml:227-232` -> the comment is `:228-232` (`:227` is the
  `plot_features:` key). The draft's self-congratulatory parenthetical about
  the red-team's ":227-231" was removed.
- **`data_info.yml:578` does not declare an index.** It is prose in the
  KgFactor note. More importantly, only **Uganda and GhanaSPS** declare
  `season` as a `crop_production` index level; Ethiopia declares
  `(t, i, plot_id, j, u)` and Tanzania `(t, i, plot_id, j)`. Since NEW_11
  proposes to build for Uganda, Ethiopia and Tanzania, the "reconcile with
  crop_production's existing season level" step was rewritten to name the two
  countries where the level does not exist.
- `DESIGN_erhs_plot_features_2026-05-20.org:54-57` and
  `ethiopia.py:524/747/849/1000/1181` all verified correct.

### NEW_12 (transforms umbrella) -- corrected (item 10)

- Filename errors fixed (above).
- `data_info.yml:419-436` -> `:419-440`. The ValuePerAnimal note's two-variant
  text is `:428-435`; all three column names (`s11iq3`, `ag_r04`, `a6aq6`,
  `a6aq14b`/`s6aq14b`) and the reservation/realised distinction verified.
- `W5.do:2538-2560` -> the HDDS recode is `:2540-2552`; `:2537` is the `use`
  line and the banner is `:2532-2534`. **New finding**: EPAR's recode is not a
  partition -- itemcode 704 appears in both FRUITS (`:2543`) and SWEETS
  (`:2550`), 1003 in both OILS AND FATS (`:2549`) and SPICES (`:2551`), so
  first-match wins and the grouping depends on statement order.
- `W5.do:2614` is the `use` line; the Tanzania rcsi formula is `:2617`, and it
  is an **eight-term** variant with different weights from Malawi's five-term
  one. The draft presented the Malawi formula as "the" formula; corrected, and
  a "must not" added.
- The Uganda W5 rcsi absence is now cited on both sides:
  `EPAR_UW_Uganda_UNPS_W5.do:2868` and that wave's `readme.md:284`.
- `livestock_income`: `:5959` is broader than "number_sold + number_slaughtered
  times a laddered price" (it nets purchases and adds milk/eggs/manure less
  costs). Stated.
- **`fertiliser`/`fertilizer.*rate` = 0 was misleading.** `grep -ic fertili`
  on `transformations.py` returns 24, all `nitrogen_kg`'s product-to-N-share
  machinery (`:2421-2462`, `:2469-2531`). No application rate exists, which is
  the true claim; the numerator half already does.
- `food_coping` is declared by six countries (Malawi, Ethiopia, Tanzania,
  Nigeria, Mali, Burkina Faso); Malawi's five strategies at
  `Malawi/_/data_scheme.yml:156-168` map onto EPAR's five-term formula. Cited.

## Citation verdicts

Legend: OK = opened, text supports the sentence. FIX = corrected in place.
WRONG = claim removed or reversed.

| draft | citation | verdict |
|---|---|---|
| 01 | `Ethiopia/_/data_scheme.yml:113-134` | OK |
| 01 | `data_scheme.yml:120` (quote) | FIX -> `:120-121` |
| 01 | `grep -rni crop_cf` zero | OK |
| 01 | four `Crop_CF` sidecars; "Wave3 + wave5 cached" | FIX -> 4 sidecars / 3 md5s / all cached; W4 == W5 blob |
| 01 | `transformations.py:1920` | OK (widened `:1920-1948`, `:1822` added) |
| 01 | `W5.do:324` as the Crop_CF reason | WRONG (it is the land-area file) -> removed |
| 01 | `W5.do:345-353` commented out | OK, but "EPAR does not apply it" WRONG -> live at `:800-802`, twin `W4.do:744` |
| 02 | `CONTENTS.org:1036-1039` | FIX -> `:1042-1049`; premise stale, see `:1051-1062` |
| 02 | "two of four blobs cached" | WRONG -> three distinct md5s, all cached |
| 02 | `W5.do:324` | OK |
| 03 | `2019-20/_/food_acquired.py:17` | OK |
| 03 | `CONTENTS.org:653-660` "omits both" | WRONG -> `:652-664`, both listed at `:659-662` |
| 03 | `malawi.py:915` signature | OK (widened `:915-917`) |
| 03 | "condition unused in module" | OK; "the new fact" WRONG (`CONTENTS.org:201-207`) |
| 03 | "every caller passes `condition=None`" | WRONG -> all four pass `'ag_g13c'` |
| 03 | `Nonstandard.../README.md` cross-wave borrow | OK |
| 03 | `conversionfactors.do:1326` | FIX -> `W1.do:946/1326/2006` |
| 04 | `malawi.py:1734`, `:1730-1732` | OK (widened) |
| 04 | `grep ag_r07` zero in our tree | OK |
| 04 | `conversionfactors.do:1529` | FIX -> `W1.do:1529`; plus W2/W3/W4 |
| 04 | readmes "absent by survey design in W3/W4" | WRONG -> about start-of-season + valuation; `ag_r07` present in all four |
| 04 | "column does not exist in 2016-17/2019-20" | WRONG -> present in all four (read from cached blobs) |
| 05 | `food_acquired.py:24`, `:115` | OK |
| 05 | "every caller is a food_acquired.py" | FIX -> two sites, `malawi.py:185` is the other |
| 05 | `malawi.py:903`, `:1117` | OK; "silently" OK, comment at `:1114-1116` claims a log that does not exist |
| 05 | `conversionfactors.do:1253-1268` | FIX -> `W1.do:1253-1270`, pails `:1265-1266`, plates `:1268-1270` |
| 05 | `conversionfactors.do:1240-1248`, `:1315` | FIX (file only); line numbers OK |
| 06 | `tanzania.py:1163` | OK |
| 06 | `tanzania.py:1046-1054` | FIX -> `:1046-1051`, in `plot_features_for_wave` (`:983`) |
| 06 | `transformations.py:1801` `_screen_reported_factors` | WRONG -> `:1714` (`:1801` is `SURVEY_MEDIAN_MIN_REPORTS`) |
| 06 | parquet: 11,125 / 7,500,000 / 2,218.41 / 85,732.98 / 2,708 | OK, all re-measured |
| 06 | `W5.do:578-579` | OK |
| 06 | `W5.do:254` unit-error theory | FIX -> `:254` is commented out; `:255` is the applied decimal rule |
| 07 | `tanzania.py:1352` | OK |
| 07 | `2020-21/_/plot_inputs.py:21` | WRONG -> `:23` (`:22` is the call) |
| 07 | "no merge inside the function" | OK (`grep` over `:1352-1549`, one `pd.concat` at `:1491`) |
| 07 | `W5.do:2473` `keep(1 3)` | OK; "drops 80 input rows" NOT established -- side unstated by EPAR |
| 07 | CONTENTS grep "zero hits for every term" | FIX -> true only with `-w`; file is 1,841 lines; `open_issues.tsv` has #569 |
| 08 | `Nigeria/_/crop_production.py:1-25` | OK (widened `:1-28`) |
| 08 | both greps zero, `grep -c KgFactor` zero | OK |
| 08 | `transformations.py:1920-1932` | FIX -> `:1923-1933` |
| 08 | `W4.do:982` | OK (context `:980` added) |
| 08 | `W4.do:577-585` "three columns" | FIX -> four, `:575-592` |
| 08 | `W4.do:595-597` | OK |
| 09 | `Nigeria/_/CONTENTS.org:731-743` | FIX -> `:738-750` |
| 09 | `W4.do:391-397` BID table | WRONG -> table `:364-372`, alternative `:374-382`, applied `:390-409` |
| 09 | `W4.do:388-390` standard units | WRONG -> `:385-388` |
| 09 | "~23x disagreement", "EPAR's own caveat" | WRONG -> 0.16x-71.6x, median 4.9x; EPAR does not comment |
| 09 | `W1.do:309-357` twin | OK; adds that W1 has no observed grid |
| 10 | `Uganda/_/CONTENTS.org:2098-2102` | FIX -> `:2105-2109` |
| 10 | `grep 99999` "only fct_uganda.csv" | FIX -> three files |
| 10 | `W1.do:483`, `:558` | OK |
| 11 | twelve wave-script file:lines | OK, all twelve |
| 11 | 21 raw DM variables, zero hits | OK |
| 11 | `data_info.yml:227-232` | FIX -> comment is `:228-232` |
| 11 | `data_info.yml:578` "index level" | FIX -> prose, not a declaration; only Uganda + GhanaSPS declare `season` |
| 11 | `DESIGN_erhs...:54-57` | OK |
| 11 | `ethiopia.py:524/747/849/1000/1181` | OK |
| 11 | `W5.do:413-424`, `:433-439` | OK (collapse form and `person_ids` provenance added) |
| 11 | `W3.do:261`, `:263`; `W4.do:515` | OK |
| 12 | `data_info.yml:419-436` | FIX -> `:419-440` |
| 12 | `conversionfactors.do:3339-3346`, `:4525`, `:4529-4532`, `:5959` | FIX (file only); line numbers OK |
| 12 | rcsi formula "the" formula | FIX -> Malawi's five-term; Tanzania `W5.do:2617` is eight-term |
| 12 | `W5.do:2538-2560`, banner `:2537` | FIX -> recode `:2540-2552`, banner `:2532-2534`, `use` `:2537` |
| 12 | `W5.do:2614` rcsi | FIX -> `:2614` is the `use`; formula `:2617` |
| 12 | Uganda W5 rcsi absence "per its own readme" | OK (cited: `W5.do:2868`, `readme.md:284`) |
| 12 | `fertiliser`/`fertilizer.*rate` = 0 | FIX -> `grep -ic fertili` = 24 (`nitrogen_kg`); no rate |
| 12 | other `grep -c` zeros | OK |

## Not verified

- **Which side EPAR's 80 unmatched Tanzania rows fall on** (`W5.do:2473`).
  The do-file states a count without a direction, and its two comments are
  from different vintages (11 vs 80). NEW_07 now states the mechanics and the
  ambiguity rather than asserting a loss.
- **Whether `ag_r07` is populated** (as opposed to present) in each Malawi
  wave. Column presence was read from the cached blobs; null rates were not
  measured. NEW_04 makes that a precondition rather than a claim.
- **The contents of the `Crop_CF_*` tables themselves.** The duplicate row
  (crop 74 / unit 62, 4.34 vs 6.125) rests on EPAR's comments at `W5.do:349`
  and `:801` plus its `drop` at `:802` / `W4.do:744`; our own blobs were not
  opened.

## One item for the maintainer, outside these drafts

`Ethiopia/_/CONTENTS.org:1399` says "EPAR's do-file resolves it by keeping
4.34 and dropping 6.125" and cites `EPAR_UW_Ethiopia_ESS_W5.do:349`. **The
sentence is correct; only the citation is wrong.** `:349` is inside the
commented-out block. The de-dup that runs is `W5.do:800-802`, documented at
`:801`, with the twin at `W4.do:744`. The amendment is a re-cite, not a
retraction.

## Late pass (after the per-file edits)

- `grep -niE 'workplan|red-team|redteam|refuter'` -- three survivors of the
  same class as the `NEW_xx` cross-references (internal artefacts a GitHub
  reader cannot open): `NEW_11:104` and `NEW_12:107`, `:136`. All rewritten as
  plain prose. In-repo `DESIGN_*.org` paths were left, since those are
  readable from the repo.
- NEW_08's new claim "as Uganda does after #849" was verified and given its
  citation: `Uganda/_/data_scheme.yml:200-202`, `KgFactor: {type: float,
  optional: true}`.
- NEW_09's "the often-quoted ~23x" reworded -- nobody quotes it outside this
  survey's notes; the sentence now names the single cell and says plainly that
  it is not the disagreement.
