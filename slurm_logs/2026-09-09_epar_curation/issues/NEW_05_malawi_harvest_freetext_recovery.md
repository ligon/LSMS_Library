Title: Malawi: no free-text unit/crop recovery on the harvest side, and the unmapped-crop drop is not counted
Labels: enhancement, data-gap

Malawi's food-acquisition pipeline recovers "other (specify)" free-text units
into usable labels; the harvest side of the same survey (Module G / Module P,
read via `_harvest_block` / `_sale_block`) does no such recovery, and drops
rows whose crop code did not map without counting them.

## Evidence

**Ours**
- `_clean_freetext_unit` has two call sites, both food-side:
  `Malawi/2010-11/_/food_acquired.py:115`
  (`df[detail_col] = df[detail_col].map(_clean_freetext_unit)`, imported at
  `:24`), and `Malawi/_/malawi.py:185` inside `handling_unusual_units`
  (`:156`), which only `2016-17/_/food_acquired.py:88` and
  `2019-20/_/food_acquired.py:72` call. Nothing in `_harvest_block`,
  `_sale_block`, or any harvest-side wave script calls it.
- Crop-name free text: `malawi.py:903` defines `_crop_codes`; rows whose code
  did not map are dropped at `malawi.py:1117`
  (`harv = harv[harv['crop'].notna()]`) inside
  `assemble_crop_production` (`:1033`), rather than recovered from the
  "other (specify)" text.
- The comment immediately above that line (`:1114-1116`) says the dropped rows
  are "logged by row count". They are not: there is no print, warning or
  counter anywhere in `assemble_crop_production`. The comment describes a log
  that does not exist.
- The row-count cost of both gaps is unmeasured. This issue is filed to make
  them visible before someone measures.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/Malawi IHS/Malawi IHS Wave 1/`)
- `EPAR_UW_Malawi_IHS_W1.do:1253-1270` -- free-text UNIT recovery on the
  harvest side: bag sizes recoded to kg with the matching quantity multiplier
  (`replace unit=1 if strmatch(unit_os, "20 KG BAG")` at `:1255`, then
  `replace quantity_harvested=quantity_harvested*20 ...` at `:1256`; the same
  pair for 25/50/90/100 kg bags and for milligrams at `:1253-1254`), pails at
  `:1265-1266` and plates at `:1268-1270`.
- `:1315-1322` -- a glued-magnitude regex that recovers a quantity multiplier
  out of free text: `gen qmult = regexs(0) if(regexm(unit_os, "[0-9]+"))`
  (`:1315`), then volume (`:1316-1319`) and weight (`:1320-1322`) branches
  that rescale `quantity_harvested` by `qmult/5` and `qmult/50`.
- `:1240-1248` -- free-text CROP-NAME recovery: LEMONS (`:1240`), GROUNDNUTS
  (`:1241`), African/Buffalo/Kidney beans + KALONGONDA (`:1242`),
  COWPEAS/NSEULA (`:1243`), CHINESE CABBAGE (`:1244`), CUCUMBER (`:1245`),
  KABUTHU (`:1246`), KALISERE (`:1247`), KAWALA/NKHUNGUDZU (`:1248`) -- the
  tail of a block that opens at `:1230` ("recode crop_code values that were
  erroneously listed in crop_code_os") and runs to `:1248`, all on the harvest
  side, all things our `_crop_codes` map does not attempt to recover.

## What a fix would touch

- First, a count: how many harvest rows per wave are lost at
  `malawi.py:1117`, and how many carry a `u` that survives as an unrecognized
  free-text string. Nothing else should be written before that number exists.
- The `:1114-1116` comment, which must either become true (emit the count) or
  stop claiming a log.
- If the row count justifies it: a harvest-side call into
  `_clean_freetext_unit` (or a harvest-specific variant, since the vocabulary
  differs from food's "other (specify)" text) and a crop-name recovery step
  modeled on EPAR's `:1240-1248` list, derived from our own "other, specify"
  strings.

## What it must NOT do

- Do NOT wire a recovery before measuring the row-count cost -- if the gap is
  a handful of rows, documenting it in `CONTENTS.org` is the right-sized fix.
- Do NOT copy EPAR's free-text string list verbatim; the raw "other, specify"
  text must be pulled from our own source files first, since free-text
  spelling differs between extracts.
- Do NOT copy EPAR's quantity rescaling (`:1256`, `:1319`, `:1322`) into a
  stored `Quantity`. A "20 KG BAG" row whose quantity is multiplied by 20 and
  whose unit becomes kg is no longer the reported value; if we recover the
  magnitude it belongs in a `KgFactor`-style layer, counted, not folded into
  `Quantity`.

## Cross-references

Same Malawi harvest-side pass as the IHS5 crop-conversion and `ag_r07` issues
filed alongside this one.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (N7)
