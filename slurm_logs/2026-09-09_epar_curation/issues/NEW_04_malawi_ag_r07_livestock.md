Title: Malawi: _livestock_block never reads ag_r07 ("owned exactly 12 months ago"), which is present in all four waves
Labels: enhancement, data-gap

Malawi's IHS Module R carries a second reported headcount -- "How many
[LIVESTOCK] did your household own exactly 12 months ago?" (`ag_r07`) -- in
every wave we hold, and `_livestock_block` does not read it.

## Evidence

**Ours**
- `lsms_library/countries/Malawi/_/malawi.py:1734-1736` --
  `def _livestock_block(df, *, hhid, animalcode, owned_flag='ag_r01', headcount='ag_r02', acquired='ag_r10', sold='ag_r16', value='ag_r04', t=None)`.
  `ag_r07` is not among the reported columns the block accepts.
- `grep -rn 'ag_r07' lsms_library/countries/Malawi/` returns zero hits: not
  read by any wave script, not declared in any `data_info.yml`.
- The comment at `malawi.py:1728-1732` notes that the columns actually read
  (`ag_r00/r0a/r01/r02/r04/r10/r16`) carry the same name in every wave, and
  that gifts (`ag_r09*`) is deliberately not one of the reported columns.
  `ag_r07` is not mentioned at all -- it was not considered, not excluded.
- The column is present in all four waves' Module R source files (read
  directly from the cached DVC blobs named by each wave's `.dvc` sidecar):

  | wave | file | `ag_r07` | Stata variable label |
  |---|---|---|---|
  | 2010-11 | `Full_Sample/Agriculture/ag_mod_r1.dta` (and the Panel twin) | present | "How many [LIVESTOCK] did your HH own exactly 12 months ago?" |
  | 2013-14 | `AG_MOD_R1_13.dta` | present | same question |
  | 2016-17 | `Cross_Sectional/ag_mod_r1.dta` (and `Panel/ag_mod_r1_16.dta`) | present | same question |
  | 2019-20 | `Cross_Sectional/ag_mod_r1.dta` (and `Panel/ag_mod_r1_19.dta`) | present | "How many [LIVESTOCK] did your household own exactly 12 months ago?" |

  So the gap is all four waves, not a subset.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/Malawi IHS/`)
- `ren ag_r07 nb_ls_1yearago` appears in every Malawi wave do-file:
  `Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:1529`,
  `Wave 2/EPAR_UW_Malawi_IHS_W2.do:2481`,
  `Wave 3/EPAR_UW_Malawi_IHS_W3.do:1555`,
  `Wave 4/EPAR_UW_Malawi_IHS_W4.do:3012`, each followed by per-species
  splits (`nb_cattle_1yearago`, `nb_smallrum_1yearago`, ...). EPAR reads the
  column in all four waves.
- The wave readmes say something adjacent that must NOT be read as "the
  column is absent": `Malawi IHS Wave 3/readme.md:97` and
  `Wave 4/README.md:98` carry, under TLU, "Survey question on how many
  animals owned by household at start of the season are not available.
  Livestock holdings are valued using observed sales prices." That is about a
  START-OF-SEASON ownership question and about VALUATION; it is not about
  `ag_r07`, which those same waves' do-files read.
  (`Wave 1/readme.md:97` is "Known Issues: None"; it carries no such text.)

## What a fix would touch

- `_livestock_block`'s signature (an `owned_1yr_ago` or similarly named
  optional keyword) and all four wave scripts that call it, to pass `ag_r07`.
- A canonical column name and note in `lsms_library/data_info.yml` beside
  `HeadCount` / `HeadAcquired` / `HeadSold`, saying it is a stock at a second
  point in time and is not a flow.
- Whatever downstream reduction wants it (a herd-change or TLU-change
  transform); none exists today.

## What it must NOT do

- Do NOT use the twelve-months-ago count to impute or replace the current
  headcount (`ag_r02`) -- it is a distinct reported quantity, not a fallback
  for a missing current value.
- Do NOT derive an acquisition or loss flow from
  `ag_r02 - ag_r07` at build time. That is a transform over two reported
  stocks, and the survey also asks acquisitions (`ag_r10`) and sales
  (`ag_r16`) directly; the two routes will disagree and the disagreement is
  the finding, not something to average away.
- Do NOT assume the column is populated everywhere just because it is
  present; count nulls per wave before wiring anything downstream.

## Cross-references

Same Malawi harvest/livestock pass as the IHS5 crop-conversion issue filed
alongside this one.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (N6)
