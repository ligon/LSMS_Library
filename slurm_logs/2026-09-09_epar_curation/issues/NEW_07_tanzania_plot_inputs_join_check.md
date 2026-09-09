Title: Tanzania: check plot_inputs against plot_features/crop_production for the 2020-21 roster mismatch EPAR reports
Labels: enhancement, diagnostics

EPAR's 2020-21 fertiliser-rate pipeline merges its input-quantities file
against a plot/season roster and reports 80 unmatched rows out of 10,607 on a
merge that keeps only master-side and matched rows. We have not checked
whether an analogous mismatch exists between our own `plot_inputs` and
`plot_features` / `crop_production` for the same wave.

## Evidence

**Ours**
- `lsms_library/countries/Tanzania/2020-21/_/plot_inputs.py` builds
  `plot_inputs` by calling `plot_inputs_for_wave('2020-21', sec3a, sec4a,
  colmap)` at `:22` (`tanzania.py:1352`), which stacks rows from
  `ag_sec_3a.dta` (plot-level fertiliser / herbicide / pesticide, one row per
  plot-input) and `ag_sec_4a.dta` (seed, one row per plot-crop)
  independently. `grep -n 'merge\|\.join\|concat\|plot_features\|crop_production'`
  over the whole function body (`tanzania.py:1352-1549`) finds one hit -- a
  `pd.concat` of the block's own pieces at `:1491` -- and no merge with any
  other table. There is no build-time merge against `plot_features` or
  `crop_production` either.
- `assert df.index.is_unique` at `2020-21/_/plot_inputs.py:23` checks
  uniqueness only, not roster membership.
- Word-boundary grep for `ag3b_34`, `rent`, `rental`, `ag4a_25`, `ag4a_27`
  and `plot_inputs` across `Tanzania/_/CONTENTS.org` (1,841 lines): zero hits
  for all six. In `open_issues.tsv` the only hit is #569 ("Register
  plot_labor/crop_production/plot_inputs in index_info"), which is about
  index-name registration, not roster agreement. Nothing on our side records
  this check having been done, in either direction.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/Tanzania NPS/Tanzania NPS Wave 5/`)
- `EPAR_UW_Tanzania_NPS_W5.do:2473` -- `merge 1:1 hhid plot_id season using
  "...Tanzania_NPS_W5_input_quantities.dta", nogen keep(1 3)`, carrying two
  comments: "11 plots have expenses but don't show up in the all_plots roster"
  and "MGM 5.18.2024: 80 not matched, 10,527 matched". The master is the
  appended `ag_sec_3a` + `ag_sec_3b` plot/season roster (`:2465-2469`), the
  using file is the input quantities. `keep(1 3)` retains master-only and
  matched rows and DROPS using-only rows. EPAR does not say which side the 80
  fall on; the inline comment names the direction it was worried about
  (rows with expenses and no roster plot), at an earlier count of 11.
  80 / 10,607 is 0.75%.

## What a fix would touch

- A one-off check (not necessarily new production code) comparing
  `plot_inputs`'s `(t, i, plot_id)` set for 2020-21 against
  `plot_features`'s and `crop_production`'s for the same wave, in both
  directions, with counts.
- If a mismatch is found: since our tables are built independently rather than
  merged, the fix is not "stop dropping rows" (we drop none) but deciding
  whether to warn a user who joins `plot_inputs` against `plot_features` and
  gets a partial match -- likely a `null_read_audit.py`-style report rather
  than a build-time change.

## What it must NOT do

- Do NOT introduce a build-time merge between `plot_inputs` and
  `plot_features` / `crop_production` to detect this -- the three tables are
  deliberately independent at the grain level; the check is diagnostic, not
  structural.
- If a mismatch is found, do NOT silently drop the unmatched `plot_inputs`
  rows the way `keep(1 3)` does -- report the count, keep the rows.
- Do NOT extend the check to other waves or countries without first
  checking that the same source-module shape (`AG_SEC_3` / `AG_SEC_4`) applies.

## Cross-references

Same Tanzania 2020-21 pass, and the same source modules, as the
`crop_production.Quantity` plausibility issue filed alongside this one.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (N10)
