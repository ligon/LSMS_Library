# EPAR Agricultural Development Indicator Curation x LSMS_Library -- survey brief

Shared artifact for the EPAR survey agents. Author: Sue (scrum master), 2026-09-09.
Companion to the WB pass in `slurm_logs/2026-06-13_wb_incidence_map/BRIEF.md`.

## Goal

Document the EPAR (Evans School Policy Analysis & Research, University of
Washington) *Agricultural Development Data Curation* project -- Technical
Report #335 and its siblings -- and identify, with evidence, the places where
LSMS_Library can LEARN from work they have done.

## The stance (maintainer, verbatim in spirit)

> As with the WB project we should not accept work done there as canonical,
> but as a useful point of comparison.

So: every EPAR construction is a *claim to reconcile*, never a reference
answer. Where they and we disagree, the finding is the disagreement plus the
evidence on each side, not "EPAR does X so we should".

**Cautionary example already in-repo.** EPAR's Uganda W5 readme flags the
2015-16 sold-value defect (GH #829, `a5aq8` divided by 100 in the distributed
release) under `### ALL PLOTS` ("Sold values are inaccurate in W5 ... generated
as missing"), and the very next section `### GROSS CROP REVENUE` says "No
issues in this section" while its `value_crop_production` descends from that
same `sold_value` (`EPAR_UW_Uganda_UNPS_W5.do:476-547`). See
`slurm_logs/price_sources/PAPERS_uganda_2015-16_value_sold.org` section 2.
**Rule: "No issues in this section" is not clearance.** Check whether a
section's inputs come from a section that does have an issue.

## Sources (all read-only, on disk)

- `/global/scratch/fsa/fc_jevons/ligon/reference/epar/NOTES.md` -- provenance
  (commit hashes, dates, licences). Cite the hash when you cite a file.
- `/global/scratch/fsa/fc_jevons/ligon/reference/epar/LSMS-Agricultural-Indicators-Code/`
  -- the TR#335 code. One ~300 KB do-file per country-wave
  (`{Country}/{Country} Wave N/EPAR_UW_{C}_W{N}.do`), a ~30 KB `readme.md` per
  wave with a `### SECTION` / `- **KNOWN ISSUES:**` skeleton, per-country
  `Nonstandard Unit Conversion Factors/` (Uganda, Malawi), panel keys
  (`Malawi IHS/mwi_panel_ids.dta`, `Ethiopia ESS/Panel Key/hhid_panel_key.dta`),
  `_Summary_statistics/EPAR_UW_335_master_list_indicators.xlsx` +
  `EPAR_UW_335_SUMMARY_STATISTICS.do`, `Batching/`.
- `/global/scratch/fsa/fc_jevons/ligon/reference/epar/Household-Consumption-Data/`
  -- value of household food consumption by source (purchases / own
  production / gifts), 16 countries incl. six EHCVM countries, GhanaSPS,
  Kenya, India, Sierra Leone; `Final Data/*.dta` included.
- `/global/scratch/fsa/fc_jevons/ligon/reference/epar/358_Estimating-Crop-Yields/`
  -- 2017 construction-decision sensitivity study for crop yields (Ethiopia,
  Tanzania).
- `/global/scratch/fsa/fc_jevons/ligon/reference/epar/webpage.txt` -- the
  project page.
- NOT cloned: `LSMS-Data-Dissemination` (3 GB). Allowlist for on-demand fetch
  into `reference/epar/dissemination/` (record it in `NOTES.md`):
  `EPAR_UW_335_AgDev_Indicator_Estimates.xlsx` (7.7 MB) and any single
  per-wave `README.md`. Nothing else without asking the scrum master.

## Reading recipe for a 300 KB do-file (do this, do not read the whole file)

1. Read the wave `readme.md` in full. It is the map.
2. Section banners: `grep -n -A1 '^\*\{20,\}' FILE.do | grep -v '^\*\|^--'`
   (~120 banners per file).
3. Dive only into sections that map to a feature we have (list below) or to
   a KNOWN ISSUE. `sed -n 'A,Bp'` the section; cite `file:line`.
4. Cross-wave: the readmes are near-copies across waves; diff two of them
   (`diff <(...) <(...)`) to find the wave-specific statements fast.

## Our side (the crib -- state learnings as DELTAS from this, not rediscoveries)

Design: item-level rows at the survey's natural grain; every aggregate,
index, ratio, imputation or valuation lives in code
(`lsms_library/transformations.py`), never in a parquet. No winsorization
anywhere. Weights normalised to within-wave mean 1 at API time.

Item features (all seven ISA countries incl. these five; see
`lsms_library/data_info.yml` for canonical columns):

- `crop_production (t, i, plot, j, u, condition, season)` -- REPORTED
  Quantity + native unit `u`, Quantity_sold, Value_sold, `condition`
  (dry/fresh/shelled ... index level), optional survey-reported `KgFactor`
  (kg per unit of `u`; Uganda 2018-19 only so far, GH #849).
- `plot_inputs (t, i, plot, input)`, `plot_labor (t, i, plot, source, season)`
  with `PersonDays`/`Wage`, `livestock (t, i, animal)` with HeadCount /
  HeadSold / Value, `plot_features (t, v, i, plot_id)` with Area / tenure,
  `assets` (item ownership), `community_prices (t, v, j, u)` (surveyed, not
  imputed), `food_acquired (t, i, j, u, s)` with `s in {purchased, produced,
  inkind, other}`, `household_roster`, `sample` (v, weight, panel_weight,
  strata, Rural), `anthropometry (t, i, pid)`, `people_last7days (t, i, pid)`.

Transforms (`transformations.py:1578-3210`, each documented against the WB
LSMS-ISA_Ag column it reproduces): `harvest_kg` (layers: reported KgFactor ->
survey_median of reported for same (u, condition) in the country-wave, N>=5
-> library-inferred factor -> none; counts, never clips), `yield_kg`,
`total_labor_days` / `_family_` / `_hired_`, `nitrogen_kg`, `seed_kg`, `tlu`,
`livestock_engaged`, `dependency_ratio`, `farm_size`, `nb_plots`,
`median_price_valuation` (WB EA -> admin -> national median ladder, >=10 obs),
`asset_index` (PCA), `anthropometry_zscores`. Food side:
`food_expenditures(basis='purchased'|'total')`, `food_prices(units=...)`,
`food_quantities(units=...)`, `conversion_to_kgs`, `_get_kg_factors`.

Our waves: Ethiopia 2011-12, 2013-14, 2015-16, 2018-19, 2021-22; Malawi
2004-05, 2010-11, 2013-14, 2016-17, 2019-20; Nigeria 2010Q3/2011Q1 ...
2023Q3/2024Q1 (post-planting / post-harvest rounds are separate `t`);
Tanzania 2008-09, 2010-11, 2012-13, 2014-15, 2019-20, 2020-21; Uganda
2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20.

Per-country idiosyncrasies and decisions already taken:
`lsms_library/countries/{C}/_/CONTENTS.org` (1,000-2,200 lines each; read
the LOGBOOK / WAITING / TODO entries too -- a CLOSED issue can have a live
caveat parked there). Open issues: `open_issues.tsv` in this directory
(number, labels, title); cite by `#NNN`. Recent price-study findings:
`slurm_logs/price_sources/SYNTHESIS.org`, `PHASE2_SYNTHESIS.org`,
`DATA_PROBLEMS.csv`.

## Classification tag (apply to EVERY EPAR construction you report)

- `reported` -- carried from the instrument as asked.
- `imputed` -- filled or replaced by a rule (median price ladders, regional
  median conversion factors, parcel-share plot areas, winsorization).
- `aggregated` -- summed / collapsed / indexed to a coarser grain.

The tag is the finding: an `imputed` construction is a methodology we may
learn from but must never store; a `reported` field we lack is a wiring gap.

## The three-cell register (country agents)

For every KNOWN ISSUE (and every wave-specific caveat) in the EPAR readmes,
place it in one cell, with evidence on both sides:

1. EPAR found it AND we did (cite `CONTENTS.org` line or `#NNN`).
2. EPAR found it AND we did not (candidate new issue -- give the raw variable,
   wave, and what EPAR did about it).
3. We found it AND EPAR did not (cite ours; note whether EPAR's numbers are
   exposed, as in the #829 case).

Waves EPAR covers that we don't, and vice versa, are their own short table.

## Rules

- READ-ONLY against `lsms_library/` and the reference clones. No edits, no
  builds, no `Country(...)` calls that would rebuild caches (a warm
  `pd.read_parquet` of a cached table is fine if you need a column list).
- Never invoke the `dvc` CLI. Never download microdata.
- Every claim carries `path:line` (relative to the clone root or the repo
  root). "I checked" is not a finding; name what you found.
- If your finding contradicts `CONTENTS.org`, report and quote both; do not
  assume the file is stale.
- This pass is documentation and code-reading. A numeric parity run against
  EPAR's final `.dta` files is a later, separate dispatch. Do not attempt it.
- Prefer ASCII. `=code=` markup in Org. Tables are welcome.

## Output

One file per agent in this directory, `FINDINGS_<scope>.org`, with this
skeleton:

```
#+title: EPAR x LSMS_Library -- <scope>
#+date: 2026-09-09
* Coverage (their waves vs ours; what they build for this scope)
* KNOWN ISSUES register (three-cell table; every row has evidence both sides)
* Construction decisions by feature area (each tagged reported/imputed/aggregated, with .do:line)
* Ranked learnings (what we could take, why, what it would touch in our repo, and the risk)
* Contradictions with our CONTENTS.org / issues (quote both)
* What I did not read (so the synthesis knows the gaps)
```

Return in your final message a <= 25-line summary: the top 5 learnings and
the top 3 new-issue candidates, each with one `path:line`.
