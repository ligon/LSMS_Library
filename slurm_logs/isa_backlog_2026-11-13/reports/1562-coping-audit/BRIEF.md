# Stream: 1562-coping-audit (READ-ONLY; no worktree, no commits)

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.
You make no commits and edit no tracked file.  Read code by absolute path in
`/local/job41201840/mirrors/LSMS_Library` (on development `f138a3d61`; do not
run any git command that changes it).  Burst jobs clone `f138a3d61` through
`tools/burst-env.sh`.

- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/1562-coping-audit/`.
- Issue: #1562 (`gh issue view 1562 -R ligon/LSMS_Library`).
- Rulings (`isa-decisions.org`): "Coping-strategy contract, GH1562" and "EHCVM
  coping order, GH1562".  Verbatim core: HowCoped0..2 mean the household's
  REPORTED order (1st/2nd/3rd), filled only where an instrument records an
  order (e.g. Uganda s16q04a/b/c).  Every shocks country also gets an
  UNORDERED set column of chosen strategies.  EHCVM 2018-19 fills the set only
  (not positions) until its order is proven.  The FIRST STEP is an audit of
  every shocks country's coping semantics.  That first step is your job.

## Work
1. Enumerate every country and wave that declares `shocks`.  For each, record:
   - the source variables behind each HowCoped column, with their variable
     labels and value labels;
   - what the instrument records: a respondent order (1st/2nd/3rd), a
     code-ordered hook list, a multiple-response yes/no grid, or a single
     strategy.  Use the questionnaire where it is held, and say which;
   - whether the positions as served today are a respondent order;
   - what the unordered set would be built from, and whether it can be built.
   For each country, read the shocks-related passages of its `CONTENTS.org` and
   name the idiosyncrasies.
2. Measure on a cold private build (burst job), per wave:
   - the HowCoped fill rates;
   - the number of distinct strategies per household-shock;
   - whether the served positions duplicate (the same strategy in two slots).
3. Cost the implementation options:
   - (i) the set built at build time in each wave's config or script, a
     whole-country re-warm per country;
   - (ii) the set derived at read time from the build output, which works only
     where every chosen strategy reaches the build;
   - (iii) a mix.
   Say for each option which countries' hashes move, and what EHCVM's "set
   only, positions empty" requires.
4. Draft the canonical `lsms_library/data_info.yml` shocks entry: position
   semantics, the set column (dtype and representation in parquet and pandas;
   how `MULTI_SEP` is used elsewhere), and canonical values.  Propose two or
   three NAMES for the set column.  Naming is the user's call; do not pick one.
   Measure whether editing the canonical `data_info.yml` moves any hash, rather
   than assuming it.

## Deliver
`REPORT.md`:
- a per-country, per-wave table;
- the measurements;
- the options with their costs;
- the draft schema;
- the questions for the user.
Keep it readable; put the per-wave detail in an appendix or a TSV in `out/`.
