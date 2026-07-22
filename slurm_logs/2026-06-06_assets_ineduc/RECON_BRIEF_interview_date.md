# Recon brief — interview_date gap-fill (2026-06-06)

READ-ONLY recon. Do NOT edit/create/commit any file. Repo:
/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library (branch development).
Venv python: ./.venv/bin/python

## CRITICAL DVC RULE
Read data ONLY via `from lsms_library.local_tools import get_dataframe`:
`get_dataframe('<Country>/<wave>/Data/<file>.dta')` (countries-relative) or
`get_dataframe('../Data/<file>.dta')`. This uses a lock-free direct-S3 bypass.
**NEVER run `dvc pull` / `dvc fetch` from the CLI** — it takes the global DVC
lock and will fail under concurrency. (Prior recon agents were blocked solely
because they used the CLI.)

## Goal
Determine whether the assigned country can implement the **interview_date**
feature, and if so give a ready-to-paste config.

## Canonical target (per the merged #325/#335 fix)
- `data_scheme.yml` block:
  ```
  interview_date:
    index: (t, i)
    Int_t: datetime
  ```
- `data_info.yml` block: a single source file (the cover page / household
  identification module) with `idxvars: i: <hhid>` and
  `myvars: Int_t: <date col>`. Do NOT emit `v` or `date` columns — `v` is
  joined from `sample()` automatically; a stray `v`/`date` column breaks the
  cross-country concat (that was the #325 bug).
- `Int_t` must be a datetime. Surveys store the interview date either as a
  single date field OR as separate day/month/year columns. If separate, the
  country will need a small per-wave date-combine helper (script path) —
  note that, don't write it.

## Study an existing implementation first
Find one the #335 fix shipped:
`grep -rl 'interview_date' lsms_library/countries/*/_/data_scheme.yml`
e.g. India (SECT00 intdate/month/year), Kosovo, Cambodia, Iraq, Uganda.
Read its data_info.yml `interview_date:` block to copy the idiom (and note
whether it used a single date col or a day/mo/yr combine).

## Steps
1. Confirm the country does NOT already declare interview_date.
2. Read its household_roster block (`<Country>/<wave>/_/data_info.yml`) to get
   the canonical `i` (household id) column + the cover/roster source file.
3. Per wave, locate the interview-date field(s) on the cover page / hh-ident
   file. Load via get_dataframe; identify the date column OR the
   day/month/year triple. Check coverage (% non-null) and that `i` matches
   the roster.
4. Decide: YAML-path (single date col) or needs a date-combine helper.

## Report (<200 words)
Lead with CONFIRMED / CONFIRMED-NEEDS-HELPER / NOT-IMPLEMENTABLE / BLOCKED.
Then per wave: source file, `i` column, the date column(s) and how to form
`Int_t`, coverage %, and a ready-to-paste `data_info.yml interview_date:`
block. List which waves are doable. Note the confidence level. Do NOT write
any file.
