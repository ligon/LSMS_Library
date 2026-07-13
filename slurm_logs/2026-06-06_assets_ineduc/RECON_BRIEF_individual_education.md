# Recon brief — individual_education gap-fill (2026-06-06)

READ-ONLY. Do NOT edit/create/commit. Repo:
/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library (branch development).
Venv: ./.venv/bin/python

## CRITICAL DVC RULE
Read data ONLY via `from lsms_library.local_tools import get_dataframe`:
`get_dataframe('<Country>/<wave>/Data/<file>.dta')`. Lock-free direct-S3 bypass.
**NEVER run `dvc pull`/`dvc fetch` CLI** — it takes the global DVC lock and fails.

## Goal
Can the assigned country implement **individual_education** (per-person
educational attainment)? If so, give a ready-to-paste config.

## Canonical target
- `data_scheme.yml`:
  ```
  individual_education:
    index: (t, i, pid)
    Educational Attainment: str
  ```
- `data_info.yml`: per-wave education source (a standalone education module OR
  the roster file if attainment is embedded); `idxvars: i: <hhid>`, `pid: <person
  id>` — BOTH must match the household_roster's i/pid (this is the #1 failure
  mode: a wrong pid column gives spine orphans). `myvars: Educational
  Attainment: <col>`. Per-wave free-text/coded labels are fine — NO cross-country
  harmonize_education table needed (that is tracked separately as issue #171).
  Do NOT add `v` (joined from sample()).

## Study an existing implementation
`grep -rl 'individual_education:' lsms_library/countries/*/_/data_scheme.yml`
(e.g. India SECT01A v01a05, Liberia, China S01B, Azerbaijan A03, Cambodia
hh_sec_3, Guatemala ECV11P10). Copy the idiom.

## Steps
1. Confirm the country does NOT already declare individual_education.
2. Read its household_roster block to get canonical i + pid.
3. Per wave, find the attainment column (standalone module or roster-embedded);
   load via get_dataframe; CONFIRM the pid column matches the roster pid space
   (compute the orphan fraction of (i,pid) keys vs the roster).
4. Note label vocabulary size/language.

## Report (<220 words)
Lead with IMPLEMENTABLE / IMPLEMENTABLE-WITH-CAVEATS / NOT-IMPLEMENTABLE / BLOCKED.
Then per wave: source file, i, pid (with orphan %), attainment column, vocabulary,
a ready-to-paste data_info `individual_education:` block, confidence. Do NOT write files.
