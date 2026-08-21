# Recon brief — assets gap-fill (2026-06-06)

READ-ONLY. Do NOT edit/create/commit. Repo:
/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library (branch development).
Venv: ./.venv/bin/python

## CRITICAL DVC RULE
Read data ONLY via `from lsms_library.local_tools import get_dataframe`:
`get_dataframe('<Country>/<wave>/Data/<file>.dta')`. Lock-free direct-S3 bypass.
**NEVER run `dvc pull`/`dvc fetch` CLI** — it takes the global DVC lock and fails.

## Goal
Can the assigned country implement **assets** (household durable goods)? If so,
give a ready-to-paste config.

## Canonical target
- `data_scheme.yml`:
  ```
  assets:
    index: (t, i, j)
    Quantity: float
    Value: float
    Age: float      # only if present
  ```
- `data_info.yml`: one durables source file per wave; `idxvars: i: <hhid>` (must
  match the household_roster's `i`), `j: <item name/code>`; `myvars:` Quantity,
  Value, and Age if present. Do NOT add `v` (joined from sample()).
- Dedup rule when a HH reports item j multiple times: Quantity=sum, Value=sum,
  Age=min. WIDE-format durables (one column per item) need a reshape -> note it
  (script path); clean LONG (one row per (HH,item)) -> YAML path.

## Study an existing implementation
`grep -rl 'assets:' lsms_library/countries/*/_/data_scheme.yml`
(e.g. Liberia sect15, Iraq, Kosovo, Guyana, Guatemala E14). Read its data_info
`assets:` block to copy the idiom.

## Steps
1. Confirm the country does NOT already declare assets.
2. Read its household_roster block to get the canonical `i`.
3. Per wave, locate the durable-goods module; load via get_dataframe; identify
   item (j), quantity, value, age columns; check coverage + that i matches roster.
4. YAML vs script-path call.

## Report (<220 words)
Lead with IMPLEMENTABLE / IMPLEMENTABLE-WITH-CAVEATS / NOT-IMPLEMENTABLE / BLOCKED.
Then per wave: source file, i, j, Quantity/Value/Age columns, YAML-vs-script,
coverage %, a ready-to-paste data_info `assets:` block, confidence. Do NOT write files.
