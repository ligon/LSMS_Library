# Sold-kg branch: red-team follow-ups F2 and F4 (GH #1560 / #1571)

Recorded by Sue (coordinator) from the fix agent's final message. The agent was Claude Opus 5.5; the harness refused its own report write.

- Branch: `feature/1560-1571-sold-kg-20261112`.
- Head: `5fca3904e1919bd11fdbe9c4ab7eaa426f72a7be`, published to the mirror.
- Red-team-approved base for this delta: `43632ff5b`.

## Commits
- `21fde2322` (F2). Only the two message formatters and one new constant in `quantity_audit.py` changed. Row selection and the structured reports are unchanged. Three tests were added. The label now names the harvest unit explicitly, because the reference is the p90 of the `(t, harvest u, j)` cell.
- `5fca3904e` (F4). Disclosure only, in Niger and Mali `CONTENTS.org` and in the canonical `data_info.yml` note for `Quantity_sold_kg`.

## New message (cold build, Mali 2021-22)
"3 of 7,498 row(s) report a 'Quantity_sold_kg' more than 100x the 90th percentile of comparable rows ... (each value is the TOTAL kilograms sold on its row, not a count of any unit; comparable rows share the wave, the row's HARVEST unit ...): ... Arachide harvest_u=Sac (100 Kg) 1,150,000 kg sold (x511.1 its cell's p90 of 2,250 kg sold, cell keyed on t,harvest_u,j); Riz ... 437,500; Riz ... 400,000."

The low tail reads: "Coton harvest_u=Kg 5 kg sold (1/160.0 of its cell's p10 of 800 kg sold)".

## F4 disclosure (counts from the red team)

| pattern | Niger | Mali |
|---|---|---|
| kg of one sack (5+ sacks sold) | 25 (12 / 8 / 5 by wave) | 15 |
| sack count typed as kg | 4 | 30 |

The one-sack rows are off by roughly the number of sacks. The count rows instead give the sack's stated mass, 50 or 100 (the agent's refinement).

## Key note, ruling (c)
The PR body states that `sold-kg-estimate`, a sibling of `survey-kg-estimate`, is the coordinator's implementation reading and open to objection. Two landed tests pin `survey-kg-estimate` to the `Quantity_kg` rows.

## Validation
Job 41252145, private roots.
- **Cold check:** the same 4 Mali rows are named and no Niger row is.
- **Tests:** 2,768 passed, 1 skipped (a warm Tanzania cache), 0 failed, across 79 files. `test_gh1493_near_zero_screen.py -m "not requires_s3"`: 27 passed.
- **Hash census, `43632ff5b` to head:** 0 of 555 tables, 0 of 2,321 waves, 0 of 42 build-transform fingerprints and 0 of 38 framework-import fingerprints move.

## Not run
- gh1493's `requires_s3` tests.
- The full suite.
- The Make backend and `Feature()`; the red team ran both at `43632ff5b`.

## Notes
- The pytest logs under `sold-kg-1560/out/pytest-*.txt` probably carry household ids in their warning summaries. Keep them private.
- Niger `CONTENTS.org` still says the rainy 9999 Kg sale "breaks kg = quantity", which the red team did not reproduce. The PR body says so.
