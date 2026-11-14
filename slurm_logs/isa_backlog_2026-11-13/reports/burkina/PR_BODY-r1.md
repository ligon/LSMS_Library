
## Burkina Faso: 2021-22 crop_production, date specs, the `Unknown` unit, and the 2014 C05/C06 values

One Burkina re-warm: 25 of 555 table hashes and 75 of 2,321 wave hashes, Burkina only; 0 `build_transforms_fingerprint`.

### #1118: a blank input unit is `Unknown`
- Both builders (2018-19 `plot_inputs.py`, 2021 `s16b_2021.py`) wrote a minted `Manquant`; they now write `Unknown`.
- Counted first: 0 served rows carried `Manquant`, and it is no source label. `plot_inputs`, `nitrogen_kg` and `seed_kg` are frame-equal.
- Partial for the umbrella: the other EHCVM countries remain.

### #1436 / #824: a 2021-22 `crop_production` driver
- Built from the panel's s16c lines plus s16d sales, on the option-(d) rule ruled for Mali on 2026-11-03.
- A sale goes on its crop's single line (keyed `hh-crop-sale-single-row`, 1,331 sales), otherwise on a Farm-level row (217).
- Lines and sales are **matched by crop code**: the two files label one crop list differently, and by label 332 sales would lose their line.
- `Value_sold` is 270,464,270 FCFA, the raw s16d sum.
- 179 recorded harvests, all on unfinished lines, are keyed `harvest-to-date`.
- `harvest_kg` serves 56 rows, `yield_kg` 53.
- 2018-19 is unchanged apart from a new all-NA `Derivation` column.
- Partial: survey kg, sold kg, sold condition and 2014 are not wired.

### #1437: diagnosed
- On all 7,663 finished rows the whole harvest block (Q16C.12–16) is blank. Nothing is imputed.

### #1045: `interview_date` through the build-time parser
- Six specs per wave, ISO format, `##N/A##` declared as a sentinel. Frame-equal to base; 0 `unparsed`.
- Reads now warn, and `LSMS_DATE_STRICT=1` refuses the table (as for Ethiopia).

### #1442: C05/C06 values served as `Expenditure` (closes #1442)
- Archived `d81358091`, reviewed. The 2026-11-04 ruling: direct answers, no derivation.
- Re-measured under #1509:
  - no existing `food_prices` row moves (before #1509, 27,789 did);
  - 17 produced rows move to the disclosed `item_unit_nonpurchase` fallback, and they improve against the survey's unit weights;
  - 2014 kg +0.03%;
  - `basis='total'` 2014 rises 47.0%.

### For maintainer review
1. Harvest from dispositions for Burkina: 3,958 household-crops would gain a harvest; this is not ruled.
2. Survey kg, sold kg and sold condition for Burkina 2021-22: not ruled.
3. Crop labels: code 55 (Anacarde?), and 2018-19 spellings. Today 0 of 984 cowpea and 0 of 291 rice seed rows match a 2021-22 harvest.
4. Two repeated s16c lines: an audited collapse (1 `GrainCollapseWarning`; `LSMS_GRAIN_STRICT` refuses the table), or Niger's `production_obs`.
5. `interview_date` window: not added.
6. `make matrix` is needed at integration.

### Verification
- Cold base vs candidate in private roots: 23 of 33 calls frame-equal; the rest move only as above. Warm equals cold.
- Tests: 70 Burkina-related files, 3,403 passed, 0 failed. Whole suite: 18,197 passed; 1 environmental failure (an empty private root without `--rebuild-caches`).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
