
## Burkina Faso: 2021-22 crop_production, date specs, the `Unknown` unit, and 2014 C05/C06 values

One Burkina re-warm: 25 of 555 table hashes and 75 of 2,321 wave hashes, Burkina only; 0 of 42 `build_transforms_fingerprint`.

### #1436 / #824 / #1437: 2021-22 `crop_production`
- **Rows:** the panel's 8,476 s16c lines, every one kept, plus 642 Farm-level rows.
- **Sales:** attached to harvest lines by **crop code** (the two files label one list differently):
  - 1,331 sales on their single line, keyed `hh-crop-sale-single-row`; 217 on Farm-level rows;
  - `Value_sold` 270,464,270 FCFA, the raw s16d sum;
  - `Condition_sold` served from `s16dq05d` on all 1,548 sales.
- **Harvest from dispositions** (ruling 2026-11-13, "Extend Mali's rule"):
  - 3,958 of 6,397 finished household-crops get a harvest; 2,272 span units and stay NA.
  - In 2018-19, where harvest and dispositions share a row, the sum equals the harvest exactly on 87.9% of 12,490 finished rows.
- **The 179 recorded quantities are partial harvests** (`harvest-to-date`; a median 50% of the plot still to harvest). Do not read them as yields.
- **`production_obs`** (ruling 2026-11-13; Niger #1360): the two repeated s16c pairs are distinct lines, and both are served.
  - `LSMS_GRAIN_STRICT=1` passes.
  - 2018-19 carries `unresolved_observation`, so `Feature('crop_production')` for 2018-19 is digest-equal to base.
- **Labels:**
  - Codes 55/58/64 are labelled from the file's own roster text (Anacarde, Banane douce, Soja), registered, basis derived-from-data. Code 65 takes s16d's `Autre (à préciser)`.
  - Units are served through the read-path `u` table, so `u` and `Unit_sold` share one vocabulary.
- **Ag transforms:** `harvest_kg` 586 rows and `yield_kg` 506 for 2021-22.
- **Not served:** survey kg and sold kg (residual).
- **#1437's cause stays open.**

### #1045: `interview_date` through the build-time parser
- Six specs per wave; frame-equal to base; 0 `unparsed`.
- Every read now emits six `DatetimeParseWarning`s (the `##N/A##` sentinels), and `LSMS_DATE_STRICT=1` refuses the table, as for Ethiopia.

### #1118: a blank input unit is `Unknown`
- 0 rows moved, as counted first; the change is a guard.

### #1442: C05/C06 values as `Expenditure` (closes #1442)
- The archived `d81358091`, reviewed, under the 2026-11-04 ruling: direct answers, no derivation.
- Under #1509:
  - no existing `food_prices` row moves;
  - 17 produced rows move to the disclosed fallback rung;
  - 2014 kg +0.03%.
- `basis='total'` 2014 is +47.0% and **not comparable across waves**: the EHCVM waves carry no own-production or gift value.
- Equal money keyed in two blocks is counted twice: 1,090 / 353 `(i, j, visit)`.
- Passages 2–4 rest on variable labels only.

### For maintainer review
1. Confirm option (d) for Burkina 2021-22 (F9: the authority is the batch brief).
2. Confirm code 65 served as s16d's `Autre`.
3. Harmonise 2021-22 crop and unit spellings to 2018-19's? Today 0 of 984 cowpea and 0 of 291 rice seed rows match a harvest.
4. Survey kg and sold kg for Burkina 2021-22 (residual).
5. `interview_date` window (not added).
6. A cold 2018-19-only crop request lacks the `Derivation` column (F8). Site Q and `PopulationHeterogeneityWarning` are new on 2021-22 (F11).
7. `make matrix` at integration.

### Verification
- Cold base vs candidate in private roots: 23 of 33 calls frame-equal; the rest move as stated. Warm equals cold.
- Tests: 70 Burkina-related files, 3,411 passed, 0 failed.
- Whole suite: 18,206 passed. Its 3 failures are environmental; the two non-Burkina ones were rerun in fresh roots and pass on the candidate.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
These three notifications are the leftover waiters for the rerun (job 41276377). Their result is already in the report I just sent: candidate 59 passed, 1 skipped; base fails only `test_gh932_safe_ci_failure_locations`. Nothing has changed since: the head is still `d4d063fa11dfe582409e830c7b488f34fdae468d`, and no Burkina jobs are queued.
