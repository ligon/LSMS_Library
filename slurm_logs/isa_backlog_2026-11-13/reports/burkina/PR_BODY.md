## Burkina Faso: 2021-22 crop_production, date specs, the `Unknown` unit, and 2014 C05/C06 values

This needs one Burkina re-warm: 25 of 555 table hashes and 75 of 2,321 wave hashes move, all Burkina; 0 of 42 `build_transforms_fingerprint` move.

### #1436 / #824 / #1437: 2021-22 `crop_production`
- **Rows:** all 8,476 s16c lines in the panel are kept, plus 642 Farm-level rows.
- **Sales:** attached by **crop code**, because the two files label the same code list differently.
  - 1,331 sales sit on their crop's single s16c line, keyed `hh-crop-sale-single-row`; 217 go on Farm-level rows.
  - `Value_sold` totals 270,464,270 FCFA, equal to the raw s16d sum.
  - `Condition_sold` is served from `s16dq05d` on all 1,548 sales, with the same fold #1528 applies to 2018-19.
- **Harvest from dispositions** (ruling 2026-11-13, "Extend Mali's rule"):
  - 3,958 of the 6,397 finished household-crops get a harvest: 3,421 on a single line, 537 on Farm-level rows. 2,272 have dispositions in more than one unit and stay NA.
  - The rule is checked against 2018-19, where a harvest and its dispositions share a row. Of the 12,561 finished rows whose positive dispositions are all in the harvest's unit, the sum equals the harvest exactly on 87.4%. On the 12,490 of those with at least one disposition, it is 87.9%.
  - 239 totals add dispositions recorded in two or more product states (fresh, dried, etc.); 197 of them fold to different `condition` tokens, 3.5% of the kg on Kg totals. These are served under `unknown_condition`.
  - Treating all unit fields as one list is evidenced only for codes 1–7. 2 of the 32 totals on bare unit codes combine fields and rest on that assumption.
- **The 179 recorded harvest quantities are partial** (`harvest-to-date`; the median plot still had 50% left to harvest). Do not read them as yields.
- **`harvest_kg` / `yield_kg` for 2021-22 (586 / 506 rows):**
  - Most are dispositions totals: 530 in Kg, plus 56 recorded partial harvests.
  - 57 of the `harvest_kg` rows are Farm-level rows, not plots.
- **`production_obs`** (ruling 2026-11-13; same level as Niger #1360):
  - The two repeated s16c (household, plot, crop) keys are distinct lines, and both are served.
  - `LSMS_GRAIN_STRICT=1` passes.
  - **The Country 2018-19 index gains a `production_obs` level**, `unresolved_observation` on every row.
  - `Feature('crop_production')` keeps the 2018-19 values, but **its `production_obs` dtype changes from `str` to `string` in calls without Niger**.
- **Labels:**
  - Codes 55, 58 and 64 are labelled from the file's own roster text (Anacarde, Banane douce, Soja). This is registered as a derivation, basis derived-from-data.
  - **The 4 code-64 lines whose text names maize are served as Soja**, because their code says so.
  - **Code 65 is served as s16d's `Autre (à préciser)`.** It is carried by the crop-code link and has no derivation key.
- **Units:** served through the read-path `u` table, so `u` and `Unit_sold` share one vocabulary (`Kg`, `Plat Yoruba`).
- **Not served yet:** survey kg and sold kg (left as a residual). The cause of #1437 stays open.

### #1045: `interview_date` through the build-time parser
- Six declarative specs per wave. The served table is frame-equal to base, with 0 `unparsed` values.
- Every read now emits six `DatetimeParseWarning`s, one per spec, reporting the `##N/A##` sentinels.
- `LSMS_DATE_STRICT=1` refuses the table, as it does for Ethiopia.

### #1118: a blank input unit is `Unknown`
- 0 rows move; I counted before changing anything. The change is a guard.

### #1442: C05/C06 values as `Expenditure` (closes #1442)
- This is the archived commit `d81358091`, reviewed and re-measured. Under the 2026-11-04 ruling these are direct survey answers, not a derivation.
- Under #1509:
  - no existing `food_prices` row moves;
  - 17 produced rows move to the disclosed fallback rung;
  - 2014 kg rises 0.03%.
- `basis='total'` for 2014 rises 47.0% and is **not comparable across waves**, because the EHCVM waves carry no own-production or gift value.
- The same money keyed into two blocks is counted twice: 1,090 / 353 served `(i, j, visit)` groups, from 1,074 / 350 raw source rows.
- Passages 2–4 rest on variable labels only.
- 45 money-only rows keep a recorded unit.

### For maintainer review
1. Confirm option (d), attaching each sale to its crop's single line, for Burkina 2021-22. Its only authority so far is the batch brief.
2. Confirm serving code 65 as s16d's `Autre`.
3. Should the 2021-22 crop and unit spellings be harmonised to 2018-19's? Today 0 of 984 cowpea seed rows and 0 of 291 rice seed rows match a harvest.
4. Survey kg and sold kg for Burkina 2021-22 remain a residual.
5. No `interview_date` window was added.
6. Disclosures:
   - a cold 2018-19-only crop request lacks the `Derivation` column;
   - on 2021-22, Site Q reports 1 low `Quantity` and 2 high `Sale_price` rows, and `Feature` raises `PopulationHeterogeneityWarning`.
7. `make matrix` still needs to run at integration.

### Verification
- **Cold builds:** base against candidate in private roots. 23 of 33 calls are frame-equal; the rest move only as stated above. Warm reads equal cold builds.
- **Tests:**
  - 70 Burkina-related files: 3,411 passed, 0 failed.
  - Touched files at the head: 477 passed.
  - Whole suite: 18,206 passed. Its 3 failures are environmental; the two outside Burkina pass on the candidate in fresh roots.
- **Hash census at the head:** Burkina only; 0 build-transform and 0 framework-import fingerprints move.
- **Red-team delta review:** APPROVE; S1–S4 applied.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
