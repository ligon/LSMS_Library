**Delta verdict: APPROVE.** New candidate `2ee44de32c1d13ad8f5c1b4ea13ac3c5b7e9e847` (from `3336ffc99`), base `f138a3d61`. The fixes for F1, F2, F3, F4 and F9 all hold, nothing else moved, and the hash census is unchanged.

Everything below ran cold in private roots in one burst job, 41280936; outputs are in `reports/coping-redteam/out/delta/`.

**1. F2, W1 assets (Age/Value as NA).** Fixed.
- **Mechanism.** A wave-folder-named module is an existing pattern (`Iraq/2012/_/2012.py`).
  - `Wave.data_scheme` excludes the stem, so it declares no table. The worker's test confirms this.
  - The hook loads at the wave level too: `Country('Nigeria')['2010Q3'/'2011Q1'].assets()` both carry `Quantity`, `Age` and `Value`.
  - Neither W1 `mapping.py` nor `nigeria.py` defines `assets`, here or on the Nigeria head `a6955194a`.
  - One caveat for later: `Wave.formatting_functions` loads the two files in set order. If `mapping.py` ever gains an `assets` function, which one wins would depend on the hash seed.
- **Cold, warm and `assume_cache_fresh` agree** on all 16 one-quarter requests (8 quarters × `assets`, `shocks`):
  - post-harvest `assets` give 0 rows with 3 columns;
  - `assets(['2010Q3'])` gives 38,947 rows with Age/Value NA;
  - no request errors.
- **Make backend.** A cold `2011Q1` request gives 0 rows, cold `2010Q3` gives Age/Value NA, and warm `2011Q1` gives 0 rows. The all-waves make build equals the default build.
- **Values.** All-waves `assets` equals base's post-planting rows and the previous candidate on every column.
- **One harmless difference.** A cold single-quarter W4 `shocks` request has no `HowCoped3` column, while the warm slice carries it all-NA. Row counts agree and the column is optional.
- **Raw file.** `sect5b_plantingw1` has 56,816 rows, `s5q3` "how long ago was item acquired? (years)" (56,755 non-null) and `s5q4` "if you wanted to sell one of this today…" (56,588 non-null). W2 wires these same variables as Age and Value. So NA here means "not wired", as the worker says.

**2. F3, the slot-aware class.** Fixed.
- **Scope.** Against the previous candidate, 16 countries' served `shocks` are identical. In Uganda exactly 96 cells change in `HowCoped0` and 96 in `HowCopedSet`, now "Did nothing".
- **Uganda data.** No "Nothing else" is left in any position. The recorded gap (code 40 first, then a strategy) is on 3 rows in 2013-14 and 3 in 2015-16. The worker's 55 / 41 test passes on the warm root.
- **Loader.** It refuses five malformed shapes, all tested. Allowing filler plus the slot-aware class while refusing "did nothing" plus the slot-aware class is the right asymmetry. `classes_for_wave` correctly keeps the new class out of the "did nothing" set.
- **Idempotence** is tested.
- **One step beyond the ruling's wording (must-state).** The ruling names the set {Did nothing}. The worker also writes "Did nothing" into `HowCoped0`, replacing the recorded "Nothing else" (the parquet keeps the original). It is coherent and disclosed; the user may prefer the recorded label in the position.

**3. F1, F4, F9.** All fixed.
- F1: the synthetic merges with Nigeria `3d34550d5` and the current head `a6955194a` both pass, 13 Nigeria tests each.
- F4 and F9: the CONTENTS and `coping.yml` text now matches my measurements.
- The coping, gh1562 and assets-additive test files pass on the warm root: 145 passed, 11 skipped, 0 failed.

**4. Hash census.** Unchanged.
- base → `2ee44de32`: btf 0/42, fwimp 0/38, tables 218/555, waves 538/2,321, in the same 10 countries.
- No-op edits in `alone_first_for_wave` and in the new lone-answer block move 0 hashes.

**5. The flaky CI-wrapper test.** Unrelated to this diff.
- Serially, the first run of `test_gh932_safe_ci_failure_locations.py` failed 1 of 23 tests on base and candidate alike, then passed 23/23 twice on each.
- Under `-n 8`, alongside the private-CI and schema test files, it passed 420/420 on both.
- I kept only the summary line, so I cannot name the test that failed. The delta touches no CI script or test of that file.

**Still open for integration (unchanged):**
- F5: GNB 2021-22 needs the option-25 stamp on 3 rows, and its `optional:` positions need revisiting.
- F6: the Windows build pin must be re-measured.
- F7 and F8 must-states: strict mode now raises for 6 more countries, and there are 8 new `dropped` Nigeria cells.

**Disclosure:** I ran a bare `python3 -c 1` on n0035 once more (no import, no data). That makes two such invocations in this review.
