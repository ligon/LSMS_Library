**CHANGES**, limited to the memo and the `CONTENTS.org` files. The code changes are right and I would approve them as they stand. Candidate `529f0bd57c667e69d11a71f859f6244acc0c5d5d`, base `f138a3d61`. My checks ran in burst job 41280001, cold, in private roots.

**1. The F6 guard works both ways — confirmed.**
- On a real Niger read:
  - When the distributional audit is made to fail, its "could not run" warning appears and the 6 price-screen reports are still filed.
  - When the price screen is made to fail, its own warning appears and the 2018-19 `Quantity` report is still filed.
  - The frame comes back at 49,042 rows both times.
- The new test `test_each_screen_has_its_own_guard` passes. In the price-failure half it checks that the distributional audit ran, not that it filed a report; the real-data check covers that.
- `test_gh1559`, `test_quantity_screen` and `test_build_transform_hash`: 134 passed, 1 skipped (the warm Tanzania test).

**2. F4 and F5 — fixed.** The emitted warning now reads "may be the answer; that is not established", and the old sentence is gone. The docstring now cites Nigeria 2016Q1: 8,507 rows, 5,520 judged, 0 named.

**3. The `s7fq13d` join is right; the simulated wiring is right but under-disclosed.**
- **The join.** I re-joined without the crop map, on household, plot and quantity only.
  - 23,272 of the 23,348 positive rows match exactly once; 76 are ambiguous where a plot has two crops with the same quantity.
  - The crop agrees on all 23,348.
  - 2017-18's `updated_ids` map is empty, so no id walk is needed.
  - My simulated wiring gives 33.1M kg against the worker's 32.8M; the gap is those 76 rows. Other waves are unchanged.
- **D2 (should-fix):** the refused rows are not left unserved, and the memo implies they are.
  - All of them are over the 250 kg ceiling (I count 2,162 against the worker's 2,167, again the 76 rows): `Charretée` 1,576, `Autre` 534, `Sac` 31, `Grenier` 21. None is a `Kg` row off 1.
  - Every one is re-served by `survey_median`, the median of the factors the screen accepted for the same unit.
  - They carry about 6.0M of the 32.8M kg. At their own reported factors they would be 82.2M kg.
  - So 1,576 of the 3,729 cart rows are capped at a median of other carts.
  - The memo §1, §7(d) and Q0, and the Mali `CONTENTS.org` paragraph, should say this and link it to Q5 (the ceiling). The 32.8M splits as `reported` 25.9M and `survey_median` 7.2M.

**4. F2 and F9 — carried correctly, with one wrong generalisation.**
- The household-gate figures, the 48 / 39 and 264 / 261 `KgFactor_sold` counts, "coverage not level", Farm-level recipients and the 3E one-cell veto all match my runs.
- **D1 (should-fix):** "every cell … 5 `Kg` sales, from 2 households" in memo lines 31 and 236, and in the Niger `CONTENTS.org` 2018-19 bullet, is true of roselle only.
  - Fresh tomato's 5 sales come from 5 households. That is why the 5-household gate keeps 23 rows: they are all tomato.
  - Memo line 143 already says this correctly.
- **Minor:** "7.6% → 94.6%" is over all crops, while "about 5% → 65-85%" is over three crops. Each is labelled, but they are not on one base.

**5. Hash census — still 0.** 0 of 42 / 38 / 555 / 2,321, with no errors on either side. No in-tree parquets; all my jobs have ended.

Both fixes are text only and move no hash. Once they are in, I would approve.
