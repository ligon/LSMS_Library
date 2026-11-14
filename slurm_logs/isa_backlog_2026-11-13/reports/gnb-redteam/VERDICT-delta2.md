**Second delta verdict: APPROVE.** Candidate `6a124b46af94cbdf41bcd8faf4fbaa4713222ea4` against previous `90bdd6742`. Base `f138a3d61`, with `c35c95eaf` as the reference for the leaver counts. Everything was measured cold in private roots in one burst job, 41280044. One item goes to the user: how the 685 "Nunca" rows are labelled.

**1. The 3,945 withheld rows**
- **The rows:** 3,945 purchased rows, each with Quantity and Expenditure NA.
  - Each carries the new withheld key and the 7-day key, and not the as-released key.
  - Every other 2021-22 row carries the as-released key alone.
- **The inputs:** the new key's inputs are the same 3,945 rows; their (i, j) pairs match the withheld rows exactly.
- **Collision rule:** it fires on nothing. The index is unique.
- **Everything else is frame-equal to `90bdd6742`:**
  - all other `food_acquired` rows;
  - `food_quantities` (kgs and units);
  - `food_prices` (default and unitvalue);
  - `food_expenditures`.

**The 685 "Nunca" rows: serving them as purchased is weakly defensible, and the data point the other way.**
- On these rows, `s07bq06` says the household never bought the item.
- Among consumed rows answering Nunca, only a small share have a positive purchased quantity: 502 of 23,204 (2.2%) in 2018-19, whose quantities are not rounded, and 354 of 21,423 (1.7%) in 2021-22.
  - In 2018-19, 16,174 of those rows report own production and 7,072 a gift.
  - None of them, in either wave, carries a purchase value.
  - By contrast, about 88% of the other consumed rows have a positive purchased quantity.
- None of the 685 has `s07bq07a` or `s07bq08`, and own production is NA on 48 of them.
- Togo's test (no own production or gift reported, so it is a purchase) assumes `s07bq04` and `s07bq05` are exact. Here they are rounded too, so a 0 there is weak evidence.
- What changes: no number moves, because the derived tables drop these NA rows. Only the label does, so anyone counting purchase incidence from `s='purchased'` gets 685 false purchases.
- Options for the user:
  - **(a)** keep them as purchased, following the template;
  - **(b)** serve them as `s='other'` under the same withheld key. `other` is a canonical `s` value. This is what the data support;
  - **(c)** omit them. This contradicts the ruling that no reported item disappears.
- The 521 rows whose last purchase was more than 30 days ago are ambiguous. I did not measure that group.

**2. Leavers' MonthsSpent**
- MonthsSpent is NA on all 341 leavers.
- The key sits on exactly 12 rows, all of them leavers, in 3 households. All 12 recorded answers are Sim, and the inputs join on (t, i, pid).
- All other roster values equal the previous candidate. 2021-22 `household_characteristics` is back to 40,322 and is frame-equal to `c35c95eaf`'s; 2018-19's is unchanged.
- **The canonical `Derivation` declaration changes no other country:**
  - Togo, Benin, Niger and Albania rosters are frame-equal to the previous candidate, with no `Derivation` column.
  - The census moves nothing outside Guinea-Bissau.
- **Two side effects to put in the PR body:**
  - Guinea-Bissau's 2018-19 roster now also carries an all-NA `Derivation` column, as it already carried `Departed`. The worker's "frame-equal" claim for that slice is therefore true only once that column is dropped; I did not compare the remaining values directly.
  - A `Feature('household_roster')` call that includes Guinea-Bissau gains a `Derivation` column, NA for every other country (Togo is otherwise equal).

**3. Hash census:** confirmed, Guinea-Bissau only.
- Base → candidate: 22 of 555 tables and 44 of 2,321 waves; `build_transforms_fingerprint` 0 of 42; `framework_imports_fingerprint` 0 of 38.
- Previous → candidate: 22 tables and 22 waves, all Guinea-Bissau 2021-22.

**Not run in this round:** pytest. I relied on the worker's runs at `b943fa813`. Outputs are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/gnb-redteam/out/delta2/`.
