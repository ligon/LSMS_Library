# Red-team verdict: sold kilograms, Niger and Mali `crop_production` (GH #1560 / #1571)

- **Reviewer:** independent red team, Claude Opus 5.5 (`claude-opus-5-5`). I did not write the candidate, and I was not given the author's report.
- **Candidate:** `feature/1560-1571-sold-kg-20261112` @ `43632ff5b1f21a14845d4cd01766d01b9649459d`.
- **Base:** development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.
- **Rulings checked:** `.sucoder/isa-decisions.org` (branch `docs/isa-sweep-20261103`), "Sold kilograms, GH1560 + GH1571" and "Sold-kg details".

## VERDICT: APPROVE. No code change required.

- **Before merge, one question for the ruler (F1):** confirm that ruling (c)'s "the same 'estimate' key" may be met by per-country *sibling* keys, as the candidate does. The evidence below favours the sibling.
- **Data:** no defect found. Every served value matches its source. Every other table is unchanged, and so is the Make backend.
- **Cache:** the cost is as the author states.
- **Other notes:** F2–F4 are non-blocking.

## How it was measured

**Jobs.** Six Slurm submissions on `savio4_htc`, none on n0035: 41251335–37, 41251499 and 41251808, plus a first attempt of job 4 that failed (see "Process").

**Isolation.**
- Each side was a `git clone --shared` of the mirror at the full SHA, with the import identity asserted.
- Each side had a private `/local` root. `LSMS_DATA_DIR`, `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG` were exported before `bin/savio_venv.sh mount`, and `data_root()` was asserted private in every script.
- Sources were read only through `get_data_file` / `get_dataframe` into the private L1, with no `dvc` CLI.
- BLAS threads were pinned to 1, with `LSMS_BUILD_WORKERS=1` and `--dist=loadfile`.

**Clean-tree checks.**
- After jobs 1, 2, 3 and 5: 0 in-tree parquets and a clean `git status`.
- In-tree raw sources were checked after jobs 3 and 5: 0.
- Job 4 only read pickles and was not checked.

**Outputs.** `scripts/`, `logs/`, `results/` and `frames/` are in this directory. No household ids appear below.

## Idiosyncrasies relied on (`Niger/_/CONTENTS.org`, `Mali/_/CONTENTS.org`)

- **Niger 2021-22 s16d repeats.** The file repeats each crop-level record once per s16c harvest LINE. Exact repeats are dropped (3,329), and a sale goes only on a crop's single served line (#1360), otherwise on one Farm-level row (#1457).
- **2014-15 AS2E2** is one row per household and crop.
- **2011-12 rainy 2E** asks the sale per harvest line.
- **3E (2011-12 dry season)** is per cycle. Quantity and `Quantity_kg` are multiplied by `as03eq06`; the sale is not.
  - Ruled to be "first cycle". The Stata label of `as03eq11a` says "en moyenne pour chaque cycle".
  - One byte-identical row is dropped.
- **Codes by module.** Each module has its own code width, all unlabelled:
  - rainy 999999 / 9999999;
  - 3E 99999;
  - 2014-15 9999 / 9999999.
  - The rainy sold kg uses a narrower 9999.
- **Mali `Farm-level` means two things:**
  - EAC-I tree rows in 2014-15 and 2017-18, which carry 0 sold kg (measured);
  - 2021-22 sale rows: 70, every one with its kg. 40 of those 70 share their row with a `harvest-from-dispositions` total.
- **TODO/WAITING/LOGBOOK sweep.** Neither file has any such entry beyond the `#+TODO:` declaration. Nothing found contradicts `CONTENTS.org`.

## Findings

**F1. Ruling (c) says "the same 'estimate' key"; the candidate mints sibling keys.**
- The keys are `Niger::crop_production::sold-kg-estimate` and `Mali::crop_production::sold-kg-estimate`.
- **Row count matches the ruling:** 1,818 + 1,136 = 2,954 rows. They are exactly the rows with a `Quantity_sold_kg`, the 553 + 70 Farm-level rows included, and no other wave carries the key.
- **Disclosed** in both `CONTENTS.org` and both `derivations.yml` files.
- **Evidence for the sibling reading.** Two landed tests pin `survey-kg-estimate` to exactly the `Quantity_kg` rows:
  - `tests/test_niger_crop_survey_kg.py::test_the_estimate_key_is_exactly_where_2021_22_has_a_total`
  - `tests/test_mali_crop_survey_kg.py::test_the_key_is_exactly_where_a_value_is`

  Both assert `keyed == Quantity_kg.notna()`. Both also join that key's `inputs` to the s16c harvest rows.
- **What the literal reading would cost.** Reusing the key would break that landed contract. It would also make a "is `Quantity_kg` an estimate?" containment test fire on sale-only Farm-level rows. Only the wording differs.

**F2. Site Q's `Quantity_sold_kg` message prints a kilogram total beside the harvest unit. Non-blocking.**
- **The ladder.** The column is screened on the generic `(t, u, j)` ladder, where `u` is the harvest unit. `Quantity_sold` is screened on `Unit_sold` (GH #1569: "NEVER substitute harvest u").
- **The ladder does not change what is named** (recomputed three ways):

  | ladder | Mali named | Niger named |
  |---|---|---|
  | `(t, harvest u, j)`, as served | 3 high, 1 low | 0 |
  | `(t, Unit_sold, j)` | the same 4 | 0 |
  | `(t, j)` | the same 4 | 1 low |

  No positive row goes unjudged under any of them.
- **The message is misleading.** It reads `Arachide u=Sac (100 Kg) 1,150,000 (x511.1 ... same wave, unit and crop ...)`, which looks like 1,150,000 sacks.
  - Suggested fix: key the cell on `(t, j)` or `Unit_sold`, or print the unit as `harvest_u`.
- **Report-only holds:**
  - The served frames are identical apart from the new column.
  - `LSMS_QUANTITY_STRICT=1` already raised on both countries at base, on `Quantity`, so the strict behaviour does not change.
- **The low tail is the author's addition**, made "as `Quantity_kg`". Ruling (d) says only "screens". Its single hit is a 5 kg cotton sale, plausibly real.

**F3. Masks not explicitly ruled. Low risk; maintainer to note.**
- **Zero rule:** "0 beside a positive `Quantity_sold` → NA". 66 values (3E 36, 2014-15 26, Mali 4).
  - It mirrors the landed harvest-kg rule and is physically sound: all 36 3E zeros sit beside a positive quantity and a positive value, and 23 of the 26 in 2014-15 sit beside a positive value.
  - No 0 is served.
- **3E 99999 (39)** is that module's documented code; 33 of the 39 sit beside `as03eq09c` = 99999.
- **2014-15 9999 (11)** is that wave's quantity code and the column maximum. The 11 are:
  - 7 all-code lines (`AS02EQ12A` 9999, `AS02EQ13` 9999999, unit `Manquant`);
  - 4 positive sack sales (2 Sac 100, 2 Sac 50), which are the only masked values with neither a ruling nor an all-code line. Measured:
    - they would imply 83–5,000 kg per sack;
    - 3 of 3 that sit beside a valid crop harvest kg (`AS02EQ110E`) exceed it;
    - the fourth sits beside `AS02EQ110E` = 9999.
- **Rainy 9999 (ruled (b)), re-measured:**
  - 10 at exactly 9999, 13 in 5,000–9,998, 1 above (max 11,000);
  - of the 10, 3 carry the quantity code and 2 the value code;
  - the 7 with a positive quantity are Tiya/Botte at 500–5,000 kg per unit, and all 7 exceed the line's harvest kg;
  - kg = quantity on 151 of 151 Kg sales.
  - **Not reproduced:** the author's "the one sold in Kg would break kg = quantity". In my count that row has no positive sold quantity.

**F4. Residual per-unit or count answers under the "totals" reading. Recommend disclosure.**
- **Per-unit answers.** Sack-unit sales of 5 or more sacks whose kg is within 25% of ONE sack's mass (the forms' "poids d'UNE UML" reading):
  - Niger: 12 in 2011-12, 6 + 2 in 2014-15, 5 in 2021-22;
  - Mali: 9 Sac 100, 1 Sac 25, 5 Sac 50.
- **Count answers.** kg equals the sack count: Mali 24 Sac 100 and 6 Sac 50; Niger 4.
- **3E is the noisiest module** (served rows, after masks):
  - Sac 100: median 80 kg per sack, p10 8, log-log slope 0.67;
  - kg = quantity on 4 of 9 Kg sales.
- **Consequence.** On these rows `Value_sold / Quantity_sold_kg` is off by about `Quantity_sold` times, and Site Q does not catch them.
- **Disclosure today.** Mali `CONTENTS.org` mentions the count rows only qualitatively; Niger's mentions neither pattern.
- Served as recorded is what the ruling asks for, so this is not a defect.

**Pre-existing, not introduced.** 23 of the 242 rainy 2011-12 (household, crop) pairs with two or more sale lines repeat an identical (quantity, kg, value) triple on every line.
- It may be a crop-level answer copied per line, the s16d pattern.
- The new column inherits whatever `Quantity_sold` and `Value_sold` already do there.
- Worth an issue.

**Process deviations (mine).**
- On n0035 I ran one empty `python3 -` heredoc (a no-op: no import, no data).
- On n0035 I also used `sed` and one `perl -0pi` in-place edit on my own scripts in this directory. No repository file was edited.
- The first submission of job 4 failed on my script: unpickling needs the package on `PYTHONPATH`. I deleted its log and resubmitted (41251499).

## Answers to the questions

1. **Source fields.** Each wave reads the right field, as a total, on the right row.
   - **Labels:** `as02eq12c` "quantité en kg" beside `as02eq12a` "quantité en uml ... vendue"; `as03eq11c` "quantité en kgs vendue"; `AS02EQ12C` "Quantité en kg"; Niger and Mali `s16dq05c` "16D.05c. Estimation Quantité totale UML en kg". None of the kg columns has a value-label set.
   - **Total controls:**
     - kg = quantity on Kg sales: rainy 151/151, 2014-15 20/22, Niger 2021 45/48, Mali 495/495;
     - median kg per Sac 100 = 100 and per Sac 50 = 50 (56 in 2014-15); Tiya 2.5–2.7;
     - log-log slopes 0.89–1.07 on sacks, Tiya and Kg in the rainy, 2014-15 and Niger 2021 files.
     - **Exceptions:** Mali Sac 100 0.70, 3E 0.6–0.8, rainy Panier 0.75, 2014-15 Botte 0.67 and Unité 0.80, Niger 2021 panier 0.13 (n = 12). See F4.
   - **Attachment.** The multiset of (sold quantity, sold kg, value) on served rows equals the masked source records exactly: rainy 1,286; 3E 356; Niger 2021 1,818; Mali 1,136.
     - 2014-15 has 1,051 against 1,052. The extra source record is a non-sale 0 kg record whose crop has no single served line, so it rides on no row.
     - A non-sale record on a one-line crop would ride onto its line. None with kg does.
2. **Repeated lines.** Each kilogram is counted once.
   - **Niger s16d.** After the 3,329 exact repeats are dropped, no (household, crop) has more than one row with `s16dq05c` (0 pairs), and no multi-row pair carries kg.
   - **Mali and 2014-15.** Mali s16d is 5,068 rows on 5,068 keys; 2014-15 AS2E2 is 5,226 on 5,226.
   - **Served rows.** No (t, i, crop) carries kg on more than one row in 2014-15, 2021-22 or Mali.
   - **2011-12** is line-grain by design: 272 multi-line crops (see the pre-existing note).
3. **Masks and codes.** Justified (F3).
   - **The seven (a) records reproduce exactly.**
     - 8,740 kg; flag Non 6 / Oui 1.
     - Quantity and value are NA on 6 and both 0 on the Oui record. None has a code-cleared quantity or value, so none is a sale of unknown size.
     - All are single-cycle with no sold unit, and 6 equal the line's harvest kg.
   - **The Oui record.** Masking it matches the ruling, whose own count is "the seven ... (own sold flag No on 6)".
   - **3E window.**
     - On 31 multi-cycle sales, the sold kg is at most the per-cycle harvest kg on 28 of 29 and never between the per-cycle and season totals.
     - The sold quantity is at most the per-cycle harvest on 28 of 28.
     - So not multiplying by cycles is right. First versus average cycle cannot be tested.
4. **Estimate key.** It covers exactly the 2,954 rows with a value.
   - **`derivation_inputs` joins 1:1** on (t, i, crop): 1,818/1,818 and 1,136/1,136, with 0 value mismatches against `s16dq05c`.
   - **The no-sale inputs** join on 7/7. The served kg on those rows is NA, and the inputs sum to 8,740.
   - **`hh-crop-sale-single-row` inputs** still join (583, 1,265, 1,070) and now carry the kg columns, with 0 mismatches.
   - **`attrs['derivations']`** counts are correct and survive `Feature()`.
5. **Site Q.** Report-only, both tails as `Quantity_kg`, on the harvest-`u` ladder (F2).
   - **`quantity_audit.py` is outside every fingerprint:**

     | probe | country hashes | wave hashes | `build_transforms_fingerprint` |
     |---|---|---|---|
     | base + the candidate's `quantity_audit.py` | 0/544 | 0/2,258 | 0/41 |
     | base + no-ops in 8 of its functions | 0/544 | 0/2,258 | 0/41 |
     | positive control: no-op in `df_data_grabber` | 544/544 | 2,228/2,258 | 41/41 |

   - **Candidate against base:** Niger 24/24 and Mali 23/23 country hashes, 96 and 92 wave hashes, no other country, 0 `build_transforms_fingerprint`. The author reports 25/24 of 555; my enumeration differs, but in both every Niger and Mali table moves and no other does.
6. **Unchanged data** (cold, separate private roots).
   - **`crop_production`.** Rows (49,042; 50,752) and index are equal. `assert_frame_equal` passes with `Quantity_sold_kg` and `Derivation` dropped, and the column order is otherwise base's.
   - **`Derivation`** changes only by additions: Niger 1,818 `sold-kg-estimate` + 7 `no-sale`; Mali 1,136. No key is removed.
   - **Everything else is frame-equal:** all 46 other Niger and Mali tables (derived food and roster tables included), Uganda `crop_production`, and `harvest_kg` / `gross_crop_revenue` for all three countries.
   - **`Feature('crop_production')(['Niger','Mali','Uganda'])`** assembles to 233,958 rows, equal apart from the new column and keys. The column is `Float64`; Uganda's is all NA.
   - **Make backend** (cold root): `assert_frame_equal` to the default for both countries, attrs included.
7. **Tests.** Two targeted runs, 68 files, each on both sides. **The full suite was not run.**
   - Run 1 (42 files: Niger/Mali crop, Site Q, derivations, schema, hashes):
     - candidate 2,383 passed, 0 failed, 3 skipped;
     - base 2,343 passed, with the same 3 warm-cache skips.
   - Run 2 (26 more files from both grep lists: the producer/compiler, concatenator, extension-key, EAC-I namespace and yield-area files, `test_label_selection`, `test_gh842`, Nigeria crop and others):
     - 915 passed, 0 failed, 85 skipped on both sides;
     - every skip is a warm-cache gate on Nigeria or another country, none in a Niger or Mali file.
   - Every `requires_s3` Niger and Mali test ran.
   - **Nothing was materially weakened.** These are narrow re-pins forced by the new key:
     - `test_gh1462` gained the column;
     - `test_gh824`'s `.eq(TOTAL)` became containment;
     - Farm-level "`Derivation` isna" became "subset of {SOLD_KG}".
   - **Gap the new tests leave.** Their oracles check counts and sums. The triple, join and repeat checks above close that gap.
