**Delta verdict: APPROVE**

- Candidate: `9ca3518a35e6c688577283121baeb49a939b9e0f`. Previous candidate: `af81784630ef1ad650a705f4f9c85dde98912797`. Base: `f138a3d61c203f892148592675a7ef72e160a322`.

The worker implemented all three rulings correctly, and every claim I re-ran held. One test gap is worth closing before merge but does not block it (D1).

**The six points you asked about**

1. **Registration.**
   - **Two keys is right.** `records_for` treats an empty table slot as "applies to every table", and the summary then lists such a key at 0 rows in every Malawi table. Measured: each key appears only in its own table's `attrs['derivations']` (1,186 rows on `crop_production`, 1,153 on `plot_inputs`); `plot_features`, `food_acquired` and `sample` list neither. Recommend the user confirm two.
   - **Stamps land on exactly the relabelled rows.**
     - `crop_production`: keyed rows equal cowpea rows, 1,186 = 160 / 310 / 716, with no stray stamp and no missed row. 2010-11's 160 are the 157 #1523 harvest lines plus 3 Farm-level sales.
     - Every other key equals base's on every row. The 40 doubly-keyed rows (37 `sale-other-unit-single-row`, 3 `sale-basis-no-reference`) are sorted, unique and `+`-joined.
     - `plot_inputs`: keyed rows equal the cowpea seed rows, 1,153 = 155 / 301 / 697, none of them on Module H Farm-level rows.
   - **Inputs join on every route:**

     | rows | join key | crop_production | plot_inputs |
     |---|---|---|---|
     | plot rows | `(t, i, crop, plot_id)` | 157 / 302 / 697 | 155 / 301 / 697 |
     | Farm-level sales | `(t, i, crop)` | 3 / 8 / 19 | none |

     The inputs are unique on `(i, crop, plot_id)`. 2010-11 has 2 input lines and 2016-17 has 1 with no served seed row; these are the payload-less seed lines `seed-line-no-payload` deletes, which settles the 302-vs-301 gap.
   - **The 3 changed frames.** Of the 41 pre-existing `derivation_inputs` frames, 38 are equal, including every 2010-11 frame. The other 3 differ only in `crop`, Other → Cowpea: `sale-other-unit-single-row` 2016-17 (5 rows) and 2019-20 (26), `sale-basis-no-reference` 2019-20 (3). That is 34 rows, exactly the later waves' doubly-keyed rows (5 + 29). This is acceptable: it is what lets those inputs join the served rows, and they join fully on both arms.
2. **F2 slot pairing.** A code-48 line now reads `ag_d20_oth` only. Served `crop` moves are identical to `af8178463` (310 / 716 and 301 / 697); the new reason `text_not_for_code_48` fires on no line.
3. **The #1712 drop.**
   - **Absent and conditional.** The six are gone from the served frame and were blank on every base row. My synthetic probe through `_seed_acquisition_block` shows the drop is conditional: blank (NA or `''`) on every row of the file → dropped; filled on a row the carrier keeps → served.
   - **Edge case.** If a release fills one only on rows the carrier does not keep, the column comes back blank on every served row and fails `no_all_null_columns` again. That failure is loud, not silent (D2).
   - **`ag_h33` and `ag_h36b_oth`** are the same evidence class as the six: zero non-blank source rows in every file that carries them, stored as `''`. Leaving them is correct scope; dropping them needs a ruling.
   - **`ag_h42b_os` is different.** It has text in the source (54 rows in 2010-11 Full_Sample, 28 in the unread Panel file), but none on carried rows.
4. **Prose.** CONTENTS and the PR body now state F1–F4 and F8 correctly, with attribution, and agree with my measurements: `hh_x02`, the questionnaire and BID quotes, the exact slot pairing, the code-49 loss, Site Q 73 → 72, the one-helper fingerprint cause, and the unreconciled 2016-17 Panel denominator.
5. **Tests.**
   - The #1457 and sale-basis edits only set the label key aside from ladder counts; ladder keys are still matched exactly.
   - The new `test_gh1228` tests pin the stamps and joins.
   - Candidate: 766 passed, 0 skipped, 0 failed over 14 files. `test_table_structure -k Malawi`: 32 passed, 0 failed (102 skipped as uncached); the two base `plot_inputs` failures are gone.
6. **Hash census, base to candidate.**

   | kind | moved |
   |---|---|
   | `build_transforms_fingerprint` | 0 of 42 |
   | `framework_imports_fingerprint` | 1 of 38 (Malawi) |
   | table hashes | 26 of 555 (Malawi) |
   | wave hashes | 156 of 2,321 (Malawi) |

   The only new closure part is `local_tools._dvc_working_directory`. The canonical `data_info.yml` declaration moves no hash.

**Frames, cold, base against candidate**
- Row counts are unchanged.
- `crop_production` moves only `crop` (310 / 716) and `Derivation` (160 / 310 / 716). Its warnings are identical.
- `plot_inputs` moves only `crop` (301 / 697), gains `Derivation` and loses the six.
- `harvest_kg` (default and shipped) is identical row by row once cowpea is mapped back to Other. `seed_kg`, `nitrogen_kg` and `gross_crop_revenue` are frame-equal; `crop_diversity` moves on 7 and 52 households.

**Findings**
- **D1, should-fix (not blocking): no test pins the #1712 drop.** Nothing asserts that the six are absent or that the drop is conditional; `test_table_structure` catches only the all-null symptom, and only with a warm cache. Fix: a synthetic test along the lines of my probe (blank → dropped, filled → served).
- **D2, note:** the drop condition looks at every row of the file, not the carried rows; see point 3 for the edge case. Accept, or decide on carried rows.
- **D3, note:** the `plot_inputs` stamp starts from NA instead of any existing `Derivation` (`malawi.py:4749` at the candidate). Harmless today, since base `plot_inputs` has no `Derivation`, but a future upstream stamp would be overwritten. Union with the existing column if present.
- **D4, note:** `_assert_label_is_text_only` stops every Malawi `crop_production` / `plot_inputs` build if IHS6 code 50 is ever mapped to the cowpea label. It is disclosed; carry it into the 2024-25 residual issue.
- **D5, note:** the `test_gh1190` source pin now also exempts `assemble_plot_inputs`. The fixture README forbids refreshing the baseline, so this is the only option, but it narrows the pin further.
- **D6, note (cosmetic):** the `plot_inputs` entry's second assumption has basis `modelling-choice` although it only cross-references the `crop_production` entry, whose assumptions are questionnaire-based.

**What I did not run:** the other 24 Malawi tables, the make backend, warm or fresh-process reads, `Feature()`, and the worker's full 64-file test set.

**For the user:** confirm two keys. The other open questions are whether to drop `ag_h33` and `ag_h36b_oth` too, whether to name the 15 two-text plots whose code-48 text says cowpea, and whether to fix the code-49 loss.

Jobs: 41277200 (cold builds, delta checks, census, tests) and 41277454 (the corrected #1712 probe; my first probe misaligned on the test helper's index `[44]*n` and was discarded). Outputs are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi-redteam/out/job7/` and `out/job8/`, scripts in `scripts/`. I ran no Python on n0035 in this delta.
