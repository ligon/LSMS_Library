**Delta verdict: APPROVE.** Candidate `79739f8162b706b72bbc54dffe739e855b154f80` (previous `0d62f49df61bb43190275aaa3a2520f724eed6af`, base `f138a3d61c203f892148592675a7ef72e160a322`). Every fix does what it claims, the hash census is still 0, and the delta adds no new defect. I re-ran everything in burst job 41278035; outputs are in `reports/strata-redteam/out/j7/`.

**Hash census:** 0 moved on every kind, from base and from `0d62f49df`: `build_transforms_fingerprint` 0/42, framework imports 0/38, `Country._table_cache_hash` 0/555, `Wave._input_hash` 0/2,321, no errors.

**Each fix:**
- **F1.** All four places now say the Mali 2014-15 one-wave column shape depends on cache state, the same on base, and point readers to `.get('strata')`: the `absent` comment, the delivered test's comment, Mali `CONTENTS.org` and R3. R3 is now a pre-existing build/read-composition defect, which matches my job 41276936.
- **F2.** A key that is not exactly `format_key(*parse_key(key))` is now refused. My earlier probe, a padded `' Mali::sample::region-milieu-stratum '`, is refused with the new message, and four new cases are tested.
- **F4.** The new static test passes on all 7 native records, and its negative controls (Burkina 2014, Malawi 2010-11, Mali with a wrong variable or file) pass. On a copied config tree I planted 5 defects of my own; it caught 4:
  - a relabelling `strata` formatter in a wave's `mapping.py` (Niger 2011-12);
  - a `mapping:` added to the declaration (Uganda 2009-10);
  - a sub-df hook that writes `strata` (Niger 2014-15 `df_strate`);
  - a relabelling `strata` formatter in the country module (`uganda.py`).
  - **Missed:** a `sample` hook that hands the relabel to a helper function (Mali 2017-18), because the check reads only the hook's own body.
- **F5.** The pending test now requires that nothing is tagged, so a half-tagged pending country fails. The GNB proposal now covers 2018-19 and 2021-22 and is written as blocks to merge, with the duplicate-key warning in the header. Benin's proposal is written the same way.
- **F7.** The guide now uses `.get(wave)` and explains `KeyError` and the `unrecorded` placeholder. The GhanaLSS row reads "1991-92 onward", and GNB 2021-22 is noted.
- **F8.** The `holds` refusal names its own origin (GH #1577) and no longer cites the Uganda exemplar; a test checks this. A comment now says what `confidence` means on a `served_strata` block.
- **F9.** The Mali 2014-15 locator quotes CONTENTS verbatim, `deflator` qualifier included. The file loads cleanly on every country.
- **F10.** The Mali 2021-22 note states both counts and the household set each is counted over: CONTENTS' 429 of 6,133, and the served 415 of 5,011, in 36 clusters, all relabels.

**Tests:** the whole non-S3 suite gives 15,584 passed, 35 skipped, 4 xfailed, 0 failed. The Mali delivered tag tests (`requires_s3`) give 4 of 4 passed.

**Notes, none blocking:**
- The F4 check is shallow, as the missed helper case shows. It also assumes a fixed order between a wave's `{wave}.py` and `mapping.py`; the framework merges those two files through a set (`Wave.formatting_functions`), so their precedence is not fixed. Recording both in the test's docstring is enough.
- The GNB 2021-22 block ships at `confidence: medium`. At integration, measure that the key sits on every row with a stratum before raising it to high.
- Still for Sue and the user, unchanged: F11 (the end-of-file CONTENTS merge conflicts) and Q1–Q5, including whether to keep `constructed` (Q5).

No Python ran on n0035 in this round.
