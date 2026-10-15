**Corrected 2026-10-15: both countries' warm-read defects remain open.** The October 14 narrowing said "both halves no longer reproduce on development," citing a successful Mali cold build and Tanzania's complete raw parquet. Those observations did not establish warm API completeness. Both countries' previously reported missing waves reproduce on development `3914d8804`; their parsing mechanism is now isolated. No production fix has been applied.

| Country / waves | Raw cache rows | Cold API | Warm API | Newly nulled valid dates |
|---|---:|---:|---:|---:|
| Mali 2014-15 | 3,804 | 3,804 | 3,804 | 0 |
| Mali 2017-18 | 8,390 | 8,390 | 8,390 | 0 |
| Mali 2018-19 | 6,602 | 6,602 | 0 | 6,602 |
| Mali 2021-22 | 6,143 | 6,143 | 0 | 6,143 |
| Tanzania 2008-09 through 2014-15 | 16,540 | 16,534 | 16,534 | 0 |
| Tanzania 2019-20 | 1,184 | 1,184 | 0 | 1,184 |
| Tanzania 2020-21 | 4,709 | 4,709 | 0 | 4,709 |

Mali totals: **24,939 raw/cold → 12,194 warm**, losing 12,745 valid dates. Tanzania totals: **22,433 raw, 22,427 cold → 16,534 warm**, losing 5,893 valid dates. Tanzania's six originally missing dates (2/1/0/3 in its four older waves) are separate from the warm-only loss.

Both complete, fresh raw caches mix timestamp representations. Mali's EAC-I waves use `YYYY-MM-DD HH:MM:SS` and its EHCVM waves use `YYYY-MM-DD`. Tanzania's older UPD waves use space-separated timestamps while its two later waves use ISO `T` timestamps. Whole-column declared-dtype coercion with `pd.to_datetime(..., errors='coerce')` infers the first format under the recorded pandas 3.0.6 environment, nulling all valid dates from the four named waves. `_finalize_result` subsequently deletes those all-null rows.

Pure in-memory replay accounts for the exact difference, including content fingerprints. Normalization preserves every row in both countries; Tanzania's preloaded-sample `v` join also preserves all rows and original null counts. Default parsing plus the null filter exactly reproduces each saved warm result. Per-wave and whole-column `format='mixed'` parsing agree and, with the same null filter, exactly reproduce each saved cold result. These are diagnostic counterfactuals, not an adopted library-wide parsing policy.

The [September 9 Mali comment](https://github.com/ligon/LSMS_Library/issues/798#issuecomment-5600421916) correctly found zero NaT when parsing each homogeneous wave, but its exclusion of dtype coercion did not test the mixed whole column. The proposed fabricated-visit mechanism is unnecessary to explain either measured loss. Tanzania's disjoint Extended/Refresh sub-panels and UPHI back-casting remain documented survey-design facts; neither causes this loss of valid dates. Do not change visit keys, household grain or the reducer as a side effect of a parsing repair.

This is pre-existing behavior. The original issue names the same four dropped waves; the September 9 comment records Mali's exact current raw counts and warm loss. Six relevant normalization/join/dtype/finalization/parquet-writing function bodies are unchanged across v0.14.0, pre-train `452302ad`, corpus candidate `7fa05179` and final `3914d8804`; Tanzania's UPD interview-date script is unchanged too. This is source equivalence plus earlier observed missing waves, not a historical rebuild. Tanzania's exact earlier counts and historical dependency environments remain unverified.

A bounded workaround was verified using ten ordinary single-wave calls on a separate complete warm-cache copy:

```python
country = ll.Country('Mali', preload_panel_ids=False)  # also verified for Tanzania
dates = pd.concat([country.interview_date(waves=[wave])
                   for wave in country.waves])
```

This returned exactly the cold values, index, row order and dtypes: 24,939 Mali rows and 22,427 Tanzania rows. All 140 original/copied parquet identities stayed unchanged; no cache bypass was used. This guidance is limited to the tested complete-cache/source/environment. The ordinary all-wave call remains defective.

Remaining work:

- [ ] Repair and regression-test both countries' complete-cold/incomplete-warm date paths with explicit date-interpretation semantics. Cover all three string representations, preserve valid dates and intended household grain, and distinguish genuine missing dates from coerced valid dates. Do not close this from cold-only success or complete raw-cache metadata.
- [ ] Refresh/promote `.coder/coverage/latest.csv` on the final source, recording known failures and distinguishing raw-cache coverage from served API coverage. Its old `12c0dec2b` snapshot remains stale; a truthful refresh need not wait for all defects to be repaired.
- [ ] Correct Mali `_/data_scheme.yml:69-73`: `groupby.first()` is no longer the signed reducer (#871), and the claim that EHCVM revisits each household across multiple vagues conflicts with CONTENTS' "each grappe ... exactly one vague". Reconcile the documentation without silently changing keys or aggregation policy. Record the date-format/warm-read limitation in both countries' CONTENTS.

The original Guatemala/Panama nutrition cells now grade undeclared/absent; they are not outstanding implementation requests here. The original report and September 9 comment remain historical evidence.

Release disposition: keep these material limitations explicit and this issue open. Both raw caches and their full-wave footers were verified unchanged and can remain promotion candidates subject to the other gates. That does not establish complete warm API output, universal cold/warm equality, or issue resolution.
