**Corrected 2026-10-15: Mali's warm-read defect remains open.** The October 14 narrowing said "both halves no longer reproduce on development," citing a successful Mali cold build and Tanzania's complete raw parquet. Those observations did not establish warm API completeness. The Mali loss reported in the September 9 comment reproduces on development `3914d8804`; its mechanism is now isolated. No production fix has been applied.

| Mali wave | Complete raw cache / cold API | Warm API |
|---|---:|---:|
| 2014-15 EAC-I | 3,804 | 3,804 |
| 2017-18 EAC-I | 8,390 | 8,390 |
| 2018-19 EHCVM | 6,602 | 0 |
| 2021-22 EHCVM | 6,143 | 0 |
| Total | 24,939 | 12,194 |

Mali combines two survey families. The complete, fresh raw country cache stores EAC-I dates as `YYYY-MM-DD HH:MM:SS` and EHCVM dates as `YYYY-MM-DD`. Index normalization preserves every row. Whole-column declared-dtype coercion with `pd.to_datetime(..., errors='coerce')` infers the first format under the recorded pandas 3.0.6 environment and nulls exactly the 12,745 EHCVM dates. `_finalize_result` then drops those all-null rows. Pure in-memory replay reproduces the saved warm content fingerprint exactly (`9285bfbf9190053270d4f4d20b4f8ec4b92e52610c4e5637559003c63196aa94`).

Per-wave parsing and whole-column `format='mixed'` agree and recover all 24,939 rows with exactly the saved cold fingerprint (`2d4fefbce6238f216a09e3042abaec82c8ed3aaeab330a630564f471ffeaf299`). These are diagnostic counterfactuals, not an adopted library-wide parsing policy. The September 9 comment's zero-NaT result for each wave was true, but it did not test the mixed whole column; its exclusion of dtype coercion was therefore incorrect. The proposed fabricated-visit mechanism is unnecessary to explain this measured loss. Do not alter the visit key or aggregation contract to repair a parsing failure.

This is pre-existing behavior: the [September 9 owner comment](https://github.com/ligon/LSMS_Library/issues/798#issuecomment-5600421916) reports the same counts and warm loss under `eecd6305` and `d2729e81`. The relevant dtype, finalization and parquet-writing function bodies are unchanged across v0.14.0, pre-train `452302ad`, corpus candidate `7fa05179` and final `3914d8804`. That is source equivalence plus earlier observed behavior, not a claim to have rebuilt every historical dependency environment.

Remaining work:

- [ ] Repair and regression-test Mali's complete-cold/incomplete-warm date path with explicit date-interpretation semantics. Cover the actual mixed string representations and preserve valid dates, intended household grain and unchanged source rows. Do not close this from a cold-only success.
- [ ] Independently measure Tanzania `interview_date` 2019-20 and 2020-21 through the served cold and warm API. A six-wave raw parquet with a current hash does not resolve the original dropped-wave report. Do not infer that Mali's mechanism also explains Tanzania.
- [ ] Refresh/promote `.coder/coverage/latest.csv` on the final source, recording known failures and distinguishing raw-cache coverage from served API coverage. Its old `12c0dec2b` snapshot remains stale; a truthful refresh need not wait for all defects to be repaired.
- [ ] Correct Mali `_/data_scheme.yml:69-73`: `groupby.first()` is no longer the signed reducer (#871), and the claim that EHCVM revisits each household across multiple vagues conflicts with CONTENTS' "each grappe ... exactly one vague". Resolve the documentation against the country contract without silently changing keys or aggregation policy. Record the date-format/warm-read limitation in CONTENTS.

The original Guatemala/Panama nutrition cells now grade undeclared/absent; they are not an outstanding implementation request in this issue. The original report and September 9 comment remain historical evidence.

Release disposition: retain this material Mali limitation explicitly. The raw cache retains all 24,939 rows and its four-wave manifest and was verified unchanged; it can remain a cache-promotion candidate subject to the other release gates. That is not a claim that warm Mali API output is complete, that cold/warm results universally agree, or that this issue is resolved.
