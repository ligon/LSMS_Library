# Actual release-target outcome review

APPROVE the reported successful `make release` result, with the coverage limits below retained. This review does not establish a remote tag, GitHub Release, PyPI upload or installed-release smoke result.

The actual target ran at `cb44e3522a2426ba03431ca9a218cc63fe03f6c9`, exited zero, and finished at 2026-10-15 15:37:21 UTC. Its terminal summary is **7,376 passed, 314 skipped, nine xfailed, one existing non-strict xpassed, zero failures/errors**, with 968 warnings in 1,024.34 seconds. The outer make invocation took 1,028.67 seconds. The private XML contains exactly 7,700 cases, including 314 `pytest.skip` and nine `pytest.xfail` elements; its remaining 7,377 cases include the non-strict XPASS, whose separate count comes from the terminal summary.

Independently verified the complete log SHA256 `99012c6a69034ed9425023785bead72464b4ce992f91ff456f01498d8c395711`, XML SHA256 `ee54438d87a10c6fe83af30e69ab31e03b84b98a3cb025b9af6542e5087f86f2`, literal terminal summary and local annotated tag object `d4ff03d28ab417c5d98f8e1e7c94d1261b8331bc`, peeled to the exact master source above. Raw logs/XML remain private.

## Actual skips

| Observed category | Count |
|---|---:|
| Structural test: raw cache absent at collection | 150 |
| No declared index (22) or declared columns (23) | 45 |
| Explicit Armenia/Nepal microdata-unavailable messages | 20 |
| Sample prerequisites/comparisons, detailed below | 69 |
| External Uganda replication directory unavailable | 15 |
| Legacy derived-table parquet guards | 10 |
| Parameter combination belongs to the other country | 2 |
| Optional `LSMS_EXPECTED_ROOT` identity assertion not enabled | 1 |
| GhanaLSS comparison has no pinned totals | 1 |
| Guyana parquet read retry exhausted; separately covered below | 1 |
| **Total** | **314** |

**Zero skip messages came from credential guards.** This classification does not call every skip a licensed-source gap or a successful data assertion.

The 150 structural cache gates comprise 66 assertions on 11 country/table pairs also recorded as errors in the prior corpus, 52 panel-ID parquet assertions and 32 derived-food parquet assertions. The cache messages establish test preconditions, not new source failures. In particular, absent persisted derived parquets can coexist with successful API results. The separate ten legacy guards cover derived Uganda household/food tables and panel-attrition checks; they retain the coverage debt described in #1006. The 45 undeclared-schema skips likewise reflect absent declarations, not proven absence of survey data.

The 69 sample skips are: four with no non-null weights; four with no weight column; seven needing weight plus Rural; ten needing both weight columns; 15 with fewer than two comparable expansion-weight waves; 21 with no identified refreshment households; and eight emitting the generic “no raw sample parquet to read (cold data root?)” message.

Those eight generic messages are two each for China, Kazakhstan, Pakistan and Peru. Saved manifest evidence contains all four raw sample caches with a `t` index and no `weight` column. `tests/test_sample.py:79-111` returns `None` for missing weight as well as absent file, missing `t`, or no usable weights; callers at lines 384 and 419 use the same message. Thus the message does not establish a cold/missing cache, and the recorded weight-column absence is relevant supporting evidence. No per-test raw snapshot identifies the exact branch taken. This review does not re-adjudicate scientific source-weight availability or the previously recorded Pakistan provenance contradiction.

`make-release-outcomes.json` gives all exact reason-category counts, per-reason message hashes/prefixes and parameter-free test-function names. The standalone classifier fails on an unknown reason rather than silently assigning it. Reproduction requires the private XML, saved summary and archived aggregate corpus TSV; it does not call a survey API.

## Expected failures

Four xfails are the existing `test_has_weight_column` expectations for China, Kazakhstan, Pakistan and Peru. Their messages describe source-weight limitations; recording these expected outcomes is not an independent scientific verification of that claim.

The additional five are the existing #602 tenure-vocabulary defects in Albania, Cambodia, Kosovo, Tajikistan and Timor-Leste `plot_features`. The exact `TestFeatureSanity` guard only xfails these known countries when all diagnostic errors are declared-spelling errors (`tests/test_table_structure.py:309-329`). They remain actual defects and agree with the earlier Feature audit findings. The earlier credentialed CI's four xfails were the sample-weight expectations; this release run has those same four plus the five known tenure failures. None is relabeled as a pass or a newly approved scientific mapping.

## Guyana skipped assertion and separate follow-up

Original skipped node: `tests/test_table_structure.py::TestFeatureSanity::test_feature_is_sane[Guyana/interview_date]`. `_read_or_skip` at lines 164-196 catches either `FileNotFoundError` or `ArrowInvalid`, retries after 0.25 seconds, and emits the “vanished/rewritten mid-run” message after the second exception. The XML does not retain which exception occurred or identify the causal writer. A concurrent cold-build interaction is plausible because the executed Guyana regression clears/rebuilds all seven country and wave tables, but this is not proven attribution.

The parent subsequently ran that exact existing node alone at the release source on 13 physically distinct copied Guyana files. It **passed once, with zero skips**, in 9.11 seconds. I verified its one-case unskipped XML, log/XML digests, matching source SHA and 13-copy identity proof. The runner checks original and copy SHA/size/mtime/device/inode equality after the test; its successful result records both preserved. This covers the formerly skipped warm-cache sanity assertion without changing the original **314** skip count or claiming a newly identified race cause, cold-build result or scientific validation.

Guyana CONTENTS was read first, including its complete known-issues section (there are no separate task LOGBOOK entries). Relevant idiosyncrasies are household key `(ED,SN,HH)`, cluster key `(ED,SN)` and script fallback despite no country Makefile. Its warning that “A warm-cache check would pass vacuously” applies to the separate cold-collapse regression; the follow-up is explicitly the ordinary cached sanity assertion. The existing unresolved source count “154 EDs” versus recorded 130 ED numbers/168 segments remains unchanged and does not explain this skip.

Evidence: parent `make-release-summary.json`; private `make-release-run/release-tests.xml`; `guyana-sanity-followup/{result.json,copy-proof.json}` and `verify_guyana_skip.py`. `make-release-evidence-review.json` pins all inspected source/evidence digests and follow-up results. This reviewer performed no tests, builds, survey/cache reads, Git mutations or GitHub writes.
