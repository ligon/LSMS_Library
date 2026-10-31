# #1254: keep median prices within their reported wave

Scientific/source base f952a32e646bcebe94b4c400ea30edef02b172bb. Implements
[maintainer ruling6367366848](https://github.com/ligon/LSMS_Library/issues/1254#issuecomment-6367366848):
a carried t index level or column participates in all geographic/national
median grouping by default. pool_waves=True explicitly disables automatic t
inclusion; an explicitly requested t key still groups by wave. Existing resolver
index-over-column priority and duplicate-name refusal remain. No wave metadata,
unit/weight/threshold/missing-answer, sample, currency conversion or source-table
policy changes.

Six invented baseline cases reproduce pooled505 where separate wave prices10
and1000 were expected, covering t index/column and geographic/national fallback.
After the fix,202 selected synthetic tests pass with eight data cases deliberately
deselected. Later test-only strengthening adds custom-key cross-wave perturbation
invariance and default duplicate-t coverage: the final21 new cases plus the
strengthened existing no-transformations-in-build-closure guard all pass (22).
Product code is unchanged between the202 and22 runs. Existing sold-unit,
weighted-median and internal own-production `_t/_j/_u` behavior stays covered.
No source data or Country/Feature/cache acceptance is asserted.

Both approved Python3.11.6 fresh metadata processes use explicit checkout import
identity, private data/config roots, numeric threads1 and one CPU. Baseline uses
90f5eb4 worktree source; its only delta to f952 is three Uganda documentation
files. All four registered build table scopes,16 registered entry points and
framework-import fingerprint over747 tracked country .py paths match candidate.
The median method is not reachable from registered build closures. No direct L2
country/wave cache hashes were called; these are the actual fingerprint input
components, not an invented all-cache census. Baseline/candidate metadata probes
complete in5.227/10.874 seconds with zero denials. This read-path API change has
zero measured component-hash cost; no data re-warm was needed or attempted.

Original tests/metadata probes deny payload-format opens, network and unapproved
subprocesses. The first import-only attempt stopped on libmagic's system-library
lookup before any test; its log is retained. Only exact original import-time
`/sbin/ldconfig -p` was then permitted, with later subprocesses still denied.
Successful baseline-red/candidate/metadata runs record no denial. All selected
runs are bounded120+5 seconds. The metadata-only API audit enumerates15 skills.

External evidence is retained under /local/job40561868/gh1254-wave-valuations-v1:
baseline-red.log (import refusal), baseline-red-v2.log (six genuine regression
failures), candidate-tests-v1.log (202PASS), candidate-tests-v2.log (21PASS before
final test-only refinement), candidate-tests-final.log (22PASS), fresh baseline/
candidate fingerprints and FINGERPRINT-COMPARISON.json. No private payload,
credentials or scientific witness values are included in this checkpoint.
