# Prior-art ledger — GH #1748 static sample coverage locators

Search: ripgrep, Git, current CI and frozen PR checks. Inherits STANDING.md
and the 1294 and 932 CI-output ledgers. The #1764 cache fix is independent.

## §1 Coverage and purpose

The credentialed full-data suite reports a recurring bare locator for
`test_covers_all_waves` with no country or wave. Add checked-in diagnostic
test functions for the 46 current public ISA country-wave labels. The
existing dynamic coverage requirement, including non-ISA countries and new
waves, stays in place. No new log channel, case-property parser or permission.

## §2 Reuse

- `tests/test_sample.py` supplies the existing expected-wave exemptions,
  non-availability/build-failure skips and per-process sample cache.
- `scripts/private_ci_check.py` already emits AST-verified, committed test
  function names. Leave its fixed revision, capture and counts-only fallback.
- `.github/workflows/pr-data-validation.yml` places one test file on one
  xdist worker, so the named checks see the same already-read sample.
  The test topology and loader outside this file are not changed.

## §3 Invariants

Named cases are additive diagnostics. They never replace the dynamic check,
  synthesize data, change wave expectations, infer a cache/read stage, or
  expose JUnit parameters, assertion messages, private paths, table values or traces.
  A retired or exempt wave is skipped, not added to the expected set. A new
  wave without a named probe remains covered by the original assertion.
  No new source read beyond the existing cached sample, CI config change,
  sidecar, or fingerprint change.

## §4 Verification plan

Use invented samples for one missing, all present, exempt and newly declared
waves. Prove cache reuse and run the existing private-wrapper controls. Read
the current collection and the named definitions from their committed source,
  then verify a real subprocess only prints static locators on forced failure.

## §5 Verified locally

All 46 literal source names and operands independently match the public
logical-wave declarations on the reviewed base. The coverage test still
uses its original dynamic set differences and shared data gates. Source-free
sample/CI wrapper regression checks: 90 passed under two workers. A proved
cause for any already-completed hosted failure is outside this change.
