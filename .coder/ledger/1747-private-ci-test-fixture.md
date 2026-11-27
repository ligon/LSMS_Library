# Prior-Art Ledger — #1747 private CI subprocess fixture

**Search tier used:** ripgrep + Git, checked-in wrapper and source-free tests,
shared alternate reproducer, and historical CI red-team report.

## §1 Task, restated

Verify the protected CI wrapper's actual subprocess opt-in without making a
mandatory named-result assertion depend on the product checkout's object
storage. Test the count-only timeout behavior separately.

## §2 Existing machinery

| symbol | path | role |
|---|---|---|
| `public_repo` | `tests/test_gh932_safe_ci_failure_locations.py` | Commits a known static test name in a disposable Git repo |
| `main`, `_source_head`, `_checked_in_tests` | `scripts/private_ci_check.py` | Resolve source identity from the executing wrapper checkout and verify each file at the fixed commit |
| `_git` | same | Two-second local Git budget with a count-only fallback on failure |
| #1294 / #932 ledgers | `.coder/ledger` | Private child streams and temporary XML, exact status, and only source-verified optional public locators |

## §3 Definitions & conventions

- Follow `STANDING.md §3` and the #1294/#932 CI ledgers. All child text,
  XML parameters and failure reasons stay private. The static locator is
  optional and must be admitted through the committed source proof.
- The test calls the exact real stdlib wrapper, not a replacement runner.
  It needs a predictable checked-in identity, not product Git history.

## §4 Invariants & assumptions

- No workflow, runner, timeout, source allowlist, JUnit, data or cache
  change. Preserve caller-outside-root execution and status 7, exact
  stdout, empty stderr, private sentinel suppression, and cleanup checks.
- Use a lone local Git fixture, as the neighboring source-identity tests do.
  A deliberately delayed product checkout is evidence of one failure path,
  not attribution of historical jobs without their timing logs.

## §5 Reuse decision

- Reuse `public_repo`; copy the exact wrapper into it and run the copied
  entrypoint from outside that repo. Do not bypass its credentialed CI
  source validation.
- Inject `subprocess.TimeoutExpired` separately for each Git lookup arm.
  Assert the ordinary count-only fallback without weakening the positive
  integration assertion.

## §6 Open questions for the human

- No policy choice is needed for this test-only change. Historical real-job
  timing labels are unavailable, so the specific cause remains unassigned.

### Phase 3 — verification

- Positive CLI check OK (§2–5). The exact development test passes against an
  ordinary shared alternate on this node. With a deliberate caller-only
  `ls-tree` delay over two seconds, it fails with an unmapped failure and
  retains private output handling. The revised test passes in the same
  caller with the same injected delay. Git telemetry confirms it consults
  only the small local fixture, with successful tree and blob reads.
- Timeout controls OK (§4–5). Four injected Git timeouts exercise revision,
  tree, blob-size and blob-data arms. Each produces counts only.
- The 163 focused tests passed under two loadfile workers with no sources or
  credentials. Workflow, wrapper and CI privacy scope are unchanged.
