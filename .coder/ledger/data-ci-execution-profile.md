# Prior-Art Ledger — credentialed CI execution profile

**Search tier used:** ripgrep + git floor. No production symbol changes; the
blast radius is the `data-tests` job in `.github/workflows/ci.yml` only.
Base: `47ce939afa1f253a5a76e0832620a3cbb6a4b817`.

## §1 Task, restated

Apply the repository's existing single-build-worker and BLAS thread limits to
credentialed CI, including its Uganda prerequisite build, and use the existing
600-second thread timeout and 25-test duration report. Preserve the sequential
full-suite execution needed to compare the historical #941 session-order case.
This does not diagnose any running job or change data, dependencies, assertions,
selection, credentials, skips, or production cache hashes.

## §2 Existing machinery

| machinery | anchor | evidence / reuse |
|---|---|---|
| Unit CI execution controls | `.github/workflows/ci.yml`, `jobs.unit-tests.steps[Run tests]` | Already uses all four env limits and the timeout/durations flags; reuse. Final candidate `4f1f044bb` unit job passed 3,276 tests. |
| Credentialed CI | `.github/workflows/ci.yml`, `jobs.data-tests` | Existing `make -j2`, sequential `pytest --tb=short`, credential setup, and dispatch condition stay intact. |
| Timeout dependency | `pyproject.toml:62`, `pytest-timeout = "^2.3.0"` | Already declared; verify local plugin and option availability, no dependency edits. |
| Sequential #941 probe | `slurm_logs/release_landing_2026-10-14/941-sequential.org` | Existing 451-pass / zero-skip fresh-root ordered run used one build worker, BLAS=1, and 600-second thread timeouts. |
| Historical CI marking analysis | `slurm_logs/release_landing_2026-10-14/907-coverage.org` if present; private `control/framework/validation-followup/907-REPORT.md` | Distinguish execution controls from test marking/selection; no marking edits here. |

## §3 Definitions & conventions in force

- `AGENTS.md` / `CLAUDE.md`, scrum-master addendum **3b**: suite recipe requires
  `LSMS_BUILD_WORKERS=1`; "Pin the BLAS thread count too — the worker count
  alone is not enough." The documented DVC handle inheritance and thread
  oversubscription hazards motivate consistent settings, not a diagnosis of
  any currently running Actions job.
- `STANDING.md` §4 remains authoritative for sanctioned IO, cache and path
  invariants. This task changes no country config or library code and invokes
  no DVC command.
- Sequential `pytest --tb=short` is the existing credentialed job's selection
  and order; preserve it to retain the #941 comparison boundary.

## §4 Invariants & assumptions

- No xdist added to `data-tests`; no new test filters, marks, skip logic,
  retries, `continue-on-error`, or exit-code suppression.
- Keep `Build Uganda tables` and its `make -j2`/30-minute step timeout unchanged.
  Job-level env ensures each build subprocess and the test process inherit
  single-build-worker and single-BLAS-thread controls.
- Reuse the unit job's existing 600-second **thread** timeout. An exceeded
  timeout is a real nonzero result and can end the pytest process; this is a
  diagnostic bound, not successful completion or an implicit skip.
- No claim that old jobs deadlocked, nor that #941 is explained or resolved.
  Existing unpinned jobs remain separate evidence.

## §5 Reuse decision

| behavior | decision | reason |
|---|---|---|
| Build/BLAS limits | reuse existing unit-job values at data-job scope | Both build and test steps need the documented limits. |
| Per-test timeout/durations | reuse existing unit-job flags | Already declared plugin and established CI settings. |
| Sequential test execution | preserve existing data-job command | Maintains session-order comparison; xdist would change that boundary. |

## §6 Open questions for the human

None blocks this explicitly authorized narrow fix. The coordinator owns any
rerun/cancellation decision. Completion of existing jobs and #941 causal
attribution remain independent questions.

### Phase 3 — verification

Pending YAML/structural comparison, command availability, and independent review.
