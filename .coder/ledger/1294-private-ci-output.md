# Prior-Art Ledger — GH #1294 private credentialed CI output

**Search tier:** ripgrep + git floor. Parent is exact PR head
`0e208a25b57d7b678fdb655744de606abbe59754`; clean isolated worktree verified.

## §1 Task, restated

Route credentialed CI prebuild, pytest and Windows script-build subprocess
output through a stdlib-only private capture. Preserve argv, coverage, gates,
secrets scope and real exit status, while public logs contain only a fixed stage
label and numeric status/outcome counts. No real survey data is used in tests.

## §2 Existing machinery

| symbol/path | behavior and existing verification | decision |
|-------------|------------------------------------|----------|
| `.github/workflows/ci.yml` `data-tests` | Uganda prerequisite Make and sequential full pytest, reader credentials; original PR adds plain assertions and warning-summary suppression | extend command boundary; preserve test arguments |
| `.github/workflows/ci.yml` `windows-script-build` | credentialed installed-wheel Uganda shocks/assets build; `scripts/windows_build_check.py:80,92` currently prints exception/warning messages | wrap existing command; preserve installed-wheel/env checks |
| `scripts/windows_build_check.py` | existing aggregate digest/result checks and platform tool assertions | reuse unchanged, capture its complete output |
| `scripts/reencrypt_s3_creds.py` | subprocess capture for credential re-encryption; not a generic safe CI command reporter | do not repurpose unrelated credential logic |
| CI unit summary `xml.etree.ElementTree` | aggregates JUnit counters for skipped-test disclosure; not an output privacy boundary | reuse stdlib XML approach, derive counts from testcase elements only |
| mounted pytest terminal / pytest-instafail0.5 | plain assertions still retain explicit messages; capture defaults all, short summary uses exception text; instafail obeys `tbstyle=no` | subprocess capture is necessary beyond flags |
| `tests/test_build_subprocess_env.py` | existing synthetic child-process identity checks | reuse subprocess-based test style; no change to its assertions |

## §3 Definitions & conventions in force

- Campaign design authority: parent `CI-OUTPUT-DESIGN.md`, 2026-10-24.
- Canonical survey/API definitions remain `STANDING.md §3`, `CLAUDE.md`, and
  `lsms_library/data_info.yml`; this CI task changes no survey interpretation.
- Public output means both wrapper stdout and stderr, including failures,
  startup errors, malformed XML and plugin output. Failure text, captured
  streams, XML properties/skip reasons/parameter values are never public.
- Failed source identifiers cannot be proven from arbitrary JUnit attributes;
  this wrapper therefore emits counts only, as the design permits.

## §4 Invariants & assumptions

- Inherit `STANDING.md §4`: no raw survey IO, DVC CLI, cache changes or schema
  changes. Tests use synthetic secret sentinels only.
- Commands are argv, `shell=False`; never reconstruct a shell command.
- Temp directory uses OS user temp. POSIX modes are explicitly0700/0600;
  hosted Windows inherits the runner user temp/profile ACL, per root clarification
  2026-10-24. This is a public Actions output boundary, not same-user isolation.
  Output and optional XML are removed on normal exit; no private uploads.
- Preserve the child's status; negative POSIX signal statuses become shell
  convention `128 + signal`. Startup failure has an explicit nonzero status.
- Wrapper startup errors and cleanup failures never print exception text.
- System Python launches the wrapper; Poetry/Make output is captured inside
  the child. Windows conda environment and installed-wheel checks are retained.
- Allowed files: this ledger, `.github/workflows/ci.yml`,
  `scripts/private_ci_check.py`, `tests/test_private_ci_check.py` only.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| existing CI checks/arguments and wheel identity | reuse | current coverage is retained |
| subprocess execution/capture | new stdlib wrapper | no generic safe-output command wrapper exists in repo |
| JUnit outcome summary | extend stdlib ElementTree pattern | counts only; ignore all text and identifying attributes |
| test logs | new private temporary output | pytest flags cannot suppress arbitrary plugin or prerequisite build output |
| public stage | new fixed vocabulary | caller-controlled labels must not become an output channel |

## §6 Open questions

None blocking the scoped correction. Root explicitly approved the hosted Windows
user temp/profile ACL assumption (2026-10-24), counts-only JUnit, and the private
`{junit}` argv placeholder. Native Windows executable behavior remains a CI gate;
Linux tests do not establish a native Windows result.

### Phase 3 — verification

- `private_ci_check.main` — OK (§3/§4): argv subprocess capture starts before
  child execution, fixed validated stages only, no child/exception/XML text is
  forwarded, startup failure125 and real child failures preserved.
- `_junit_counts` — OK (§2/§3): extends existing stdlib XML approach; derives
  testcase outcomes, ignores names/parameter values/text/properties and refuses
  malformed/unexpected/DTD documents. No safe source-id provenance is assumed.
- `_exit_status` — OK (§4): preserves ordinary statuses and normalizes negative
  POSIX signal status to shell128+signal. Signal15 gives143 in actual subprocess.
- Three workflow calls — OK (§2/§4): existing Uganda Make and full pytest args,
  credential gates, installed-wheel Windows commands/env unchanged; all output
  is captured before Poetry/Make/pytest starts. No private artifact upload.
- Reuse recheck — OK (§2/§5): repo has no equivalent generic command wrapper;
  existing credential helper and Windows fingerprint script remain unchanged.
- Verification: `tests/test_private_ci_check.py`: **30 passed, 0 failed**,9.48s,
  mounted Python3.11.6, taskset22,24, xdist2 `--dist=loadfile`, BLAS/OMP/MKL1,
  `LSMS_BUILD_WORKERS=1`, `LSMS_SKIP_AUTH=1`. Exact real-path worktree import
  identity was asserted; logs live under campaign fix1294 control directory.
- Synthetic child pytest failures exercise explicit assert/exception messages,
  stdout/stderr/logs/warnings, setup/teardown/collection failures, skip/xfail and
  parameter names; both ordinary pytest and installed pytest-instafail0.5 paths
  preserve failure status without publishing fake sentinels. Passing warning
  capture still passes. Missing/malformed/malicious JUnit, argv with spaces and
  shell metacharacters, permissions/cleanup/startup statuses also pass.
- Scope: four authorized files only; no real microdata/network/acquisition,
  dependency/fixture/lock edits or weakened existing assertions. No native
  Windows execution claim. SIGKILL/host teardown cleanup remains OS/runner-owned.
