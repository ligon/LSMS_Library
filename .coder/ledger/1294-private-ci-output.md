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
- Temp directory is owner-only; output and optional XML live there and are
  removed on exit. No private output/XML upload artifacts.
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

- Windows owner-only ACL implementation must be verified; POSIX chmod alone
  does not establish Windows DACL privacy. Ask root if its concrete runtime
  constraints require a different supported Windows version. No native Windows
  result will be claimed from Linux verification.

### Phase 3 — verification

Pending implementation and synthetic subprocess validation. No data tests run.
