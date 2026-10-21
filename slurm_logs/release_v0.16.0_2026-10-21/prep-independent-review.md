# Independent review: frozen v2 release verification preparation

**PASS for the documented, assertion-enabled execution and stated scope.**
No blocking release-gate or payload-proof defect found. Read-only review; no
project import/test, real CI request, data/cache operation, source edit or Git/GitHub mutation.
The six reviewed controller/README/smoke files match their frozen MANIFEST hashes.

- `Makefile:115-123` checks tag absence, calls the supplied `POETRY run pytest`
  without an ignored-error prefix, then creates the annotated tag. It has no
  setup/build prerequisite. The adapter's exact argv check matches that call.
- `poetry-pytest-only` refuses disabled assertions, non-pytest commands and
  argument drift. It validates the new1519 seed file set/hashes and private
  L2 files before tests, and requires all16 worker identity files/reporters,
  pytest exit0, and exact per-node outcome/reason parity against532. The
  recorded baseline has9137 outcomes and no machine-specific /local paths in
  skip/xfail/xpass reasons. Failure or an exception exits before the tag path.
- `release_identity.py` checks the full expected SHA, tracked source, exact
  interpreter/package/config/data paths, worker options, affinity and loaded
  thread pools, including controller and xdist workers. It repeats checks
  after tests and immediately before the adapter's final success.
- `ci_gate.py:8-18` binds run ID, full SHA, automatic push, master, repository
  and ci.yml; only completed/success returns success. Other conclusions,
  identity drift, malformed metadata, wait expiry and repeated read errors
  fail closed. It polls read-only and never reruns tests or mutates a ref.
  The frozen workflow's master-push condition selects credentialed data-tests;
  this controller records workflow-level success, not independent per-job
  checkout/gate logs. Preserve that evidence distinction in the release report.
- `verify_published.py` pins local HEAD and the peeled release tag, downloads
  exact0.16 PyPI wheel/sdist records, checks lengths/digests and metadata/CLI,
  and compares the complete11753 package-file set and every byte to tagged
  Git objects. The installation is private/offline/no-deps/no-compile. The
  separate smoke pins installed import/config identity, denies network/data
  builds and calls only metadata discovery. It correctly does NOT claim a
  wheel rebuilt from sdist, fresh dependency resolution or installed microdata.
- The actual publish workflow builds from the release/dispatch tag with full
  history and dynamic-version plugin, then uploads through Trusted Publishing.
  Its own version guard only rejects0.0.0; the subsequent PyPI verifier supplies
  the exact0.16.0 and source-payload proof rather than overstating that guard.

## Constraints to preserve

Keep the chosen master checkout/refs frozen through adapter return and the
Makefile's following tag line: these are separate processes, not an atomic
ref lock. The CI wait can extend to10800seconds; the parent owns allocation
and progress monitoring. Baseline parity is deliberately strict: any genuine
outcome/reason change requires inspection and does not authorize a tag merely
because pytest exited0.

The future `verify_published.py` and installed smoke use Python `assert`
without their own disabled-assertion guard. The current review interpreter
has assertions enabled and the documented command uses normal Python, so this
is not a blocker for that invocation. Explicitly keep PYTHONOPTIMIZE unset
(and do not add -O); a small optional `if not __debug__: raise RuntimeError`
guard would make this constraint fail closed as the adapter already does.
No broader redesign is recommended.

All review readers/writers are stopped after this report. Production and
prepared-controller files remain untouched.

Sue
