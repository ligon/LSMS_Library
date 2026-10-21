# Additional static release-gate review

Sue reviewed the version-2 adapter, identity hook and CI barrier without running project code.
This is a producer-side control review; the parent's separate final review remains required.

Result: no blocking control-flow defect found within the following assumptions and scope.

- The unchanged real Makefile checks tag absence, invokes the adapter without an ignored-error
  prefix, and reaches `git tag -a` only after a zero exit. There is no setup/install prerequisite
  or local build in the actual release recipe. The adapter itself contains no tag/GitHub mutation.
- The adapter rejects commands other than the exact `run pytest` form and rejects disabled
  Python assertions. The checked arguments pin 16 workers, loadfile distribution and a 600-second
  thread timeout, with maxfail one. Package/config/data/root/interpreter/affinity and actual loaded
  BLAS pools are checked before `pytest.main` in that process.
- The identity module is loaded explicitly by pytest in the controller and every xdist worker.
  It accounts for xdist's documented implementation resetting worker `numprocesses=None` and
  `dist='no'`, while checking its exported worker count is 16. All worker identities are new files
  in a newly created output directory; the final gate requires all 16 identities and reporting
  workers. A failed worker or missing identity cannot yield the successful release exit.
- Nonzero pytest status, any changed baseline outcome/reason, missing identities, changed source
  or exception exits nonzero before the CI wait or tag. A failure cannot fall through into success.
  Direct XPASS identities are compared against the original 532 plugin outcomes, not inferred
  from ambiguous JUnit PASS encoding.
- A complete local summary is written before the CI barrier. The barrier performs only a GET
  for the specified run, verifies the exact full SHA and master push workflow identity, and permits
  only completed/success. Failed, cancelled, timed-out and other completed conclusions refuse;
  identity mismatch, malformed metadata, wait timeout and repeated read errors refuse. No test
  invocation occurs in the barrier. Nine stubbed metadata/command/time controls pass.
- On successful CI completion, the adapter repeats checkout/package/config/data/HEAD checks
  immediately before setting its final success record and returning zero. The real Makefile then
  creates the tag. This is not an atomic lock across two processes: the parent must keep the chosen
  master checkout and refs frozen between this final assertion and the tag recipe, as already planned.
- The new target has the exact original 532 START file set and content hashes, not the 16 files
  subsequently added to the completed suite cache. Source hash/stat identity and distinct destination
  inodes were checked after copying; only L1 is symlinked. No source cache was edited or cleared.

No project tests, package imports, real CI requests or source/Git mutations were used for this review.
The adapter's end-to-end real execution and the future artifact verifier remain unexecuted until
the parent supplies and authorizes the final master pin. The PyPI checker promises exact payload
and offline installed-configuration checks, not fresh dependency resolution or installed microdata.
