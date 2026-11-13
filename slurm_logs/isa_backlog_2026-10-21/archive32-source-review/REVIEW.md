# Archive32 source safety review

Sue, GPT-6. Source and saved metadata only.

No source blocker remains under root's explicit scoped frozen-source contract.
This does not report writer execution or archival acceptance.

The exact plan is `f157b0a08cdfcc340f55791b10fa6543ec031f6b71d2dc84d726d8c23dab93e7`.
Launcher is `e30072863778def4ff81fb2158a4fc3ddb2fb2f2d2b061271959ddfc5dd41e66`.
Verifier is `d7b84d77a1f40524be907d77a24d31bef447ca992c461a2928708da5cf130021`.
All 75 static preparation inputs match the plan, including final recheck.
The standalone common source and its 16 functions are identical in all four controllers;
the seven selection functions are identical in launcher, verifier and postexit.
Only source AST parsing was performed; these controllers were not imported or executed.
Archive31 plan, launcher, verifier, original-launch/postexit sources and root acceptance were read as prior art, never executed.

The finite five roots contain 1,977 included files / 462,361,511 bytes and 671 exclusions.
All included endpoints are regular, nonsymlink, nlink1, UID43292 in the saved plan.
The 172 genuine raw L1 copies are excluded using frozen preparation/protection witnesses;
this review did not open or rehash their bytes.
The included 156 proof parquets and 451 pickles are all private, opaque operands.
Two full raw-table-derived oracles and one native identity witness have explicit distinct classifications.
Actual L2 proof parquets are retained without a blanket data-directory exclusion.
The 1,455 directories, late lifecycle endpoints and one absent declared phase0 control are separately recorded.
No live parent, source checkout or mounted runtime image is an archive root.

Three qualifications remain explicit:

- At `archive32-launcher.py:355`, payload streaming uses `Path.open`; that streaming fd itself is not O_NOFOLLOW/fstat-bound.
  The immediately preceding `receipt()` at line352 uses O_NOFOLLOW/fstat and nlink1;
  full source-after checks and all archive-member hashes remain required.
  Root explicitly accepted these frozen-source before/after checks as the contract.
  This review makes no atomic fd snapshot, zero-race or universal syscall trace claim.
- The three saved preparation snapshots each have zero observed readable root hits and seven UNKNOWN process scopes.
  Unreadable axes are not treated as zero access. No unrelated process was queried for this review's fresh check.
- Included input modes are 1,634 at0600 and343 at0644 beneath recorded private ancestors.
  Root's original overstrict assertion stop is preserved, with no chmod.
  The measured input receipt `dd57e8949323a287081fb102eae1f759bf81beff25ea19d3238b2194f389a248` binds the exception.
  This does not relax the four fresh durable outputs'0600 requirement or the existing directory's02700 requirement.

At `archive32-common.py:25`, full hash receipts use O_NOFOLLOW, fstat and unchanged physical receipts.
Selection uses lstat/scandir and excludes link objects without traversing targets.
Included hardlinks are forbidden; exact deliberate synthetic objects have explicit metadata exclusions.
The dir_fd/intFD/two-endpoint write and network boundaries are present.
The boundaries are scoped Python instrumentation, not a kernel-wide adversarial guarantee.

At `archive32-launcher.py:324`, exact plan/approval/static input/selection gates precede durable writes.
All four output paths are checked absent and opened with O_EXCL and0600.
The old102 entries/31 archives and exact destination identity are preserved by physical metadata gates;
old archive contents are not newly rehashed here.
The tar stream is capped at8GiB; the saved upper bound is479,109,120 bytes.
Each PAX timestamp is built from integer seconds/nanoseconds, with nine decimal digits.
The private manifest embeds exact plan, approval, static source texts and full receipt maps.

At `archive32-verifier.py:319`, the verifier reads the gzip/tar stream without extracting to disk.
Every member must be unique, expected, regular, ordered, size/mode/UID/GID exact;
Decimal reconstruction preserves nanosecond mtime, and full opaque SHA must match.
All expected names must occur, with unchanged archive/manifest and durable controller receipts.
Its --verify-source path repeats full included source hashes and metadata-only exclusions/sets.

At `run_archive32_original.py:196`, each actor waits behind a pipe start gate while the original
Popen PID, source, argv, UID/maps, cwd, proc-stat and pre/post times are persisted.
The launcher separately binds its child verifier's actual original PID, stdout and terminal0.
At `postexit_archive32.py:318`, separate read-only postexit requires those original zero outcomes,
checks the actual child self-PID, durable full hashes and complete source physical sets.
It does not pretend to replay tar members or excluded raw bytes.
Exact external SDK/harness records and root independent acceptance are still later gates.
A writer failure must retain partial outputs; this source offers no automatic overwrite/removal.

Historical strict failures and setup stops remain part of the private archive.
Archival is not a new product or science verdict.
The writer/verifier aggregate stdout contains counts, hashes and process identities;
full raw-table operands, warning exposures and private footers remain in0600 artifacts under02700.
No public frame or household identifiers are emitted by this review.

Evidence and complete pins are in `EVIDENCE.json`, `FINDINGS.json`,
`ROOT-SCOPE-CLARIFICATION.json` and `FINAL-PINS.json` beside this report.
No product/runtime/provider/test/API/durable operation was performed.
The initial missing `python` shell command exited127 and is recorded; the subsequent source-only
checks used `/usr/bin/python3 -S -B`. Unknown Popen metadata was not reconstructed.
