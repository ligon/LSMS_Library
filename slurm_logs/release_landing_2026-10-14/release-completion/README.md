# v0.15.0 release completion

Released 2026-10-15 from `cb44e3522a2426ba03431ca9a218cc63fe03f6c9`.
[GitHub Release](https://github.com/ligon/LSMS_Library/releases/tag/v0.15.0),
[PyPI](https://pypi.org/project/LSMS-Library/0.15.0/).

The release includes the maintainer-approved Uganda #967 fixture and #1014
assets repair. Approval remains scoped to those reviewed changes. All 23
implementation PRs landed; #1015 archived the prior final evidence and #999
merged development into master with both parents preserved. The 21 intended
issue closures and ten selected open residuals matched their expected states.
The completion PR carries tagged master ancestry back into development; its
body records external refs after merge, which cannot be embedded in its own commit.

## Executed gates

| Evidence | Actual result |
|---|---|
| [Actual make release review](make-release-review/MAKE_RELEASE_REVIEW.md) | 7,376 passed; 314 skipped; nine xfailed; one existing non-strict xpassed; zero failures/errors. No credential-guard skips. |
| [Guyana follow-up](guyana-sanity-followup/result.json) | The single cached sanity assertion passed on an isolated copy. Original retry exception/cause unknown; original suite skip count retained. |
| [Master CI](master-ci/REPORT.md) | 4,927 data passes, 2,768 skips, four xfailed, one xpassed; no failures. Every applicable job passed, including installed native Windows cold builds. No master advance. |
| [Publication](publish-ci/REPORT.md) | Automatic release-event build and OIDC PyPI upload succeeded for tag v0.15.0 at the exact master commit. |
| [Downloaded artifacts](published-artifacts.json) | Both PyPI files match published SHA256; all 11,751 wheel package paths/blobs equal the tagged source, with no extras or omissions. |
| [No-Git rebuild and installed comparison](artifact-comparison.json) | All 11,756 wheel members have identical bytes, inventory, order and compared metadata. All installed package files equal the published wheel. Compressed archive bytes differ, as explained below. |
| [Fresh installation](installation.json) | Installed exact 0.15.0 public PyPI wheel and 170 dependencies into a clean environment; [pip check](pip-check.json) passed. [Version inventory](installed-distributions.json) retained. |
| [Empty-cache/config smoke](installed-smoke.json) | Correct installed identity, 20 public callables, three country configurations, catalog/provenance and synthetic quantity checks passed outside Git. |
| [Installed real-data smoke](installed-data-smoke/result.json) | One ordinary warm Uganda assets call returned the exact 116,808-row/eight-wave expectation, with all 172 private cache files and installed package unchanged. |
| [Frozen-root preservation](preservation-after-isolated-release-tests.json) | Both 1,519-file original/final roots retain SHA, size, mtime, device and inode after isolated release testing. |

The installed data call used a separate prepared physical cache copy and
scrubbed credentials; it does not claim a credentialed cold rebuild. Native
Windows credentialed cold tests are separate master-CI evidence. No APIs ran
against either frozen evidence root or shared L2 during installation checks.

## Rebuild compression distinction

Published wheel SHA256:
`a19b458f2d2fce0bb67c192338ffb02fb745f78af3ba77b0d5db1c3395c0f9f0`.
Published source archive:
`e8ed24a6609268b02220e2eb94c7809ab5c153468094614c8af2b8444c78c657`.
Rebuilt wheel:
`89bae95850facae20756c7dd0628c22af0ba510270b1607420ec80a3c185a75b`.

The initial prepared verifier expected whole-archive equality. Execution found
identical contents but different compressed bytes. Independent
[container review](artifact-validation/container-correction/REVIEW.md) explains
the entire 5,645-byte difference through DEFLATE streams and associated
compressed-size/offset fields. Member inventory/content/order and all other
headers match; no difference was ignored or a package-content assertion relaxed.
Local zlib-ng reproduces every rebuilt compressed stream; CI's compressor
identity is unknown. The amended verifier was independently reviewed and is
archived under `artifact-validation/container-correction/verify_artifacts.py`.
The original prepared scripts/review remain intact in `artifact-validation/`;
that original verifier is historical and was superseded for final verification.
No byte-identical-archive claim is made for this published release.

## Scope and retained limitations

The prior reviewed shared-cache promotion still admits 1,518 files: 153 new,
1,215 backed-up replacements, 150 unchanged. No files were deleted. The excluded
Uganda nutrition counterpart was already absent. L1 and rollback backups remain
preserved. The reciprocal FCT/nutrition hashless-sibling behavior remains #788.
Four attributed served-result exceptions, 19 source/wiring errors and existing
Feature findings remain documented in the [release notes](../../../docs/releases/v0.15.0.md).
Mali/Tanzania warm interview-date loss remains #798, with the tested per-wave
workaround; no broad scientific approval or new cache-policy fix is inferred.
All 193 ref versions have dispositions: 101 landed, 63 superseded, 19 rejected,
six archives and four integration refs. No historical branch was deleted.

This archive contains aggregate outcomes, public identifiers/digests and
source-only verification tools. Raw survey/test/install logs, JUnit XML,
distributions, environments and parquets remain outside Git. `MANIFEST.json`
pins this final archive; the earlier `CHECKPOINT_REVIEW.md` describes a historical
prepublication checkpoint, and the final review records the completed delta.
