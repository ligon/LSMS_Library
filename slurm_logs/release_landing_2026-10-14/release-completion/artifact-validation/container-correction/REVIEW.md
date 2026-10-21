SCOPE DEVIATIONS

None. Read-only inspection of the two wheel files, local compression implementation, saved rebuild record and parent-owned verification script. No library imports/APIs, installed-data smoke, corpus/cache access, archive mutation, source edit or Git/GitHub operation. Archive SHA-256, size, mtime, device and inode are unchanged.

APPROVE the amended verification in verify_artifacts.py at the SHA-256 recorded in review.json. The no-Git sdist rebuild is exactly equivalent in package contents, while its compressed ZIP bytes are not reproducible across these two environments. This is an evidence-backed correction to an overly strong archive-byte assertion, not permission to overlook package differences.

| Property | Published PyPI wheel | Local sdist rebuild |
|---|---:|---:|
| Archive bytes | 8,852,718 | 8,847,073 |
| Compressed payload bytes | 6,417,010 | 6,411,365 |
| ZIP overhead bytes | 2,435,708 | 2,435,708 |
| Members | 11,756 | 11,756 |

Published SHA-256: a19b458f2d2fce0bb67c192338ffb02fb745f78af3ba77b0d5db1c3395c0f9f0.
Rebuilt SHA-256: 89bae95850facae20756c7dd0628c22af0ba510270b1607420ec80a3c185a75b.

All 11,756 uncompressed member bytes are exactly equal, including RECORD, METADATA and WHEEL. Member order, archive comments and every compared non-layout ZIP metadata field agree: timestamps, compression method, entry comments/extras, creator/extractor versions, platform, flags, volume, internal/external attributes, CRC and uncompressed size. There are zero duplicate member names. Every entry uses DEFLATE.

Exactly 9,666 compressed streams differ; 7,934 of them differ in compressed length. The -5,645-byte compressed payload delta equals the entire archive-size delta. A separate binary-header proof checks all 11,756 local headers, all 11,756 central-directory headers, the prefix and end record. They are identical after normalizing only compressed sizes and the offsets necessarily shifted by those sizes. There are no unexplained descriptors or padding. Thus no other container-byte difference remains unaccounted for.

The local runtime reports zlib 1.3.0.zlib-ng. Independently compressing every member with that host's default raw-DEFLATE encoder reproduces all 11,756 local rebuilt streams exactly, versus 2,090 published streams. Local Poetry core uses ZIP_DEFLATED with the default level. This directly locates the difference in compressed representation. The exact CI compression implementation/version is unknown from the evidence inspected and is not asserted.

The amended verifier retains strict assertions for equal member sets, every uncompressed member byte, installed package contents/no extra installed files, member order, archive comments, fourteen metadata fields and equal ZIP overhead. It records both archive SHA-256/sizes, archive_bytes_equal=false and the count of different compressed sizes. Its interpretation correctly says content equality rather than byte-identical wheels. The separately checked public PyPI digest remains the identity of the downloaded/pip-installed artifact; the local rebuild digest must not replace it.

Recommended release wording: "The wheel rebuilt from the published sdist without Git has exactly identical member contents, inventory, ordering and checked ZIP metadata, including RECORD/METADATA/WHEEL. Its archive SHA differs because DEFLATE streams differ; all 5,645 bytes of size difference are explained by compression. Archive-byte reproducibility is not claimed."

comparison.json records all per-member compressed changes and aggregate totals; headers.json records the complete container-header proof. analyze.py and verify_headers.py are their source generators. The parent-owned script was reviewed, not edited or executed by this reviewer. Actual installed verification and data-smoke results remain separate gates.
