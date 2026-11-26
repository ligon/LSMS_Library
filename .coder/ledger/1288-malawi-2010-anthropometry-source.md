# #1288 - Malawi 2010-11 anthropometry source paths

## Scope and prior art

The 2010-11 wave reads two fixed Full_Sample household modules via string
concatenation. The existing AST scanner sees the two bare filenames, which
resolve to absent paths under Data for the wave hash or under _/ for the
private Make collector. Full_Sample and Panel contain identically named
files and are not interchangeable (Malawi CONTENTS.org, IHS3 design).
Use the same two exact Full_Sample operands as before.

Reuse scan_script_data_refs, source_fingerprint and Country private Make
collection; see STANDING.md, the 1288 Malawi source-literals ledger, and
1288-malawi-2013-anthropometry-source.md. Unlike IHS3, the existing 2013-14
bare names were already correct for the wave hash. The change must not touch
the differentiated roster PIDs, measurement grain, age/sex computation,
eligibility, other wave scripts, or backend policy.

## Verification

Use real sidecars in the scanner and in the actual metadata-only private
collector, explicitly reject the two identically named Panel objects, and
verify both wave-hash and private-copy source resolution. A change in either
used sidecar must move this wave's anthropometry hash. Exercise the actual
driver with invented inputs to preserve exact getter paths, calls and writer.
Measure Malawi fingerprint footprint and the directly affected runtime route
where exact held data are available. Never infer closure of #1288 from this
one pair or download additional survey blobs in this lane.

## Acceptance

The two IHS3 scanner/collector checks fail before the literal repair while
both IHPS controls pass. With the repair, 29 focused Malawi/Uganda checks
pass. Independent review confirmed unchanged path strings, selection and
writer calls. No survey bytes were downloaded.

Separate offline cold public builds at development and candidate used
digest-verified private copies of 21 held sources, with field projection.
Before repair the Make transaction omits both IHS3 inputs and the cold
public calls fail when the offline child attempts its remote fallback.
After repair both inputs are staged and the public default/Make payloads
and metadata match exactly (31,038 rows, all four supported waves).
Repeated default Country, Feature, and the IHS3 singleton agree. Running
the actual standalone IHS3 driver against the same two held inputs at both
heads yields all 7,788 rows, schema and metadata exactly unchanged.

52 of 182 Malawi fingerprints change (26 on IHS3 and their country hashes).
No other wave fingerprints move. Dynamic-path coverage outside these two
fixed references and direct copy-vs-fingerprint distinctions stay open.
