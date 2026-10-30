# Nigeria #1120 D1 / #1261 public-base extraction

## §1 Task
Finish the retained round-specific sample/cluster repair over public d65e8b136,
including labelled PH same-location correction; exclude #1119/#637/education.
New conventional cached-source verification is authorized by the 2026-10-29
worker brief; historical controls and custody are not replayed or accepted.

## §2 Existing machinery (git + ripgrep floor)
Reuse retained 5462b4eab (product 16636094f, refinement 9cd98fabf, empty
append correction 57b99d3de), reviewed with its ledger and Nigeria sibling
REPORT/PUBLIC-INTEGRATION. Reuse cluster_id, df_data_grabber/get_dataframe,
Wave script routing and Site 2 most_complete_row, Country weight normalization,
id_walk, and derivation registry. Synthetic coverage is retained in
 test_gh1120_nigeria_same_location.py. No framework or other-country edits.
Source mirror holds local heads, not the remote-tracking refs named in brief.

## §3 Definitions
Inherit STANDING.md, CLAUDE.md and canonical data_info.yml. Q3 uses PP cover,
Q1 uses PH cover, each with its own raw weights. Household cover > supplement
cover > same-round roster. D2 unlocated singleton differs from recorded ea=0
moved singleton. W4 hhsmlc=1 labelled YES / W5 hh_moved=2 labelled NO, IN
ORIGINAL DWELLING alone permit complete own-PP identity reuse for PH ea=0.
W3 unlabelled answers do not. PH payload/weights remain PH. This does not
assert national population equivalence.

## §4 Invariants and contradictions
CONTENTS.org says PP "defines the household universe for the wave" and "The
same households are visited in both rounds". D1 intentionally supersedes this
behavior; independently test W2 44 PH-only / 39 geolocated, not left-join alone.
Retained 66/64 movement counts predate the refinement; do not repin them by
subtraction. Full source docs include live WAITING nonfood recall caveat.
W5 geo remains unwired. Panel JSON and education remain separate products.
Never weaken source numeric assertions: compare NA masks, extension arrays and
nonmissing numpy values exactly, calibrated with tiny/nextafter differences.
Use prepared image, private roots, one CPU, bounded cached-source calls only.

## §5 Reuse decisions
Surgically reuse retained cover helpers, thin scripts, sample/cluster schema
blocks and derivation keys; keep all unrelated public bytes. New evidence
script independently derives round membership and keys from verified cached
sources and compares actual Country/Wave/Feature results. No new reader guard
framework, no acquisitions, no historical control replay.

## §6 Remaining gates
Independent root review/publication. Source availability and fresh API results
are measured in evidence/REPORT.md; historical source-only receipts remain
qualified. Missing caches block only their declared dependent checks.

## §7 Fresh verification and dispositions (2026-10-29)
All 25 selected declared blobs were available and MD5-verified in private L1;
no acquisition or shared-cache write. Independent raw-source oracles passed
all five waves: W2 44 PH-only / 39 geolocated, two EAs absent PP household
cover; W4/W5 labelled own-PP reuse 20/31. All ten rounds have nonnull sample v.
Source PH weights and labelled payload remain exact; normalizations checked
with extension-array and nonmissing NumPy equality, including tiny/nextafter
negative calibration. Final full sample has 49,089 rows; clusters have 7,128.
60 focused helper tests passed. Root explicitly authorized D1 reconciliation
of three obsolete PP-equality functions: 29 selected genuine API tests passed
with unchanged fixtures/unrelated assertions. Original 9-failure/2-pass receipt
is preserved. No full-country-suite or separate #1318/#1309 composition claim.

Evidence REPORT.md, scripts, JSON and JUnit live in this dispatch's private
campaign evidence directory; raw frames/source witnesses remain separately in
mode700 private-witnesses/f7eccd39e595453b852556e71fbf9977. Root retains review,
integration and external publication. #1264 panel links, #978 W5 geo wiring,
Site2 geo disagreements and incomplete cluster-selected provenance remain
explicit residuals; this product makes no national-population equivalence claim.
