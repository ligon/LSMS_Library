# Prior-Art Ledger — #1288 Niger livestock declared source

Search tier: ripgrep+git floor. Source baseline9514be02e.

## §1 Task, restated
Expose the existing Niger2014-15 livestock source to the existing literal-source scanner by spelling the identical full getter path as one literal. No source or data policy changes. This fixes the finite private direct-country-builder dependency closure, not global computed-path evaluation.

## §2 Existing machinery
- `local_tools.scan_script_data_refs`: AST string-literal discovery; tested in `test_cache_hash_invalidation.py`. Reuse unchanged.
- `Country._make_declared_inputs` and `_make_build_root`: resolve script refs relative to its directory and copy authenticated held inputs into the private transaction; existing #1288 native tests. Reuse unchanged.
- Niger2014 `_/livestock.py`: single `base + filename` getter, owned==1, GRAPPE/MENAGE/EXTENSION identity, `_finish_livestock`. Preserve all processing.
- Niger `wave_producers.py`/Makefile: declared-wave preparation then country compiler. Preserve native force behavior; no operator -B or forced target.

## §3 Definitions & conventions
Inherits `STANDING.md`, `CLAUDE.md`, global `data_info.yml`. Personally read complete Niger CONTENTS3381 lines, SHA1048c56e38376c1b6733c256078ac57bbc8d9a40c6f5fe5ec679388c199333ad (own3275 full prefix plus all106 later lines). Full EXTENSION key is load-bearing. Recorded statement “Only the harvest is ruled” and dry-season/3C/3E source caveats remain; this livestock path repair makes no crop ruling.

## §4 Invariants
Existing sanctioned getter/writer only (STANDING§4). Exact same resolved raw file, categories=False, ownership gate, full identifier, numeric conversions, grouping, rows and attributes. No source/default/schema/weight change. Dynamic-source baseline remains unverified; do not relabel fixed result as baseline. No shared rewarm.

## §5 Reuse decision
Reuse scanner, sidecar resolution, direct-builder preparation, native helpers and ordinary private actual-route operator. Extend country-source regression only to prove actual full literal resolves to the committed sidecar and preparation source list; no new parser.

## §6 Open questions
No scientific decision needed. Root authorized literal repair and conditional held-only four-source actual verification. Independent review and measured metadata impact required before PR; baseline Niger actual route remains unrun.

## Phase 3
OK (§2–§5): exact source inverse proves only identical full getter literal; no processing/global resolver/parser change. Three focused controls PASS, including real sidecar and actual preparation collector.

Actual fixed-source default/Make cold+freshwarm processes all0,4heldDTAs20,877,001B,12fullStata parses,19active observers0denials, warmcache inventory exact. Both within-backend original strict cold/warm comparisons PASS. Original outerdriver1 and ALL4 crossbackend original strict failures are preserved: i.levels retains2,022unused raw-only mapped IDs on Make (10,049vs8,027 labels), while exactexpandedkeys/order/types/payload/dtypes/masks/typedattrs match. No sort/cast/tolerance or removal of unusedlevels. This is a broader representation boundary, not source-literal acceptance by normalization. Baseline9514 Niger source/API remains UNRUN; no replay.

Metadata0/errors0/denials0 on full38/450/1895/35/186:19Niger table+19Niger2014wave hashes move through unchanged conservative all-wave-build-script hashing;0BTF/FIF and allother metadata/keysets/public/overrides/18None unchanged. No exclusions/sharedrewarm.

Reports: B/reports/1288-niger-livestock-source-literal-20261109. Original failedpostprocessing and firstcloseddiagnostic shutdowncallback issue retained, finalclosedclassification0/no source/API/raw/parquet access. Independent review and exact-head configuredCI PENDING; source repair not whole1288 closure.
