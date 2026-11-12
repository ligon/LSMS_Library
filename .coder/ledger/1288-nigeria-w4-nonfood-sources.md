# GH #1288 — Nigeria W4 nonfood source declarations

**Search tier:** git + ripgrep + stdlib AST floor; no GitNexus tool available.
**Base:** 9e9c1361a876eb0e7418edc6d49866eeebf94494 (tree f9e27bf8e3bc0ed75b052c05eecda9f6f895e452).

## §1 Task
Finish only the seven-source 2018-19 residual of the accepted W1–W3 nonfood
repair. Add the independently accepted guarded script-relative declarations,
preserving the complete original reader and processing bytes/AST. Extend only
the existing CASES list and run exactly 13 new W4 pure cases, never the prior39.

## §2 Existing machinery
- `local_tools.scan_script_data_refs` (3559) already finds every W4 literal.
  `source_fingerprint` (3538) prefers sidecar metadata. Reuse unchanged.
- `Country._make_declared_inputs` (5701) resolves literals beside scripts;
  `Wave._input_hash` (1539) resolves under wave/Data. Both accept ../Data/.
  The accepted regression module tests the actual scanner and collector on
  metadata-only receivers, not constructors or source getters.
- Nigeria `2015-16/_/nonfood_expenditures.py` (23–46): accepted ordered
  declarations and explicit RuntimeError guard; reuse this exact idiom for W4.
- Nigeria `2018-19/_/nonfood_expenditures.py` (14–20): four PH then three PP
  CSVs, and an unchanged `get_dataframe(fn)` with no kwargs. No path rewrite.
- Nigeria `_/nonfood_expenditures.py` (34ff) enumerates four folders; preserve
  compiler, Makefile, data_scheme, mapping, sample and all framework bytes.
- Accepted prior art: `.coder/ledger/1288-nigeria-nonfood-sources.md`, its git
  history, tests/test_gh1288_nigeria_nonfood_sources.py, and original dispatch
  de272de26c0d43438154cb39838fb6b3 evidence/REPORT.md and pure-runtime operators.
  Its 39 passing cases are historical evidence; no repeat is authorized.

## §3 Definitions & conventions
Inherit STANDING.md §§3–4, CLAUDE.md and lsms_library/data_info.yml.
Full Nigeria CONTENTS.org read, including incoming PH and source appendices.
Its Sampling Design says PP ~Q3 and PH ~Q1 of the following year; W4 retains
2019Q1 PH then 2018Q3 PP. The nonfood section says “t is the round” and
“Recall windows are POOLED, and no column records it”. Its W4 manual conflict
“12 months reference period” / “PAST 6 MONTHS” does not settle recall policy.
The compiler's “wave scripts' own groupby” conflicts with the actual
`drop_duplicates` and uniqueness assertions. Preserve this contradiction and
all original history, rather than declaring historical prose stale.

## §4 Invariants
Remove only added declarations/guard/comment to recover the WHOLE original
W4 source byte-for-byte and AST-for-AST. Every reader argument, effective path,
order, kwargs, dictionary, processing, reducer, assertion and writer remains
exact. Every incoming tracked object is exact except W4, one CASES tuple,
this own ledger and an append-only dated CONTENTS completion note.
No actual source/header/row/blob/producer/API/Make/cache/provider/network or
census. No scientific source/ID/filter/recall/unit/weight/panel/default choice.
The bootstrap source-only runtime label is superseded by the explicit pure
grant and APPROVED-RUNTIME-BINDINGS.md. Bind the approved MAIN image to own
cwd/package/countries/config, private empty data and false credentials; use
existing narrowed observer, nonroot outer user, unshare --user --net, BLAS and
build1, rootdir=W, -n2 --dist=loadfile, direct logs and XML. Freeze source,
finite new-W4-only selection and operator before execution; keep immutable
until the actor closes. Preserve every failure and forbid arbitrary replays.

## §5 Reuse decision
Reuse the accepted guard and actual scanner/collector tests. Extend CASES with
independent literal W4 names in original PH/PP order; preserve all old tuples
and function definitions exactly. Add no guard framework. Existing conservative
script-byte hashing will move fingerprints; exact footprint remains unmeasured
here and integration-owned. No lost-row claim follows from anchor repair.

## §6 Open questions / boundary
No blocking question within the explicit engineering/pure grant. Pooled recall,
country serving, cold/warm and scientific acceptance, whole1288 closure and
exact hash impact remain outside this worker. Publish only private results.git.

## Phase 3
OK (§§2–5): removing only the added declarations/guard/comment recovers the
whole original W4 source byte-for-byte and AST-for-AST. All seven original
reader strings and their order, empty kwargs/default categorical behavior,
processing/dictionaries/reducers/assertions/writer and sidecar bytes are exact.
The three original CASES tuples and every test function are byte-exact; only
the independent W4 tuple extends the accepted tests. The guard AST equals the
accepted W3 guard after substituting only its wave name.

Exactly 13 new W4 cases pass once at product/test head
46b1bfdaeae58fa836e3533437d05709883652a1, with zero failures/errors/skips.
The prior39 were not replayed. Actual scanner and metadata-only collector
controls pass, plus the healthy guard and five drift types at optimize0/2.
JUnit retains all13 exact selected nodes. The existing plugin names per-node
receipts by callspec ID, so the three healthy functions overwrite one filename:
11 call receipts survive. This evidence limitation is retained; no actor was
replayed. The first summarizer's incorrect13-file assumption and traceback
are preserved separately before its mechanical XML-based correction.

The approved245395456-byte image hash/generation and unchanged normal helper
pass. Three controller/gw0/gw1 identity receipts bind current import/cwd,
countries root, own MAIN/private L1/data and false credentials inside the
network namespace. Empty private config is bound by frozen environment and
the unchanged config resolver source. Three observer profiles remain active,
zero denials; actor/wrapper exit0,59.45067386003211s, process group closed
without cleanup. Private data/config files0, source/operator hashes exact.
Metadata profile events: scanner67 (including generator resumes, not67
distinct calls), collector1, source_fingerprint8, guard AST modules11. There
are no native/source getter/parser/constructor/builder/writer/provider roles.

The initial api-audit Make recipe failed because Poetry is absent; the existing
stdlib auditor separately enumerated all15 skill descriptions. An operator
setup check wrongly assumed a3.12 stdlib path; the approved image is3.11.
Its failure/script/log are retained and only the exact known platform binding
was corrected before freeze. No test body failed or was changed/retried.

Evidence: this dispatch's private SOURCE-PROOF.json, pure-runtime/PURE-FREEZE,
PURE-RECEIPT/PURE-SUMMARY, exact operator/selection/log/XML/identity/observer
receipts, REPORT.md and EVIDENCE-INDEX.json. Final carry adds only this ledger
verification and a dated country-doc appendix; all incoming history and old
source39 proof remain intact. No actual source/API/cold-warm/scientific or
whole1288 acceptance follows. Exact hash footprint remains integration-owned.
