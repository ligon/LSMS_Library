# Prior-Art Ledger — PR1683 cache recovery test contracts

## §1 Task
On exactedc4f0e0b6b085d35032e0fbde1449260f221240, reconcile four actual CI failures with the accepted1071 cache-only recovery contract. Test-only; preserve the original4FAIL/13888PASS/5673SKIP/4X result and WindowsPASS. ROOT owns publication.

## §2 Existing machinery
Reuse `local_tools._read_managed_parquet_cache` and its initial `pd.read_parquet` OSError/ArrowInvalid boundary; reuse the Wave owner/forced script builder and Country coverage-before-eviction gate. Existing `test_gh1196_wave_parquet_routes.py` uses actual cache gates and a `_Rebuilt` stub to detect rebuild descent, strict index-policy `_exact` helpers, and fatal footer/restoration controls. `test_gh920_rebuild_reason.py` checks actual read-gate reasons and preserved union coverage. No new reader or production helper.

## §3 Contract authority
STANDING §§2–4 and `.coder/ledger/1071-managed-cache-read-tranche.md`: one normal rebuild of pre-existing caller-owned unreadable initial payload; accurately name unreadability, remove only observed entry, force script rebuilding, preserve coverage. Fresh-output/footer/index/schema/null/grain/programmer failures remain fatal. Source-free fixture country metadata is not an actual-country/native build.

## §4 Findings/invariants
CI run42739059934/job129575330661 atedc4 has three GH1196 script failures because obsolete expectations require original initial-payload errors, while the accepted build stub correctly raises `_Rebuilt`. GH920's old country.get_dataframe spy no longer intercepts the real managed read gate. Preserve all strict frame/index/dtype/attrs tests and fatal phase/error identity controls. No production/config/schema/exclusion changes, native/provider/shared-cache access, fullsuite or census replay.

## §5 Reuse decision and finite verification
Extend only two existing GH1196 test functions and one GH920 test: initial failure triggers exactly one actual rebuild descent after rejection; script forceTrue; accurate warning; inject GH920 error at actual pd.read_parquet and preserve read-error/coverage. Run affected six route/error cells, GH920 node, and directly relevant healthy/fatal-phase source-free companions under MAIN/private roots/import identity/threadbuild1/-n2loadfile. Baseline is preserved public CI, not rerun.

## §6 Open questions
None: ROOT authorized justified engineering/test corrections; accepted1071 semantics are unchanged. Actual targeted result/final source proof pending. Source search uses git/rg/AST; no GitNexus dependency.

## Closed focused acceptance
Runtimefa209aeb410fdbf32cdc4e7e860fac6f1efdce4a, actor42091 CLOSED0/19.701s (pytest13.40s):17distinctPASS/0FAIL/ERROR/SKIP. Six modified Wave cells+one modified Country cell; six unchanged fatalfooter/restoration cells; three strictsame-stream/atomichealthy cells; one healthyCountryhit. Initial cache rejection warning, exactentryremoval beforebuild, onebuild, literal scriptargs/forceTrue, actualCountrypayloadengine/read-error/coverage/eviction all reached. No actual producer/native country/API or fullsuite/census replay.
All3MAIN/private/nonrootnamespace identities, operator/sourcebindings and cleanheadverified;0tripwiredenials;top-levelprivate data/config0files. Entireproduction/config and19,267nonowned mode/type/blob objects exactedc4; only3named testfunctionASTs changed and allother testAST exact. OriginalCI4F/13888P/5673S/4X and WindowsPASS log retained externally at reports/ci-1683-correction-20261110. Finalization updates only this ledger; no new semantic/test changes.
Allfour temporaryreal-reader secretcopies from the closed1281attempts were removed after no-live-reference verification; globalreaderconfiguration untouched, no content/hash/export. Cleanupreceipt lives in the same report. ROOT owns PR update/configuredCI; separate1071A+B source remains independent.
