# Prior-Art Ledger — GH1574 public own BID documentation holding

**Search tier:** git/ripgrep/AST source inspection; no package import, payload reopen, credential/config-home read, cache/write/getter/DVC runtime. Initial base5a06d85cd02237cc515b0cda57012a9cab3d5e11; ordinary disjoint integration now includes actual GitHub developmentfdec032efd4b98ed4b99883c9586f5ef07b7bf3e, own isolated proposal tree. Inherits STANDING.md/CLAUDE.md.

## §1 Task
Prepare one managed ingestion of already-fetched public own Mali2017-18 English Basic Information Document (World Bank catalog3409/resource47785), target `Mali/2017-18/Documentation/bid_eaci17.pdf`, with generated DVC sidecar, SOURCE provenance and dated CONTENTS availability note. Root must review the concrete code/write scope before any cache/remote/credential operation. This makes documentation available; it does NOT resolve GH1574’s missing51-code stratum list or choose labels/weights/defaults.

## §2 Existing machinery
| Symbol/path | Current role | Decision |
|---|---|---|
| data_access.push_to_cache:1737–1802 | One exact documentationpath; managed dvc add, credentialpath context and dvc push; bool success | reuse only, no bare CLI |
| _check_write_access / permissions:1606/1178 | Credential availability gate, not actual IAM proof; private empty WB config avoids auto-unlock branch | reuse with explicit private reader/writer profiles, futuregrant |
| _s3_writer_credentialpath:1683 | Temporarily overrides S3 remote credentialpath to writer, restores config.local bytes | reuse; do not export secret pairs |
| _run_dvc_with_lock_retry | Existing known-lock-only bounded retries; othererrors return/stop | reuse unchanged |
| get_data_file:3018–3244 | Local/materialized hits precede permissions; otherwise own sidecar→privateL1→suffixed materialized copy, then DVCFS/WB fallback | reuse in FRESH readback tree with NO targetPDF and emptyprivateL1; forbid acquisition |
| config.s3_{creds,write_creds}_path | Explicit envpath overrides; no globalfile rewrite required | use private600copies only if Rootgrants |
| Documentation/SOURCE.org / LICENSE.org | Own catalog3409/idno/DOI/citation and access terms | preserve existing metadata/license; append document provenance |
| installed dvc config_schema / Repo.site_cache_dir | Private cache.dir/core.site_cache_dir and analytics/check_update settings supported | private local overrides only; no installs/source edits |

## §3 Country facts and contradictions
Personal full3551-line Mali CONTENTS prefix authenticated SHAe10f15f214218de84c76850ee9c7f4bab17e3be00315a1d960d85ed6df6d26a7 including LOGBOOK/WAITING/TODO, plus my personally written/read31 appended1594 lines, gives CURRENT3582lines/SHAe719eef161dd6ac839d431c073b10ee7ea782eb750a028e318b003f95916fd80. Latest bytes equal reviewed final728 exactly; priorfull3481+all70changes remain recorded. Idiosyncrasy:2017 EAC-I revisits2014 EAs but selects new households, rural Lourd is households3/6/9 (one-third), Leger the other six, distinct from EHCVM. Its declared national representation excludesKidal, while own BID also reports security-related unvisited EAs without identifying them; do not infer weights’ realized coverage.

CONTENTS quotes the English BID and says “NOT present in this repo.” This remains TRUE of the current library holding despite the newly fetched PRIVATE original. Preserve that prior statement as historical; add a dated availability note ONLY after managed push and cold managed readback. Existing quotations and survey rulings are not replaced as stale.

Fresh GH1574 remains OPEN,0comments:47of51 occupied2017 strata;50otherurban across7regions/51Bamako; completeown51code list needed. Closed219-page five-held-PDF textsearch and separate32-page new own BID textsearch did not locate a complete list. No visualtable confirmation; not proof of absence from images/tables. No labels or source/schema/weight changes authorized.

Workflow contradiction: CONTRIBUTING’s older writer instructions recommend exported AWS secret pairs, whereas current `_s3_writer_credentialpath` explicitly switches the managed remote to the writer INI then restores it. Use the current helper without extracting/printing/exporting secret values; leave unrelated prose unchanged. The gate classifies write from profile presence, so only actual push plus cold remote readback establishes holding.

## §4 Invariants / future write scope
- Existing public original is immutable private700/600,1223133B, physicalMD5a3c822057e8eb09ab756b82095595332, SHA33423a38c73f5459b102ecd7e1eb6f9e830346e839119d2085c71a6730b3e2c0. OfficialURL https://microdata.worldbank.org/catalog/3409/download/47785, fetched2026-11-08T13:52:56Z,1request/0redirect/auth, application/octet-stream attachmentbid_eaci17.pdf. No refetch/parse/renderer.
- Root writer grant must explicitly cover ONE managed `push_to_cache(path,remote='ligonresearch_s3')` and its internal add/remote-modify/push CLI, generated exactsidecar/.gitignore, own privateDVCcache/sitecache/tmp/index and ONE corresponding content-addressed S3 object. No bare CLI/manualsidecar assertion, other files or installed/shared config/cache changes.
- Copy profiles privately600 only aftergrant; source originals untouched, never printed/published; private WB config empty/no APIkey/no GPG/login; clean privatecredentials/config.local in finally.
- Cache.dir and core.site_cache_dir point private, no sharedL1/L2writes/rewarm; no source data/Country/Feature/Make. `core.autostage` may stage own sidecar/.gitignore, not refs/GitHub.
- PushreturnTrue alone is insufficient. A fresh readback config tree containing the generated sidecar but NO PDF plus emptyprivateL1/readonlyprofile/freshprocess must retrieve via normal get_data_file. Verify exactbinaryMD5/SHA/size/PDFmagic; no WB/acquisition fallback. This prevents local-file or writer-L1 hits masquerading as remote availability.
- Only aftersuccessful readback append provenance/availability and review metadata diff. Library source/config/table code and build inputs unchanged. Expectedtable/wave/BTF/FIF footprint0 is a static expectation, not an unrun census claim. No new scientificinterpretation.

## §5 Reuse
Reuse normal managed singlepath helper/credential context and readonly documentation reader, existing installed MAIN/dependencies and exact publicoriginal. New code is only an external frozen transaction/receipt/write-scope wrapper; it does not alter tools or production APIs. No tests/sourcecohort because the task is managed documentation custody; actual finite managed readback is its meaningful validation once authorized.

## §6 Remaining decision/gates
ROOT reviews frozen operator/credential locations/cache/write/readback scope before any writer operation. If writer profile missing/invalid, STOP; no login/install/producercontact or substitute credentials. Final sidecar/provenance/availability cannot be claimed before success. GH1574 remains open for its scientific code-list/visual evidence. Root alone GitHub publication; merged1594 source actors/packets remain separate and immutable; no sentinel policy is reimplemented here.

### Integration admission
Actual GitHub ref independently readfdec032efd4b98ed4b99883c9586f5ef07b7bf3e. The sole own1574ledger is disjoint from incoming four Mali1594 paths; ordinary no-ff merge, no rebase/force. Current CONTENTS3582/hash authenticated as above. Managed calls are multi-step external operations, not atomic remote rollback; failed/partial publication is preserved for review, never silently labelled available.


## §7 Phase 3 — accepted managed holding, 2026-11-08

Root reviewed the exact V2 per-phase receipts and finalization-only proof and accepted the successful documentation holding, with the outer wrapper FAILED1 explicitly preserved. This closes the finite holding operation; GH1574 remains OPEN for its complete51-code stratum list/visual evidence. No label, weight, source-eligibility, schema or default decision follows.

- Original public input:1223133bytes, physicalMD5 `a3c822057e8eb09ab756b82095595332`, SHA256 `33423a38c73f5459b102ecd7e1eb6f9e830346e839119d2085c71a6730b3e2c0`; original source post-witness equal. No refetch or PDF parse in ingestion.
- V1 FAILED before any normal helper: import-time pinned DVC `Repo._btime` attempted its own `.dvc/tmp/btime` marker outside the original parent-write allowance.0managed helper/CLI attempts; profile/config.local cleanup true. The exact path was source-qualified from the pinned caller, not an independently logged V1 pathname. Original V1 and its failure remain immutable.
- Root-authorized V2 permitted only that own-worktree exclusive marker via pinned actual caller/max2 metadata attempts. Normal `push_to_cache('Mali/2017-18/Documentation/bid_eaci17.pdf', remote='ligonresearch_s3', dvc_add=True)` returned true; actual managed commands were exactly add, remote-modify and push. Writer receipt PASS,1private cache file, tracked DVC config unchanged and config.local restored.
- Fresh separate same-head reader tree contained the generated sidecar but NO PDF; fresh process/empty private L1/readonly profile normal `get_data_file` returned the exact original PDF bytes. Reader receipt PASS,2private files (one L1 blob and its materialized PDF),0managed CLI. Both phase guards active with0denials and2exact own btime attempts each. No Country/Feature/Make/survey-source operation.
- Outer V2 wrapper FAILED1 after124.159s, AFTER both phases and append-only SOURCE/CONTENTS updates: its final source-binding loop wrongly expected the old `.gitignore` hash despite that file's explicitly authorized generated ignoreline. It was not repaired/relabelled/replayed. The final original-profile post-stat loop was UNREACHED; no missing individual post-stat counters are asserted. Cleanup in finally actually recorded original PDF post-equality=true, private profiles removed=true and both config.local overrides removed=true.
- Finalization-only Git evidence verified exactly4owned paths, generated `/bid_eaci17.pdf` ignoreline, actual sidecar untouched (SHA256 `8f1950d47046283740890e446299edf82977c845ef272a854719c3c505aa1938`), exact prior SOURCE/CONTENTS prefixes and7unchanged/3authorized source-binding changes. All19130other HEAD/index mode/type/object tuples and ordinary Git working-diff/type/execmode witnesses were unchanged BEFORE this ledger closure. This is Git control-plane/metadata evidence, not a new physical reread of source/PDF/pickle/cache payloads.

Durable evidence root is `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/reports/`. Original V1 preparation/failure: `1574-mali-managed-bid-ingestion-proposal-20261108/`. V2 frozen operators and grant: `1574-mali-managed-bid-ingestion-proposal-v2-20261108/` (manifest `4bffab5f5265f526aa41ebdc6b6213a9498c3c91604c0452e16d8f952c99aad6`). Actual private per-phase/final receipts: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/private/1574-managed-bid-ingestion-v2-20261108/`. Root-accepted qualified finalization: `1574-managed-holding-finalization-evidence-20261108/` (manifest `7195c8cc23247c23ea84eaf91ce5a8cd65d2cb4174a999799e0d5a103359c68c`). No credentials are retained in those receipts.

The prior “NOT present in this repo” note is preserved as historical; the dated append records actual availability. Closed32-page textsearch did not locate a complete51-list; visual investigation is pending, with no absence-from-images/tables claim. No new API/hash/fullCI/source/PDF-reader campaign was run for finalization. Root owns current-development integration/GitHub publication; final source commit contains only the four authorized holding paths and this ledger.
