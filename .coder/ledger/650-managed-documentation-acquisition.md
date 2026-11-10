# GH650 managed published documentation acquisition

## 1. Scope and authority
ROOT selects the source-only proposal at B/reports/650-add-wave-documentation-design-20261110/PROPOSAL.md: public fetch_documentation(country,wave,catalog_id,*,push=True,remote=None), default best-effort add_wave adjunct and documentation=False escape. Baseed1f7e540b7144bd71ddd97280311e552989f97d. Ledger precedes implementation. Synthetic/mock acquisition only; no real network, documents, raw acquisition, DVC push, source API or shared environment writes.

## 2. Existing machinery
Read prior-art skill, STANDING, full CONTRIBUTING.org and add-wave skill,1115/1128,1149,1276/1283 ledgers/source/tests. Git/rg search tier. Reuse push_to_cache_batch (writer credential context/lock retry), acquisition context, urllib/tempfile/atomic replacement patterns. Existing get_data_file retrieves held documents but WB fallback extracts data archive basenames, so it cannot acquire catalog documents. _find_stata_zip_url returns None both for failure and no ZIP; documentation needs distinct discovery status. Preserve raw _AcquiredRelease/_ACQUIRED_RELEASES/_record_acquired_release and raw list returns. Relevant tests: wave_provenance acquisition/completion classes, staged_reacquisition, gh1276_acquisition_subdir, data_access batch publisher controls.

## 3. Definitions
Inherit STANDING/CLAUDE IO and provenance rules. Published document holding is not fielded CAPI/export correspondence or current remote freshness. Minimum: source/citation, actual terms or cited terms, and published questionnaire/manual/technical/report/DDI with explicit missing/retrieval status. ROOT chooses atomic Git-managed Documentation/DOCUMENTS-{catalog_id}.json receipt. Raw HELD identity remains data-release-only; document failures never falsify or erase it.

## 4. Invariants
Separate document membership/batch. Default adjunct failure is explicit best effort, unchanged raw success/list/stamp. push=False performs no DVC operations and labels local-only. Never overwrite another catalog receipt/source/terms/unowned differing file/symlink; validate destinations before placement and use atomic per-file writes. Current held receipt/resource id is not freshness proof. Unknown API/HTML layout is discovery_failed, not an empty listing. Bounded Related Materials fallback is required by closed WB6161 API400+successful resource inventory; no crawler/dependency. Existing held Niger docs and Ethiopia BID/PR1685 three forms are not an acquisition campaign here.

## 5. Implementation and verification plan
Add bounded discovery/direct-resource staging and report helper inside data_access, one public acquisition entry point, one default add_wave adjunct, narrow contributor/skill guidance. Mock exact resource API/HTML/direct bodies and publisher. Stub adjunct explicitly in unrelated raw fixture setups. New tests pin default call, escape, raw-success/doc-failure, recognized empty versus discovery failure, catalog ownership/collisions, preserved bytes, revised/identical document, push=False and partial/wrong-path push. Existing acquisition companion selection is finite and source-bound. Freeze before one private guarded focused cohort; preserve failures/correct affected controls only.

## 6. Status
Implementation not yet started; no tests executed. No scientific or data-default change. ROOT owns review/PR/merge.
