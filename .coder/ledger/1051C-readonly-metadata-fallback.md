# GH1051(C) metadata fallback after positively local L1 write denial

## §1 Scope / authority
ROOT2026-11-09 selects issue1051(C) proposed remoteexists/HEAD fallback, PROPOSAL.md at reports/1051C-readonly-metadata-fallback-design-20261109. Base7eab1f6843171f3dbad1c6d12bdd794aa03e5f1f. Internalopt-in only from source_fetchable_fast; localEACCES/EROFS duringmkdir/tempwrite/rename mayqueryalreadyresolved genericremote.fs.exists onexistingtwoMD5keys. Notdecodeproof/scientificchange; no streaming/reader/Repo.fetch/index/WB inprobe. Ordinarywarmer callers retainNone/besteffort/retry/layout/cleanup.

## §2 Prior art / reuse
Git+rg floor, inherit STANDING §§2–4 and sanctionedIO/cachedefs. Independentlyreviewed diagnosis6e4aa6123 uses actualsource_tier/source_fetchable_fast/_ensure_dvc_pulled/_dvc_cache_path/_get_file_with_retry, _probe_source anddegradedverdict/readgate. Reuse finiteTinyCSV[7,11]/FSleaf fixture andprivateL2 readgate receiver machinery from tests/test_gh809_hashless_country_parquet.py; do not copyold33P/fourintentionalFAILoracles intoacceptednewexpectations. Existing1051diagnosis historicalnotreset. No realcountryfixture needsCONTENTScorpus read.

## §3 Definitions / engineering boundary
Only positively local cachepermission failure: directmkdir/rename operationcontext, or get_file OSError.filename namingEXACTdeclaredtemporarydestination. EACCES/EROFS onremote/config, missing/ambiguousfilename, diskfull andunrelatedIO doNOTtriggerHEAD. Remotealreadyresolved beforecachemkdir, existingflat/files-md5keys authoritative, max2metadataexistschecks/noextraretry. Availabilitybool onlyadvisory, processmemo/once-per-key retry unchanged.

## §4 Invariants
No ordinarycalleroptsin; ordinaryget_dataframe maystillstreamviaDVCFS whenL1unwritable. No network/raw/sharedcache/credentials/dependency/schema/exclusions/globalfailurestate/newuserknob. Writable/cachedpath doeszeroHEAD. Cleanup remainsbeforeexit, no partialtemp leak. Unavailablemetadatafalse/errorsfalse withoutclaimingdecode. Sharedensurebodyhashcostmeasuredonceafterfinalsemanticfreeze, no sharedrewarm. CIV product/tests preservedexactparent.

## §5 Implementation / verification
Extendensure keyword-onlyprivateoption+smalllocalpermission/remoteexists helpers andfastpredicate; no newreader. Probe/cachingdocs reflectexception. Newinvented controls coverlocalmkdir/temp/rename2errno×2layout, remoteambiguous/refused/outage/unrelatederrors/badsidecars/noextraHEAD/cachehits, memoandactualordinaryTinyCSVstream/privateL2heal. Existingpure675/ensure suites relevant. Existing675 one-argwarmerfake mustaccept the explicitnewinternalkeyword andasserttrue (fixturecontractupdate only, alloriginalassertions preserved). No generalfallbackintrospection/hack tokeeptheobsoletefakeworking. Freezebeforeoneappropriatefocusedcohort, thenoneexistingmetadata census againstclosedCIV3de map carried7eab bysemanticidentity; no baseline rerun.

## §6 Open boundaries
No concrete machineryconflict: genericfs.exists isexistingbackendAPI, alreadyresolvedremote hasbothsourcekeys. Exceptionfilename-onlyclassification conservative; anambiguous/localalias error withoutpositivefilename retainsoldfalseadvisoryratherthanguessing. Existence doesnotproveactualstreamdecode/rebuild success; testboth. ROOTownsindependentreview/configuredfullCI/merge.
