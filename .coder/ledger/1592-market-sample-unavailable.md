# GH1592 — market labels when sample is unavailable (diagnosis/charter only)

## §1 Task and authority

ROOT authorizes diagnosis and a concrete charter ONLY from728992c7697bdf25286e3d63a7ae70092829a843. Fresh GH1592 OPEN/0comments says market= with sample missing warns that cluster_features labels will be used, then raises KeyError missingv. Either an actually defined cluster fallback or an honest causal refusal is allowed. No product/default/source/config change is authorized before ROOT reviews the charter. Generic invented fixtures only; no country semantics, sources, native/API/Make/warm/acquisition/GitHub or prior-source replay.

## §2 Existing machinery (git/ripgrep floor)

- Country._add_market_index, country.py:3414: household sample[column] on(i,t) first, cluster lookup on(t,v) for missing labels; catches OSError/DvcException sample access and warns; if v absent, reuses _join_v_from_sample then unconditionally projects v. Actual methods can be borrowed by an invented stub without Country construction.
- Country._join_v_from_sample:3228: existingv is untouched; FileNotFoundError or sample lackingv returns unchanged input with SampleUnavailableWarning; guards/refusals propagate. Cached unavailable state avoids repeated sample acquisition on later calls. Do not change this contract.
- Country._market_lookup:3145: per-wave cluster_features metadata-provider scan, categorical label mapping, full(t,v) lookup/cache; unavailable/malformed wave diagnostics exist. No cluster table can infer a household'sv without a real household-to-cluster relation.
- Country._report_unlabelled_markets:3553: keep unresolved m asNA, existing NullIndexKeyWarning/NullIndexKeyError/LSMS_READ_STRICT report; no row dropping or fake labels.
- tests/test_failure_is_not_absence.py::TestMarketLookup: missing sample OSError with inputalreadycarryingv genuinely labels from cluster; nonavailability refusals propagate. ::TestVJoin preserves no-v degraded behavior.
- tests/test_add_market_index_keeps_unlabelled.py and test_add_market_index_dedup.py: invented bound-method stubs, householdlabel priority/fullkeys/tallrows/no duplication/attrs/unlabelled reporting.
- _build_registry._closure_parts and tests/test_failure_is_not_absence.py::test_no_handler_edit_can_move_a_build_fingerprint provide read-path closure witnesses. Measure actual tagged closure reachability before any patch; do not claim full450/1895cache census or choose exclusions here.

## §3 Definitions/contracts in force

Inherit STANDING§§3–4 and CLAUDE§sample and ClusterIdentity: sample owns household(i,t)->v; cluster_features owns(t,v)->geography. v must not be fabricated from i, table order, cluster row counts or matching-region guesses. _add_market_index must preserve householdlabels first, actual v identity, existing canonicalkeys, tallrow multiplicity/attrs and keptNA markets. No region/cluster scientific choice.

Applicable skill/contracts read: prior-art-ledger, cross-country-features, add-feature/sample. The sample skill's prose says missingv falls back to household_roster, whereas current Country uses sample; record this contradiction as prior-art evidence rather than silently trusting prose. No skill or countrydoc edit is part of diagnosis. Current issue explicitly distinguishes absent source from a real config/data guard refusal; don't swallow the latter.

## §4 Invariants and hypotheses to test

Hypothesis from static path: sample FileNotFoundError is caught/warned in _add_market_index, then _join_v_from_sample catches it independently/returns inputnov, and [['i','t','v']] raises an incidental KeyError before any cluster lookup. A cluster lookup only supplies labels once an actual v exists; sample missing plus no v provides no causal mapping. This must be reproduced, not assumed.

Positive controls: already-v with sample absent and well-defined cluster provider; sample available with household label and nov; sample suppliesv and no label; conflicting HH/cluster labels retain HH; tallinput and samev across differentt; unknownlabels keepNA/report under existing strict switch. Refusal controls: actual nonavailability versus ValueError/KeyError/RuntimeError guards, sample withnov, no sample declaration. Input/attrs/masks unchanged; no real Country or provider source call.

## §5 Reuse/charter options

REUSE existing householdlabel priority, _join_v_from_sample, _market_lookup and null-indexreport machinery. No replacement loader/parser/join policy.

A. Honest causal refusal is the minimal likely route when sample cannot be read and the input has no actualv: preserve the original availabilityexception/cause, explain missing household-to-cluster relation, and do not promise a cluster fallback that cannot run. Keep existing successful cluster fallback when v already exists. Exact exception/warning behavior awaits the new invented witness and ROOT review.
B. Real fallback only where an existing declared household-to-cluster mapping already suppliesv. Do not scan actual countries or invent a relation to make B appear possible. A broader mapping/provider policy would need separate explicit design.

No implementation is selected here. Any eventual product edit must preserve healthyframes/errors/warnings and receive independentreview/hash/API/CI as appropriate; no sourcecampaign follows from this charter.

## §6 Gates

This initial ledger is committed before diagnostic code. Next: new source-free invented-provider witnesses, exact guard/import/privatefalsecredential/unshare receipts, actual metadata-only taggedclosure impact, then concrete external CHARTER/REPORT + updated ownledger. ROOT reviews before any product edit. 1578 independent review takes priority once frozen; Nigeria1518 correction review is already closed separately. No additional agents.

## §7 Executed diagnosis and concrete charter (no product edit)

At initial ledger74af961d9165607118538183def195c2fbc0147e,12new invented controls PASS/0skip in7.28s pytest,13.78s wrapper. All3controller/worker guards active0denials; private datafiles0, credentialavailabilityfalse, unshare-rn, exactcheckout/import/countries/mainimage, BLAS/build1/pytest2loadfile. Bound original methods onto a plain invented stub; no Country construction or actual provider/getter/API/build/cache/source operation. No prior cohort replay.

Confirmed: FileNotFoundError+nov makes2sample calls/0clusterlookup calls, emits falsefallbackpromise plus servedwithoutv warnings, then raises incidental KeyErrormissingv. OSError+nov also promises a fallback nevercalled; second sample call raises originalOSError. Healthyalready-v+missing source calls cluster once and preserves full(t,v)/tallrow values. HealthyHHlabels work evenwithoutv and overrideclusterlabels; sample-providedv/noHHlabel uses existingdedup without multiplication. GuardValueError/KeyError/RuntimeError preserve exceptionidentity and emit no fallback warning. A returned sample with neitherv norrequestedlabel is another pre-existingnovshape path, not a missing-source causalexception.

Baseline metadata reachability: all16tagged runtimeentry functions,141closure nodes, none contains Country._add_market_index/_join_v_from_sample/_market_lookup or SampleUnavailableWarning. This establishes the current taggedbuildroute boundary, not an executed450/1895globalhashcomparison or an unrunframeworkimportcensus. A later candidate must verify exactmetadatahashimpact; no hash-exclusion edit is justified.

External CHARTER proposes minimalA for ROOT: within existingavailabilitycatch, if the input lacksv, raise the ORIGINAL exception before promising clusterlabels; do not retry sample or inventv. Preserve existingalready-v clusterlookup, HHpriority, actualrefusal/errors, categorical/fullkey/attrs/tallrow/NAmarket contracts. Warning wording on attemptedexisting-v fallback may be made honestlyconditional without asserting success. No automaticrealclusterfallback is defined for a householdnovframe because canonicalcluster_features supplies(t,v)->label, not(i,t)->v.

The returned-sample-nov/labelmissing shape guard is explicitly a ROOT scope choice: a contextual causal ValueError could replace its incidentalprojectionKeyError, but it is not silently bundled. Likewise no changes to _join_v_from_sample's general unavailable-source degrade contract, source/provider semantics, normalizedv rules or null-market strictness are proposed.

Phase3 OK anchored§§2–4: existing producer/lookup/join machinery reused for diagnosis; no reinvention/contradiction/productfix. All library/config/tests bytes remain exact728992; onlythisledger changed. ROOT reviews externalcharter before any implementation. No real scientificcountryfact found; countryCONTENTS not scoped/modified. Sample skill's roster-versus-sample prose discrepancy remains a quotedpriorart caveat, no stale-doc assumption or unrelatededit.

## §8 ROOT selected minimal OptionA — implementation ledger FIRST

ROOT approved ONLYOptionA on freshly authenticated GitHubdevelopmentfa76ee4364d3e27cdbe08f0bb1d2014b060a8f9b. New isolated authorbranch fix/1592-market-sample-unavailable-20261108; old diagnosis74af/845291, all12baselinewitnesses and otherlane actors remain immutable. Country.py/_build_registry/failure-contract tests are byte-exact728992 at this newbase; no country scope/source fact is introduced.

Implement within existing _add_market_index availabilitycatch(OSError,DvcException): if the input has no existingv column after reset_index, bare-raise ORIGINALavailabilityexception before falsefallbackwarning, with no second samplecall. Existingv (indexorcolumn) still enables its existing full(t,v) lookup, including NAmarket/strictbehavior. Its warning may honestly say attempting rather than promising labelling success. No newparser/provider/map/v/nullkey rule, no _join_v_from_sample or globalhash-exclusion change. Returned sample lackingbothv/requestedlabel remains the unselectedseparateshapequestion and its current behavior stays unchanged.

Reuse the actual method, exceptionclasses, sample/cluster provider seams and nullindexreporting. Add NEW meaningful invented-source regressions and exactcurrentBASEfunction parity on healthy HHpriority/samplev/tallrows/alreadyv/error identities, with strictprivatefalsecred/unshare/sourceguards. Do not replay completed12baselinecohort/native/source/API/Make/warm. Metadatahashreach/scope and strictAPI as appropriate remain source-free finalgates, independentreview before publication. ROOT owns GitHub; no automatic scientificfallback or extra implementation selection.

## §9 Phase3 — final anchored verification, ledger-only freeze

Runtimeauthority remains838d20412d9ad3f8bb5725d3a9b4137172b010c9, basefa76ee4364d3e27cdbe08f0bb1d2014b060a8f9b. This final ledger-only commit is made in a NEWseparatedetachedfinalizationtree; author838,actualCIverifycheckout/ref and externaloperators stayimmutable. Alllibrary/config/tests/buildinputs/modes/objects must remain exact838; externalBYTE-PROOF records the exhaustive comparison.

OK (§§2–4/8): only _add_market_index gains the selectedbareoriginalavailabilityraise beforefalsewarning/no secondsamplecall withoutv, plus truthfulattemptingwording for existingv and boundedmethoddoc. Allotherhelper/exceptionguard/warningclass/strict/nullmarket/default/signature/exclusion behavior remains exact. No returned-sample-shape repair, sourceprovider/parser/vmapping/country/scientific rule was added. Returned sample withneithervnorrequestedlabel remains the documentedoldKeyError; presentall-NA v retains its existingroute rather thanbeinginferredabsent.

Authorverification:38distinct focusedPASS (24newtracked+7existingpureNAmarket+7freshcurrentBASEparity/static),0skip/deselected,21.52s pytest. ThreeNEWnegatives fail onunchangedBASE5051 beforefix; old12diagnosiscohort notreplayed. Healthyfullframes/index/dtypes/masks/attrs/errors match exactfa76method; source/refusalexceptionidentity and healthyHHpriority/samplev/tallrow/full(t,v) fallback protected. Privatefalsecred/unshare/MAINvenv/importidentity/BLASbuild1/pytest2loadfile; source/API/writeguards active0denials/datafiles0. StrictAPIaudit0, all15skillslisted, nountrackedhashedinputs.

ActualROOTapprovedcandidate-onlyFULLmetadata census completed0:0/450countrytable,0/1895wave,0/35build,0/186frameworkimport hashchanges;38countries/18Nonecells and public553/2313/39surface unchanged. Reused ROOTadmittedCLOSED06d0baseline44b62fffa738f66d24251e530748e179182164f36d99a7c9bbc0f6cb281240f8 via authenticatedfa76/BFAunion; NOnewbaselineactor. Establishedoperatorbyteexactdd7667, allsource/write/profileguards and trackedMalawiCSVexception retained,0errors/denials/private datafiles. Country(preloadFalse)exception applied ONLYthisguardedmetadataenumeration; unit/reviewfixtures use noactualCountryconstructor.

Independentreview CLOSED/PASS exact838:35NEWdistinctcontrols/noP1P2,6activeguards0denials/datafiles0,cleanreadonlytree. Reviews originalexceptionobject/cause/callcounts, nofalsewarnings/cachepoisoning, healthyretry/HHpriority/customcategoricalmarket/fulltimekeys/tallrows, presentallNAv, lookupfailure afterhonestwarning, NAmarket/strictledger, unchangedunselectedshape, exacthelper/signature/normalizedAST. A nestedreviewwarningcollector harness failure is preserved; onlyaffected/newexternalcontrols rerun with strongerzero-warning checks (15retained+20targeted=35distinct), no authorfix/old12/census/source replay. DurableindependentVERDICT/evidence at reports/review-1592-market-unavailable-20261108.

FullprivateofflineCI41020555 is PENDING at this docsfreeze (2026-11-08T07:24:46.653285+00:00); no terminalreceipt exists yet. No fullsuitepass/count is claimed. Actualfrozen838run uses8CPU32G/free--cold,4loadfile/fulltests with privateemptyL1/config/data beforeCIimport,falsecreds/unshare/ownmount/BLASbuild1. Its later terminalcounts remain an externalreceipt; code/test byteproof extends838 evidence to this docs-onlyhead.

No newtests/metadata/source/cohort/native/API/Make/warm rerun occurs for this ledger-onlyclosure. No source/countryfact or whole scientificfallback was selected. ROOT owns finalconfiguredCI/GitHub/merge; noGitHubwrite here. Originaldiagnosis, expectednegativefails, independentharnessfailure and allfrozenoperators retained. Phase3 OK anchored on selectedauthority and actualbounded evidence; fullCIstatus remains qualified above.
