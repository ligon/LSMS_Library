# GH1051D cross-source fast-probe connection-failure window

Search tier: git + ripgrep/AST floor; GitNexus tools unavailable. Base cf8cdd998b44b1503ac522dbcd15111e22196300. ROOT-DECISION.md (913ea47c…) overrides earlier Option A/default exclusions; admitted D-PROPOSAL.md SHA256 93ca350ca5a34a40cdab3e634087a6a42f92e1c41489cb76898791caa2ca654e. Historical D56 and Ddesign remain immutable, never replayed.

## §1 Task
Implement B+C1: advisory-only 5.0s non-sliding window after the complete existing two-layout sequence uniformly fails with positively identified connection-establishment evidence. Retain and select the used eligible S3 route, including normal default/profile-backed demonstrably nonrefreshing authentication. No country fixture or scientific change.

## §2 Existing machinery
Inherit STANDING §§2–4, actual identical AGENTS.md/CLAUDE.md, and 1051 A/B/C/E ledgers. No separate 675 or D ledger exists on this base; 675 implementation/tests, supplied D proposal/design report provide prior art.
- local_tools.source_tier/_dvc_cache_path/source_fetchable_fast:611: local/untracked/L1 first, advisory bool; existing675/C tests.
- _ensure_dvc_pulled:350: sidecar/md5, both cache layouts, ordinary None warmer, C local-denial exists fallback; C tests.
- _get_file_with_retry:290/_is_transient_fetch_error:263: retry/cleanup/delay authority; existing ensure tests.
- country._probe_source:9494/_SOURCE_PROBES: completed bool memo; 675/C tests. _degraded_cache_verdict/_mark_degraded_written: table/wave once-key/just-written bounds; B/E tests.
- Installed DVC get_remote constructs fresh wrappers; dvc_s3.fs caches a native s3fs on each wrapper; s3fs skip_instance_cache. Native _s3/_s3creator/session/client signer credentials can establish a nonrefreshing generation without getter/refresh/discovery.
Call-site sweep: ensure called by reader, dictionary reader, data_access and four country scripts; retry called only by ensure. source_fetchable_fast only production caller is _probe_source. Shared ensure/retry closure is HIGH fingerprint blast radius; measure once, no exclusions added.

## §3 Definitions
STANDING §3, CLAUDE Cache Behavior, canonical data_info.yml remain authoritative. ROOT selection and admitted proposal sections “What counts as endpoint-connect evidence”, “Placement and behavior”, “Finite policy” define this engineering change. Retry eligibility is NOT outage eligibility. EndpointConnectionError needs structured connector/DNS cause; ConnectTimeoutError needs validated same route. Responses/temp progress veto before cleanup. HEAD never seeds; C1 max-two exists unchanged. Static/anonymous/profile eligibility uses already-resolved metadata only, no credential bodies, files, refresh, identity HEAD or fresh native client.

## §4 Invariants
STANDING §4 and A/B/C/E: primary errors/config hard guard, unavailable/coverage-unknown grading, B ALL within/ANY across, legacy drops, once-key and just-written remain. Ordinary ensure defaults context=None: no state consultation/publication/suppression; None and decoder results exact. Suppressed False is transient, not _SOURCE_PROBES; completed actual False recovery intentionally unchanged. Lock only transitions; never IO/sleep under lock. PID/fork/reset/config/session changes block old publications. Lazy release, no close in-flight client, max16 scopes decline at capacity, suppressed hits never slide deadline. No schema/SDK/default/exclusion/config/data changes, real raw/network/API or old cohorts.

## §5 Reuse decisions and literal controls
Extend four existing seams; new tiny private S3-specific evidence/window/outcome helpers because existing source bool memo cannot express cross-source route continuity or transient deferral. Reuse full retry/layout/local/metadata/streaming paths. One new portable source-free test module uses actual seams and invented Alpha/Beta ordinary FS/DVC leaves. Literal prospective matrix below is inherited with ROOT corrections: D3 demonstrably nonrefreshing default/profile route IS eligible; D10 selects only C1.

| ID | Setup and required outcome |
|---|---|
| D1 | Stable eligible scope; Alpha has uniform ConnectTimeout at both layouts: 6 wrapper GETs with default 3 attempts, 4 library retry sleeps. Distinct uncached Beta within 5 s: 0 GET/exists; no permanent Beta False. Existing SDK internal requests are not counted. |
| D2 | Uniform positive EndpointConnectionError with structured connector cause: 2 wrapper GETs, no library retry sleep; Beta shares suppression despite fresh Remote/backend/native instances. |
| D3 | Change backend class, endpoint authority, bucket/prefix, owner, roots, auth token/config generation or PID: Beta runs its own sequence. Same remote name alone never matches. Unprovable default endpoint or dynamic auth never suppresses. |
| D4 | First layout miss/connect-fails; second succeeds: positive result, no window. First connect-fails; second misses/auth-fails/read-times-out/closes: no window; Beta remains independent. |
| D5 | Earlier library retry is read/closed/auth/local failure, final error is connect: no window. Temp exists, even empty, before cleanup or stat is denied: no GET window. Wrong/missing endpoint route or generic connection superclass: no window. |
| D6 | Each excluded raw error and SDK-translated auth/404/EIO shape: no cross-source suppression. A local EROFS cannot masquerade as a remote outage. |
| D7 | During active scope window, Beta gains untracked local file or either L1 blob: True with 0 remote operations. Changing Beta md5 rechecks that md5's local tiers, without inventing an object-specific outage key. |
| D8 | Beta is suppressed at t=1 and t=4.9; expiry stays t=5. At t=5 Beta can fetch successfully with no manual `_SOURCE_PROBES.clear()`. A source actually probed False retains the separately documented baseline bug. |
| D9 | C direct mkdir/tempwrite/rename exact EACCES/EROFS, no active window: existing eligibility, either-layout positive HEAD and max-two exists bound preserved. Remote/ambiguous permission, EIO/ENOSPC: 0 fallback HEAD. HEAD-only failures do not seed a window in this proposal. |
| D10 | C1: active GET window plus observed local mkdir denial still runs the original exists sequence. C2 alternative: same setup is transient suppressed; both metadata keys are checked after expiry. Independently pin the selected behavior; do not call both unchanged C. |
| D11 | Seed a window, then call ordinary warmer/reader/build with both local caches absent: full original two-layout/retry/stream route still runs; None contract and exact decoder result are preserved. No outcome context reaches it. |
| D12 | Multiple table/wave keys: each keeps its existing one-rebuild bound; a just-written degraded key still bypasses probing. Window expiry/reset does not grant another rebuild to an attempted key. |
| D13 | Reset or reachable response while older failed probe is in flight: generation blocks its late publication. Concurrent first probes are allowed to finish and are not claimed to be single-flight. |
| D14 | Scope capacity reached: no cross-scope eviction-induced guess and no insertion beyond 16; decline suppression for the new scope. Process restart has no outage record. No credential/query/exception payload is logged. |
| D15 | Tracked default S3 route, no endpointurl: A saves exactly zero operations. B retains the used route after Alpha's uniform failures; equal-scope Beta has 0 remote wrapper calls, without constructing its fresh native client for identity. |
| D16 | Default route bucket cache empty/cached/changed, failure authority/client mismatch or session closed/refreshed: suppress only a demonstrably retained/selected unchanged route. No identity/discovery HEAD is added. |
| D17 | B expires/resets while another source owns its backend: release the window's reference, leave in-flight work intact, use fresh resolution for the next actual probe. Suppressed hits never prolong resources or False memo. |


Freeze production/tests/selection/oracles before ONE ordinary source-free cohort; all project observation exports explicitly absent controller/workers, 2 loadfile workers max, mounted prepared interpreter, private roots/absent credentials/unshare-rn. No automatic rerun. Then ONE candidate-only existing metadata census vs supplied closed E SHA2566618775057aaadf6f47ba4a584aed3ccd2acd6d479b44a573a447dc8490b312c/header73842347, CARRY proves cf8 ledger-only. ALL maps/keysets/14nonhash/18None/errors/denials, actual footprint. Final ledger-only child conserves entire frozen nonledger tree.

## §6 Open questions / authority
No blocking contract question identified. ROOT expressly authorizes immediate continuation after this first ledger-only commit; no permission wait. Any genuine source incompatibility goes to absolute QUESTIONS_FROM_AGENT while independent work continues. ROOT owns patch review, independent peer, CI, publication/integration/merge. No whole1051 closure/default-operator eligibility/field latency claim.

## Phase 3
UNFINISHED; behavioral tests and census UNRUN.
