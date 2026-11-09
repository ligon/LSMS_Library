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

## Phase 3 — closed verification and bounded result

Runtime/source freeze: 4390a52b9ad7461cab0d0198cb9c2d33099ed3f5. First new commit
9ec15516ff2bc75c1c4923cbe76a105fd0564b94 was ledger ONLY over cf8. This final
checkpoint is ledger ONLY; all tested/censused nonledger objects stay exact.

- Four existing seams — OK (§2–§5): `_probe_source` uses a context-local private
  outcome while preserving one-argument source helper substitutions and original
  warmer keyword signature. Deferred False is never source-memoized; actual
  completed False retains the explicitly separate legacy limitation. Ordinary
  warming does not read advisory context/window, and keeps None/reader results.
- S3 scope/route/evidence/window helpers — OK (§3–§5): normal fs_args preparation
  plus populated native/session/client/signer metadata, static/anonymous and
  nonrefreshing shared-profile credentials, retained backend SELECTED for equal
  later scopes. No fresh native/client/discovery for identity. Actual failures
  bind the authority and requested object/discovery route. All exposed attempts
  across both layouts must be positive connect evidence; mixed responses, read,
  closed, auth/missing/local errors and pre-cleanup temp progress veto. Same
  five-second completion deadline, no sliding, max16 decline at capacity, short
  locks/no IO or sleep, lazy reference release/no explicit client close.
- Generations/resources — OK (§4): owner/root/config/environment and populated
  native/session/client/auth changes fail open; reset/reachable response prevents
  older in-flight publication. PID/fork replaces inherited mutex. Opaque runtime
  state object follows existing nonconstant-state conventions, so activation and
  reset do not become build constants; no fingerprint exclusion was added.
- A/B/C/E and retry contracts — OK (§4): entire original `_is_transient_fetch_error`,
  `_remote_md5_available`, `_local_cache_write_denied` and every other existing
  local_tools AST node exact except the three authorized seams; country.py only
  `_probe_source` changes. Primary loud errors/config guards, E coverage unknown,
  B ALL-within/ANY-across and legacy footers, once-key/just-written bounds retain
  actual selected companion coverage. C1 always runs its unchanged max-two exists
  branch after observed local denial, active GET window included; HEAD never seeds.

ONE ordinary source-free focused cohort CLOSED0: **114 passed = 103 new + 11
selected existing**, 0 failures/errors/skips, 9.72 pytest / 15.777 outer seconds.
Source/tests/selection/literal oracles frozen before execution. D1–D17 represented
with ROOT B+C1/default-profile corrections. D1 Alpha6 GET/4 sleeps, distinct Beta0
GET/0 HEAD/0 sleeps; structured Endpoint positive2 GET/0 sleeps. Real backend
configuration preparation reads only invented profile metadata; native provider
leaves are fake. Static/anon/profile positives and refreshing/unknown negatives,
configured-default retention without new native construction, route/config/auth/
owner/root/PID changes, full retry/layout/progress/error vetoes, local/L1 priority,
expiry/response/reset/concurrent in-flight/cap16/reference lifetime and safe fork
are discriminated. Actual ordinary CSV stream decodes [7,11], warmer None and
original final exception identity hold. Meaningful companions: 2 legacy675
compatibility, 3 A primary errors, 5 E/B coverage/readgate cases and 1 C ordinary
stream/private L2 heal. No historical56/E71/A78/B41 bulk replay or new baseline.
Three controller/worker receipts assert mounted interpreter/own cwd/PYTHONPATH/
config-root, absent optional observation exports and only loopback under
unshare-rn. Two loadfile workers max; BLAS/OMP/MKL/build1. Config/data roots0files;
only invented fixture files in owned private tmp. Integrity/cleanup unconditional.

ONE existing candidate-only metadata census CLOSED0 / **136.693 seconds**, versus
supplied closed E header7384234791251cd33352c4df0f35a823a9fbc482, baseline SHA256
6618775057aaadf6f47ba4a584aed3ccd2acd6d479b44a573a447dc8490b312c. Supplied CARRY
proves cf8 ledger-only over that code. Baseline never replayed. Existing operators
retain all guards and tracked config-CSV allowance; only interpreter-prefix and
report namespace substitutions, byte-exact inverse proof. Fresh owned0700 roots,
absent credentials, unshare-rn. Bounded metadata constructors/config fingerprints
only; no actual table/API/getter/build. **452/452 country-table, 1885/1903 wave,
37/37 build-transform, 124/186 framework-import hashes change**; ALL keysets and
14 nonhash fields exact, all18None exact, errors0/denials0/privateconfigdata0.
Broad harmonized-cache invalidation disclosed; no rewarm/exclusion/schema bump.
Candidate map SHA256616632e39e5f95c00f6d261a395f377768a0081a161e061330aee388c1ab0ef6.

Infrastructure launch evidence is qualified: detached launch returned PID3533143;
a later ps result had no row, its controller log was empty and hash.log/receipt/
map were absent at that checkpoint. No terminal return code or exit cause was
recorded, so these observations do not establish why it stopped. Both launches
used the SAME evidence/hash.log exclusive-x path, opened BEFORE spawning the
collector; the file was never removed. This establishes at most one collector
start even if a delayed controller later resumes. The tracked foreground
controller and actual collector closed0. Original empty log/preflight preserved;
no further runtime/census was launched. No failed behavioral test/census assertion,
source revision, weakened guard or passed-case replay occurred.

Limits: eligibility is demonstrated on invented default/profile/static metadata,
not actual operator credentials. Unknown/refreshing/proxied/unsupported routes
retain existing cost. C HEAD-only offline cost and completed source-False memo
recovery remain. Savings are sequential eligible wrapper-call reductions after
first sequence, not SDK HTTP attempts, first-sequence savings, field latency or
universal outage detection. No real raw/network/API/acquisition/Make producer/
shared-cache build or country config/scientific changes. ROOT patch review,
independent peer, configured CI, integration/publication/merge remain pending;
whole1051 is not closed. Evidence: supplied dispatch evidence directory, five
mandatory owned regular0600 files plus source/plan/logXML/map/inverse receipts.
