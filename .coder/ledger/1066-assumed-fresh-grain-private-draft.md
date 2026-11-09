# GH1066 — stored grain replay; policy selected 2026-11-09

Current status: the user selected the policy below; earlier PRIVATE/HOLD
statements record the original unselected prototype and remain historical.

## §1 Task and hold

Isolated from actual development1c1ef12da4f592e1899db67cf937251aa890ac86.
ROOT authorizes a concrete recommended replay prototype and synthetic evidence
only. GH1066 remains OPEN/TierC; the maintainer warning-replay decision is
unanswered. HOLD this private candidate: no ready PR, GitHub mutation, merge,
resolution claim or timeout-as-consent. No country inputs/metadata, raw/DVC,
network, shared caches, weights, corpus, census or full-suite work.

## §2 Existing machinery — REUSE (rg + Git floor)

- country.py Country._read_assumed_fresh_cache5037: successful coveredmanifest
  and legacyfullwave branches; both replay date audits, neither grain.
- Country._read_l2_country_cache4990: normal read uses
  _grain_reports_for_waves(read_parquet_grain_audit(...),waves) followed by
  _replay_grain_audit; tested by tests/test_grain_collapse.py and1040 gates.
- _replay_grain_audit9252 and _record_grain_report9222 re-file from_cache=True
  and reuse the single warning/strict emitter; no replacement warning engine.
- load_dataframe_with_dvc6552 clears table grain/date ledgers before reading.
  Assumedfresh bypasses that initializer. A successful assumed hit should
  clear its table's old ledgers before replay in this proposed matching mode.
- Existing _replay_dates local helper clears _DATETIME_LEDGER and preserves
  waves-filtered replay_datetime_audit. Reuse it as a shared _replay_audits.
- Existing _restrict_to_waves full-request pass-through preserves genuine NULL
  keys and outside-wave rows; covered subsets select requested t. Both stay.
- local_tools.to_parquet/read_parquet_grain_audit and date/coverage footer
  helpers are the existing sanctioned synthetic writer/readers.

## §3 Definitions and authority

STANDING.md/CLAUDE.md and SkunkWorks/grain_aggregation_policy.org remain the
baseline; no reducer, missing-key, stored-content or completeness rule changes.
Existing tests/test_grain_collapse.py9–13 says destructive collapse is loud
and its signal survives the cache. Existing1045 ledger explicitly leaves the
assumedfresh grain gap separate. Current1066 body separates merged1105 write
repair from this read-policy residual. Its sole historical comment says
"Fixed ... by PR1105 ... Left open until ... master"; it is NOT approval of a
new assumedfresh replay contract. Current fallback write6848 already supplies
grain_audit: DO NOT redo1105.

## §4 Invariants / proposed tradeoff

Only successful assumed hits replay; miss/race/coverage and JSON exclusions
remain unchanged. Same wave/folder filtering, warning class/wording, report
country/table/from_cache fields, strict emitter and date order as normal reads.
Clear BOTH table ledgers before grain emission so a strict grain failure does
not retain earlier date reports. Preserve unrelated-table ledger entries.
No expected-hash calculation, cache grade/degraded probe, builds, stamps or
writes on assumed hits. Replaying a historical footer is NOT freshness or
served-completeness proof. Missing footer is no evidence, not a clean census.
Recommended reuse intentionally makes LSMS_GRAIN_STRICT raise on an assumed
hit with stored loss; this is part of the unanswered maintainer choice.

## §5 Finite draft verification

Freeze before ONE focused pytest cohort. Tiny private synthetic parquets only,
fresh child processes, actual read gates/helpers and finite fake receiver;
normal reference uses existing outer ledger reset semantics and a fixed
synthetic expected hash. Compare warnings+grain_reports/date ledger to normal
read for coveredsubset and legacyfull cases. Preserve genuine NULL keys,
wave/folder filtering, missing audits, strict behavior, race/miss/JSON bypass,
no hash/probe/build/write behavior and unchanged cache bytes/mtime.
The same cohort includes an external pinned1c actual-source baseline witness:
normal gate replays stored loss; assumed gate stays silent. No real Country
constructor or metadata/resources, native/provider call, raw data or new guard
framework. No hash-neutrality measurement claimed; future selected policy
needs measured footprint, even though this method is already excluded.

## §6 Pending decision

Should assume_cache_fresh inherit normal stored grain warning+ledger AND
LSMS_GRAIN_STRICT semantics, as this private draft demonstrates? ROOT owns
presentation when the maintainer is available. This ledger is not an approval.
An initial broad text search returned irrelevant numeric CSV false hits;
they were not inputs or authority; subsequent reads were scoped source/md/org.

## §7 Closed private draft evidence — still HOLD

Frozen runtime94df999ff38fc33183c577675aaf344f18458903. One ordinary
MAIN/unshare/thread1/loadfile2 cohort, DIAGNOSIS_OBSERVATIONS explicitly absent,
actual session1272 terminal outer0; 17 unique PASS/0fail/error/skip in107.513s.
Fifteen portable candidate cases plus two original1c fresh-process witnesses;
no retries. Three controller/worker import/config identities retained.
Coveredsubset and legacyfull branches match the normal helper's warning text,
filtered reports/from_cache/date evidence and payload; strict grain precedes
date strict like normal reads. Missing audits clear only this table's evidence;
miss/race/JSON exclusions preserve their prior ledger behavior. Genuine NULL
keys/full legacy rows remain. Assumed hits invoked no hash/grade/probe/build/
stamp/write spies and preserved synthetic parquet bytes/mtime/size. Normal
reference uses a fixed synthetic expected hash, not a real fingerprint census.
The original1c witnesses retain silence/stale grain ledger alongside normal
replay, without treating the unanswered policy as an accepted failure oracle.
No actual Country constructor, country/native input, DVC/provider/network,
shared cache, census or full suite. Controller data/config empty; synthetic
parquets were intentionally written only inside pytest private fixtures.

Evidence: B/reports/1066-assumed-fresh-grain-private-draft-20261109/
focused.xml, focused.log, FOCUSED-RECEIPT.json, identities/, BASELINE-covered.json,
BASELINE-legacy.json, SOURCE-PROOF.json, OBJECT-PROOF.json and DRAFT.patch.
This final child changes only this ledger; production/tests remain runtime
exact. No GitHub mutation, ready PR, merge or issue-resolution claim.
Recommendation is now concrete: replay historical evidence with existing
strict semantics, without asserting freshness/completeness. Maintainer choice
remains required; time or successful tests do not select it.

## User disposition and current integration — 2026-11-09

Exact user selection, relayed by ROOT: **“Replay warnings and normal strict
policy (Recommended)”**. This answers restoration of stored grain warnings,
grain_reports and LSMS_GRAIN_STRICT refusal. The earlier HOLD is revoked for
this selected policy; it did not expire or follow from test success. No new
knob or science choice. Ordinary merge onto actualba03 preserves the tested
prototype runtime; only decision prose/comments/docstrings change next.
Original17PASS means15candidate cases +2baseline witness cases, not19cases.
Original current1c missing-replay counterexamples remain preserved.

ROOT owns GitHub/release; independent peer review remains required before
merge. One final candidate-only existing metadata census is authorized; no
new source/raw/API/build/corpus/fullsuite or baseline replay.

## Closed selected-policy current gate

Runtime1a2ecdfe4ed65702fc281599781f1ddb6e306f2c is tested94df exact
except decision comment/module-docstring;15candidate+2baseline cases carry
without replay. One finalmetadata session37806 actualouter0/123.270s, exact
cleanhead/prepost and private data/config0. Original40f staging retained;
ROOT corrected baseline to closedNGA5fca because candidate lacksD. No
wrong-baseline comparison or secondcollector was run. Corrected map
SHAa46be1dbc169960e965da68f1503554734f28241372b3cd24300ab7e5c25fcc2
keeps original5fca header; currentmap SHA
8ac1c0aa3a524191159d12197f069ba32e9976895a783938f2a8bd66cbb46a41.
Actual0/452country/0/1903wave/0/37BTF/0/186FIF;14nonhash/keysets exact,
18None/errors0/denials0. R1066-selected-policy-current-20261109 retains
sourcecarry/patch and metadata comparison/receipt/correction. Finalchild
ledger-only; independentreview pending at freeze, ROOT owns publication.
No source/raw/API/test/baseline replay or shared-cache/registry change.
