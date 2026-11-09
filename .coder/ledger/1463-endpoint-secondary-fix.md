# Prior-Art Ledger — GH1463 exact secondary endpoint repair

**Initial source:** a63306951177cde76de21bdcefd4026eb382f5a3.
**Search tier:** git + ripgrep + AST floor; no GitNexus tool available.
**State:** exact production repair and bounded synthetic verification completed; ROOT review/integration pending.

## §1 Task, restated
Admit only botocore.exceptions.EndpointConnectionError alongside the existing
NoCredentialsError import and into the existing Wave.grab_data secondary dfs
handler. Preserve primary bare re-raise and the entire GH675 classifier,
required-config guard, drop metadata, cache grades, probes and healing policy.
User explicitly authorizes implementation after this ledger-only checkpoint;
no approval request or historical native run is needed.

## §2 Existing machinery
- `country.py:1920–2405`: tagged `Wave.grab_data`, nested get_data candidate
  loop, secondary handler at 2102, classification at 2154, final config-only
  guard at 2236–2275, drop filing at 2284, wave writer at 2382. Tested by
  `tests/test_gh1547_optional_credentials.py` (invented Alpha helpers to reuse)
  and `tests/test_gh675_degraded_build.py` (pure companion selections only).
- `country.py:8744–8910`: `_drop_record`, `_file_wave_drops`,
  `_degraded_drops`, `_probe_source`, `_degraded_cache_verdict`,
  `_warn_degraded_serve`. Reuse without changes. GH675 pure tests pin these.
- `Wave._read_l2_wave_cache` at 1824 and
  `Country._read_l2_country_cache` at 4805: actual bound cache gates to exercise
  on invented private parquets; reuse rather than simulate the verdict alone.
- `local_tools.py:2135` df_data_grabber, sanctioned get_dataframe and
  `to_parquet` at 3328, footer readers: reuse at finite invented-source leaves.
- `Wave.__getattr__` generated getters at 1244–1265: actual dispatcher can
  invoke the bound grab_data on the invented receiver, without a Country API.
- Prior shared ledger skill read from supplied context because ~/.sucoder copy
  is absent. Repository STANDING.md, README and TEMPLATE read. CLAUDE.md is
  a symlink resolving to AGENTS.md (identical resolved source).

## §3 Definitions & conventions in force
Inherits `.coder/ledger/STANDING.md` §3–§4, `CLAUDE.md` Cache Behavior and
Data Access, and `lsms_library/data_info.yml` as canonical schema authority.
Task expectations use the invented receiver's declared types independently;
never derive expected dtype by casting to actual data.

`country.py` guard explicitly says: “Only the config-bug drops (a KeyError, or
-- GH #675 -- a `file:` naming no source at all) can escalate; an unavailable
file stays soft.” Required versus optional is loss metadata, not a demand
that unavailable-required sources hard-fail. Source evidence is a local file
or DVC sidecar; no evidence is config/no-such-source. Endpoint itself does not
invent a kind. Build drops are footer/ledger records, not new public attrs.

Full Burkina_Faso/_/CONTENTS.org personally read BEFORE substantive analysis:
3,344 lines, SHA256 23e9ea9428755ce192234c75042a66fdf488595de929cf4d82d37a0302ced425,
including all TODO/WAITING/history (no LOGBOOK heading present).
Relevant idiosyncrasy: 2021–22 panel grappe GPS comes from the secondary
2018–19 source; CS has no antecedent and no centroid (2582–2623). Earlier
misnamed geo columns are explicitly config/missing-column, not degraded
(530–541); this is distinct from an unreachable endpoint.
Recorded tension, not silently adjudicated: “2021-22 -- no usable cluster GPS”
(651) is followed by the dated correction “Done 2026-11-03” (664); later
withholding leaves 516 carried panel fixes (3197–3199). No contradiction of
the current handler contract was found and no country patch is warranted.
WAITING #1148 exposure and #1226 tomato folding remain outside scope.

## §4 Invariants & assumptions
- Exactly two production AST changes: import alias plus one existing tuple
  member. No wrapping/translation/retry/global catch/hash exclusion.
- Primary endpoint object, cause and traceback remain intact through bare raise.
- Existing exception boundaries (InTreeSourceError, parquet-format,
  ValueError, PermissionError, other botocore errors) stay loud where currently
  outside this tuple; outer formatter and final hook endpoint stays loud.
- Unavailable loss (required OR optional) records truthful drops and degrades;
  config-required loss raises before drop filing/writing. Presence is judged
  after siblings/derived/drop/final hook.
- Prepared PROJECT_PYTHON; exact own cwd/package/config/head; private runtime
  roots; absent credentials; BLAS/OMP/MKL/build=1; at most 2 pytest workers,
  --dist=loadfile and file logs. Invented sources only. No actual Country,
  Feature, native producer, real source, S3/outage, Make data build or full CI.
- No shared cache/credential/dependency mutation; owned paths are this ledger,
  country.py, and NEW tests/test_gh1463_endpoint_secondary.py only.
- grab_data is an all-table build_transform root; anticipated both-tier
  corpus-wide metadata/build invalidation is UNMEASURED. No runtime hash
  census, schema bump, exclusion or re-warm is authorized here.

## §5 Reuse decision
- **extend** exact existing secondary handler by one exception class.
- **reuse** all classifiers, final loss guards, writer/stamps, read gates and
  fast probe memoization. Do not create a second availability policy.
- **reuse** GH1547 invented Harness/fixture/drop expectations and GH675 pure
  controls. **new** endpoint regression tests assert the intended repaired
  contract and independently declared storage types; the historical diagnosis
  tests assert the baseline defect and are unsuitable as product tests.

## §6 Open questions for the human
None blocks the authorized patch. ROOT independently reviews/integrates and owns
PR/GitHub/merge and any broader cache/rebuild measurements.

## Phase 3 — verification
Code/tests frozen at ccb40d5ab2a28b0895fa4eac5878cba3140b5f67 for the final
endpoint run. Production SHA256:
02349e38f583410250bcb93e43401c422ccdbcda226145c627b0dc1c9694ea6f.
Final test SHA256 is in external FROZEN-code-tests.json; this final update is
ledger-only. No concurrent editor/verifier or agent was used.

- **OK (§2–§4)** import + secondary handler: exact text replacement and complete
  module AST equality after removing ONLY the authorized alias and tuple member.
  Every other production AST node and tracked production file is unchanged.
  Primary bare raise remains with no expression/cause or traceback translation.
- **OK (§3–§4)** 29 new endpoint cases pass (test03, exit 0, 20.03 s): actual
  bound grab_data and generated Wave dispatcher, actual df_data_grabber
  formatter boundary, local/sidecar/no-source classification, optional/required
  loss, source/provenance/unrelated errors, final hooks, drop footer and both
  bound cache gates. Four fresh-interpreter children serve endpoint-degraded
  parquets twice with one fast probe each, exact independent storage schemas
  and unchanged bytes/mtime. State-reset parent controls verify healing misses,
  once-per-key behavior and actual rebuilds with now-available invented rows.
  This is bounded framework composition, not independent survey validation.
- **OK (§2–§4)** 59 unchanged companion cases pass at identical production
  bytes in test01: 31 GH1547, 16 pure GH675, 6 null-preserving storage,
  6 pure failure-boundary controls. No real-country fixture was used.
  This is not a final all-88 rerun: disjoint latest passing sets are stated.
- **Preserved actual attempts:** test01 exit 1: 59 PASS, 1 new-module collection
  ERROR (bare helper import failed); test02 exit 1: 18 PASS, 11 FAIL. Seven
  failures exposed expected-frame constructor inference to string instead of
  the explicit prewrite object provider; four exposed an output path moved
  without rebinding data_root. Explicit Series dtypes and the ordinary private
  LSMS_DATA_DIR binding fixed the test setups. No actual-data casts, changed
  existing assertions/tolerances/skips or production expansion. Frozen attempt
  sources, logs, XML and exact commands are external evidence. These failures
  retain their original status; test03 is new evidence, not a relabel.
- **OK (§4)** prepared-runtime identities and frozen bytes asserted before each
  test command; 2 pytest workers --dist=loadfile, BLAS/OMP/MKL/build all 1,
  file logs. No shared cache writes, credentials, source acquisition, SDK
  request, real Country/Feature/table/native/Make data build, or full CI.
- **Audit:** make api-audit exit 2 (Poetry absent, recipe exit 127); direct
  PROJECT_PYTHON scripts/api_surface_audit.py --strict exit 0, all 15 skills
  listed. No additional task skill applies. No runtime/dependency mutation.
- **Boundary qualification (§4):** the existing try also encloses sub-frame
  callable formatters. An EndpointConnectionError raised there is now soft
  too; a column_mapping error and final request-hook endpoint stay loud.
  The catch was not relocated or broadened to a parent exception class.
- **Anticipated metadata/build impact: UNMEASURED.** Static all-table
  build_transform root implies both harmonized tiers invalidate corpus-wide;
  no hash census, source build, shared clear/re-warm or full-CI claim follows.

Historical context/1463-DIAG-* is copied byte-for-byte into external evidence:
first 68 PASS / 4 FAIL on strict prewrite-object versus stored-string Region,
then 4 matching-storage passes. Those original assertions/results are frozen,
never an all-green historical cohort, and the entire old cohort was not rerun.
No CONTRADICTION or REINVENTION against §2–§4 was found. Unrelated broader API,
scientific, metadata census, live outage/retry timing and ROOT integration
acceptance remain unexecuted; no new country quirk or CONTENTS patch is proposed.
