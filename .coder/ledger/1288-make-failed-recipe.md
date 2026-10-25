# Prior-Art Ledger — GH #1288: failed country Make recipes

**Search tier:** ripgrep + git floor (explicit AGENTS.md GitNexus substitute).
**Original base:** `0bf64b472830fc87fd73648c57eb47d95e0006f1`, branch
`fix/1288-make-failure-20261025`. Inherits `STANDING.md`.
**Recovery branch:** `fix/1288-recovered-make-failure-20261025`.
**Recovery control:** current development `3f77cf3ca`; recovered source/test
pin `aaf02bc113ba2030e196a26fd34d635923f5929d`.

## §1 Task, restated

On the direct non-DVC backend, a declared `materialize: make` table with a
country builder must stop when an existing Make recipe or prerequisite fails.
It must not serve old wave outputs, try another target or run the country
Python fallback after that failure. Positively identified absence of the exact
requested target and an unavailable Make executable keep compatibility fallback.
No cleanup, successful-target freshness policy or survey changes belong here.

## §2 Existing machinery

| Symbol / source | Existing behavior and test | Decision |
|---|---|---|
| `Country._probe_country_builder`, `country.py:5085` | `make -n`, empty root, C locale; no-rule-for-target vs `needed by` distinction; `test_gh1039_make_backend_no_country_rule.py` | Extract/reuse one shared diagnostic classifier; positively match exact target, fail closed for other failures. |
| `_aggregate_wave_data.run_make_target.try_make`, `country.py:5309` | Existing candidate and target fallbacks; currently catches any failed Make; new synthetic baseline reproduces stale failed/unreached waves. | Extend with explicit strict flag used only by direct non-DVC materialize:make country-builder call. |
| `run_make_target.try_script`, `country.py:5351` | Country script fallback | Reuse unchanged when Make missing or requested rule absent; prevent entry after real strict recipe failure. |
| `_aggregate_wave_data` direct branch, `country.py:5970` | Country builder vs wave routing; `test_gh1039_make_backend_no_country_rule.py` | Extend only the existing genuine country-builder route. |
| `_build_wave_script_target`, `country.py:203` | Wave failures already propagate; missing Make runs Python; `test_gh968_no_make_fallback.py` | Reuse unchanged; no policy expansion. |
| `_evict_hashless_wave_caches`, `_evict_stale_country_cache` | GH479 and GH866 scoped eviction; `test_gh866_stale_country_parquet_not_laundered.py` | Reuse unchanged; never delete cached files here. |
| `tests/test_niger_concatenator_refusal.py::niger_tree` | Real Niger Makefile/concatenator/module, synthetic wave scripts/private roots | Reuse for decisive regression and healthy controls. |
| `Country._finalize_result`, `country.py:4025` | Preferred index order `i,t,v,m`, then remaining declared levels | Reuse: expected oracle canonicalizes its own level order; actual outputs stay untouched. |

## §3 Definitions and conventions

- IO/cache invariants inherit `STANDING.md` §3/§4 and `CLAUDE.md` cache behavior.
- `_has_country_builder` documents that a present but broken rule must fail
  loudly, not be silently replaced by waves (GH1039).
- Niger `_/CONTENTS.org` (full previous read, unchanged blob
  `a24e14139511f6387c9846051fe3e38d100be4f8` in integrated baseline and this base):
  “Every Niger wave has a wave script for all five concatenated tables”. Its
  plot_features 2011/2014 skips are legitimate and outside this change. The
  missing-wave refusal does not authenticate an existing old wave parquet.
- Declared `float` maps to nullable `pd.Float64Dtype()` in shipped
  `_SCHEME_DTYPE_MAP`; exact healthy oracles use that dtype on EXPECTED values.
- The required narrow contract was adopted by the parent before implementation:
  real failed country recipes are terminal; zero-exit stale outputs and other
  backend routes remain separately tracked.

## §4 Invariants and assumptions

- Strictness is opt-in only on direct non-DVC, declared materialize:make,
  genuine country-builder route; default, JSON and wave calls keep defaults.
- Exit2 alone is ambiguous. Reuse one classifier at both probe and execution;
  exact-target no-rule with no `needed by` is absence. Broken prerequisites,
  recipe errors and unknown/non-English diagnostics fail closed.
- Capture only strict Make stderr under C locale for classification, re-emit it
  unchanged to stderr, and leave stdout streamed. Preserve CalledProcessError
  as the cause of a country/table/target contextual RuntimeError; invent no wave.
- Successful mtime reuse must not rebuild or delete outputs. No cache deletion.
- Tests remain synthetic/private/offline; keep original baseline controls and
  their index-order qualification; do not replace corpus evidence with units.

## §5 Reuse decision

Shared diagnostic predicate: **extract/extend** existing GH1039 conditions,
not a second looser classifier; strengthen the target match to an exact quoted
requested name and reject recipe-error diagnostics. Runner: **extend** via one
explicit flag. Data construction/finalization/caches: **reuse**, no edits.
Tests: **new** focused orchestration regressions, reusing the existing Niger
fixture and fake-country Make shapes rather than provider builds.

## §6 Open questions / boundaries

No blocking question: parent approved the failure contract. Remaining separate
work is zero-exit/no-replacement freshness and stale country-var serving.
Fingerprint cost is expected to be broad because the orchestrator is ALL scoped;
parent will measure approved pins later. No registry exclusions or census here.

### Phase 3 — historical author verification (not fresh acceptance)

- **OK (§2/§4)** `_make_target_is_missing`: one extracted classifier serves
  both existing probe and strict execution. Exact quoted target, no needed-by,
  only recognized diagnostics; binary classification uses a decoded copy.
- **OK (§4)** strict `run_make_target`: only direct non-DVC declared
  materialize:make country-builder call opts in. Original CalledProcessError
  and country/table/target context survive; stdout streamed, stderr replayed.
  Default/JSON/wave execution does not opt into the strict flag, and eviction
  policy is unchanged. The shared builder probe also uses the strengthened
  exact-target classifier: prefix-only target matches and absence messages
  accompanied by extra diagnostics now conservatively retain builder-present
  status. Those probe/routing decisions can affect default-backend subset
  requests too; no blanket default-behavior equivalence is claimed.
- **OK (§2/§3)** healthy expected frames retain exact column/index/value/dtype
  checks and use declared nullable Float64; actual frames are unchanged.
  Warm also compares actual attrs and exact output inode/mtime/size/SHA256.
- **OK (§4)** candidate synthetic selection at040a34e8:75PASS plus two healthy
  oracle failures (expected NumPyfloat64 vs declared nullable Float64).
  Oracle-only3d36102f changes three lines; ONLY those two controls rerun and
  PASS. Thus77 distinct tests verified, not a fictitious single77PASS run.
  Wall125.18s+57.58s=182.76s; both guarded runs had zero refusals. No more
  author tests, no corpus/provider build, no hash census.
- **Intentional behavior contradiction, not stale-document assumption:** Niger
  CONTENTS2056–2062 describes “three \"Makefile execution failed\" warnings and
  one \"Python fallback failed\"”; the parent-approved strict policy stops at
  the first real country recipe failure. Its dated measured evidence is
  retained verbatim; no country document is edited in this scoped patch.
- **Boundary:** native recipe stderr bytes are preserved (including a tested
  nonUTF8 byte/CRLF). NonUTF8-encoded Windows *target names* may not match the
  UTF8-decoded positive-absence classifier and fail closed. ASCII Windows
  path/quote variants are synthetic classifier controls, not native Windows
  execution evidence. No permission to broaden encoding/fallback semantics.
- Baseline proof remains `fix1288/baseline-control/REPORT.org`: genuine
  contractFAIL+3healthyPASS after preserved oracle correction. Baseline
  cold/replacement checks did not persist full pandas dtype objects; baseline
  warm compared exact typed frames and actual attrs. That qualification is
  retained independently of this candidate's stronger typed oracle.
- Report, guard countmaps and two unfiled freshness residual drafts live under
  `fix1288/implementation/`. Zero-exit freshness/country-var staleness and
  independently measured fingerprint cost remain outside this author run.

### Recovery and pending fresh verification - 2026-10-25

The shared-mirror original head `78173e7b83d783a5ba7e58d7456e2511068b6b4d`
preserves ledger `47c62e9a`, implementation `040a34e8`, healthy-oracle fix
`3d36102f` and final ledger `78173e7b`. The coordinator cherry-picked only
those four commits onto `0102af8cf`, then merged development `3f77cf3ca`
(reviewed GH1225/PR1302), yielding the recovery pin above. The original
preview's pending GH1281 declared-wave KeyError guard was not imported.
The complete recovery delta against `3f77cf3ca` remains these four paths:
this ledger, `country.py`, `test_gh1288_make_failed_recipe.py` and
`test_niger_concatenator_refusal.py`. No production adaptation is proposed.

The complete current Niger `_/CONTENTS.org` (all 2,080 lines, including
residuals) was read during recovery. Its relevant distinction is explicit:
all four waves have scripts for all five concatenated tables; missing output
means a failed/unrun wave. `plot_features` legitimately skips the 2011/2014
waves and is outside this change. The dated three-Make-warning plus Python
fallback description at lines 2056-2062 intentionally differs from the
approved first-failure terminal behavior; retain that historical evidence.

The historical runtime reports/guards named above are unavailable locally.
The completed-evidence archive at `isa-resume-20261025T1430Z` explicitly
excluded live GH1288. Committed source and this ledger survive; the old
75-pass/two-oracle-failure plus two-pass continuation is an attributed author
record, not independently recovered runtime proof. Do not assume the ended
job's node-local files survive or reconstruct missing results as successes.

Proposed fresh finite selection, all synthetic and private, on Slurm:

- Candidate: the new GH1288 module (34 predicted cases), full Niger
  concatenator module (25), GH968 module (5), nine named synthetic GH1039
  compatibility controls and four GH866 structural placement controls:
  **77 predicted cases**. This is a newly specified selection, not a claim
  to have recovered the historical selection. Assert actual collection count
  before running; record the exact selected node IDs.
- Current-base discriminators: overlay only the recovered new test module
  into a fresh control clone, preserving all tracked product bytes. Select
  six failed-wave cases, three viable-recovery recipe cases and one broken
  prerequisite case, plus healthy replacement, exact warm equality and
  missing-Make controls. Predictions are ten meaningful failures and three
  healthy passes; actual causes and JUnit records, not exit status alone,
  determine whether the control discriminates the defect.
- Healthy controls retain full frame/index/dtype and attrs checks; no oracle
  weakening. Preserve failed/unreached old wave bytes, original stderr/cause,
  positive absence/tool fallback and successful warm output identity checks.
- A separately approved metadata-only control/candidate hash census reuses
  `slurm_logs/isa_backlog_2026-10-21/1218-current-validation/hash_census.py`.
  It enumerates every country-table across each country's waves, including
  pairs without wave declarations; compare complete keysets and errors as
  well as changed hashes. No provider reads, builds, network, fingerprint
  exclusions or speculative fixed corpus counts are permitted.

The coordinator supplies the vetted Slurm isolation/setup. This worker will
prepare only the finite selection and `run.sh` payload in private control
storage after surfacing this plan; no local tests/builds/census or job
submission. Receiving-node state, clones, caches and temporary files must
be under that job's `/local`, with the existing image, exact package/config
identity, private roots and numerical/build worker counts pinned to one.

This is a partial GH1288 repair. A zero-exit Make recipe that leaves old
wave output, or an existing country-var target mtime-skipped before any
recipe runs, never enters the terminal-failure branch. Those freshness
residuals and default/JSON/wave fallback policies remain separate; successful
verification here will not justify closing the whole issue.

### Fresh recovery evidence and census refusal - 2026-10-25

Frozen source c5d2593faa1f775d449768fe910530f5f160b64e was checked in
receiving-node private clones by Slurm job40501304. The finite candidate
selection collected and passed all77 cases, with no skipped/error cases.
The current-development control collected13: ten failures with the intended
causes and three healthy passes. Root inspected all10 JUnit causes: three
generic materialization errors lost the original recipe context, and seven
failed-recipe/prerequisite calls returned instead of raising. The baseline
failure alone does not authenticate the returned stale payload's full values.
Parent/child guard calibration denied the outside synthetic DTA and loopback
connection while allowing the private fixture. Test guard refusals concerned
only the absent native standard-library python311.zip search candidate;
they are retained, with receiving-node absence evidence, not called zero
refusals. Audit coverage is Python-level and is not an operating-system sandbox.

Independent source review found no blocking defect. The noted low coverage
gap is lack of shared-probe integration tests for unusual prefix-only or
mixed diagnostics; the classifier itself covers these cases. Strict execution
scope and the shared probe's conservative hardening are distinguished above.

The original job nevertheless FAILED1 at its summary guard check. Both
metadata censuses produced549country/40framework/2109wave entries and no
reported errors, but the guard blocked nine present tracked CSV build inputs.
cached_file_hash catches PermissionError as OSError and substitutes None, so
errors={} is insufficient evidence of complete fingerprints. Root rejects
these census values and their provisional all-entry movement counts as final
cache-impact proof. Preserve the original manifests/logs/JUnit/failed summary;
correct only the metadata harness in an additive run, without replaying77tests.

A separate pinned-Git static inventory found446raw declarations in38countries:
171 non-JSON materialize:make review candidates, including90 in the eight ISA
countries. The171 split into93with own country scripts,46Makefile-only clues
and32with neither. Script/Makefile presence is not observed reachability;
every row is explicitly UNMEASURED. This is a reproducible first exposure
inventory, not a claim that171pairs fail or that the requested runtime exposure
measurement is complete. Source blobs, exclusions and unknowns are retained.
