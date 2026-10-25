# Prior-Art Ledger — GH #1288: failed country Make recipes

**Search tier:** ripgrep + git floor (explicit AGENTS.md GitNexus substitute).
**Base:** 0bf64b472830fc87fd73648c57eb47d95e0006f1; isolated branch
`fix/1288-make-failure-20261025`. Inherits `STANDING.md`.

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

### Phase 3 — verification

- **OK (§2/§4)** `_make_target_is_missing`: one extracted classifier serves
  both existing probe and strict execution. Exact quoted target, no needed-by,
  only recognized diagnostics; binary classification uses a decoded copy.
- **OK (§4)** strict `run_make_target`: only direct non-DVC declared
  materialize:make country-builder call opts in. Original CalledProcessError
  and country/table/target context survive; stdout streamed, stderr replayed.
  No default/JSON/wave-route or eviction-policy change.
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
