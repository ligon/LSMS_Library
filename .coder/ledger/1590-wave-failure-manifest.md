# Prior-Art Ledger — #1590 failed waves and cache coverage

**Base:** development `8558fefe0`. **Search tier:** ripgrep + git; live issue
1590 and the Nov 5b ruling in the sweep's DECISIONS/MORNING were read.
Inherits STANDING.md §§3–4 and CLAUDE.md §Cache Behavior.

## §1 Task, restated

Do not cache a declared wave's failed build as completed coverage. Propagate
hook AttributeError and failed Python scripts; refuse a present malformed
updated_ids JSON instead of walking household IDs without it. Preserve
successful YAML empties, unsupported waves, and the established optional
source/drop degradation contract. The approved global build fingerprint
change invalidates old incorrectly complete manifests; re-warming is later.

## §2 Existing machinery

| Existing machinery | Current anchor | Tested / reuse |
|---|---|---|
| Wave getter per-result `_with_origin` | country.py:1920, 5731 | GH1292 successful-empty tests; reuse |
| `_aggregate_wave_data.build_wave` | country.py:5718 | GH1281 declared-error tests, GH1292; extend |
| `run_make_target.try_script` | country.py:5641 | failure-is-not-absence / no-Make tests; extend terminal failure |
| Parent wave outcome loop | country.py:5805 | `_parallel_waves` capture/relay and GH1292; reuse outcome transport |
| Country no-stage/manual cache writes | country.py:6080, 6280 | GH891, GH1281; extend coverage argument |
| `_cache_covers_waves`, `_union_waves` | country.py:4627, 4661 | GH891; reuse gates and monotone union |
| `_compute_panel_ids`, JSON cache loader | country.py:6374, 5937 | panel ID persistence tests; extend corrupt-input failure |
| Source drop bookkeeping | Wave.grab_data, `_BUILD_DROP_LEDGER` | GH675; preserve without changing optionality |
| Tagged orchestrator fingerprint | country.py:5391, _build_registry.py | build-transform hash tests; reuse, no exclusions |

## §3 Definitions and conventions in force

- CLAUDE.md's manifest records waves **BUILT**, not observed t values. An
  empty successfully read wave can be covered; missing output cannot prove it.
- GH1292 ledger's producer proof is a truthy YAML resource and this getter
  invocation's `memory`, `parquet`, or `materialized` origin. Preserve its
  strict-subset, original-schema witness and non-cacheable answer semantics.
- An undeclared wave is absence (GH1099/1281), not a failed build. A declared
  getter's AttributeError is a code/hook failure, after declaration is known.
- GH675 explicitly permits unavailable enrichment sources to degrade a
  nonempty wave. Their drop stamps/read gates remain authoritative; this task
  does not make every optional unavailable source fatal.
- A present JSON that will not parse is malformed input, not absence. A
  failure must not leave the panel-ID attempt memoized as successful absence.

## §4 Invariants and proposed safe placement

1. Keep the scheduler's outer `(has_table, result)` protocol and existing
   nested `(wave_result, successful_empty)` outcome. Account for coverage in
   the parent after relay, not in a worker-local set, public attrs, or t values.
2. Remove the catch that converts a hook AttributeError to a warning. Retain
   the contextual KeyError route and genuine declaration absence. First run
   a clean-source census of the catch branch.
3. A Python fallback whose subprocess exits nonzero raises its original
   CalledProcessError. A successful run with no output remains unproven;
   a genuinely produced empty parquet must be distinguished explicitly from
   the no-builder/no-output sentinel before it may count as covered.
4. Return explicit completed coverage from load_from_waves to the two active
   country-cache write sites. Preserve ordinary return types at its other
   callers. Cover nonempty successful outcomes, proven empties and known
   unsupported waves; omit declared missing outputs. Preserve successful
   country-builder behavior and existing all-empty failure semantics.
5. Corrupt present JSON must escape the early ValueError fallback; reset a
   failed lazy panel computation so repeated calls on the same Country cannot
   suppress the refusal. Missing optional panel data keeps its existing path.
6. Existing stale partial manifests need no special metadata migration: the
   approved tagged-orchestrator change moves expected hashes. Demonstrate a
   poisoned old-hash cache cannot be re-stamped complete after failure, and
   that repaired sources subsequently recover full coverage. Explicit
   assume_cache_fresh remains the documented caller promise, not a repair.
7. Do not infer complete retired DVC-stage coverage from requested waves if
   those stage outputs are missing; inventory this separate path and report
   any scope extension before changing it.

## §5 Reuse decisions

| Need | Decision | Reason |
|---|---|---|
| Completion evidence | Extend existing invocation outcome | Already carries positive-empty proof across workers |
| Cache manifests / recovery | Reuse writer, gates, union, hashes | No new cache tier or sidecar required |
| Fault controls | Extend GH1281/GH1292/failure test patterns | Exercise actual API and IO leaves, not a parallel framework |
| Error reporting | Reuse original exceptions and contextual notes | Do not introduce a catchable generic failure wrapper |
| Old bad manifests | Reuse content-hash staleness | Global hash cost explicitly approved; measure, do not hide |

## §6 Review checkpoint

No new survey-policy ruling is identified. Root reviews this concrete design
before implementation. Remaining implementation distinction: pass positive
script-output evidence without turning a no-builder empty sentinel into a
successful empty wave. The retired DVC-stage path is inventoried separately.

Verification plan: synthetic actual Country/Wave calls for three fault routes,
cold and warm partial manifests, same-instance corrupt-JSON retries, repaired
sources, optional source drops, valid empty/unsupported wave controls, serial
and relay behavior. Source-backed clean/fault census covers Albania's script
route and Burkina_Faso's hooks and panel IDs after reading their full CONTENTS.
Compare exact healthy frames before/after privately; report aggregates only.
Use private L2, shared L1, fixed import/config identity, one build/BLAS thread;
no shared re-warm or source mutation. Run broader tests after targeted checks.

### Phase 3 — verification

Pending implementation and measured evidence.
