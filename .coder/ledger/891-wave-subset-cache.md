# Ledger — #891 country cache thrashes across wave subsets (+ #892)

Inherits `.coder/ledger/STANDING.md`. Cites `CLAUDE.md` §"Cache Behavior
(v0.7.0+)" rather than restating it.

## §1 Task

`Country.<table>(waves=[...])` and `Country.<table>()` share one cache path but
compute different content hashes, so alternating requests rebuild each other.
#892 is the sibling correctness bug in the `assume_cache_fresh` short-circuit.

## §2 Prior art found (reuse, do not rebuild)

| machinery | path | why it is the right hook |
|---|---|---|
| `_CACHE_HASH_KEY` / `cache_freshness` / `stamp_parquet_hash` | `local_tools.py:1846,2019,2040` | the v0.8.0 gates; STANDING §5 says reuse, do not hand-roll |
| `_GRAIN_AUDIT_KEY` + `read_parquet_grain_audit` | `local_tools.py:1860,1986` | **the precedent**: a second schema-metadata key riding alongside the hash, footer-only read (~1 ms), rotates atomically, removed by `cache clear`. `lsms_cache_waves` is built to this exact shape |
| `to_parquet(..., cache_hash=, grain_audit=)` | `local_tools.py:2074` | the sanctioned writer already carries metadata kwargs; add `cache_waves=` beside them |

No existing wave-coverage or wave-filter machinery exists — grepped for
`isin(waves)`, `.loc[waves]`, `level='t'` restrictions across `country.py`.
This is genuinely new, which is why it is recorded here.

## §3 Definitions in force (cited)

- Cache tiers, `LSMS_NO_CACHE`, `assume_cache_fresh` semantics — `CLAUDE.md`
  §"Cache Behavior (v0.7.0+)".
- Hash composition and its exclusions — `CLAUDE.md` §"Automatic content-hash
  staleness (v0.8.0)"; `Country._table_cache_hash` docstring.
- Cached parquets store **pre-finalize** data — STANDING §4.

## §4 Invariants this change must not break

1. **No corpus-wide invalidation.** `_table_cache_hash` folds
   `sorted(set(waves) | set(self.waves))`. For an all-waves request that is
   byte-identical to today's `sorted(waves)`, so every warm all-wave parquet
   stays `fresh`. Only subset requests move, and they move *onto* the all-wave
   hash. **Verify by probing hashes before/after on warm countries.**
2. **Core aggregates nothing new.** The filter is a row selection on `t`; no
   groupby, no reducer.
3. Cached parquet stays pre-finalize; filtering happens on the cached frame
   before `_finalize_result`, not after.
4. `LSMS_NO_CACHE` / `LSMS_BUILD_BACKEND=make` behaviour unchanged.

## §5 Migration hazard, and why it is already safe

A pre-fix parquet has no manifest. Coverage cannot be guessed — GhanaLSS
`household_roster` on this machine holds **1 of 7 waves** with no manifest, so
"assume full coverage" would silently serve a one-wave answer to an all-wave
request.

It is nonetheless safe, because the *old* wave-dependent hash is itself a
coverage marker for the migration window: a parquet built from a subset was
stamped with that subset's hash, which cannot equal the new all-waves
expectation, so it grades `stale` and rebuilds. Therefore:

- freshness `fresh` + no manifest  -> coverage is `self.waves` (it must have
  been stamped by an all-waves request)
- freshness `legacy` / `unverifiable` + no manifest -> coverage unknown; serve
  only a full-`self.waves` request, otherwise rebuild once (which writes a
  manifest and ends the thrash for that cell)

## §6 Design decisions

- **D1 union-build, not build-all.** On a miss, build
  `requested | cached_coverage`, not `self.waves`. Build-all is simpler but
  makes a single-wave user of a 7-wave country pay 7x on a cold build. The
  union is monotone (coverage only grows, bounded by `self.waves`), so each
  wave is built at most once and alternating *distinct* subsets converge —
  which plain superset-serving does not: without the union, `['a']` then `['b']`
  then `['a']` rebuilds every time, because each build shrinks coverage to its
  own request. That is the reported bug merely narrowed.
- **D2 manifest records waves BUILT, not waves with rows.** A wave can
  contribute zero rows and is dropped at `country.py:3806-3810`. Deriving
  coverage from `df.index.get_level_values('t')` would make such a wave
  permanently unsatisfiable and reintroduce the thrash.
- **D3 #892 fixed here, not separately.** Same root cause; and fixing #891
  without it would propagate #892 from the `assume_cache_fresh` branch to the
  normal path, since the wave-in-hash is currently what prevents it there.

## §7 Verification plan

- hash-stability probe over warm countries (invariant 1)
- alternating-request regression test, separate processes, one cache (the test
  the issue asks for)
- `assume_cache_fresh` + subset returns exactly the subset (#892)
- legacy-parquet migration: no manifest + subset request -> rebuild, not a
  wrong-coverage serve

## §8 Verification results (2026-09-12)

- **Wave-independence**: `_table_cache_hash` STABLE across all/one/two-wave
  requests for GhanaLSS, Albania, Uganda x {household_roster, sample}.
- **Thrash, Albania, 6 alternating calls**: 6 country-cache writes before,
  **1** after; returned waves correct at every step.  Timings 21 s cold then
  0.2-0.7 s warm (HEAD: 0.3-1.7 s).  The penalty HEAD paid was
  re-aggregation, not a cold build, because L2-wave was warm -- so this is a
  modest per-call win on Albania and would be far larger on a script-path
  country.  Only Albania was measured; do not generalise the figure.
- **Fingerprint attribution** (why the design is shaped this way): editing
  `load_dataframe_with_dvc` AT ALL moves `build_transforms_fingerprint`
  (one comment line: `c4396fc7` -> `960b055c`), because `_aggregate_wave_data`
  is `@build_transform()`-tagged.  The `country.py` changes here cost 0 on
  their own; the corpus invalidation comes from `to_parquet`, which
  `_build_registry` excludes DELIBERATELY.  Both were free in practice: the
  corpus measured **200 of 201 L2-country parquets already stale at HEAD**
  (v0.12.0 landed after the last re-warm), so there was no fresh cache to lose.
- **Tests**: 20 new (`tests/test_gh891_wave_subset_cache.py`) + 65 and 89 in
  the cache/hash/grain suites.  One failure,
  `test_canonical_shape_via_cache_miss.py::test_ghanalss_panel_consistency`,
  **reproduces identically at HEAD** -- pre-existing, not caused here.  Its
  `panel_ids_targets_exist` check reports 100% found / 0% inconsistent and
  still grades `fail`, which looks like a defect in the check; not pursued.

## §9 Known open, deliberately not fixed here

**Cache MECHANISM code lives inside the CONTENT fingerprint.** Any edit to the
v0.8.0 read gate cold-rebuilds the corpus, which is a standing tax on exactly
the code most likely to need fixing.  `_build_registry._EXCLUDED_CALLABLES`
already makes this argument for `cache_freshness` / `stamp_parquet_hash`; the
gate that CALLS them is hashed as closure text.  The fix is to extract the gate
into a `Country._read_l2_country_cache` and exclude it.  Not bundled here: it
is a refactor of the repo's most delicate function and belongs behind its own
review, not inside a bug fix.  Landing it later costs one more corpus
invalidation -- cheap now (the corpus is already stale), so it should be soon.
