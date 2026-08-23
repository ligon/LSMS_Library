# Prior-Art Ledger — weight-normalisation (`sample.weight` scale harmonisation)

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`.

**Search tier used:** ripgrep + git (floor). **gitnexus was UNAVAILABLE this
session** — the MCP tools listed in `CLAUDE.md` ("MUST run `gitnexus_impact`
before editing any symbol") are not exposed to this agent, and `ToolSearch`
returns no `gitnexus_*` schema. The blast radius for `_finalize_result` was
therefore established by ripgrep over call sites plus a direct read of
`lsms_library/_build_registry.py::_EXCLUDED_CALLABLES`. Recorded as a
process gap, not silently skipped.

## §1 Task, restated

`sample.weight` (and `panel_weight`) arrive from the wave configs on **two
incompatible scales**: 13 (country, wave) cells ship *normalised* weights
(within-wave mean 1.0000, so the column sums to the wave's household count)
and 80 cells ship *expansion* weights (summing to a national population;
wave means 25.7 → 8,659). Nothing in the returned frame marks which is
which, and **CotedIvoire mixes both across its own waves** (1985–89 LSMS
`ALLWAITN` ≈ N vs. 2018–19 EHCVM population-scaled). A user summing
`weight` across waves silently gets numbers three orders of magnitude apart.

Decision (@ligon): **normalise every wave's weights to within-wave mean 1 at
API time** — for each `(country, wave)` divide by that wave's own mean over
non-null values. No threshold, no configuration, nothing to tune; the
division is a no-op (to floating tolerance) on cells that are already
normalised. Applies to `weight` and `panel_weight` independently, each by
its own non-null mean. Parquets keep the raw values.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country._finalize_result` | `lsms_library/country.py:2369` | the single post-cache-read pipeline (kinship, spellings, dtypes, `id_walk`, v-join, currency) | integration surface | **extend** — the new step is one call at the end of this method |
| `_enforce_canonical_dtypes` | `lsms_library/country.py:4377` | canonical dtype coercion inside `_finalize_result` | `tests/test_schema_consistency.py` | cite — normalisation must run **after** it so nothing re-coerces the produced floats |
| `_EXCLUDED_CALLABLES` | `lsms_library/_build_registry.py:112` | per-callable exclusions from the build fingerprint; **already contains `Country._finalize_result`** | `tests/test_build_transform_hash.py` | cite — this is *why* the placement is hash-neutral |
| `Country._table_cache_hash` / `Wave._input_hash` | `lsms_library/country.py:2520` / `585` | compute the embedded `lsms_cache_hash`; fold `btf=build_transforms_fingerprint(table)` | cache tests | cite — measured before/after, must not move |
| `Country._join_v_from_sample` | `lsms_library/country.py:1904` | calls `self.sample()` while finalising *other* tables | exercised everywhere | cite — means `sample()` is re-finalised often; the transform must be idempotent |
| `tests/test_sample.py::test_weighted_population_stable_across_waves` | `tests/test_sample.py:180` | asserts adjacent-wave `sum(weight)` ratio ∈ (0.2, 5.0); **xfails CotedIvoire** for "incommensurate by design" | yes | **amend** — the incommensurability it documents is exactly what this change removes |

**Nothing in `transformations.py`, `diagnostics.py`, `feature.py` or
`bench/feature_audit/` consumes `weight`/`panel_weight` numerically.** The
only references are (a) `data_scheme` `optional:` bookkeeping in
`diagnostics.py`, (b) a null-coverage narrative in `null_read_audit.py`,
(c) the tests above and `tests/test_tanzania_grain_gh323.py` (invariance
*within* a household, scale-free). So no grader assumes expansion scale.

## §3 Definitions & conventions in force

- **Two weight types** — `CLAUDE.md` §"`sample()` and Cluster Identity":
  *"Two weight types: `weight` (cross-sectional; positive for all interviewed
  HH including refreshment); `panel_weight` (longitudinal; NaN/zero for
  refreshment). Pre-refreshment waves have the same value in both columns."*
  This change adds a *scale* convention on top; it does not alter the
  cross-sectional/longitudinal distinction.
- **Cache vs. API** — `CLAUDE.md` §"Cache Behavior": *"cached parquets store
  pre-transformation data … `pd.read_parquet(cache_path)` shows raw
  `Relationship` strings; the Country API shows decomposed …"*. Weight
  normalisation joins that list: raw in the parquet, normalised at the API.
- **GhanaLSS weights** — `GhanaLSS/_/CONTENTS.org` §"Sampling Design >
  Weights": *"The `sample` feature only includes expansion weights
  (GLSS5--GLSS7) to avoid mixing weight types. GLSS1--GLSS4 have `weight`
  and `panel_weight` as NaN."* Superseded by this change (marked in place,
  not deleted).
- **GLSS3 vs GLSS4 design change** — GSS Poverty Profile Appendix 4
  (printed p.57), quoted in `slurm_logs/ghana_audit/FINDINGS_glss3_selfweighting.org`:
  self-weighting was *restored* for GLSS3 by the Scott & Amenuvegbe (1991)
  procedure and *"The same procedure though was not applied for GLSS 4."*
  Normalisation makes the two *scales* comparable; it does not make the two
  *designs* the same.
- `lsms_library/data_info.yml` declares **no** `weight`/`panel_weight` entry
  (the only match at `:136` is a prose comment), so there is no canonical
  dtype/spelling rule to contradict.

## §4 Invariants & assumptions

- **Placement must not move the cache hash.** `Country._finalize_result` is
  in `_build_registry._EXCLUDED_CALLABLES`, whose comment reads: *"is
  READ-path: re-applied on every read AFTER the cache, so an edit surfaces
  regardless of cache freshness … excluding it keeps the build fingerprint
  from over-invalidating on read-path edits"*. The closure walk therefore
  never descends into `_finalize_result`, so a helper reached **only** from
  it is invisible to `build_transforms_fingerprint`. **Do not** plumb
  anything through `_aggregate_wave_data`'s signature — it *is* tagged
  `@build_transform`, and a single defaulted kwarg there moved every table's
  fingerprint in a prior task. Verified empirically (§Phase 3).
- **Idempotence is load-bearing, not a nicety.** `_join_v_from_sample`
  calls `self.sample()` from inside the finalize of every household-level
  table, and `Feature`/`convert` re-enter the API. Dividing an already
  mean-1 column by its mean (1.0) is the identity, so re-application is
  safe by construction.
- **`attrs['id_converted']` must survive** (STANDING §4). The transform
  assigns columns in place on the existing frame and never rebuilds it, so
  `attrs` is untouched; a `.copy()` is taken only when a write is needed and
  `attrs` is copied explicitly.
- **pandas 3.0** (STANDING §4): no `inplace=`, `.loc[]` assignment, `pd.NA`
  for non-numeric missing. Weights are floats, so `np.nan` is correct here.
- **Never divide by 0 or NaN.** A wave whose non-null mean is 0 or NaN is
  skipped with a warning rather than emitting `inf`.
- **Within-wave normalisation loses relative wave size.** Pooling across
  waves now weights each wave by its *sample* size, not its *population*.
  Accepted (no population estimation is intended) and documented.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| per-wave mean of a weight column | reuse `groupby(level='t').transform('mean')` | pandas skips NaN in `mean`, which *is* the required "non-null mean" semantics |
| where the transform runs | **extend** `_finalize_result` | the decided location; also the only hash-neutral one (§4) |
| scope gate | **new** (`method_name == 'sample'`) | the decision is about `sample`; no other table is touched |
| a `weights='expansion'` kwarg | **not built** | explicitly out of scope; possibility recorded in the docstring only |

## §6 Open questions for the human

- `tests/test_sample.py::test_weighted_population_stable_across_waves` was a
  *miscoded-weight* detector working through cross-wave population jumps.
  After normalisation `sum == n`, so at API level it degrades to a
  sample-size stability check. The miscoding signal now only exists in the
  raw parquet. Amended + re-documented here rather than deleted; whether to
  add a raw-parquet-level replacement is a maintainer call.
- GLSS1–GLSS4 weights become wireable, but **PR #714 owns that** and this PR
  deliberately wires nothing.

---
### Phase 3 — verification

- `_normalise_sample_weights` (`country.py`) — **OK (anchored on §2/§4/§5)**:
  new pure module-level helper; no existing symbol computes a within-wave
  weight mean (grepped `weight`, `normalis`, `expansion`, `groupby.*weight`).
  Reached only from `_finalize_result`; measured hash-neutral across 10
  countries × 5 tables and all `build_transforms_fingerprint` values.
- `Country._finalize_result` — **OK (anchored on §4)**: one added call at the
  end of the DataFrame branch, after `_enforce_canonical_dtypes` and the
  `dropna(how='all')`, before `attach_currency`. No signature change.
- `tests/test_sample.py` — **OK (anchored on §2/§6)**: xfail removed because
  the documented cause was removed; new invariant + unit tests added.
- `tests/test_label_selection.py::test_columnless_country_is_otherwise_healthy`
  — **OK (anchored on §1)**: a synthetic-fixture expectation, not a consumer.
  Its `Columnlessland` sample ships raw weights 1..7 in one wave (mean 4) and
  the test pinned those literals; they are now served as 1/4..7/4. Re-pinned
  with the reason at the assertion. **Found by grepping for build-time and
  test-time weight consumers *after* the corpus sweep passed** — the sweep
  could not have caught it, because the fixture lives outside the corpus.
  The same grep confirmed **no country `_/*.py` build script calls
  `Country(...).sample()`**, so no build path bakes normalised weights into
  another table's parquet (which would have violated §4's "parquets keep raw
  values").
