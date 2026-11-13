# Proposed tests, commands and intended changes (no test file implemented)

Proposed new file: `tests/test_mali_community_prices_builder.py`.
Use the real Mali config/helper/finalizer and the existing1039/808 fixture
patterns; synthesize tiny source frames rather than depend on unavailable
public microdata or skip on any build exception. No fixture/golden refresh.

1. Actual Mali config binding: country script absent, country `_has_country_builder`
   False; probe actual Makefile under a fresh empty data root, not regex or a
   pre-existing target. Keep a non-vacuous positive wave-target dry run.
2. Actual country Makefile all target retains no community var dependency;
   wave script+exact pattern remain, materialize:make remains, and
   `_is_script_path` remains True. Required Price/Quantity schema errors remain
   loud under both backends. Do not assert unrelated country script absence.
3. Controlled actual-wave runner preserves the real finalizer's i==v shim,
   obs1/2/3, distinct variety/passage/Description and missing/zero Quantity
   under existing eligibility. Reuse existing822 test fixtures/controls rather
   than reproducing its mapper or inventing a new source canonicalizer.
4. Both backends call existing wave aggregation. Synthetic unique payloads and
   a country-builder sentinel prove it is not invoked; assertions cover full
   values/index/columns/dtypes/masks/attrs. A typed declared empty frame with
   matching names/columns results for each unsupported strict subset and the
   combined unsupported subset, cold and with populated country cache state.
   Baseline actual Make155931 wrong-wave rows are discriminating evidence.
5. Healthy2014-15/default/all requests preserve the complete supported frame;
   Make now equals default FULLattrs, including explicit id_converted=True.
   Known baseline Make lacks that attr; do not strip it or normalize public
   frames. Raw COUNTRY-cache absence under the future Make wave route is a
   separately asserted contract; raw native shim remains unchanged.
6. CLI `_load_table` or CliRunner materialize forwards supported and repeated
   unsupported --wave values and uses the same Country route; no country var
   substitute is invented. Direct retired var target fails with no rule,
   while actual wave target still resolves. `make -C lsms_library materialize`
   output must be a private explicit --out, never countries/build/in-tree.

Source-backed validation remains separate from data-free unit controls and
must compare the complete155931-row822 reference in actual order/fullattrs;
it cannot replace full equality with sums/counts or sorted projections.
COUNTRY current hashes+coverage and legitimate hashless native wave caches
are recorded separately. Every old obs3 cell survives via full-frame equality.

After implementation, exact focused starting selection:

```bash
# Always use the approved writer's exact cwd/import/config identity,
# image Python, private roots, active socket/write guards and unshare -rn.
# OPENBLAS/OMP/MKL/NUMEXPR/BLIS/VECLIB, build and Make threads=1.
python_image=/local/job40255479/venv_img/bin/python
"$python_image" -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider \
 tests/test_mali_community_prices_builder.py \
 tests/test_mali_community_price_readings.py \
 tests/test_community_prices_variety.py::TestDeclaration \
 tests/test_community_prices_variety.py::TestAssembly \
 tests/test_gh1039_make_backend_no_country_rule.py \
 tests/test_gh808_script_path_predicate.py \
 tests/test_gh866_stale_country_parquet_not_laundered.py::test_load_from_waves_takes_the_flag_and_defaults_it_off \
 tests/test_gh866_stale_country_parquet_not_laundered.py::test_the_eviction_precedes_the_country_fallback \
 tests/test_gh866_stale_country_parquet_not_laundered.py::test_exactly_the_country_tier_call_sites_pass_the_flag \
 tests/test_gh866_stale_country_parquet_not_laundered.py::test_the_rebuild_failure_is_still_raised_not_swallowed
"$python_image" scripts/api_surface_audit.py --strict
"$python_image" bench/feature_audit/scan.py --features community_prices \
 --countries Mali Niger --phases 1,2 --jobs 1 --out /private/finding-records.jsonl
```

The new file above is deliberately not yet present.
The four866 selectors are source guards only; its unrelated cache-gated delivered-country poison tests are outside this two-source scope.
Existing1039 test_barren_subset_with_a_country_builder_keeps_the_legacy_fallback explicitly pins why the old route is preserved generically; this task must change Mali builder presence rather than weaken that framework test.
The existing real-country predicate parameter table has no Mali community_prices expectation to edit. Use exact current selectors
after collection; no hidden deselection, skip, xfail or blanket exception pass.
Mali real built controls may additionally run against staged held data; never
allow the existing build-test skip fallback to stand in for source evidence.
The healthy Niger comparator requires its full CONTENTS read and an actually
current source-pinned private cache gate before use in the implementation/review;
no Niger primary source or postchange frame is claimed by this diagnosis.

Root will approve the focused writer and boundary before any tracked change.

Sue
