#!/bin/bash
# Historical/replay command forms, frozen held-source evidence only.
# Do not execute against the existing immutable roots. An independent review
# needs a NEW physical data root and detached checkout, consistently adapted
# run.sh/identity.py/heads.json/sitecustomize.py and root-prepared ignored tmp.
# The launcher's cwd/import/countries/data assertions and guards stay active.
# The complete selection below reconstructs the 111 actual JUnit cases.
set -eu
p=/local/job40255479/control/isa-backlog-20261021/Mali/1091-fix
if test "${LSMS_NEW_PHYSICAL_REVIEW_ROOT:-}" != approved-and-adapted; then
  echo 'Adapt source pins and all private roots before replay; frozen evidence must not be overwritten.' >&2
  exit 2
fi

# Original order: ledger FIRST, approved production/test changes, then actual
# baseline/candidate hash inventory BEFORE ANY country rebuild.
bash "$p/run.sh" baseline-hash "$p/hash_inventory.py"
bash "$p/run.sh" candidate-hash "$p/hash_inventory.py"
# The actual hash-summary gate requires exactly24Mali/525others/39unchanged.
# Preserve original summary and source-pinned hashes; do not restamp caches.
bash "$p/run.sh" candidate-hash "$p/protect.py" before
bash "$p/run.sh" candidate-hash "$p/prepare.py"
# Await terminal PREPARATION.json: all42physicalMD5sourcecopies, comparator
# current hash/full coverage and source/reference copy receipts BEFORE APIs.

bash "$p/run.sh" candidate-tests -m pytest -p pytest_identity -n 2 --dist=loadfile -q \
  -o "cache_dir=$p/pytest-cache-candidate" --basetemp "$p/pytest-candidate-final" \
  tests/test_mali_community_prices_builder.py \
  tests/test_mali_community_price_readings.py \
  --deselect=tests/test_mali_community_price_readings.py::test_public_country_and_feature_carry_native_readings \
  tests/test_community_prices_variety.py::TestDeclaration \
  tests/test_community_prices_variety.py::TestAssembly \
  tests/test_gh1039_make_backend_no_country_rule.py \
  tests/test_gh808_script_path_predicate.py \
  tests/test_gh866_stale_country_parquet_not_laundered.py::test_load_from_waves_takes_the_flag_and_defaults_it_off \
  tests/test_gh866_stale_country_parquet_not_laundered.py::test_the_eviction_precedes_the_country_fallback \
  tests/test_gh866_stale_country_parquet_not_laundered.py::test_exactly_the_country_tier_call_sites_pass_the_flag \
  tests/test_gh866_stale_country_parquet_not_laundered.py::test_the_rebuild_failure_is_still_raised_not_swallowed \
  --junitxml "$p/candidate-tests-complete-private.xml"
# Baseline uses a physically copied byte-identical new test file. No source
# baseline edits; -c/confcutdir keep unrelated checkout conftest out of this
# private test copy. Expected exit1:24meaningfulfailures/4passes, not errors.
set +e
bash "$p/run.sh" baseline-tests -m pytest -p pytest_identity -c /dev/null \
  --rootdir "$p" --confcutdir "$p" -n 2 --dist=loadfile -q \
  -o "cache_dir=$p/pytest-cache-baseline" --basetemp "$p/pytest-baseline-complete" \
  "$p/test_mali_community_prices_builder.py" \
  --junitxml "$p/baseline-tests-complete-private.xml"
negative_status=$?
set -e
test "$negative_status" -eq 1
# Direct unmodified original marked public test, actual held sources, no skip:
bash "$p/run.sh" candidate-tests "$p/held-public-test.py"

# Each backend/consumer has its own physically private L1/L2. Four actual
# empty-target builds, each followed by a separate fresh-process warm call.
for backend in default make; do
  for request in all valid; do
    bash "$p/run.sh" "candidate-$backend-$request" "$p/runtime.py" cold
    bash "$p/run.sh" "candidate-$backend-$request" "$p/runtime.py" warm
  done
done
# Only after successful normal supported build:16physicalcopies of natural
# current country/native outputs prepare8populatedroots, never manual stamps.
bash "$p/run.sh" candidate-hash "$p/populate.py"
bash "$p/subsets.sh" default
bash "$p/subsets.sh" make
# This covers each unsupported single wave and the combined strict subset,
# cold/populated and both backends, initial plus fresh-process warm:32stages.

# Healthy current L2 gates precede each call; default/None refs remain separate.
bash "$p/run.sh" baseline-feature "$p/features.py"
bash "$p/run.sh" candidate-default-all "$p/features.py"
bash "$p/scanner.sh" baseline-feature
bash "$p/scanner.sh" candidate-default-all
# Actual scanner command is --features community_prices --countries Mali Niger
# --phases 1,2 --jobs1, full unmodified findings retained; no kwargs grid.

# Actual CLI forwarding and three serialization-stage controls. Parent CLI
# roots are prepared by physical copies/current metadata gates, not source
# rebuilds or stamps. The parent unsupported cache's known raw row-wave vs
# requested-only-coverage mismatch is explicit original defect evidence.
bash "$p/run.sh" candidate-hash "$p/prepare-cli-supported.py"
bash "$p/run.sh" baseline-cli-supported "$p/cli.py" supported
bash "$p/run.sh" baseline-cli-control "$p/cli.py" unsupported
bash "$p/run.sh" candidate-default-valid "$p/cli.py" supported
bash "$p/run.sh" candidate-make-valid "$p/cli.py" supported
bash "$p/run.sh" candidate-default-cold-unsupported "$p/cli.py" unsupported
bash "$p/run.sh" candidate-make-cold-unsupported "$p/cli.py" unsupported
bash "$p/run.sh" candidate-hash "$p/verify-cli.py"
bash "$p/run.sh" candidate-hash scripts/api_surface_audit.py --strict
bash "$p/run.sh" candidate-hash "$p/verify.py"
bash "$p/run.sh" candidate-tests "$p/protect.py" after
# After ordinary runtime/evidence commits and all consumers exit, release.py
# records final HEAD/clean/runtime blobs and original evidence protection.
# The original failed harness/strict artifact stops are retained alongside
# terminal logs. Never rerun cold commands over a populated frozen root or
# replace strict frame/attrs comparisons with normalization.
