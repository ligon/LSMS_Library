Independent review pins: final `9e8ebeabb0e996684d325d05987763a8c3581cc8`, runtime/tests `fdadc0698efcd300f5f09c21b60c30f570f96621`, baseline `aba2badfe2f701a8b3793f1243fd8ed4fce18a53`.
Private root: `/local/job40255479/control/isa-backlog-20261021/review-Mali-1091`.
Use only a new adapted root/checkouts for replay; these sources and evidence are frozen.

Executed command sequence:

```sh
/local/job40255479/venv_img/bin/python <private>/bootstrap.py
bash <private>/run.sh baseline-hash <private>/hash_inventory.py
bash <private>/run.sh candidate-hash <private>/hash_inventory.py
bash <private>/run.sh candidate-hash <private>/prepare_review.py
# Require PREPARATION.json terminal_success before dependent probes.
bash <private>/run.sh candidate-verify <private>/source_oracle.py
bash <private>/baseline_stages.sh
# Original2017 raw-layer assertion stop retained; completed cases not repeated.
bash <private>/baseline_resume.sh
bash <private>/candidate_stages.sh
bash <private>/run.sh candidate-tests <private>/held-public-test.py
bash <private>/run.sh candidate-hash <private>/prepare_features.py
bash <private>/feature_stages.sh
bash <private>/cli_stages.sh
bash <private>/run.sh candidate-hash scripts/api_surface_audit.py --strict
bash <private>/run.sh candidate-verify <private>/verify_review.py
/local/job40255479/venv_img/bin/python <private>/freeze_review.py
```

`<private>` means the absolute root above. `run.sh` pins cwd/PYTHONPATH/countries/package/config/data, absent credential paths, private temp/XDG/DVC/matplotlib, active write/socket guards, `unshare -rn` and numerical/build/Make threads1. No bypass or acquisition flag is used.
All pickle/API programs import `identity.py` before reading; the unmodified scanner executes through `scanner_entry.py` after identity assertions.
`exact.py` contains explicit zero-tolerance full-frame/index/dtype/mask checks and recursively exact typed attrs, including declared record fields. It is not a pickle-byte equality surrogate.

The exact111-case pytest selection:

```sh
bash <private>/run.sh candidate-tests -m pytest -p pytest_identity -n 2 --dist=loadfile -q \
  -o cache_dir=<private>/pytest-cache-candidate --basetemp <private>/pytest-candidate \
  tests/test_mali_community_prices_builder.py tests/test_mali_community_price_readings.py \
  --deselect=tests/test_mali_community_price_readings.py::test_public_country_and_feature_carry_native_readings \
  tests/test_community_prices_variety.py::TestDeclaration tests/test_community_prices_variety.py::TestAssembly \
  tests/test_gh1039_make_backend_no_country_rule.py tests/test_gh808_script_path_predicate.py \
  tests/test_gh866_stale_country_parquet_not_laundered.py::test_load_from_waves_takes_the_flag_and_defaults_it_off \
  tests/test_gh866_stale_country_parquet_not_laundered.py::test_the_eviction_precedes_the_country_fallback \
  tests/test_gh866_stale_country_parquet_not_laundered.py::test_exactly_the_country_tier_call_sites_pass_the_flag \
  tests/test_gh866_stale_country_parquet_not_laundered.py::test_the_rebuild_failure_is_still_raised_not_swallowed \
  --junitxml <private>/candidate-tests-private.xml

# Expected exit1:24 meaningful failures and4 passes, no fixture errors/skips.
bash <private>/run.sh baseline-tests -m pytest -p pytest_identity -c /dev/null \
  --rootdir <private> --confcutdir <private> -n 2 --dist=loadfile -q \
  -o cache_dir=<private>/pytest-cache-baseline --basetemp <private>/pytest-baseline \
  <private>/test_mali_community_prices_builder.py --junitxml <private>/baseline-tests-private.xml
```

Native, raw-country, public and serialized CLI layers are separate targets. Preserve the initial strict failures; do not normalize values/dtypes/attrs or re-run a cold command over its populated root. Supported Make naturally rewrites its hashless native file; DEFAULT warm and unsupported requests have their own zero-write checks.
The optional `metadata_probe.py` stopped before metadata access when `get_data_file` returned None; it is not a passed variable-label/PDF proof and was not bypassed. Current `get_dataframe` value-label/source-oracle evidence is separate.

The review is bounded to the scoped feature/countries/backends/subsets; it does not rebuild the other23 Mali tables or the corpus, run a full suite, or prove integration with other patches.

Sue
