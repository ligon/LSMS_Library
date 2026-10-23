The frozen addendum is rooted at `/local/job40255479/control/isa-backlog-20261021/review-1078-unit-attribution` and runtime `a4c92ccf6a40a8bf75cbb6c0bf2933cd1e69ec7b`.
Final author `d46795c303e2409ddb8ed5e4c62efca4e87ca347` has identical runtime/test blobs and modes.

These are the executed commands, with outputs retained privately. The full selection ran **once**; it remains a failed selection with nine isolation failures. Do not rerun a launcher against this frozen evidence root. A reproduction needs a new private root, detached source and root-prepared metadata target.

```sh
/local/job40255479/venv_img/bin/python /local/job40255479/control/isa-backlog-20261021/review-1078-unit-attribution/run_diagnostic_unit.py
/local/job40255479/venv_img/bin/python /local/job40255479/control/isa-backlog-20261021/review-1078-unit-attribution/attribute_and_compare.py
/local/job40255479/venv_img/bin/python /local/job40255479/control/isa-backlog-20261021/review-1078-unit-attribution/run_artifact_check.py
/local/job40255479/venv_img/bin/python /local/job40255479/control/isa-backlog-20261021/review-1078-unit-attribution/freeze_addendum.py
```

`run_diagnostic_unit.py` records the exact cwd/environment/pytest command in `unit/command-private.json`, asserts actual conftest credentials unavailable before collection and invokes:

```sh
unshare -rn /local/job40255479/venv_img/bin/python -m pytest -n 8 --dist=loadfile --timeout=600 --timeout-method=thread --durations=25 --tb=short -ra -o cache_dir=<private>/unit/pytest-cache --basetemp=<private>/unit/pytest-temp --junitxml=<private>/unit/junit-private.xml
```

The source checkout is both cwd and PYTHONPATH, with exact countries-root/package assertions; the launcher removes AWS credential/profile/session variables and cache bypass variables, points reader/AWS credential files at absent private paths, sets `LSMS_SKIP_AUTH=1`/`AWS_EC2_METADATA_DISABLED=true`, and supplies private data/config/XDG/tmp/DVC/matplotlib and numerical/build/Make thread counts of one.
The guard permits only the new private root, the exact root-prepared ignored DVC metadata target and the existing device exceptions. Its source policy is unchanged apart from namespace and diagnostic recording.

`attribute_and_compare.py` compares every original/new full `(JUnit classname, parameterized test name, outcome)` occurrence, including duplicates. It verifies each denial's actual stack/test/operation and source assertions. It does not recreate missing original event metadata.

`run_artifact_check.py` invokes only the artifact checker under the same socket/write isolation and `unshare -rn`; it does not call Country/Feature APIs or source loaders. `exact_artifacts.py` asserts package/cwd/config/data/guard identity before reading, explicitly sets `check_exact=True, rtol=0, atol=0`, checks record fields and frozen containers recursively, and preserves the sole Niger strict cross-stage dtype failure. Seven deliberate exact-comparator negatives are checked before comparing saved artifacts.

`freeze_addendum.py` uses read-only Git commands and stdlib SHA receipts; it verifies old author/diagnosis/reference manifests, cached artifacts, all protected tracked snapshots and zero new consumers. No original report/manifest is modified.

See `ADDENDUM.org` for the explicit preliminary unguarded attrs-type census qualification, historical attribution limit, original failed selection and separate prior nine-case pass.

Sue
