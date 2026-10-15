# SCOPE DEVIATIONS

None. Read the release skill; used isolated worktrees and private tool/install environments. No source, fixture, baseline, dependency declaration, lockfile, shared environment, tag, remote, or GitHub mutation. These are local sanity artifacts, not a tagged release. No maintainer scientific decision or Uganda nutrition-baseline approval is inferred.

## Result

Release tooling and installed-package validation pass on frozen integrated head **74c9398ecf3c1908feb539a99bce3f3f6bf6d04f**. Dynamic version is **0.14.0.post344.dev0+74c9398ec**, matching `git describe` (`v0.14.0-344-g74c9398ec`); the version plugin is active and no tag was created.

| Check | Result | Evidence |
|---|---|---|
| `make api-audit STRICT=1` | PASS, 2.35 s | api-audit.log, tool-steps.json |
| MkDocs build | PASS, 5.72 s | docs.log, site/ |
| Poetry wheel and sdist | PASS, 46.89 s | build.log, dist/, package-manifest.json |
| Sdist wheel rebuild without Git | PASS, 14.23 s; byte-identical wheel | sdist-rebuild.log/json, artifact-comparison.json |
| Installed-wheel data-free smoke outside checkout | PASS | installed-smoke.log/json |
| Full frozen-head unit suite | 3,274 passed; 4,405 skipped; 2 known Nigeria failures; 60 warnings, 111.49 s | unit-suite.log/json |
| Corrected Nigeria file at 6c4c2fc13d8ca7ae6593ba8062877f374a375d23 | 12 passed; 6 skipped, 9.20 s | nigeria-corrected.log/json |

The two full-suite failures are `tests/test_nigeria_sold_unit.py::TestRawFiles::test_sold_unit_shares_the_harvest_unit_value_label_set[2010-11]` and `[2012-13]`. They match the coordinator's independently observed no-credential CI failures. The test-only correction was already in progress elsewhere; this lane did not duplicate it. The targeted corrected run passed under the same credential-free settings. The full suite was not repeated on that test-only successor.

## Artifact and installed-package evidence

- Wheel: `lsms_library-0.14.0.post344.dev0+74c9398ec-py3-none-any.whl`, 11,756 archive members. SHA256 `47b032a028aa51138d4cd52551c4dac57e41359a37fc8906afe22933e118ca64`.
- Sdist: `lsms_library-0.14.0.post344.dev0+74c9398ec.tar.gz`, 11,755 members. SHA256 `ee93610f6ac66b2e9b44f45d1322b7aa183b045471b4d563483b3f437e8cabd1`.
- Zero tracked `lsms_library/` files missing from the wheel. This includes `catalog_relations.py`, `catalog_relations.yml`, country SOURCE.org/config inputs, and DVC configuration/encrypted credential payloads. No plaintext credentials were copied into the installed tree or private config.
- Sdist extracted outside every checkout; `python -m build --wheel --no-isolation` preserves the correct version without `.git`. The rebuilt wheel has the same SHA256 and every member has identical bytes.
- Installed with `pip install --no-deps` into a separate private environment. Smoke cwd is the control directory; `PYTHONPATH` and `LSMS_COUNTRIES_ROOT` are absent. Import and config paths resolve inside that environment's `site-packages/lsms_library`, not a checkout.
- Checked 20 public callables, 38 registered country configurations, 38 declared features, Country/Feature discovery, Uganda's eight waves and table declarations, Malawi/Tanzania declarations, the newly shipped catalog aliases/derived constituents, Malawi's multiple SOURCE.org IDs, and Tanzania's CATALOG_COVERS IDs. No raw-data build was requested. A synthetic food-acquired frame also runs through the quantity transform.
- No-Git `_intree_source_files` reports zero raw files or stray wave directories, and correctly reports that Git was not consulted. Runtime does not change installed package bytes or add files except normal Python bytecode; comparison includes import-time changes against original wheel members.
- The initial smoke harness incorrectly assumed at least 40 *registered* countries. The actual config-based catalog contains 38 (staged directories without data_scheme.yml are deliberately excluded). The harness now tests declared API contracts and records the count; no production code or repository test assertion changed.

## Isolation and limitations

Frozen worktree: `/local/job39919092/release-work/framework/release-validation`.
Corrected-test worktree: `/local/job39919092/release-work/framework/release-validation-6c4`.
Both remain clean and contain zero in-tree parquets (`final-hygiene.json`).

All processes were pinned to CPUs 16–23 with OpenBLAS/OMP/MKL threads = 1 and LSMS_BUILD_WORKERS=1. Pytest used six workers and `--dist=loadfile`, with output redirected to files. Unit runs scrubbed World Bank/S3/AWS environment credentials, used missing private AWS config paths, disabled EC2 credential lookup, set LSMS_SKIP_AUTH=1, and used an empty LSMS_CONFIG_DIR. The private unit data root was initially empty with no shared-L1 symlink; it ends with only empty DVC prefix directories. No global data re-warm or cache clear occurred.

The tools' private data root has only the authorized shared DVC blob symlink; no L2 files were created there. Installed-package smoke used a separate empty config/data root, with only an empty dvc-cache directory created. Both private config roots remain empty.

Private build environment: `/local/job39919092/release-work/framework/release-validation-env`; installed-wheel environment: sibling `release-validation-installed`. Both use a new environment-local `.pth` to read the existing mounted dependency closure; shared `.pth` files are not processed, and the shared environment was not mutated. Build/docs tools were installed only into the private tools environment: Poetry 2.5.1, poetry-core 2.5.0, poetry-dynamic-versioning 1.10.1, build 1.6.1, MkDocs 1.6.1, Material 9.7.7, mkdocstrings 1.0.7, Python handler 2.0.9. `tool-install.log` records installation. The smoke therefore validates package installation/import/config with the existing dependency closure; it is not a fresh dependency-resolution compatibility matrix or Windows/other-Python validation.

Documentation completed with existing informational notes: release pages v0.12.0/v0.13.0/v0.14.0 are absent from navigation, and Black/Ruff is unavailable for optional signature formatting. Material also prints its upstream MkDocs 2 warning. No docs failure was suppressed and no docs source fix was proposed.

## Reproduction

The saved launchers record all isolation variables, commands, cwd and head. From this directory, using `/local/job39919092/venv_img/bin/python`:

```sh
python run_frozen.py unit-suite /local/job39919092/venv_img/bin/python -m pytest -n 6 --dist=loadfile -q --tb=short
python run_corrected.py nigeria-corrected /local/job39919092/venv_img/bin/python -m pytest tests/test_nigeria_sold_unit.py -n 6 --dist=loadfile -q --tb=short
python tool_steps.py
python run_smoke.py
python rebuild_sdist.py
python verify_artifacts.py
```

`tool_steps.py` runs actual strict Make API audit, MkDocs, version/plugin checks and Poetry build in order. `installed_smoke.py` contains the exact smoke assertions. `rebuild_sdist.py` uses the already extracted local source archive (not a checkout); `verify_artifacts.py` checks full byte identity and installed source content. No command tags or publishes anything. Original completed timings are retained in the JSON records; rerunning launchers replaces their logs.
