---
name: release
description: Use this skill when cutting a release (poetry build + wheel upload) of the LSMS Library. Covers poetry-dynamic-versioning plugin install, the Linux keyring hang, and the need for outbound internet during builds.
---

# Release Tooling Gotchas

Collected hazards from actually shipping wheels of `lsms_library`.
Read this before you run `poetry build`.

## `poetry-dynamic-versioning` must be installed as a Poetry plugin

The `[tool.poetry.requires-plugins]` declaration in `pyproject.toml`
is **not enough** in Poetry 2.x. Without the plugin actually
installed, `poetry version` reports the static `0.0.0` from
`pyproject.toml` and `poetry build` produces a mis-versioned wheel
that looks like it worked.

Install once per machine:

```sh
poetry self add "poetry-dynamic-versioning[plugin]>=1.0.0,<2.0.0"
```

If that fails with `Permission denied: 'INSTALLER'` (which happened
on the 2026-04-10 Savio login node due to a read-only system
`pycparser`), fall back to:

```sh
python3 -m pip install --user --ignore-installed \
    "poetry-dynamic-versioning[plugin]>=1.0.0,<2.0.0"
```

Verify with `poetry self show plugins` — the plugin should be
listed. Then `poetry version` should report the dynamic version
from the latest git tag, not `0.0.0`.

## `poetry build` hangs on Linux keyring without a TTY

Poetry 2.x pulls in `keyring` + `SecretStorage` as transitive
dependencies. On Linux hosts without a graphical session (Slurm
compute nodes, CI containers, headless login sessions), `poetry
build` can hang indefinitely on an internal keyring lookup. The
subprocess never makes progress and never errors out.

Workaround — set `PYTHON_KEYRING_BACKEND` to the null backend
**before** calling `poetry build`:

```sh
PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring poetry build
```

Slurm submission scripts for release builds should export this env
var unconditionally.

## Outbound internet on Savio compute nodes (was: blocked, now: works)

Originally observed 2026-04-10: `poetry build` failed with DNS
errors on Savio2 compute nodes; release builds had to be run from
the login node.

**Updated 2026-04-28**: outbound HTTPS reaches `pypi.org`,
`github.com`, and `files.pythonhosted.org` from at least
`n0291.savio2` (HTTP 200 in 30–200 ms each).  Either the original
outage was transient or the cluster's network policy has been
relaxed.  **Release builds can run from a compute node again** —
verify with a quick `curl -s -o /dev/null -w "%{http_code}\n"
https://pypi.org/` before relying on it.

The login-node fallback remains a safe default for release builds
that take long enough to be expensive to retry.

## Working around a broken system poetry

Independent of the above, the system poetry on Savio
(`/global/home/users/ligon/.local/bin/poetry`) has been observed
to fail with:

```
ModuleNotFoundError: No module named 'packaging.licenses'
```

on `poetry --version`, `poetry env info`, and `poetry install`.
Root cause: Poetry 2.x expects `packaging>=24.2` (which exposes
`packaging.licenses`); the user-site `packaging` is older, and
poetry's vendored deps don't shadow it cleanly.

**For tests** — bypass poetry entirely.  Run pytest directly via
the project venv:

```sh
.venv/bin/python -m pytest -n $(nproc) --dist=loadfile
```

If a `make` target routes through `$POETRY` and the wrapping
`make setup` fails on `poetry install`, just touch the stamp to
skip it (the venv is already populated):

```sh
touch .make/setup.stamp
```

then call pytest directly as above.

**For builds** — install poetry into the project venv so the
broken user-site poetry is bypassed:

```sh
.venv/bin/pip install --upgrade poetry \
    "poetry-dynamic-versioning[plugin]>=1.0.0,<2.0.0"
.venv/bin/poetry self show plugins   # verify plugin is listed
PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring \
    .venv/bin/poetry build
```

A more invasive alternative is to upgrade the user-site
`packaging`:

```sh
python3 -m pip install --user --upgrade packaging
```

That fixes the system poetry in place but may have flow-on effects
on other user-site packages.  Prefer the venv-local install above.

## Typical release sequence

```sh
# 1. Make sure you're on a tagged commit
git tag v0.7.0
PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring poetry build

# 2. Verify the wheel filename carries the right version
ls dist/        # should show lsms_library-0.7.0-*.whl

# 3. Upload (from a machine that can reach PyPI)
poetry publish
```

If `ls dist/` shows `lsms_library-0.0.0-*.whl`, you have the
dynamic-versioning plugin problem above.
