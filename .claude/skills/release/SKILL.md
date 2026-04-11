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

## Compute nodes have no outbound internet (Savio)

`poetry build` reaches out to `pypi.org` during the build (for
build-isolation dependency fetching, even for a simple library). On
Savio compute nodes this fails with DNS resolution errors and the
build aborts.

**Release builds must run on the login node or on a host with
outbound internet.** Regular `pytest` runs don't need internet and
can still run on compute nodes via Slurm.

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
