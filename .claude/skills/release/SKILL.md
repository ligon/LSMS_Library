---
name: release
description: Use this skill when cutting a release of the LSMS Library. The primary path is automated — publish a GitHub Release and CI builds + uploads to PyPI via Trusted Publishing. Covers that flow and its release-event gotcha + workflow_dispatch fallback, plus the manual `poetry build` fallback (dynamic-versioning plugin, Linux keyring hang, broken-system-poetry, Savio outbound-network).
---

# Releasing `lsms_library`

## Primary path: automated publish on GitHub Release (CI)

As of v0.8.0 the canonical release path is **CI-driven** — you do **not**
`poetry build` + upload by hand. `.github/workflows/publish.yml` builds the
sdist+wheel and uploads to PyPI via **Trusted Publishing (OIDC)** — no stored
token, and it runs in CI so the keyring hang below never applies. The version
is derived from the **git tag** by poetry-dynamic-versioning (the workflow
checks out the tag, `fetch-depth: 0`), so there is no manual version bump and a
guard fails the build if it resolves to `0.0.0`.

### Before cutting: merge `development` → `master` and auto-close resolved issues

Releases tag `master` (the default branch); routine PRs land on `development`.
So **before** `make release`, open a **`development` → `master` merge PR** and
put closing keywords for every issue resolved since the last release in its body:

```
Closes #498, #499, #500, #501, #502, #530, #551
```

This is the *only* point those issues auto-close: GitHub fires closing keywords
only when they reach the default branch, and only via `Closes`/`Fixes`/`Resolves
#N` (keyword + space) — **not** the `fix(#N):` conventional-commit scope style
the fix commits use. Omit this and the resolved issues stay open after the merge
(exactly what happened up to v0.8.0 — a batch had to be closed by hand). Build a
candidate list, then **verify each is genuinely resolved** (not an umbrella issue
a PR merely touched) before listing it:

```sh
# open issues whose fix PR branch (fix/<n>-… / feat/<n>-…) merged since the last tag
gh pr list --state merged --limit 400 --json number,headRefName \
  -q '.[].headRefName' | grep -oE '(fix|feat)/[0-9]+' | grep -oE '[0-9]+' | sort -un
```

Cut a release:

```sh
make release v=0.8.0           # runs tests, creates annotated tag v0.8.0 (local)
git push origin v0.8.0         # push the tag
gh release create v0.8.0 --title v0.8.0 --notes-file docs/releases/v0.8.0.md
```

Publishing the GitHub Release *should* fire the workflow (`on: release:
published`). **Then verify it actually ran** — `pypi.org` shows the new version
and `gh run list --workflow=publish.yml` shows a `release` run.

### Gotcha: the `release` event sometimes doesn't fire

GitHub silently suppresses the `release: published` trigger in some cases —
notably a release **created/published via a token** (the `gh` CLI), an **old or
recreated draft**, or a tag that predates the workflow. Symptom: the release is
published but `gh api .../actions/workflows/publish.yml/runs` shows
`total_count: 0`. Publishing via the **web-UI "Publish release" button** (your
own user identity) is the most reliable way to fire it.

If it doesn't fire, use the **`workflow_dispatch` fallback** — always reliable,
and the way v0.7.4 was shipped:

```sh
gh workflow run publish.yml --ref master -f tag=v0.8.0
gh run watch $(gh run list --workflow=publish.yml -L1 --json databaseId -q '.[0].databaseId')
```

Both triggers build from the **tag** (`github.event.release.tag_name ||
inputs.tag`), so the dispatch produces an identically-versioned artifact.

### One-time PyPI setup (already done for LSMS_Library)

PyPI Trusted Publishing is configured for this project: repo `ligon/LSMS_Library`,
workflow `publish.yml`, environment blank
(`pypi.org/manage/project/LSMS_Library/settings/publishing/`). A *new* project
or a fork needs this registered before the first automated publish, or the
upload step fails auth (the build step still succeeds).

---

# Manual / local-build fallback (CI down, or a local sanity build)

Collected hazards from actually shipping wheels of `lsms_library` by hand.
Read this before you run `poetry build` locally.

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

## Outbound internet on Savio compute nodes (works)

Outbound HTTPS to `pypi.org`, `files.pythonhosted.org`, and
`github.com` works from Savio2 compute nodes — confirmed
2026-04-28 on `n0291.savio2` and 2026-05-06 on `n0293.savio2`
(HTTP 200 in 30–210 ms each).  **Release builds, `poetry install`,
`pip install`, `dvc push`, and `poetry publish` all run fine from
a compute node.**

Historical note: a 2026-04-10 session reported DNS failures on a
Savio2 compute node; that outage hasn't recurred in any subsequent
session.  Treat it as a one-off — don't preemptively fall back to
the login node.  If a fresh session does see DNS or connection
failures, a quick `curl -sS -o /dev/null -w "%{http_code}\n"
https://pypi.org/` confirms whether outbound is the actual culprit
before chasing it further.

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

## Manual release sequence (fallback only — prefer the CI path above)

```sh
# 0. First merge development -> master + close resolved issues (see "Before cutting" above)

# 1. Make sure you're on a tagged commit
git tag v0.8.0
PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring poetry build

# 2. Verify the wheel filename carries the right version
ls dist/        # should show lsms_library-0.8.0-*.whl

# 3. Upload (from a machine that can reach PyPI)
poetry publish
```

If `ls dist/` shows `lsms_library-0.0.0-*.whl`, you have the
dynamic-versioning plugin problem above.
