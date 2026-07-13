## What (GH #436 Item 1)

Make the country **config** tree relocatable the way `data_dir` already makes the **data** tree, so a git worktree / alternate config checkout is read by the installed (`.pth`-pinned) package and can **self-verify** — retiring the worktree-can't-build limitation the WB-parity loop worked around.

- `config.countries_dir()` — env `LSMS_COUNTRIES_ROOT` → config.yml `countries_dir` → None (mirrors `data_dir()`).
- `paths.countries_root()` — `lru_cache`d resolver; override → package-relative **default** (byte-identical to the old constant). The `COUNTRIES_ROOT` constant is **removed**.

## Why the issue's 1-line sketch wasn't enough

`COUNTRIES_ROOT` was only used by `catalog`/`diagnostics`. The **load-bearing** resolution — `Country.file_path`, `Wave.file_path` — and the data-access layer (`_COUNTRIES_DIR`, ~35 sites) each resolved `countries/` independently via `files("lsms_library")` / `Path(__file__).parent`, bypassing it. Audited the whole repo and routed **all** of them through the one resolver: country.py (both `file_path`s + the `__init__` traversal check), feature.py, catalog.py, diagnostics.py, cli.py (4 sites + `_countries_root`), data_access.py & local_tools.py `_COUNTRIES_DIR`, dvc_permissions.py, and both utils (geo_audit.py, generate_dvc_stages.py). Migrated 8 test modules + `conftest.py` off the retired constant.

## Adversarial review (3 parallel red-team lenses) — all addressed

- **Completeness**: caught 2 util bypasses (`geo_audit`, `generate_dvc_stages` — the latter reachable from `cli generate-stages`) that the first pass mislabelled "retired/deferred" → now routed.
- **Behavior**: the only semantic delta is symlink-collapse at `Wave.file_path`/`feature` (`files()` → `.resolve()`); **proven inert downstream** and consistent with the sites already `.resolve()`d. Default-preserving confirmed per layer.
- **Import-safety**: no cycle (`config` is a stdlib leaf; `paths` imports it lazily); every module imports standalone, with and without the override.

The full cold gate then caught two more `COUNTRIES_ROOT` importers (7 tests, then `conftest.py`) that static grep/agents missed — fixed; a repo-wide grep now confirms zero importers remain.

## Verification

- Default-preserving: `countries_root()` == package dir; `Uganda.file_path` unchanged (every layer).
- Override honored across config / paths / `Country.file_path` / `_COUNTRIES_DIR` with the env set **before import** (the worktree model) — new subprocess-isolated test `tests/test_countries_root_override.py` (4 cases).
- Full cold gate (`-n 24 --dist=loadfile --rebuild-caches`): _<result on completion>_.

## Scope / deferred

- **Item 2** (declarative per-feature `join_v`/derived in `data_scheme.yml`) — follow-on PR.
- 4 GhanaLSS wave-level `mapping.py` `files()` self-locates — per-country config scripts that re-anchor to the package by design; a separate question.
- `_COUNTRIES_DIR` snapshots resolve at import (worktree model = env before import); documented inline. Fully-dynamic re-pointing mid-process is out of scope.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
