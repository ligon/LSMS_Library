## SCOPE DEVIATIONS

`git diff --stat development..HEAD` shows exactly two files:

- `lsms_library/local_tools.py` (named by the task)
- `tests/test_gh810_no_cwd_rooted_dvc.py` (new; implied by the task's "add a tmp_path-cwd test", not named by path)

No stop-list match. No `CONTENTS.org`, `CLAUDE.md`, or country file touched.

## 1. Commits and branch

Branch `fix/810-dvc-open`, pushed to `origin` (the shared mirror) as a new branch. Two commits:

- `4d4d16d64` — `change_id` reads its crosswalk through `get_dataframe`, plus the new test file.
- `bebe68a51` — `get_dataframe`'s else-branch goes straight to `_try_get_data_file` and raises `FileNotFoundError(ENOENT, why, fn)` when that returns None. `import dvc.api` dropped; only `DVCFileSystem` remains imported from `dvc.api`.

## 2. Verification

| command | result |
|---|---|
| package-identity assert (`PYTHONSAFEPATH=1 … assert realpath startswith cwd`) | ok, `/local/job39198803/clone/lsms_library/__init__.py` |
| new test file against unfixed code | `5 failed in 16.55s`. Four fail with the issue's own signature: `dvc.utils.strictyaml.YAMLSyntaxError: unable to read: 'foo.dta.dvc', YAML file structure is corrupted`. The fifth was a bug in my test (it used level `i`; `change_id` maps `j`). |
| new test file after commit 1 only | `4 failed, 1 passed in 12.41s` (the four cwd-trap tests still reach the old else-branch) |
| new test file after commit 2 | `5 passed in 8.20s` |
| full suite, Makefile recipe (`-n 8 --dist=loadfile`, `LSMS_BUILD_WORKERS=1`, BLAS pinned, output to file) | `4244 passed, 203 skipped, 3 xfailed, 1 xpassed, 814 warnings in 1867.34s (0:31:07)`, `exit=0` |
| `find lsms_library/countries -name '*.parquet'` | 0 before and 0 after the suite |

The suite count is not comparable to the 5,666 in CLAUDE.md. This private root is warm for four countries only, so cache-gated modules skip. That is coverage the root cannot reach, not lost tests.

Unstubbed end-to-end, from a tmp cwd holding a malformed `foo.dta.dvc`, requesting a nonexistent source:

| | before | after |
|---|---|---|
| exception | `YAMLSyntaxError` on `foo.dta.dvc` (cwd walk) | `FileNotFoundError`, message names the countries root and the WB fallback |
| cold | 0.01 s at the `dvc.api.open` step here (tiny cwd; 300–450 s from `/tmp` per the issue) | 51.6 s |
| warm | — | 0.0 s |

Be clear about what the test discriminates. In this fixture the tmp cwd is tiny, so the cwd walk is fast and the `elapsed < 1.0` bound is not what catches the bug. The exception-class assertion is the load-bearing one (`YAML*` pre-fix, `FileNotFoundError` post-fix). The bound is kept because the task asked for it, with `DVCFS` and `_try_get_data_file` stubbed so the branch under test is what is timed. If it ever flakes on a loaded node, loosen the bound, never the class check.

The 51.6 s cold cost is the correctly-rooted `DVCFS.open` index walk over 9,986 sidecars in `file_system_path`, plus `get_data_file`'s `fs.exists`. It was paid before this change too, it is not the cwd walk, and it is not fixed here (~93 s on Lustre per the existing comments).

## 3. Cache cost (stated, not paid)

Probed `build_transforms_fingerprint` (3 values) and `Country._table_cache_hash` for every table of Uganda, GhanaLSS, Ethiopia and Kosovo (81 values), before and after each commit:

| commit | fingerprints moved | country hashes moved |
|---|---|---|
| `4d4d16d64` (`change_id`) | 0 of 3 | 0 of 81 |
| `bebe68a51` (`get_dataframe`) | 3 of 3 | 81 of 81 |

So the second commit is a corpus-wide re-warm: `df_data_grabber` is `@build_transform`-tagged and closes over `get_dataframe`, so any edit to that function moves every table's build fingerprint. Not paid in this job. Landing this pairs with the GH #959 decision (execute the re-warm vs. accept a mixed-writer corpus) so the corpus re-warms once, not twice.

## 4. Surprises

- **Triage said `change_id` has 4 callers. `local_tools.change_id` has zero in-corpus callers.** The four call sites (GhanaLSS `household_roster.py`, Ethiopia `food_acquired.py`) import country-local copies in `ghanalss.py`, `ghanasps.py` and `ethiopia.py`, and all three already read the crosswalk via `get_dataframe(fn)`. My edit brings the public function into line with them and changes no built table.
- **The issue names `YAMLValidationError`; a malformed sidecar raises `YAMLSyntaxError`.** Same DVC family, class depends on the malformation. The test guards on the `YAML` name prefix rather than one class.
- **One directory-level sidecar exists** (`EthiopiaRHS/_dataverse_archive.dvc`, a `.dir` entry). It is read by no script or YAML, so the else-branch is provably the "genuinely absent source" case and removal is safe.
- GitNexus timed out, so I substituted `rg` call-site sweeps (9 `change_id` references, all country-local copies; 951 `get_dataframe` call sites, signature unchanged) for the impact analysis.
- CONTENTS.org: nothing contradicted; this is framework-only, no country file touched.

## 5. Decisions

- **Fail with `FileNotFoundError`, not a new class.** The old branch also ended in a re-raised `FileNotFoundError`, and `grab_data` (`country.py:1435`, `:1510`) and `df_data_grabber(missing_ok=)` catch that class, so the #515 soft-degrade and multi-file candidate loops keep working.
- **Go straight to `_try_get_data_file`, not `DVCFS.open` first.** `file_system_path` already tried `DVCFS.open` on the correct root and got `FileNotFoundError`; a second attempt would be a redundant walk.
- **`change_id` via `get_dataframe`** rather than `DVCFS.open`: `DVCFS` resolves relative paths against the repo root, so the callers' `../{t}/Data/{f}` form would break; `get_dataframe` resolves it via its `Path.cwd()/fn` sidecar candidate. Two sanctioned side effects: a polluted in-tree crosswalk copy is now refused, and Site R reports on the crosswalk read.
- **`dvc.api.open` made unreachable, not merely unreached.** One test patches `dvc.api.open` to raise and asserts it is never hit.

Follow-ups named, not done (outside named paths): `_sync_legacy_dvc_creds` in `__init__.py` and `data_access.py` exists solely for `dvc.api.open` callers, and none remain anywhere in the package, so it is a removal candidate. The CLAUDE.md gotcha about `get_dataframe('../Data/..')` from a chdir'd cwd falling through to `dvc.api.open` describes this same bug and can be retired once this lands. The residual cold `DVCFS` index walk for a genuinely absent source is its own issue.