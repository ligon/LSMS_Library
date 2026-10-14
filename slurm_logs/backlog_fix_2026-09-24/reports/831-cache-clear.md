**SCOPE DEVIATIONS**
- `tests/test_gh831_cache_clear_intree_sources.py` (new). This is the unit test the task's Verify line asks for, not named by path in the brief.
- No stop-list matches. `lsms_library/cli.py` is the only other file touched. `local_tools.py:1195-1214` (read policy) is untouched.

**1. Commit and branch**
- `a12690068` on `fix/831-cache-clear`, pushed to `origin` (the mirror). Confirmed with `git ls-remote origin refs/heads/fix/831-cache-clear` matching HEAD.

**2. Verification**
- Package identity assert: `ok /local/job39198802/clone/lsms_library/__init__.py`.
- `pytest tests/test_gh831_cache_clear_intree_sources.py tests/test_cli.py tests/test_gh803_intree_parquet_never_read.py tests/test_gh831_no_runtime_writes_into_config_tree.py tests/test_build_transform_hash.py` (verbatim recipe, output to `/tmp/pytest_831_sha.log`, run on the committed SHA): `62 passed in 43.20s`.
- Hash neutrality, measured: `build_transforms_fingerprint` for `food_acquired`/`household_roster`/`sample` and `Country('Uganda')._table_cache_hash` for the same three tables are byte-identical with HEAD's `cli.py` and the new one (6 of 6 lines identical). Also pinned structurally by the new `test_no_cli_symbol_reaches_any_build_fingerprint`.
- Real tree on this checkout: `_intree_source_files` over Uganda/Panama/GhanaLSS/Malawi reports 0 sidecar-less, 0 sidecar-beside, 0 stray dirs, `git_consulted=True` (11,695 tracked paths under `countries/`). With `tracked=None` (the no-git fallback) it reports exactly 1 false positive, `Panama/1997/Data/unittable.csv`.
- Planted tree with one sidecar-less `.dta`, one sidecar-beside `.dta` and one `2015-16/Data/` dir: all three reported, in three distinct blocks, sidecar-less first.
- `lsms-library cache clear --country Panama --dry-run` against my private `LSMS_DATA_DIR`: `No cached files matched the provided filters.` and no #831 warning (tree is clean).
- `find lsms_library/countries -name '*.parquet'`: 0.

**3. Cache cost**
- Nil. `cli.py` is a leaf module nothing in the library imports, so no build fingerprint reaches it; the measurement above confirms 0 of 6 probed hashes moved. No table re-warms.

**4. Surprises**
- The assumption that a digit-named stray dir (or a country dir without `_/`, like Brazil) would crash `cache clear --all` at `Country(name)` is **false, measured**: both `Country('Brazil', preload_panel_ids=False).clear_cache(dry_run=True)` and `Country('2015-16', ...)` under a planted `LSMS_COUNTRIES_ROOT` return `[]` without raising. So `--all` already tolerates them; my root-level sweep is what makes them *visible*.
- Nine country dirs have no `_/` (Bosnia-Herzegovina, Brazil, Bulgaria, Harmonized_LSMS-ISA_Ag, KenyaLPS, Kyrgyz Republic, Nicaragua, Rwanda, Tanzania_Kegera), so "no `_/`" cannot identify a stray wave dir.
- Nine git-tracked `.csv` lookup tables live in-tree; six match the `*.csv` ignore pattern yet are tracked (`.gitignore` un-ignores only `fct_*.csv`/`unit_labels.csv` by name).
- CONTENTS.org: n/a, framework change, no country idiosyncrasy found or contradicted.
- Docs not updated: CLAUDE.md's "`cache clear` now says so" sentence is about parquets and stays true; that sentence and `docs/guide/caching.md` could be extended to mention the source-file report. Left for the reviewer since neither file was named.

**5. Decisions**
- **Tracked-file exclusion uses `git ls-files`, not `check-ignore`**, because six of the nine tracked csvs are ignore-pattern matches. Git is consulted via subprocess (precedent: `_dvc_cmd` in the same file).
- **Files under any `_/` path component are always excluded** as config by position (`_BUILD_INPUT_SUFFIXES` already treats `_/*.csv` as build inputs). This is what keeps a pip install, which has no `.git`, from listing the shipped tables on every run. The one stated false positive in the no-git case is `Panama/1997/Data/unittable.csv`, and the report prints a line saying git was not consulted.
- **Digit-prefixed root dir name is the stray-dir discriminator**, the same one the issue used (`ls -d countries/[0-9]*`). These are swept regardless of `--country`.
- **Sidecar test reuses `local_tools._is_polluted_workspace_copy`** so there is one definition of "has a sidecar" shared with the reader.
- **Report is silent on a clean tree** and prints the sidecar-less class before the sidecar-beside class, with wording that states current reader behaviour accurately ("SERVES these in preference to the DVC blob" vs "refused by get_dataframe").
- **Read policy deliberately unchanged**: what `get_dataframe` serves remains the open escalation.