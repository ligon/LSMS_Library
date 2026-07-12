# LSMS Library

## Design Philosophy
Harmonize the *interface*, not the data. Surveys differ in structure; the library provides a uniform API without discarding survey-specific detail.

## Core API
```python
import lsms_library as ll
uga = ll.Country('Uganda')
uga.waves          # ['2005-06', '2009-10', ...]
uga.data_scheme    # ['people_last7days', 'food_acquired', ...]
food = uga.food_expenditures()  # Standardized DataFrame
```

The `Feature` class assembles the same table across every country that declares it:
```python
roster = ll.Feature('household_roster')
df = roster()                         # all countries
df = roster(['Ethiopia', 'Niger'])    # subset
```
The returned DataFrame prepends a `country` index level.

## Task-Specific Skills (read these on demand)

- `.claude/skills/add-feature/SKILL.md` — adding a new table to a country. Has sub-skills for `sample`, `food-acquired` (and its nested `food-acquired/units` — decoding/cleaning the unit `u` label: leak audit, decode toolkit, and the silent-failure gotchas), `shocks`, `assets`, `panel-ids`, and `pp-ph` (post-planting/post-harvest countries — Nigeria, Ethiopia, GhanaSPS).
- `.claude/skills/multi-round-waves.md` — Tanzania `2008-15/` multi-round folder pattern and `wave_folder_map`.
- `.claude/skills/tanzania-panel-design.md` — NPS sub-panel split (extended vs. refresh).
- `.claude/skills/demand-estimation.md` — running CFE demands via the Country API.
- `.claude/skills/release/SKILL.md` — poetry build gotchas when cutting a wheel.
- `.claude/skills/profiling/SKILL.md` — attributing CPU / memory cost inside Country / Feature / Wave hot paths (pyinstrument, cProfile + snakeviz, tracemalloc). Adds `make profile …` targets and a `--profile` flag on `bench/build_feature.py`.
- `bench/feature_audit/README.md` — the **Feature() audit harness**: a deterministic cross-country sweep (`scan.py`) that builds every `Feature('foo')` and `Feature('foo')(**kwargs)`, checks sanity + assembly invariants, clusters findings (`cluster.py`), then triages → red-teams → files them via a Workflow (`audit.workflow.js`). Read before auditing `Feature()` or registering a feature in `index_info`.
- `.claude/skills/backlog-workflow/SKILL.md` — working down the **GitHub issue backlog** with multi-agent orchestration: triage/classify open issues, reconcile against merged work, prioritise by leverage, dispatch worktree-isolated fix-agents that verify against the audit harness, red-team, and open PRs for human merge. Builds on the audit harness; read before running a backlog sweep.
- `prior-art-ledger` (shared sucoder skill at `~/.sucoder/skills/prior-art-ledger/SKILL.md`) — read at the **start of a non-trivial task** (adding/changing an estimator, transform, derived table, or analysis) to avoid reinventing tested machinery or contradicting a definition in force. The repo's ledgers live in `.coder/ledger/` (see `.coder/ledger/README.md`): a §0 standing baseline (`STANDING.md`) of the most reuse-prone machinery + invariants, a per-task `TEMPLATE.md`, and one ledger per task at `.coder/ledger/<issue-or-slug>.md`. Repo adaptation: per-task (not a single shared file), and §3/§4 *cite* `STANDING.md` / `CLAUDE.md` / `lsms_library/data_info.yml` rather than duplicate them.
- `scrum-master-hpc` (shared sucoder skill at `~/.sucoder/skills/scrum-master-hpc/SKILL.md`) — dispatching subagents, worktrees, DVC lock hygiene. Read this before using the Agent tool for multi-country work. Library-specific addenda:
  1. Subagents share the parquet cache at `~/.local/share/lsms_library/`, so concurrent agents building different countries don't conflict.
  2. The venv is at `{repo_root}/.venv/bin/python` (not in worktrees) — set `PYTHONPATH` to the worktree so development-branch code is picked up.
  3. **`.venv/lib/python3.11/site-packages/lsms_library.pth` hardcodes the main-repo path**, so a worktree agent can silently verify against the *main checkout's* code.  **Always set `PYTHONPATH=<worktree>`** — it *does* beat the `.pth` (corrected 2026-07-12; this entry previously claimed the opposite, and the wrong claim sent people to a needless fresh-venv build).

     **The actual trap is `python <script>.py`.**  Python sets `sys.path[0]` to the **script's own directory**, *not* cwd.  So `cd`-ing into the worktree does **not** protect a script run — cwd never enters `sys.path` at all, the `.pth`'s main-repo path wins, and you import the main checkout while believing you are testing the worktree.  Verified:

     | invocation (cwd = worktree) | `PYTHONPATH` | imports |
     |---|---|---|
     | `python -c "import lsms_library"` | unset | worktree ✓ (cwd is `sys.path[0]`) |
     | `python bench/scan.py` | unset | **main checkout ✗ — the trap** |
     | `python bench/scan.py` | `=<worktree>` | worktree ✓ |

     This has already cost a worker agent a full wasted audit run (PR #594).  Since the failure is *silent* and looks like success, **assert it** rather than assume it:
     ```python
     import lsms_library; assert 'worktrees' in lsms_library.__file__, lsms_library.__file__
     ```
     For **config**-only edits (`countries/{C}/_/...` — YAML/scripts/`.org`, the common case), `LSMS_COUNTRIES_ROOT=<worktree>/lsms_library/countries` is the cleaner lever: the live package reads the worktree's config tree (GH #436) and library-code identity stops mattering.  A fresh in-worktree venv is **not** needed for either case.  See the `.pth`-pinned package imports pitfall in the scrum-master-hpc skill.
  4. *Savio compute nodes only*: `.venv` is typically a symlink to `/local/jobNNN/venv` (node-local SSD) and goes stale whenever you land on a different node.  Recovery recipe lives in `.venv.lustre/README_WHY_THIS_EXISTS.md` at the repo root.  **Do not just grab `.venv.lustre/bin/python`** — every import will round-trip through Lustre.  Follow the README's **adopt-or-build recipe** (adopt any importable `/local/job*/venv` already on the node → tar-pipe build only if none → reclaim the stale ones), which gives cross-job persistence without root.  Note: a stable `/local/$USER/<repo>` path is NOT creatable by us (`/local` is `root:root`); a genuinely persistent path needs an HPC-support ticket (README "Admin endgame").  **Opt-in fast path**: `bin/savio_venv.sh` packages the venv as a single squashfs image (`.venv.sqfs`, ~255 MB / one Lustre inode vs ~33k files) and mounts it per job via apptainer's `squashfuse_ll` (`mount`/`umount`/`update` subcommands) — kills the Lustre-MDS load and sidesteps the per-job reaper; see `docs/savio_venv.md` for the model + how to rebuild the image when deps change.  This guidance is Savio-specific; other environments (login nodes, laptops, non-HPC clusters) use a normal in-tree `.venv/` and this paragraph doesn't apply.

## Repository Layout
- Country root-level symlinks (e.g. `Uganda -> lsms_library/countries/Uganda`) are convenience only; actual config lives under `lsms_library/countries/`.
- **DVC repository is rooted at `lsms_library/countries/`, NOT the top-level repo.** `.dvc/`, remotes, and credentials all live there. Run `dvc` CLI commands from that directory or they fail with missing-remote errors.
- **Two Makefiles**: top-level `Makefile` (Poetry setup, pytest, build); `lsms_library/Makefile` (country-specific test/build/materialize/demands). `make -C lsms_library help` for details.

## Cache Behavior (v0.7.0+)

The library has three cache tiers (see `docs/guide/caching.md` for the full picture):

- **L1** -- DVC blob cache at `~/.local/share/lsms_library/dvc-cache/{md5[:2]}/{md5[2:]}`. Content-addressed copies of raw `.dta` source files. Populated lazily by `_ensure_dvc_pulled()` on first read.
- **L2-wave** -- per-wave harmonized parquet at `~/.local/share/lsms_library/{Country}/{wave}/_/{table}.parquet`. Written by `Wave.grab_data()` on first call (YAML path) or by `_/{table}.py` scripts via `to_parquet()` (script path).
- **L2-country** -- per-country aggregated parquet at `~/.local/share/lsms_library/{Country}/var/{table}.parquet`. The v0.7.0 fast path: `load_dataframe_with_dvc` reads it at the top of the function before consulting DVC at all. If the parquet exists and `LSMS_NO_CACHE` is not set, it returns directly. This gives 10–17× cross-process speedups vs. pre-v0.7.0 on all 40 countries.

Override the data root with `data_dir` in `~/.config/lsms_library/config.yml` or the `LSMS_DATA_DIR` env var (env var wins). All three tiers move together.

- **Automatic content-hash staleness (v0.8.0).** Each cached parquet embeds a content hash (pyarrow schema metadata key `lsms_cache_hash`) covering its pre-finalize inputs: `LSMS_CACHE_SCHEMA` (a manual library-version lever in `local_tools.py`), the wave's `data_info.yml`, the wave + country modules (`{wave_folder}.py`, `{country}.py`, `mapping.py`), any wave/country `_/{table}.py` script, the country `_/data_scheme.yml` (table registry + `materialize` flags), the country `_/Makefile` (defines every `!make` build), the build-time `*.org` inputs in `_/` (`food_items.org`, `categorical_mapping.org`, … — `CONTENTS.org` excluded as docs), and the **DVC sidecar md5** of each declared source (`.dta` never hashed/downloaded). On read: match → serve; mismatch → rebuild; **no embedded hash** (pre-v0.8.0 parquet) → trust-once + re-stamp (no rebuild-all on upgrade). Implemented in `Wave._input_hash()` / `Country._table_cache_hash()` with read gates at the L2-country read and both L2-wave reads (`local_tools.cache_freshness` / `stamp_parquet_hash`). Recompute is ~1–4 ms (config/sidecar reads only — no `.dta` hash, no S3, no tree walk), so the v0.7.0 fast path is preserved. *Excluded by design* (they re-run post-read, never touching the parquet): `_finalize_result`, kinship, spellings, categorical mappings, `_join_v_from_sample`. *Limitations*: (a) script-path sources referenced via **dynamically-built paths** (not string literals) are covered only by the script-text hash — an out-of-band edit to such a source still needs a manual clear; (b) script-path L2-wave parquets are written hashless by their scripts, so they can't self-invalidate — `Country._evict_hashless_wave_caches` evicts them before **every** rebuild descent (stale L2-country hash, **absent** L2-country parquet, or `LSMS_NO_CACHE`) so the rebuild re-runs the script (**GH #479** — this closes former *residual F1*: eviction previously fired only in the *stale-hash* branch, so a partial cache (waves present, country table never materialized), or an `LSMS_NO_CACHE` run for which script-path waves are "soft", silently reused stale hashless wave data; the deferred *stamp-after-Make* alternative was rejected as unsafe because `make` can mtime-skip a "fresh" output). Belt-and-suspenders: `Country._assert_built_required_columns` (post-`_finalize_result`, script-path tables only) raises an actionable error if a build is missing a required declared column — the stale-wave-parquet symptom — instead of silently returning wrong data; (c) `assume_cache_fresh=True` skips the hash check entirely. Manual overrides remain (in increasing aggression):
    1. `LSMS_NO_CACHE=1` in the session — bypasses L2-country reads and L2-wave reads on the YAML path. *Soft*: script-path L2-wave parquets (Nigeria PP/PH, Tanzania multi-round) are still read by `run_make_target` without consulting the env var, so a stale L2-wave parquet can shadow a source-script fix.
    2. `lsms-library cache clear --country {Country}` — physically removes L2-country (`var/`), the country-level companion (`_/`), AND every L2-wave parquet (`{wave}/_/{table}.parquet`) for that country. As of 2026-04-25 this also enumerates round-name wave dirs (Nigeria's `2012Q3/`, `2013Q1/`) that don't match `Country.waves`.
    3. `pytest --rebuild-caches` (or `make test-full`) — same as the CLI above for *every* country under `data_root()`, run before the test session, then sets `LSMS_NO_CACHE=1`. Use when verifying a wave-script fix in CI; previously a stale L2-wave parquet could pass a test the source-only fix would have failed (Nigeria/Senegal age sentinels, 2026-04-25).
    4. `LSMS_BUILD_BACKEND=make` — bypasses both parquet tiers entirely; see below.
- **`LSMS_BUILD_BACKEND=make`** bypasses both parquet tiers — every call rebuilds from source, with no L2-country or L2-wave writes or reads. (L1 still serves blobs; the bypass is over the harmonized parquet layer only.)
- **`assume_cache_fresh=True`** is a narrower in-process short-circuit at the top of `_aggregate_wave_data` that still calls `_finalize_result` (so kinship expansion, spelling normalization, and `_join_v_from_sample` still apply). Use when L2-country is known fresh to skip all DVC / existence checks. It ignores `LSMS_NO_CACHE`. (`trust_cache=True` is a deprecated alias for `assume_cache_fresh=True` — still accepted, emits `DeprecationWarning`; slated for removal in a future release.)
- **Cache vs. API**: cached parquets store pre-transformation data. Kinship expansion, canonical spelling enforcement, and dtype coercion happen in `_finalize_result()` on every read — not at cache write time. So `pd.read_parquet(cache_path)` shows raw `Relationship` strings; the Country API shows decomposed `(Sex, Generation, Distance, Affinity)`.
- **DVC stage layer is retired (v0.7.0).** Country-level `dvc.yaml` files are now `stages: {}`. All data loading goes through the cache + `load_from_waves` path. The `reproduce()` code path in `country.py` is dead code pending removal. See `SkunkWorks/dvc_object_management.org`.
- **L2-wave cache for YAML-path tables (added 2026-04-15).** `Wave.grab_data()` now caches its result at `data_root()/{Country}/{wave}/_/{table}.parquet` on first call, for YAML-path tables only (script-path tables already wrote L2-wave parquets via their `to_parquet()` calls). This eliminates the per-wave DVC metadata lookup that previously made `_market_lookup` (and any other wave-iterating code) hit DVC SQLite on every call. Measured speedup: 408s → 0.02s per wave on Uganda `cluster_features`. Same staleness semantics as L2-country (no auto-invalidation; `LSMS_NO_CACHE=1` forces rebuild). The `to_parquet()` function gains an `absolute_path=True` kwarg to skip call-stack inference when the caller builds an absolute path itself.

## Two Build Paths: YAML vs. Makefile/Script

There are two ways a table gets built at the wave level.

- **YAML path (`data_info.yml`)**: preferred for simple cases. `Wave.grab_data()` reads `idxvars`/`myvars`, calls `df_data_grabber()` to extract columns from one source file, applies formatting functions, and returns a DataFrame. No parquet written at the wave level.
- **Script path (legacy `_/{table}.py`)**: declared with `materialize: make` or `!make` in `data_scheme.yml`. A standalone Python script calls `to_parquet()` to write a wave-level parquet. Required when YAML cannot express the transformation:
  - Multiple rounds per wave directory needing distinct `t` values (see `.claude/skills/add-feature/pp-ph/SKILL.md`).
  - Multi-wave source files with a `round` column (Tanzania `2008-15/`; see `multi-round-waves.md`).
  - Elaborate unit conversions or cross-file joins (Nigeria / GhanaLSS `food_acquired`).

**Rule of thumb**: column mappings from one source file per wave → YAML. Cross-file concatenation, per-row `t` assignment, or multi-wave source files → script with `materialize: make`.

## Data Access

Microdata must be obtained from the [World Bank Microdata Library](https://microdata.worldbank.org/) under their terms of use. Contributors pushing write access need GPG/PGP keys.

**User config** lives at `~/.config/lsms_library/config.yml`:
```yaml
microdata_api_key: your_key_here
# data_dir: /path/to/override        # same as LSMS_DATA_DIR env var
# countries_dir: /path/to/config     # same as LSMS_COUNTRIES_ROOT env var
```
Lookup order for each setting: environment variable → config file → None.

**Two independent roots (GH #436 — code/config separation).** `data_dir` /
`LSMS_DATA_DIR` overrides where the *derived caches + DVC blobs* live (see
`docs/guide/caching.md`). `countries_dir` / `LSMS_COUNTRIES_ROOT` overrides
where the *config tree* (`countries/{C}/_/data_info.yml`, `data_scheme.yml`,
`*.org`, per-country `.py`) is read from — default is package-relative
(`files("lsms_library")/"countries"`, via `paths.countries_root()`). Keeping
them independent lets a pip-install or a git worktree point at a config tree
under development while sharing the warm cache. **Resolve config paths via
`countries_root()` / `Wave.file_path` / `Country.file_path`, never via a
hardcoded `files("lsms_library")/"countries"`** — the latter ignores the
override.

**Always read files with `get_dataframe()` from `local_tools`.** It handles `.dta` / `.csv` / `.parquet` via a fallback chain: local file on disk → DVC filesystem (`DVCFileSystem`) → WB NADA download via `data_access.get_data_file()`. A script written as `get_dataframe('../Data/file.dta')` works whether the file is local, DVC-cached, or has never been downloaded.

> **Never run `dvc pull` / `dvc fetch` from the CLI to materialize source data — especially in parallel (multiple agents, `make -j`, scatter-gather sweeps).** The DVC CLI takes the global `.dvc/tmp/lock` and rebuilds the repo index by walking ~7k `.dvc` sidecars (~93 s/call on Lustre); concurrent callers collide and fail with *"Unable to acquire lock"* (measured 91.7% failure at 12 concurrent — see `slurm_logs/dvc_lock_repro/`). `get_dataframe()` / `_ensure_dvc_pulled()` instead download the blob **directly from S3 (lock-free, ~0.1 s)** via the v0.7.3 bypass — so any number of readers can run concurrently. The *write* path (`push_to_cache` / `push_to_cache_batch`) still takes the lock; it retries on contention with backoff (`_run_dvc_with_lock_retry`). A coordinating fetch/write queue was considered and judged **unnecessary**: reads are already lock-free, and writes don't contend in practice — a single contributor's writes batch into one `dvc add`/`push`, and the rare concurrent-writer overlap is absorbed by the retry above. See `SkunkWorks/dvc_writer_distribution.org`.

**Always write parquets with `to_parquet(df, 'name.parquet')`** from `local_tools`. It redirects to `data_root()` via `_resolve_data_path()`, which inspects the call stack to infer country/wave and handles three patterns: bare `foo.parquet` from wave scripts, `../var/foo.parquet` from country scripts, and `../wave/_/foo.parquet` cross-wave refs. Stale parquets from before this migration may still exist in-tree; they are harmless artifacts.

**Anti-patterns — do not use:**

| Anti-pattern                                             | Why                                              |
|----------------------------------------------------------|--------------------------------------------------|
| `dvc.api.open(fn, mode='rb')` + `from_dta(f)`            | Couples to DVC internals, skips the WB fallback  |
| `pd.read_stata('/absolute/path/...')`                    | Breaks on other machines, no DVC/WB fallback     |
| `pyreadstat.read_dta(path)` directly                     | Same — bypasses all access layers                |
| `from_dta('lsms_library/countries/...')` with abs path   | Non-portable; use relative `../Data/` paths      |
| `dvc pull` / `dvc fetch` CLI to materialize data         | Takes the global `.dvc/tmp/lock`; serializes and fails under concurrency. Use `get_dataframe()` (lock-free direct-S3 bypass). |

**Adding new waves**: `lsms_library.data_access.discover_waves()` / `add_wave()`; `push_to_cache_batch()` for batched `dvc add` + `dvc push`. See `CONTRIBUTING.org`.

**Wave provenance (`lsms_library/provenance.py`).** Every wave dir records the WB catalog entry it came from in `Documentation/SOURCE.org`, as org keyword lines (`#+CATALOG_ID:`, `#+CATALOG_IDNO:`, `#+PROVENANCE_SOURCE:` ∈ {`worldbank`, `external`, `unknown`}, …) appended beneath the original free-text URL — which stays the file's first URL, so the legacy `_read_source_url()` reader is unaffected. Human-written prose in a pre-existing `SOURCE.org` is preserved verbatim under a `* Notes (preserved …)` heading; the writer is idempotent. `discover_waves()` matches on the **catalog id**, never on a wave label reconstructed from the entry's year range — labels collide across distinct surveys (Nigeria GHS-Panel W4 `3557` vs. Living Standards Survey `3827`, both 2018–2019) and one entry can span two wave dirs (Uganda `1001` → `2005-06/` + `2009-10/`). Where no id is recorded it falls back to the label heuristic but reports `local_status='unknown'` with `local=False` — silence must not masquerade as knowledge. Countries sharing an ISO code (GhanaLSS/GhanaSPS → GHA; Tanzania/Tanzania_Kegera → TZA) are disambiguated by an `idno_pattern` in `_COUNTRY_CATALOG`; non-WB countries (`EthiopiaRHS`, `KenyaLPS`) are explicitly `discoverable=False`. Re-stamp with `python scripts/backfill_wave_provenance.py`.

**Three-tier credential model.** The WB Microdata API key is the sole real gate; the S3 bucket is a read cache over the authoritative WB NADA downloads.

| User has                                                   | Gets                                                  |
|------------------------------------------------------------|-------------------------------------------------------|
| Nothing                                                    | Import warns; data-access calls raise `RuntimeError`  |
| Valid WB Microdata API key                                 | Direct WB downloads + auto-unlocked S3 read cache     |
| WB API key + S3 write creds                                | The above + push access (for RAs materializing waves) |

Auto-unlock decrypts `s3_reader_creds.gpg` with an obfuscated passphrase at import time. That obfuscation is cosmetic anti-grep, NOT a security gate — the WB API key check is the authoritative policy. Don't "fix" the obfuscation.

## Canonical Schema

`lsms_library/data_info.yml` is the single source of truth for cross-country conventions:
- required columns per table (e.g. `household_roster` requires `Sex`, `Age`, `Generation`, `Distance`, `Affinity`);
- accepted values (e.g. `Sex: [M, F]`, `Affinity: [consanguineal, affinal, step, foster, unrelated, guest, servant]`);
- rejected spellings (e.g. `Relation` → use `Generation, Distance, Affinity`).

`tests/test_schema_consistency.py` reads from this file — never hardcode schema rules in tests.

**Kinship decomposition (Kroeber 1909).** `household_roster` uses four columns instead of a single `Relationship` string: `Sex`, `Generation` (0=same, +1=parent, −1=child), `Distance` (0=lineal, 1=sibling line, 2=cousin), and `Affinity`. `_expand_kinship()` in `_finalize_result()` transforms `Relationship` automatically using `lsms_library/categorical_mapping/kinship.yml`. Unrecognized labels emit a warning — add them to the YAML with their `[Generation, Distance, Affinity]` tuple.

**Canonical spellings.** Columns in `data_info.yml` can declare a `spellings` inverse dict mapping canonical value → list of accepted variants. `_enforce_canonical_spellings()` replaces variants with canonical forms at API time, on both column values and index levels.

**Automatic categorical mappings.** If a column/index name in a returned DataFrame matches a table name in the country's `categorical_mapping.org` (case-insensitive) and that table has a `Preferred Label` column, the mapping is applied automatically — no `mappings:` declaration needed. For name mismatches (e.g. `harmonize_food` for index `j`), use the explicit `mappings:` syntax in `data_info.yml`. Cross-country label harmonization is a design sketch; see `SkunkWorks/cross_country_label_harmonization.org`.

## MonthsSpent / MonthsAway / WeeksAway (2026-04-15)

`household_roster` can optionally include a residence-duration column:

- **`MonthsSpent`**: months the person lived in the household (0–12). Used by Uganda (direct question) and EHCVM countries (binary: Oui→12, Non→0 from `s01q12`). Also used where the source records months present directly (Serbia, Liberia, India, Kazakhstan, pre-EHCVM Niger/Burkina Faso/Mali).
- **`MonthsAway`**: months absent (0–12). Used by Ethiopia (W1–W3), Tanzania, Malawi, Albania, GhanaLSS, Tajikistan, and others where the survey asks "how many months was [NAME] away?"
- **`WeeksAway`**: weeks absent. Used by Ethiopia W4–W5 and Cambodia, where the questionnaire switched from months to weeks.

`roster_to_characteristics()` in `transformations.py` resolves whichever column is present: `MonthsAway` is converted via `12 - value`, `WeeksAway` via `12 - weeks/(52/12)`. The filter excludes members with NaN (question not asked — departed/deceased) and zero months present, **except** infants (age < 1). Countries without any residence column are unaffected — the old count-everyone behavior continues.

**Root cause context**: the replication pipeline (`lsms.tools.get_household_roster`) did `dropna(how='any')` on `[HHID, sex, age, months_spent]`, implicitly excluding departed members. The current API's runtime derivation previously counted everyone in the roster, producing a 1315-HH drift on Uganda `household_characteristics`. Adding MonthsSpent + the filter resolved this to ~220 residual outliers (age-bracket boundary shifts from `age_handler`'s DOB-derived fractional ages).

**EHCVM note**: EHCVM 2018-19 countries lack a continuous months variable. The binary `s01q12` ("lived continuously 6+ months?") is mapped to 0/12. Guinea-Bissau may need Portuguese keys (`Sim`/`Não`) alongside `Oui`/`Non`. See CONTENTS.org files for per-country documentation.

## Coverage Matrix (v0.9.0+)

`make matrix` grades every `(country, feature, wave)` cell on a tier ladder — `absent` / `dropped` / `broken` / `builds` / `sane` / `blessed` — and commits a snapshot to `.coder/coverage/latest.csv`. `ll.coverage()` reads it back. See `docs/guide/coverage.md`.

**`sane` is not `blessed`, and the difference matters.**

- **`sane`** — the automated checks passed. *No human has necessarily looked at a single number.*
- **`blessed`** — a human read the actual numbers for that cell and believes them. Recorded in the git-tracked `.coder/coverage/blessed.csv`.

A feature can build cleanly, pass every sanity check, and still be quietly wrong — wired to the wrong source column, or carrying an unverified unit conversion. So **for anything feeding published analysis, `sane` is not enough.**

> **The rule: if you used a cell in real analysis and looked at its numbers, bless it in the same PR.**
> `country,feature,wave,blessed_by,date,note` (`wave` blank for country-level features). Blessings accrete; never bulk-seed them. An empty blessing file is honest — a file full of blessings nobody gave makes `blessed` a synonym for `sane` and destroys the tier.
> Do **not** put `#` comments in `blessed.csv`: `load_blessed()` reads it without a `comment=` arg, so a comment line becomes a phantom blessed cell.

**Do not trust a `sane` cell as proof an issue is fixed.** Know what the grader does *not* look at — it is a cold build (so it cannot see warm-cache-only divergences) and it does not check currency labels.

### Adjudicating `absent` cells

`absent` says only "not declared for this wave" — which conflates *"the survey never asked"* with *"nobody wrote the config yet"*. Verdicts go in the git-tracked `.coder/coverage/absent_verdicts.csv` (`country,feature,wave,verdict,checks_run,evidence,adjudicated_by,date`):

| verdict | meaning | closes the cell? |
|---|---|---|
| `todo` | data is there, config missing | no — stays `absent`, but now sized + sourced |
| `asked-not-distributed` | instrument asked; the shipped extract lacks it | **yes** → acquisition queue |
| `not-asked` | genuinely never asked | **yes** → closed forever |
| `unsure` | a required check could not be run | no — stays in the queue, records why |

> **A closing verdict REQUIRES evidence — `load_verdicts()` refuses one without it.** A closing verdict is a permanent, unsupervised write. An unevidenced negative is unfalsifiable, and therefore permanent whether or not it is true.
>
> This already went wrong: `Albania/_/data_scheme.yml` asserted *"earlier waves have no shocks module"*, but Albania 2005's `migrationE_cl.dta` carries `m6e_q00 = 'Type of Shock Code'` with ten shock types. False, uncatchable (nothing recorded *how* it was reached), and it suppressed ~5 cells of work. **Never write an unevidenced "no module here" claim, in a verdict file or in a YAML comment.**

Before closing a cell, run the four checks (see `docs/guide/coverage.md`). The two that are most often skipped and most often wrong: **C2 (sibling-wave differential) is necessary but NEVER sufficient** — module vocabularies change completely between waves; and **C4 (the questionnaire) is mandatory**, because *absence in the shipped `.dta` is not absence in the instrument*. Only the questionnaire separates `not-asked` from `asked-not-distributed`.

## Derived Tables

`household_characteristics`, `food_expenditures`, `food_prices`, and `food_quantities` are **auto-derived at runtime** via `_ROSTER_DERIVED` and `_FOOD_DERIVED` in `country.py` (source transforms live in `transformations.py`). **Do NOT register them in `data_scheme.yml`** — `Country.data_scheme` auto-surfaces them when the source table (`food_acquired` or `household_roster`) is present. A `!derived` YAML tag was considered and rejected (2026-04-18): it would create a migration burden with no gain over the hardcoded dicts + auto-discovery.

**`labels=` kwarg (2026-04-18).** Any table with a `j` index level accepts `labels='Aggregate'` (or any column name from the country's `food_items` / `harmonize_food` table). For derived food tables (`food_expenditures`, `food_quantities`), `reaggregate=True` sums collapsed categories. For all other tables (including `food_acquired`), `reaggregate=False` renames without summing, preserving per-unit and per-source row detail.

**`units=` kwarg (2026-05-06, Phase 4).** `food_prices()` and `food_quantities()` accept a `units=` kwarg that controls the price/quantity basis of the returned DataFrame. Other tables reject it.

- `food_prices(units=...)`: `'kgvalue'` (default — `Expenditure / Quantity_kg`, currency per kg, backward-compatible with the pre-Phase-4 implementation), `'unitvalue'` (`Expenditure / Quantity` per native `u`; gives 1 = "Kwacha per Kwacha" for `u='Value'` rows), `'kgprice'` (reported `food_acquired.Price` × kg_factor), `'unitprice'` (reported `Price` per native `u`). The `*price` modes return NaN where the survey did not record a unit price; **no silent fallback** to the `*value` modes.
- `food_quantities(units=...)`: `'kgs'` (default — kilograms where `u` is convertible, native quantity carried with the native `u` tag where it isn't; the `u` index distinguishes `'kg'` from native rows), `'units'` (sum of native `Quantity` per `(t, v, i, j, u, s)`).
- The `'kgvalue'` default deliberately departs from the term-of-art "unit value" common in the literature (Deaton 1988, 1997), which usually means `Expenditure / Quantity` standardized to kg. The denominator-explicit naming avoids ambiguity.
- See `slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org` for full rationale.

**Uganda derivation path now fires** (post-#245 `_/food_acquired.py` Phase-3 fix and the subsequent `food_prices_quantities_and_expenditures.py` removal). Wave-level scripts emit canonical `[t, i, j, u, s]` parquets with columns `[Quantity, Expenditure, Price]` via `uganda.food_acquired_to_canonical()`; the country-level `_/food_acquired.py` concatenates them; `_FOOD_DERIVED` produces `food_expenditures` / `food_prices` / `food_quantities` at runtime from that.  Pre-#245 the country-level concatenator did `df['t'] = t` then `groupby(['i','t','j','u'])`, which raised on pandas 2.x; the bug stayed hidden behind a stale country-level cache.  Surfaced and fixed by the `--rebuild-caches` regression net (PR #243).

`Country._DEPRECATED` maps removed/deprecated table names to deprecation messages. `__getattr__` checks it before `data_scheme`, returning a method that emits `DeprecationWarning` and calls a compatibility shim. Currently contains `locality` only. See `docs/migration/locality.md`.

## `sample()` and Cluster Identity

The `sample` table (index `(i, t)`, columns `v`, `weight`, `panel_weight`, `strata`, `Rural`) is the single source of truth for mapping households to their sampling cluster. **As of 2026-04-10, `v` is joined from `sample()` at API time** by `_join_v_from_sample()` in `country.py`, called from `_finalize_result()` for any household-level table when the country has `sample` in its `data_scheme` and `v` isn't already present. **A feature can opt out of the v-join declaratively** (GH #436/#455 — the old hardcoded `_no_v_join` set in `country.py` is gone): in the canonical `lsms_library/data_info.yml`, either declare the table's index *without* `v` under `Index Info > index_info` (auto-exempts, e.g. `shocks`/`assets`), or add the feature to the `Join v from sample > skip_extra` list (e.g. `livestock`/`income`). No `country.py` edit. See `.claude/skills/add-feature/SKILL.md`.

Rules:
- Do NOT put `v` in feature `data_scheme.yml` indexes other than `cluster_features` (which owns it).
- Do NOT bake `v` into feature parquets. Wave scripts should write `(t, i, ...)` and let the framework join.
- Do NOT use `dfs:` merge blocks just to join `v` from a cover page — collapse to a single-file extraction.
- Two weight types: `weight` (cross-sectional; positive for all interviewed HH including refreshment); `panel_weight` (longitudinal; NaN/zero for refreshment). Pre-refreshment waves have the same value in both columns.
- Country caveat: `Country(name).household_roster()` only gets `v` in the index if the country has `sample` in its `data_scheme.yml`.
- `_join_v_from_sample()` skips when `v` is already in `df.columns` (not just `df.index.names`), so legacy scripts with `v` as a non-index column still work. Prefer putting `v` in the index or nowhere in new code.

Skill: `.claude/skills/add-feature/sample/SKILL.md`. Migration history: `slurm_logs/PLAN_sample_v_migration.org`, `slurm_logs/DESIGN_sample_as_v_source.org`.

`panel_ids` and `updated_ids` are `@property` attributes on `Country`, not methods — they return dicts, not DataFrames. Code iterating over `data_scheme` entries and calling `getattr(c, name)()` must special-case these. Use `diagnostics.load_feature(c, name)` which handles both.

## Panel ID Transitive Chains and the `attrs` Flag

`_finalize_result()` runs `id_walk()` and sets `df.attrs['id_converted'] = True` to prevent double-application. **`merge()` and `set_index()` drop `attrs` in pandas 2.x** — both appear in `_join_v_from_sample()`. When `attrs` is lost, `_finalize_result` runs `id_walk` a second time on already-converted data, and for countries with transitive chains (A→B→C, where B is itself a mapping key) this produces household-level ID collisions and duplicate index entries. Burkina Faso 2021-22 had 392 duplicate tuples before this landed in commit `4db41a27`.

**Rule**: any framework method touching a DataFrame downstream of `id_walk()` in `_finalize_result()` must explicitly copy `attrs`:
```python
result = flat.set_index(new_idx)
result.attrs = dict(df.attrs)  # preserve id_converted flag
```

## Gotchas with Teeth

- **`age_handler` returns fractional years when DOB is available.** As of 2026-04-15, when both a reported integer age and a DOB are present and agree within 1 year, `age_handler()` returns the DOB-derived fractional age (e.g. 3.75 instead of 3). This is more precise but means `Age` in `household_roster` is no longer always integer-valued. Code that assumes `int(age)` or bins on integer boundaries (e.g. `household_characteristics` age brackets) will see ~200 boundary-shift cases per country. The `_enforce_canonical_dtypes` Int64 coercion in `_finalize_result` rounds fractional ages to integer for the API output, but intermediate DataFrames (wave-level, pre-finalize) carry the fractional values.

- **`other_features` is obsolete** — it's fully replaced by `cluster_features` + `_add_market_index()` at query time. Do not read `other_features.parquet` in new code. The `m` index should NOT be baked into cached parquets; it's added on demand when the user passes `market='Region'`. If a wave-level script genuinely needs region for data processing (Malawi's region-specific unit factors), read the cover-page `.dta` directly.

- **EHCVM countries**: in Senegal, Mali, Niger, Burkina Faso, Benin, Togo, and Guinea-Bissau, each `grappe` is visited in exactly one `vague` — so `v: grappe` (not `v: [vague, grappe]`) and `i: [grappe, menage]` (not `[vague, grappe, menage]`). CotedIvoire 2018-19 is also EHCVM but predates the list above. **EHCVM 2018-19 waves lack a continuous MonthsSpent variable** — they use binary `s01q12`/`s01q13` (>=6 months y/n), mapped to MonthsSpent 12/0. See CONTENTS.org per country.

- **`format_id` is auto-applied to `idxvars` but NOT to `myvars`**. A numeric column reaching `myvars` (e.g. a cluster ID) stays float and gets `.0` suffixes when stringified. Fix with an explicit formatting function in the wave module. `is_this_feature_sane()` checks for this via `_check_float_stringified_index`.

- **Joining `v` via `dfs:` is legacy**. Since Phase 2 (2026-04-10) you should not add a cover-page sub-df just to pick up `v` — let `_join_v_from_sample()` do it. Existing `dfs:` merges are grandfathered but should be collapsed when touched.

- **Housing schema is categorical, not binary.** Uganda and Malawi `housing` have `Roof` and `Floor` columns with material-name values (`Grass`, `Iron Sheets`, `Smoothed Mud`, …). Uganda maps via `categorical_mapping.org`; Malawi normalizes case via inline `mapping:` dicts in each wave's `data_info.yml`. Consumers who want binary indicators derive them trivially (`df['Roof'] == 'Grass'`); the reverse is not possible.

- **Categorical columns from `.dta` / `.sav`**. `get_dataframe()` returns pandas categoricals from Stata/SPSS. `select_dtypes(exclude=['object']).max()` crashes on unordered categoricals — exclude `'category'` too. `groupby().first()` crashes similarly — convert to string with `.astype(str).replace('nan', pd.NA)` first. YAML mapping keys must be string keys (`'urbana': Urban`) not numeric (`1: Urban`) when the raw labels are strings.

- **`lsms` upstream dependency has been retired.** `lsms (>=0.4.13,<0.5.0)` is no longer in `pyproject.toml`. The dead imports were cleaned up in the 2026-04-13 session (commits `f8178fcc`, `75a2e55a`, `7cfe6c6a`, `1ac20d9c`, `16d07628`); `rg 'from lsms\.tools import' --type py lsms_library/` should return zero. **Do not use `from lsms.tools import` in new code** — use `get_dataframe` and `df_data_grabber` from `local_tools` instead.

- **`locality` is deprecated.** `Country('Uganda').locality()` emits `DeprecationWarning` and returns via `legacy_locality(country)` from `transformations.py`, which joins `sample()` and `cluster_features()` to reproduce the legacy `(i, t, m) -> v` shape. The 9 wave-level `locality.py` scripts and `uganda.other_features()` have been deleted. See `docs/migration/locality.md`.

- **`_log_issue` writes to the user cache, not the source tree.** Materialization failures are appended to `~/.cache/lsms_library/issues.log` (via `platformdirs.user_cache_path`), keeping `lsms_library/ISSUES.md` as a human-maintained tracker that is never auto-modified. Override the log path with `LSMS_ISSUES_LOG=/path/to/file`. Fixed in GH #148.

## Pandas 3.0 Targets

This codebase targets pandas 3.0+. Headline rules — for the full breakdown see cq unit `ku_c12795f626444715a6d8b71acc657b60`:

- No `inplace=True` anywhere (it's removed in 3.0).
- `pd.NA`, not `np.nan`, for missing values in string / ID / categorical columns. `np.nan` is still fine for numeric floats.
- `pd.isna()` / `pd.notna()` over `np.isnan()` — the latter raises `TypeError` on `pd.NA`.
- `.bfill()` / `.ffill()`, not `fillna(method=...)` (removed in 2.0).
- Use `df.loc[mask, 'col'] = val`, not `df[mask]['col'] = val` — chained indexing silently fails under CoW.
- `.iloc[0]` for positional Series access; `series[0]` is deprecated.

## Countries Without Microdata

Some countries have configs but no source `.dta` in the repository:

| Country          | Reason                            | Source                                                       |
|------------------|-----------------------------------|--------------------------------------------------------------|
| Nepal            | NSO hosts data, not WB            | https://microdata.nsonepal.gov.np/ (free registration)       |
| Armenia          | No data files downloaded          | WB catalog, external hosting                                 |
| Timor-Leste 2001 | No `_/` config for this wave      | WB catalog                                                   |
| Guatemala        | No PSU/cluster variable in data   | ENCOVI 2000 design                                           |

## Design / Skunkworks References

- `SkunkWorks/dvc_object_management.org` — content-hash cache invalidation (stage layer retired in v0.7.0; hash-based invalidation **implemented in v0.8.0** — see the "Convergence: implemented design" section).
- `SkunkWorks/dvcfilesystem_runtime_override.org` — how the pip-install scenario works (runtime config override, lazy credential validation, no git ancestor required).
- `SkunkWorks/cross_country_label_harmonization.org` — design sketch for `Feature(...)(harmonize=...)`.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **LSMS_Library**. Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze --skip-agents-md` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/LSMS_Library/context` | Codebase overview, check index freshness |
| `gitnexus://repo/LSMS_Library/clusters` | All functional areas |
| `gitnexus://repo/LSMS_Library/processes` | All execution flows |
| `gitnexus://repo/LSMS_Library/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
