# Prior-Art Ledger — null-read-guard (silent all-null reads)

> Per-task ledger. Inherits `.coder/ledger/STANDING.md`. Edit in place; git is the
> journal.

**Search tier used:** ripgrep + git (floor). gitnexus not consulted (index
freshness unverified in this worktree; the floor was sufficient — the machinery
being mirrored is named explicitly in `CLAUDE.md`).

## §1 Task, restated

The library's readers can return a DataFrame of the **right shape with null
contents, raising nothing**. Every existing guard checks *shape*:
`Country._assert_built_required_columns` checks a declared column is *present*;
the `dfs:` sub-frame guard (GH #515/#323) checks a required column is *present*
after a sub-df drop; `_audit_index_collapse` checks *index uniqueness*.
`get_dataframe` — the only sanctioned reader, and hence the natural chokepoint
over the local / DVC / WB-fallback paths — does **no post-read content
validation at all**.

Task: measure how often a naive "column is 100% null after read" test would fire
across the corpus, then (if the rate permits) add a guard that **warns** by
default, is **fatal under its own `LSMS_READ_STRICT` lever**, is collectable by
`bench/feature_audit/`, and names country / wave / table / column / source.

Three motivating instances:

1. **#699** — `Peru/1990/Data/*.SSP` are SAS XPORT V5. `pyreadstat.read_xport`
   returns **7 of 10 columns entirely NaN** and raises nothing (`pandas.read_sas`
   reads all 10). Today `get_dataframe` has no XPORT branch at all, so these
   files raise `ValueError("Unknown file type")` — **the silent-null class here
   is prospective**, latent behind whoever wires the wave.
2. **Ghana `.DAT`/`.DCT`** — the `.DCT` declares fixed-width; the `.DAT` ships
   comma-delimited. A fixed-width parse yields all-NaN, no exception. Another
   agent owns that parse fix; this task owns only the post-read detection.
3. **Niger 2014-15 `Latitude`** — declared `float` in
   `Niger/_/data_scheme.yml:23`, 0 of 270 rows populated, passes everything
   (`CLAUDE.md`, §"A dropped optional sub-df…").

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `get_dataframe` | `lsms_library/local_tools.py:805` | the only sanctioned reader; local → DVC → WB fallback; `read_file` tries spss/parquet/dta/csv/excel/feather/fwf in order | yes | **extend** — add post-read probe at its single `return df` |
| `Country._assert_built_required_columns` | `lsms_library/country.py:~2355` | post-build, script-path tables: raises if a *required declared column is missing* | PR #243 regression net | **extend** — same site gains a nullity (not just presence) probe |
| `_audit_index_collapse` | `lsms_library/country.py:4473` | measures what a collapse would destroy; returns `None` when provably lossless, else a report dict | yes | **pattern to mirror** (report-dict shape, "None means provably clean") |
| `_format_grain_report` | `lsms_library/country.py:4570` | turns a report dict into an actionable sentence naming country/table/wave | yes | **pattern to mirror** |
| `_record_grain_report` / `_emit_grain_report` / `_GRAIN_LEDGER` | `lsms_library/country.py:4676`–`4667` | file report for the cache writer, then raise (strict) or warn | yes | **pattern to mirror** |
| `grain_reports()` | `lsms_library/country.py:4670` | public read-only accessor for tests / audit harness | yes | **pattern to mirror** → `null_read_reports()` |
| `_grain_strict()` | `lsms_library/build_transforms.py:396` and `country.py:4462` | env predicate; `LSMS_GRAIN_STRICT` in `{1,true,yes}` | yes | **pattern to mirror** → `_read_strict()` / `LSMS_READ_STRICT` |
| `GrainCollapseWarning` / `GrainCollapseError` | `country.py:4454` / `4447` | own classes so CI can target them | yes | **pattern to mirror** → `NullReadWarning` / `NullReadError` |
| `_GRAIN_AUDIT_KEY` stamp + `_replay_grain_audit` | `local_tools.py` / `country.py:4688` | persist the finding into the L2 parquet and re-emit on the warm read | yes | **decide** — only needed if the emitting site is build-only |
| `build_transform` / `framework_imports_fingerprint` | `lsms_library/_build_registry.py` | folds build-function closures into `Wave._input_hash` | yes | **avoid** — must not perturb (measured, §Phase 3) |

## §3 Definitions & conventions in force

- **`get_dataframe` is the only sanctioned reader**; `to_parquet` the only
  sanctioned writer — `STANDING.md §4`, `CLAUDE.md` §"Data Access".
- **Warn-by-default, strict-by-env** is the established shape for a "we destroyed
  / mis-served data" signal: `country.py:4462` docstring — *"making it fatal out
  of the box breaks ~30 countries at once and gets reverted, and a revert is how
  the class survives."*
- **No known-bad allowlist** — same docstring: *"an allowlist is the same disease
  with a registry."*
- **A signal must survive the cache.** `CLAUDE.md` §"Grain Collapse": the L2
  parquet is written post-transformation, so a detector at the destruction site
  is structurally unable to fire on a warm read. Hence stamp + replay.
- **Niger 2014-15 `Latitude` is *honestly* absent.** `Niger/_/CONTENTS.org:384`:
  *"2014-15 genuinely ships no geovariables/offsets file of any kind (the wave
  directory contains none), so its Latitude/Longitude are honestly absent -- not
  mis-addressed. It is NOT wired, and that is correct."* The wave's
  `data_info.yml` declares no `Latitude` in `cluster_features.myvars` at all.
  **Therefore no read produces it** — it materialises in the cross-wave concat.
  A `get_dataframe`-only guard is structurally blind to it.
- **`optional:` is country-grain, the absence is wave-grain** — `CLAUDE.md`
  §"A dropped optional sub-df…": *"`data_scheme.yml` is **country**-grain, so
  `optional: true` disarms the column for *every* wave … while the check is
  per-wave."* Same mismatch applies here.

## §4 Invariants & assumptions

- `STANDING.md §4` in full (sanctioned IO, no `v` in feature parquets, pandas 3.0
  rules, `attrs['id_converted']` survival).
- **Lane**: this task owns *post-read validation only*. It must not touch
  `read_file`'s parse chain (another agent, `.DAT`/`.DCT`) nor
  `_apply_categorical_mappings` (GH #694, a different null-producing bug
  downstream of both sites).
- **Cache-hash neutrality is a claim to be measured, not assumed.** Every `_/*.py`
  build script imports `get_dataframe`, and `framework_imports_fingerprint`
  folds resolved import closures into `Wave._input_hash`. Measured before/after
  — see Phase 3.
- **An all-null column is sometimes legitimate.** A guard that fires on hundreds
  of innocent cells is noise, and noise is how the original #323 warning got
  ignored (`country.py:4444`).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| null-content audit of a frame | **new** (`_audit_null_content`) | no existing helper measures *values*; all three existing guards are shape-only (§1) |
| warning/error classes, strict predicate, ledger, public accessor | **extend by mirroring** grain's shape verbatim | `CLAUDE.md` brief: "one learnable shape beats two" |
| strict lever | **new env var `LSMS_READ_STRICT`** | different concern from grain collapse; same semantics (`{1,true,yes}`) |
| emission sites | **two** (Site R in `get_dataframe`, Site B post-build in `country.py`) | Site R cannot see Niger (§3); Site B cannot see a mis-parse of an undeclared column. Grain's own Site 1 / Site 2 is the precedent |
| audit-harness visibility | reuse the `grain_reports()` accessor pattern | brief requires it; `bench/feature_audit/scan.py` currently collects **no** grain reports either (measured) — so this adds the collection for both |

## §6 Open questions for the human

- **Strict mode will raise on a documented-correct cell.** Under
  `LSMS_READ_STRICT=1`, Niger 2014-15 `Latitude` fires even though
  `CONTENTS.org:384` says the absence is correct. Fixing that properly needs
  *wave-grain* optionality, which does not exist (`optional:` is country-grain).
  Not built here. Blocks: turning `LSMS_READ_STRICT=1` on in CI.

## §7 Measurement (the half of the task that decided the design)

**Raw records, scripts and hash probes: `slurm_logs/null_read_guard/`** (see its
`README.org`). The numbers below are reproducible from there.

Cold. `LSMS_DATA_DIR=/global/scratch/.../nullguard_data`, private, empty at
start; its `dvc-cache` symlinked to the shared L1 (content-addressed immutable
blobs — sharing them changes nothing measurable, and the *parquet* tiers, which
are what could fake a clean result by skipping `get_dataframe`, were private and
cold).

**Site R — reads.** 2,264 declared source references enumerated from every
`data_info.yml` (`file:` + `dfs:`) plus `scan_script_data_refs` over every
wave/country `_/ *.py`. 1,337 held and read through the real `get_dataframe`
with its own defaults; 909 not held (772 are `.parquet` *outputs*, not sources);
18 unmeasured (read raised).

| test | firings |
|---|---|
| any column 100% null | 887 cells / 153 files (1.4% of 65,099 columns) |
| ...that a table actually asked for | **3** (Albania 2002 `metadata_cl.dta` → `int_t_v2@interview_date`) |
| frame ≥ 1/3 columns all-null | **0** — corpus max 0.283 |
| frame 100% null | **0** |

**Site B — built tables.** 504 cold builds; 488 completed.

| test | firings |
|---|---|
| required declared column null in EVERY wave | **0** |
| required declared column null in SOME wave | **86**, over 42 of 488 tables, 17 countries |

**Known-bad parses, reproduced on the real blobs** (evidence the threshold is in
a real gap, not fitted to noise): `pyreadstat.read_xport` on Peru
`EXPEND.SSP` 2/5, `PANEL.SSP` 2/4, `N00A.SSP` 7/10; `read_fwf` on GhanaLSS
`COMM.DAT` per `COMM.DCT` 254/311. Range 0.40–0.82 vs. corpus max 0.283.

**Both #699 and the `COMM.DAT` class are PROSPECTIVE, not currently firing.**
`Peru.data_scheme == []` (no table reads a `.SSP`; `get_dataframe` raises
"Unknown file type" on them today — loud, not silent), and no declared table
reads `COMM.DAT` (0 matches in the manifest). The corpus cannot fire Site R
today. Its value is protecting the parse work now in flight.

**Unmeasured, stated as gaps** — never counted as clean: 137 script-route
non-parquet refs whose relative path my resolver could not resolve (Nigeria's
`Nigeria/2010-11/Data/*.csv` literals are root-relative, not `_/`-relative);
Nepal (11 tables) and Armenia (2) hold no microdata; Peru is unreadable today.

---
### Phase 3 — verification

- `null_read_audit._all_null_columns` / `audit_read` / `audit_declared_columns`
  — **OK (§2, §5)**: no existing helper measures *values*; the three named
  guards are shape-only. Not a reinvention of `_audit_index_collapse` (that
  measures row destruction under a collapse; this measures emptiness), but
  deliberately the same report-dict/`None`-means-clean shape.
- `NullReadWarning` / `NullReadError` / `_read_strict` / `_NULL_READ_LEDGER` /
  `null_read_reports` — **OK (§2, §5)**: structural mirrors of
  `GrainCollapseWarning` / `GrainCollapseError` / `_grain_strict` /
  `_GRAIN_LEDGER` / `grain_reports`, per the brief's "one learnable shape".
  Own env lever, same `{1,true,yes}` spelling (`country.py:4462`).
- Site B placement in `Country._finalize_result` — **OK (§4)**: it is the
  documented read-path stage and is already in `_EXCLUDED_CALLABLES`. Measured:
  0 of 37 probed `_table_cache_hash` values move. Placing it beside
  `_assert_built_required_columns` in `_aggregate_wave_data` would have moved
  all 37 (that method is a tagged build orchestrator). Pinned by
  `test_site_b_host_is_excluded_so_site_b_costs_no_invalidation`.
- Site R placement in `local_tools.get_dataframe` — **CONTRADICTION of the
  brief's expectation, reported not hidden (§4)**: it moves all 37 hashes. Not
  avoidable at the sanctioned chokepoint — `df_data_grabber` is
  `@build_transform()`-tagged and references `get_dataframe`, so its source is
  in every table's fingerprint. The concurrent `.DAT`/`.DCT` parse fix edits
  the same function and incurs the identical one-time rebuild.
- `_EXCLUDED_CALLABLES` additions for the audit's own callables — **OK (§4)**:
  same rationale the file already gives for `_finalize_result`. Without them a
  reworded warning would rebuild the corpus. Pinned by
  `test_the_audit_module_is_not_folded_into_any_build_fingerprint`.
- Required-vs-optional reading — **OK (§3)**: reuses
  `country._required_scheme_columns`, the existing single source of truth, so
  this guard cannot drift from the two that already use it.
- `bench/feature_audit/scan.py` `_null_read_findings` — **OK (§2)**: drains the
  public accessor into typed findings. Note it duplicates the string capture
  (`runtime_warning`) for the same event; `cluster.py` dedups by fingerprint.
- **Not touched, per lane**: `read_file`'s parse chain (sibling agent) and
  `_apply_categorical_mappings` (GH #694). Neither site is inside or upstream of
  `_apply_categorical_mappings`; Site B runs *after* it in `_finalize_result`,
  so it will *report* #694's damage rather than obstruct its fix.
