# Prior-Art Ledger — GH #1045 run 1: one build-time parser for every calendar quantity

> Per-task ledger. Inherits `STANDING.md` (§0 baseline: `get_dataframe` /
> `to_parquet` are the only IO, cache hashes are content-addressed, `attrs`
> survive only when every input agrees). Charter: `.coder/charter-1045-date-parser.md`.
> Inventory (phase 0): `.coder/ledger/1045-calendar-inventory.org`.

**Search tier used:** ripgrep + git (floor). GitNexus MCP failed to connect
this session; every blast radius below is an `rg` call-site sweep.

## §1 Task, restated
A declared calendar column (today: `interview_date.Int_t` / `Interview start` /
`Interview end`, the only `datetime` declarations in `data_info.yml` and in any
country `data_scheme.yml`) is built ad hoc per wave and then GUESSED into a
datetime by the read-time cast in `_finalize_result` after the waves are
concatenated. Run 1 adds one build-time parser (`parse_datetime`) with a
declarative `data_info.yml` spec (`calendar:` required), a per-wave parse
report that is stamped into the L2-wave and L2-country parquets and replayed on
warm reads (own lever `LSMS_DATE_STRICT`), a registered derivation for padded
partial dates, and migrates the Mali and Tanzania `interview_date` waves to it.
The read-time cast becomes a pass-through for `datetime64` input.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `_coerce_datetime_column` | `country.py:6231` | read-time cast: per-value `format='mixed'`; year-last d/m order settled per `t` from evidence, else `NaT` + warning | `tests/test_gh798_datetime.py` (PR #1025) | **extend**: pass-through for datetime64 (already), add OFF-by-default string warning |
| `DatetimeCoercionWarning`, `DatetimeAmbiguityWarning` | `country.py:6165,6185` | warning family with `country/table/column/count/examples` (+`reason`,`wave`) | same | **reuse / move** to the parser module, re-exported from `country` so `country.DatetimeCoercionWarning` still resolves |
| `_YEAR_LAST_DATE` | `country.py:6206` | regex for `d/m/Y` or `m/d/Y` | same | **reuse** the evidence rule (moved with the classes) |
| `_to_datetime_mixed` | `country.py:6211` | `format='mixed'`, warns on mixed tz | same | reuse as the no-format string fallback idea; parser has its own per-value path |
| `calendars.ethiopian_to_gregorian` / `parse_ethiopian_month` | `calendars.py` | Ethiopian (y,m,d) -> Gregorian `date`; month names -> 1..13 | `tests/test_calendars.py` | **reuse** for `calendar: ethiopian` |
| `null_read_audit` lever/report/ledger | `null_read_audit.py:116-480` | `_read_strict()`, `NullReadError/Warning`, `_NULL_READ_LEDGER`, `null_read_reports()`, `_clear_*` | `tests/test_null_read_guard.py` | **reuse the pattern** (own lever `LSMS_DATE_STRICT`, own ledger) |
| `_GRAIN_AUDIT_KEY`, `read_parquet_grain_audit`, `to_parquet(grain_audit=)` | `local_tools.py:2238,2384,2506` | stamp a report list in parquet schema metadata; footer-only read | grain tests | **extend**: sibling key `lsms_datetime_audit`, `to_parquet(datetime_audit=)` |
| `_replay_grain_audit`, `_replay_wave_stamp`, `_wave_grain_audit`, `_GRAIN_LEDGER.pop` | `country.py:6977,7012,7035,4644` | replay a stamp on warm read; wave-stamp replay at all three wave read sites; per-wave filter for the wave write | grain tests (#868) | **extend** each with the datetime twin |
| `_parallel_waves._ledgers/_delta/relay` | `_parallel_waves.py:399-560` | relay worker ledgers to the parent | `tests/test_parallel_waves.py` | **extend**: third ledger |
| `build_transform(tables=)` / `build_transforms_fingerprint` | `_build_registry.py:62,425` | scoped fingerprint folding; root entry points folded per table; callees recursed by `_closure_parts` | `tests/test_build_transform_hash.py` | **reuse**; plus an `_EXCLUDED_CALLABLES` entry so the ALL-scoped `grab_data` does not pull the parser into every table (see §4) |
| `derivations.py` loader, `MULTI_SEP`, `union_keys`, `summary_for_frame`, `lsms_library/derivations.yml` | `derivations.py:115,169,590` | registry, row key column `Derivation`, `'a+b'` multi-key | `tests/test_derivations.py` | **reuse**: framework entries (empty country slot) |
| `df_data_grabber` / `Wave.column_mapping` | `local_tools.py:1589`, `country.py:1230` | YAML `myvars` -> columns; list-valued myvar + formatter = row-wise construction (`Mali/2014-15/_/mapping.py:112`) | integration | **extend** `column_mapping`/`grab_data` with a dict-valued `datetime:` spec |
| `melt_visit_intervals` | `local_tools.py` | EHCVM visit start/end melt, stamps visit derivation key | EHCVM tests | untouched in run 1 (not a pilot country path) |

## §3 Definitions & conventions in force
- `interview_date` columns `Int_t`, `Interview start`, `Interview end`: `type: datetime`, `Derivation` optional — `lsms_library/data_info.yml:389-420`.
- Declared-type map `'datetime'|'date'|'timestamp' -> 'to_datetime'` — `country.py:5843`.
- Derivation key `country::table::name`, empty slot = all; two keys on a row joined `+` sorted — `derivations.py:142,169`; bases vocabulary incl. `modelling-choice` — `derivations.py:119`.
- Strict-lever spelling `{1,true,yes}` — `null_read_audit.py:143`.
- Rulings: `U_UNKNOWN` for missing unit (#1041) — not touched; float-stringified codes normalised at read (`format_id` class) — the parser normalises `'2014.0'`-style components numerically at the point of read.

## §4 Invariants & assumptions (landmines)
- **The fingerprint walks callees.** `Wave.grab_data` is `@build_transform()` (all tables) and `_closure_parts` recurses into every lsms_library callable it names. A parser called from `grab_data` would therefore be folded into EVERY table — defeating decision 1 — unless excluded from recursion. Root entry points are folded from `_BUILD_TRANSFORMS` directly (no `_is_build_callable` check), so `parse_datetime` in `_EXCLUDED_CALLABLES` **and** tagged `tables=['interview_date']` is folded for exactly that table. Scripts reach it through `framework_imports_fingerprint`, which also skips excluded callables — harmless, because a script-path `interview_date` gets the parser through its btf anyway.
- **The wiring sites are hashed.** `Wave.grab_data`, `to_parquet`, `_aggregate_wave_data`, `_replay_wave_stamp`, `_replay_grain_audit`, `_wave_grain_audit` are all in every table's closure (`_EXCLUDED_CALLABLES` does not list them). Any edit there — including adding the datetime stamp/replay — moves every table's hash once. This is a ONE-TIME corpus invalidation for the wiring, distinct from the scoped cost of a later parser edit. Must be measured and stated, not paid (dispatcher ruling).
- **A build-time report must outlive the cold build** (#323): the parquet a warm read serves was written by a process that may be gone; stamp at the L2-wave (YAML path, `grab_data`), at the L2-country write, and in the script subprocess (`to_parquet` in `make`), replay at every warm read site.
- **Script subprocess has no framework context**: the precedent (`build_transforms.reduce_to_agreed` docstring) is that the script hands the ledger to `to_parquet`. Here `to_parquet` collects the process ledger itself, matched by table = parquet stem.
- **`to_parquet` stringifies object columns** — a column of `datetime.date` objects becomes `large_string` (`local_tools.py:2575`). That, not the sources, is why Mali 2014-15/2017-18 and Tanzania 2008-15/2019-20 store strings. A `datetime64` column survives.
- **Tanzania `2008-15/` is one folder, four rounds** via `round` -> `t`; the script dedups on `(r_hhid, round)` with an assert (`Tanzania/2008-15/_/interview_date.py`); reports must be per round `t`, not per folder.
- **Mali EAC-I waves (2014-15/2017-18) have two passages**; `_normalize_dataframe_index` collapses `visit` (`Mali/_/data_scheme.yml:70-83`); parsing must happen before that and not change which row is selected (most-complete-row: a NaT vs a date changes completeness!).
- `_finalize_result` is read-path and excluded (`_build_registry.py:127`): editing `_coerce_datetime_column` moves no hash.
- pandas 3: `pd.NA` for missing ints (`Int64`), `datetime64[us]` resolution declared.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| warning classes | reuse (move to `datetime_parse`, re-export) | one family; tests import from `country` |
| year-last evidence rule | reuse logic | same contract as PR #1025, now applied per wave at build time |
| Ethiopian conversion | reuse `calendars` | tested |
| strict lever + ledger + accessor | new (own lever, copy of the `null_read_audit` pattern) | decision 5: own concern |
| stamp key + reader | extend local_tools (sibling of grain audit) | same atomic-with-data argument |
| replay | extend `_replay_wave_stamp` + country warm reads | one replay site per read site, like #868 |
| padded partial dates | reuse derivations registry (framework entries) | decision 3 |
| parser | **new** `lsms_library/datetime_parse.py`, re-exported from `local_tools` | nothing parses components/stata/SPSS/strings with sentinels + report today; a module of its own keeps the scoped tag and the exclusion in one place |

## §6 Open questions for the human
- Wiring cost is corpus-wide once (§4). Merge with the held corpus group (#1019/#1024/#1027/#675) or accept a second re-warm — @ligon at merge time (charter §5).
- Fieldwork `window:` not declared for the pilot: no window source was read in this run (questionnaire/BID dates); the mechanism is tested on synthetic data.

---
### Phase 3 — verification (run 1, at `837032c8c`)
- `DatetimeCoercionWarning` / `DatetimeAmbiguityWarning` / `_YEAR_LAST_DATE` — OK (anchored on §2, §5): moved to `datetime_parse`, re-exported from `country`; `country.DatetimeCoercionWarning is datetime_parse.DatetimeCoercionWarning` (test). A new base `DatetimeWarning` puts the build-time `DatetimeParseWarning` beside, not under, the read-time class.
- `parse_datetime` year-last rule — OK (§2): same evidence rule as `_coerce_datetime_column` (PR #1025), applied per wave `t` at build time; the read-time copy is untouched for un-migrated countries (`test_pr1025_contract_unchanged_for_unmigrated_strings`). Not a REINVENTION: the read path is the thing being retired, and it stays only as the fallback.
- Ethiopian dates — OK (§2): `calendars.ethiopian_to_gregorian` / `parse_ethiopian_month`, no new arithmetic.
- Lever / ledger / accessor — OK (§2, §5): the `null_read_audit` pattern, own lever `LSMS_DATE_STRICT`, `_DATETIME_LEDGER` in `_EXCLUDED_CONSTANTS`.
- Stamp key + reader + `to_parquet(datetime_audit=)` — OK (§2): sibling of `lsms_grain_audit`; replay at every read site the grain stamp replays at, plus the `assume_cache_fresh` short-circuit (which the grain stamp does not — pre-existing gap, residual).
- Scoped tag — OK (§4 landmine 1): tagged roots + `_EXCLUDED_CALLABLES`; measured: a no-op parser edit moves `build_transforms_fingerprint` for exactly `interview_date`; `grab_data`'s closure contains no `datetime_parse` part.
- Wiring cost — as predicted in §4 landmine 2: every verifiable hash moves once (btf 39/39, cth 549/549, wih 2,109/2,139).
- Partial-date derivation — OK (§2): framework entries, keyed per table (`::interview_date::...`); an empty table slot was tried first and CONTRADICTED `derivations.summary_for_frame`'s rows-0 listing (it attached to every table; caught by `test_derivations::TestAttach` and `test_malawi_sale_basis::TestDelivered`).
- `column_mapping` — CONTRADICTION found and fixed: always adding a `datetime_specs` key broke `ehcvm.py`'s single-file unpack of the mapping (code review); the key is now added only when a spec exists.

---
### Merge with the corpus group (follow-up `merge`, at `ddd79800c` = #1019/#1024/#1027/#1052)
- **#908 (read gate extracted into `Country._read_l2_country_cache`)** -- composed: the country-tier date replay moved INTO the gate beside `_replay_grain_audit` (still `waves=`-filtered); `_DATETIME_LEDGER.pop` moved to the top of `load_dataframe_with_dvc`, beside the grain/drop pops and BEFORE the gate. The gate and the replay functions are excluded, so measured: a no-op edit to either moves 0/39 btf, 0/549 `_table_cache_hash`, 0/2,139 `_input_hash`.
- **#675 (`lsms_build_drops`)** -- composed: both keys at all four write sites (L2-wave; L2-country no-stage / stage-consolidate / DVC-fallback); both replayed at the L2-wave gate and the L2-country gate (fresh-process test `test_date_and_drop_stamps_coexist_at_every_gate`, mutation-checked). `_parallel_waves` relays grain, date-parse and drop ledgers together (`test_a_forked_wave_relays_all_three_ledgers`).
- **Registry** -- union of `_EXCLUDED_CALLABLES` / `_EXCLUDED_CONSTANTS`; `EXPECTED_ENTRY_POINTS` unchanged by the merge (15; it moved 10 -> 15 at `db695bec5`).
- **Pilot re-run vs a `ddd79800c` build in a separate root** -- Mali 24,939 / 0 diffs; Tanzania 22,427 -> 22,428, 0 value diffs, the one new row keyed (cold, fresh-process warm, `assume_cache_fresh`).
- **#1040 (wave-tier gates, same bundle)** -- one conflict, the `assume_cache_fresh` serve; compose by putting the date replay inside `Country._read_assumed_fresh_cache` on each hit (trial-merged in a throwaway worktree: 214 passed, 2 skipped (#809 not-warm) across #1040/#1045/#675/#809/#920/parallel/hash tests). The wave-tier gate needs nothing: the date replay lives in `_replay_wave_stamp`, which #1040's `_read_l2_wave_cache` calls.
- **Found, pre-existing on both sides -- FIXED on this branch by `3e931cfe7` (GH #1066, 2026-10-19; it rides the corpus bundle):** the DVC-fallback L2-country write (`except (DvcException, FileNotFoundError)`) stamps no `lsms_grain_audit`, so on a country root with no DVC repo (pip install, `LSMS_COUNTRIES_ROOT` checkout) a grain loss warns once on the cold build and is silent on every warm read -- reproduced identically at `ddd79800c`.

---
### Red-team r1 follow-up (2026-10-19; verdict pass-with-fixes; run 1 accepted by @ligon 2026-10-19)
- **F1 (required) -- closed, both halves.** (a) The synthetic fresh-process fixtures now build either tree (`_make_country(write=)`): `no-stage` copies the shipped `countries/.dvc/config` + `no_scm` (REUSE: the `tests/test_gh1001_no_intree_credential_write.py` pattern; a bare `no_scm` config is refused because `local_tools._build_dvcfs` adds credentials to `ligonresearch_s3` whenever a `.dvc/config` exists and DVC needs that remote's url), `dvc-fallback` is the old tree. `_assert_write` checks the route from the cold build's stderr. Both stamp/replay tests are parametrized over the two writes, so the fallback keeps the coverage r1 found it had nowhere else. (b) `test_real_l2_country_stamp_is_written_and_replayed_warm[Mali|Tanzania]`, cache-gated: process 1 reads (rebuilds a stale root through the no-stage write), the footer is asserted, process 2 reads warm (`from_cache` on every report) and must emit exactly the stamp's warnings -- one `DatetimeParseWarning` naming Tanzania 2008-09, none for Mali (pins the sibling-class decision on real data). Mutation M3' (no-stage `datetime_audit=` only): synthetic 3 red (every no-stage case), every fallback case green; real-data 2 red **on a warm, fresh root without a cache clear** -- the write is inside the fingerprinted `_aggregate_wave_data`, so the mutation moves the hash and forces the rebuild. M2' (no-stage `build_drops=` only): `coexist[no-stage]` red, `coexist[dvc-fallback]` green.
- **F3 -- closed by the exclusion (recommended item 2).** `country._replay_wave_stamp` added to `_EXCLUDED_CALLABLES` (one line, end of the #1045 "(b) Pure REPORTING" block). Whole-corpus probe: run-1 head -> +exclusion 39/39 (+`None`), 549/549, 2,109/2,139; +exclusion -> no-op in `_replay_wave_stamp` 0/0/0; F3 reproduced without it 39/549/2,109; positive control `df_data_grabber` 39/549/2,109; base `ddd79800c` -> tip still 39/549/2,109 (one re-warm). CONTRADICTION avoided in the docs: `_replay_grain_audit` stays folded through the stage-cache read (a no-op there still moves 39/549/2,109) -- the AGENTS.md table says so rather than implying the whole replay side is free. REUSE: the #908 probe helpers (`_gate_probe_hashes`, `_probe_edit`) for the pin `test_editing_the_wave_stamp_replay_moves_no_hash`. `git merge-tree` vs `origin/fix/1040-wave-gate`: the registry line merges cleanly; the test first conflicted at the file's tail (1040 appends after the #908 tests) and was moved beside `test_excluded_callables_are_write_or_hash_only` -- final merge-tree: only the known `assume_cache_fresh` hunk in `country.py`.
- **§3 harness false positive -- closed.** The check is `diagnostics._check_no_constant_columns` (scan.py relays `is_this_feature_sane`), so the exemption is made there, by NAME (`derivations.COLUMN`), not by an all-but-one-NA threshold. Real scan of Mali/Tanzania `interview_date`: 11 -> 10 records, only `no_constant_columns Tanzania ['Derivation']` gone. 0/0/0 hashes.
- **Not acted on, by instruction:** verdict §5's five questions (EHCVM time of day; DK day; two-digit years; derivations `inputs` contract; warning-class split) are with @ligon. Run 1's RESIDUAL #1045 is unchanged (its open item 3, raw joinable `partial_date_inputs`, is due before run 2).
