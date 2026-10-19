# Workshop Charter — GH #1045: one build-time parser for every calendar quantity (run 1)

> Agreed with @ligon in phases 0–1 (2026-10-18). §2 is the oracle the engine
> loops against. This charter covers **run 1** of #1045 (inventory + parser +
> Mali/Tanzania pilot); later runs reuse it with §1 widened.

## §0 Question, restated
Dates are parsed in the wrong place (a read-time guess in `_finalize_result`,
after the waves are concatenated) and constructed ad hoc (87 `to_datetime`
calls in 52 country files). @ligon wants "a general date-time parser clever
enough to reliably handle all date-time variables", applied with **the same
discipline and mechanism across the corpus** -- not only `interview_date`, but
every calendar quantity, including ones read today as strings or kept as
`(d, m, y)` integer triples.

Premises challenged in phase 0 and how they were resolved:
- *"One parser fixes it" vs. what it can be checked against.* A parser with no
  consumer cannot be validated on real data, so run 1 includes a two-country
  pilot (Mali, Tanzania -- the #798 pair).
- *"Build-time parsing" vs. the warm cache.* A build-time warning fires once,
  on the cold build; the report must be stamped into the parquet and replayed
  (decision 2), else the change hides the signal (#323's lesson).
- *Script-path tables run in `make` subprocesses*, so a report recorded there
  never reaches the parent: the stamp must be written by the writer
  (`to_parquet`) in the subprocess. Riskiest piece; see §5.
- *"Partial dates are a parser option"* -- rejected: a padded day is a
  construction and goes through the derivation registry (decision 3).
- *"Untagged parser is cheaper"* -- rejected: the fingerprint hashes the text of
  the calling `mapping.py`, not the parser, so an untagged parser goes stale
  silently (decision 1).

Decisions (@ligon, 2026-10-18; recorded in #1045): (1) scoped
`@build_transform(tables=[...])`; (2) stamp + replay of the parse report;
(3) partial dates served as day 1 and registered as a derivation; (4) a
declarative `data_info.yml` spec with `calendar:` required (no default);
(5) own strict lever `LSMS_DATE_STRICT`.

## §1 Scope & non-goals
**In scope (run 1):**
- **Phase 0 inventory** of every calendar quantity in the corpus, from the
  SOURCES (wave `data_info.yml`, wave/country scripts, a name/label sweep of
  declared source files via `get_dataframe`), not only the served parquets:
  where it is read, how it is built today, its calendar, served or not, current
  stored type. Committed as `.coder/ledger/1045-calendar-inventory.org` (or
  `.csv` + notes).
- **The parser** (`local_tools`, name provisional `parse_datetime`): component
  columns (y/m/d[/H/M/S], partial allowed), Stata `%td`/`%tc` and SPSS
  numerics, strings with `format=` or the PR #1025 evidence rule; per-wave
  sentinels; out-of-range and fieldwork-window counts; `calendar:` required,
  Ethiopian via `lsms_library.calendars`; month-of-year and year-only kinds
  (nullable integer, not a date).
- **The declarative spec** in `data_info.yml` (`datetime:` / `format:` /
  `stata:` / `sentinels:` / `calendar:` / `window:` / `resolution:`), wired
  through `Wave.grab_data` for YAML-path tables; scripts call the function.
- **Reporting**: `DatetimeCoercionWarning` family, `datetime_reports()`
  accessor, `LSMS_DATE_STRICT=1` -> `DatetimeParseError`; the report stamped
  into parquet schema metadata at BOTH the L2-wave and L2-country tier (and from
  `to_parquet` in script subprocesses) and replayed on warm reads.
- **Partial-date derivation** registered (framework-level entry in
  `lsms_library/derivations.yml`), stamped on the rows it serves; where a row
  already carries a key (EHCVM visit), keys are joined with `MULTI_SEP`.
- **Read-time cast** (`_coerce_datetime_column`): pass-through for datetime64;
  the "warn on strings" switch is implemented but OFF by default until the
  migration finishes.
- **Pilot migration**: Mali and Tanzania `interview_date` (every wave).

**Out of scope / non-goals:**
- Migrating any country other than Mali and Tanzania (later runs).
- `harvest_month` / `planting_month` / `shocks` years (Phase 3) and dates of
  birth / `age_handler` (Phase 4) -- inventoried in Phase 0, not migrated.
- Durations (`MonthsAway`, `Days`, `PersonDays`, ...).
- Changing any served date VALUE without an explained, counted reason.
- Any shared-L2 write, any corpus re-warm, any DVC CLI.
- Merging: the PR is for human merge.

## §2 Definition of done — the loop oracle
- [ ] **Inventory** committed; every calendar quantity found carries: country,
      wave, table/column (or "unserved"), source variable(s), current
      construction (path:line), calendar, current stored type. The census of
      2026-10-18 (in #1045) is a subset of it -- every row of that table appears.
- [ ] **Parser tests** pass in a new `tests/test_gh1045_parse_datetime.py`
      covering: each input kind (components, partial y-m, y-only, %td, %tc,
      SPSS, string with format, string without format -> PR #1025 rule),
      sentinels reported as sentinels, out-of-range components counted not
      coerced, fieldwork-window counts, Ethiopian calendar via
      `lsms_library.calendars`, a spec with no `calendar:` refused, month-of-year
      kind, `LSMS_DATE_STRICT=1` raises.
- [ ] **Stamp/replay**: a test builds a table whose parse reports a sentinel,
      then in a FRESH process reads it warm and receives the same report
      (warning + `datetime_reports()`), for one YAML-path and one script-path
      table.
- [ ] **Scoped tag**: a no-op edit to the parser moves
      `build_transforms_fingerprint` for exactly the `tables=` list and no
      other table (measured, positive and negative control); a test asserts
      `tables=` equals the set of tables declaring a calendar column in
      `data_info.yml`.
- [ ] **Partial-date derivation**: registered; loader accepts it;
      `ll.derivations()` lists it; every served padded date carries the key
      (test); a row with an existing key carries both.
- [ ] **Pilot**: Mali and Tanzania `interview_date` parquets store
      `timestamp[us]`; served dates are identical to the pre-change served
      frame row by row, OR every difference is counted and explained (expected
      for Mali: the PR #1025 NaT/ambiguity rows -- say which are recovered and
      why); no `DatetimeCoercionWarning` for a cause the spec now handles.
- [ ] **Read-time cast** is a pass-through on datetime64 input (test); the
      string-warning switch exists, is OFF by default, and is tested ON.
- [ ] **Cache cost** measured and stated: which hashes move, per country/table.
- [ ] **Full suite** green apart from known environmental failures, run per the
      Makefile recipe.
- [ ] **Docs**: CLAUDE.md/AGENTS.md section ("Dates") with the measurements;
      Mali and Tanzania CONTENTS.org updated; #1045 residual drafted.
- [ ] Reviewed and accepted by @ligon (judgement call).

## §3 Constraints & assumptions
- **Budget:** one engine run on the savio3 node (≤ ~6 h wall); ≤ 3
  implement↔red-team iterations.
- **Tools allowed:** repo venv; private `LSMS_DATA_DIR`; `get_dataframe` /
  `get_data_file` only (no DVC CLI); PDFs read only by subagents.
- **Data available:** staging re-warm at `3e3bd26ee` (stale for 9 countries
  since 2026-10-18; Mali and Tanzania among them -- rebuild in the private root).
- **Must not break:** PR #1025's ambiguity contract for un-migrated countries;
  the cache-hash invariants in CLAUDE.md; every existing test.
- **Conventions:** CLAUDE.md (strict levers per concern, no allowlists, no
  silent coercion, derivations registry, `countries_root()` read-only,
  pandas 3 rules); `.coder/ledger/STANDING.md`.

## §4 Links
- **Prior-art ledger:** `.coder/ledger/1045-date-parser.md` (phase 2).
- #1045 (design and decisions), #798 / PR #1025 (`_coerce_datetime_column`,
  `DatetimeAmbiguityWarning`), #1037, #1038, `lsms_library/calendars.py`,
  `lsms_library/null_read_audit.py` (reporting/lever pattern),
  `lsms_library/derivations.py`, `_build_registry.py`.

## §5 Open questions & risks
- **Script-path stamping** across the `make` subprocess boundary (Tanzania
  `2008-15`, Nigeria): if `to_parquet` cannot carry the report cleanly, kick
  back to @ligon with options rather than drop decision 2 for script tables.
- **Timing of the scoped re-warm**: the tag moves every table on its list once
  (~30 countries' `interview_date` + `crop_production` + `shocks`). Merging with
  the held corpus group (#1019/#1024/#1027/#675) makes that free; otherwise it is
  a second large re-warm. @ligon decides at merge time.
- **Resolution**: declare `datetime64[us]` (14 countries already store it;
  Liberia `[ms]`).
- **Two derivations on one row** (EHCVM visit + padded day): `MULTI_SEP` join is
  the precedent; confirm `summary_for_frame` counts both.
- **Calendar of interview dates in Ethiopia / EthiopiaRHS**: unknown per wave;
  Phase 0 must establish it from the questionnaire, never infer it.
