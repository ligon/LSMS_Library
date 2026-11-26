# Prior-art ledger — #1045 Nigeria W1–W3 calendar migration

Inherits [STANDING.md](STANDING.md) §0. Search tier was git and ripgrep,
live tracker rulings read before implementation, and the approved held
date-component census on 47df62d49.

## §1 Task

Migrate Nigeria W1–W3 interview_date to the established build-time parser.
Keep source order, round tags, status and complete dates. Serve only the
three additional visits supported by the existing partial-date rule.
Leave W4/W5, sample, roster and weights unchanged.

## §2 Existing machinery

- `datetime_parse.assign_datetime` and `datetime_reports` are the shared
  #1045 implementation. Tanzania's `2008-15/_/interview_date.py`
  uses both on a multiround script path and stamps existing to_parquet
  metadata. `Country._replay_wave_stamp` supplies warm reporting.
- `::interview_date::date-day-imputed` is already registered in
  `lsms_library/derivations.yml`. Its framework input callable returns
  aggregate counts, not raw/rejoinable records. That distinct #1045
  residual is not repaired here.
- Nigeria's existing source files, format_id key handling, case/codes
  shared status formatters and finalizer's sample-v join are unchanged.

## §3 Definitions

- #1045 decisions preserve an explicitly declared calendar, footer
  replay, independent LSMS_DATE_STRICT and registered first-day padding
  for a genuine year/month plus blank day. No other imputation here.
- Six released NBS household forms, PDF p2, use Q13 civil day/month/year
  and a separate start time. PH forms PDF p1 give February/March 2011,
  2013, 2016, agreeing with the paired PP/PH survey vintages recorded
  in Nigeria's own SOURCE.org (catalog 1002, 1952, 2734). This grounds
  the Gregorian civil calendar without inferring it from raw date ranges.
- #1096 (PR #1508) keeps dates of non-interviews and their own status,
  and keeps genuinely undated rows from entering on status alone.
  A supported partial date carries its provenance rather than claiming
  an observed day. No status or quarter window filter is permitted.

## §4 Exact evidence and invariants

- 28,940 source rows, 28,708 old dated rows identically retained. The
  new parser adds two 2015Q3 rows and one 2016Q1 row with complete-visit
  status 1 and a true Stata system-blank day, but known year and month.
  Remaining omissions are 214 all-blank and 15 impossible calendar days.
  No declared sentinels on these observed fields. Full counts, sidecars,
  and reproducible audit are in the private Phase A evidence
  runtime/ng-1045-phase-a.pFc0SQZN.
- W3's native monthly codes, not its categorized month text, remain
  the input. The W3 PH alternate date text is diagnostic only and
  conflicts with three complete triples. W3 PP custom logical t for
  PH-only households remains in force. Do not change accepted dates,
  status units, or any earlier date's grain.
- W4 status/timestamps and W5's undeclared date coverage are unchanged.
  W4 device clock and any scope expansion belong to separate rulings.
- Keep the country legacy Make concatenator separate from the normal
  public Country default/Make API and wave-footers path. Make no claim
  about direct Make manifest/report behavior outside the actual check.

## §5 Reuse decision

Call the registered parser after preserving the two existing round reads
and concatenation order. Keep date/status and optional derivation columns;
drop the date-null complement as before. Stamp through the existing
writer. Do not introduce new calendar code or edit shared functions.

## §6 Questions

None for the scoped migration. W4 clock, raw partial-date replay,
other quantities and broad #1045 completion stay open.

### Phase 3 — verification

- OK (§§2–5): 66 focused source-free tests passed, 26 data tests
  deselected. The actual wave scripts kept the intended read and write
  order, retained a dated refusal, dropped both blank and impossible
  dates, stamped both logical round reports per physical wave, replayed
  only the requested round, and attached exactly three modeled keys to
  the synthetic table. GH #1096's data oracle independently pins the
  exact cover row and expected date, including the three additions.
- OK (§4): metadata-only census found all 20 Nigeria country-table
  hashes and 120 wave-table hashes (20 per migrated logical round)
  changed; all 80 W4/W5 hashes and the prior round declarations stayed
  fixed. No shared parser/transform code or old wave module changed.
- Country and Feature real-data parity is a separate independent check;
  this implementation did not reopen raw data after Phase A.
