# Prior-Art Ledger — #1045 Nigeria W4

**Search tier used:** ripgrep+git floor (wave script, shared parser, cache
tests, country documentation and the released NBS v03 dictionary).

## §1 Task, restated

Move the existing Nigeria 2018-19 interview_date script for the 2018Q3 and
2019Q1 household visits to the build-time parser. Audit the decoded
InterviewStart by logical round without interpreting the device time zone
or changing the served visits, clocks, or response status.

## §2 Existing machinery

| symbol | path | what it does | tested? | choice |
|---|---|---|---|---|
| `df_data_grabber` | `lsms_library/local_tools.py` | Projects the native cover and formats the household id | yes | reuse |
| `assign_datetime` | `lsms_library/datetime_parse.py` | Parses aligned input and files reports by logical round | `tests/test_gh1045_parse_datetime.py` | reuse |
| `wave_datetime_audit` | `lsms_library/datetime_parse.py` | Selects reports for the physical folder, including both logical rounds | same | reuse |
| `interview_result_label` | `lsms_library/countries/Nigeria/_/nigeria.py` | Maps the recorded result label, not the earlier paper code card | `tests/test_gh1096_nigeria_interview_result.py` | reuse |

## §3 Definitions & conventions

- Standing repo conventions in `STANDING.md §3`. `Nigeria/_/data_scheme.yml`
  and the W4 clock and response sections in `Nigeria/_/CONTENTS.org` define
  the grain and distinguish a dated visit from a completed interview.
- NBS v03 covers F98/V3073 and F99/V3097 label InterviewStart as date-time.
  The exact committed Stata sidecars decode `%tc` to `datetime64[ms]`.

## §4 Invariants & assumptions

- Keep all and only 5,224 planting and 5,012 harvest visits with a recorded
  date, including 203 dated noninterviews. The 39 and 13 blank times stay
  unserved, but the audit must count them before that filter.
- Keep the two household cover inputs, native millisecond precision, calendar
  date and time of day, and existing key, order and response handling.
  Do not attach a timezone, change a date at a day boundary, or floor the clock.
- Standing cache conventions in `STANDING.md §4`. Scope the new reports to
  the existing physical W4 footer with its two logical wave keys.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| Start clock | reuse decoded Stata series and shared Gregorian parser | the survey stores a full clock, not date components or raw numeric ticks at this stage |
| Missingness | reuse shared report and current drop rule | missing dates are counted but are not visits in this table |
| Resolution | restore the input millisecond storage unit after parsing | the shared parser returns microseconds by default, but the existing wave parquet is millisecond-resolution |

## §6 Open questions for the human

- Which timezone, if any, does the released device clock represent? The
  dictionary and survey field documentation do not establish it. That blocks
  interpreting or modifying the clock and does not block auditing it.

### Phase 3 — verification

- W4 clock inputs — OK (anchored on §3–4): both admitted Stata covers decode
  `%tc` to an unzoned millisecond series. Independently decoded native
  timestamps match the legacy path after restoring that storage unit.
- Shared parse and drop rule — OK (anchored on §2–5): the report attaches
  before the current date filter and keeps the two logical rounds distinct.
  The exact cover census finds 39 and 13 blanks and 203 dated noninterviews.
- Date-time precision — OK (anchored on §4–5): the fixture includes
  millisecond values bracketing midnight, with unchanged labels and time
  values and no synthesized date-only placeholders.
