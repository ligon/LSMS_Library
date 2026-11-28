# Prior-Art Ledger — Uganda interview dates (#1045)

**Search tier used:** ripgrep + git, existing instrument and raw-identity records.

## §1 Task, restated

Migrate Uganda's eight existing household interview-date wave scripts to the
declared Gregorian day parser. Keep their original cover paths, identifiers,
wave labels and country compiler. Stamp date diagnostics from the original
cover rows on the wave output and the native Make compiler result.

## §2 Existing machinery

| Symbol | Authority | Decision |
|---|---|---|
| Parser, partial-date keys, strict mode | `lsms_library/datetime_parse.py` (`assign_datetime`, `wave_datetime_audit`); `lsms_library/derivations.yml` | Reuse |
| Report stamp and footer reader | `lsms_library/local_tools.py` (`to_parquet`, `read_parquet_datetime_audit`) | Reuse |
| Native compiler and ID mapping | Uganda `_/interview_date.py`, `_/uganda.py:id_walk`, #1287 declaration tests | Keep one walk and missing-wave refusal |
| Country/Feature replay | `Country._read_l2_country_cache`, `Country.load_dataframe_with_dvc`; calendar parser tests | Reuse, no framework edit |
| Phase 0 scope and country caveats | `1045-calendar-inventory.org`, Uganda `_/CONTENTS.org` | Keep interview/visit/clock and shock/DOB scopes separate |

## §3 Definitions & conventions in force

See `STANDING.md` §§3–4 and `lsms_library/data_info.yml:795`.
The existing `Int_t` is the household-cover calendar date. Dates use midnight
without a timezone. The World Bank DDIs for
[1001/F135](https://microdata.worldbank.org/catalog/1001/data-dictionary/F135),
[2166/F64](https://microdata.worldbank.org/catalog/2166/data-dictionary/F64),
[2059/F70](https://microdata.worldbank.org/catalog/2059/data-dictionary/F70),
[2663/F74](https://microdata.worldbank.org/catalog/2663/data-dictionary/F74),
[3460/F83](https://microdata.worldbank.org/catalog/3460/data-dictionary/F83),
[3795/F71](https://microdata.worldbank.org/catalog/3795/data-dictionary/F71)
and [3902/F76](https://microdata.worldbank.org/catalog/3902/data-dictionary/F76)
document the 2009–20 fields. The separate
[1001/F41](https://microdata.worldbank.org/catalog/1001/data-dictionary/F41)
listing names alternate date fields and separate clock fields; the held 2005
sidecar and actual cover projection verify the selected `h1bq2a/b/c` instead.
The 2019–20 date producer uses `Data1/HH/gsec1.dta` (same declared digest as
the alternate sample cover path), not a new source.

## §4 Invariants & assumptions

Full-date values, household identity, grain and wave tagging are unchanged.
Do not infer date bounds, time of day, timezone, response status, or visit
from adjacent columns. A row without a valid date remains unserved. A row
with a valid date must not be removed because its optional provenance is null.

An independent projection of the eight original DVC covers (24,362 rows)
reproduced the 98 old null-date rows. Eleven are valid partials in the decided
#1045 policy: four year-only 2005–06 and seven year/month 2010–11. No previously
parsed date changed or was lost. Of the remaining 87, 72 are wholly blank
(48 in 2010–11, four in 2015–16, 20 in 2019–20) and 15 are impossible dates in
2010–11. No source-wide sentinel is declared.

## §5 Reuse decision

Use `assign_datetime` on the existing indexed components, with
`calendar='gregorian'` and day resolution. No new derivations or parser.
Read the exact input wave footers on the native compiler route. Filter the
output only on `Int_t` so that observed dates with no derivation survive.

## §6 Open questions for the human

No new decision in this scope. #1045's raw partial-date provenance and device
clock questions remain open separately.

### Phase 3 — verification

- Wave parsing and country audit shipping — OK (§§2–5): the source-free
  wave/compile/replay tests execute the eight actual scripts and original
  compiler, including the W8 path and the strict invalid-day refusal.
  Together with the parser and declaration tests, 270 passed and two skipped.
- Country and Feature default — OK (§§3–5): a fresh native run and separate warm process
  reproduced 24,264 exact original rows and 11 padding keys/households/dates
  verified by a separate projection of their original covers. All other frame
  metadata is exact. Eight reports replay on all-wave requests, with only the
  selected report replayed for 2010–11 and 2009–10 by Country.
  The final isolated ten new tests were repeated after tightening fixture isolation.
- Scope — OK (§§1–2): one-time fingerprint movement in all 224 Uganda wave
  entries and 28 Uganda country entries. No parser, registry or sample edit.
