# Prior-art ledger — Niger's EHCVM interview dates (#1045)

**Search tier:** ripgrep and git, the exact held covers, producer documentation,
and the already-landed Burkina Faso EHCVM treatment (#1752).
See [STANDING.md](STANDING.md).

## §1 Task, restated

Move the six existing start/end bindings per wave from inferred string coercion
to explicit Gregorian, ISO date-time parsing at build time. Niger declares
interview dates only for 2018–19 and 2021–22, with one existing household/visit
key. No new interviews, dates, clocks, or coverage are inferred.

## §2 Existing machinery

| Machinery | Authority | Decision |
|---|---|---|
| `datetime` YAML spec | `lsms_library/datetime_parse.py` and the Burkina Faso configs (#1752) | Reuse |
| Three visit ordinals | `Niger/_/niger.py:interview_date`, `local_tools.melt_visit_intervals` | Keep |
| Parse footer and subset replay | `Wave.grab_data`, `Country._read_l2_country_cache` | Reuse |
| Lowest dated visit in numeraire | `conversion._interview_dates` and Niger CONTENTS (#1585) | Keep and verify |

## §3 Definitions & conventions in force

The household cover measures six civil, timezone-naive timestamps, not visit
dates to be floored or converted. Census on the exact held 2018–19 and 2021–22
sidecars verifies every nonblank, nonplaceholder value in all 12 fields is
`YYYY-MM-DDTHH:MM:SS`. The current catalog dictionaries label the same six
fields as start/end by ordinal. Both producer 2021–22 household forms confirm
the role in Section 0 (p. 3):
[form 1](https://microdata.worldbank.org/catalog/6276/download/100373),
[form 2](https://microdata.worldbank.org/catalog/6276/download/100372).
The [2018–19 v02 dictionary](https://datacatalog.ihsn.org/catalog/10298/data-dictionary/F1?file_name=s00_me_ner2018)
shows length 19. The current [2021–22 DDI](https://microdata.worldbank.org/metadata/export/6276/ddi)
shows length 9 despite the exact held release's 19-character values. The
online versions and widths do not stand in for the original tracked bytes.
The [Survey Solutions guide](https://docs.mysurvey.solutions/faq/date-and-time-formats/)
describes this syntax, but not the actual question-time or device timezone
of these files.

## §4 Invariants & assumptions

Keep the input cover, keys, literal host clock, visit ordinal and original
start/end assignments. A visit with neither endpoint is still absent.
Keep older-than-fieldwork and reversed endpoint cases without reordering,
new window-based diagnosis, or timezone inference (#1585). The visit 1
date remains the input to currency conversion in both EHCVM waves.

Original covers have 6,024 and 6,622 rows. The earlier wave has 83 exact
`##N/A##` placeholders and six blanks. The later wave has 140 blanks and no
placeholder. Every old parsed timestamp remains exact under the explicit
specification, across all 75,876 operands. No new timestamp appears.

## §5 Reuse decision

Use six existing declarative specs per wave, with microsecond precision by
default. Declare only the measured 2018–19 placeholder. Keep the shared hook
and report replay. No edits to the age/roster date, shared parser or cast
switch.

## §6 Open decisions

Clock authority and the general derivation-input contract stay with #1045.
The precise 2021–22 date window for later visits is not established by a
fieldwork calendar and is not used here.

### Phase 3 — verification

- Raw format and legacy parse parity — OK (§§3–5), measured on both exact
  original covers. All timestamp/blank positions match the old rule.
- Real cold/warm parity — OK (§4): 37,898 exact visits including all keys,
  dtypes and attrs by stage, on explicit DVC Country and Feature, and on
  explicit Make Country. The inherited visit index still reads as nullable
  `Int64` cold and physical `int64` warm on the matched routes.
- Report and numeraire controls — OK (§§2–5): 12 reports stamp or replay,
  six per requested wave. Warm reads decode no sources. The actual currency
  lookup preserves its 12,646 first-visit timestamps exactly.
- Focused verification — OK (§§2–5): 86 pass and two skip. One-time hash
  cost is 25 country and 50 EHCVM wave keys in Niger. All 50 ECVMA wave
  keys are unchanged.
