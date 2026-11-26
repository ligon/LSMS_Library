# Prior-art ledger — Malawi legacy interview dates (#1045)

**Search tier:** ripgrep and git, exact held source projections and producer
DDIs/forms. See [STANDING.md](STANDING.md).

## §1 Task, restated

Move the five existing date producers to the shared calendar parser, keeping
their files, component identities, interview-date fields and visit ordinals.
Do not add the modern 2024–25 table while separate PR #1774 is in review.

## §2 Existing machinery

| Machinery | Authority | Use |
|---|---|---|
| Gregorian component, string and typed-date parsing | `lsms_library/datetime_parse.py` | Reuse explicit formats and the registered partial-day key |
| Date dictionary spec and file-override guard | `Wave.column_mapping`, `Wave.grab_data` | Declarative on single-file typed IHS3; do not remove the guard |
| Existing ordinal melt and absence semantics | `malawi.interview_date`, `local_tools.melt_visit_intervals` | Reuse the melt; carry per-date provenance by source row where present |
| First dated visit for monetary conversion | `conversion._interview_dates` | Preserve the visit-selection rule, not wrong day/month values |

## §3 Definitions & conventions in force

[2004–05 IHS2 A14, p. 2](https://microdata.worldbank.org/catalog/2307/download/35030)
and its [cover dictionary](https://microdata.worldbank.org/catalog/2307/data-dictionary/F1)
bind numeric day/month/year to a14a/b/c. Legacy parsing built an ambiguous
string and swapped the recorded day and month on 3,052 of 11,280 dates.
The exact inputs contain no blank components or invalid dates.

[2010–11 IHS3 A23, p. 3](https://microdata.worldbank.org/catalog/1003/download/40797)
and the [DDI](https://microdata.worldbank.org/metadata/export/1003/ddi) name
the two interview dates. The held Stata displays both as `%dD_m_Y` and the
existing decoder returns date-typed midnight, not epoch numbers for re-decoding.

[2013–14 IHPS A23/A37, p. 3](https://microdata.worldbank.org/catalog/2248/download/41257)
and the [cover dictionary](https://microdata.worldbank.org/catalog/2248/data-dictionary/F75)
distinguish attempt 1 of the two visits from other attempts. Preserve the
original triplets and decoded English month labels. Three visit-1 rows
have valid year/month and a system-blank day. These receive first of month
and the existing registered key. The other missing/impossible dates stay NA.

Both later pooled waves use ISO date strings on their own original files,
with different field and household-ID names by file, and visit 2 only on
panel. The [v06 panel dictionaries](https://catalog.ihsn.org/catalog/8702/data-dictionary/F397)
([2019](https://catalog.ihsn.org/catalog/8702/data-dictionary/F398)) confirm the
two visit variables are not the separate consumption date. Only 2016–17
panel visit 2 carries the exact `##N/A##` marker (five occurrences).

## §4 Invariants & assumptions

Keep all current (i, t, visit) keys and other dates exact. First-of-month
placeholders gain three keys and never mark another visit on the same household.
The two pooled tables retain their per-file ID routing and first-visit shapes.
An explicit panel-only source flag records the source of a visit-2 slot, even with a missing ID;
no cross-section slots are inferred or counted as missing there. No new clocks,
fieldwork windows, nominal dates, population/weight rule, or CAPI interpretation.

## §5 Reuse decision

Use `parse_datetime` for the four wave-specific hooks and the existing
`melt_visit_intervals` for the actual shape. Scope provenance by the source
row and visit, not household alone. No shared parser or melted-helper edit.
Use a declarative typed date spec for IHS3. The common country-local helper
is hashed by the existing build-input census, even for unused wave members.

## §6 Open questions

The separate 2024–25 producer #1774 and shared raw derivation-input contract
remain distinct. Neither is inferred or altered here.

### Phase 3 — verification

- Exact source audit — OK (§§3–4). Seven predecessor covers against the old
  field routing, with 3,052 transpositions and three newly served visit dates.
- Public cold/warm comparisons — OK (§4). Independent exact DVC Country,
  DVC Feature and explicit Make Country outputs preserve all 69,484 previous
  keys, values outside the verified transpositions, and all 71,954 sample
  rows and attributes. Exact dates and padded provenance match a separate
  native-key projection. The Country and Wave discovery sets are identical.
- Date reports/consumers — OK (§§2–5). Nine reports replay; 2013–14 and
  2019–20 subsets emit two each. No warm native decodes. All three
  recovered first visits replace a later visit in the conversion lookup,
  not a missing lookup entry. Across the 3,055 changed lookup dates, all six
  shipped currency/price targets select the same factors in an equal-amount
  control. No policy change to the converter.
- Cache cost — OK (§5). The six wave sets and country set each move 26
  existing Malawi hashes because the shared country-local helper is itself
  a build input. Shared table transform fingerprints are identical.
