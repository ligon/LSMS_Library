# Prior-Art Ledger - one eligible roster member

**Search tier used:** ripgrep, Git history, GitHub issue/PR search. Inherits [STANDING.md](STANDING.md).

## §1 Task, restated

`Country.household_characteristics()` and `Feature("household_characteristics")` are derived from household roster. A request with one eligible row, including after the residence filter, must return its household count and log size instead of throwing in indicator construction. Confirm with invented Niger and Uganda roster fixtures. No country config or sample build is needed.

## §2 Existing machinery

| Symbol | Source | Decision |
|---|---|---|
| `Country._ROSTER_DERIVED` | `lsms_library/country.py` | Reuse the existing public derive route. |
| `roster_to_characteristics` | `lsms_library/transformations.py` | Leave filtering, household key, bins and grouping unchanged. |
| `dummies` | `lsms_library/transformations.py` | Keep one element per input row. The scalar created by `Series.squeeze` is the defect. |
| `test_roster_residence_filter.py`, `test_roster_to_characteristics_movers.py` | `tests/` | Reuse existing compatibility checks for large rosters, movement and conditioning. |

## §3 Definitions in force

The derived table and household grain are specified in STANDING.md §3, CLAUDE.md "Derived Tables", and `lsms_library/data_info.yml`. The age-bin semantics and resident filter are existing `roster_to_characteristics` behavior. No metadata semantics change here.

## §4 Invariants

Retain the previous indicator order, integer counts, household index and log-size formula. Preserve all existing residence, age, missingness and mover interpretations. STANDING.md §4 applies. No native read, cache rebuild, new derived producer, or Country scheme changes.

## §5 Reuse decision

Remove the one-row scalar conversion inside the existing indicator helper. No new estimator or grouping.

## §6 Open questions

None for this defect. Sampling attrs and diagnostic attribution remain separately tracked in issue 1136.

### Phase 3 - verification

- `dummies` - OK (§2, §4, §5). Seven source-free tests fail on the pinned base and pass on the candidate (38 focused passes overall), spanning the transform and public Country/Feature routes with invented Niger and Uganda rosters. An independent 21-case old/new oracle finds 12 exactly unchanged controls and nine repaired singleton cases. All 202 country and 1,166 wave L2 fingerprints across the eight ISA countries are identical.
