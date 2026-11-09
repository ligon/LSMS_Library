# Recall windows and `period=`

`food_expenditures()` and `food_quantities()` return the survey's recorded
window by default. Passing `period='day'`, `'week'`, `'month'` or `'year'`
rescales the flow using its declared wave exposure:

```
value_in_period = recorded_value * target_days / exposure_days
exposure_days = recall_days * n_asks_for_this_column
```

A month is 365.25/12 days and a year is 365.25 days. No conversion is applied
when `period=None`. This is a change of reference period, not an estimate of
unobserved acquisitions, a correction for missing interviews or a claim that
the window represents the whole year. Prices are not flows: `food_prices`,
`food_acquired` and unrelated tables refuse non-None `period=` explicitly.

```python
import lsms_library as ll

record = ll.recall('Mali')
daily = ll.Country('Mali').food_expenditures(period='day')
```

Inspect the recall record before choosing a period. The packaged records
currently establish a single declared exposure for **55 of 67** food cells.
The remaining cells have no established window or vary within the wave.
Questionnaire, variable-label and config-comment rungs remain distinct; a
config-comment assertion converts but stays visibly weaker than a quoted
questionnaire. Filename guesses and not-recorded cells do not convert.

`ll.recall()` includes `n_asks_by_column` where a column differs from the
module's count. Burkina Faso 2014 expenditure covers four seven-day asks
(28 days), while quantity covers only the first ask (7 days). Mali 2014-15
covers two seven-day asks (14 days). Its existing public series remains
collapsed: this metadata correction adds no visit axis and changes no default
quantity, expenditure, identifier or missing-value treatment.

## Unavailable waves

`Country` raises `lsms_library.errors.PeriodUnavailableError` if a requested
conversion cannot be established. The error names the country, table, target,
unavailable waves and reasons, and convertible waves. No known wave is returned
scaled alongside an unknown wave left on its original clock. A metadata-safe
refusal precedes the data read; uncertain routes are checked against represented
wave values after reading. Existing metadata-proven-absent subsets stay empty.

`Feature('food_expenditures')(..., period='day')` follows Contract B: keep
convertible wave rows, drop unavailable cells, emit one aggregate warning and
record their names in `attrs['period_unavailable']`. If none survives, the
existing canonical empty index remains available. A degraded country is read
without conversion over the same requested wave universe, then sliced and
converted. It is never re-read with a smaller wave selection to recompute
kilogram factors. Whole-read kilogram diagnostics retain that inference scope;
population, recall, sampling and derivation records describe the served slice.

## What the conversion record means

`attrs['period'][country]` records the target, days, column and each wave's
exposure, factor, ask-count source and evidence rung. The exposure is one
declared constant per wave. If a served food table carries visits, the record
can disclose a food-row shortfall proxy. A zero-acquisition visit has no food
rows, so this is an upper bound on missing asks, not household cover status.
It never changes the denominator or household weights. The historical Burkina
food-row 9.3% / -3.94% result has that qualification; it is not a fresh measure
or a household missing-interview correction.

For collapsed Mali the record says `shortfall_source='no-visit-level'` and
`shortfall=None`. Where no visit frame is read, including a degraded Feature
slice, it says `not-measured`. No household ask count is invented. A future
visit carrier or household cover-status correction requires its own design.

The conversion uses declared exposure, not realized time between interviews.
It does not equalize recall decay, first-ask effects, diary design, item lists
or other survey measurement differences. Harmonizing the period does not make
the underlying surveys interchangeable.

The evidence source is `.coder/coverage/food_recall.csv`; regenerate packaged
records with `scripts/promote_recall_records.py`. Preserve evidence citations
and qualifications when updating it. The library reads the packaged YAML,
including from an installed wheel; it does not depend on the checkout CSV.
