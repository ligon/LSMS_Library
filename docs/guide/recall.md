# The Recall Record and `period=`

`food_expenditures` returns a number whose reference period used to be recorded
nowhere. Most of the corpus is **7 days asked once**; GhanaLSS delivers **30
days** (six five-day asks), Burkina Faso 2014 **28** of spending (four quarterly
7-day passages) but **7** of quantity (only passage 1 fields the quantity grid),
Mali 2014-15 **14** (two EACI passages of one panel, summed), Guatemala and
Panama **15**. Pooling GhanaLSS with Ethiopia compared a 30-day figure with a
7-day one, and nothing said so. GH #851.

Two phases:

1. **The record** (Phase I, 2026-09-09) — what window each `(country, wave)`
   food cell covers, with its evidence rung. Facts only.
2. **`period=`** (Phase II, ruling of @ligon 2026-10-18) — the one conversion
   the record licenses, on `food_expenditures` and `food_quantities`.

## The record

```python
import lsms_library as ll

ll.recall()                         # every (country, wave), one row each
ll.recall('GhanaLSS')
df = ll.Country('Ethiopia').food_acquired()
df.attrs['recall']                  # {country: {wave: RecallRecord}}
```

Each record carries four facts — kept separate because each is needed on its
own:

| field | meaning |
|---|---|
| `recall_days` | length of ONE ask |
| `n_asks` | how many times the module is asked — **asks, not interviewer visits** (GLSS7 fields seven visits and asks food at six) |
| `n_asks_by_column` | optional `{column: n}` for a flow column asked a different number of times than the module — Burkina Faso 2014 is `{Quantity: 1}` against `n_asks: 4`. It may name only `Expenditure` or `Quantity` and needs the evidence the count does |
| `within_wave_variation` | the axis the window varies on *inside* the wave (`stratum`, `by-s`, …); when set, both numbers are null |
| `basis` | the evidence rung: `questionnaire` > `variable-label` > `config-comment` > `filename` > `not-recorded` |

`total_exposure_days = recall_days * n_asks` is a property, not a stored field;
`rec.exposure_days(column)` is the same with the column's `n_asks_by_column`
override, when it has one.
The records live in `countries/{C}/_/recall.yml`, generated from the evidence
base `.coder/coverage/food_recall.csv` by `scripts/promote_recall_records.py`;
edit the CSV, not the YAML. The record rides through `Feature()` (GH #873).

## `period=`

```python
eth = ll.Country('Ethiopia')
eth.food_expenditures(period='day')            # each wave / its exposure
eth.food_quantities(period='year', units='kgs')
ll.Feature('food_expenditures')(period='month')
```

**Accepted values** — a closed vocabulary, one spelling each:

| `period=` | days |
|---|---|
| `None` (default) | no conversion: the window the survey recorded |
| `'day'` | 1 |
| `'week'` | 7 |
| `'month'` | 365.25 / 12 = 30.4375 |
| `'year'` | 365.25 |

`month` and `year` use the Julian year, the convention the only earlier in-repo
conversion stated (`Togo/_/data_scheme.yml`). Anything else is a `ValueError`
naming the list.

**The conversion.** The derived tables sum `visit` away, so a row covers every
ask of its column in the wave. Each row's flow column — `Expenditure` on
`food_expenditures`, `Quantity` on `food_quantities` — is rescaled by its own
wave's record:

```
value_in_period = value * period_days / (recall_days * n_asks_for(column))
```

where `n_asks_for(column)` is the column's `n_asks_by_column` entry if it has
one, else `n_asks`. It is linear and per-`t`, so it commutes with the `labels=`
re-aggregation, `market=` and `numeraire=`; `Country` applies it first. Nothing
else in the frame changes — index, row count, other columns. The conversion is
recorded on `df.attrs['period']`:

```python
out = ll.Country('GhanaLSS').food_expenditures(waves=['2016-17'], period='year')
out.attrs['period']['GhanaLSS']
# {'period': 'year', 'period_days': 365.25, 'columns': ['Expenditure'],
#  'waves': {'2016-17': {'exposure_days': 30.0, 'factor': 12.175,
#            'recall_days': 5.0, 'n_asks': 6, 'n_asks_source': 'n_asks',
#            'basis': 'questionnaire', 'ask_count_basis': 'measured'}}}
```

`n_asks_source` is `'n_asks_by_column'` where the column's own count was used
(Burkina Faso 2014's `food_quantities`).

**Read `basis` before trusting a converted number.** `config-comment` means a
maintainer asserted the window in repo prose; it converts (the ruling counted
those cells), but it is not a questionnaire.

### Which tables accept it

| table | `period=` |
|---|---|
| `food_expenditures`, `food_quantities` | accepted |
| `food_prices` | **refused** — `TypeError`. A price is a rate per unit of the good, not a flow over time; rescaling it by a recall window changes its meaning, not its period. |
| `food_acquired` | refused — it keeps the `visit` level, so a row covers ONE ask, not the cell's exposure |
| every other table | refused — `TypeError`, as for any kwarg a table does not take |

A refusal happens before any data is read, from `Country(...)` and from
`Feature(...)` alike (the two share one message, `recall.period_refusal`).

### Where the window is unknown: Contract B

The third instance of the pattern `labels=` (`LabelUnavailableError`) and
`units=` (`UnitsUnavailableError`) already follow.

**`Country(...)` raises** `lsms_library.errors.PeriodUnavailableError` (a
`ValueError`) when the result holds ANY wave whose window is not established,
and converts nothing:

```python
from lsms_library.errors import PeriodUnavailableError
try:
    ll.Country('Nigeria').food_expenditures(period='week')
except PeriodUnavailableError as e:
    e.unavailable   # {'2018Q3': "the window varies within the wave (within_wave_variation='by-s') ...", '2019Q1': ...}
    e.convertible   # ['2010Q3', '2011Q1', '2012Q3', '2013Q1', '2015Q3', '2016Q1']
    ll.Country('Nigeria').food_expenditures(period='week', waves=e.convertible)
```

A wave is refused when its record is absent, varies within the wave, rests on
a weak rung (`not-recorded`, `filename`), or lacks `recall_days` / `n_asks`.
It raises rather than returning the known waves converted and the rest
unscaled (one column on two clocks) or filling them with `NA` (which `sum()`
reads as zero — GH #828).

**`Feature(...)` degrades — at the grain of the WAVE.** A country whose other
waves convert is **kept, restricted to them**; a country with none is dropped.
One aggregated warning names every dropped `(country, wave)` cell, and the
assembled frame carries them:

```python
df = ll.Feature('food_expenditures')(period='year')
df.attrs['period_unavailable']   # {'GhanaLSS': ['1991-92'], 'Malawi': [...], ...}
df.attrs['period']               # one conversion record per kept country
```

This is a deliberate departure from the `labels=` / `units=` precedent, which
drops whole countries. The window is a property of the `(country, wave)` cell,
not of the country: dropping by country would discard GhanaLSS's six
convertible waves for its one stratified wave — exactly the cells `period=`
exists to compare — and would keep 17 of the 32 convertible cells instead of
32.

**How a degraded country is converted: a slice of the unconverted read.**
`Feature` reads the country's table again *without* `period=` — the very frame
an unconverted `Feature` call gets — keeps the rows of the convertible waves,
and applies the same conversion to them. So for every kept row:

```
Feature(t)(period=p, **kw) row  ==  Feature(t)(**kw) row  x  its wave's factor
```

exactly — bit-for-bit, measured on Mali and Burkina Faso (`food_quantities`,
default `units='kgs'`), Uganda (`units='kgs'`, `labels='Aggregate'`,
`market='Region'`), GhanaLSS and Nigeria (`food_expenditures` with
`labels='Aggregate'`, `market='Region'`; `food_quantities` with `units='kgs'`,
`labels='Aggregate'`), and pinned by `TestDeliveredFeature`. The second read
costs time: 146 s against 104 s for the unconverted
`Feature('food_expenditures')()` on the full corpus.

*Why not re-call the country with `waves=` its convertible waves* (the first
version): `food_quantities(units='kgs')` infers kg factors by pooling over the
waves *requested* (GH #850), so a wave subset moved numbers that have nothing to
do with `period=` — Mali 2014-15 +2.90% kg (49,312 of 108,711 rows changed),
Burkina Faso 2014 +0.73%, Uganda 2019-20 44% of rows (red team r1, R3). A slice
of the unconverted read moves none. The per-wave `attrs` records (`population`,
`recall`, `derivations`) are re-attached for the slice, so they describe the
rows served; `labels_unmatched` still reports the whole read.

`Country(c).<table>(waves=e.convertible, period=p)` — the escape hatch the
error names — remains a *subset request*: under `units='kgs'` its rows can
differ from the `Feature` rows for the same waves, for the GH #850 reason
above.

## Coverage today: **32 of 67** cells convert

Measured 2026-10-19 on a warm corpus (red-team r1 fixes applied, on
`development` at `2f3104c5f`): `Feature('food_expenditures')(period='year')`
keeps 32 of 67 cells — **3,405,677 of 6,147,074 rows** (55.4%) — and every
kept cell's total is its unconverted total times exactly `365.25 / exposure`,
using each column's own exposure, with the kept cell's rows unchanged.
`Feature('food_quantities')(units='units', period='year')` keeps the same 32
cells, 3,937,370 of 7,796,201 rows, on the same check. (Over `food_acquired`,
summed per country from `Country(c).food_acquired()`, the same 32 cells are
7,489,800 of 11,927,184 rows.)

| country | waves | exposure | asks x days | basis |
|---|---|---|---|---|
| Burkina_Faso | 2014 | `Expenditure` 28, `Quantity` 7 | spending 4 x 7 (quarterly passages); quantity 1 x 7 — only passage 1 fields the quantity grid (`n_asks_by_column: {Quantity: 1}`) | config-comment |
| Ethiopia | 2011-12, 2013-14, 2015-16, 2018-19, 2021-22 | 7 | 1 x 7 | questionnaire |
| GhanaLSS | 1987-88, 1988-89 | 14 | 1 x 14 (12A; 12B is derived onto the same fortnight) | questionnaire |
| GhanaLSS | 1998-99, 2012-13 | 30 | 6 x 5 | config-comment |
| GhanaLSS | 2005-06 | 30 | 10 x 3 | questionnaire |
| GhanaLSS | 2016-17 | 30 | 6 x 5 | questionnaire |
| Guatemala | 2000 | 15 | 1 x 15 | variable-label |
| Mali | 2014-15 | 14 | 2 x 7 — the module is asked at both EACI passages of one panel and the build sums them (`ask_count_basis: measured`) | variable-label |
| Nigeria | 2010Q3, 2011Q1, 2012Q3, 2013Q1, 2015Q3, 2016Q1 | 7 | 1 x 7 | config-comment |
| Panama | 1997, 2003, 2008 | 15 | 1 x 15 | config-comment |
| Serbia | 2007 | 7 | 1 x 7 | config-comment |
| Tanzania | 2008-09, 2010-11, 2012-13, 2014-15, 2019-20, 2020-21 | 7 | 1 x 7 | config-comment |
| Togo | 2018 | 7 | 1 x 7 | questionnaire |
| Uganda | 2019-20 | 7 | 1 x 7 | questionnaire |

The 35 that do not:

| why | cells |
|---|---|
| **window varies within the wave** (3) | GhanaLSS 1991-92 — by stratum: urban 10 asks x 3 days = 30, rural 7 x 2 = 14 (GLSS3 interviewer manual §1.9); Nigeria 2018Q3, 2019Q1 — by acquisition source `s` |
| **`not-recorded`** (32) — nobody has read the questionnaire | Benin 2018-19; Burkina_Faso 2018-19, 2021-22; Cambodia 2019-20; CotedIvoire 2018-19; EthiopiaRHS 1989, 1994a, 1994b, 1995, 1997; GhanaSPS 2009-10, 2013-14, 2017-18; Guinea-Bissau 2018-19; Malawi 2004-05, 2010-11, 2013-14, 2016-17, 2019-20; Mali 2018-19, 2021-22; Niger 2018-19, 2021-22; Senegal 2018-19, 2021-22; Uganda 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19 |

Nepal and Peru declare `food_acquired` but have no `recall.yml` (and no built
food data on most installs); a wave with no record is refused like a
`not-recorded` one.

**Why 32 and not the 28 in the issue.** 28 was counted when Phase I closed
(`634db2697`, 2026-09-09). Since then GhanaLSS 2005-06 lost its `by-s` flag once
`s8hq3` was found to be the 2nd-visit own-production quantity, so both sources
run visits 2–11 (`ddfe2726c`, +1); GLSS1/2 recorded 12A's fortnight and served
12B derived onto it (`00f0a5fbe`, +2); Uganda 2019-20 was promoted to
questionnaire grade (`04e716295`, +1). `tests/test_recall_period.py` fails if
this page's bold count drifts from the live records again.

The not-recorded half is the work queue: reading one questionnaire per series
and recording it in `food_recall.csv` converts its cells with no code change
(`.coder/HANDOFF_recall_research.md` ranks the series by leverage) — once the
cell's *delivery* is also classified (next section).

## The exposure is what the sources deliver, and a test checks it

A record that miscounts the asks feeding a column serves a wrong number with
full confidence. Red team r1 (2026-10-19) found two such cells among the 32,
both certified by the first cut of `period=`:

- **Burkina Faso 2014 `food_quantities`, 4x too low.** One per-wave exposure
  (4 x 7 = 28) divided a quantity that only passage 1 asks
  (`Burkina_Faso/_/CONTENTS.org`). `food_quantities(units='units',
  period='week')` was 1,052,184.4; it is now 4,208,737.6, the passage-1 sum.
  Fixed by `n_asks_by_column: {Quantity: 1}`; `food_expenditures` is unchanged
  at 28 days.
- **Mali 2014-15, 2x too high on both tables.** The record said one ask; the
  build reads `EACIALI_p1` and `EACIALI_p2` — the same 3,804 households — and
  sums them (`Mali/_/CONTENTS.org`). `food_expenditures(period='week')` was
  5,652,532,205; it is now 2,826,266,102.5.

`tests/test_recall_period.py::TestDeliveredExposure*` now checks every
convertible cell, by delivery class:

| class | cells | check |
|---|---|---|
| `visit-level` — the country declares `visit` in `food_acquired`'s index | Burkina_Faso 2014; GhanaLSS 1987-88, 1988-89, 1998-99, 2005-06, 2012-13, 2016-17 | **generic**: per flow column, the visits carrying a value must equal `n_asks_for(column)` |
| `declared-visit` — a YAML wave naming a `visit` idxvar or several source files, its `visit` dropped by a hook | Mali 2014-15 (2 files), Togo 2018 (`vague`, a sample split) | **generic for the YAML path**: the most distinct visits any household has in the declared sources; with no data, several files must mean that many asks |
| `one-ask-by-inspection` — a script-path wave with no `visit` level | the other 23 | **table-driven**: listed by hand with the source the script reads, pinned to `n_asks == 1` |

A convertible cell in no class fails the suite, so a record promoted from
`not-recorded` cannot convert until someone has said how its asks are
delivered. Against the r1 records the check fails on exactly R1 and R2.

## What `period=` does not do

- **It does not use the survey's own dates.** It divides by the *declared*
  exposure, one constant per wave. Dividing by the *realised* interval is
  refuted on this corpus — on GLSS4, doubling the gap between visits moves
  reported spend +1.2% where proportionality predicts +100% — so it would
  inject fieldwork tempo rather than normalise (`lsms_library/recall.py`
  module docstring; Scott and Amenuvegbe 1990).
- **It does not equalise how the window was elicited.** A single-ask 7-day
  survey is entirely first-ask; GhanaLSS's 30 days spread one first-ask premium
  (measured 1.84x unaided, 1.17x diary-assisted, at identical interval lengths)
  over six asks. Dividing by days does not remove that difference, nor recall
  decay, list length, or diary-versus-recall. The arithmetic is correct *given*
  the total; whether the total estimates consumption is the same measurement
  question `food_expenditures` already carries unconverted (charter §6,
  `.coder/charter-recall-windows.md`).
- **It does not adjust for a household that missed an ask.** `n_asks` is the
  schedule's count (`ask_count_basis` says whether it was measured -- from the
  delivered `visit` level, or for Mali 2014-15 from the households the two
  passage files share -- or declared); a household interviewed at fewer visits
  is divided by the full exposure.
- **It does not convert a window that varies within the wave.** GLSS3's
  stratum-varying window needs a per-row carrier and is out of Phase II's
  scope; it is filed as its own issue.
- **No pooling warning.** A `RecallHeterogeneityWarning` modelled on
  `PopulationHeterogeneityWarning` was sketched in the issue body but is not in
  the ruling, and a pooling-time warning for the recall record was declined on
  2026-09-11 (`SkunkWorks/derived_values.org`): `attrs['recall']` is the signal.

## Cache cost

`period=` itself: none. The derived food tables are computed at read time and
never written to a parquet, and nothing added here is reachable from a build
fingerprint. Measured by stash-probe on 10 countries: **0 of 292** hashes moved
(12 `build_transforms_fingerprint`, 205 `Country._table_cache_hash`, 75
`Wave._input_hash`); a positive control (a no-op line in the
`@build_transform`-tagged `df_data_grabber`) moved all 292. `period=None`
returns byte-identical frames. The r1 fixes add one cost that is not
`period=`'s: correcting the false docstring in `Mali/2014-15/_/mapping.py`
moves every Mali table's hash (one Mali re-warm; no served number changes).
