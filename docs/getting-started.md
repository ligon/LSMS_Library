# Getting Started

## Installation

```bash
pip install LSMS_Library
```

## Data Access

The library provides code for working with LSMS survey data. The underlying
microdata must be obtained directly from the
[World Bank Microdata Library](https://microdata.worldbank.org/) under their
terms of use.

## Quick Start

```python
import lsms_library as ll

# Load a country
uga = ll.Country('Uganda')

# See available survey waves
uga.waves
# ['2005-06', '2009-10', '2010-11', '2011-12', '2013-14', '2015-16', '2018-19', '2019-20']

# See available standardized data types
uga.data_scheme
# ['people_last7days', 'food_acquired', 'food_expenditures', ...]

# Access standardized food expenditure data across all waves
food_exp = uga.food_expenditures()
```

The returned DataFrame uses a MultiIndex with levels like `(i, t, m, j)` for
household, time, market/region, and item.

## Exploring Available Data

Every country exposes the same discovery pattern:

```python
# What tables are available?
uga.data_scheme
# ['cluster_features', 'household_roster', 'food_acquired', 'shocks', ...]

# What waves are covered?
uga.waves
# ['2005-06', '2009-10', '2010-11', ...]

# Access any table by name
roster = uga.household_roster()
shocks = uga.shocks()
earnings = uga.earnings()
```

## Loading a Single Wave

You can also drill into a specific wave:

```python
wave = uga['2019-20']
roster = wave.household_roster()
```

## What's Next

- [Country guide](guide/country.md) -- deeper look at single-country workflows
- [Feature guide](guide/feature.md) -- cross-country analysis
- [Caching](guide/caching.md) -- performance tuning
- [Panel data](guide/panel-data.md) -- longitudinal analysis
