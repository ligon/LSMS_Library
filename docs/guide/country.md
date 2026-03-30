# Working with a Country

The [`Country`][lsms_library.Country] class is the primary entry point. It
provides access to all survey waves, standardized tables, and panel data for a
single country.

## Basic Usage

```python
import lsms_library as ll

uga = ll.Country('Uganda')
```

### Discovery

```python
# Available survey waves
uga.waves
# ['2005-06', '2009-10', '2010-11', '2011-12', '2013-14', '2015-16', '2018-19', '2019-20']

# Available standardized tables
uga.data_scheme
# ['people_last7days', 'cluster_features', 'shocks', 'food_acquired', ...]
```

### Loading Tables

Every entry in `data_scheme` is callable as a method:

```python
food_exp = uga.food_expenditures()
roster   = uga.household_roster()
shocks   = uga.shocks()
```

Each returns a pandas DataFrame with a meaningful MultiIndex.

### Filtering by Wave

Pass `waves=` to load a subset of survey rounds:

```python
recent = uga.food_expenditures(waves=['2018-19', '2019-20'])
```

### Adding a Market Index

For demand-system estimation, add a region-level market identifier:

```python
food = uga.food_expenditures(market='Region')
# Index now includes level `m` derived from cluster_features
```

## Accessing a Single Wave

Use bracket notation to get a [`Wave`][lsms_library.Wave] object:

```python
wave = uga['2019-20']
wave.data_scheme   # tables available for this wave
df = wave.household_roster()
```

## Harmonization Pipeline

When you call a table method, the library applies these transformations
transparently before returning the DataFrame:

1. **Kinship expansion** -- if the wave produces a `Relationship` column, it is
   decomposed into `Generation`, `Distance`, and `Affinity` using the mapping
   in `lsms_library/categorical_mapping/kinship.yml`.
2. **Categorical mappings** -- column or index names matching a table in the
   country's `categorical_mapping.org` are mapped automatically.
3. **Canonical spellings** -- variant spellings (e.g. `Male` -> `M`,
   `Féminin` -> `F`) are normalized using the rules in `data_info.yml`.
4. **Dtype enforcement** -- columns are cast to declared types from
   `data_scheme.yml`.

## Derived Tables

Some tables are computed automatically from others:

| Derived Table                | Source Table       |
|------------------------------|--------------------|
| `food_expenditures`          | `food_acquired`    |
| `food_prices`                | `food_acquired`    |
| `food_quantities`            | `food_acquired`    |
| `household_characteristics`  | `household_roster` |

You call them the same way -- the derivation is transparent.

## Supported Countries

Countries are organized under `lsms_library/countries/`. To see what's
available:

```python
from pathlib import Path
import lsms_library

countries_dir = Path(lsms_library.__file__).parent / 'countries'
[d.name for d in sorted(countries_dir.iterdir())
 if d.is_dir() and not d.name.startswith('.')]
```
