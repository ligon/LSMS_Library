# Cross-Country Analysis with Feature

The [`Feature`][lsms_library.Feature] class answers "what do we know about a
given table across the developing world?" It assembles a single harmonized
DataFrame for a table across all countries that provide it.

## Basic Usage

```python
import lsms_library as ll

roster = ll.Feature('household_roster')

# Which countries provide this table?
roster.countries
# ['Burkina_Faso', 'China', 'Ethiopia', 'GhanaLSS', 'India', 'Mali', ...]

# What are the guaranteed columns?
roster.columns
# ['Sex', 'Age', 'Generation', 'Distance', 'Affinity']
```

## Loading Data

`Feature` is callable. Invoke it to load and concatenate data:

```python
# Load all available countries
df = roster()

# Load specific countries
df = roster(['Mali', 'Uganda'])
```

The returned DataFrame has a `country` index level prepended:

```
                                    Sex  Age  Generation  Distance       Affinity
country  t        i     pid
Mali     2014-15  1003  1           M    45   0           0         consanguineal
                        2           F    38   0           0         affinal
Uganda   2019-20  4001  1           M    52   0           0         consanguineal
```

## Design Decisions

### Lazy Loading

Construction is cheap -- `Feature('household_roster')` discovers which
countries declare the table but loads nothing from disk. Data is fetched on
demand when you call the instance.

### Union of Columns

The returned DataFrame takes the **union** of all columns across countries, not
the intersection. Required columns from `data_info.yml` guarantee a common
core; country-specific extras appear as `NA` where absent.

This makes gaps visible (e.g. `District` present for Ethiopia but `NA` for
Mali) rather than silently dropping information.

### Harmonization Flows Through

All per-country harmonization (kinship decomposition, canonical spellings,
categorical mappings, dtype coercion) is applied by each `Country` before
concatenation. `Feature` delegates to each country's existing table method.

### Error Handling

If a country's data fails to load, `Feature` emits a warning and continues
with the countries that succeeded:

```python
import warnings
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    df = roster()
    # Inspect w for any per-country failures
```

## Available Tables

Any table declared in a country's `data_scheme.yml` can be used with `Feature`.
Common ones include:

| Table                        | Description                         |
|------------------------------|-------------------------------------|
| `household_roster`           | Demographics, kinship decomposition |
| `cluster_features`           | Region, rural/urban classification  |
| `food_acquired`              | Food acquisition with units         |
| `food_expenditures`          | Derived food spending               |
| `shocks`                     | Household shocks and coping         |
| `individual_education`       | Educational attainment              |
| `panel_ids`                  | Cross-wave household ID linkage     |
