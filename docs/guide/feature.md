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
# ['Albania', 'Armenia', 'Azerbaijan', 'Benin', 'Burkina_Faso', 'Cambodia', ...]

# What are the guaranteed columns?
roster.columns
# ['Sex', 'Age', 'Relationship', 'Generation', 'Distance', 'Affinity']
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

`transformations.seed_kg` and `transformations.nitrogen_kg` retain this
country identity when reducing pooled `plot_inputs` to plots. Their output
index is `(country, t, i, plot_id)` whenever the input carries a `country`
index level or column, including one selected country and empty results.
Country labels must be non-null. Ordinary country-less inputs return
`(t, i, plot_id)`; `attrs['country']` alone does not add an index level.
`transformations.fertilizer_rate` also retains country in its numerator,
area grouping, join and output when both `plot_inputs` and `plot_features`
carry it. Its output is `(country, t, i, plot_id)` for `on='plot'` and
`(country, t, i, parcel)` for the existing default `on='parcel'`, including
one labeled country and empty results. Country must be non-null on both
inputs; labels on only one input are refused, with no inference or
broadcasting. An index level takes precedence over a redundant country
column. Area keys may still be columns. Country-less calls keep their
existing contract, and attrs alone do not add country.
The crop reducers `harvest_kg`, `gross_crop_revenue` and `crop_diversity`
likewise prepend supplied country identity, including singleton and empty
results. Crop conversion estimates are computed separately within each
country; identical unit labels and household keys do not establish a shared
conversion factor. `yield_kg` requires non-null country on both crop and area
operands or neither, retains country through the land join, and selects each
row's existing country-specific land route. Labels outrank scalar attrs;
country-less inputs keep their existing behavior. This preserves each
instrument's land keys and definitions, without making crop occurrence
entropy or differently recorded harvest conditions comparable across countries.
Other transforms have their own documented grouping and joining contracts.

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

All per-country harmonization (categorical mappings, kinship decomposition,
canonical spellings, dtype coercion) is applied by each `Country` before
concatenation. `Feature` delegates to each country's existing table method.

### `strata` Is Not One Vocabulary

`Feature('sample')` pools a column that means different things in different
countries, and sometimes in different waves of one country. A region, a region
crossed with urban and rural, a survey's own design code and a district are all
served as `strata`, because that is what each survey's design or files offer:

| `strata` holds | Where |
|---|---|
| Region x milieu, as a label | EHCVM waves of Senegal, Mali, Niger, Burkina Faso; Ethiopia |
| Region alone | Benin, Togo, Guinea-Bissau, Cote d'Ivoire 2018-19; GhanaLSS 1998-99 onward |
| The file's own stratum, a label | Niger 2011-12 and 2014-15 (region x agro-ecological zone) |
| The file's own stratum, a code | Mali 2017-18, Burkina Faso 2014 |
| Nothing | Mali 2014-15 |

This table covers the cases measured for GH #812, not every country. So
`strata` identifies a stratum within one wave of one country. Do not compare or
group on it across countries, and check before doing so across waves. Spelling
differs too: the EHCVM labels end in `Urbain` or `Rural`, as the surveys write
them, and Ethiopia's in `Urban` or `Rural`. Where the
label is a construction it is registered: `df.attrs['derivations']` names those
countries, and `Country(c).derivations()` gives the rule and its evidence. Each
country's `_/CONTENTS.org` records what its `strata` holds.

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

Most tables declared in a country's `data_scheme.yml` can be used with
`Feature`. Common ones include:

| Table                        | Description                         |
|------------------------------|-------------------------------------|
| `household_roster`           | Demographics, kinship decomposition |
| `cluster_features`           | Region, rural/urban classification  |
| `food_acquired`              | Food acquisition with units         |
| `food_expenditures`          | Derived food spending               |
| `shocks`                     | Household shocks and coping         |
| `individual_education`       | Educational attainment              |

!!! note "Country-level-only tables are not `Feature` tables"

    `panel_ids` and `updated_ids` appear in `data_scheme` but have no
    cross-country assembly: `Feature('panel_ids')(['Malawi'])` returns an empty
    DataFrame rather than raising. They are graded `n/a` in the coverage matrix
    and are reached per country, as `Country(...).panel_ids`.
