# LSMS Library

A Python library providing a uniform interface to Living Standards Measurement
Study (LSMS) household surveys from multiple countries and years, without the
data loss typical of traditional harmonization approaches.

## The Problem

LSMS datasets are invaluable for studying poverty, consumption, and household
welfare across developing countries. However, each country's survey uses
different variable names, food classification systems, questionnaire structures,
and file formats.

Researchers typically spend weeks learning each new dataset's idiosyncrasies or
use pre-harmonized datasets that sacrifice detail and comparability.

## The Solution

LSMS Library provides an **abstraction layer** that gives you a consistent
interface to work with any supported LSMS dataset. Instead of harmonizing the
data itself (which loses information), we harmonize the *way you access* the
data:

```python
import lsms_library as ll

uga = ll.Country('Uganda')
uga.waves          # ['2005-06', '2009-10', ..., '2019-20']
uga.data_scheme    # ['food_acquired', 'household_roster', ...]

food = uga.food_expenditures()   # Standardized DataFrame, all waves
```

## Cross-Country Analysis

The [`Feature`](guide/feature.md) class makes it easy to assemble a single
harmonized DataFrame across every country that provides a given table:

```python
roster = ll.Feature('household_roster')
roster.countries   # ['Burkina_Faso', 'Ethiopia', 'Mali', 'Uganda', ...]
df = roster()      # Load all countries into one DataFrame
```

## Key Features

- **Uniform Interface** -- consistent names across countries (e.g.
  `food_expenditures()`, `household_characteristics()`)
- **Multi-Wave Panel Support** -- household IDs harmonized across waves
  automatically
- **Zero Data Loss** -- original survey detail preserved
- **Cross-Country Analysis** -- `Feature` class concatenates harmonized data
  across countries
- **DVC Integration** -- stream data from remote storage
- **Parquet Cache** -- materialize once, reuse within a session; cross-session read path and content-hash invalidation land in v0.7.0 and v0.8.0 (see the [Caching guide](guide/caching.md))
- **Extensible** -- add new surveys via YAML configuration files

## Next Steps

- [Getting Started](getting-started.md) -- installation and first steps
- [Country Guide](guide/country.md) -- single-country workflows
- [Feature Guide](guide/feature.md) -- cross-country analysis
- [API Reference](api/country.md) -- complete class documentation
