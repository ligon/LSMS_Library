# Panel Data Analysis

For countries with panel surveys, household IDs are automatically harmonized
across waves, enabling longitudinal analysis without manual matching.

## Tracking Households Over Time

```python
import lsms_library as ll

uga = ll.Country('Uganda')

# Get food expenditures across all waves
food_exp = uga.food_expenditures()

# The MultiIndex includes time (t), so you can track households
household_over_time = food_exp.xs('00c9353d8ebe42faabf5919b81d7fae7', level='i')

# Compare specific waves
wave_2015 = food_exp.xs('2015-16', level='t')
wave_2019 = food_exp.xs('2019-20', level='t')
```

## Panel Attrition

Check how many households appear across wave pairs:

```python
from lsms_library import tools

panel = tools.panel_attrition(uga.household_characteristics(), uga.waves)
# Returns a matrix:
#         2005-06 2009-10 2010-11 ...
# 2005-06    3122    2606    2386
# 2009-10     NaN    2974    2617
# Diagonal = total households per wave
# Off-diagonal = overlap between waves
```

## Panel IDs

The `panel_ids` and `updated_ids` properties provide the raw ID mappings:

```python
# Computed lazily on first access
uga.panel_ids      # dict mapping waves to ID tables
uga.updated_ids    # dict of {old_id: new_id} per wave
```

To eagerly preload panel IDs at construction time:

```python
uga = ll.Country('Uganda', preload_panel_ids=True)
```

## How ID Harmonization Works

Different surveys handle panel IDs differently:

- **Stable IDs** -- the same household keeps the same ID across waves
- **Backward links** -- each wave provides a mapping to the previous wave's ID
- **Composite IDs** -- IDs are constructed from multiple survey fields

The library's `updated_ids` mechanism walks the ID chain so that a single
canonical ID refers to the same household across all waves. This happens
transparently when you call any table method.
