# Getting Started

## Installation

```bash
pip install LSMS_Library
```

## Data Access

The library ships configuration and harmonization code, not survey data. The
underlying microdata comes from the
[World Bank Microdata Library](https://microdata.worldbank.org/) under their
terms of use, and the library downloads it for you on first read -- but only
once you have given it a **World Bank Microdata API key**.

Without a key, `Country('Uganda')` and `uga.waves` still work; the first call
that needs real data does not. Set the key before working through the Quick
Start below.

### 1. Get a key

Register at [microdata.worldbank.org](https://microdata.worldbank.org/) and
generate an API key from your account page. Registration is free; the key is
what records your acceptance of the World Bank's terms of use.

### 2. Tell the library about it

Either write it to the config file:

```yaml
# ~/.config/lsms_library/config.yml
microdata_api_key: your_key_here
```

or set an environment variable:

```bash
export MICRODATA_API_KEY=your_key_here
```

The lookup order for every setting is **environment variable -> config file ->
unset**, so the variable wins where both are present. The config directory
follows `XDG_CONFIG_HOME` and can be relocated wholesale with `LSMS_CONFIG_DIR`
(useful for a synced or git-crypted dotfiles repo).

### 3. Check what you can reach

```python
from lsms_library.data_access import permissions

permissions()
# {'wb_api': 'read', 'ligonresearch_s3': 'read'}
```

Resources you cannot reach are simply absent from the dict rather than reported
as an error, so this is the quickest way to tell a credential problem from a
data problem. `can_read('wb_api')` answers the same question as a boolean.

### What the key unlocks

| You have | You get |
|---|---|
| Nothing | The library imports and warns; anything needing real data fails |
| A valid WB Microdata API key | Direct World Bank downloads, **and** the S3 read cache, which is considerably faster |
| That key plus S3 write credentials | The above, plus push access for materializing new waves |

The API key is the only real gate. The S3 bucket is a read cache in front of the
same World Bank downloads, and a valid key is what unlocks it.

!!! note "Changed in v0.10.1"

    A valid API key is now **sufficient on its own** to reach the S3 read cache.
    Before v0.10.1 the unlock also required the `gpg` binary, which is not a
    Python package and ships by default on neither Windows nor macOS -- so a
    correctly-configured key could still fail with a `NoCredentialsError` from
    three layers down. If you installed `gpg` solely to make this library work,
    you no longer need it. See the [v0.10.1 release notes](releases/v0.10.1.md).

### If the S3 unlock fails

The library warns and keeps going, naming what it tried:

```
LSMS_Library: your World Bank API key validated, but unlocking the S3 read cache failed.
  Falling back to direct World Bank downloads -- slower, but functional; nothing is blocked.
```

This is a degraded mode, not a failure: downloads still come from the World
Bank. If the message mentions a missing `cryptography` package, `pip install
cryptography` restores the fast path.

To suppress credential handling entirely -- for CI, or for offline work against
an already-populated cache -- set `LSMS_SKIP_AUTH=1`.

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

The returned DataFrame uses a MultiIndex. For the call above the levels are
`(i, t, v, j, s)` -- household, wave, cluster, item, and acquisition source:

```python
food_exp.index.names
# FrozenList(['i', 't', 'v', 'j', 's'])
```

Levels vary by table, so check `.index.names` rather than assuming. In
particular `m` (market/region) is **not** present by default -- it is added only
when you ask for it, with `uga.food_expenditures(market='Region')`.

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
