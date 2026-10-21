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

Almost every entry in `data_scheme` is callable as a method:

```python
food_exp = uga.food_expenditures()
roster   = uga.household_roster()
shocks   = uga.shocks()
```

!!! warning "Two entries are properties, not methods"

    `panel_ids` and `updated_ids` appear in `data_scheme` but are
    `@property` attributes returning `dict`, not callables. `c.panel_ids()`
    raises `TypeError: 'dict' object is not callable` -- use `c.panel_ids`.

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

1. **Categorical mappings** -- column or index names matching a table in the
   country's `categorical_mapping.org` are mapped automatically.  A table only
   maps when it has a `Preferred Label` column, and it is keyed on the *first*
   column that is neither `Preferred Label` nor a requested label variant --
   so write it `| Original Label | Preferred Label |` (or
   `| Code | Preferred Label |`), raw label or code **first**.  A
   `| Code | Label |` table with no `Preferred Label` is documentation, not a
   mapping, and changes nothing.
2. **Kinship expansion** -- if the wave produces a `Relationship` column, it is
   decomposed into `Generation`, `Distance`, and `Affinity` using the mapping
   in `lsms_library/categorical_mapping/kinship.yml`.  Because step 1 runs
   first, a country `#+name: Relationship` table with `Preferred Label`
   canonicalises the raw survey wording *before* the kinship lookup sees it
   (GH #797; the order was the reverse until then), so one canonical entry in
   `kinship.yml` -- `Spouse` -- can serve `Spouse (Wife/Husband)`,
   `Wife/husband`, and any other wording a wave uses.  The mapping **replaces**
   the raw label in the returned `Relationship` column; the cached parquet
   keeps the raw string, because mappings are a read-path transform (see
   [Caching](caching.md)).
   One consequence of the order: `household_roster(labels={'Relationship': 'X'})`
   feeds the *selected* label variant into the kinship lookup, not the raw
   label -- no country curates a multi-label `Relationship` table today, so
   this has no live instance.
3. **Canonical spellings** -- variant spellings (e.g. `Male` -> `M`,
   `Féminin` -> `F`) are normalized using the rules in `data_info.yml`.
4. **Dtype enforcement** -- columns are cast to declared types. A country's
   `data_scheme.yml` is applied first, then the canonical
   `lsms_library/data_info.yml`, which **wins** on conflict (e.g. `Age: float`
   in Albania's `data_scheme.yml` is overridden to `Int64`).

## Derived Tables

Some tables are computed automatically from others:

| Derived Table                | Source Table       |
|------------------------------|--------------------|
| `food_expenditures`          | `food_acquired`    |
| `food_prices`                | `food_acquired`    |
| `food_quantities`            | `food_acquired`    |
| `household_characteristics`  | `household_roster` |

You call them the same way -- the derivation is transparent.

These are derived **tables**, computed at read time from a source table. That
is a different thing from a derived **value**: a number inside a table that the
library constructed from raw survey answers rather than reading off one. Those
are labelled on the row, in a `Derivation` column carrying the key of the
construction -- see the [Derived Values guide](derivations.md). A derived table
does not relabel the rows it inherits; it carries only the
`df.attrs['derivations']` summary, pointing at the table where the derivation
was made.

## Supported Countries

Countries are organized under `lsms_library/countries/` (or the tree named by
`LSMS_COUNTRIES_ROOT`). To see what's available:

```python
ll.countries()                          # every registered country
ll.countries(feature='food_acquired')   # those that declare a table
```

Listing the directories yourself is **not** the same thing: the tree also
holds staged microdata that nobody has wired up yet (`Bulgaria`, `Nicaragua`,
...), which `ll.countries()` omits and `ll.Country()` refuses.

### Which names are accepted

`ll.Country(name)` accepts **exactly** the names in `ll.countries()`: a
directory directly under the config root that carries a `_/data_scheme.yml`
(GH #915). Everything else raises a `ValueError`, before any data is read:

| `name` | raises |
|---|---|
| a registered country, e.g. `'Uganda'` | nothing |
| no such directory, e.g. `'Ghana'` (a typo, or a wrong case like `'uganda'`) | `ValueError: No such country 'Ghana'. Did you mean 'GhanaSPS' or 'GhanaLSS' ...?` |
| not a single non-hidden directory entry: `''`, `'./Uganda'`, `'Uganda/'`, `'Uganda/_'`, `'Uganda/2019-20'`, `'.dvc'`, a `Path` | `ValueError: No such country ...` |
| a directory without `_/data_scheme.yml`, e.g. `'Bulgaria'` | `UnregisteredCountryError` (from `lsms_library.errors`, a `ValueError` subclass) |
| a path out of the root, e.g. `'../..'` | `ValueError: ... path traversal not allowed` |

A non-string name that cannot be joined to a path (`5`, `None`, `b'Uganda'`)
still raises `TypeError`.

**The opt-in, for acquisition and scaffold code only.** Code that works on a
country directory *being built*, before its `_/data_scheme.yml` exists, passes
`ll.Country(name, allow_unregistered=True)`: wave discovery
(`data_access.discover_waves`), provenance stamping
(`scripts/backfill_wave_provenance.py`) and cache maintenance
(`lsms-library cache clear` / `cache list --country`) do. It relaxes only the
`_/data_scheme.yml` requirement. Such a `Country` declares no tables, and its
`waves` come from the `Documentation/SOURCE.org` scan. Analysis code should
never need it.

On the command line, `lsms-library waves|tables --country <name>` prints the
refusal and exits 1. `lsms-library countries` lists exactly `ll.countries()`.

*Known exception (#915-a, undecided).* An entry that is a symlink to a
directory **outside** the root is listed by `ll.countries()` but refused as
path traversal. A relative or symlinked `LSMS_COUNTRIES_ROOT` is fine; only
the entry escaping the root is affected.
