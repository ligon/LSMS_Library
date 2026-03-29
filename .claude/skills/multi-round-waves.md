# Multi-Round Wave Folders and wave_folder_map

Some countries store multiple survey rounds in a single folder (e.g.
Tanzania's `2008-15/` contains rounds 2008-09 through 2014-15).  The
Country class handles this via `wave_folder_map`.

## How it works

The country module (e.g. `tanzania.py`) defines:

```python
waves = ['2008-09', '2010-11', '2012-13', '2014-15', '2019-20', '2020-21']
wave_folder_map = {
    '2008-09': '2008-15', '2010-11': '2008-15',
    '2012-13': '2008-15', '2014-15': '2008-15',
    '2019-20': '2019-20', '2020-21': '2020-21',
}
```

The Country class picks this up in the `waves` property and stores it
as `self.wave_folder_map`.  When you call `country['2008-09']`, it
creates `Wave(year='2008-09', wave_folder='2008-15', ...)`.

The wave-level script (e.g. `2008-15/_/food_acquired.py`) produces
data for *all* rounds with `t` in the index.  `Wave.grab_data()`
reads the parquet and filters to `df[df.index.get_level_values('t') == self.year]`.

## Critical: use wave_folder for file paths

Anywhere the code constructs a file path to look for or write a
parquet, it must use `self.wave_folder` (the folder name, e.g.
`'2008-15'`), NOT `self.year` (the logical wave, e.g. `'2008-09'`).

Locations that were fixed for this:

- `Wave.grab_data()` — `external_parquet` path
- `Wave.food_acquired()` — parquet existence check
- `Country._aggregate_wave_data()` — `run_make_target()` output
  candidates and make targets

## Parquet caching

`Wave.grab_data()` checks whether the parquet already exists before
invoking Make.  This is important for multi-round waves where the
build is expensive (~60s for Tanzania 2008-15) and every sub-wave
would otherwise re-trigger it.
