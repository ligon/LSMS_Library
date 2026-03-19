## 2025-11-19 07:44:01Z Uganda – food_quantities

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: 'No data produced for food_quantities via DVC.'`
- MWE: `cd lsms_library/countries && poetry run dvc repro Uganda/dvc.yaml:materialize@uganda::::food_quantities`
- Status: RESOLVED 2025-11-19 — `_load_materialize_stage_map` now honors `${item.output}` and `Country.load_dataframe_with_dvc` drives Repo() from `lsms_library/countries`, so the stage reproduces correctly.
## 2025-11-19 07:47:47Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `ValueError: Cannot reorder index levels for '2018-19': Length of order must be same as number of levels (3), got 2`
- MWE: `cd lsms_library/countries && poetry run dvc repro Uganda/dvc.yaml:materialize@uganda::::people_last7days`
## 2025-11-19 12:31:02Z Uganda – food_expenditures

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: 'No data produced for food_expenditures via DVC.'`
## 2025-11-19 13:13:39Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `ValueError: Cannot reorder index levels for '2018-19': Length of order must be same as number of levels (3), got 2`
## 2025-11-19 13:16:31Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `ValueError: Cannot reorder index levels for '2018-19': Length of order must be same as number of levels (3), got 2`
## 2025-11-20 22:16:18Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "None of ['m'] are in the columns"`
## 2025-11-20 22:35:48Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "['i'] not in index"`
## 2025-11-20 22:35:50Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "None of ['m'] are in the columns"`
## 2025-11-20 22:37:11Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "['i'] not in index"`
## 2025-11-20 22:37:13Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "None of ['m'] are in the columns"`
## 2025-11-20 22:38:34Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "['i'] not in index"`
## 2025-11-20 22:38:36Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "None of ['m'] are in the columns"`
## 2025-11-20 22:45:02Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "Could not assign region index 'm' for 1833 households; sample=[{'i': 'H00101-04-01', 't': '2013-14'}, {'i': 'H00102-04-01', 't': '2013-14'}, {'i': 'H00104-04-01', 't': '2013-14'}, {'i': 'H00110-04-01', 't': '2013-14'}, {'i': 'H00208-04-01', 't': '2013-14'}]"`
## 2025-11-20 22:45:04Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "None of ['m'] are in the columns"`
## 2025-11-20 22:47:06Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "Could not assign region index 'm' for 1833 households; sample=[{'i': 'H00101-04-01', 't': '2013-14'}, {'i': 'H00102-04-01', 't': '2013-14'}, {'i': 'H00104-04-01', 't': '2013-14'}, {'i': 'H00110-04-01', 't': '2013-14'}, {'i': 'H00208-04-01', 't': '2013-14'}]"`
## 2025-11-20 22:52:49Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "Could not assign region index 'm' for 1833 households; sample=[{'i': 'H00101-04-01', 't': '2013-14'}, {'i': 'H00102-04-01', 't': '2013-14'}, {'i': 'H00104-04-01', 't': '2013-14'}, {'i': 'H00110-04-01', 't': '2013-14'}, {'i': 'H00208-04-01', 't': '2013-14'}]"`
## 2025-11-20 22:55:40Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "Could not assign region index 'm' for 66 households; sample=[{'i': '00c9353d8ebe42faabf5919b81d7fae7', 't': '2018-19'}, {'i': '02dd448165ce46279ca601a02865d543', 't': '2018-19'}, {'i': '037866653c7c4cb99a80f05a38cdafb2', 't': '2018-19'}, {'i': '039a11571b874a88b7a6c200469fe4f3', 't': '2018-19'}, {'i': '062da72d5d3a457e9336b62c8bb9096d', 't': '2018-19'}]"`
## 2025-11-20 23:05:27Z Uganda – people_last7days

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `KeyError: "None of ['m'] are in the columns"`
## 2025-11-22 15:57:36Z Uganda – household_characteristics

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `CalledProcessError: Command '['make', '-s', '../2005-06/_/household_characteristics.parquet']' returned non-zero exit status 2.`
## 2025-12-05 14:04:13Z TestCountry – household_roster

- Waves: 2020-21
- Error: `TypeError: 'Mock' object does not support the context manager protocol`
## 2025-12-05 14:33:06Z TestCountry – test_data

- Waves: 2020-21
- Error: `FileNotFoundError: [Errno 2] No such file or directory: '/tmp/test/countries/TestCountry/2020-21/_'`
## 2025-12-05 15:14:31Z TestCountry – test_data

- Waves: 2020-21
- Error: `TypeError: 'Mock' object does not support the context manager protocol`
## 2025-12-05 15:18:37Z TestCountry – household_roster

- Waves: 2020-21
- Error: `TypeError: 'Mock' object does not support the context manager protocol`
## 2025-12-05 15:22:56Z TestCountry – household_roster

- Waves: 2020-21
- Error: `TypeError: 'Mock' object does not support the context manager protocol`
## 2025-12-05 15:23:22Z TestCountry – household_roster

- Waves: 2020-21
- Error: `TypeError: 'Mock' object does not support the context manager protocol`
## 2026-03-18 – Enforce dtypes from data_scheme.yml

- The `data_scheme.yml` declares column types (e.g., `EffectedIncome: bool`) but these are not enforced after loading.  YAML mappings that produce True/False mixed with NaN result in `object` dtype instead of pandas nullable `boolean`.
- **Proposed fix:** After `grab_data()` or `load_from_waves()`, cast columns to the types declared in the scheme.  Use `pd.BooleanDtype()` for bool, `pd.Int64Dtype()` for int, etc. — these support `pd.NA`.
- **Scope:** `country.py`, likely in `_finalize_result()` or `_aggregate_wave_data()`.
- **Risk:** Low — only affects columns with explicit type declarations in the scheme.

## 2026-03-17 23:44:40Z Uganda – household_roster

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `ImportError: cannot import name '_DIR_MARK' from 'pathspec.patterns.gitwildmatch' (/home/ligon/miniforge3/envs/lab/lib/python3.13/site-packages/pathspec/patterns/gitwildmatch.py)`
