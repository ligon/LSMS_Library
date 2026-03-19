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

- Status: **RESOLVED** — `_enforce_declared_dtypes()` added to `_finalize_result()`.  Uses nullable pandas dtypes (`BooleanDtype`, `Int64Dtype`, `Float64Dtype`, `StringDtype`).  List declarations (`Sex: [Male, Female]`) cast to `StringDtype` with value validation in `diagnostics._check_value_constraints`.

## 2026-03-18 – categorical_mapping lookup broken for index variables

- `Wave.categorical_mapping` is a property returning a dict, but `column_mapping()` at line 316 calls it as `self.categorical_mapping(table_name)` — `TypeError: 'dict' object is not callable`.
- Also: `Wave.categorical_mapping` property returns `None` because `dict.update()` returns None (line 384).
- Also: the `categorical_mapping.org` file is looked up at the **wave** level (`{wave}/_/categorical_mapping.org`) but typically lives at the **country** level (`_/categorical_mapping.org`).  The country-level dict is loaded via `self.country.categorical_mapping` but the wave-level lookup fails.
- Additionally: Mali's `data_info.yml` files use `mappings:` (plural) but the code only handles `mapping:` (singular).  The plural form is silently ignored.
- **Net effect:** The categorical mapping table reference from `data_info.yml` (`- mapping: ['table_name', 'key_col', 'value_col']`) does not work for index variables in `idxvars`.  It may work for `myvars` if the Country-level (not Wave-level) code path is used.
- **Scope:** `country.py` lines 316, 376-384.

## 2026-03-17 23:44:40Z Uganda – household_roster

- Waves: 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20
- Error: `ImportError: cannot import name '_DIR_MARK' from 'pathspec.patterns.gitwildmatch' (/home/ligon/miniforge3/envs/lab/lib/python3.13/site-packages/pathspec/patterns/gitwildmatch.py)`
## 2026-03-19 – Tanzania panel_ids design problem

- Tanzania's `panel_ids` mechanically works (6/6 checks pass, 11,347 mappings), but the design has a fundamental issue: household splits are handled by retroactively assigning new canonical IDs back to wave 1.  This inflates the baseline household count (9,785 "canonical" households vs ~3,200 actually surveyed).
- The World Bank harmonised panel code (`Append_TZA.do`) uses a different, more intuitive approach: track the household **head** across waves, assign the canonical ID to whichever split-off contains the original head, and give split-offs that don't contain the head a new ID.  This keeps wave 1 counts accurate.
- Additionally, 2019-20 and 2020-21 are **two separate panel branches** (extended panel vs refresh panel) sharing 2014-15 as a common ancestor.  The hardcoded `previous_wave = '2014-15'` for 2020-21 in `tanzania.py` handles this but it means zero household overlap between 2019-20 and 2020-21.
- **Proposed fix:** Rewrite Tanzania's `map_08_15()` and panel_ids logic to follow the WB head-tracking approach.  Reference: `/var/tmp/lsms-isa-harmonised/reproduction/Reproduction_v2/Code/Cleaning_code/Append_TZA.do` lines 23-70.
- **Scope:** `tanzania.py` (map_08_15, panel_ids functions), rebuild `panel_ids.json` and `updated_ids.json`.

## 2026-03-19 – Housing schema inconsistency across countries

- Malawi housing agent produced binary indicators (`Thatched roof: float`, `Earthen floor: float`) matching Uganda's legacy output.
- Other country agents (Ethiopia, Tanzania, Mali, Niger, Burkina Faso) produced categorical columns (`Roof: str`, `Wall: str`, `Floor: str`, etc.) — richer data, consistent with "pass the detail" principle.
- **Fix needed:** Malawi (and Uganda) housing should use categoricals like the other countries. The raw data has the material type labels; the binary indicators discard information.
- **Scope:** Rewrite Malawi housing `data_info.yml` entries to extract `hh_f08` as `Roof: str` (categorical material) instead of mapping to binary. Update Uganda similarly.

## 2026-03-19 05:30:57Z Nigeria – shocks

- Waves: 2010Q3, 2011Q1, 2012Q3, 2013Q1, 2015Q3, 2016Q1, 2018Q3, 2019Q1
- Error: `FileNotFoundError: [Errno 2] No such file or directory: '/var/tmp/coder-mirrors/LSMS_Library/lsms_library/countries/Nigeria/2010Q3/_'`
## 2026-03-19 05:37:51Z Malawi – shocks

- Waves: 2004-05, 2010-11, 2013-14, 2016-17, 2019-20
- Error: `TypeError: 'dict' object is not callable`
## 2026-03-19 13:08:01Z Malawi – food_acquired

- Waves: 2004-05, 2010-11, 2013-14, 2016-17, 2019-20
- Error: `ValueError: No objects to concatenate`
