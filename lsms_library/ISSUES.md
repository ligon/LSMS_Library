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
