"""Build plot_labor for Benin EHCVM 2021-22 (GH #552).

One source file, s16a_me_ben2021.dta, with 2018-19's grids: family days s16aq33b_*, s16aq35b_*,
s16aq37b_*, and hired s16aq39/41/43 {a workers, b days, c wage} over four worker categories.
Only the number of member columns grows (67 from 54).  One row per (plot, source), summed over
member, category and phase: the grain the table declares, as in 2018-19/_/plot_labor.py.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import household_ids, plot_ids

src = get_dataframe('../Data/s16a_me_ben2021.dta', convert_categoricals=False)
cols = list(src.columns)


def _num(col):
    return pd.to_numeric(src[col], errors='coerce').astype('Float64')


hh = household_ids(src)
plot = plot_ids(src, 's16aq02', 's16aq03')

family_cols = [c for c in cols if c.startswith(('s16aq33b_', 's16aq35b_', 's16aq37b_'))]
family_days = sum(_num(c).fillna(0) for c in family_cols)
family_any = pd.concat([_num(c).notna() for c in family_cols], axis=1).any(axis=1)
family = pd.DataFrame({'i': hh.values, 'plot_id': plot.values, 'source': 'family',
                       'PersonDays': family_days.where(family_any.values, pd.NA).values,
                       'Wage': pd.NA})

hired_days = pd.Series(0.0, index=src.index, dtype='Float64')
hired_wage = pd.Series(0.0, index=src.index, dtype='Float64')
any_days = pd.Series(False, index=src.index)
any_wage = pd.Series(False, index=src.index)
for base in ['s16aq39', 's16aq41', 's16aq43']:
    for workers_col in [c for c in cols if c.startswith(base + 'a_')]:
        suffix = workers_col[len(base + 'a'):]
        cell = _num(workers_col) * _num(base + 'b' + suffix)
        wage = _num(base + 'c' + suffix)
        hired_days = hired_days.add(cell.fillna(0))
        any_days = any_days | cell.notna().values
        hired_wage = hired_wage.add(wage.fillna(0))
        any_wage = any_wage | wage.notna().values
hired = pd.DataFrame({'i': hh.values, 'plot_id': plot.values, 'source': 'hired',
                      'PersonDays': hired_days.where(any_days.values, pd.NA).values,
                      'Wage': hired_wage.where(any_wage.values, pd.NA).values})

df = pd.concat([family, hired], ignore_index=True)
df['t'] = '2021-22'
df['source'] = df['source'].astype('string')
df['plot_id'] = df['plot_id'].astype('string')
df['PersonDays'] = pd.to_numeric(df['PersonDays'], errors='coerce').astype('Float64')
df['Wage'] = pd.to_numeric(df['Wage'], errors='coerce').astype('Float64')
df = df[df['PersonDays'].notna() | df['Wage'].notna()]
df = df.set_index(['t', 'i', 'plot_id', 'source'])

assert df.index.is_unique, 'plot_labor 2021-22 repeats a (plot, source)'
assert len(df) > 0, 'plot_labor 2021-22 produced no rows'
to_parquet(df, 'plot_labor.parquet')
