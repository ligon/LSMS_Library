"""Build nonfood_expenditures for Togo EHCVM 2021-22 (GH #1697).

s09b..s09f, the five recall windows of section 9, as in 2018; see _/CONTENTS.org
"nonfood_expenditures (EHCVM section 9)" for the instrument, for why the window is
carried and not annualised, and for why part A is excluded.  What differs here:

*** THE ITEM CODES MOVED.  53 of the 131 codes name another item than in 2018 (415 was
Parfums and is a washable face mask here), so this wave reads its own nonfood_items, in
2021-22/_/categorical_mapping.org, which wins over the country's by the level rule. ***
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import household_ids

T = '2021-22'
MODULES = {'b': '7 days', 'c': '30 days', 'd': '3 months', 'e': '6 months', 'f': '12 months'}

items = tools.get_categorical_mapping(tablename='nonfood_items', idxvars='Code',
                                      dirs=['./'], **{'Preferred Label': 'Preferred Label'})
items = {int(code): label for code, label in items.items()}
assert len(set(items.values())) == len(items), 'nonfood_items is not injective'

frames = []
for letter, window in MODULES.items():
    src = get_dataframe(f'../Data/s09{letter}_me_tgo2021.dta', convert_categoricals=False)
    src = src[src[f's09{letter}q02'] == 1]
    code = src[f's09{letter}q01'].astype('Int64')
    unknown = sorted(set(code.dropna().unique()) - set(items))
    assert not unknown, f's09{letter}: item codes with no nonfood_items row: {unknown}'
    frames.append(pd.DataFrame({
        't': T,
        'i': household_ids(src).astype('string').values,
        'j': code.map(items).astype('string').values,
        'RecallWindow': window,
        'Expenditure': pd.to_numeric(src[f's09{letter}q03'], errors='coerce').values,
    }))

df = pd.concat(frames, ignore_index=True)
df['Expenditure'] = df['Expenditure'].astype('Float64')
df['RecallWindow'] = df['RecallWindow'].astype('string')

windows = df.groupby('j')['RecallWindow'].nunique()
assert (windows == 1).all(), f'an item in two windows: {windows[windows > 1].index.tolist()}'
df = df.set_index(['t', 'i', 'j'])[['Expenditure', 'RecallWindow']]
assert df.index.is_unique, 'nonfood_expenditures 2021-22 repeats a (household, item)'
to_parquet(df, 'nonfood_expenditures.parquet')
