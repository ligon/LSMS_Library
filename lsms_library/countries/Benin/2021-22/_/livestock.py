"""Build livestock for Benin EHCVM 2021-22 (GH #1697).

One source file, s17_me_ben2021.dta, a grid of (household, species) for the households asked:
6,416 of its 92,371 rows answer s17q03 = Oui.  The species is s17q01 here and s17q02 in
2018-19, on one code list.  s17q06 head owned, s17q08 bought and s17q10 sold in the last 12
months.  Unique on the key, so nothing is summed.

Code 12 has no value label, and the questionnaire's list ends at 11, `Autres volailles`.  It is
on the grid of vague 2 only (4,019 households), 5 of them answering Oui.  Those 5 rows are served
under the code, `12`: leave the code, invent no label.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import household_ids

src = get_dataframe('../Data/s17_me_ben2021.dta', convert_categoricals=False)
src = src[src['s17q03'] == 1]

species = tools.get_categorical_mapping(tablename='harmonize_species_ehcvm', idxvars='Code',
                                        **{'Preferred Label': 'Preferred Label'})
species = {int(code): label for code, label in species.items()}
species[12] = '12'
unknown = sorted(set(src['s17q01'].astype(int)) - set(species))
assert not unknown, f'livestock 2021-22: species codes with no row: {unknown}'

df = pd.DataFrame({
    't': '2021-22',
    'i': household_ids(src).astype('string').values,
    'animal': src['s17q01'].astype(int).map(species).astype('string').values,
    'HeadCount': pd.to_numeric(src['s17q06'], errors='coerce').astype('Float64').values,
    'HeadAcquired': pd.to_numeric(src['s17q08'], errors='coerce').astype('Float64').values,
    'HeadSold': pd.to_numeric(src['s17q10'], errors='coerce').astype('Float64').values,
}).set_index(['t', 'i', 'animal'])

assert df.index.is_unique, 'livestock 2021-22 repeats a (household, species)'
assert len(df) > 0, 'livestock 2021-22 produced no rows'
to_parquet(df, 'livestock.parquet')
