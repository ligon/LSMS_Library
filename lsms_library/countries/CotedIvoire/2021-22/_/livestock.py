"""Build livestock for CotedIvoire EHCVM 2021-22 (GH #1697).

One source file, s17_me_civ2021.dta: the grid of 13 species codes on every household, of
which 1 to 11 have labels and no row answers yes on 12 or 13.  The species code is s17q01
here, s17q02 holds its label; in 2018-19 s17q02 is the code.  The answer s17q03 has no value
label here and reads 0 or 1, with 2 on two rows that record no head; 2018-19's is 1 Oui,
2 Non.  1 is the gate in both waves.  s17q06 head owned, s17q08 bought and s17q10 sold in
the last 12 months.  Unique on the key, so nothing is summed.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import household_ids

src = get_dataframe('../Data/s17_me_civ2021.dta', convert_categoricals=False)
src = src[src['s17q03'] == 1]

species = tools.get_categorical_mapping(tablename='harmonize_species_ehcvm', idxvars='Code',
                                        **{'Preferred Label': 'Preferred Label'})
species = {int(code): label for code, label in species.items()}
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
