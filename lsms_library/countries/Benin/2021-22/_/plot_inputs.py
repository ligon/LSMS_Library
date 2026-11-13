"""Build plot_inputs for Benin EHCVM 2021-22 (GH #1697).

One source file, s16b_me_ben2021.dta, a grid of 22 inputs for each of the 3,687 farming
households plus 4,345 blank rows for the others.  11,368 rows answer s16bq02 = Oui.

  s16bq03a/b                 the quantity used, and its unit: `u`
  s16bq05, s16bq07a/b        a gift or donation, its quantity and unit
  s16bq08, s16bq09a/b        a purchase, its quantity and unit

THE PURCHASE IS s16bq08 (the Stata labels of both waves; Burkina Faso's GH #1188).  Benin
2018-19's script serves s16bq05 and s16bq07a under Purchased and Quantity_purchased, which is
the gift question.  This wave reads the purchase.  So those two columns do not mean the same
thing across Benin's waves until 2018-19 is corrected: see _/CONTENTS.org.

s16bq03b has a code the file does not label: 6, on 160 rows.  The questionnaire prints codes 1
to 5 for this variable, and s16bq09b's list, where 6 is Sachet, does not settle it (97 of the
160 are cuttings, up to 100,000 of them).  It is served as its code, `6`, as an unlabelled size
is: leave the code, invent no label.

The seed's crop sits in the input label, through harmonize_seed_crop.  Grain
(t, i, input, crop, u), and the same uniqueness guard as 2018-19.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.ehcvm import served_unit_labels
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import household_ids

CROP_NA = '(not crop-specific)'
UNIT_NA = 'Unknown'


def _table(name):
    return tools.get_categorical_mapping(tablename=name, idxvars='Original Label',
                                         **{'Preferred Label': 'Preferred Label'})


def _units(series):
    """Each unit label as the API serves it; NA stays NA; a bare code as its integer."""
    raw = series.astype('string').str.strip().str.replace(r'^(\d+)\.0$', r'\1', regex=True)
    known = raw.dropna().unique()
    served = dict(zip(known, served_unit_labels('Benin', known)))
    return raw.map(served).astype('string')


src = get_dataframe('../Data/s16b_me_ben2021.dta', convert_categoricals=True)
codes = get_dataframe('../Data/s16b_me_ben2021.dta', convert_categoricals=False)

applied = (codes['s16bq02'] == 1).values
src, codes = src[applied], codes[applied]

label = src['s16bq01'].astype('string').str.strip()
input_map = _table('harmonize_input')
unknown = sorted(set(label) - set(input_map))
assert not unknown, f'plot_inputs 2021-22: labels with no harmonize_input row: {unknown}'
seed_crop = _table('harmonize_seed_crop')

yes_no = {'Oui': True, 'Non': False}
df = pd.DataFrame({
    't': '2021-22',
    'i': household_ids(codes).astype('string').values,
    'input': label.map(input_map).astype('string').values,
    'crop': label.map(seed_crop).astype('string').fillna(CROP_NA).values,
    'u': _units(src['s16bq03b']).fillna(UNIT_NA).values,
    'Quantity': pd.to_numeric(codes['s16bq03a'], errors='coerce').astype('Float64').values,
    'Purchased': src['s16bq08'].astype('string').str.strip().map(yes_no).astype('boolean').values,
    'Quantity_purchased': pd.to_numeric(codes['s16bq09a'], errors='coerce').astype('Float64').values,
    'Unit_purchased': _units(src['s16bq09b']).values,
    'Received_as_gift': src['s16bq05'].astype('string').str.strip().map(yes_no).astype('boolean').values,
    'Quantity_received_as_gift': pd.to_numeric(codes['s16bq07a'], errors='coerce').astype('Float64').values,
    'Unit_received_as_gift': _units(src['s16bq07b']).values,
}).set_index(['t', 'i', 'input', 'crop', 'u'])

assert len(df) > 0, 'plot_inputs 2021-22 produced no rows'
duplicated = df.index[df.index.duplicated()]
assert duplicated.empty, (
    f'plot_inputs 2021-22: {len(duplicated)} duplicate tuples on (t, i, input, crop, u): '
    'two reported input lines collide on one key.  Split the bucket in '
    'harmonize_seed_crop rather than letting a row disappear.')
to_parquet(df, 'plot_inputs.parquet')
