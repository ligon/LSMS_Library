"""Build plot_inputs for Niger EHCVM 2018-19 (GAP 2, item-level).

Single source file: s16b_me_ner2018.dta — the household agricultural-input
roster, one row per input-type the household was asked about.  Columns:
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (Oui / Non) — application gate (all Oui in 2018)
  s16bq03a / s16bq03b   quantity used + native unit
  s16bq05   purchased? (Oui / Non)
  s16bq07a  quantity purchased (native unit)

We keep s16bq02==1 (applied) rows — in 2018 every row is already an applied
input, but the gate is kept for parity with 2021 (where most rows are
not-used / not-asked).  The EHCVM roster has NO crop column — the seed's
crop is embedded in the input-type label ('Semences de petit mil'),
resolved to the harmonize_food crop label via harmonize_seed_crop.
Non-seed input rows carry crop NaN.  i is the EHCVM composite id ('E_' +
grappe + '0' + zero-padded menage) via niger.i.  No plot column; grain is
(t, i, input, crop, u).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import (i as niger_i, _input_maps, _input_labels,
                   _seed_crop_labels, _unit_labels, _finish_plot_inputs)


src = get_dataframe('../Data/s16b_me_ner2018.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/s16b_me_ner2018.dta', convert_categoricals=False)

input_map, unit_map, seed_crop_map = _input_maps()

# Keep only inputs the household actually applied (s16bq02 == 1 = Oui).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = src.apply(lambda r: niger_i(pd.Series([r['grappe'], r['menage']],
                                           index=['grappe', 'menage'])), axis=1)

# purchased: s16bq05 Oui/Non (loaded as labels with convert_categoricals)
purchased = src['s16bq05'].map({'Oui': True, 'Non': False})

df = pd.DataFrame({
    'i':                  hh.values,
    'input':              _input_labels(src['s16bq01'], input_map).values,
    'crop':               _seed_crop_labels(src['s16bq01'], seed_crop_map).values,
    'u':                  _unit_labels(src['s16bq03b'], unit_map).values,
    'Quantity':           pd.to_numeric(srcn['s16bq03a'], errors='coerce').values,
    'Purchased':          purchased.values,
    'Quantity_purchased': pd.to_numeric(srcn['s16bq07a'], errors='coerce').values,
})

df = _finish_plot_inputs(df, '2018-19')

assert len(df) > 0, 'plot_inputs 2018-19 produced no rows'
to_parquet(df, 'plot_inputs.parquet')
