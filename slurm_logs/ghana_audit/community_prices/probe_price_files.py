"""Probe every GhanaLSS wave's price-survey file: shape, columns, dtypes, head.

Read-only.  Run from the worktree with LSMS_COUNTRIES_ROOT / LSMS_DATA_DIR set.
"""
import os
import sys

import pandas as pd

from lsms_library.paths import countries_root
from lsms_library.local_tools import get_dataframe

assert 'wt-glss-prices' in str(countries_root()), countries_root()

FILES = {
    '1987-88': ['Data/PRICE.DAT'],
    '1988-89': ['Data/PRICE.DAT'],
    '1991-92': ['Data/Prices/G3PRICE.DTA'],
    '1998-99': ['Data/Prices/G4PRICE.DTA'],
    '2012-13': ['Data/PRICES/price_sec0.dta', 'Data/PRICES/price_sec1.dta',
                'Data/PRICES/price_sec2.dta'],
    '2016-17': ['Data/g7price.dta'],
}


def main(waves=None):
    root = countries_root() / 'GhanaLSS'
    pd.set_option('display.width', 250)
    pd.set_option('display.max_columns', 60)
    for w, fns in FILES.items():
        if waves and w not in waves:
            continue
        for fn in fns:
            path = root / w / fn
            print(f'\n===== {w} {fn}')
            try:
                df = get_dataframe(str(path), convert_categoricals=False)
            except Exception as e:
                print('READ FAILED:', type(e).__name__, e)
                continue
            print('shape', df.shape)
            print('dtypes:\n', df.dtypes.to_string())
            print('head:\n', df.head(8).to_string())
            print('nunique:\n', df.nunique().to_string())


if __name__ == '__main__':
    main(sys.argv[1:] or None)
