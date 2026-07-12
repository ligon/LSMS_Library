import warnings; warnings.filterwarnings('ignore')
import pandas as pd, os
from lsms_library.local_tools import data_root, map_index
for ctry in ['Tanzania','Mali']:
    p = os.path.join(str(data_root()),ctry,'var','community_prices.parquet')
    if not os.path.exists(p):
        print(f"{ctry}: var MISSING"); continue
    df = pd.read_parquet(p)
    print(f"{ctry} var BEFORE: {df.index.names} shape {df.shape} cols {list(df.columns)}")
    mi = map_index(df.copy())
    print(f"{ctry} after map_index: {mi.index.names} shape {mi.shape} cols {list(mi.columns)}")
