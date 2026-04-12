"""
Nigeria Wave 3 (2015-16) housing.
Housing collected at post-planting visit only → t = '2015Q3'.
"""
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet


def extract_label(x):
    """Strip numeric prefix from '1. GRASS' → 'Grass'."""
    try:
        return x.split('. ', 1)[-1].title()
    except AttributeError:
        return pd.NA


def normalize_other(x):
    if pd.isna(x):
        return pd.NA
    if 'Other' in str(x):
        return 'Other'
    return x


idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2015Q3'),
)

myvars = dict(
    Roof=('s11q7', extract_label),
    Floor=('s11q8', extract_label),
)

df = df_data_grabber(
    '../Data/sect11_plantingw3.dta',
    idxvars,
    **myvars,
)

df['Roof'] = df['Roof'].map(normalize_other)
df['Floor'] = df['Floor'].map(normalize_other)

df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'housing.parquet')
