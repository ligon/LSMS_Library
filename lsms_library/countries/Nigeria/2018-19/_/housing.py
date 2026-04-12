"""
Nigeria Wave 4 (2018-19) housing.
Housing collected at post-planting visit only → t = '2018Q3'.
"""
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet


def extract_label(x):
    """Strip numeric prefix from '1. THATCH (GRASS OR STRAW)' → 'Thatch (Grass Or Straw)'."""
    try:
        return x.split('. ', 1)[-1].title()
    except AttributeError:
        return pd.NA


roof_mapping = {
    'Thatch (Grass Or Straw)': 'Thatch',
    'Corrugated Iron Sheets': 'Iron Sheets',
    'Clay Tiles': 'Clay Tiles',
    'Concrete/Cement': 'Concrete',
    'Plastic Sheet': 'Plastic Sheeting',
    'Asbestos Sheet': 'Asbestos Sheet',
    'Mud': 'Mud',
    'Step Tiles': 'Step Tiles',
    'Long/Short Span Sheets': 'Long/Short Span Sheets',
    'Other (Specify)': 'Other',
}

floor_mapping = {
    'Sand/Dirt/Straw': 'Sand/Dirt/Straw',
    'Smoothed Mud': 'Smoothed Mud',
    'Smooth Cement/Concrete': 'Smooth Cement',
    'Wood': 'Wood',
    'Tile': 'Tile',
    'Other (Specify)': 'Other',
    'Terrazo': 'Terrazzo',
}

idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2018Q3'),
)

myvars = dict(
    Roof=('s11q7', extract_label),
    Floor=('s11q8', extract_label),
)

df = df_data_grabber(
    '../Data/sect11_plantingw4.dta',
    idxvars,
    **myvars,
)

df['Roof'] = df['Roof'].map(lambda x: roof_mapping.get(x, x) if pd.notna(x) else pd.NA)
df['Floor'] = df['Floor'].map(lambda x: floor_mapping.get(x, x) if pd.notna(x) else pd.NA)

df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'housing.parquet')
