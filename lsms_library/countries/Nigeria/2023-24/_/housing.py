"""
Nigeria Wave 5 (2023-24) housing.
Housing collected at post-harvest visit only → t = '2024Q1'.
"""
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet


def extract_label(x):
    """Strip numeric prefix from '2. CORRUGATED IRON SHEETS' → 'Corrugated Iron Sheets'."""
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
    'Long/Short Span Sheets': 'Long/Short Span Sheets',
    'Step Tiles': 'Step Tiles',
    'Zinc Sheet': 'Zinc Sheet',
    'Other (Specify)': 'Other',
}

floor_mapping = {
    'Sand/Dirt/Straw': 'Sand/Dirt/Straw',
    'Smoothed Mud': 'Smoothed Mud',
    'Smooth Cement/Concrete': 'Smooth Cement',
    'Wood': 'Wood',
    'Tile': 'Tile',
    'Terazo': 'Terrazzo',
    'Marble': 'Marble',
    'Other(Specify)': 'Other',
}

idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2024Q1'),
)

myvars = dict(
    Roof=('s9q10', extract_label),
    Floor=('s9q11', extract_label),
)

df = df_data_grabber(
    '../Data/Post Harvest Wave 5/Household/sect9_harvestw5.dta',
    idxvars,
    **myvars,
)

df['Roof'] = df['Roof'].map(lambda x: roof_mapping.get(x, x) if pd.notna(x) else pd.NA)
df['Floor'] = df['Floor'].map(lambda x: floor_mapping.get(x, x) if pd.notna(x) else pd.NA)

df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'housing.parquet')
