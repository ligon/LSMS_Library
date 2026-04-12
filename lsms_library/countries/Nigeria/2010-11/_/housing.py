"""
Nigeria Wave 1 (2010-11) housing.
Housing collected at post-harvest visit only → t = '2011Q1'.
"""
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet


def extract_string(x):
    try:
        return x.split('. ')[-1].title()
    except AttributeError:
        return pd.NA


roof_mapping = {
    'iron sheets': 'Iron Sheets',
    'grass': 'Grass',
    'abestos sheet': 'Asbestos Sheet',
    'concrete': 'Concrete',
    'clay tiles': 'Clay Tiles',
    'plastic sheeting': 'Plastic Sheeting',
    'wood': 'Wood',
    'mud': 'Mud',
    'other (specify)': 'Other',
}

floor_mapping = {
    'smooth cement': 'Smooth Cement',
    'smoothed mud': 'Smoothed Mud',
    'sand/dirt/straw': 'Sand/Dirt/Straw',
    'tile': 'Tile',
    'wood': 'Wood',
    'other (specify)': 'Other',
}

idxvars = dict(
    i='hhid',
    t=('hhid', lambda x: '2011Q1'),
)

myvars = dict(
    Roof=('s8q7', lambda x: roof_mapping.get(str(x).lower().strip(), pd.NA) if pd.notna(x) else pd.NA),
    Floor=('s8q8', lambda x: floor_mapping.get(str(x).lower().strip(), pd.NA) if pd.notna(x) else pd.NA),
)

df = df_data_grabber(
    '../Data/Post Harvest Wave 1/Household/sect8_harvestw1.dta',
    idxvars,
    **myvars,
)

df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'housing.parquet')
