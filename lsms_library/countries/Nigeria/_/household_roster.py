import pandas as pd
from lsms_library import Country

from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

country = Country('Nigeria', preload_panel_ids=False)
declared_folders = set()
for quarter in country.waves:
    wave = country[quarter]
    if 'household_roster' in wave.data_scheme:
        declared_folders.add(wave.wave_folder)

X = []
for t in ['2010-11','2012-13','2015-16','2018-19','2023-24']:
    if t not in declared_folders:
        continue
    X.append(get_dataframe('../%s/_/household_roster.parquet' % t))

x = pd.concat(X,axis=0)

to_parquet(x, '../var/household_roster.parquet')
