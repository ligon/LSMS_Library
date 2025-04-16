from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd
from ethiopia import change_id, Waves

def id_walk(df,wave,waves):
    
    use_waves = list(waves.keys())
    T = use_waves.index(wave)
    for t in use_waves[T::-1]:
        if len(waves[t]):
            df = change_id(df,'../%s/Data/%s' % (t,waves[t][0]),*waves[t][1:])
        else:
            df = change_id(df)

    return df
    
x = {}

for t in Waves.keys():
    print(t)
    x[t] = get_dataframe('../'+t+'/_/other_features.parquet')
    if 't' in x[t].index.names:
        x[t] = x[t].droplevel('t')
    x[t] = id_walk(x[t],t,Waves)
    x[t] = x[t].stack('k').dropna()
    x[t] = x[t].reset_index().set_index(['j','m','k']).squeeze()

z = pd.DataFrame(x)
z.columns.name = 't'

z = z.stack().unstack('m')

#z['m'] = 'Uganda'

# Harmonize region labels
regions = {'addis ababa':'Addis Ababa',
           'tigray':'Tigray',
           'afar':'Afar',
           'amhara':'Amhara',
           'benishangul gumuz':'Benishangul-Gumuz',
           'benshagul gumuz':'Benishangul-Gumuz',
           'dire dawa':'Dire Dawa',
           'diredwa':'Dire Dawa',
           'gambela':'Gambela',
           'gambelia':'Gambela',
           'harar':'Harari',
           'harari':'Harari',
           'oromia':'Oromia',
           'snnp':'SNNP',
           'somali':'Somali',
           'somalie':'Somali'
           }

z = z.rename(columns=regions)

z = z.stack().unstack('k')

z = z.reset_index().set_index(['j','t','m'])

to_parquet(z, '../var/other_features.parquet')
