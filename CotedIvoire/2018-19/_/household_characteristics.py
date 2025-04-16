from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_')
from lsms.tools import get_household_roster
import dvc.api
import pandas as pd


def household_characteristics(fn='',sex='',age='',HHID='HHID',months_spent='months_spent',filter=None):

    if type(sex) in [list,tuple]:
        sex,sex_converter = sex
    else:
        sex_converter = None

    if type(age) in [list,tuple]:
        age,age_converter = age
    else:
        age_converter = None

    with dvc.api.open(fn,mode='rb') as dta:
        df = pd.read_stata(dta,convert_categoricals=False,preserve_dtypes=False)

    df['HHID'] = df.grappe*1000+df.menage

    if filter is not None:
        df = df.query(filter)

    z = get_household_roster(df,sex=sex,sex_converter=sex_converter,age=age,age_converter=age_converter,HHID=HHID,months_spent=months_spent,fn_type=None)

    return z


t = '2018'

myvars = dict(fn='../Data/Menage/s01_me_CIV2018.dta',
              age=('s01q03c',lambda y: 2018-y), sex=('s01q01',lambda s: 'm' if s==1 else 'f'),
              filter='vague==1')


z0 = household_characteristics(**myvars)

z0.columns.name = 'k'
z0.index.name = 'j'

z0['t'] = t
z0['m'] = "Cote d'Ivoire"

z0 = z0.reset_index().set_index(['j','t','m'])

# Get rural/urban indicator

t = '2019'

myvars['age'] = ('s01q03c',lambda y: 2019-y)
myvars['filter'] = 'vague==2'


z1 = household_characteristics(**myvars)

z1.columns.name = 'k'
z1.index.name = 'j'

z1['t'] = t
z1['m'] = "Cote d'Ivoire"

z1 = z1.reset_index().set_index(['j','t','m'])

z = pd.concat([z0,z1])

# Get rural/urban indicator
with dvc.api.open('../Data/Menage/s00_me_CIV2018.dta',mode='rb') as dta:
    df = pd.read_stata(dta,convert_categoricals=False,preserve_dtypes=False)[['vague','grappe','menage','s00q04']]

df['j'] = (df.grappe*1000+df.menage).apply(lambda x: '%d' % int(float(x)))
df['t'] = df['vague'].apply(lambda r: ['2018','2019'][r==2])
df['m'] = "Cote d'Ivoire"

df['Rural'] = df['s00q04'] - 1
df.set_index(['j','t','m'],inplace=True)

z = z.join(df['Rural'])

to_parquet(z, 'household_characteristics.parquet')

