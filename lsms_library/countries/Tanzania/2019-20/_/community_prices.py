from lsms_library.local_tools import to_parquet, get_dataframe
#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pint import UnitRegistry, UndefinedUnitError, DimensionalityError

ureg = UnitRegistry(case_sensitive=False)
ureg.define('Piece = 1*count')

fn = '../Data/CM_SEC_F_ID.dta'  # Data on prices

# Prices reported for village (=v=), district capital (=d=). Each is price =p=
# is of some weight =w= measured in units =u=.

b = dict(int_key    = 'interview__key',  # interview__{key,id} both unique identifiers?
         i          = 'item_id',
         price_v    = 'cm_f063',
         weight_v   = 'cm_f062',
         unit_v     = 'cm_f061',
         price_d    = 'cm_f066',
         weight_d   = 'cm_f065',
         unit_d     = 'cm_f064',
         )

df = get_dataframe(fn)

df = df[b.values()]
df = df.rename(columns={v:k for k,v in b.items()}).set_index(['int_key','i'])

df = df.dropna(how='all')

#########################################
# Now place for which prices are reported
#########################################


fn = '../Data/CM_SEC_F.dta'  # Data on prices

c = dict(int_key    = 'interview__key',  # interview__{key,id} both unique identifiers?
         region     = 'cm_f01',
         district   = 'cm_f02',
         ward       = 'cm_f03',
         village    = 'cm_f04',
         ea         = 'cm_f05',
         )

place = get_dataframe(fn,convert_categoricals=True)

place = place.replace('**CONFIDENTIAL**',np.nan)
place = place.loc[:,place.count()>0] # Drop columns with no data

place = place[c.values()]
place = place.rename(columns={v:k for k,v in c.items()}).set_index(['int_key'])

place = place.dropna(how='all')

### Merge ###
out = pd.merge(df.reset_index('i'),place,on='int_key',how='outer')

#######################################################################
# Link community prices to household clusters.
#
# Issue #113 hoped to join community prices to households via the EA
# (=cm_f05=) <-> household =clusterid= relationship in HH_SEC_A.dta.
# That linkage does NOT hold in the data:
#   - The community =interview__key= has zero overlap with the household
#     =interview__key= (community is its own questionnaire instrument).
#   - The community location codes (region/district/ward/village/EA in
#     cm_f01..cm_f05) and the household =clusterid= / =sdd_cluster=
#     (region-rural-district-EA) use incompatible coding schemes: at the
#     EA level only ~2/31 community location tuples match any household
#     cluster, and the community EA column is mostly missing.
# The finest geographic key that DOES reconcile is the region name
# (~20/21 community regions match a household region).  So community
# prices are linked to households at the *region* level (market index
# =m=), serving as a region-level fallback price -- not a cluster-level
# one.  See CONTENTS.org "#113" and HH_SEC_A for the verification.
#######################################################################

# Region name is reported directly on the community record (cm_f01);
# use it as the market index m.
out['m'] = out['region']

out = out.reset_index().set_index(['int_key','i','m'])

# Handle unit conversions
def to_kgs(q,u,ureg=ureg):
    """Convert quantity q of units u to kgs or ls"""
    if type(u) is float: return ureg.Quantity(np.nan,'Piece')
    try:
        x = ureg.Quantity(float(q),u.lower())
    except UndefinedUnitError:
        return ureg.Quantity(float(q),'Piece')

    try:
        return x.to(ureg.kilogram)
    except DimensionalityError:
        if x.u == 'Piece': return x
        return x.to(ureg.liter)

def price_per_unit(p,q,ureg=ureg):
    try:
        return p/q
    except ZeroDivisionError:
        return ureg.Quantity(np.nan,q.u)

out['w_v']=out[['weight_v','unit_v']].T.apply(lambda x : to_kgs(x['weight_v'],x['unit_v']))

village_price = out[['price_v','w_v']].T.apply(lambda x: price_per_unit(x['price_v'],x['w_v']))

out['w_d']=out[['weight_d','unit_d']].T.apply(lambda x : to_kgs(x['weight_d'],x['unit_d']))

district_price = out[['price_d','w_d']].T.apply(lambda x: price_per_unit(x['price_d'],x['w_d']))

vg = village_price.apply(lambda x: x.m).groupby(['i','m'])

to_parquet(vg.median().unstack('m'), 'community_prices.parquet')
