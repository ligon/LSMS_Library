"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np

unitlabels = pd.read_csv('unitlabels.csv',index_col=0).squeeze().to_dict()

p={}
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16']:
    p[t] = pd.read_parquet('../'+t+'/_/food_prices.parquet')
    p[t] = p[t].stack('itmcd')
    p[t] = p[t].reset_index().set_index(['HHID','itmcd','units']).squeeze()

p = pd.DataFrame(p)
p.columns.name='t'
p = p.stack()
p = p.reset_index().replace({'units':unitlabels})
p = p.set_index(['t','HHID','itmcd','units'])
p.index.names = ['t','j','i','units']
p = p.unstack('i')

p.to_parquet('food_price.parquet')

