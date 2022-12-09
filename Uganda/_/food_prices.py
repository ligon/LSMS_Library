"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np

#unitlabels = pd.read_csv('unitlabels.csv',index_col=0).squeeze().to_dict()

p={}
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16','2018-19','2019-20']:
    p[t] = pd.read_parquet('../'+t+'/_/food_prices.parquet').squeeze()

p = pd.DataFrame(p)
p.columns.name='t'
p = p.stack()

p = p.unstack('i')
p['m'] = 'Uganda'
p = p.reset_index().set_index(['t','m']).T

p.to_parquet('food_price.parquet')

