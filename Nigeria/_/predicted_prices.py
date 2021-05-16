#!/usr/bin/env python
"""
Use methods of Summer (1973) to predict prices for different goods, units, places, and times.
"""
import numpy as np
import pandas as pd
import pylab as pl
import matplotlib.cm as cm
from cfe.estimation import ols

P = []
for t in ['2010-11','2012-13','2015-16','2018-19']:
    P.append(pd.read_pickle('../%s/_/individual_unit_values.pickle' % t))

p = pd.concat(P,axis=0)

# Eliminate infinities & zeros
p = p.replace(np.inf,np.nan)
p = p.replace(-np.inf,np.nan)
p = p.replace(0,np.nan)

logp = np.log(p)

# Construct different "settings"
s=pd.DataFrame({'Good-Setting':list(zip(logp.index.get_level_values('t'),
                                        logp.index.get_level_values('m'),
                                        logp.index.get_level_values('i')))},index=logp.index).squeeze()

Settings = pd.get_dummies(s)

Units = pd.get_dummies(logp.index.get_level_values('u'))
Units.index = logp.index
del Units['Kg'] # Reference prices in Kgs.

X = pd.concat([Settings.reset_index(drop=True),
               Units.reset_index(drop=True)],axis=1)

X.index = logp.index

bhat=ols(X,logp,return_se=False)[0]
logphat=X.dot(bhat.T)
logphat.index=logp.index

prices=logphat.groupby(level=['t','m','i']).median().unstack()
prices.columns=prices.T.index.droplevel(0)
prices0=logp.groupby(level=['t','m','i']).median().unstack()
prices0.columns=prices0.T.index.droplevel(0)

pl.figure(1,figsize=(10,10))
pl.clf()

colors = iter(cm.rainbow(np.linspace(0, 1, prices.shape[1])))

for i in prices:
    pl.scatter(prices[i],prices0[i],color=next(colors))

pl.legend(prices.columns, loc='lower right', ncol=2,scatterpoints=2)
v=pl.axis()
pl.axis((v[0],v[1]*1.1,v[2],v[3]))

pl.xlabel('Predicted log prices')
pl.ylabel('Observed log prices')
pl.savefig('/tmp/predicted_prices_scatter.png')

V=pd.concat([prices.stack(),prices0.stack()],axis=1).dropna(how='any').cov()
print("R^2=%6.4f" % (V.iloc[0,0]/V.iloc[1,1],))
