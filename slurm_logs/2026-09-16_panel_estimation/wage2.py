import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, lsms_library as ll
C=ll.Country('GhanaLSS'); lab=C.labor()
agri=lab['Industry'].between(100,199)
w=lab[agri&(lab['OwnFarmOrBusiness']==False)]
hpw,wky,e=w['HoursPerWeek'],w['WeeksPerYear'],w['Earnings']; u=w['EarningsUnit'].astype(str)
h=pd.Series(np.nan,index=w.index)
h[u=='Hour']=e[u=='Hour']; h[u=='Week']=e[u=='Week']/hpw[u=='Week']
h[u=='Month']=e[u=='Month']/(hpw[u=='Month']*4.333)
h[u=='Year']=e[u=='Year']/(wky[u=='Year']*hpw[u=='Year'])
h=h.replace([np.inf,-np.inf],np.nan); h=h[h>0]
wage=h.groupby(level='t').median()
print('median agricultural wage, cedis/hour:', wage.round(2).to_dict())

def hh(s,n): return s.groupby(level=['t','i']).sum(min_count=1).rename(n)
ann=lab['WeeksPerYear']*lab['HoursPerWeek']
fam=hh(ann.where(lab['OwnFarmOrBusiness'].fillna(False).astype(bool)&agri),'family_hours')
pl=C.plot_labor()
hired=hh(pl['Cost'].where(pl.index.get_level_values('source')=='hired'),'hired_cost')
exch=hh(pl['PersonDays'].where(pl.index.get_level_values('source')=='exchange'),'exchange_days')
d=pd.concat([fam,hired,exch],axis=1)
d['wage']=d.index.get_level_values('t').map(wage)
d['family_value']=d['family_hours']*d['wage']
d['hired_hours']=d['hired_cost']/d['wage']
print()
print('medians per household-year (households reporting each):')
print(d.groupby(level='t')[['family_hours','hired_hours','exchange_days','family_value','hired_cost']].median().round(0).to_string())
print()
both=d[['family_hours','hired_cost']].dropna()
both=both[(both>0).all(axis=1)]
sh=(both['family_hours']*both.index.get_level_values('t').map(wage))
sh=sh/(sh+both['hired_cost'])
print('households reporting BOTH family and hired labour:', len(both))
print('  median family share of total labour value: %.3f  (p25 %.3f, p75 %.3f)'%(sh.median(),sh.quantile(.25),sh.quantile(.75)))
print()
print('NOTE: the hours-share equals the value-share BY CONSTRUCTION when one')
print('wage is used both to value family hours and to deflate hired cost.')
print()
tot=d.groupby(level='t')[['family_value','hired_cost']].sum()
tot['family_share']=tot['family_value']/(tot['family_value']+tot['hired_cost'])
print('aggregate labour value by wave (cedis):'); print(tot.round(0).to_string())
