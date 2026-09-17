import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, lsms_library as ll
pd.set_option('display.width',210)
C=ll.Country('GhanaLSS'); lab=C.labor()
agri=lab['Industry'].between(100,199)
wk=lab[agri&(lab['OwnFarmOrBusiness']==False)&(lab['Earnings'].fillna(0)>0)]
DAYW=wk[wk['EarningsUnit'].astype(str)=='Day']['Earnings'].groupby(level='t').median()
HPD=lab.loc[agri,'HoursPerDay'].dropna().median()
def hh(s,n): return s.groupby(level=['t','i']).sum(min_count=1).rename(n)
ann=lab['WeeksPerYear']*lab['HoursPerWeek']
fam=hh(ann.where(lab['OwnFarmOrBusiness'].fillna(False).astype(bool)&agri),'family_hours')
pl=C.plot_labor(); src=pl.index.get_level_values('source')
hired=hh(pl['Cost'].where(src=='hired'),'hired_cost')
exch=hh(pl['PersonDays'].where(src=='exchange'),'exchange_days')
cp=C.crop_production()
out=hh(cp[['Value_sold','Value_seed','Value_given','Value_lost']].sum(axis=1,min_count=1),'output')
area=hh(cp['Area_ha'],'area_ha')
pi=C.plot_inputs(); inp=hh(pi['Cost'],'input_cost')

d=pd.concat([out,area,fam,hired,exch,inp],axis=1)
dayw=d.index.get_level_values('t').map(DAYW)
d['family_days']=d['family_hours']/HPD
d['hired_days']=d['hired_cost']/dayw
obs=d[['family_days','hired_days','exchange_days']].notna().any(axis=1)
d['labor_days']=d[['family_days','hired_days','exchange_days']].fillna(0).sum(axis=1).where(obs)

def report(label, cols):
    sub=d[cols].dropna(); sub=sub[(sub>0).all(axis=1)]
    n=sub.groupby(level='i').size(); hh_both=(n>=2).sum()
    panel=sub[sub.index.get_level_values('i').isin(n[n>=2].index)]
    lg=np.log(panel); ws={}
    for c in cols:
        y=lg[c]; mu=y.groupby(level='i').transform('mean')
        ws[c]=round(((y-mu)**2).sum()/((y-y.mean())**2).sum(),3)
    print('%-52s hh-years=%-6d households BOTH waves=%-5d' % (label,len(sub),hh_both))
    print('%-52s within share: %s'%('',ws))

report('OLD core   output, land, FAMILY labour only',      ['output','area_ha','family_days'])
report('OLD full   + hired cost + input cost (separate)',  ['output','area_ha','family_days','hired_cost','input_cost'])
print()
report('NEW  output, land, TOTAL labour-days',             ['output','area_ha','labor_days'])
report('NEW  + input cost',                                ['output','area_ha','labor_days','input_cost'])
print()
print('composition of labor_days (median per hh-year):')
print(d[['family_days','hired_days','exchange_days','labor_days']].groupby(level='t').median().round(1).to_string())
print()
z=d['labor_days'].dropna()
print('households with labor_days == 0 :', int((z==0).sum()), 'of', len(z))
print('hh-years where hired>0          : %.1f%%   where exchange>0: %.1f%%'%(
    100*d['hired_days'].fillna(0).gt(0).mean(), 100*d['exchange_days'].fillna(0).gt(0).mean()))
