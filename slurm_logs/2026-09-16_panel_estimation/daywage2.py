import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, lsms_library as ll
C=ll.Country('GhanaLSS'); lab=C.labor()
agri=lab['Industry'].between(100,199)
w=lab[agri&(lab['OwnFarmOrBusiness']==False)&(lab['Earnings'].fillna(0)>0)]
day=w[w['EarningsUnit'].astype(str)=='Day']['Earnings'].groupby(level='t').median()
hpd=lab.loc[agri,'HoursPerDay'].dropna().median()
print('day wage (cedis/day):', day.round(0).to_dict(), ' | ag hours/day:', hpd)
print('=> hourly:', (day/hpd).round(2).to_dict(), ' (previously reported: 40.65 / 41.19)')
print()
def hh(s,n): return s.groupby(level=['t','i']).sum(min_count=1).rename(n)
ann=lab['WeeksPerYear']*lab['HoursPerWeek']
fam=hh(ann.where(lab['OwnFarmOrBusiness'].fillna(False).astype(bool)&agri),'family_hours')
pl=C.plot_labor()
hired=hh(pl['Cost'].where(pl.index.get_level_values('source')=='hired'),'hired_cost')
d=pd.concat([fam,hired],axis=1)
d['dayw']=d.index.get_level_values('t').map(day)
d['family_days']=d['family_hours']/hpd
d['hired_days']=d['hired_cost']/d['dayw']
cp=C.crop_production()
out=cp[['Value_sold','Value_seed','Value_given','Value_lost']].sum(axis=1,min_count=1).groupby(level='t').sum()

print('### everything in PERSON-DAYS, the corpus unit ###')
print(d.groupby(level='t')[['family_days','hired_days']].median().round(1).to_string())
both=d[['family_days','hired_days']].dropna(); both=both[(both>0).all(axis=1)]
sh=both['family_days']/(both['family_days']+both['hired_days'])
print('  households with both: %d ; median family share of person-days: %.3f'%(len(both),sh.median()))
print()
famval=(d['family_hours']*(d['dayw']/hpd)).groupby(level='t').sum()
print('### family labour valued at the DAY wage vs output ###')
for t in out.index:
    print('  %s  family value %12.0f  output %12.0f  ratio %.2f   (was %.2f at the salaried wage)'%(
        t, famval[t], out[t], famval[t]/out[t], {'1987-88':1.02,'1988-89':0.96}[t]))
