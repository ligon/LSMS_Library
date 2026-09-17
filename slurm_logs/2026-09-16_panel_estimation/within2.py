import warnings; warnings.filterwarnings("ignore")
import numpy as np, pandas as pd, lsms_library as ll
C=ll.Country('GhanaLSS')
def hh(s,n): return s.groupby(level=['t','i']).sum(min_count=1).rename(n)
cp=C.crop_production()
out=hh(cp[['Value_sold','Value_seed','Value_given','Value_lost']].sum(axis=1,min_count=1),'output')
area=hh(cp['Area_ha'],'area_ha')
lab=C.labor(); ann=lab['WeeksPerYear']*lab['HoursPerWeek']
own=lab['OwnFarmOrBusiness'].fillna(False).astype(bool)&lab['Industry'].between(100,199)
fam=hh(ann.where(own),'family_hours')
pl=C.plot_labor(); hired=hh(pl['Cost'].where(pl.index.get_level_values('source')=='hired'),'hired_cost')
pi=C.plot_inputs(); inputs=hh(pi['Cost'],'input_cost')
d=pd.concat([out,area,fam,hired,inputs],axis=1)

for label,cols in (('CORE',['output','area_ha','family_hours']),
                   ('FULL',['output','area_ha','family_hours','hired_cost','input_cost'])):
    sub=d[cols].dropna(); sub=sub[(sub>0).all(axis=1)]
    n=sub.groupby(level='i').size(); panel=sub[sub.index.get_level_values('i').isin(n[n>=2].index)]
    lg=np.log(panel)
    print('%s  households=%d  hh-years=%d'%(label,panel.index.get_level_values('i').nunique(),len(panel)))
    for c in cols:
        y=lg[c]; mu=y.groupby(level='i').transform('mean')
        ssw=((y-mu)**2).sum(); sst=((y-y.mean())**2).sum()
        print('    %-13s within share SSW/SST = %.3f   (sd within = %.3f)'%(c, ssw/sst, np.sqrt(ssw/len(y))))
    print()
# how much is lost by requiring positive hired/input spend?
sub=d[['output','area_ha','family_hours']].dropna()
sub=sub[(sub>0).all(axis=1)]
print('of the CORE hh-years, share with ZERO/missing hired labour: %.1f%%'%(100*(1-d.loc[sub.index,'hired_cost'].fillna(0).gt(0).mean())))
print('of the CORE hh-years, share with ZERO/missing input spend : %.1f%%'%(100*(1-d.loc[sub.index,'input_cost'].fillna(0).gt(0).mean())))
