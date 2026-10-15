from pathlib import Path
import argparse,hashlib,json,subprocess
import lsms_library as ll
import numpy as np
import pandas as pd
import pandas.testing as pdt
from lsms_library import transformations as T
parser=argparse.ArgumentParser()
for flag in ('evidence','inputs','data-root','root'): parser.add_argument('--'+flag,type=Path,required=True)
args=parser.parse_args()
assert Path.cwd().resolve()==args.root.resolve()
assert Path(ll.__file__).resolve().is_relative_to(args.root.resolve()),ll.__file__
ROOT=args.evidence; ST=ROOT/'stages'; IN=args.inputs
fa=pd.read_pickle(IN/'base_food_acquired.pkl'); fct=pd.read_pickle(IN/'base_fct.pkl')
flat=fa.reset_index(); base='8ec0bab4e'; final='live'
refs=list(json.loads((ST/'summary.json').read_text()))
def load(r,t): return pd.read_pickle((ROOT/'counterfactual' if '+' in r else ST)/f'{r}_{t}.pkl')
def hashdf(df): return hashlib.sha256(pd.util.hash_pandas_object(df,index=True).values.tobytes()).hexdigest()
def details(a,b):
 old=load(a,'n'); new=load(b,'n'); assert old.index.equals(new.index) and old.columns.equals(new.columns)
 f0=load(a,'factors'); f1=load(b,'factors'); assert fa.index.equals(f0.index) and fa.index.equals(f1.index)
 z=flat[['t','j','u']].copy()
 z['oldkg']=(fa.Quantity*f0.kg_per_unit).fillna(0).to_numpy()
 z['newkg']=(fa.Quantity*f1.kg_per_unit).fillna(0).to_numpy()
 z['dkg']=z.newkg-z.oldkg
 z['rung']=f1.get('KgFactorSource',pd.Series('unpartitioned',index=f1.index)).to_numpy()
 changed=(f0.kg_per_unit.ne(f1.kg_per_unit)&~(f0.kg_per_unit.isna()&f1.kg_per_unit.isna())).to_numpy()
 grouped=z.groupby(['j','u','rung'],dropna=False)[['oldkg','newkg','dkg']].sum()
 grouped['rows']=z.groupby(['j','u','rung'],dropna=False).size()
 for nutrient in fct.index:
  density=grouped.index.get_level_values('j').map(fct.loc[nutrient].fillna(0)).fillna(0).to_numpy()
  grouped[nutrient]=grouped.dkg*density
 calc=grouped[fct.index].sum(); expected=(new-old).sum()
 np.testing.assert_allclose(calc,expected,rtol=1e-9,atol=2e-6)
 diff=old.ne(new)&~(old.isna()&new.isna())
 def table(frame):return frame.reset_index().to_dict('records')
 result={'old':a,'new':b,'factor_rows_changed':int(changed.sum()),'native_rows_with_quantity_and_change':int((changed & fa.Quantity.notna().to_numpy()).sum()),'nutrient_rows_changed':int(diff.any(axis=1).sum()),'nutrient_cells_changed':int(diff.to_numpy().sum()),'kg_delta':float(z.dkg.sum()),'nutrient_delta':expected.to_dict(),'source_replay_max_abs_error':float((calc-expected).abs().max()),'by_final_rung':table(grouped.groupby('rung').sum()),'top_contributors':{}}
 for nutrient in ['Energy','Vitamin A','Vitamin B-12']:
  by=grouped.groupby('j').sum().sort_values(nutrient)
  result['top_contributors'][nutrient]={'negative':table(by.head(8)[['dkg',nutrient]]),'positive':table(by.tail(8)[['dkg',nutrient]])}
 grouped.to_csv(ROOT/f'attribution_{a}_to_{b}.csv')
 return result
result={'successive':[details(a,b) for a,b in zip(refs,refs[1:])],'overall':details(base,final)}
old=load(base,'n'); new=load(final,'n')
result['denominator_counterfactual']=[details('e07e2a93f','e07e2a93f+denom'),details('e07e2a93f+denom','60f98ab1d')]
result['nutrients']=[{'nutrient':c,'old':float(old[c].sum()),'new':float(new[c].sum()),'percent':float((new[c].sum()/old[c].sum()-1)*100),'changed_rows':int(old[c].ne(new[c]).sum()),'old_nonzero_rows':int(old[c].ne(0).sum()),'new_nonzero_rows':int(new[c].ne(0).sum())} for c in old.columns]
# Prove the replay agrees with independent current cold result, ignoring API-added v.
actual=pd.read_pickle(IN/'base_nutrition.pkl').droplevel('v').reorder_levels(['i','t']).sort_index()
pdt.assert_frame_equal(new.sort_index(),actual,check_exact=True,check_names=False,check_flags=False)
# Base food/FCT raw parquet fingerprints match the historical fixture.
fixture=json.loads(subprocess.check_output(['git','show','8ec0bab4e:tests/fixtures/uganda_baseline.json']))
assert hashdf(old)==fixture['var/nutrition.parquet']['content_hash']
parents=json.loads((ROOT/'parents'/'summary.json').read_text())
stages=json.loads((ST/'summary.json').read_text())
for parent,previous in [('e07e2a93f^','8ec0bab4e'),('60f98ab1d^','e07e2a93f'),('f98c9762e^','60f98ab1d'),('7cde56579^','f98c9762e')]:
 assert parents[parent]['nutrition']==stages[previous]['nutrition']
 assert parents[parent]['kg_total']==stages[previous]['kg_total']
fullold=json.loads((ROOT/'full-old'/'summary.json').read_text())
assert fullold['live']['nutrition']==stages['8ec0bab4e']['nutrition']
result['exact_endpoint_parent_and_full_old_checks']=True
data=args.data_root/'Uganda'
result['input_fingerprints']={}
for table in ['food_acquired','fct']:
 df=pd.read_parquet(data/'var'/f'{table}.parquet'); recorded=fixture[f'var/{table}.parquet']
 result['input_fingerprints'][table]={'hash':hashdf(df),'historical_hash':recorded['content_hash'],'shape':list(df.shape),'historical_shape':recorded['shape']}
 assert hashdf(df)==recorded['content_hash']
# Exact metric vocabulary change split: volume assumption versus explicit mass.
f0=load(base,'factors'); f1=load('e07e2a93f','factors')
change=f0.kg_per_unit.ne(f1.kg_per_unit)&~(f0.kg_per_unit.isna()&f1.kg_per_unit.isna())
z=flat.loc[change.to_numpy(),['t','j','u']].copy()
z['dkg']=(fa.Quantity*(f1.kg_per_unit-f0.kg_per_unit)).loc[change].to_numpy()
def family(u):
 u=str(u).lower()
 if T._parse_explicit_metric(u,volume_as_mass=False) is not None or u in {k for k in T.KNOWN_METRIC if k not in T._FLUID_UNITS}:return 'explicit_mass'
 if T._parse_explicit_metric(u) is not None or u in T._FLUID_UNITS:return 'volume_as_mass'
 return 'other'
z['family']=z.u.map(family)
for n in fct.index:z[n]=z.dkg*z.j.map(fct.loc[n]).fillna(0)
result['vocabulary_by_family']=z.groupby('family')[['dkg',*fct.index]].sum().reset_index().to_dict('records')
result['vocabulary_units']=z.groupby(['family','u']).agg(rows=('u','size'),kg_delta=('dkg','sum'),energy_delta=('Energy','sum'),vitamin_a_delta=('Vitamin A','sum'),b12_delta=('Vitamin B-12','sum')).reset_index().to_dict('records')
# Existing endpoint/range assumptions; this reports served exposure, no alternative policy.
f=load(final,'factors'); z=flat[['j','u']].copy(); z['kg']=(fa.Quantity*f.kg_per_unit).fillna(0).to_numpy()
z['family']=z.u.map(family);z['rung']=f.KgFactorSource.to_numpy()
z['is_range']=z.u.str.contains(r'\d\s*-\s*\d|above|up to',case=False,regex=True)
for n in fct.index:z[n]=z.kg*z.j.map(fct.loc[n]).fillna(0)
result['current_mass_by_basis']=z.groupby(['rung','family'])[['kg',*fct.index]].sum().reset_index().to_dict('records')
result['range_exposure']=z.loc[z.is_range].groupby('u').agg(rows=('u','size'),kg=('kg','sum'),energy=('Energy','sum'),vitamin_a=('Vitamin A','sum'),b12=('Vitamin B-12','sum')).reset_index().to_dict('records')
(ROOT/'analysis.json').write_text(json.dumps(result,indent=2))
print('NUTRIENTS',json.dumps(result['nutrients'],indent=2));print('VOCAB',json.dumps(result['vocabulary_by_family'],indent=2));print('INPUTS',result['input_fingerprints']);print('TOP',json.dumps(result['overall']['top_contributors'],indent=2))
