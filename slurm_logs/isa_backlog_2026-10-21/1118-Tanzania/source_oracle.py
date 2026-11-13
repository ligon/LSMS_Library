"""Independent field-binding oracle; raw rows and household keys stay private."""
from pathlib import Path
from collections import Counter
import contextlib,importlib.util,json,os,socket
import pandas as pd
from pandas.testing import assert_frame_equal
import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library.local_tools import get_dataframe,format_id
from lsms_library.transformations import U_UNKNOWN
wt=Path(os.environ['ISA_CHECKOUT']).resolve();ev=Path(os.environ['TASK_EVIDENCE']).resolve()
assert Path.cwd().resolve()==wt and Path(ll.__file__).resolve().is_relative_to(wt)
assert not ev.is_relative_to(wt)
assert countries_root().resolve()==wt/'lsms_library/countries'
try:
 with socket.socket() as s:s.connect(('203.0.113.1',443))
except OSError as e:assert 'forbids network' in str(e)
else:raise AssertionError('offline guard missing')
spec=importlib.util.spec_from_file_location('tanzania_oracle',wt/'lsms_library/countries/Tanzania/_/tanzania.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
@contextlib.contextmanager
def country_cwd():
 old=Path.cwd();os.chdir(wt/'lsms_library/countries/Tanzania/_')
 try:yield
 finally:os.chdir(old)
with country_cwd():cropmap=mod._crop_labels();fertmap=mod._plot_harmonized_codes('harmonize_input')
def val(r,k):return pd.NA if pd.isna(r[k]) else float(r[k])
def positive(x):return pd.notna(x) and x>0
def yn(x):return {1:True,2:False}.get(x,pd.NA) if pd.notna(x) else pd.NA
seed_units={1:'kg',2:'20 liter bucket',3:'small cup',4:'large cup',5:pd.NA}
liquid_units={1:'kg',2:'litre',3:'millilitre',4:pd.NA}
summary={}
for wave,n3,n4,hh,plot in [('2019-20','AG_SEC_3A','AG_SEC_4A','sdd_hhid','plotnum'),('2020-21','ag_sec_3a','ag_sec_4a','y5_hhid','plot_id')]:
 d3=get_dataframe(ev/'held/Tanzania'/wave/'Data'/f'{n3}.dta',convert_categoricals=False)
 d4=get_dataframe(ev/'held/Tanzania'/wave/'Data'/f'{n4}.dta',convert_categoricals=False)
 records=[]
 def add(r,name,crop,q,u,p,pq=pd.NA,improved=pd.NA,channel=''):
  records.append(dict(t=wave,i=format_id(r[hh]),plot_id=format_id(r[plot]),input=name,crop=crop,Quantity=q,u=u,Purchased=p,Quantity_purchased=pq,Improved=improved,channel=channel))
 for _,r in d4.iterrows():
  code=r['cropid'];crop=cropmap.get(int(code),pd.NA) if pd.notna(code) else pd.NA
  q=val(r,'ag4a_10_1')
  if pd.isna(q) and pd.isna(crop):continue
  if pd.isna(crop) and pd.notna(code):crop=f'crop_{int(code)}'
  pq=val(r,'ag4a_10c_1');paid=val(r,'ag4a_12');uc=r['ag4a_10_2'];st=r['ag4a_08']
  add(r,'Seed',crop,q,seed_units.get(uc,pd.NA) if pd.notna(uc) else pd.NA,positive(pq) or positive(paid),pq,{1:True,2:False,3:True}.get(st,pd.NA) if pd.notna(st) else pd.NA,'seed')
 # Keep source-slot order explicit; it determines the unchanged first-unit rule.
 for channel,used,qc,uc,pc,pqc,tc in [
  ('organic','ag3a_41','ag3a_42',None,'ag3a_43','ag3a_44',None),
  ('inorganic1','ag3a_47','ag3a_49',None,'ag3a_51',None,'ag3a_48'),
  ('inorganic2','ag3a_54','ag3a_56',None,'ag3a_58',None,'ag3a_55'),
  ('herbicide','ag3a_60','ag3a_62_1','ag3a_62_2','ag3a_63',None,None),
  ('pesticide','ag3a_65a','ag3a_65b_1','ag3a_65b_2','ag3a_65c',None,None)]:
  for _,r in d3.loc[d3[used].eq(1)].iterrows():
   if channel=='organic':name='Organic Fertilizer'
   elif tc is not None:
    typ=r[tc];name=fertmap.get(int(typ),pd.NA) if pd.notna(typ) else pd.NA
    if pd.isna(name):name='Other Fertilizer'
   else:name=channel.capitalize()
   code=r[uc] if uc else None;unit='kg' if uc is None else (liquid_units.get(code,pd.NA) if pd.notna(code) else pd.NA)
   paid=val(r,pc);p=yn(paid) if channel=='organic' else positive(paid)
   add(r,name,pd.NA,val(r,qc),unit,p,val(r,pqc) if pqc else pd.NA,channel=channel)
 raw=pd.DataFrame(records)
 raw.to_pickle(ev/f'private-source-long-{wave}.pkl')
 key=['t','i','plot_id','input','crop'];grouped=raw.groupby(key,dropna=False,sort=True)
 conflicts=Counter();duplicates=0;duprows=0
 for keyval,g in grouped:
  if len(g)>1:duplicates+=1;duprows+=len(g)
  units=tuple(sorted(set(g['u'].dropna())))
  if len(units)>1:conflicts[(str(keyval[3]),str(keyval[4]),units)]+=1
 oracle=grouped.agg(Quantity=('Quantity',lambda s:s.sum(min_count=1)),u=('u','first'),Purchased=('Purchased','max'),Quantity_purchased=('Quantity_purchased',lambda s:s.sum(min_count=1)),Improved=('Improved','max'))
 oracle=oracle.astype({'Quantity':'Float64','u':'string','Purchased':'boolean','Quantity_purchased':'Float64','Improved':'boolean'})
 missing=oracle['u'].isna()|oracle['u'].astype('string').str.strip().eq('').fillna(False)
 oracle.loc[missing,'u']=U_UNKNOWN
 with country_cwd():actual=mod.plot_inputs_for_wave(wave,d3,d4,dict(hhid=hh,plot=plot,crop='cropid'))
 assert_frame_equal(actual.reset_index().astype({k: 'string' for k in key}).sort_values(key).reset_index(drop=True), oracle.reset_index().astype({k: 'string' for k in key}).sort_values(key).reset_index(drop=True), check_dtype=False)
 assert len(raw)-len(actual)==duprows-duplicates
 # Independent per-field expectations cover every retained source row; no quantity=0 synthesis.
 summary[wave]={'source_long_rows':len(raw),'rows_after_existing_collapse':len(actual),'channel_rows':raw['channel'].value_counts().to_dict(),'source_use_rows_without_quantity':int(raw.Quantity.isna().sum()),'output_without_quantity':int(actual.Quantity.isna().sum()),'duplicate_groups':duplicates,'duplicate_source_rows':duprows,'rows_collapsed':len(raw)-len(actual),'unknown_output_units':int(missing.sum()),'all_bound_fields_match_helper':True,'conflicting_unit_groups':sum(conflicts.values()),'conflict_patterns':[{'input':k[0],'crop':k[1],'units':list(k[2]),'groups':v} for k,v in sorted(conflicts.items())]}
 oracle.to_pickle(ev/f'private-source-expected-{wave}.pkl')
(ev/'source-oracle-summary.json').write_text(json.dumps(summary,indent=2,default=int)+'\n')
print(json.dumps(summary,indent=2,default=int))
