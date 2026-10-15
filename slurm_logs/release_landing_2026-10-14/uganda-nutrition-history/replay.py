from pathlib import Path
import argparse, hashlib, json, os, subprocess, types, sys
import numpy as np
import pandas as pd
import lsms_library as ll
from lsms_library import transformations as live
p=argparse.ArgumentParser(); p.add_argument('--root', type=Path, required=True); p.add_argument('--out', type=Path, required=True); p.add_argument('--refs',nargs='+',required=True); p.add_argument('--input-dir',type=Path,default=Path('/local/job39919092/release-work/control/countries/uganda')); a=p.parse_args()
assert Path.cwd().resolve()==a.root.resolve()
assert Path(ll.__file__).resolve().is_relative_to(a.root.resolve()),ll.__file__
a.out.mkdir(parents=True,exist_ok=True)
INPUT=a.input_dir
fa=pd.read_pickle(INPUT/'base_food_acquired.pkl'); fct=pd.read_pickle(INPUT/'base_fct.pkl')
# Public food input carries v, but derivation ignores it; independent read-only
# endpoint comparison below checks this choice rather than relying on the claim.
def fingerprint(df):
 return {'shape':list(df.shape),'index_names':list(df.index.names),'columns':sorted(df.columns),'dtypes':{str(k):str(v) for k,v in df.dtypes.items()},'content_hash':hashlib.sha256(pd.util.hash_pandas_object(df,index=True).values.tobytes()).hexdigest()}
def nutrition(q):
 mask=q.index.get_level_values('u').astype(str).str.lower()=='kg'
 kg=q[mask].droplevel('u').sum(axis=1).groupby(['i','t','j']).sum()
 common=kg.index.unique(level='j').intersection(fct.columns)
 wide=kg.unstack('j').loc[:,common].fillna(0)
 density=fct.loc[:,common].fillna(0)
 n=wide@density.T
 n.index.name='Nutrient'
 return n
result={}
for ref in a.refs:
 if ref=='live': T=live; sha=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
 else:
  source_ref=ref.split('+')[0]
  sha=subprocess.check_output(['git','rev-parse',source_ref],text=True).strip()
  code=subprocess.check_output(['git','show',f'{sha}:lsms_library/transformations.py'],text=True)
  if ref.endswith('+denom'):
   old="po = v_infer[price].groupby(index + ['u']).median().median(axis=1)"
   assert code.count(old)==1
   code=code.replace(old,"po = v_infer[price].divide(v_infer[quantity], axis=0).groupby(index + ['u']).median().median(axis=1)")
  T=types.ModuleType('lsms_library.replay_'+sha); T.__package__='lsms_library'; T.__file__=f'{sha}:lsms_library/transformations.py'; sys.modules[T.__name__]=T
  exec(compile(code,T.__file__,'exec'),T.__dict__)
 q=T.food_quantities_from_acquired(fa,units='kgs'); n=nutrition(q)
 q.to_pickle(a.out/f'{ref}_q.pkl'); n.to_pickle(a.out/f'{ref}_n.pkl')
 if hasattr(T,'food_kg_factors'): factors=T.food_kg_factors(fa)
 else:
  fac=T._get_kg_factors(fa); unit=fa.index.get_level_values('u').astype(str).str.lower()
  factors=pd.DataFrame({'kg_per_unit':unit.map(fac)},index=fa.index)
 factors.to_pickle(a.out/f'{ref}_factors.pkl')
 rec={'sha':sha,'nutrition':fingerprint(n),'kg_total':float(q.loc[q.index.get_level_values('u')=='kg','Quantity'].sum()),'nutrients':n.sum().to_dict(),'factor_sources':factors.attrs.get('kg_factor_sources',{})}
 result[ref]=rec
 print(ref,rec['nutrition']['content_hash'],n.shape,rec['kg_total'],flush=True)
 (a.out/'summary.json').write_text(json.dumps(result,indent=2))
