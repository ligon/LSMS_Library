"""Aggregate-only comparison of fixed nutrition operands across BLAS kernels."""
import argparse,hashlib,json,os,subprocess,sys
from pathlib import Path

def digest(df):
 import pandas as pd
 return hashlib.sha256(pd.util.hash_pandas_object(df,index=True).values.tobytes()).hexdigest()

p=argparse.ArgumentParser();p.add_argument('--inputs',type=Path,required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--worker',choices=['Haswell','SkylakeX'])
a=p.parse_args()
if a.worker:
 import numpy as np,pandas as pd
 from threadpoolctl import threadpool_info
 x=pd.read_pickle(a.inputs/'x.pkl');y=pd.read_pickle(a.inputs/'y.pkl');raw=pd.read_pickle(a.inputs/'raw.pkl')
 n=x@y.T;diff=n.to_numpy()-raw.to_numpy();changed=diff!=0
 # Nutrition is finite and nonnegative; uint64 subtraction measures float64 ULPs.
 bits=n.to_numpy().view(np.uint64);oldbits=raw.to_numpy().view(np.uint64)
 ulps=np.maximum(bits,oldbits)-np.minimum(bits,oldbits)
 info=[{k:v for k,v in t.items() if k!='filepath'} for t in threadpool_info()]
 d={'requested_kernel':a.worker,'blas':info,'digest':digest(n),'same_index':n.index.equals(raw.index),'same_columns':n.columns.equals(raw.columns),'same_dtypes':n.dtypes.equals(raw.dtypes),'changed_rows':int(changed.any(axis=1).sum()),'changed_cells':int(changed.sum()),'columns':{str(c):int(changed[:,i].sum()) for i,c in enumerate(n.columns) if changed[:,i].any()},'changed_wave_counts':{str(k):int(v) for k,v in n.loc[changed.any(axis=1)].groupby('t').size().items()},'max_abs':float(np.abs(diff).max()),'max_relative':float(np.divide(np.abs(diff),np.abs(raw.to_numpy()),out=np.zeros(diff.shape),where=raw.to_numpy()!=0).max()),'max_ulps':int(ulps.max()),'sorted_digest':digest(n.sort_index()),'finite_all':bool(np.isfinite(n.to_numpy()).all()),'nonnegative_all':bool((n.to_numpy()>=0).all()),'input_file_hashes':{name:hashlib.sha256((a.inputs/name).read_bytes()).hexdigest() for name in ['x.pkl','y.pkl','raw.pkl']}}
 a.out.write_text(json.dumps(d,indent=2)+'\n')
else:
 a.out.mkdir(exist_ok=True,parents=True)
 results=[]
 for label,kernel in [('initial','SkylakeX'),('counterfactual','Haswell'),('restored','SkylakeX')]:
  output=a.out/(label+'.json');env=os.environ.copy();env.update(OPENBLAS_CORETYPE=kernel,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1')
  subprocess.run([sys.executable,__file__,'--inputs',str(a.inputs),'--out',str(output),'--worker',kernel],env=env,check=True)
  results.append(json.loads(output.read_text()))
 assert results[0]['digest']==results[2]['digest']=='12d25f6ec6d069af7d3cff703b5091bc4770944eaafacf2e87bc2f058f450e3a'
 assert results[1]['digest']=='f3d3c34d3fad5253d6b026d3d397d2895bc92ce3124dbdd16a92da1553d89fd6'
 assert results[0]['input_file_hashes']==results[1]['input_file_hashes']==results[2]['input_file_hashes']
 aggregate={'source':'852ac2bfcd5f798533f56e7efa1f77db80c0138f','outcome':'exact CI hash reproduced by BLAS kernel only; reversible','shape':[23777,15],'sequence':results,'ci_actual_runner_kernel':'not logged; matching output reconstructed locally, not directly observed on CI','ci_raw_artifacts_available':False}
 (a.out/'comparison.json').write_text(json.dumps(aggregate,indent=2)+'\n');print(json.dumps(aggregate,indent=2))
