"""Check the actual invariance test against measured BLAS outputs privately."""
import hashlib,json,os,subprocess,sys
from pathlib import Path
import numpy as np,pandas as pd
from threadpoolctl import threadpool_info
import lsms_library as ll
from lsms_library.paths import countries_root,data_root
from tests import test_uganda_invariance as inv
repo=Path.cwd().resolve();private=Path(os.environ['LSMS_DATA_DIR']).resolve()
assert repo==Path(os.environ['LSMS_EXPECTED_ROOT']).resolve()
assert Path(ll.__file__).resolve().parent.parent==repo
assert Path(inv.__file__).resolve().parent.parent==repo
assert countries_root().resolve()==repo/'lsms_library/countries'
assert private==data_root().resolve() and private.is_relative_to(Path('/local'))
assert private != repo and not private.is_relative_to(repo)
inputs=Path(sys.argv[1]);out=Path(sys.argv[2]);x=pd.read_pickle(inputs/'x.pkl');y=pd.read_pickle(inputs/'y.pkl');raw=pd.read_pickle(inputs/'raw.pkl')
n=x@y.T;root=private/'Uganda';target=root/'var/nutrition.parquet';target.parent.mkdir(parents=True,exist_ok=True)
n.to_parquet(target)
inv.test_parquet_matches_baseline(root,ll.Country('Uganda').waves,'var/nutrition.parquet')
diff=n.to_numpy()-raw.to_numpy();mask=diff!=0
changed=n.copy();changed.iloc[0,0]+=1.0;changed.to_parquet(target)
try:inv.test_parquet_matches_baseline(root,ll.Country('Uganda').waves,'var/nutrition.parquet')
except AssertionError as e:assert 'content hash mismatch' in str(e)
else:raise AssertionError('deliberate changed value was accepted')
n.to_parquet(target)
d={'runtime_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'package':str(Path(ll.__file__).resolve()),'cwd':str(repo),'countries_root':str(countries_root()),'private_data_root':str(private),'coretype':os.environ['OPENBLAS_CORETYPE'],'blas':[{k:v for k,v in info.items() if k!='filepath'} for info in threadpool_info()],'exact_hash':inv._fingerprint(n)['content_hash'],'real_invariance_passed':True,'deliberately_changed_value_rejected':True,'same_index':n.index.equals(raw.index),'same_columns':n.columns.equals(raw.columns),'same_dtypes':n.dtypes.equals(raw.dtypes),'changed_cells_vs_primary':int(mask.sum()),'changed_rows_vs_primary':int(mask.any(axis=1).sum()),'max_abs_vs_primary':float(np.abs(diff).max()),'input_sha256':{name:hashlib.sha256((inputs/name).read_bytes()).hexdigest() for name in ['x.pkl','y.pkl','raw.pkl']}}
out.write_text(json.dumps(d,indent=2)+'\n')
