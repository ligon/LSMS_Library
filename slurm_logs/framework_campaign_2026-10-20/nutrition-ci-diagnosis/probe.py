import hashlib,json,os,shutil,subprocess,sys
from pathlib import Path
import numpy as np,pandas as pd,pyarrow as pa
import lsms_library as ll
from lsms_library.paths import data_root,countries_root
repo=Path.cwd().resolve(); private=Path(os.environ['LSMS_DATA_DIR']).resolve()
assert Path(ll.__file__).resolve().parent.parent==repo
assert countries_root().resolve()==repo/'lsms_library/countries'
assert data_root().resolve()==private
assert str(private).startswith('/local/job40255479/nutrition-ci-')
source=Path('/local/job40255479/corpus-combined-data/Uganda')
if not (private/'Uganda').exists(): shutil.copytree(source,private/'Uganda')
if not (private/'dvc-cache').exists(): (private/'dvc-cache').symlink_to('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache',target_is_directory=True)

def digest(df):return hashlib.sha256(pd.util.hash_pandas_object(df,index=True).values.tobytes()).hexdigest()
raw=pd.read_parquet(private/'Uganda/var/nutrition.parquet')
fct=pd.read_parquet(private/'Uganda/var/fct.parquet')
q=ll.Country('Uganda',assume_cache_fresh=True).food_quantities()
q=q[q.index.get_level_values('u').astype(str).str.lower()=='kg'].droplevel('u').sum(axis=1).groupby(['i','t','j']).sum()
common=q.index.unique(level='j').intersection(fct.columns)
x=q.unstack('j').loc[:,common].fillna(0);y=fct.loc[:,common].fillna(0)
n=x@y.T
out={'source':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'versions':{'numpy':np.__version__,'pandas':pd.__version__,'pyarrow':pa.__version__},'raw':{'shape':list(raw.shape),'digest':digest(raw),'index':raw.index.names},'reconstruction':{'digest':digest(n),'shape':list(n.shape),'same_index':n.index.equals(raw.index),'same_columns':n.columns.equals(raw.columns),'changed_cells':int((n!=raw).to_numpy().sum()),'max_abs':float(np.abs(n.to_numpy()-raw.to_numpy()).max())},'variants':{}}
for order in ('C','F'):
 for order2 in ('C','F'):
  arr=np.array(x.to_numpy(),order=order)@np.array(y.to_numpy().T,order=order2)
  frame=pd.DataFrame(arr,index=x.index,columns=y.index)
  out['variants'][order+order2]={'digest':digest(frame),'changed_cells':int((arr!=raw.to_numpy()).sum()),'max_abs':float(np.abs(arr-raw.to_numpy()).max())}
np.savez(private/'inputs.npz',x=x.to_numpy(),y=y.to_numpy(),raw=raw.to_numpy())
x.to_pickle(private/'x.pkl');y.to_pickle(private/'y.pkl');raw.to_pickle(private/'raw.pkl')
Path(sys.argv[1]).write_text(json.dumps(out,indent=2,default=str)+'\n')
print(json.dumps(out,indent=2,default=str))
