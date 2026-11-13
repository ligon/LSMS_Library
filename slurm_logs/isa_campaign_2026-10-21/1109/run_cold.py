import json,os,time,warnings,subprocess
from collections import Counter
from pathlib import Path
import pandas as pd
import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library.local_tools import data_root
from lsms_library.transformations import nitrogen_kg
checkout=Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]).resolve()
assert Path.cwd().resolve()==checkout
assert Path(ll.__file__).resolve()==checkout/'lsms_library/__init__.py'
assert countries_root().resolve()==checkout/'lsms_library/countries'
root=Path(os.environ['LSMS_DATA_DIR']).resolve();assert data_root().resolve()==root
assert not (root/'Malawi/var/plot_inputs.parquet').exists()
start=time.monotonic()
with warnings.catch_warnings(record=True) as caught:
 warnings.simplefilter('always')
 frame=ll.Country('Malawi').plot_inputs()
 n=nitrogen_kg(frame)
frame.to_pickle(root/'cold-served.pkl');n.to_pickle(root/'cold-nitrogen.pkl')
org=frame.index.get_level_values('input').astype(str)=='Organic Fertilizer'
summary={'checkout':str(checkout),'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'shape':list(frame.shape),'organic_rows':int(org.sum()),'organic_quantity_null':int(frame.loc[org,'Quantity'].isna().sum()),'organic_quantity_zero':int(frame.loc[org,'Quantity'].eq(0).sum()),'quantity_dtype':str(frame.Quantity.dtype),'nitrogen_shape':list(n.shape),'nitrogen_total':float(n.Nitrogen_kg.sum()),'warnings':dict(Counter(type(x.message).__name__ for x in caught)),'seconds':round(time.monotonic()-start,2),'wave_counts':{str(k):int(v)for k,v in frame.groupby(level='t',dropna=False).size().items()},'module_sha256':__import__('hashlib').sha256((checkout/'lsms_library/countries/Malawi/_/malawi.py').read_bytes()).hexdigest(),'in_tree_parquets':len(list((countries_root()/'Malawi').rglob('*.parquet')))}
(root/'cold-summary.json').write_text(json.dumps(summary,indent=2))
assert summary['organic_rows']==11075,summary
assert summary['in_tree_parquets']==0,summary
print(json.dumps(summary))
