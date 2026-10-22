import os,json,pathlib,subprocess,time,hashlib
import pandas as pd
import pyarrow.parquet as pq
import lsms_library as ll
from lsms_library.paths import data_root,countries_root
TOP=pathlib.Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert pathlib.Path(ll.__file__).resolve().is_relative_to(TOP)
assert countries_root().resolve()==TOP/'lsms_library/countries'
out=pathlib.Path(os.environ['EVIDENCE_OUT']);out.mkdir(parents=True,exist_ok=True)
country=ll.Country('Uganda')
results={}
for table in ['food_acquired','sample','earnings','enterprise_income','income','fct','nutrition']:
 started=time.monotonic(); df=getattr(country,table)(); df.to_parquet(out/f'served-{table}.parquet')
 raw=data_root('Uganda')/'var'/f'{table}.parquet'
 if raw.exists():
  import shutil
  shutil.copy2(raw,out/f'raw-{table}.parquet')
 results[table]={'shape':list(df.shape),'index':list(df.index.names),'columns':list(df.columns.astype(str)),'seconds':round(time.monotonic()-started,3),'stored_hash':(pq.read_metadata(raw).metadata or {}).get(b'lsms_cache_hash',b'').decode() if raw.exists() else None}
 (out/'build-summary.json').write_text(json.dumps(results,indent=2)+'\n')
 print(table,results[table]['shape'],results[table]['seconds'],flush=True)
