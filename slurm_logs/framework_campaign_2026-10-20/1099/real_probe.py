from pathlib import Path
import hashlib,json,os,subprocess,tempfile
import pandas as pd
import lsms_library
from lsms_library import Country
from lsms_library.paths import data_root
from lsms_library.local_tools import read_parquet_cache_waves
root=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip())
assert Path(lsms_library.__file__).resolve().is_relative_to(root.resolve()),lsms_library.__file__
# Country CONTENTS read first: Senegal livestock lacks v by design; its country
# scheme configures the item-level roster only for 2018-19. No source rewrite.
shared=Path.home()/'.local/share/lsms_library/dvc-cache'
records=[]
with tempfile.TemporaryDirectory(prefix='1099-senegal-') as tmp:
 private=Path(tmp); (private/'dvc-cache').symlink_to(shared,target_is_directory=True)
 os.environ['LSMS_DATA_DIR']=str(private);data_root.cache_clear()
 c=Country('Senegal');p=private/'Senegal/var/livestock.parquet'
 for state in ('cold','partial','all','no_cache'):
  if state=='partial': c.livestock(waves=['2018-19'])
  if state=='all': c.livestock()
  if state=='no_cache': os.environ['LSMS_NO_CACHE']='1'
  before=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
  manifest=read_parquet_cache_waves(p) if p.exists() else None
  df=c.livestock(waves=['2021-22'])
  after=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
  assert df.empty,(state,len(df));assert before==after,(state,'changed country cache')
  records.append(dict(state=state,shape=list(df.shape),index=list(df.index.names),dtypes={k:str(v) for k,v in df.dtypes.items()},cached_waves=manifest,cache_preserved=before==after))
 os.environ.pop('LSMS_NO_CACHE',None)
print(json.dumps(records,indent=2))
(root/'slurm_logs/framework_campaign_2026-10-20/1099/real-probe.json').write_text(json.dumps(records,indent=2)+'\n')
