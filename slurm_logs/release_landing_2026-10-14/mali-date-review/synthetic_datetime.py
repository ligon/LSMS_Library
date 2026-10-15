"""Read-only synthetic control: no Country/Feature calls or survey/cache reads."""
from pathlib import Path
import hashlib,json,subprocess
import pandas as pd
import lsms_library as ll
from lsms_library.country import _enforce_declared_dtypes, _enforce_canonical_dtypes
ROOT=Path('/local/job39919092/release-work/framework/uganda-assets')
OUT=Path('/local/job39919092/release-work/control/countries/mali-date-review')
assert Path(ll.__file__).resolve().parent==ROOT/'lsms_library'
result={'source':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'pandas':pd.__version__,'scope':'synthetic only; pure dtype functions; no API/cache operations','cases':{}}
strings=['2014-07-18 00:00:00','2017-10-22 00:00:00','2018-11-14','2021-11-15']
for label,values in [('datetime_first',strings),('date_first',list(reversed(strings)))]:
 frame=pd.DataFrame({'Int_t':values})
 default=pd.to_datetime(frame['Int_t'],errors='coerce')
 per_style=pd.concat([pd.to_datetime(frame['Int_t'].iloc[:2],errors='coerce'),pd.to_datetime(frame['Int_t'].iloc[2:],errors='coerce')]).sort_index()
 mixed=pd.to_datetime(frame['Int_t'],errors='coerce',format='mixed')
 declared=frame.copy();_enforce_declared_dtypes(declared,{'Int_t':'datetime'})
 canonical=frame.copy();_enforce_canonical_dtypes(canonical,'interview_date')
 assert default.isna().sum()==2 and per_style.notna().all() and mixed.notna().all()
 assert declared['Int_t'].isna().equals(default.isna())
 result['cases'][label]={'default_valid':int(default.notna().sum()),'per_style_valid':int(per_style.notna().sum()),'mixed_valid':int(mixed.notna().sum()),'declared_valid':int(declared['Int_t'].notna().sum()),'canonical_valid':int(canonical['Int_t'].notna().sum()),'after_declared_dropna_rows':len(declared.dropna(how='all')),'no_visit_axis':True}
# Genuine datetime objects do not impose a single inferred string format.
objects=pd.Series([pd.Timestamp(s)for s in strings],dtype=object)
result['typed_object_valid']=int(pd.to_datetime(objects,errors='coerce').notna().sum())
assert result['typed_object_valid']==4
(OUT/'synthetic-result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
