import json, os, subprocess
from pathlib import Path
import pandas as pd
import lsms_library
from lsms_library.transformations import seed_kg, nitrogen_kg
repo=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
assert Path(lsms_library.__file__).resolve()==repo/'lsms_library/__init__.py'
rows=[(c,'2000-01','h1','p1',item,'Maize','Kg',qty) for c,qty in [('A',1.),('B',2.)] for item in ['Seed','Urea']]
f=pd.DataFrame(rows,columns=['country','t','i','plot_id','input','j','u','Quantity']).set_index(['country','t','i','plot_id','input','j','u'])
out={}
for fn in [seed_kg,nitrogen_kg]:
 pooled=fn(f)
 out[fn.__name__]={'pooled_rows':len(pooled),'pooled_index_names':pooled.index.names,'pooled_values':pooled.iloc[:,0].tolist(),'separate_values':{c:fn(f.xs(c,level='country')).iloc[:,0].tolist() for c in ['A','B']}}
print(json.dumps({'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'results':out},indent=2))
