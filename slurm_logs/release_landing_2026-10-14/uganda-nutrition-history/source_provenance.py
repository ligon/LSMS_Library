"""Read recorded FCT sources, without choosing between conflicting claims."""
from pathlib import Path
import argparse,json
import pandas as pd
import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--inputs',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
assert Path.cwd().resolve()==a.root.resolve()
assert Path(ll.__file__).resolve().is_relative_to(a.root.resolve()),ll.__file__
org=a.root/'lsms_library/countries/Uganda/_/nutrition.org'
cross=df_from_orgfile(org,name='i_to_fct',encoding='ISO-8859-1')
cross['Preferred Label']=cross['Preferred Label'].str.strip()
fct=df_from_orgfile(org,name='fct',encoding='ISO-8859-1')
fct['FCT Code']=pd.to_numeric(fct['FCT Code'],errors='coerce');fct=fct.set_index('FCT Code')
density=pd.read_pickle(a.inputs/'base_fct.pkl')
foods=['Sweet Potatoes (fresh)','Sweet Potatoes (dry)','Fresh Fish','Dry/Smoked Fish','Eggs','Milk (fresh)','Cooking Oil'];rows=[]
for label in foods:
 group=cross[cross['Preferred Label'].eq(label)]
 code=float(group['FCT Code'].iloc[-1]);row=fct.loc[code]
 rec={'food':label,'FCT_code':code,'source_description':row['food_description'],'source':row['fct_source'],'source_code':str(row['fct_source_code']),'source_notes':str(row['fct_source_descr']),'Energy_per_kg':float(density.loc['Energy',label]),'Vitamin_A_per_kg':float(density.loc['Vitamin A',label]),'B12_per_kg':float(density.loc['Vitamin B-12',label]),'source_numeric_A_per_100g':float(row['vit_a_rae_mcg'])}
 assert rec['Vitamin_A_per_kg']==rec['source_numeric_A_per_100g']*10
 rows.append(rec)
a.out.write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(rows,indent=2))
