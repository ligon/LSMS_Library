import glob, yaml, os
import pandas as pd
from lsms_library.local_tools import get_dataframe

specs = [
 ('Benin','2018-19','s00_me_ben2018.dta'),
 ('Burkina_Faso','2018-19','s00_me_bfa2018.dta'),
 ('Burkina_Faso','2021-22','s00_me_bfa2021.dta'),
 ('CotedIvoire','2018-19','Menage/s00_me_CIV2018.dta'),
 ('Guinea-Bissau','2018-19','s00_me_gnb2018.dta'),
 ('Mali','2018-19','s00_me_mli2018.dta'),
 ('Mali','2021-22','s00_me_mli2021.dta'),
 ('Niger','2018-19','s00_me_ner2018.dta'),
 ('Niger','2021-22','s00_me_ner2021.dta'),
 ('Senegal','2018-19','s00_me_sen2018.dta'),
 ('Senegal','2021-22','s00_me_sen2021.dta'),
 ('Togo','2018','../Data1/s00_me_tgo2018.dta'),
]
root='lsms_library/countries'
print(f"{'wave':26s} {'n':>6} {'q23a_nn':>7} {'q24a_nn':>7} {'median_gap_days':>15}")
for c,w,fn in specs:
    base=f'{root}/{c}/{w}'
    path=os.path.normpath(f'{base}/Data/{fn}') if not fn.startswith('..') else os.path.normpath(f'{base}/{fn}')
    try:
        df=get_dataframe(path)
        a=pd.to_datetime(df['s00q23a'],errors='coerce')   # visit1 start
        c2=pd.to_datetime(df['s00q24a'],errors='coerce') if 's00q24a' in df.columns else pd.Series([pd.NaT]*len(df))
        gap=(c2-a).dt.days
        med=gap.median()
        print(f"{c+'/'+w:26s} {len(df):>6} {a.notna().sum():>7} {c2.notna().sum():>7} {med!s:>15}")
    except Exception as e:
        print(f"{c+'/'+w:26s} FAILED {type(e).__name__}: {str(e)[:50]}")
