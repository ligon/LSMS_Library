import pandas as pd
from lsms_library.local_tools import get_dataframe
specs=[('Senegal/2018-19','Senegal/2018-19/Data/s00_me_sen2018.dta'),
 ('Senegal/2021-22','Senegal/2021-22/Data/s00_me_sen2021.dta'),
 ('Benin/2018-19','Benin/2018-19/Data/s00_me_ben2018.dta'),
 ('Burkina/2018-19','Burkina_Faso/2018-19/Data/s00_me_bfa2018.dta'),
 ('Burkina/2021-22','Burkina_Faso/2021-22/Data/s00_me_bfa2021.dta'),
 ('CotedIvoire/2018-19','CotedIvoire/2018-19/Data/Menage/s00_me_CIV2018.dta'),
 ('GuineaBissau/2018-19','Guinea-Bissau/2018-19/Data/s00_me_gnb2018.dta'),
 ('Mali/2018-19','Mali/2018-19/Data/s00_me_mli2018.dta'),
 ('Mali/2021-22','Mali/2021-22/Data/s00_me_mli2021.dta'),
 ('Niger/2018-19','Niger/2018-19/Data/s00_me_ner2018.dta'),
 ('Niger/2021-22','Niger/2021-22/Data/s00_me_ner2021.dta'),
 ('Togo/2018','Togo/2018/Data1/s00_me_tgo2018.dta')]
root='lsms_library/countries/'
print(f"{'wave':20s} {'n':>6} | nonnull start per visit (q23a/q24a/q25a/q26a) | q22 'visits needed' dist")
for name,p in specs:
    try:
        df=get_dataframe(root+p)
        def nn(c): return int(df[c].notna().sum()) if c in df.columns else '-'
        v=[nn(f's00q2{k}a') for k in (3,4,5,6)]
        q22=df['s00q22'].dropna().astype('Int64').value_counts().sort_index().to_dict() if 's00q22' in df.columns else 'NO q22'
        print(f"{name:20s} {len(df):>6} | v1={v[0]} v2={v[1]} v3={v[2]} v4={v[3]} | q22={q22}")
    except Exception as e:
        print(f"{name:20s} FAILED {type(e).__name__}: {str(e)[:40]}")
