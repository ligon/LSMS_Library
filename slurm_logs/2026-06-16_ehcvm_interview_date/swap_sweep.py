import pandas as pd
from lsms_library.local_tools import get_dataframe
specs=[('Senegal/2021-22','lsms_library/countries/Senegal/2021-22/Data/s00_me_sen2021.dta'),
 ('Burkina/2018-19','lsms_library/countries/Burkina_Faso/2018-19/Data/s00_me_bfa2018.dta'),
 ('Burkina/2021-22','lsms_library/countries/Burkina_Faso/2021-22/Data/s00_me_bfa2021.dta'),
 ('CotedIvoire/2018-19','lsms_library/countries/CotedIvoire/2018-19/Data/Menage/s00_me_CIV2018.dta'),
 ('GuineaBissau/2018-19','lsms_library/countries/Guinea-Bissau/2018-19/Data/s00_me_gnb2018.dta'),
 ('Mali/2021-22','lsms_library/countries/Mali/2021-22/Data/s00_me_mli2021.dta'),
 ('Niger/2018-19','lsms_library/countries/Niger/2018-19/Data/s00_me_ner2018.dta'),
 ('Niger/2021-22','lsms_library/countries/Niger/2021-22/Data/s00_me_ner2021.dta'),
 ('Togo/2018','lsms_library/countries/Togo/2018/Data1/s00_me_tgo2018.dta')]
print(f"{'wave':22s} {'n':>6} {'lbl%':>5} {'swap%':>5} {'v2dur_lbl':>12} {'v2dur_swap':>12}  top")
for name,p in specs:
    try:
        df=get_dataframe(p)
        A=pd.to_datetime(df['s00q23a'],errors='coerce');B=pd.to_datetime(df['s00q23b'],errors='coerce')
        C=pd.to_datetime(df['s00q24a'],errors='coerce');D=pd.to_datetime(df['s00q24b'],errors='coerce')
        m=A.notna()&B.notna()&C.notna()&D.notna();A,B,C,D=A[m],B[m],C[m],D[m];n=len(A)
        lbl=100*((A<=B)&(B<=C)&(C<=D)).mean();swp=100*((A<=C)&(C<=B)&(B<=D)).mean()
        M=pd.DataFrame({'A':A,'B':B,'C':C,'D':D}).reset_index(drop=True)
        top=M.apply(lambda r:''.join(sorted('ABCD',key=lambda k:r[k])),axis=1).value_counts().head(1)
        print(f"{name:22s} {n:>6} {lbl:5.1f} {swp:5.1f} {str((D-C).median()):>12} {str((D-B).median()):>12}  {top.index[0]}({top.iloc[0]})")
    except Exception as e:
        print(f"{name:22s} FAILED {type(e).__name__}: {str(e)[:40]}")
