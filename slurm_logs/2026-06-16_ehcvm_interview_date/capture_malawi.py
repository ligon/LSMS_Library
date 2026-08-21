import sys, pandas as pd
import lsms_library as ll

label = sys.argv[1]  # 'baseline' or 'retrofit'
out = f'slurm_logs/2026-06-16_ehcvm_interview_date/malawi_{label}.pkl'

df = ll.Country('Malawi').interview_date()
df = df.sort_index()
df.to_pickle(out)

print(f'[{label}] shape={df.shape}  index={list(df.index.names)}  cols={list(df.columns)}')
print(f'[{label}] dup index tuples: {df.index.duplicated().sum()}')
datecol = 'Int_t' if 'Int_t' in df.columns else df.columns[0]
print(f'[{label}] NaT in {datecol}: {df[datecol].isna().sum()}')
if 'visit' in df.index.names:
    vlev = df.index.get_level_values('visit')
    print(f'[{label}] visit value_counts:\n{pd.Series(vlev).value_counts().sort_index().to_string()}')
    tlev = df.index.get_level_values('t')
    print(f'[{label}] per-wave visit counts:')
    print(pd.crosstab(pd.Series(tlev, name="t"), pd.Series(vlev, name="visit")).to_string())
print(f'[{label}] saved -> {out}')
