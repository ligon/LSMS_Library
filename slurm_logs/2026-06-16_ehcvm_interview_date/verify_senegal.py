import pandas as pd
import lsms_library as ll

df = ll.Country('Senegal').interview_date()
print('grain :', list(df.index.names))
print('cols  :', list(df.columns))
print('shape :', df.shape, '| dup idx tuples:', df.index.duplicated().sum())
vis = pd.Series(df.index.get_level_values('visit'))
tt  = pd.Series(df.index.get_level_values('t'))
print('per-wave x visit:')
print(pd.crosstab(tt, vis).to_string())
for c in df.columns:
    print(f'  {c}: dtype={df[c].dtype}  NaT={df[c].isna().sum()}')
# backward-compat: collapse visit with first -> should match visit-1 'Interview start'
keys = [n for n in df.index.names if n != 'visit']
first = df.groupby(level=keys).first()
v1 = df.xs(1, level='visit')
print('collapsed rows:', len(first), '| visit==1 rows:', len(v1))
same = first['Interview start'].sort_index().equals(v1['Interview start'].sort_index())
print('first()[Interview start] == visit1 Interview start:', same)
print('\nsample rows:')
print(df.sort_index().head(4).to_string())
