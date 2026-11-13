import sys, pandas as pd
df = pd.read_parquet(sys.argv[1])
t = df.index.get_level_values('t'); u = df.index.get_level_values('u')
d = pd.DataFrame({'t': t, 'kg': (u == 'Kg'),
   'v': pd.Series(df['Value_sold'].values).map(lambda x: 'NaN' if pd.isna(x) else ('0' if x == 0 else 'pos')).values,
   'q': pd.Series(df['Quantity_sold'].values).map(lambda x: 'NaN' if pd.isna(x) else ('0' if x == 0 else 'pos')).values})
print(d.value_counts(['t', 'kg', 'v', 'q']).sort_index().to_string())
