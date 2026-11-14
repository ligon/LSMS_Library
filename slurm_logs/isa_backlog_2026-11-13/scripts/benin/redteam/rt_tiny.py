import pickle, sys, json
import pandas as pd
p = pickle.load(open(sys.argv[1], 'rb'))
f = p[p.index.get_level_values('t') == '2018-19'].reset_index()
big = f[f['Quantity_purchased'].astype(float) >= 9999]
print(big[['i', 'input', 'crop', 'u', 'Quantity', 'Quantity_purchased', 'Unit_purchased']].to_string())
print(f['Unit_purchased'].value_counts(dropna=False).to_dict())
print(f['Unit_received_as_gift'].value_counts(dropna=False).to_dict())
g = p[p.index.get_level_values('t') == '2021-22'].reset_index()
print(g['Unit_purchased'].value_counts(dropna=False).to_dict())
# purchase larger than use, same unit
same = f[(f['Unit_purchased'].astype(str) == f['u'].astype(str)) | ((f['Unit_purchased'].astype(str) == 'Kilogramme') & (f['u'].astype(str) == 'Kg'))]
print('same-unit purchases', len(same), 'with Quantity_purchased > Quantity:', int((same['Quantity_purchased'].astype(float) > same['Quantity'].astype(float)).sum()))
