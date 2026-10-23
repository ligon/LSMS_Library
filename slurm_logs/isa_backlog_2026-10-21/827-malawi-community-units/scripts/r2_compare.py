"""PR head (8dea4ecf2) vs r2 head: community_prices cell-by-cell, against the raw oracle.
argv: checkout, base.parquet, new.parquet, oracle_keys.parquet, new_attrs.json, out.json. Aggregates only."""
import os, sys, json
import pandas as pd
checkout, bp, np_, okp, attrs_p, out = sys.argv[1:7]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep)
sys.path.insert(0, os.path.join(checkout, 'lsms_library/countries/Malawi/_'))
import malawi
KEY = 'Malawi::community_prices::struck-item-one-unit'
base = pd.read_parquet(bp).reset_index(); new = pd.read_parquet(np_).reset_index()
imap = malawi._price_item_code_map()
ok = pd.read_parquet(okp); ok['j'] = ok['code'].astype(int).map(imap).astype('string')
res = {'rows_base': len(base), 'rows_new': len(new),
       'columns_base': list(base.columns), 'columns_new': list(new.columns)}
m = base.merge(new, on=['t', 'v', 'j'], how='outer', suffixes=('_b', '_n'), indicator=True, validate='one_to_one')
res['join'] = {k: int(v) for k, v in m['_merge'].value_counts().items()}
def same(a, b):
    return ((a == b) | (a.isna() & b.isna())).fillna(False).astype(bool)
for col in ['u', 'Price', 'Available', 'NumberOfUnits']:
    res[f'{col}_differ'] = int((~same(m[f'{col}_b'], m[f'{col}_n'])).sum())
diff = ~same(m['NumberOfUnits_b'], m['NumberOfUnits_n'])
d = m[diff]
res['nunits_changed_from_na'] = bool(d['NumberOfUnits_b'].isna().all())
res['nunits_changed_to'] = {str(k): int(v) for k, v in d['NumberOfUnits_n'].value_counts(dropna=False).items()}
res['nunits_changed_by_t'] = {k: int(v) for k, v in d['t'].value_counts().sort_index().items()}
keys_changed = set(map(tuple, d[['t', 'v', 'j']].astype(str).values))
keys_oracle = set(map(tuple, ok[['t', 'v', 'j']].astype(str).values))
res['oracle_rows'] = len(keys_oracle)
res['changed_eq_oracle'] = keys_changed == keys_oracle
res['changed_minus_oracle'] = len(keys_changed - keys_oracle); res['oracle_minus_changed'] = len(keys_oracle - keys_changed)
keyed = new['Derivation'] == KEY
res['derivation_values'] = {str(k): int(v) for k, v in new['Derivation'].value_counts(dropna=False).items()}
res['derivation_by_t'] = {k: int(v) for k, v in new.loc[keyed.fillna(False), 't'].value_counts().sort_index().items()}
keys_keyed = set(map(tuple, new.loc[keyed.fillna(False), ['t', 'v', 'j']].astype(str).values))
res['keyed_eq_oracle'] = keys_keyed == keys_oracle
res['derivation_dtype'] = str(pd.read_parquet(np_)['Derivation'].dtype)
res['nunits_eq1_new'] = int((new['NumberOfUnits'] == 1).sum()); res['nunits_eq1_base'] = int((base['NumberOfUnits'] == 1).sum())
res['keyed_by_u'] = {k: int(v) for k, v in new.loc[keyed.fillna(False), 'u'].value_counts().items()}
res['keyed_price_zero'] = int((keyed.fillna(False) & (new['Price'] == 0)).sum())
res['keyed_available'] = {str(k): int(v) for k, v in new.loc[keyed.fillna(False), 'Available'].value_counts(dropna=False).items()}
res['price_sum_base'] = float(base['Price'].sum()); res['price_sum_new'] = float(new['Price'].sum())
res['attrs_derivations'] = json.load(open(attrs_p)).get('derivations')
json.dump(res, open(out, 'w'), indent=1)
print(json.dumps(res, indent=1))
