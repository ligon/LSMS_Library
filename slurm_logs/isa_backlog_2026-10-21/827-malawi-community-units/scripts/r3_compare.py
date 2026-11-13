"""3be88e0ad (r2) vs r3: community_prices cell-by-cell against the r3 raw oracle.
argv: checkout, r2.parquet, r3.parquet, oracle_keys_r3.parquet, r3_attrs.json, out.json. Aggregates only."""
import os, sys, json
import pandas as pd
checkout, bp, np_, okp, attrs_p, out = sys.argv[1:7]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep)
sys.path.insert(0, os.path.join(checkout, 'lsms_library/countries/Malawi/_'))
import malawi
KEY = 'Malawi::community_prices::struck-item-one-unit'
CIG = malawi._price_item_code_map()[33]
base = pd.read_parquet(bp).reset_index(); new = pd.read_parquet(np_).reset_index()
imap = malawi._price_item_code_map()
ok = pd.read_parquet(okp); ok['j'] = ok['code'].astype(int).map(imap).astype('string')
res = {'rows_r2': len(base), 'rows_r3': len(new), 'cigarette_j': CIG,
       'columns_equal': list(base.columns) == list(new.columns)}
m = base.merge(new, on=['t', 'v', 'j'], how='outer', suffixes=('_b', '_n'), indicator=True, validate='one_to_one')
res['join'] = {k: int(v) for k, v in m['_merge'].value_counts().items()}
def same(a, b):
    return ((a == b) | (a.isna() & b.isna())).fillna(False).astype(bool)
for col in ['u', 'Price', 'Available', 'NumberOfUnits', 'Derivation']:
    res[f'{col}_differ'] = int((~same(m[f'{col}_b'], m[f'{col}_n'])).sum())
dn = ~same(m['NumberOfUnits_b'], m['NumberOfUnits_n']); dd = ~same(m['Derivation_b'], m['Derivation_n'])
res['nunits_and_derivation_differ_on_same_rows'] = bool((dn == dd).all())
d = m[dn]
res['changed_all_cigarette'] = bool((d['j'] == CIG).all())
res['changed_from'] = {str(k): int(v) for k, v in d['NumberOfUnits_b'].value_counts(dropna=False).items()}
res['changed_to'] = {str(k): int(v) for k, v in d['NumberOfUnits_n'].value_counts(dropna=False).items()}
res['changed_by_t'] = {k: int(v) for k, v in d['t'].value_counts().sort_index().items()}
res['changed_u'] = {k: int(v) for k, v in d['u_n'].value_counts().items()}
cig = new[new['j'] == CIG]
res['cigarette_rows_r3'] = len(cig); res['cigarette_priced_r3'] = int(cig['Price'].notna().sum())
res['cigarette_nunits_notna_r3'] = int(cig['NumberOfUnits'].notna().sum()); res['cigarette_keyed_r3'] = int(cig['Derivation'].notna().sum())
res['cigarette_u_r3'] = {k: int(v) for k, v in cig['u'].value_counts().items()}
keyed = new['Derivation'].eq(KEY).fillna(False)
res['keyed_total_r3'] = int(keyed.sum()); res['keyed_by_t_r3'] = {k: int(v) for k, v in new.loc[keyed, 't'].value_counts().sort_index().items()}
res['keyed_by_u_r3'] = {k: int(v) for k, v in new.loc[keyed, 'u'].value_counts().items()}
kk = set(map(tuple, new.loc[keyed, ['t', 'v', 'j']].astype(str).values)); ko = set(map(tuple, ok[['t', 'v', 'j']].astype(str).values))
res['oracle_rows'] = len(ko); res['keyed_eq_oracle'] = kk == ko
res['nunits_eq1_r2'] = int((base['NumberOfUnits'] == 1).sum()); res['nunits_eq1_r3'] = int((new['NumberOfUnits'] == 1).sum())
res['price_sum_r2'] = float(base['Price'].sum()); res['price_sum_r3'] = float(new['Price'].sum())
res['attrs_derivations_r3'] = json.load(open(attrs_p)).get('derivations')
json.dump(res, open(out, 'w'), indent=1); print(json.dumps(res, indent=1))
