"""Country vs Feature for Benin plot_inputs, household_roster, food_acquired; u sentinels."""
import json, sys, warnings
import pandas as pd
warnings.simplefilter('ignore')
import lsms_library as ll
out = {}
c = ll.Country('Benin')
for t in ('plot_inputs', 'household_roster', 'sample', 'food_acquired'):
    cdf = getattr(c, t)()
    f = ll.Feature(t)(['Benin'])
    fdf = f.xs('Benin', level='country')
    ci = set(zip(cdf.index.get_level_values('i'), cdf.index.get_level_values('t').astype(str)))
    fi = set(zip(fdf.index.get_level_values('i'), fdf.index.get_level_values('t').astype(str)))
    r = {'country_rows': len(cdf), 'feature_rows': len(f), 'feature_index_unique': bool(f.index.is_unique),
         'same_i_t': ci == fi, 'feature_index': list(map(str, f.index.names))}
    if t == 'plot_inputs':
        for name, df in (('country', cdf), ('feature', fdf)):
            u = pd.Series(df.index.get_level_values('u').astype(str))
            r[f'{name}_u_Unknown'] = int(u.eq('Unknown').sum())
            r[f'{name}_u_Manquant'] = int(u.eq('Manquant').sum())
        w = cdf.index.get_level_values('t').astype(str) == '2018-19'
        p = cdf[w]
        r['2018-19'] = {
            'rows': int(w.sum()),
            'Purchased_true': int(p['Purchased'].astype('boolean').sum()),
            'Quantity_purchased_n': int(p['Quantity_purchased'].notna().sum()),
            'Quantity_purchased_sum': float(p['Quantity_purchased'].astype(float).sum()),
            'Unit_purchased_n': int(p['Unit_purchased'].notna().sum()),
            'Received_as_gift_true': int(p['Received_as_gift'].astype('boolean').sum()),
            'Quantity_received_as_gift_sum': float(p['Quantity_received_as_gift'].astype(float).sum()),
            'Unit_received_as_gift_n': int(p['Unit_received_as_gift'].notna().sum()),
            'Unit_purchased_values': p['Unit_purchased'].astype(str).value_counts().to_dict(),
        }
        # columns on Feature vs Country
        r['feature_cols'] = list(map(str, f.columns))
        fc = fdf[[x for x in cdf.columns if x in fdf.columns]]
        r['feature_equal_on_columns'] = {
            col: bool(sorted(map(str, fc[col].tolist())) == sorted(map(str, cdf[col].tolist())))
            for col in fc.columns}
    out[t] = r
    print(t, json.dumps(r, default=str)[:1500], flush=True)
json.dump(out, open(sys.argv[1], 'w'), indent=1, default=str)
print('DONE')
