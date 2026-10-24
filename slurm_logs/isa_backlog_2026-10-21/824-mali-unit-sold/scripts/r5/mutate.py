"""Apply one named mutation to a checkout.  Adapted from the red-team review
(mali-sales-review/scripts/mutate.py, 2026-10-23): same mutants, anchors
updated for the follow-up commit, plus I/J/K/L.  usage: mutate.py <checkout> <name>"""
import sys
from pathlib import Path
root, name = Path(sys.argv[1]), sys.argv[2]
MALI = root / 'lsms_library/countries/Mali/_/mali.py'
W14 = root / 'lsms_library/countries/Mali/2014-15/_/crop_production.py'
YML = root / 'lsms_library/countries/Mali/_/derivations.yml'
SALE = "    sale = derive_tree_sale_window(quantity(v['qs']), _mask_value_code(src[v['v']]))\n"
M = "months.astype('Float64').to_numpy(dtype='float64', na_value=np.nan)"
JOIN = "    out[tree & sold] = MULTI_SEP.join(sorted((DERIVATION_TREES,\n                                              DERIVATION_TREE_SALES)))\n"
MUT = {
    'A_rescale_both': (MALI, SALE, SALE + f"    sale = sale.mul({M}, axis=0)\n"),
    'A2_rescale_value': (MALI, SALE, SALE + f"    sale['Value_sold'] = sale['Value_sold'] * {M}\n"),
    'A3_rescale_qty': (MALI, SALE, SALE + f"    sale['Quantity_sold'] = sale['Quantity_sold'] * {M}\n"),
    'B_drop_sale_key': (MALI, "    out[tree & sold] = MULTI_SEP.join(", "    out[tree & sold & False] = MULTI_SEP.join("),
    'B2_drop_harvest_key_on_sold': (MALI, JOIN, "    out[tree & sold] = DERIVATION_TREE_SALES\n"),
    'B3_unsorted_join': (MALI, JOIN, "    out[tree & sold] = MULTI_SEP.join((DERIVATION_TREES, DERIVATION_TREE_SALES))\n"),
    'C_sale_key_all_trees': (MALI, "    out[tree & sold] = MULTI_SEP.join(", "    out[tree] = MULTI_SEP.join("),
    'C2_sale_key_on_zero': (MALI, "(df['Quantity_sold'] > 0) | (df['Value_sold'] > 0)",
                            "(df['Quantity_sold'] >= 0) | (df['Value_sold'] >= 0)"),
    'D_inputs_drop_unit': (MALI, "    '2014-15': ['s3bq01', 's3bq03', 's3bq09', 's3bq13a', 's3bq13b', 's3bq14'],",
                           "    '2014-15': ['s3bq01', 's3bq03', 's3bq09', 's3bq13a', 's3bq14'],"),
    'E_inputs_all_roster': (MALI, "    src = producing_trees(src, wave)\n", ""),
    'F_yml_drop_value_col': (YML, "  columns: [Quantity_sold, Value_sold]\n", "  columns: [Quantity_sold]\n"),
    'G_unmask_value_code': (MALI, "    return x.mask(x.isin(_VALUE_CODES))\n", "    return x\n"),
    'H_blank_edge_sales': (MALI, SALE, SALE + "    sale.loc[(months.isna() | months.eq(0)).fillna(True).to_numpy(dtype=bool), :] = np.nan\n"),
    # new in round 5
    'I_unmask_field_code': (W14, "    'Value_sold': _mask_value_code(s3a['s3aq24']),\n", "    'Value_sold': s3a['s3aq24'],\n"),
    'J_unmask_tree_code': (MALI, SALE, "    sale = derive_tree_sale_window(quantity(v['qs']), pd.to_numeric(src[v['v']], errors='coerce'))\n"),
    'K_inputs_drop_2017_unit': (MALI, "    '2017-18': ['s11fq01', 's11fq03', 's11fq10', 's11fq14a', 's11fq14b',\n",
                                "    '2017-18': ['s11fq01', 's11fq03', 's11fq10', 's11fq14a',\n"),
    'L_harvest_inputs_drop_unit': (MALI, "    '2014-15': ['s3bq01', 's3bq03', 's3bq09', 's3bq10a', 's3bq10b'],",
                                   "    '2014-15': ['s3bq01', 's3bq03', 's3bq09', 's3bq10a'],"),
}
path, old, new = MUT[name]
s = path.read_text(encoding='utf-8')
n = s.count(old)
assert n == 1, f'{name}: anchor occurs {n} times'
path.write_text(s.replace(old, new), encoding='utf-8')
print('applied', name, 'to', path.relative_to(root))
