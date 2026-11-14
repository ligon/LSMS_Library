"""Apply one named source mutation to a checkout's transformations.py.

usage: mutate.py CHECKOUT NAME     (restore with git checkout -- lsms_library/transformations.py)
"""
import sys

MUT = {
    'no_purchase_filter': [("    priced = _purchase_evidence(df)\n", "    priced = df\n")],
    'eligible_all': [("    eligible = ~cell_purchased\n",
                      "    eligible = np.ones(len(cell_purchased), dtype=bool)\n")],
    'eligible_if_no_estimate': [("    eligible = ~cell_purchased\n",
                                 "    eligible = np.isnan(rungs['kg_item']) & np.isnan(rungs['kg_unit'])\n")],
    'fallback_reads_all': [("    return df.assign(Expenditure=df['Expenditure'].mask(purchased))\n",
                            "    return df\n")],
    'no_unit_rung': [("    kg_unp = np.where(eligible, fallback['kg_unit'], np.nan)\n",
                      "    kg_unp = np.full(len(eligible), np.nan)\n")],
    'no_item_fallback': [("    kg_np = np.where(eligible, fallback['kg_item'], np.nan)\n",
                          "    kg_np = np.full(len(eligible), np.nan)\n")],
    'unitvalue_keeps_value': [("_CURRENCY_UNPRICEABLE_MODES = ('kgvalue', 'kgprice', 'unitvalue')",
                               "_CURRENCY_UNPRICEABLE_MODES = ('kgvalue', 'kgprice')")],
    'unitvalue_keeps_unknown': [("    on_unknown = (_unit_sentinel_mask(v) & ~on_currency\n"
                                 "                  if has_u and by_label else no_rows)",
                                 "    on_unknown = no_rows")],
    'units_keeps_unknown': [("        q = q[~unknown].dropna().groupby(group_by).sum()",
                             "        q = q.dropna().groupby(group_by).sum()")],
}

path = f'{sys.argv[1]}/lsms_library/transformations.py'
src = open(path).read()
for old, new in MUT[sys.argv[2]]:
    n = src.count(old)
    if n != 1:
        sys.exit(f'mutation {sys.argv[2]}: pattern found {n} times')
    src = src.replace(old, new)
open(path, 'w').write(src)
print('applied', sys.argv[2])
