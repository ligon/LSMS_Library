"""Rulings (a) and (d): which 3E records the no-sale mask matches, and what Site Q names.

Usage: followup.py REPORT_DIR.  Run after build.py in the same private root.
Aggregates and cell-level descriptions only: no household identifiers.
"""
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library import paths, quantity_audit as Q
from lsms_library.local_tools import get_dataframe

rep = Path(sys.argv[1]); rep.mkdir(parents=True, exist_ok=True)
assert '/private/sold-kg-1560/' in str(paths.data_root().resolve())
out = {}

# ---- (a): the seven 3E records ---------------------------------------------
CR = paths.countries_root()
f = str(CR / 'Niger' / '2011-12' / 'Data' / 'NER_2011_ECVMA_v01_M_Stata8' / 'ecvmaas3e_p1.dta')
n = get_dataframe(f, convert_categoricals=False)
l = get_dataframe(f, convert_categoricals=True)
keep = ~n.duplicated(keep='first')
n, l = n[keep], l[keep]
num = lambda s: pd.to_numeric(s, errors='coerce').astype('float64')
kg, q, v, hk = num(n['as03eq11c']), num(n['as03eq11a']), num(n['as03eq12']), num(n['as03eq09c'])
q = q.where(q != 99999); v = v.where(v != 9999999); kgm = kg.where(kg != 99999)
sale = (q > 0) | (v > 0)
m = kgm.notna() & ~sale
st = lambda x: np.where(x.isna(), 'missing', np.where(x == 0, 'zero', 'positive'))
cls = pd.DataFrame({'sold_flag_as03eq10': l.loc[m, 'as03eq10'].astype('string').fillna('<NA>').values,
                    'as03eq11a': st(q[m]), 'as03eq12': st(v[m]),
                    'sold_unit_as03eq11b': l.loc[m, 'as03eq11b'].astype('string').fillna('<NA>').values,
                    'kg_equals_harvest_kg': (kgm[m] == hk[m]).values,
                    'cycles_as03eq06': num(n.loc[m, 'as03eq06']).values,
                    'crop': l.loc[m, 'as03eq03'].astype('string').values})
out['a_mask'] = {
    'records': int(m.sum()), 'kg': float(kgm[m].sum()),
    'by_flag_quantity_value': {' / '.join(map(str, k)): int(g) for k, g in
                               cls.groupby(['sold_flag_as03eq10', 'as03eq11a', 'as03eq12']).size().items()},
    'by_flag_kg_equals_harvest': {' / '.join(map(str, k)): int(g) for k, g in
                                  cls.groupby(['sold_flag_as03eq10', 'kg_equals_harvest_kg']).size().items()},
    'sold_unit': cls['sold_unit_as03eq11b'].value_counts().to_dict(),
    'cycles': {str(k): int(v_) for k, v_ in cls['cycles_as03eq06'].value_counts().items()},
    'crops': cls['crop'].value_counts().to_dict(),
    'the_oui_record': cls[cls['sold_flag_as03eq10'].str.lower().str.startswith('oui')].drop(
        columns='crop').to_dict('records'),
}
# The served rows that carry the key.
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    Q._clear_quantity_reports()
    niger = ll.Country('Niger').crop_production()
niger_reports = [r for r in Q.quantity_reports(country='Niger', table='crop_production')]
d = niger.reset_index()
keyed = d['Derivation'].astype('string').str.contains(
    'Niger::crop_production::dry-season-sold-kg-no-sale', regex=False).fillna(False)
out['a_mask']['served_keyed_rows'] = int(keyed.sum())
out['a_mask']['served_keyed_rows_waves_plots'] = d.loc[keyed, ['t', 'plot_id']].value_counts().astype(int).to_dict().__repr__()
out['a_mask']['served_keyed_quantity_sold_kg_all_na'] = bool(d.loc[keyed, 'Quantity_sold_kg'].isna().all())

# ---- (d): what Site Q names -------------------------------------------------
def summarise(reports):
    res = []
    for r in reports:
        if r['column'] != 'Quantity_sold_kg':
            continue
        res.append({'wave': r['wave'], 'direction': r['direction'], 'rows': r['rows'],
                    'n_implausible': r['n_implausible'], 'max_ratio': r['max_ratio'],
                    'offenders': [{k: o.get(k) for k in ('j', 'u', 'value', 'reference', 'ratio', 'rung')}
                                  for o in r['offenders']]})
    return res


with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    Q._clear_quantity_reports()
    mali = ll.Country('Mali').crop_production()
mali_reports = Q.quantity_reports(country='Mali', table='crop_production')
out['d_site_q'] = {'Niger': summarise(niger_reports), 'Mali': summarise(mali_reports),
                   'other_columns_report_counts': {
                       c: sorted({(r['column'], r['direction'], r['wave'], r['n_implausible'])
                                  for r in rs if r['column'] != 'Quantity_sold_kg'})
                       for c, rs in (('Niger', niger_reports), ('Mali', mali_reports))}}

# Where does Mali's 1,150,000 kg sale stand against its cell?  Re-run the audit with
# K = 0 so every positive row reports its reference (diagnostic only; restored).
k0 = Q.QUANTITY_OUTLIER_K
try:
    Q.QUANTITY_OUTLIER_K = 0
    allrows = Q.audit_quantities(mali, 'Quantity_sold_kg', country='Mali', table='crop_production')
finally:
    Q.QUANTITY_OUTLIER_K = k0
big = [dict(o, wave=r['wave']) for r in allrows for o in r['offenders'] if o['value'] >= 1e6]
out['d_site_q']['mali_largest_against_its_cell'] = [
    {k: o.get(k) for k in ('wave', 'j', 'u', 'value', 'reference', 'ratio', 'rung')} for o in big]
out['d_site_q']['K'] = k0
(rep / 'followup.json').write_text(json.dumps(out, indent=1, default=str, ensure_ascii=False))
print(json.dumps({'a': {k: out['a_mask'][k] for k in ('records', 'kg', 'served_keyed_rows',
                                                       'by_flag_quantity_value')},
                  'd_counts': {c: [(r['wave'], r['direction'], r['n_implausible'])
                                   for r in out['d_site_q'][c]] for c in ('Niger', 'Mali')},
                  'mali_big': out['d_site_q']['mali_largest_against_its_cell']}, default=str))
