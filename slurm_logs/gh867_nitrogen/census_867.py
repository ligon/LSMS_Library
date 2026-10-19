"""GH #867 census: nitrogen_kg label coverage over every plot_inputs table.

Usage: python census_867.py OUT.json [--map base|branch]
Reads plot_inputs through the Country API (so a stale cache rebuilds in the
private LSMS_DATA_DIR), runs nitrogen_kg, and records per-country tallies.
"""
import json, sys, warnings, time
import pandas as pd
import lsms_library as ll
from lsms_library import transformations as T

COUNTRIES = ['Benin', 'Burkina_Faso', 'CotedIvoire', 'Ethiopia', 'GhanaLSS',
             'Guinea-Bissau', 'Malawi', 'Mali', 'Nigeria', 'Niger', 'Senegal',
             'Tanzania', 'Togo', 'Uganda']
out_path = sys.argv[1]
res = {}
for c in COUNTRIES:
    t0 = time.time()
    try:
        pi = ll.Country(c).plot_inputs()
    except Exception as e:
        res[c] = {'error': repr(e)}
        print(c, 'ERROR', e, flush=True)
        continue
    inp = (pi.index.get_level_values('input') if 'input' in pi.index.names
           else pi['input']).astype(str)
    qty = pd.to_numeric(pi['Quantity'], errors='coerce')
    has_q = pd.Series(qty.notna().values, index=range(len(pi)))
    lab = pd.Series(inp, index=range(len(pi)))
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        n = T.nitrogen_kg(pi)
    tally = n.attrs['nitrogen_input_match']
    per_label = {}
    for l, g in lab.groupby(lab):
        per_label[l] = {'rows': int(len(g)), 'rows_with_quantity': int(has_q[g.index].sum())}
    # per-label N contribution (kg) -- one nitrogen_kg per matched label
    contrib = {}
    for l in tally['matched_labels']:
        mask = (lab == l).values
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            sub = T.nitrogen_kg(pi[mask])
        contrib[l] = {'plots': int(len(sub)), 'nitrogen_kg': float(sub['Nitrogen_kg'].sum())}
    res[c] = {
        'rows': int(len(pi)),
        'matched_rows': tally['matched_rows'],
        'unmatched_rows': tally['unmatched_rows'],
        'matched_labels': tally['matched_labels'],
        'unmatched_labels': tally['unmatched_labels'],
        'per_label': per_label,
        'matched_contrib': contrib,
        'nitrogen_plots': int(len(n)),
        'nitrogen_kg_total': float(n['Nitrogen_kg'].sum()) if len(n) else 0.0,
        'coverage_warnings': [str(x.message)[:400] for x in w
                              if issubclass(x.category, T.NutrientCoverageWarning)],
        'seconds': round(time.time() - t0, 1),
    }
    print(c, res[c]['rows'], res[c]['matched_rows'], res[c]['nitrogen_plots'],
          round(res[c]['nitrogen_kg_total'], 1), res[c]['seconds'], flush=True)
json.dump(res, open(out_path, 'w'), indent=1, sort_keys=True)
