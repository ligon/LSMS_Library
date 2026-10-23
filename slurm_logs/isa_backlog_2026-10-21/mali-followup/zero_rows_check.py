"""Mali plot_inputs: what group answer sits above each served TYPE row?

Reads a pickled ``Country('Mali').plot_inputs()`` frame (written by
measure_mali.py) and tabulates, per wave and input group, the served type
rows by quantity class (zero / positive / no quantity) against the plot's
served group-answer row (Applied True / False / no answer row).  Also counts
PLOTS with a positive type quantity and no answer row.  Aggregate counts only.

Usage: python zero_rows_check.py <mali_country.pkl> <out.json>
"""
import json
import sys

import pandas as pd

pkl, out_path = sys.argv[1], sys.argv[2]
df = pd.read_pickle(pkl).reset_index()

GROUPS = {
    'Inorganic Fertilizer?': ['Urea', 'DAP', 'NPK', 'Other inorganic'],
    'Phytosanitary products?': ['Pesticide', 'Fungicide', 'Herbicide', 'Other phytosanitary'],
}
key = ['t', 'i', 'plot_id']
res = {}
for answer, types in GROUPS.items():
    ans = df[df['input'] == answer][key + ['Applied']].rename(columns={'Applied': 'answer'})
    assert not ans.duplicated(key).any(), answer
    rows = df[df['input'].isin(types)][key + ['input', 'Quantity']].merge(ans, on=key, how='left')
    q = pd.to_numeric(rows['Quantity'], errors='coerce')
    rows['qclass'] = 'no quantity'
    rows.loc[q == 0, 'qclass'] = 'zero'
    rows.loc[q > 0, 'qclass'] = 'positive'
    rows['aclass'] = rows['answer'].map({True: 'yes', False: 'no'}).fillna('no answer row')
    tab = rows.groupby(['t', 'qclass', 'aclass']).size()
    res[answer] = {
        'type_rows': {f'{t}|{qc}|{ac}': int(n) for (t, qc, ac), n in tab.items()},
        'plots_positive_no_answer': {
            str(t): int(n) for t, n in
            rows[(rows['qclass'] == 'positive') & (rows['aclass'] == 'no answer row')]
            .drop_duplicates(key).groupby('t').size().items()},
        'plots_positive_answer_no': {
            str(t): int(n) for t, n in
            rows[(rows['qclass'] == 'positive') & (rows['aclass'] == 'no')]
            .drop_duplicates(key).groupby('t').size().items()},
    }
    zero = rows[rows['qclass'] == 'zero']
    res[answer]['zero_rows_total'] = int(len(zero))
    res[answer]['zero_rows_under_yes'] = int((zero['aclass'] == 'yes').sum())
    res[answer]['zero_rows_without_yes'] = {
        str(k): int(v) for k, v in zero[zero['aclass'] != 'yes']
        .groupby(['t', 'aclass']).size().items()}

with open(out_path, 'w') as fh:
    json.dump(res, fh, indent=1, sort_keys=True)
print(json.dumps(res, indent=1, sort_keys=True))
