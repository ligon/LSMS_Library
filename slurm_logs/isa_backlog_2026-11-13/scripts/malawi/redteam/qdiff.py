"""Red team (malawi): Site Q offender sets, base vs candidate, on the saved API frames.

usage: python qdiff.py <framesdir> <out.json>
"""
import json
import sys
import warnings
from pathlib import Path

import pandas as pd
from lsms_library.quantity_audit import audit_quantities

fr, outp = Path(sys.argv[1]), Path(sys.argv[2])
res = {}
for table, cols in (('plot_inputs', ['Quantity']), ('crop_production', ['Quantity', 'Quantity_kg', 'Quantity_sold'])):
    for col in cols:
        sets = {}
        for arm in ('base', 'cand'):
            df = pd.read_parquet(fr / f'{arm}.{table}.parquet')
            if col not in df.columns:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                reps = audit_quantities(df, col, country='Malawi', table=table, tails=('high', 'low'))
            offs = []
            for r in reps:
                for o in r.get('offenders', []):
                    offs.append(json.dumps({k: str(v) for k, v in o.items()
                                            if k not in ('reference', 'ratio', 'rung', 'cell')}, sort_keys=True))
            sets[arm] = {'n': len(offs), 'by_t': {str(r.get('t')): len(r.get('offenders', [])) for r in reps},
                         'offs': sorted(offs)}
        if len(sets) == 2:
            b, c = set(sets['base']['offs']), set(sets['cand']['offs'])
            res[f'{table}.{col}'] = {'n': [sets['base']['n'], sets['cand']['n']],
                                     'by_t': [sets['base']['by_t'], sets['cand']['by_t']],
                                     'only_base': sorted(b - c), 'only_cand': sorted(c - b)}
            print(table, col, res[f'{table}.{col}']['n'], 'only_base', len(b - c), 'only_cand', len(c - b), flush=True)
outp.write_text(json.dumps(res, indent=1))
print('QDIFF DONE', flush=True)
