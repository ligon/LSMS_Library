"""Cold-build a country's tables at one checkout and pickle them for compare.py.

usage: measure.py OUTDIR COUNTRY TABLE[:kwargs-json] ...
e.g.   measure.py /x Nigeria food_acquired 'food_quantities:{"units":"kgs"}' shocks

Writes OUTDIR/<label>.pkl (the frame, attrs dropped to keep the pickle portable),
OUTDIR/<label>.attrs.json and OUTDIR/summary.json (rows, columns, dtypes, seconds,
warning counts by class, the report ledgers this table filed).
"""
import json
import os
import pickle
import subprocess
import sys
import time
import warnings
from collections import Counter
from pathlib import Path

import lsms_library as ll

top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
from lsms_library import paths  # noqa: E402
_r = os.path.realpath(str(paths.data_root()))
assert _r.startswith('/local/job') or '/isa-sm-20261113/private/' in _r, _r

outdir = Path(sys.argv[1]); outdir.mkdir(parents=True, exist_ok=True)
country = sys.argv[2]
specs = sys.argv[3:]
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
summary = {'head': head, 'country': country, 'tables': {}}
sp = outdir / 'summary.json'
if sp.exists():                                  # restartable on requeue
    summary = json.loads(sp.read_text())


def ledgers(table):
    out = {}
    try:
        from lsms_library.quantity_audit import quantity_reports
        q = quantity_reports(country=country, table=table)
        rep, off = Counter(), Counter()
        for r in q:
            key = f"{r.get('site', 'q')}|{r.get('column', '?')}|{r.get('direction', '?')}|{r.get('t', r.get('wave', '?'))}"
            rep[key] += 1
            off[key] += len(r.get('offenders') or [])
        out['quantity_reports'] = dict(rep)
        out['quantity_offenders'] = dict(off)
    except Exception as e:  # noqa: BLE001
        out['quantity'] = f'ERR {e}'
    try:
        from lsms_library.null_read_audit import null_read_reports
        out['null_read'] = len(null_read_reports(country=country, table=table))
    except Exception as e:  # noqa: BLE001
        out['null_read'] = f'ERR {e}'
    try:
        from lsms_library.null_index_audit import null_index_reports
        out['null_index'] = len(null_index_reports(country=country, table=table))
    except Exception as e:  # noqa: BLE001
        out['null_index'] = f'ERR {e}'
    return out


c = ll.Country(country)
for spec in specs:
    table, _, kw = spec.partition(':')
    kwargs = json.loads(kw) if kw else {}
    label = table + ''.join(f'__{k}-{v}' for k, v in sorted(kwargs.items()))
    if label in summary['tables'] and (outdir / f'{label}.pkl').exists():
        continue
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            df = getattr(c, table)(**kwargs)
            err = None
        except Exception as e:  # noqa: BLE001
            df, err = None, f'{type(e).__name__}: {e}'
    rec = {'table': table, 'kwargs': kwargs, 'seconds': round(time.time() - t0, 1),
           'warnings': dict(Counter(type(x.message).__name__ for x in w)), 'error': err}
    if df is not None:
        attrs = dict(df.attrs)
        df = df.copy(); df.attrs = {}
        with open(outdir / f'{label}.pkl', 'wb') as fh:
            pickle.dump(df, fh, protocol=pickle.HIGHEST_PROTOCOL)
        (outdir / f'{label}.attrs.json').write_text(json.dumps(attrs, default=str, indent=1))
        rec.update(rows=len(df), index=list(df.index.names), columns=list(df.columns),
                   dtypes={k: str(v) for k, v in df.dtypes.items()}, attrs_keys=sorted(attrs))
    rec['ledgers'] = ledgers(table)
    summary['tables'][label] = rec
    sp.write_text(json.dumps(summary, indent=1, default=str))
    print(label, rec.get('rows'), rec['seconds'], 's', err or '', flush=True)
intree = subprocess.check_output(['bash', '-c', f'find {top}/lsms_library/countries -name "*.parquet" | wc -l'],
                                 text=True).strip()
summary['intree_parquet'] = intree
sp.write_text(json.dumps(summary, indent=1, default=str))
print('done', head, 'intree', intree)
