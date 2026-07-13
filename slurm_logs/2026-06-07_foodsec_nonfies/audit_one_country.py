#!/usr/bin/env python
"""Cold-build every declared feature for ONE country; emit a JSON health record.

Country-partitioned: each invocation owns one country, so concurrent runs for
DIFFERENT countries never touch the same cache files (no races).  Within a
country, features build sequentially (shared deps like roster build once).

Usage:  audit_one_country.py "<Country>" <out.json>
"""
import sys, os, json, subprocess, traceback, warnings
warnings.filterwarnings('ignore')

country = sys.argv[1]
out_path = sys.argv[2]

# Cold start: physically clear this country's L2 cache so every feature builds
# from source (this is how the GhanaLSS/Mali cold-build bugs surface).
subprocess.run(['.venv/bin/lsms-library', 'cache', 'clear', '--country', country],
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

import lsms_library as ll
from lsms_library.diagnostics import load_feature, is_this_feature_sane

try:
    c = ll.Country(country)
    feats = sorted(c.data_scheme)
except Exception as e:
    json.dump({'country': country, 'fatal': f'{type(e).__name__}: {e}', 'results': []},
              open(out_path, 'w'))
    sys.exit(0)

results = []
for feat in feats:
    rec = {'feature': feat}
    try:
        df = load_feature(c, feat)
        import pandas as pd
        if isinstance(df, dict):
            rec.update(status='BUILT_PROPERTY', n=len(df))
        elif isinstance(df, pd.DataFrame):
            rec['rows'] = int(len(df))
            idx = list(df.index.names)
            rec['waves'] = (sorted(map(str, df.index.get_level_values('t').unique()))
                            if 't' in idx else None)
            if len(df) == 0:
                rec['status'] = 'EMPTY'
            else:
                try:
                    r = is_this_feature_sane(df, country, feat)
                    fails = [ck.name for ck in r.checks if ck.status == 'fail']
                    warns = [ck.name for ck in r.checks if ck.status == 'warn']
                    rec['status'] = 'SANE_FAIL' if fails else 'OK'
                    rec['failed'] = fails
                    rec['warned'] = warns
                except Exception as e:
                    rec.update(status='SANE_ERROR', error=f'{type(e).__name__}: {str(e)[:160]}')
        else:
            rec.update(status='ODD', repr=str(type(df)))
    except Exception as e:
        rec.update(status='CRASH', error_type=type(e).__name__,
                   error=str(e)[:200],
                   where=traceback.format_exc().strip().splitlines()[-2][:200] if len(traceback.format_exc().splitlines()) > 1 else '')
    results.append(rec)
    json.dump({'country': country, 'results': results}, open(out_path, 'w'))  # checkpoint each feature

json.dump({'country': country, 'results': results}, open(out_path, 'w'))
print(f'{country}: {len(results)} features audited')
