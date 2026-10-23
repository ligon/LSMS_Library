"""Cold-build sample() + household_roster(); dump frames (scratch only) + per-wave i stats;
then run check_panel_consistency.  usage: served.py OUTDIR C [C...]"""
import os, sys, time, warnings, json
exec(open(os.path.join(os.path.dirname(__file__), 'assert_id.py')).read())
import pandas as pd
import lsms_library as ll
from lsms_library.diagnostics import check_panel_consistency
out = sys.argv[1]
os.makedirs(out, exist_ok=True)
summary = {}
for C in sys.argv[2:]:
    c = ll.Country(C)
    for table in ['sample', 'household_roster']:
        t0 = time.time()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            df = getattr(c, table)()
        df.to_parquet(f'{out}/{C}_{table}.parquet')
        ii = df.index.to_frame(index=False)
        per = {str(t): {'rows': int((ii['t'] == t).sum()), 'n_i': int(ii.loc[ii['t'] == t, 'i'].nunique())}
               for t in sorted(ii['t'].unique())}
        summary[f'{C}/{table}'] = per
        print(C, table, df.shape, list(df.index.names), 'dup idx', int(df.index.duplicated().sum()),
              'secs %.0f' % (time.time() - t0), flush=True)
    rep = check_panel_consistency(c)
    print('==== check_panel_consistency', C)
    rep.summarize(); sys.stdout.flush()
json.dump(summary, open(f'{out}/served_summary.json', 'w'), indent=1, sort_keys=True)
print('DONE')
