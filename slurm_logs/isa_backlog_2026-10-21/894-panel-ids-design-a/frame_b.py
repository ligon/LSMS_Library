"""Capture the exact finalized frame Design B feeds to local_tools.panel_ids()."""
import os, sys, warnings
exec(open(os.path.join(os.path.dirname(__file__), 'assert_id.py')).read())
warnings.simplefilter('ignore')
import pandas as pd
import lsms_library as ll
out = sys.argv[1]
for C in sys.argv[2:]:
    c = ll.Country(C, preload_panel_ids=False)
    df = c._aggregate_wave_data(None, 'panel_ids')
    print(C, type(df).__name__, df.shape, list(df.index.names), dict(df.dtypes.astype(str)))
    flat = df.reset_index()[['t', 'i', 'previous_i']].astype(str)
    flat.to_csv(f'{out}/{C}_frameB.csv', index=False)
