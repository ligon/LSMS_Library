"""sample() and plot_inputs() under LSMS_BUILD_BACKEND=make, cold, vs the default backend frames pickled elsewhere."""
import os, pickle, sys, warnings
from pathlib import Path
import pandas as pd
warnings.simplefilter('ignore')
import lsms_library as ll
out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
assert os.environ.get('LSMS_BUILD_BACKEND') == 'make'
c = ll.Country('Benin')
for t in ('sample', 'plot_inputs', 'household_roster'):
    try:
        df = getattr(c, t)()
        pickle.dump(df, open(out / f'{t}.pkl', 'wb'))
        print(t, len(df), list(df.index.names), flush=True)
    except Exception as e:
        print(t, 'ERR', type(e).__name__, str(e)[:500], flush=True)
print('DONE')
