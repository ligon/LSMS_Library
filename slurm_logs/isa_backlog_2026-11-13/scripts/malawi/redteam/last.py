"""Red team (malawi): code-49 lines in the served 2019-20 frames; Farm-level H rows of cowpea households."""
import sys
from pathlib import Path
import pandas as pd
fr = Path(sys.argv[1]); orc = pd.read_csv(sys.argv[2], dtype=str)
for arm in ('base', 'cand'):
    cp = pd.read_parquet(fr / f'{arm}.crop_production.parquet').reset_index()
    pi = pd.read_parquet(fr / f'{arm}.plot_inputs.parquet').reset_index()
    obs = cp['production_obs'].astype(str)
    print(arm, 'crop_production G:49 rows', int(obs.str.match(r'reported:G:49(/|$)').sum()),
          'I:49 rows', int(obs.str.match(r'reported:I:49(/|$)').sum()),
          'crop==49', int(cp['crop'].astype(str).eq('49').sum()), flush=True)
    print(arm, 'plot_inputs crop values that look numeric', pi.loc[pi['crop'].astype(str).str.fullmatch(r'\d+'), 'crop'].astype(str).value_counts().to_dict(), flush=True)
    for w in ('2016-17', '2019-20'):
        hh = set(orc.loc[orc['wave'] == w, 'i'])
        f = pi[(pi['t'].astype(str) == w) & (pi['plot_id'].astype(str) == 'Farm-level') & pi['i'].astype(str).isin(hh)]
        print(arm, w, 'plot_inputs Farm-level rows in cowpea households, by crop', f['crop'].astype(str).value_counts().to_dict(), flush=True)
