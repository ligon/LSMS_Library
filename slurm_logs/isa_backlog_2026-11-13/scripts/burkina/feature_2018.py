"""Feature('crop_production') for Burkina 2018-19: digest and shape.  Usage: OUT_JSON"""
import hashlib, json, sys, warnings
warnings.simplefilter('ignore')
import numpy as np, pandas as pd
import lsms_library as ll
f = ll.Feature('crop_production')(['Burkina_Faso'])
w = f.xs('2018-19', level='t', drop_level=False).reset_index()
w = w[sorted(c for c in w.columns if c != 'Derivation')]
h = pd.util.hash_pandas_object(w.astype(object).where(w.notna(), None).astype(str), index=False).to_numpy()
out = {'rows': int(len(w)), 'index': [str(n) for n in f.index.names],
       'digest_without_Derivation': hashlib.md5(np.sort(h).tobytes()).hexdigest(),
       'production_obs': {str(k): int(v) for k, v in w['production_obs'].value_counts().items()},
       'has_Derivation': 'Derivation' in f.columns}
json.dump(out, open(sys.argv[1], 'w'), indent=1); print(out)
