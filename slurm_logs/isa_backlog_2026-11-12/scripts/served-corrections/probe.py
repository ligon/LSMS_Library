"""derivation_inputs for the new keys (candidate arm), saved for compare.py.

usage: python probe.py <clone> <arm> <framesdir>
"""
import json
import sys
import time
import traceback
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, arm, outdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
head = assert_env(clone)
warnings.simplefilter('ignore')
import lsms_library as ll  # noqa: E402
from lsms_library.derivations import derivation_records, derivations_table  # noqa: E402

KEYS = [('Mali', 'Mali::food_acquired::rice-ration-block-withheld', '2018-19'),
        ('Senegal', 'Senegal::food_acquired::salt-per-capita-withheld', '2021-22'),
        ('Senegal', 'Senegal::food_acquired::near-zero-quantity-withheld', '2018-19'),
        ('Niger', 'Niger::food_acquired::computed-last-purchase-quantity', '2018-19'),
        ('Uganda', 'Uganda::sample::ea-same-household', '2009-10')]
R = {'arm': arm, 'head': head, 'keys': {}}
try:
    tbl = derivations_table()
    R['registry_rows'] = int(len(tbl))
    R['registry_new'] = {k: (tbl.loc[k].to_dict() if k in tbl.index else None) for _, k, _ in KEYS}
except Exception as e:  # noqa: BLE001
    R['registry_error'] = f'{type(e).__name__}: {e}'
for C, key, wave in KEYS:
    r = {}
    t0 = time.time()
    try:
        recs = derivation_records(C)
        r['registered'] = key in recs
        if key in recs:
            raw = ll.Country(C).derivation_inputs(key, wave=wave)
            r['rows'] = int(len(raw))
            r['index'] = list(map(str, raw.index.names))
            r['columns'] = list(map(str, raw.columns))
            r['households'] = int(raw.index.get_level_values('i').nunique())
            d = raw.copy()
            d.attrs = {}
            for col in d.columns:
                if d[col].dtype == object:
                    d[col] = d[col].astype('string')
            d.to_parquet(outdir / f'{arm}.{C}.inputs.{key.split("::")[-1]}.parquet')
    except Exception as e:  # noqa: BLE001
        r['error'] = f'{type(e).__name__}: {e}'
        r['traceback'] = traceback.format_exc()[-3000:]
    r['seconds'] = round(time.time() - t0, 1)
    R['keys'][key] = r
    print(arm, key, {k: v for k, v in r.items() if k != 'traceback'}, flush=True)
(outdir / f'{arm}.probe.json').write_text(json.dumps(R, indent=1, default=str))
print('PROBE DONE', arm, flush=True)
