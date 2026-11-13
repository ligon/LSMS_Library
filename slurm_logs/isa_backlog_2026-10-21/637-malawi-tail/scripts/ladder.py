"""2010-11 sale-basis ladder tallies, replayed from sources (aggregates only)."""
import json, sys, warnings, importlib.util
import pandas as pd
from lsms_library.paths import countries_root
M = countries_root() / 'Malawi' / '_'
sys.path.insert(0, str(M))
spec = importlib.util.spec_from_file_location('mw', M / 'malawi.py'); mw = importlib.util.module_from_spec(spec); spec.loader.exec_module(mw)
out = {}
for wave in sys.argv[2].split(','):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        harvest, sale, raw = mw._crop_pieces(wave)
        harv = mw._consolidate_harvest(harvest)
        S = pd.concat(sale, ignore_index=True)
        _, dec, t = mw._attach_sales(wave, harv, S)
    lad = {k: {kk: vv for kk, vv in v.items() if kk in ('sales', 'value', 'rows')} for k, v in t['sale_basis_ladder'].items() if isinstance(v, dict)}
    out[wave] = dict(sale_lines=int(len(raw)), sale_keys=int(len(S)), sale_value_total=float(pd.to_numeric(S['Value_sold'], errors='coerce').sum()),
                     sale_qty_total=float(pd.to_numeric(S['Quantity_sold'], errors='coerce').sum()),
                     suppressed=t['sale_suppressed'], mismatch=t['sale_basis_mismatch'], ladder=lad,
                     outcomes=dec['outcome'].astype(str).value_counts().to_dict())
    print(wave, json.dumps(out[wave], default=str)[:1500], flush=True)
json.dump(out, open(sys.argv[1], 'w'), indent=1, default=str)
