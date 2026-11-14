"""Which _compute_panel_ids branch does Malawi reach, and does it equal the committed JSON?
usage: branch_probe.py <clone> <label> <out.json>   (backend from env LSMS_BUILD_BACKEND)"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, label, out = sys.argv[1], sys.argv[2], Path(sys.argv[3])
assert_env(clone)
import lsms_library.country as cm  # noqa: E402

calls = []
_g, _p = cm.panel_ids_guarding, cm.panel_ids


def g(*a, **k):
    calls.append('panel_ids_guarding'); return _g(*a, **k)


def p(*a, **k):
    calls.append('panel_ids'); return _p(*a, **k)


cm.panel_ids_guarding, cm.panel_ids = g, p
here = Path(clone) / 'lsms_library/countries/Malawi/_'
committed_U = json.loads((here / 'updated_ids.json').read_text())
committed_D = json.loads((here / 'panel_ids.json').read_text())
R = {'backend': os.environ.get('LSMS_BUILD_BACKEND'), 'label': label}
res = {}
for country in ['Malawi', 'Uganda', 'Tanzania', 'Nigeria', 'Ethiopia']:
    calls.clear()
    try:
        c = cm.Country(country)
        U = c.updated_ids
        D = c.panel_ids
        rr = {'calls': list(calls), 'types': [type(D).__name__, type(U).__name__],
              'waves': {w: len(v) for w, v in (U or {}).items()}}
        if country == 'Malawi':
            Dd = dict(D.data) if hasattr(D, 'data') else D
            Dd = {(','.join(k) if isinstance(k, tuple) else k): (','.join(v) if isinstance(v, tuple) else v) for k, v in Dd.items()}
            rr['equal_committed_updated'] = U == committed_U
            rr['equal_committed_panel'] = Dd == committed_D
            if U != committed_U:
                rr['diff_waves'] = {w: sum(1 for k in set(U.get(w, {})) | set(committed_U.get(w, {}))
                                           if U.get(w, {}).get(k) != committed_U.get(w, {}).get(k)) for w in set(U) | set(committed_U)}
        else:
            cu = json.loads((Path(clone) / f'lsms_library/countries/{country}/_/updated_ids.json').read_text())
            rr['equal_committed_updated'] = U == cu
    except Exception as e:
        rr = {'error': f'{type(e).__name__}: {e}', 'calls': list(calls)}
    res[country] = rr
    print(country, rr, flush=True)
R['countries'] = res
out.write_text(json.dumps(R, indent=1, default=str))
