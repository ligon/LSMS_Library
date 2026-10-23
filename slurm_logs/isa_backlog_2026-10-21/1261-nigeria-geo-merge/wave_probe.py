"""GH #1261: per-wave/per-round measurement of Nigeria's cluster_features
`dfs:` merge and the Site 2 household -> cluster collapse.

Usage (from the checkout under test, with PYTHONPATH and LSMS_DATA_DIR set to a
fresh private root):

    python wave_probe.py <checkout> <out.json>

Instruments ``Wave._merge_subframes`` (records each sub-frame merge: rows,
keys present only on one side, null-`v` rows in the output, dtypes) and
``country._collapse_to_cluster_grain`` (records the household-grain frame the
collapse receives and the grain report it files).  Writes AGGREGATES only --
counts, dtypes, digests; no household ids, no coordinates.
"""
import json
import os
import subprocess
import sys
import warnings

checkout, out = sys.argv[1], sys.argv[2]

import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library import country as cmod  # noqa: E402

top = subprocess.check_output(['git', '-C', checkout, 'rev-parse', '--show-toplevel'],
                              text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__

CUR = {'wave': None}
MERGES: dict[str, list] = {}
COLLAPSES: dict[str, dict] = {}

_orig_merge = cmod.Wave._merge_subframes


def _keyset(df, keys):
    return set(map(tuple, df[keys].astype(str).itertuples(index=False, name=None)))


def merge_spy(self, left, right, merge_on, request, lname, rname, how='outer'):
    res = _orig_merge(self, left, right, merge_on, request, lname, rname, how=how)
    if request == 'cluster_features':
        keys = sorted(k for k in merge_on if k in left.columns and k in right.columns)
        lk, rk = _keyset(left, keys), _keyset(right, keys)
        rec = {
            'left': lname, 'right': rname, 'how': how, 'keys': keys,
            'left_rows': int(len(left)), 'right_rows': int(len(right)),
            'left_keys': len(lk), 'right_keys': len(rk),
            'only_left': len(lk - rk), 'only_right': len(rk - lk),
            'out_rows': int(len(res)),
            'out_null_v': int(res['v'].isna().sum()) if 'v' in res.columns else None,
            'out_dtypes': {c: str(res[c].dtype) for c in res.columns
                           if c in ('Region', 'District', 'Rural', 'Latitude', 'Longitude', 'v')},
        }
        MERGES.setdefault(CUR['wave'], []).append(rec)
    return res


_orig_collapse = cmod._collapse_to_cluster_grain


def collapse_spy(df, keep_levels, country=None, wave=None):
    before = len(cmod.grain_reports(country='Nigeria', table='cluster_features'))
    res = _orig_collapse(df, keep_levels, country=country, wave=wave)
    reps = cmod.grain_reports(country='Nigeria', table='cluster_features')[before:]
    idx = df.index.to_frame(index=False)
    COLLAPSES[CUR['wave']] = {
        'in_rows': int(len(df)),
        'in_null_v': int(idx['v'].isna().sum()) if 'v' in idx else None,
        'in_nan_key_rows': int(idx[keep_levels].isna().any(axis=1).sum()),
        'in_dtypes': {c: str(df[c].dtype) for c in df.columns},
        'out_rows': int(len(res)),
        'out_clusters_moved': int(res.index.get_level_values('v').astype(str)
                                  .str.startswith('moved-').sum()),
        'grain_reports': [{k: r.get(k) for k in
                           ('wave', 'site', 'levels', 'rows', 'dropped', 'destroyed',
                            'conflicting_groups', 'nan_key_rows')} for r in reps],
    }
    return res


cmod.Wave._merge_subframes = merge_spy
cmod._collapse_to_cluster_grain = collapse_spy

warnings.simplefilter('ignore')
c = ll.Country('Nigeria', preload_panel_ids=False)
result = {'waves': list(c.waves), 'per_wave': {}}
for w in c.waves:
    CUR['wave'] = w
    wv = c[w]
    try:
        cf = wv.cluster_features()
        result['per_wave'][w] = {
            'folder': c.wave_folder_map.get(w, w),
            'merges': MERGES.get(w, []),
            'collapse': COLLAPSES.get(w),
            'served_rows': int(len(cf)),
            'served_null_v': int(cf.index.get_level_values('v').isna().sum()),
            'served_dtypes': {k: str(v) for k, v in cf.dtypes.items()},
        }
    except Exception as e:  # noqa: BLE001
        result['per_wave'][w] = {'error': repr(e)[:300]}
    print(w, json.dumps(result['per_wave'][w].get('collapse', {}).get('grain_reports')
                        if 'collapse' in result['per_wave'][w] and result['per_wave'][w]['collapse']
                        else None), flush=True)

with open(out, 'w') as f:
    json.dump(result, f, indent=1, sort_keys=True)
print('wrote', out)
