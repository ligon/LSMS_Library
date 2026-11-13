"""GH #856 step 2 -- for every sale whose attachment the recovery MOVED, show
the raw harvest and sale unit codes and specify texts behind it.

    python detached_texts.py <diff.json> <out.json>
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

import lsms_library
TOP = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True,
                              cwd=Path(__file__).parent).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(TOP) + os.sep)

from lsms_library.data_access import get_data_file  # noqa: E402
from lsms_library.local_tools import format_id, get_dataframe  # noqa: E402

diff = json.loads(Path(sys.argv[1]).read_text())
moved = pd.DataFrame(diff['sale_attachment_moved_rows'])

HALVES = {
    '2016-17': [
        ('Cross_Sectional', '', 'case_id', 'cs-17-'),
        ('Panel', '_16', 'y3_hhid', ''),
    ],
    '2019-20': [
        ('Cross_Sectional', '', 'case_id', ''),
        ('Panel', '_19', 'y4_hhid', ''),
    ],
}
cache = {}


def mod(wave, half, name, suffix, hh, prefix):
    key = (wave, half, name)
    if key not in cache:
        rel = f'Malawi/{wave}/Data/{half}/ag_mod_{name}{suffix}.dta'
        df = get_dataframe(str(get_data_file(rel)), convert_categoricals=False)
        df['_i'] = prefix + df[hh].apply(format_id)
        if 'gardenid' in df.columns:
            df['_plot'] = df['gardenid'].apply(format_id) + '_' + df['plotid'].apply(format_id)
        cache[key] = df
    return cache[key]


out = []
for r in moved.to_dict(orient='records'):
    rec = {k: r[k] for k in ('t', 'i', 'plot_id', 'crop', 'perennial', 'u_before',
                             'u_after', 'Value_sold_before', 'Value_sold_after')}
    hmod, smod = ('p', 'q') if r['perennial'] == 'True' else ('g', 'i')
    hu, su = ('ag_p09b', 'ag_q02b') if hmod == 'p' else ('ag_g13b', 'ag_i02b')
    for half, suffix, hh, prefix in HALVES[r['t']]:
        h = mod(r['t'], half, hmod, suffix, hh, prefix)
        h = h[(h['_i'] == r['i']) & (h['_plot'] == r['plot_id'])]
        if len(h):
            s = mod(r['t'], half, smod, suffix, hh, prefix)
            s = s[s['_i'] == r['i']]
            rec['half'] = half
            rec['harvest'] = [(None if pd.isna(a) else int(a), str(b))
                              for a, b in zip(h[hu], h[f'{hu}_oth'])]
            rec['sale'] = [(None if pd.isna(a) else int(a), str(b), None if pd.isna(c) else int(c))
                           for a, b, c in zip(s[su], s[f'{su}_oth'], s['crop_code'])]
            break
    out.append(rec)

Path(sys.argv[2]).write_text(json.dumps(out, indent=1, default=str) + '\n')
for rec in out:
    if rec['Value_sold_after'] == 'NA':
        print('DETACHED', rec['t'], rec['i'], rec['crop'], rec['u_before'], '->', rec['u_after'],
              '| harvest', rec.get('harvest'), '| sale', rec.get('sale'))
n_att = sum(1 for rec in out if rec['Value_sold_after'] != 'NA')
print('newly attached:', n_att)
