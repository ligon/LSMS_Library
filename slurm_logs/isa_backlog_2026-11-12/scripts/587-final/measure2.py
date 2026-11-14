"""GH #587 final: where the household files classify Semovita/semolina (aggregates only).

argv: clone results_dir tag
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env, Recorder  # noqa: E402

clone, res, tag = sys.argv[1], sys.argv[2], sys.argv[3]
head = assert_env(clone)
rec = Recorder(os.path.join(res, f'measure2-{tag}.json'))
rec.data['head'] = head

from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

NG = countries_root() / 'Nigeria'
FILES = [
    '2018-19/Data/sect7b_plantingw4.dta',
    '2018-19/Data/sect10b_harvestw4.dta',
    '2023-24/Data/Post Planting Wave 5/Household/sect6b_plantingw5.dta',
    '2018-19-NLSS/Data/Household/sect6b_food_cons.dta',
]


def sweep():
    out = {}
    for rel in FILES:
        lab = get_dataframe(str(NG / rel), convert_categoricals=True)
        num = get_dataframe(str(NG / rel), convert_categoricals=False)
        e = {'columns_with_item': [c for c in lab.columns if 'item' in c.lower()],
             'column_labels': {}}
        try:
            e['column_labels'] = {c: str(v) for c, v in (lab.attrs.get('variable_labels') or {}).items()
                                  if 'item' in c.lower()}
        except Exception:  # noqa: BLE001
            pass
        for alt in ('item_os_cd', 'item_cd_alt'):
            if alt not in num:
                continue
            m = pd.to_numeric(num[alt], errors='coerce').eq(231)
            main = pd.DataFrame({'code': pd.to_numeric(num.loc[m, 'item_cd'], errors='coerce'),
                                 'label': lab.loc[m, 'item_cd'].astype('string')})
            e[f'{alt}==231'] = {
                'rows': int(m.sum()),
                'main_item_cd': {f'{int(c) if pd.notna(c) else "NA"}|{l}': int(v)
                                 for (c, l), v in main.groupby(['code', 'label'], dropna=False).size().items()},
            }
            # the other-specify recode list's neighbours (labels only)
            s = lab[alt]
            if isinstance(s.dtype, pd.CategoricalDtype):
                cats = [str(c) for c in s.cat.categories]
                e[f'{alt}_categories_200_260'] = [c for c in cats if c[:3].isdigit() and 200 <= int(c[:3]) <= 260]
        # rows of the main 'other grains / flour' style codes for scale
        out[rel] = e
    return out


rec.step('sweep', sweep)
print('MEASURE2 DONE', flush=True)
