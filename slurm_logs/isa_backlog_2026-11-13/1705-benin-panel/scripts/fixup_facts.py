"""Red-team fix-up facts (raw files): F5 the implausible Quantity_purchased rows; F3 the
63 linked households whose pre-loaded members all left, and the 19 with no pre-load."""
import warnings

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

pd.set_option('display.width', 250)
pd.set_option('display.max_columns', 30)
B = countries_root() / 'Benin'


def raw(w, n, labels=False):
    return get_dataframe(B / w / 'Data' / n, convert_categoricals=labels)


def key(g, m):
    return f"{int(g)}{int(m):03d}"


print('== F5: largest s16bq09a, 2018-19 applied lines')
lab = raw('2018-19', 's16b_me_ben2018.dta', labels=True)
cod = raw('2018-19', 's16b_me_ben2018.dta')
app = (cod['s16bq02'] == 1).to_numpy()
lab, cod = lab[app], cod[app]
t = pd.DataFrame({'grappe': cod['grappe'], 'menage': cod['menage'], 'input': lab['s16bq01'],
                  'used': cod['s16bq03a'], 'used_u': lab['s16bq03b'], 'bought': cod['s16bq09a'],
                  'bought_u': lab['s16bq09b'], 'value_cfa': cod['s16bq09c']})
print(t.sort_values('bought', ascending=False).head(12).to_string())

print('== F3: linked households by pre-load state')
c21 = raw('2021-22', 's00_me_ben2021.dta', labels=True)
c18 = raw('2018-19', 's00_me_ben2018.dta')
k18 = {key(g, m) for g, m in zip(c18['grappe'], c18['menage'])}
c21['key'] = [key(g, m) for g, m in zip(c21['grappe'], c21['menage'])]
linked = set(c21.loc[c21['PanelHH'].astype(str).str.strip().eq('Panel') & c21['key'].isin(k18), 'key'])
s21 = raw('2021-22', 's01_me_ben2021.dta')
s21l = raw('2021-22', 's01_me_ben2021.dta', labels=True)
s21['key'] = [key(g, m) for g, m in zip(s21['grappe'], s21['menage'])]
s21 = s21[s21['key'].isin(linked)]
s21l = s21l.loc[s21.index]
g = s21.groupby('key')
pre_n = g['s01q00aa'].apply(lambda x: int(x.notna().sum()))
left_n = g['s01q00aa'].apply(lambda x: int((x == 2).sum()))
all_left = pre_n[(pre_n > 0) & (pre_n == left_n)].index
no_pre = pre_n[pre_n == 0].index
print('linked households', len(linked), 'with s01 rows', s21['key'].nunique())
print('all pre-loaded members left:', len(all_left), ' no pre-loaded roster:', len(no_pre))
same_dwelling = c21.set_index('key').loc[list(all_left), 's00q07e'].astype(str).value_counts().to_dict()
print('s00q07e (same dwelling) among the all-left:', same_dwelling)
cur = s21l[s21['key'].isin(all_left) & s21['s01q00aa'].isna()]
for c in ('s01q00', 's01q00_0'):
    if c in cur:
        print(f'{c} of their current (not pre-loaded) members:', cur[c].astype(str).value_counts().to_dict())
print('their current members:', int((s21['key'].isin(all_left) & (s21['s01q00aa'] != 2)).sum()))
print('DONE')
