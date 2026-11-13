"""2017-18 s7g: 'Oui, sold' rows without a quantity -- the Lourd / Leger split?
Aggregates only."""
import os, sys
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
import pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
root = os.path.join(countries_root(), 'Mali', '2017-18', 'Data')
g = get_dataframe(os.path.join(root, 'eaci17_s7gp2.dta'))
c = get_dataframe(os.path.join(root, 'eaci17_s00p2.dta'))[['grappe', 'exploitation', 'typequest', 's0q04']]
print('s7g rows', len(g), 'cover rows', len(c), 'cover dup keys', int(c.duplicated(['grappe', 'exploitation']).sum()))
x = g.merge(c, on=['grappe', 'exploitation'], how='left', validate='m:1')
x['flag'] = x['s7gq20'].astype('string').fillna('<NA>')
x['tq'] = x['typequest'].astype('string').fillna('<NA>')
x['qty'] = pd.to_numeric(x['s7gq21a'], errors='coerce')
x['val'] = pd.to_numeric(x['s7gq22'], errors='coerce')
x['has_q'] = x['qty'].notna()
x['has_u'] = x['s7gq21b'].notna()
x['has_v'] = x['val'].notna()
print('\ns7gq20 by typequest (rows):')
print(pd.crosstab(x['flag'], x['tq'], margins=True))
oui = x[x['flag'] == 'Oui']
print('\nOui rows: has quantity / unit / value, by typequest')
print(oui.groupby('tq')[['has_q', 'has_u', 'has_v']].agg(['sum', 'count']))
noq = oui[~oui.has_q & ~oui.has_u]
print('\nOui rows with NO quantity AND NO unit:', len(noq), ' by typequest:', noq['tq'].value_counts().to_dict())
print('  ...of which a value is recorded:', int(noq.has_v.sum()), ' value > 0:', int((noq.val > 0).sum()))
print('  ...by milieu:', noq['s0q04'].astype('string').value_counts().to_dict())
hh = lambda d: d[['grappe', 'exploitation']].drop_duplicates()
print('\nhouseholds: Oui', len(hh(oui)), ' Oui-without-qty-and-unit', len(hh(noq)),
      ' of which Leger', len(hh(noq[noq.tq == 'Questionnaire Leger'])), ' Lourd', len(hh(noq[noq.tq == 'Questionnaire Lourd'])))
# all Oui rows of a household lack the quantity?
per = oui.groupby(['grappe', 'exploitation']).agg(any_q=('has_q', 'any'), tq=('tq', 'first'))
print('Oui households with no quantity on ANY sold crop:', int((~per.any_q).sum()), ' by tq:', per[~per.any_q]['tq'].value_counts().to_dict())
print('Oui households with a quantity on some crop:', int(per.any_q.sum()), ' by tq:', per[per.any_q]['tq'].value_counts().to_dict())
# Unanswered flag
na = x[x['flag'] == '<NA>']
print('\nunanswered s7gq20 rows', len(na), ' by tq', na['tq'].value_counts().to_dict(), ' with qty', int(na.has_q.sum()), ' with val', int(na.has_v.sum()))
# Non rows with a qty/value?
non = x[x['flag'] == 'Non']
print('Non rows', len(non), ' with qty', int(non.has_q.sum()), ' with val', int(non.has_v.sum()))
# other s7g columns populated for Leger? (is the whole module short?)
cols = [k for k in g.columns if k.startswith('s7gq')]
fill = x.groupby('tq')[cols].apply(lambda d: d.notna().mean().round(3))
print('\nshare non-null per column by typequest (s7g):')
print(fill.T.to_string())
