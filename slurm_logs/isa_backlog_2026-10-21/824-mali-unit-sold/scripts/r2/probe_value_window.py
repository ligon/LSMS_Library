"""Is the tree sale VALUE a total or a monthly figure?  Within crop x sold-unit
cells, regress log(value / monthly quantity sold) on log(months harvested):
slope ~1 => the value is a TOTAL over the months; slope ~0 => it is MONTHLY.
Aggregates only (coefficients, n)."""
import os, sys
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
import numpy as np, pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root
os.chdir(os.path.join(countries_root(), 'Mali', '_')); sys.path.insert(0, '.')
import mali
root = os.path.join(countries_root(), 'Mali')
def num(s): return pd.to_numeric(s, errors='coerce')
def fit(d, label):
    d = d[(d.qs > 0) & (d.v > 0) & (d.m >= 1) & ~mali._is_quantity_sentinel(d.qs)].copy()
    d['y'] = np.log(d.v / d.qs); d['x'] = np.log(d.m.astype(float))
    d['cell'] = d.crop.astype(str) + '|' + d.us.astype(str)
    # within-cell demeaning (crop x sold-unit fixed effects); keep cells with variation in months
    g = d.groupby('cell')
    d = d[g.x.transform('nunique') > 1]
    yd = d.y - d.groupby('cell').y.transform('mean'); xd = d.x - d.groupby('cell').x.transform('mean')
    b = float((xd * yd).sum() / (xd ** 2).sum())
    resid = yd - b * xd
    dof = len(d) - d.cell.nunique() - 1
    se = float(np.sqrt((resid ** 2).sum() / dof / (xd ** 2).sum()))
    print(f'{label}: slope of log(value/monthly qty sold) on log(months) = {b:.2f} (se {se:.2f}); '
          f'n={len(d)} rows in {d.cell.nunique()} crop x unit cells with varying months')
    # also: pooled, no FE
    bb = np.polyfit(d.x, d.y, 1)[0]
    med = d.assign(r=np.exp(d.y)).groupby('m').r.agg(['median', 'size'])
    print(f'   pooled (no FE) slope {bb:.2f}; median value/monthly-qty by months (groups of n >= 5): '
          + ', '.join(f'{int(k)}m:{v:.0f}(n={int(c)})' for k, (v, c) in med[med['size'] >= 5].iterrows()))
b = mali.producing_trees(get_dataframe(os.path.join(root, '2014-15', 'Data', 'EACIS3B_p2.dta')), '2014-15')
fit(pd.DataFrame({'crop': b.s3bq01, 'us': b.s3bq13b, 'qs': num(b.s3bq13a), 'v': num(b.s3bq14),
                  'm': mali._months_harvested(b.s3bq09)}), '2014-15 3B (s3bq14 / s3bq13a)')
f = mali.producing_trees(get_dataframe(os.path.join(root, '2017-18', 'Data', 'eaci17_s11fp1.dta')), '2017-18')
fit(pd.DataFrame({'crop': f.s11fq01, 'us': f.s11fq14b, 'qs': num(f.s11fq14a), 'v': num(f.s11fq15),
                  'm': mali._months_harvested(f.s11fq10)}), '2017-18 11F (s11fq15 / s11fq14a)')
# a harvest-side control: the HARVEST per month should not scale with months either way
fit(pd.DataFrame({'crop': f.s11fq01, 'us': f.s11fq11b, 'qs': num(f.s11fq11a), 'v': num(f.s11fq15),
                  'm': mali._months_harvested(f.s11fq10)}), '2017-18 11F (s11fq15 / s11fq11a, harvest denominator)')
