"""Raw oracle for Unit_sold and the tree columns, against the served (branch) table.
Keys use mali.i / mali._crop_labels (unchanged by the branch); unit labels are
canonicalised with the u table read straight from the org file, not via
mali._unit_labels.  Aggregates only."""
import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import pandas as pd, numpy as np
from lsms_library.local_tools import get_dataframe, id_walk, df_from_orgfile
C = f'{checkout}/lsms_library/countries/Mali'
sys.path.insert(0, f'{C}/_')
from mali import i as mali_i, _crop_labels
u_tab = df_from_orgfile(f'{C}/_/categorical_mapping.org', name='u')
canon = dict(zip(u_tab['Code'].astype(str).str.strip(), u_tab['Preferred Label']))
def cu(s):
    return s.astype('string').str.strip().map(lambda v: canon.get(v, pd.NA) if pd.notna(v) else pd.NA).astype('string')
mali = ll.Country('Mali'); upd = mali.updated_ids
served = pd.read_parquet(f'/local/job40390255/data-private/824-ml/out/{label}_served.parquet')
sv = served.reset_index()
def walk(df):
    f = id_walk(df.set_index(['t', 'i']), upd).reset_index()
    return f
def hh(df, key):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r[key]])), axis=1)
def plot(df, a, b):
    f = df[a].astype('Int64').astype('string'); p = df[b].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)
def report(name, exp, keys, col, scol='Unit_sold'):
    m = sv.merge(exp, on=keys, how='inner', suffixes=('', '_exp'))
    a, e = m[scol], m[col]
    ok = ((a == e) | (a.isna() & e.isna())).fillna(False)
    print(f'{name}: served rows joined {len(m)}; {scol} == oracle on {int(ok.sum())}; mismatches {int((~ok).sum())}')
    return m
# 2014-15 field
os.chdir(f'{C}/2014-15/_')
s = get_dataframe('../Data/EACIS3A_p2.dta')
e = pd.DataFrame({'t': '2014-15', 'i': hh(s, 'menage'), 'plot_id': plot(s, 's3aq01', 's3aq02'),
                  'crop': _crop_labels(s['s3aq03b']).values, 'su': cu(s['s3aq23b']).values,
                  'flag': s['s3aq22'].astype('string').values})
e.loc[e.flag.eq('Non'), 'su'] = pd.NA
e = e.dropna(subset=['crop'])
g = e.groupby(['t', 'i', 'plot_id', 'crop'], dropna=False)['su']
exp = pd.DataFrame({'n': g.nunique(), 'su_first': g.first()}).reset_index()
exp['exp_unit_sold'] = exp.su_first.where(exp.n == 1, pd.NA)
exp = walk(exp)
m = report('2014-15 field', exp[['t', 'i', 'plot_id', 'crop', 'exp_unit_sold']], ['t', 'i', 'plot_id', 'crop'], 'exp_unit_sold')
print('  served 2014-15 field rows', int(((sv.t == '2014-15') & ~sv.perennial.fillna(False)).sum()))
# 2014-15 trees
b = get_dataframe('../Data/EACIS3B_p2.dta')
b = b[b['s3bq01'].notna() & b['s3bq03'].astype('string').str.strip().eq('Oui')]
months = pd.to_numeric(b['s3bq09'], errors='coerce')
q = pd.to_numeric(b['s3bq10a'], errors='coerce'); qs = pd.to_numeric(b['s3bq13a'], errors='coerce')
sent = lambda x: x.isin([99, 9999]) | np.isclose(x.to_numpy(dtype='float64', na_value=np.nan), 9999.9)
e = pd.DataFrame({'t': '2014-15', 'i': hh(b, 'menage'), 'crop': _crop_labels(b['s3bq01']).values,
                  'exp_q': q.mask(sent(q)).values, 'exp_qs': qs.mask(sent(qs)).values,
                  'exp_v': pd.to_numeric(b['s3bq14'], errors='coerce').values,
                  'exp_m': months.where(months.between(0, 12)).astype('Int64').values,
                  'exp_unit_sold': cu(b['s3bq13b']).values, 'exp_u': cu(b['s3bq10b']).values,
                  'raw_months': months.values})
e = walk(e.dropna(subset=['crop']))
tv = sv[(sv.t == '2014-15') & sv.perennial.fillna(False)]
m = tv.merge(e, on=['t', 'i', 'crop'], how='inner')
print(f'2014-15 trees: served {len(tv)}, joined {len(m)}')
for a, ecol in [('Quantity', 'exp_q'), ('Quantity_sold', 'exp_qs'), ('Value_sold', 'exp_v'), ('months_harvested', 'exp_m'), ('Unit_sold', 'exp_unit_sold'), ('u', 'exp_u')]:
    ok = ((m[a].astype('float64') == m[ecol].astype('float64')) if a not in ('Unit_sold', 'u') else (m[a] == m[ecol]))
    ok = (ok | (m[a].isna() & m[ecol].isna())).fillna(False)
    print(f'  {a} == raw {ecol}: {int(ok.sum())} of {len(m)}')
print('  served Quantity == raw s3bq09 (the old wiring) where they differ from s3bq10a:',
      int(((m.Quantity == m.raw_months) & (m.exp_q != m.raw_months)).sum()))
# 2017-18 field
os.chdir(f'{C}/2017-18/_')
f = get_dataframe('../Data/eaci17_s7fp2.dta')
fh = pd.DataFrame({'i': hh(f, 'exploitation'), 'plot_id': plot(f, 's7fq01', 's7fq02'), 'rawcrop': f['s7fq03'].astype('string').values,
                   'crop': _crop_labels(f['s7fq03']).values})
nplots = fh.dropna(subset=['plot_id']).groupby(['i', 'rawcrop'])['plot_id'].nunique().rename('np').reset_index()
g7 = get_dataframe('../Data/eaci17_s7gp2.dta')
sale = pd.DataFrame({'i': hh(g7, 'exploitation'), 'rawcrop': g7['s7gq01'].astype('string').values,
                     'exp_unit_sold': cu(g7['s7gq21b']).values, 'flag': g7['s7gq20'].astype('string').values})
sale.loc[sale.flag.eq('Non'), 'exp_unit_sold'] = pd.NA
sale = sale.merge(nplots, on=['i', 'rawcrop'], how='left')
x = fh.merge(sale, on=['i', 'rawcrop'], how='left')
x.loc[x.np.fillna(0) > 1, 'exp_unit_sold'] = pd.NA
x['t'] = '2017-18'
x = walk(x.dropna(subset=['crop'])[['t', 'i', 'plot_id', 'crop', 'exp_unit_sold']])
report('2017-18 field', x, ['t', 'i', 'plot_id', 'crop'], 'exp_unit_sold')
# 2017-18 trees
t = get_dataframe('../Data/eaci17_s11fp1.dta')
t = t[t['s11fq01'].notna() & t['s11fq03'].astype('string').str.strip().eq('Oui')]
lab = t['s11fq10'].astype('string')
mo = lab.str.extract(r'^(\d+)\s*mois')[0].astype('Int64').where(~lab.str.startswith('Aucune'), 0)
e = pd.DataFrame({'t': '2017-18', 'i': hh(t, 'ménage'), 'crop': _crop_labels(t['s11fq01']).values,
                  'exp_q': pd.to_numeric(t['s11fq11a'], errors='coerce').values, 'exp_qs': pd.to_numeric(t['s11fq14a'], errors='coerce').values,
                  'exp_v': pd.to_numeric(t['s11fq15'], errors='coerce').values, 'exp_m': mo.values,
                  'exp_unit_sold': cu(t['s11fq14b']).values, 'exp_u': cu(t['s11fq11b']).values})
e = walk(e.dropna(subset=['crop']))
tv = sv[(sv.t == '2017-18') & sv.perennial.fillna(False)]
m = tv.merge(e, on=['t', 'i', 'crop'], how='inner')
print(f'2017-18 trees: served {len(tv)}, joined {len(m)}')
for a, ecol in [('Quantity', 'exp_q'), ('Quantity_sold', 'exp_qs'), ('Value_sold', 'exp_v'), ('months_harvested', 'exp_m'), ('Unit_sold', 'exp_unit_sold'), ('u', 'exp_u')]:
    ok = ((m[a].astype('float64') == m[ecol].astype('float64')) if a not in ('Unit_sold', 'u') else (m[a] == m[ecol]))
    ok = (ok | (m[a].isna() & m[ecol].isna())).fillna(False)
    print(f'  {a} == raw {ecol}: {int(ok.sum())} of {len(m)}')
