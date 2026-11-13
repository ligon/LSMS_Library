import os, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import pandas as pd
from lsms_library.local_tools import get_dataframe, id_walk
C = f'{checkout}/lsms_library/countries/Mali'
sys.path.insert(0, f'{C}/_'); os.chdir(f'{C}/2017-18/_')
from mali import i as mi, _crop_labels
hh = lambda d, k: d.apply(lambda r: mi(pd.Series([r['grappe'], r[k]])), axis=1)
f = get_dataframe('../Data/eaci17_s7fp2.dta')
pl = (f['s7fq01'].astype('Int64').astype('string') + '_' + f['s7fq02'].astype('Int64').astype('string'))
fh = pd.DataFrame({'i': hh(f, 'exploitation'), 'plot_id': pl, 'raw': f['s7fq03'].astype('string').values, 'crop': _crop_labels(f['s7fq03']).values})
npl = fh.dropna(subset=['plot_id']).groupby(['i', 'raw'])['plot_id'].nunique().rename('np').reset_index()
g = get_dataframe('../Data/eaci17_s7gp2.dta')
s = pd.DataFrame({'i': hh(g, 'exploitation'), 'raw': g['s7gq01'].astype('string').values, 'flag': g['s7gq20'].astype('string').values,
                  'qna': pd.to_numeric(g['s7gq21a'], errors='coerce').isna().values, 'vna': pd.to_numeric(g['s7gq22'], errors='coerce').isna().values})
x = fh.merge(s.merge(npl, on=['i', 'raw'], how='left'), on=['i', 'raw'], how='inner')
x = x[x.np.fillna(0) <= 1]
x['t'] = '2017-18'
x = id_walk(x.set_index(['t', 'i']), ll.Country('Mali').updated_ids).reset_index()
n = pd.read_parquet(f'/local/job40390255/data-private/824-ml/out/{label}_served.parquet').reset_index()
j = n[(n.t == '2017-18') & ~n.perennial.fillna(False)].merge(x, on=['t', 'i', 'plot_id', 'crop'], how='inner')
nn = ~j.flag.eq('Non').fillna(False)
print('served single-plot 2017-18 rows joined', len(j))
print(' served Quantity_sold == 0 where the raw answer is NOT Non and the raw quantity is NA:', int((nn & j.qna & (j.Quantity_sold == 0)).sum()),
      '(flag NA:', int((j.flag.isna() & j.qna & (j.Quantity_sold == 0)).sum()), ', flag Oui:', int((j.flag.eq('Oui').fillna(False) & j.qna & (j.Quantity_sold == 0)).sum()), ')')
print(' served Value_sold == 0 likewise:', int((nn & j.vna & (j.Value_sold == 0)).sum()))
