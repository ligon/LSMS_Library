"""Cold-build Malawi cluster_features / sample / housing for one checkout; save to a PRIVATE dir."""
import os, sys, json, time, warnings, pickle
checkout, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
from lsms_library.country import grain_reports
from lsms_library.paths import data_root
print('lsms_library', ll.__file__, '| data_root', data_root(), flush=True)
c = ll.Country('Malawi')
out = {}
with warnings.catch_warnings(record=True) as W:
    warnings.simplefilter('always')
    t0 = time.time()
    for w in ['2016-17', '2019-20']:
        wf = c[w].grab_data('cluster_features')
        wf.to_parquet(f'{outdir}/wave_{w}.parquet')
        print('wave', w, wf.shape, f'{time.time()-t0:.0f}s', flush=True)
    cf = c.cluster_features(); cf.to_parquet(f'{outdir}/cluster_features.parquet')
    print('cluster_features', cf.shape, f'{time.time()-t0:.0f}s', flush=True)
    s = c.sample(); s.to_parquet(f'{outdir}/sample.parquet')
    print('sample', s.shape, f'{time.time()-t0:.0f}s', flush=True)
    h = c.housing(); h.to_parquet(f'{outdir}/housing.parquet')
    print('housing', h.shape, f'{time.time()-t0:.0f}s', flush=True)
rep = grain_reports('Malawi')
with open(f'{outdir}/grain_reports.pkl', 'wb') as f:
    pickle.dump(rep, f)
cats = {}
for w in W:
    k = w.category.__name__
    cats[k] = cats.get(k, 0) + 1
print('warning categories', cats)
with open(f'{outdir}/warnings.txt', 'w') as f:
    for w in W:
        f.write(f'{w.category.__name__}: {str(w.message)[:600]}\n')
