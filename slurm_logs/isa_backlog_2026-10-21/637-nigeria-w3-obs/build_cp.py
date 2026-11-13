"""Cold build of Country('Nigeria').community_prices(); save row-level result
to the private scratch dir (never committed)."""
import sys, time, warnings, os
import lsms_library, subprocess
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.country import Country
which = sys.argv[1]
t0 = time.time()
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    cp = Country('Nigeria').community_prices()
print('built in', round(time.time() - t0, 1), 's; index', cp.index.names, 'shape', cp.shape)
cats = {}
for x in w:
    k = type(x.message).__name__
    cats[k] = cats.get(k, 0) + 1
print('warnings by class', cats)
for x in w:
    if 'Grain' in type(x.message).__name__ or 'community' in str(x.message).lower():
        print('  ', type(x.message).__name__, str(x.message)[:300])
cp.to_parquet(f'/local/job40390255/data-private/637-ng/scratch/cp_{which}.parquet')
print(cp.reset_index().groupby('t').size().to_dict())
