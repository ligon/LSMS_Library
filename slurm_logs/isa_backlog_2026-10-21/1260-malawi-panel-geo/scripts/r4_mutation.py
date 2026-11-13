"""Round 4: end-to-end fail-closed mutation.  Run with LSMS_COUNTRIES_ROOT at a
copied config tree whose 2013-14 df_main misspells dist_to_IHS3location, and a
cold LSMS_DATA_DIR.  Prints the outcome (counts only)."""
import os, subprocess, warnings
import lsms_library as ll
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
from lsms_library.paths import countries_root
print('countries_root', countries_root())
c = ll.Country('Malawi')
print('-- wave level: Country("Malawi")["2013-14"].grab_data("cluster_features")')
try:
    wf = c['2013-14'].grab_data('cluster_features')
    print('   returned', wf.shape)
except Exception as e:  # noqa: BLE001
    print('   raised', type(e).__name__, str(e)[:160])
print('-- country level: Country("Malawi").cluster_features()')
with warnings.catch_warnings(record=True) as W:
    warnings.simplefilter('always')
    try:
        cf = c.cluster_features()
        print('   returned', cf.shape, '| waves', sorted(set(cf.index.get_level_values('t'))))
    except Exception as e:  # noqa: BLE001
        print('   raised', type(e).__name__, str(e)[:200])
for w in W:
    s = str(w.message)
    if 'IHS3location' in s or 'cluster_features' in s or 'Makefile' in s:
        print('   warning', w.category.__name__, s[:200].replace('\n', ' '))
