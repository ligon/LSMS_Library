#!/usr/bin/env python
"""Cold build of Mali food_acquired into $LSMS_DATA_DIR (must be an EMPTY root
holding only a dvc-cache symlink); pickles the served frame and the 2014-15
_unit_canonicalizer() dict.  argv[1] = output prefix."""
import importlib.util, json, os, pickle, sys, time, warnings
import lsms_library
from lsms_library.country import Country
from lsms_library.paths import countries_root
prefix = sys.argv[1]
root = os.environ['LSMS_DATA_DIR']
assert not os.path.exists(os.path.join(root, 'Mali')), 'root is not cold'
spec = importlib.util.spec_from_file_location(
    'mali_1415_mapping', countries_root() / 'Mali' / '2014-15' / '_' / 'mapping.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
umap = m._unit_canonicalizer()
t0 = time.time()
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    fa = Country('Mali').food_acquired()
el = time.time() - t0
pickle.dump({'food_acquired': fa, 'unit_map': umap,
             'warnings': sorted({f"{x.category.__name__}: {str(x.message)[:160]}" for x in w}),
             'lsms_library': lsms_library.__file__, 'seconds': el},
            open(prefix + '.pkl', 'wb'))
u = fa.index.get_level_values('u')
t = fa.index.get_level_values('t')
print(json.dumps({'rows': len(fa), 'rows_2014_15': int((t == '2014-15').sum()),
                  'u_distinct_2014_15': int(u[t == '2014-15'].nunique()),
                  'unit_map_len': len(umap), 'seconds': round(el, 1),
                  'lib': lsms_library.__file__}))
