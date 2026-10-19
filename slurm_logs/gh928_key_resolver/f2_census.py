"""F2 census: does CategoricalKeyMismatchWarning fire on any real country?
And how many additive-merge fallbacks happen silently (undeclared)?"""
import json, sys, warnings
from pathlib import Path
import lsms_library
from lsms_library.country import (Country, CategoricalKeyMismatchWarning,
                                  _ADDITIVE_CATEGORICAL_TABLES, _categorical_key_column)
from lsms_library.local_tools import (CATEGORICAL_KEY_ATTR, load_categorical_level,
                                      categorical_mapping_files)
from lsms_library.catalog import _country_dirs
pkg = Path(lsms_library.__file__).resolve().parent
glob = load_categorical_level(pkg, on_unreadable=lambda p, e: None)
fired, fallbacks, aligned, declared, n = [], [], [], [], 0
for c in sorted(_country_dirs()):
    C = Country(c)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            cm = C.categorical_mapping
        except Exception as e:
            print('ERR', c, type(e).__name__, e); continue
    n += 1
    fired += [(c, str(x.message)) for x in w if issubclass(x.category, CategoricalKeyMismatchWarning)]
    # independent re-derivation from the raw country level
    country_maps = {}
    for rel in ['./', '../']:
        level = Path(C.file_path / rel / '_')
        if categorical_mapping_files(level):
            country_maps = load_categorical_level(level); break
    for name, ctab in country_maps.items():
        if CATEGORICAL_KEY_ATTR in ctab.attrs:
            declared.append((c, name, ctab.attrs[CATEGORICAL_KEY_ATTR]))
        if name in _ADDITIVE_CATEGORICAL_TABLES and name in glob:
            gk, ck = _categorical_key_column(glob[name]), _categorical_key_column(ctab)
            (aligned if gk == ck else fallbacks).append((c, name, ck, gk))
print(json.dumps({'countries': n, 'warnings_fired': fired, 'declared_tables': declared,
                  'additive_aligned': len(aligned),
                  'additive_fallbacks_undeclared': fallbacks}, indent=1))
