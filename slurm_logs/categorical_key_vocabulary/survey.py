"""Corpus survey behind the categorical-key / ignorance-vocabulary proposal."""
import json, re, sys
from collections import Counter, defaultdict
import pandas as pd
import lsms_library
assert 'job38854624' in lsms_library.__file__, lsms_library.__file__
from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root

root = countries_root()
NUMERICISH = re.compile(r'^-?\d+(\.\d+)?$')
KEYNAMES = {'code', 'original label', 'alternate spelling', 'label'}

tables = []
for cdir in sorted(p for p in root.iterdir() if p.is_dir()):
    files = [cdir / '_' / 'categorical_mapping.org'] + sorted(cdir.glob('*/_/categorical_mapping.org'))
    for f in files:
        if not f.exists():
            continue
        scope = cdir.name if f.parent.parent == cdir else f"{cdir.name}/{f.parent.parent.name}"
        try:
            d = all_dfs_from_orgfile(f)
        except Exception as e:
            tables.append({'scope': scope, 'table': '<PARSE ERROR>', 'err': str(e)})
            continue
        for name, t in d.items():
            cols = [str(c) for c in t.columns]
            has_pref = 'Preferred Label' in cols
            src = [c for c in cols if c != 'Preferred Label']
            key = src[0] if src else None
            # key-type: what do the key's values look like?
            ktype = None
            if key is not None and not hasattr(t[key], 'columns'):
                vals = [str(v).strip() for v in t[key] if str(v).strip() not in ('', 'nan', 'None', '<NA>')]
                if vals:
                    num = sum(1 for v in vals if NUMERICISH.match(v))
                    ktype = 'numeric' if num == len(vals) else ('mixed' if num else 'label')
            # does the table offer BOTH a code-ish and a label-ish column?
            codeish = [c for c in src if c.lower() in ('code', 'codes') or c.lower().endswith('code')]
            labelish = [c for c in src if c.lower() in ('original label', 'alternate spelling', 'label')]
            # ignorance spellings in Preferred Label
            spell = Counter()
            if has_pref and not hasattr(t['Preferred Label'], 'columns'):
                s = t['Preferred Label']
                for v in s:
                    if pd.isna(v):
                        spell['NaN'] += 1
                    else:
                        sv = str(v).strip()
                        if sv == '':
                            spell['empty-string'] += 1
                        elif sv == '---':
                            spell['---'] += 1
                        elif sv == '.':
                            spell['.'] += 1
                        elif sv in ('Unknown', 'unknown'):
                            spell['Unknown'] += 1
                        elif sv in ('Manquant', 'manquant'):
                            spell['Manquant'] += 1
            tables.append({'scope': scope, 'table': name, 'cols': cols, 'key': key,
                           'ktype': ktype, 'has_pref': has_pref,
                           'codeish': codeish, 'labelish': labelish,
                           'nrows': len(t), 'spell': dict(spell)})
json.dump(tables, open(sys.argv[1], 'w'), indent=0, default=str)
print(f"{len(tables)} tables surveyed -> {sys.argv[1]}")
