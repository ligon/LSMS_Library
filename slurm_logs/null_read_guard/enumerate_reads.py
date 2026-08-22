"""Enumerate every source file the corpus declares, with the raw columns each
table asks of it.  Writes a JSON manifest; reads nothing.

Usage: python enumerate_reads.py <out.json>
"""
import json, sys, os, warnings, traceback
warnings.simplefilter('ignore')

import lsms_library
assert 'worktrees' in lsms_library.__file__, lsms_library.__file__

from lsms_library.country import Country, Wave
from lsms_library.paths import countries_root
from lsms_library.local_tools import scan_script_data_refs
from pathlib import Path


def _raw_cols(spec):
    """Raw source column name(s) a column_mapping value refers to."""
    if isinstance(spec, str):
        return [spec]
    if isinstance(spec, tuple):
        src = spec[0]
        if isinstance(src, str):
            return [src]
        if isinstance(src, (list, tuple)):
            return [s for s in src if isinstance(s, str)]
        return []
    if isinstance(spec, (list, tuple)):
        return [s for s in spec if isinstance(s, str)]
    return []


def main(out_path):
    root = countries_root()
    countries = sorted(p.name for p in Path(root).iterdir()
                       if p.is_dir() and (p / '_').is_dir() and not p.name.startswith('.'))
    manifest = []          # one record per (country, wave, table, subdf, file)
    errors = []
    for cname in countries:
        try:
            C = Country(cname)
            waves = list(C.waves)
        except Exception as e:
            errors.append({'country': cname, 'stage': 'country', 'err': f'{type(e).__name__}: {e}'})
            continue
        for w in waves:
            try:
                W = C[w]
                res = W.resources or {}
            except Exception as e:
                errors.append({'country': cname, 'wave': w, 'stage': 'resources',
                               'err': f'{type(e).__name__}: {e}'})
                continue
            if not isinstance(res, dict):
                continue
            for table, di in res.items():
                if not isinstance(di, dict):
                    continue
                blocks = []
                if di.get('dfs'):
                    for sub in di['dfs']:
                        sdi = di.get(sub)
                        if isinstance(sdi, dict):
                            blocks.append((sub, sdi))
                else:
                    blocks.append((None, di))
                for subname, bdi in blocks:
                    try:
                        cm = W.column_mapping(subname or table, bdi)
                    except Exception as e:
                        errors.append({'country': cname, 'wave': w, 'table': table,
                                       'sub': subname, 'stage': 'column_mapping',
                                       'err': f'{type(e).__name__}: {e}'})
                        continue
                    for fname, m in cm.items():
                        if fname == 'df_edit' or not isinstance(m, dict):
                            continue
                        cols = {}
                        for kind in ('idxvars', 'myvars'):
                            for outc, spec in (m.get(kind) or {}).items():
                                for rc in _raw_cols(spec):
                                    cols.setdefault(rc, []).append(
                                        {'out': outc, 'kind': kind})
                        manifest.append({
                            'country': cname, 'wave': w, 'table': table,
                            'sub': subname, 'file': fname, 'route': 'yaml',
                            'cols': cols,
                        })
            # script-path source refs for this wave
            wdir = Path(root) / cname / W.wave_folder / '_'
            if wdir.is_dir():
                for py in sorted(wdir.glob('*.py')):
                    refs = sorted(set(scan_script_data_refs(py)))
                    for r in refs:
                        manifest.append({
                            'country': cname, 'wave': w, 'table': py.stem,
                            'sub': None, 'file': r, 'route': 'script',
                            'cols': {},
                        })
        # country-level scripts
        cdir = Path(root) / cname / '_'
        if cdir.is_dir():
            for py in sorted(cdir.glob('*.py')):
                for r in sorted(set(scan_script_data_refs(py))):
                    manifest.append({
                        'country': cname, 'wave': None, 'table': py.stem,
                        'sub': None, 'file': r, 'route': 'script-country',
                        'cols': {},
                    })
    json.dump({'manifest': manifest, 'errors': errors}, open(out_path, 'w'))
    files = {(m['country'], m['wave'], m['file'], m['route']) for m in manifest}
    print(f'countries={len(countries)} records={len(manifest)} '
          f'distinct (country,wave,file)={len(files)} errors={len(errors)}')
    from collections import Counter
    print('by route:', Counter(m['route'] for m in manifest))
    print('errors by stage:', Counter(e['stage'] for e in errors))


if __name__ == '__main__':
    main(sys.argv[1])
