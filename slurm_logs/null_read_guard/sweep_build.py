"""Build every declared table for every country and record which DECLARED
columns are 100% null -- whole-table and per-wave (`t` slice).

This is the SITE-B measurement: what a guard on the served frame would see.

Usage: python sweep_build.py <out.jsonl> <shard> <nshards>
"""
import json, sys, os, warnings, time, traceback
warnings.simplefilter('ignore')

import lsms_library
assert 'worktrees' in lsms_library.__file__, lsms_library.__file__

import pandas as pd
from pathlib import Path
from lsms_library.country import (Country, _required_scheme_columns,
                                  _SCHEME_NON_COLUMN_KEYS)
from lsms_library.paths import countries_root

SKIP = {'panel_ids', 'updated_ids'}


def declared_columns(entry):
    """All declared columns (required + optional), and the required subset."""
    if not isinstance(entry, dict):
        return [], []
    req = list(_required_scheme_columns(entry))
    allc = []
    for k, v in entry.items():
        if k in _SCHEME_NON_COLUMN_KEYS:
            continue
        allc.append(k)
    return allc, req


def main(out_path, shard, nshards):
    root = Path(countries_root())
    countries = sorted(p.name for p in root.iterdir()
                       if p.is_dir() and (p / '_').is_dir() and not p.name.startswith('.'))
    countries = [c for i, c in enumerate(countries) if i % nshards == shard]
    with open(out_path, 'w', buffering=1) as fh:
        for cname in countries:
            try:
                C = Country(cname)
                scheme = (C.resources or {}).get('Data Scheme') or {}
                tables = [t for t in C.data_scheme if t not in SKIP]
            except Exception as e:
                fh.write(json.dumps({'country': cname, 'status': 'country-error',
                                     'err': f'{type(e).__name__}: {e}'}) + '\n')
                continue
            for table in tables:
                t0 = time.time()
                rec = {'country': cname, 'table': table}
                try:
                    df = getattr(C, table)()
                except Exception as e:
                    rec.update({'status': 'build-error', 'secs': round(time.time()-t0, 1),
                                'err': f'{type(e).__name__}: {e}'[:300]})
                    fh.write(json.dumps(rec) + '\n')
                    continue
                rec['secs'] = round(time.time()-t0, 1)
                if not isinstance(df, pd.DataFrame):
                    rec['status'] = 'not-a-frame'
                    fh.write(json.dumps(rec) + '\n')
                    continue
                entry = scheme.get(table)
                alld, reqd = declared_columns(entry)
                rec['status'] = 'ok'
                rec['nrow'] = int(len(df))
                rec['cols'] = [str(c) for c in df.columns]
                rec['declared'] = alld
                rec['required'] = reqd
                whole = []
                for c in df.columns:
                    try:
                        if len(df) and df[c].isna().all():
                            whole.append(str(c))
                    except Exception:
                        pass
                rec['whole_allnull'] = whole
                # per-wave
                perwave = {}
                if 't' in (df.index.names or []) and len(df):
                    try:
                        tv = df.index.get_level_values('t')
                        for t in pd.unique(tv):
                            sl = df[tv == t]
                            bad = []
                            for c in df.columns:
                                try:
                                    if len(sl) and sl[c].isna().all():
                                        bad.append(str(c))
                                except Exception:
                                    pass
                            if bad:
                                perwave[str(t)] = {'n': int(len(sl)), 'allnull': bad}
                    except Exception as e:
                        rec['perwave_err'] = f'{type(e).__name__}: {e}'
                rec['perwave'] = perwave
                fh.write(json.dumps(rec) + '\n')
                del df
    print(f'shard {shard}/{nshards} done')


if __name__ == '__main__':
    main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
