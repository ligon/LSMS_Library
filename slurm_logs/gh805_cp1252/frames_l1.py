#!/usr/bin/env python
"""GH #805: hash what ``get_dataframe`` SERVES for every C1 file a census found.

``census_l1.py`` judges files with the predicate; this script reads each one
through ``lsms_library.local_tools.get_dataframe`` itself (the served decode,
value labels substituted) and hashes the frame -- columns, dtypes, index
names and ``hash_pandas_object`` -- so two code versions can be compared
frame by frame.  Run it once per code version (``PYTHONPATH``) over the same
census, then pass the earlier output to ``--compare``.

Only blobs already in ``data_root()/dvc-cache`` are read (checked with
``local_tools._dvc_cache_path`` before the read), so nothing is fetched and
nothing is written to the blob store.  Point ``LSMS_DATA_DIR`` at a root whose
``dvc-cache`` is the store the census read.

Usage:

  PYTHONPATH=<checkout> PYTHONSAFEPATH=1 OPENBLAS_NUM_THREADS=1 \\
    <venv>/bin/python slurm_logs/gh805_cp1252/frames_l1.py \\
      --census census.jsonl --out frames.jsonl [--nproc 6] \\
      [--compare <frames.jsonl from the other code version>]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import warnings
from multiprocessing import Pool


def probe(rel):
    import pandas as pd
    from lsms_library import local_tools as lt
    from lsms_library.paths import countries_root
    rec = {'rel': rel}
    path = str(countries_root() / rel)
    if lt._dvc_cache_path(path) is None:
        rec['skipped'] = 'blob not in data_root()/dvc-cache'
        return rec
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            df = lt.get_dataframe(path)
    except Exception as e:  # noqa: BLE001 -- record, never stop
        rec['error'] = f'{type(e).__name__}: {e}'[:200]
        return rec
    h = hashlib.sha256()
    h.update(json.dumps([str(c) for c in df.columns]).encode())
    h.update(json.dumps([str(t) for t in df.dtypes]).encode())
    h.update(json.dumps([str(n) for n in df.index.names]).encode())
    try:
        h.update(pd.util.hash_pandas_object(df, index=True).values.tobytes())
    except TypeError:
        h.update(df.to_csv().encode('utf-8', 'surrogatepass'))
    rec['hash'] = h.hexdigest()[:24]
    rec['shape'] = list(df.shape)
    c1 = 0
    for c in df.columns:
        s = df[c]
        if s.dtype == object or pd.api.types.is_string_dtype(s) \
                or isinstance(s.dtype, pd.CategoricalDtype):
            vc = s.astype(object).dropna().value_counts()
            c1 += int(sum(n for v, n in vc.items()
                          if isinstance(v, str) and lt._C1_RE.search(v)))
    rec['c1_cells'] = c1
    return rec


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--census', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--nproc', type=int, default=6)
    ap.add_argument('--all', action='store_true',
                    help='every census file, not only those carrying a C1 code point')
    ap.add_argument('--compare', default=None)
    args = ap.parse_args()
    from lsms_library import local_tools as lt
    sys.stderr.write(lt.__file__ + '\n')
    recs = [json.loads(l) for l in open(args.census)]
    rels = sorted(r['rel'] for r in recs
                  if args.all or r.get('verdict') not in ('clean', 'error', None))
    out = {}
    with Pool(args.nproc, maxtasksperchild=10) as pool, open(args.out, 'w') as fh:
        for rec in pool.imap_unordered(probe, rels, chunksize=1):
            out[rec['rel']] = rec
            fh.write(json.dumps(rec) + '\n')
            fh.flush()
    n_err = sum('error' in r for r in out.values())
    n_skip = sum('skipped' in r for r in out.values())
    print(f'{len(out)} files read: {len(out) - n_err - n_skip} hashed, '
          f'{n_err} errors, {n_skip} skipped (blob not held); '
          f'C1 cells served: {sum(r.get("c1_cells", 0) for r in out.values()):,}')
    if args.compare:
        other = {json.loads(l)['rel']: json.loads(l) for l in open(args.compare)}
        diff = [rel for rel in sorted(set(out) | set(other))
                if (out.get(rel, {}).get('hash'), out.get(rel, {}).get('error'))
                != (other.get(rel, {}).get('hash'), other.get(rel, {}).get('error'))]
        print(f'{len(diff)} file(s) differ from {args.compare}:')
        for rel in diff:
            a, b = other.get(rel, {}), out.get(rel, {})
            print(f'  {rel}: shape {a.get("shape")} -> {b.get("shape")}, '
                  f'C1 cells {a.get("c1_cells")} -> {b.get("c1_cells")}, '
                  f'error {a.get("error")} -> {b.get("error")}')


if __name__ == '__main__':
    main()
