"""Red team (malawi): cold build of one arm.  usage: python build_arm.py <clone> <arm> <outdir>

Saves crop_production, plot_inputs and the crop/input transforms as parquet, every
warning (category, message head), the per-wave replay of malawi._crop_pieces +
assemble_crop_production (attrs, and the raw I/Q lines with their `crop` label),
and, for every registered crop_production / plot_inputs derivation, its inputs per
wave plus a join count of the served keyed rows against them.
"""
import importlib.util
import json
import os
import sys
import time
import traceback
import warnings
from pathlib import Path

import pandas as pd

clone, arm, out = sys.argv[1], sys.argv[2], Path(sys.argv[3])
out.mkdir(parents=True, exist_ok=True)
import lsms_library as ll  # noqa: E402
from lsms_library import paths  # noqa: E402
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(clone) + os.sep), ll.__file__
dr = os.path.realpath(str(paths.data_root()))
assert dr.startswith('/local/job') or '/isa-sm-20261113/private/' in dr, dr
assert os.path.realpath(str(paths.countries_root())).startswith(os.path.realpath(clone) + os.sep)
from lsms_library import transformations as T  # noqa: E402

MW = paths.countries_root() / 'Malawi' / '_'
sys.path.insert(0, str(MW))
spec = importlib.util.spec_from_file_location('malawi_rt', MW / 'malawi.py')
mal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mal)

log = {'arm': arm, 'tables': {}, 'replay': {}, 'djoin': {}}


def dump():
    (out / f'{arm}.log.json').write_text(json.dumps(log, indent=1, default=str))


def save(df, name):
    d = df.copy()
    d.attrs = {}
    try:
        d.to_parquet(out / f'{arm}.{name}.parquet')
    except Exception:  # noqa: BLE001
        for c in d.columns:
            if d[c].dtype == object:
                d[c] = d[c].map(lambda x: x if pd.isna(x) else str(x)).astype('string')
        d.to_parquet(out / f'{arm}.{name}.parquet')


def run(name, fn):
    t0 = time.time()
    rec = {}
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            df = fn()
            rec['rows'] = int(len(df))
            rec['index'] = [str(x) for x in df.index.names]
            rec['columns'] = [str(x) for x in df.columns]
            rec['attrs_keys'] = sorted(map(str, df.attrs))
            save(df, name)
        except Exception as e:  # noqa: BLE001
            df = None
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['tb'] = traceback.format_exc()[-3000:]
    rec['warnings'] = [f'{x.category.__name__}: {str(x.message)[:1500]}' for x in w]
    rec['seconds'] = round(time.time() - t0, 1)
    log['tables'][name] = rec
    print(arm, name, rec.get('rows'), rec.get('error'), rec['seconds'], flush=True)
    dump()
    return df


c = ll.Country('Malawi')
cp = run('crop_production', lambda: c.crop_production())
pi = run('plot_inputs', lambda: c.plot_inputs())
if cp is not None:
    run('harvest_kg', lambda: T.harvest_kg(cp))
    run('harvest_kg_shipped', lambda: T.harvest_kg(mal.with_region(cp), shipped_factors=mal.crop_conversion_factors()))
    run('crop_diversity', lambda: T.crop_diversity(cp))
    run('gross_crop_revenue', lambda: T.gross_crop_revenue(cp))
if pi is not None:
    run('seed_kg', lambda: T.seed_kg(pi))
    run('nitrogen_kg', lambda: T.nitrogen_kg(pi))

for wv in ('2010-11', '2013-14', '2016-17', '2019-20'):
    t0 = time.time()
    rec = {}
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            harvest, sale, raw = mal._crop_pieces(wv)
            frame = mal.assemble_crop_production(wv, harvest, sale)
        rec['attrs'] = json.loads(json.dumps(frame.attrs, default=str))
        rec['rows'] = int(len(frame))
        r = raw.copy()
        save(r, f'raw_{wv}')
        rec['raw_rows'] = int(len(r))
        rec['raw_crop_counts'] = {str(k): int(v) for k, v in r['crop'].astype(str).value_counts().items()
                                  if k in ('Other', 'Cowpea (Khobwe)')}
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
        rec['tb'] = traceback.format_exc()[-3000:]
    rec['seconds'] = round(time.time() - t0, 1)
    log['replay'][wv] = rec
    print(arm, 'replay', wv, rec.get('rows'), rec.get('error'), flush=True)
    dump()

# every crop_production / plot_inputs derivation: inputs per wave, and a join count
recs = c.derivations()
for table, frame in (('crop_production', cp), ('plot_inputs', pi)):
    if frame is None:
        continue
    served = frame.reset_index()
    for key in sorted(k for k in recs if f'::{table}::' in k):
        for wv in ('2010-11', '2013-14', '2016-17', '2019-20'):
            rec = {}
            t0 = time.time()
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    inp = c.derivation_inputs(key, wave=wv)
                rec['inputs_rows'] = int(len(inp))
                ir = inp.reset_index()
                if 'crop' in ir.columns:
                    rec['inputs_crop_cowpea'] = int(ir['crop'].astype(str).eq('Cowpea (Khobwe)').sum())
                    rec['inputs_crop_other'] = int(ir['crop'].astype(str).eq('Other').sum())
                save(inp.reset_index(), f'dinp__{table}__{key.split("::")[-1]}__{wv}')
                if 'Derivation' in served.columns:
                    sub = served[(served['t'].astype(str) == wv)
                                 & served['Derivation'].astype('string').str.contains(key, regex=False).fillna(False)]
                    names = [n for n in inp.index.names if n in sub.columns]
                    rec['served_keyed'] = int(len(sub))
                    rec['join_on'] = names
                    if len(sub) and names:
                        kin = set(map(tuple, ir[names].astype(str).itertuples(index=False)))
                        hit = [tuple(x) in kin for x in sub[names].astype(str).itertuples(index=False)]
                        rec['joined'] = int(sum(hit))
                        cow = sub['crop'].astype(str).eq('Cowpea (Khobwe)').to_numpy() if 'crop' in sub else None
                        if cow is not None:
                            rec['served_keyed_cowpea'] = int(cow.sum())
                            rec['cowpea_joined'] = int(sum(h for h, k in zip(hit, cow) if k))
            except Exception as e:  # noqa: BLE001
                rec['error'] = f'{type(e).__name__}: {str(e)[:500]}'
            rec['seconds'] = round(time.time() - t0, 1)
            log['djoin'][f'{table}|{key}|{wv}'] = rec
            print(arm, 'djoin', key, wv, {k: v for k, v in rec.items() if k != 'join_on'}, flush=True)
            dump()
print('BUILD DONE', arm, flush=True)
