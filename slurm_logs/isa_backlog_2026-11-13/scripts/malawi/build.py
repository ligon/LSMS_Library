"""Cold-build Malawi crop_production / plot_inputs (+ crop-keyed transforms) for one arm.

usage: python build.py <clone> <arm> <framesdir>

Writes <framesdir>/<arm>.<name>.parquet for each frame, <arm>.build.json with
row counts by t, warnings (category + message head), the in-process ladder
replay tallies per wave (malawi._crop_pieces + assemble_crop_production), and
a derivation_inputs join check for every keyed crop_production row.
"""
import importlib.util
import json
import sys
import time
import traceback
import warnings
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, arm, outdir = sys.argv[1], sys.argv[2], Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)

import lsms_library as ll  # noqa: E402
from lsms_library import transformations as T  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

MW = countries_root() / 'Malawi' / '_'
sys.path.insert(0, str(MW))
spec = importlib.util.spec_from_file_location('malawi_build', MW / 'malawi.py')
mal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mal)

log = {'arm': arm, 'head': head, 'tables': {}}
FR = {}


def dump():
    (outdir / f'{arm}.build.json').write_text(json.dumps(log, indent=1, default=str))


def jsonable(x):
    return json.loads(json.dumps(x, default=str))


def run(name, fn, save=True):
    t0 = time.time()
    rec = {}
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            df = fn()
            FR[name] = df
            rec['rows'] = int(len(df))
            rec['index'] = list(map(str, df.index.names))
            rec['columns'] = list(map(str, df.columns))
            if 't' in df.index.names:
                rec['rows_by_t'] = {str(k): int(v) for k, v in
                                    df.index.get_level_values('t').value_counts().sort_index().items()}
            rec['attrs'] = {k: jsonable(v) for k, v in df.attrs.items()
                            if k in ('kg_factor_sources', 'unknown_plot_excluded', 'layer_counts',
                                     'harvest_kg_layers', 'sources')}
            if save:
                d = df.copy()
                d.attrs = {}
                try:
                    d.to_parquet(outdir / f'{arm}.{name}.parquet')
                except Exception:  # noqa: BLE001 -- mixed-type native object columns
                    cast = [c for c in d.columns if d[c].dtype == object]
                    for c in cast:
                        d[c] = d[c].map(lambda x: x if pd.isna(x) else str(x)).astype('string')
                    rec['saved_with_object_columns_as_string'] = cast
                    d.to_parquet(outdir / f'{arm}.{name}.parquet')
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['traceback'] = traceback.format_exc()[-4000:]
        cats, msgs = {}, []
        for x in w:
            cats[x.category.__name__] = cats.get(x.category.__name__, 0) + 1
            if len(msgs) < 80:
                msgs.append(f'{x.category.__name__}: {str(x.message)[:700]}')
        rec['warnings'] = cats
        rec['warning_messages'] = msgs
    rec['seconds'] = round(time.time() - t0, 1)
    log['tables'][name] = rec
    print(arm, name, {k: v for k, v in rec.items() if k not in ('traceback', 'warning_messages', 'columns')},
          flush=True)
    dump()


c = ll.Country('Malawi')
log['waves'] = list(c.waves)
run('crop_production', lambda: c.crop_production())
run('plot_inputs', lambda: c.plot_inputs())
cp, pi = FR.get('crop_production'), FR.get('plot_inputs')
if cp is not None:
    run('harvest_kg', lambda: T.harvest_kg(cp))
    run('harvest_kg_shipped', lambda: T.harvest_kg(mal.with_region(cp), shipped_factors=mal.crop_conversion_factors()))
    run('crop_diversity', lambda: T.crop_diversity(cp))
    run('gross_crop_revenue', lambda: T.gross_crop_revenue(cp))
if pi is not None:
    run('seed_kg', lambda: T.seed_kg(pi))
    run('nitrogen_kg', lambda: T.nitrogen_kg(pi))

# Site Q offenders, as data (the warning prints only the first few).
try:
    from lsms_library.quantity_audit import quantity_reports
    off = {}
    for table in ('crop_production', 'plot_inputs'):
        reps = quantity_reports(country='Malawi', table=table)
        rows = []
        for rep in reps:
            for o in rep.get('offenders', []):
                rows.append({k: (str(v) if not isinstance(v, (int, float)) else v) for k, v in o.items()})
        off[table] = {'reports': len(reps), 'offenders': len(rows),
                      'by_wave': {str(rep.get('wave', rep.get('t'))): len(rep.get('offenders', [])) for rep in reps},
                      'rows': rows}
    log['site_q'] = off
    print(arm, 'site_q', {t: (v['offenders'], v['by_wave']) for t, v in off.items()}, flush=True)
except Exception as e:  # noqa: BLE001
    log['site_q'] = {'error': f'{type(e).__name__}: {e}'}
dump()

# In-process replay of the ladder per wave (attrs do not survive to_parquet).
log['replay'] = {}
for w in ('2010-11', '2013-14', '2016-17', '2019-20'):
    t0 = time.time()
    try:
        with warnings.catch_warnings(record=True) as ws:
            warnings.simplefilter('always')
            harvest, sale, raw = mal._crop_pieces(w)
            frame = mal.assemble_crop_production(w, harvest, sale)
        rec = {k: jsonable(v) for k, v in frame.attrs.items()}
        rec['rows'] = int(len(frame))
        rec['crop_counts'] = {str(k): int(v) for k, v in
                              frame.index.get_level_values('crop').value_counts().items()
                              if k in ('Other', 'Cowpea (Khobwe)')}
        rec['raw_lines_crop_counts'] = {str(k): int(v) for k, v in raw['crop'].value_counts().items()
                                        if k in ('Other', 'Cowpea (Khobwe)')}
        rec['dropped_code_warnings'] = [str(x.message)[:600] for x in ws
                                        if 'absent from harmonize_crop' in str(x.message)]
        rec['seconds'] = round(time.time() - t0, 1)
    except Exception as e:  # noqa: BLE001
        rec = {'error': f'{type(e).__name__}: {e}', 'traceback': traceback.format_exc()[-4000:]}
    log['replay'][w] = rec
    print(arm, 'replay', w, {k: rec.get(k) for k in ('rows', 'crop_counts', 'crop_text_recovered',
                                                      'dropped_code_warnings', 'error')}, flush=True)
    dump()

# Does every keyed row join its derivation_inputs?  Joined keys ('a+b') are split.
# Each inputs frame is saved, so the two arms' frames can be compared key by key.
log['derivation_join'] = {}
inputs_dir = outdir / f'{arm}.inputs'
inputs_dir.mkdir(exist_ok=True)
for table, frame in (('crop_production', cp), ('plot_inputs', pi)):
    if frame is None or 'Derivation' not in frame.columns:
        continue
    r = frame.reset_index()
    r = r[r['Derivation'].notna()].copy()
    r['_key'] = r['Derivation'].astype('string').str.split('+')
    r = r.explode('_key')
    for (key, w), sub in r.groupby(['_key', 't']):
        if f'::{table}::' not in str(key):
            continue
        rec = {'table': table, 'rows': int(len(sub)), 'cowpea_rows': int((sub['crop'] == 'Cowpea (Khobwe)').sum()),
               'rows_with_two_keys': int(sub['Derivation'].astype('string').str.contains('+', regex=False).sum())}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                inp = c.derivation_inputs(key, wave=w)
            names = [n for n in inp.index.names if n in sub.columns]
            d = inp.copy()
            d.attrs = {}
            name = f"{str(key).split('::')[1]}__{str(key).split('::')[2]}__{w}.parquet"
            try:
                d.reset_index().to_parquet(inputs_dir / name)
            except Exception:  # noqa: BLE001
                e2 = d.reset_index()
                for col in [x for x in e2.columns if e2[x].dtype == object]:
                    e2[col] = e2[col].map(lambda x: x if pd.isna(x) else str(x)).astype('string')
                e2.to_parquet(inputs_dir / name)
            inp = inp.reset_index()
            rec['inputs_rows'] = int(len(inp))
            rec['join_on'] = names
            kin = set(map(tuple, inp[names].astype(str).itertuples(index=False)))
            hit = [tuple(x) in kin for x in sub[names].astype(str).itertuples(index=False)]
            rec['joined'] = int(sum(hit))
            cow = (sub['crop'] == 'Cowpea (Khobwe)').to_numpy()
            rec['cowpea_joined'] = int(sum(h for h, cw in zip(hit, cow) if cw))
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
        log['derivation_join'][f'{w}|{key}'] = rec
        print(arm, 'djoin', w, key, rec, flush=True)
    dump()

# Every registered key x wave of the two tables (deletions too), saved for the comparison.
from lsms_library.derivations import records_for  # noqa: E402
log['inputs_saved'] = {}
for table in ('crop_production', 'plot_inputs'):
    for key, rec in records_for('Malawi', table).items():
        waves = rec.waves if isinstance(rec.waves, (list, tuple)) else list(c.waves)
        for w in waves:
            name = f"{str(key).split('::')[1]}__{str(key).split('::')[2]}__{w}.parquet"
            if (inputs_dir / name).exists():
                log['inputs_saved'][name] = 'saved-by-join'
                continue
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    e2 = c.derivation_inputs(key, wave=w).reset_index()
                for col in [x for x in e2.columns if e2[x].dtype == object]:
                    e2[col] = e2[col].map(lambda x: x if pd.isna(x) else str(x)).astype('string')
                e2.to_parquet(inputs_dir / name)
                log['inputs_saved'][name] = int(len(e2))
            except Exception as e:  # noqa: BLE001
                log['inputs_saved'][name] = f'ERR {type(e).__name__}: {e}'
dump()
print('BUILD DONE', arm, flush=True)
