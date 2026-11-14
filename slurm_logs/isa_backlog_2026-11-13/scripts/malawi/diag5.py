"""Fifth read-only diagnostic: the three inputs frames that differ, code 49 in
plot_inputs, and why ag_h42b_os is blank on every served row.

usage: python diag5.py <clone> <verify-dir> <outdir>
"""
import json
import re
import sys
import warnings
import importlib.util
from collections import Counter
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, vdir, outdir = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

RES = {'head': head}
fr = vdir / 'frames'


def dump():
    (outdir / 'diag5.json').write_text(json.dumps(RES, indent=1, default=str))


# 1. the inputs frames that differ: which crop values moved
RES['inputs_crop_moves'] = {}
for n in ('crop_production__sale-basis-no-reference__2019-20.parquet',
          'crop_production__sale-other-unit-single-row__2016-17.parquet',
          'crop_production__sale-other-unit-single-row__2019-20.parquet'):
    a = pd.read_parquet(fr / 'base.inputs' / n).reset_index(drop=True)
    b = pd.read_parquet(fr / 'cand.inputs' / n).reset_index(drop=True)
    d = a['crop'].astype('string').fillna('<NA>') != b['crop'].astype('string').fillna('<NA>')
    other = [c for c in a.columns if c != 'crop' and c in b.columns
             and not a[c].astype('string').fillna('<NA>').equals(b[c].astype('string').fillna('<NA>'))]
    RES['inputs_crop_moves'][n] = {
        'moved': int(d.sum()),
        'from_to': {f'{x}->{y}': int(k) for (x, y), k in Counter(zip(a.loc[d, 'crop'].astype(str),
                                                                       b.loc[d, 'crop'].astype(str))).items()},
        'other_columns_differ': other}
dump()

# 2. code 49 in the served plot_inputs and crop_production
pi = pd.read_parquet(fr / 'cand.plot_inputs.parquet').reset_index()
cp = pd.read_parquet(fr / 'cand.crop_production.parquet').reset_index()
RES['code49'] = {
    'plot_inputs_rows_crop_49': {f'{t}|{inp}|{pl}': int(k) for (t, inp, pl), k in Counter(zip(
        pi.loc[pi['crop'].astype(str).eq('49'), 't'].astype(str),
        pi.loc[pi['crop'].astype(str).eq('49'), 'input'].astype(str),
        pi.loc[pi['crop'].astype(str).eq('49'), 'plot_id'].astype(str).eq('Farm-level').map(
            {True: 'Farm-level', False: 'plot'}))).items()},
    'crop_production_rows_crop_49': int(cp['crop'].astype(str).eq('49').sum()),
    'plot_inputs_crop_values_numeric_like': {str(k): int(v) for k, v in pi.loc[
        pi['crop'].astype(str).str.fullmatch(r'\d+'), 'crop'].astype(str).value_counts().items()}}
dump()

# 3. ag_h42b_os: source rows with text, and whether the carrier serves any of them
spec = importlib.util.spec_from_file_location('mw5', countries_root() / 'Malawi' / '_' / 'malawi.py')
mw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mw)
RES['h42b'] = {}
for w, halves in mw._SEED_LINE_SOURCES.items():
    for s in halves:
        path = countries_root() / 'Malawi' / w / s['h']
        raw = get_dataframe(str(path), convert_categoricals=False).reset_index(drop=True)
        for col in ('ag_h42b_os', 'ag_h33', 'ag_h36b_oth'):
            if col not in raw.columns:
                continue
            v = raw[col]
            txt = ~(v.isna() | v.astype('string').str.strip().eq('').fillna(False))
            served = pi[pi['t'].astype(str).eq(w) & pi['input_obs'].astype(str).str.contains(
                re.escape(Path(s['h']).name), regex=True)]
            pos = served['input_obs'].astype(str).str.extract(r':(\d+)/field:')[0].astype('Int64')
            pos = set(pos.dropna().astype(int))
            rows = set(raw.index[txt])
            rec = {'source_rows': int(len(raw)), 'rows_with_text': int(txt.sum()),
                   'served_positions_from_this_file': len(pos),
                   'text_rows_served': len(rows & pos)}
            if txt.any():
                sub = raw.loc[txt]
                qcols = [*s['q'], 'ag_h36a', 'ag_h38a', 'ag_h42a', 'ag_h42b']
                rec['text_rows_nonnull'] = {c: int(pd.to_numeric(sub[c], errors='coerce').notna().sum())
                                            for c in qcols if c in sub.columns}
                rec['text_rows_value_nonzero'] = {c: int(pd.to_numeric(sub[c], errors='coerce').fillna(0).ne(0).sum())
                                                  for c in s['v'] if c in sub.columns}
                rec['parent_codes'] = {str(k): int(n) for k, n in
                                       pd.to_numeric(sub.get(col.rsplit('_', 1)[0]), errors='coerce')
                                       .value_counts(dropna=False).items()} if col.rsplit('_', 1)[0] in sub else None
            RES['h42b'][f'{w}/{s["half"]}/{col}'] = rec
dump()
print(json.dumps(RES, default=str)[:4000])
print('DIAG5 DONE', flush=True)
