"""Second read-only GH #1228 diagnostic: where is the reference season recorded?

usage: python diag2.py <clone> <outdir>

1. Metadata + season-candidate value counts for Module A, the agriculture
   meta files and the household cover (hh_mod_a_filt) in 2016-17 / 2019-20 /
   2024-25, both halves.
2. D<->G plot-crop roster consistency per half: is each Module G (plot, crop
   code) one of that plot's Module D question-20 crop codes, and vice versa.
3. Module G planting month/year (ag_g05a/ag_g05b) distribution, all rows and
   code-48 rows.
"""
import json
import re
import shutil
import sys
import tempfile
import time
import traceback
import warnings
from pathlib import Path

import pandas as pd
import pyreadstat

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, outdir = sys.argv[1], Path(sys.argv[2])
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe, format_id, _ensure_dvc_pulled, _dvc_cache_path  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

ROOT = countries_root() / 'Malawi'
RES = {'head': head}


def dump():
    (outdir / 'diag2.json').write_text(json.dumps(RES, indent=1, default=str))


def _key(a):
    return str(int(a)) if isinstance(a, float) and a.is_integer() else str(a)


def local_copy(rel):
    p = ROOT / rel
    _ensure_dvc_pulled(str(p))
    blob = _dvc_cache_path(str(p))
    if blob is None:
        raise FileNotFoundError(rel)
    tmp = Path(tempfile.mkdtemp()) / Path(rel).name
    shutil.copyfile(blob, tmp)
    return tmp


def read_raw(rel, usecols=None):
    tmp = local_copy(rel)
    try:
        df, m = pyreadstat.read_dta(str(tmp), usecols=usecols, apply_value_formats=False)
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)
    return df, m


def meta(rel):
    tmp = local_copy(rel)
    try:
        _, m = pyreadstat.read_dta(str(tmp), metadataonly=True)
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)
    return m


def vc(s, n=40):
    return {str(k): int(v) for k, v in s.value_counts(dropna=False).head(n).items()}


YEARS = re.compile(r'20\d\d\s*[/-]\s*(20)?\d\d|201[4-9]|202[0-5]')
SEASON = re.compile(r'season|rainy|reference|\bRS\b|cropping|agricultural year|visit', re.I)

FILES = {
    '2016-17': ['Data/Cross_Sectional/ag_mod_a.dta', 'Data/Cross_Sectional/ag_metadata.dta',
                'Data/Cross_Sectional/hh_mod_a_filt.dta',
                'Data/Panel/ag_mod_a_16.dta', 'Data/Panel/ag_meta_16.dta', 'Data/Panel/hh_mod_a_filt_16.dta'],
    '2019-20': ['Data/Cross_Sectional/ag_mod_a.dta', 'Data/Cross_Sectional/ag_mod_meta.dta',
                'Data/Cross_Sectional/hh_mod_a_filt.dta',
                'Data/Panel/ag_mod_a_19.dta', 'Data/Panel/ag_meta_19.dta', 'Data/Panel/hh_mod_a_filt_19.dta'],
}
for w in ('2024-25',):
    d = ROOT / w / 'Data'
    FILES[w] = sorted(str(p.relative_to(ROOT / w))[:-4] for p in d.rglob('*.dvc')
                      if re.search(r'ag_mod_a|ag_meta|ag_mod_meta|hh_mod_a_filt', p.name))

t0 = time.time()
RES['files'] = {}
for w, rels in FILES.items():
    for r in rels:
        rel = f'{w}/{r}'
        rec = {}
        try:
            m = meta(rel)
            labels = dict(zip(m.column_names, m.column_labels))
            rec['n_rows'] = m.number_rows
            rec['columns'] = {c: labels.get(c) for c in m.column_names}
            cands = {}
            for c in m.column_names:
                lab = labels.get(c) or ''
                ls = m.variable_to_label.get(c)
                vl = {_key(a): b for a, b in m.value_labels.get(ls, {}).items()} if ls else {}
                if SEASON.search(lab) or YEARS.search(lab) or any(YEARS.search(str(v)) for v in vl.values()) \
                        or re.fullmatch(r'ag_[a-z]_?0|ag_[a-z]0|ag_[a-z]_0', c):
                    cands[c] = {'label': lab, 'value_labels': vl}
            if cands:
                df, _ = read_raw(rel, usecols=list(cands))
                for c in cands:
                    cands[c]['value_counts'] = vc(df[c])
            rec['candidates'] = cands
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
        RES['files'][rel] = rec
        print(rel, {k: v for k, v in rec.items() if k not in ('columns',)} if 'error' in rec
              else list(rec.get('candidates', {})), flush=True)
        dump()
print('files done', round(time.time() - t0, 1), flush=True)

HALVES = {
    ('2016-17', 'xs'): ('Data/Cross_Sectional/ag_mod_d.dta', 'Data/Cross_Sectional/ag_mod_g.dta', 'case_id', True),
    ('2016-17', 'panel'): ('Data/Panel/ag_mod_d_16.dta', 'Data/Panel/ag_mod_g_16.dta', 'y3_hhid', False),
    ('2019-20', 'xs'): ('Data/Cross_Sectional/ag_mod_d.dta', 'Data/Cross_Sectional/ag_mod_g.dta', 'case_id', False),
    ('2019-20', 'panel'): ('Data/Panel/ag_mod_d_19.dta', 'Data/Panel/ag_mod_g_19.dta', 'y4_hhid', False),
    ('2024-25', 'xs'): ('Data/Cross_Sectional/ag_mod_d.dta', 'Data/Cross_Sectional/ag_mod_g.dta', 'case_id', False),
    ('2024-25', 'panel'): ('Data/Panel/ag_mod_d_24.dta', 'Data/Panel/ag_mod_g_24.dta', 'y5_hhid', False),
}
RES['roster'] = {}
for (w, half), (drel, grel, hh, restricted) in HALVES.items():
    rec = {}
    try:
        slots = ['ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e']
        if restricted:
            d, _ = read_raw(f'{w}/{drel}', usecols=[hh, 'gardenid', 'plotid', *slots])
        else:
            d = get_dataframe(str(ROOT / w / drel), convert_categoricals=False)
        g = get_dataframe(str(ROOT / w / grel), convert_categoricals=False)
        for df in (d, g):
            df['_k'] = (df[hh].apply(format_id).astype(str) + '|' + df['gardenid'].apply(format_id).astype(str)
                        + '|' + df['plotid'].apply(format_id).astype(str))
        dl = d.melt(id_vars='_k', value_vars=[s for s in slots if s in d], value_name='code')
        dl['code'] = pd.to_numeric(dl['code'], errors='coerce')
        dl = dl.dropna(subset=['code'])
        dpairs = set(zip(dl['_k'], dl['code'].astype(int)))
        gcode = pd.to_numeric(g['crop_code'], errors='coerce')
        gp = pd.DataFrame({'_k': g['_k'], 'code': gcode}).dropna()
        gpairs = set(zip(gp['_k'], gp['code'].astype(int)))
        rec['g_rows'] = int(len(g))
        rec['g_pairs'] = len(gpairs)
        rec['d_pairs'] = len(dpairs)
        rec['g_pairs_in_d'] = len(gpairs & dpairs)
        rec['d_pairs_in_g'] = len(dpairs & gpairs)
        rec['g_plots_in_d'] = len({k for k, _ in gpairs} & {k for k, _ in dpairs})
        rec['g_plots'] = len({k for k, _ in gpairs})
        g48 = {p for p in gpairs if p[1] == 48}
        rec['g48_pairs'] = len(g48)
        rec['g48_pairs_in_d'] = len(g48 & dpairs)
        miss = sorted(gpairs - dpairs)[:10]
        rec['g_pairs_not_in_d_examples'] = [list(map(str, x)) for x in miss]
        for col in ('ag_g05a', 'ag_g05b', 'ag_g12a', 'ag_g12b'):
            if col in g:
                rec[f'{col}_all'] = vc(pd.to_numeric(g[col], errors='coerce'), 30)
                rec[f'{col}_on_48'] = vc(pd.to_numeric(g.loc[gcode.eq(48).fillna(False), col], errors='coerce'), 30)
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
        rec['traceback'] = traceback.format_exc()[-3000:]
    RES['roster'][f'{w}/{half}'] = rec
    print(w, half, {k: v for k, v in rec.items() if not k.startswith('ag_g') and k != 'traceback'}, flush=True)
    dump()
print('DIAG2 DONE', flush=True)
