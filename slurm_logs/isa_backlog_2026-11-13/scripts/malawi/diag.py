"""Read-only diagnostics for GH #1228 (D/G season, D join, Module I) and GH #1712.

usage: python diag.py <clone> <outdir>

Nothing is built; sources are read through get_dataframe (2016-17 XS Module D
through the existing restricted pyreadstat route, because a full read fails on
its latin-1 byte in ag_d55_2oth).  Output: <outdir>/diag.json (+ meta.json).
"""
import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
import time
import traceback
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pyreadstat

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, outdir = sys.argv[1], Path(sys.argv[2])
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)
warnings.simplefilter('ignore')

from lsms_library.local_tools import (get_dataframe, format_id, _ensure_dvc_pulled,  # noqa: E402
                                      _dvc_cache_path)
from lsms_library.paths import countries_root  # noqa: E402

ROOT = countries_root() / 'Malawi'
sys.path.insert(0, str(ROOT / '_'))
spec = importlib.util.spec_from_file_location('malawi_diag', ROOT / '_' / 'malawi.py')
mal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mal)

RES = {'head': head, 'sections': {}}
META = {}


def dump():
    (outdir / 'diag.json').write_text(json.dumps(RES, indent=1, default=str))
    (outdir / 'meta.json').write_text(json.dumps(META, indent=1, default=str))


def section(name):
    def deco(fn):
        t0 = time.time()
        print(f'--- {name} start', flush=True)
        try:
            RES['sections'][name] = fn()
        except Exception as e:  # noqa: BLE001
            RES['sections'][name] = {'error': f'{type(e).__name__}: {e}',
                                     'traceback': traceback.format_exc()[-6000:]}
            print(traceback.format_exc(), flush=True)
        print(f'--- {name} done {time.time() - t0:.1f}s', flush=True)
        dump()
        return fn
    return deco


def local_copy(rel):
    """A private .dta copy of a DVC-tracked source (rel to Malawi/)."""
    p = ROOT / rel
    _ensure_dvc_pulled(str(p))
    blob = _dvc_cache_path(str(p))
    if blob is None:
        raise FileNotFoundError(rel)
    tmp = Path(tempfile.mkdtemp()) / Path(rel).name
    shutil.copyfile(blob, tmp)
    return tmp


def _key(a):
    if isinstance(a, float) and a.is_integer():
        return str(int(a))
    return str(a)


def meta(rel):
    if rel in META:
        return META[rel]
    tmp = local_copy(rel)
    try:
        _, m = pyreadstat.read_dta(str(tmp), metadataonly=True)
    except Exception:  # noqa: BLE001
        _, m = pyreadstat.read_dta(str(tmp), metadataonly=True, encoding='latin1')
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)
    rec = {'columns': list(m.column_names),
           'labels': dict(zip(m.column_names, m.column_labels)),
           'var_value_labels': dict(m.variable_to_label),
           'value_labels': {k: {_key(a): b for a, b in v.items()} for k, v in m.value_labels.items()},
           'n_rows': m.number_rows}
    META[rel] = rec
    return rec


def read(rel, restricted=False):
    if not restricted:
        return get_dataframe(str(ROOT / rel), convert_categoricals=False)
    m = meta(rel)
    cols = [c for c in m['columns'] if c != 'ag_d55_2oth']
    tmp = local_copy(rel)
    try:
        df, _ = pyreadstat.read_dta(str(tmp), usecols=cols, apply_value_formats=False)
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)
    return df


def labels_of(rel, col):
    m = meta(rel)
    ls = m['var_value_labels'].get(col)
    return m['value_labels'].get(ls, {}) if ls else {}


YEARS = re.compile(r'20\d\d\s*[/-]\s*(20)?\d\d')
SEASON = re.compile(r'season|rainy|dry|reference|cropping|agricultural year', re.I)


def season_candidates(rel):
    m = meta(rel)
    out = {}
    for c in m['columns']:
        lab = m['labels'].get(c) or ''
        vl = labels_of(rel, c)
        hit_lab = bool(SEASON.search(lab) or YEARS.search(lab))
        hit_val = any(YEARS.search(str(v)) for v in vl.values())
        if hit_lab or hit_val:
            out[c] = {'label': lab, 'value_labels': vl}
    return out


HALVES = {
    ('2016-17', 'xs'): dict(d='Data/Cross_Sectional/ag_mod_d.dta', g='Data/Cross_Sectional/ag_mod_g.dta',
                            i='Data/Cross_Sectional/ag_mod_i.dta', hh='case_id', restricted_d=True),
    ('2016-17', 'panel'): dict(d='Data/Panel/ag_mod_d_16.dta', g='Data/Panel/ag_mod_g_16.dta',
                               i='Data/Panel/ag_mod_i_16.dta', hh='y3_hhid'),
    ('2019-20', 'xs'): dict(d='Data/Cross_Sectional/ag_mod_d.dta', g='Data/Cross_Sectional/ag_mod_g.dta',
                            i='Data/Cross_Sectional/ag_mod_i.dta', hh='case_id'),
    ('2019-20', 'panel'): dict(d='Data/Panel/ag_mod_d_19.dta', g='Data/Panel/ag_mod_g_19.dta',
                               i='Data/Panel/ag_mod_i_19.dta', hh='y4_hhid'),
    ('2024-25', 'xs'): dict(d='Data/Cross_Sectional/ag_mod_d.dta', g='Data/Cross_Sectional/ag_mod_g.dta',
                            i='Data/Cross_Sectional/ag_mod_i.dta', hh='case_id'),
    ('2024-25', 'panel'): dict(d='Data/Panel/ag_mod_d_24.dta', g='Data/Panel/ag_mod_g_24.dta',
                               i='Data/Panel/ag_mod_i_24.dta', hh='y5_hhid'),
}
H_SOURCES = [('2010-11', 'Full_Sample'), ('2013-14', 'single'), ('2016-17', 'Cross_Sectional'),
             ('2016-17', 'Panel'), ('2019-20', 'Cross_Sectional'), ('2019-20', 'Panel')]
SIX = ['ag_h17_os', 'ag_h26b_os', 'ag_h27_os', 'ag_h36b_os', 'ag_h26_1b', 'ag_h36_1b']

FRAMES = {}


def frames(w, half):
    if (w, half) not in FRAMES:
        h = HALVES[(w, half)]
        rel = lambda k: f'{w}/{h[k]}'  # noqa: E731
        d = read(rel('d'), restricted=h.get('restricted_d', False))
        g = read(rel('g'))
        i = read(rel('i'))
        for df in (d, g, i):
            if h['hh'] not in df.columns:
                raise KeyError(f"{w}/{half}: household column {h['hh']!r} missing; "
                               f"id-like columns: {[c for c in df.columns if 'hhid' in c.lower() or c == 'case_id']}")
        FRAMES[(w, half)] = (d, g, i)
    return FRAMES[(w, half)]


def vc(s, n=60):
    return {str(k): int(v) for k, v in s.value_counts(dropna=False).head(n).items()}


def crop_col(df):
    for c in ('crop_code', 'ag_g0d', 'ag_g0b', 'ag_i0b'):
        if c in df.columns:
            return c
    raise KeyError('no crop column')


@section('metadata')
def _meta():
    rels = []
    for (w, half), h in HALVES.items():
        rels += [f'{w}/{h[k]}' for k in ('d', 'g', 'i')]
    for w, half in H_SOURCES:
        b = [s for s in mal._SEED_LINE_SOURCES[w] if s['half'] == half][0]
        rels.append(f"{w}/{b['h']}")
    rels.append('2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta')
    out = {}
    for rel in rels:
        try:
            m = meta(rel)
            out[rel] = {'n_rows': m['n_rows'], 'n_cols': len(m['columns'])}
        except Exception as e:  # noqa: BLE001
            out[rel] = {'error': f'{type(e).__name__}: {e}'}
    return out


@section('season_candidates')
def _seasons():
    out = {}
    for (w, half), h in HALVES.items():
        for k in ('d', 'g', 'i'):
            rel = f'{w}/{h[k]}'
            try:
                out[rel] = season_candidates(rel)
            except Exception as e:  # noqa: BLE001
                out[rel] = {'error': f'{type(e).__name__}: {e}'}
    return out


@section('season_data')
def _season_data():
    out = {}
    cands = RES['sections']['season_candidates']
    for (w, half), h in HALVES.items():
        rec = {}
        try:
            d, g, i = frames(w, half)
        except Exception as e:  # noqa: BLE001
            out[f'{w}/{half}'] = {'error': f'{type(e).__name__}: {e}'}
            continue
        mods = {'d': d, 'g': g, 'i': i}
        found = {}
        for k, df in mods.items():
            rel = f'{w}/{h[k]}'
            cols = [c for c in cands.get(rel, {}) if c in df.columns]
            found[k] = cols
            for c in cols:
                rec[f'{k}:{c}:value_counts'] = vc(df[c])
            if 'visit' in df.columns:
                rec[f'{k}:visit:value_counts'] = vc(df['visit'])
        rec['candidates'] = found
        hh = h['hh']
        # every D candidate against every G (and I) candidate, per household
        for a, b in (('d', 'g'), ('d', 'i'), ('g', 'i')):
            for ca in found.get(a, []):
                for cb in found.get(b, []):
                    A = mods[a][[hh, ca]].dropna().assign(_h=lambda x: x[hh].apply(format_id))
                    B = mods[b][[hh, cb]].dropna().assign(_h=lambda x: x[hh].apply(format_id))
                    sa = A.groupby('_h')[ca].agg(lambda s: tuple(sorted(set(s))))
                    sb = B.groupby('_h')[cb].agg(lambda s: tuple(sorted(set(s))))
                    j = pd.concat([sa.rename('a'), sb.rename('b')], axis=1, join='inner')
                    single = j['a'].map(len).eq(1) & j['b'].map(len).eq(1)
                    rec[f'hh:{a}.{ca}~{b}.{cb}'] = {
                        'households_both': int(len(j)),
                        'only_in_a': int(len(sa.index.difference(sb.index))),
                        'only_in_b': int(len(sb.index.difference(sa.index))),
                        'single_equal': int((single & (j['a'] == j['b'])).sum()),
                        'single_differ': int((single & (j['a'] != j['b'])).sum()),
                        'multi_valued': int((~single).sum()),
                        'differ_pairs': vc(j.loc[single & (j['a'] != j['b'])].apply(
                            lambda r: f"{r['a'][0]}->{r['b'][0]}", axis=1), 20)
                        if (single & (j['a'] != j['b'])).any() else {},
                    }
        out[f'{w}/{half}'] = rec
    return out


def d_text_table(d, hh):
    keys = ['_kh', '_kg', '_kp']
    dn = pd.DataFrame({'_kh': d[hh].apply(format_id), '_kg': d['gardenid'].apply(format_id),
                       '_kp': d['plotid'].apply(format_id)})
    slots = sorted(c for c in d.columns if re.fullmatch(r'ag_d20[a-z]', c))
    texts = sorted(c for c in d.columns if c.startswith('ag_d20') and (c.endswith('_oth') or c.endswith('_os')))
    codes = pd.DataFrame({c: pd.to_numeric(d[c], errors='coerce') for c in slots})
    dn['_n48'] = codes.eq(48).sum(axis=1)
    dn['_nslots'] = codes.notna().sum(axis=1)
    t = pd.DataFrame({c: mal._normalise_unit_text(d[c]) for c in texts}, index=d.index)
    dn['_ntext'] = t.notna().sum(axis=1)
    dn['_text'] = t.bfill(axis=1).iloc[:, 0] if len(texts) else pd.NA
    dn['_texts'] = t.apply(lambda r: ' | '.join(str(x) for x in r if pd.notna(x)), axis=1) if len(texts) else ''
    # which text slot holds the single text
    if len(texts):
        dn['_textcol'] = t.notna().apply(lambda r: ','.join(c for c in texts if r[c]), axis=1)
    dn['_ndrow'] = dn.groupby(keys)['_kh'].transform('size')
    return dn, slots, texts


COWPEA_LETTERS = {'COWPEAS', 'COWPEA'}


def letters(s):
    return s.astype('string').str.replace(r'[^A-Z]', '', regex=True)


@section('g48_d_join')
def _join():
    out = {}
    for (w, half), h in HALVES.items():
        rec = {}
        try:
            d, g, i = frames(w, half)
            hh = h['hh']
            gc = crop_col(g)
            rec['g_crop_col'] = gc
            rec['g_crop_label_48'] = labels_of(f"{w}/{h['g']}", gc).get('48')
            rec['g_rows'] = int(len(g))
            code = pd.to_numeric(g[gc], errors='coerce')
            g48 = g[code.eq(48).fillna(False)].copy()
            rec['g48_rows'] = int(len(g48))
            dn, slots, texts = d_text_table(d, hh)
            rec['d_rows'] = int(len(d))
            rec['d_slot_cols'] = slots
            rec['d_text_cols'] = texts
            rec['d_text_labels'] = {c: meta(f"{w}/{h['d']}")['labels'].get(c) for c in texts}
            rec['d_slot_labels'] = {c: meta(f"{w}/{h['d']}")['labels'].get(c) for c in slots}
            rec['d_ntext_x_n48'] = vc(dn['_ntext'].astype(str) + 'text/' + dn['_n48'].astype(str) + 'x48')
            rec['d_rows_with_text_but_no48'] = int(((dn['_ntext'] > 0) & (dn['_n48'] == 0)).sum())
            rec['d_texts_on_no48_rows_top'] = vc(dn.loc[(dn['_ntext'] > 0) & (dn['_n48'] == 0), '_texts'], 25)
            g48['_kh'] = g48[hh].apply(format_id)
            g48['_kg'] = g48['gardenid'].apply(format_id)
            g48['_kp'] = g48['plotid'].apply(format_id)
            keys = ['_kh', '_kg', '_kp']
            g48['_ng48'] = g48.groupby(keys, dropna=False)['_kh'].transform('size')
            dk = dn.drop_duplicates(keys)
            m = g48.merge(dk, on=keys, how='left')
            incomplete = m[keys].isna().any(axis=1)
            reason = np.select(
                [incomplete, m['_ndrow'].isna(), m['_ndrow'] > 1, m['_ntext'].eq(0), m['_ntext'] >= 2,
                 m['_n48'].eq(0), m['_n48'] >= 2, m['_ng48'] > 1],
                ['key_incomplete', 'no_d_row', 'd_not_unique', 'no_text', 'multi_text',
                 'text_no48_slot', 'text_multi48_slot', 'g_multi48_on_plot'], default='single')
            m['_reason'] = reason
            rec['reason_counts'] = vc(pd.Series(reason))
            sing = m[m['_reason'] == 'single']
            rec['single_text_counts'] = {str(k): int(v) for k, v in sing['_text'].value_counts().items()}
            rec['single_textcol'] = vc(sing['_textcol']) if '_textcol' in sing else {}
            rec['single_literal_COWPEAS_or_COW_PEAS'] = int(sing['_text'].isin(['COWPEAS', 'COW PEAS']).sum())
            rec['single_cowpea_letters'] = int(letters(sing['_text']).isin(COWPEA_LETTERS).sum())
            rec['single_cowpea_letter_spellings'] = vc(sing.loc[letters(sing['_text']).isin(COWPEA_LETTERS), '_text'])
            rec['any_text_contains_COW_or_KHOBWE'] = vc(m.loc[m['_texts'].astype('string').str.contains(
                'COW|KHOB|NSEUL|MSEW', regex=True, na=False), '_reason'] + ' :: ' + m.loc[m['_texts'].astype('string').str.contains(
                'COW|KHOB|NSEUL|MSEW', regex=True, na=False), '_texts'].astype(str), 80)
            rec['multi_text_values'] = vc(m.loc[m['_reason'] == 'multi_text', '_texts'], 40)
            rec['g_multi48_texts'] = vc(m.loc[m['_ng48'] > 1, '_texts'], 20)
            # quantity on the G48 rows, by reason, for the report
            q = pd.to_numeric(m.get('ag_g13a'), errors='coerce') if 'ag_g13a' in m else None
            if q is not None:
                rec['g13a_sum_by_reason'] = {k: float(v) for k, v in q.groupby(m['_reason']).sum().items()}
            # G's own other-specify text columns on code-48 rows, if any
            gtexts = [c for c in g.columns if ('oth' in c or c.endswith('_os')) and c.startswith('ag_g0')]
            rec['g_own_text_cols'] = {c: {'label': meta(f"{w}/{h['g']}")['labels'].get(c),
                                          'nonblank_on_48': int(mal._normalise_unit_text(g48[c]).notna().sum()),
                                          'top': vc(mal._normalise_unit_text(g48[c]).dropna(), 15)}
                                      for c in gtexts}
            # per household: G48 resolution, for the sale rule
            m['_cow'] = (m['_reason'] == 'single') & letters(m['_text']).isin(COWPEA_LETTERS)
            hh_state = m.groupby('_kh')['_cow'].agg(['all', 'any', 'size'])
            rec['hh_with_g48'] = int(len(hh_state))
            rec['hh_all_g48_cowpea'] = int(hh_state['all'].sum())
            rec['hh_some_not_all_cowpea'] = int((hh_state['any'] & ~hh_state['all']).sum())
            FRAMES[(w, half, 'hh_state')] = hh_state
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['traceback'] = traceback.format_exc()[-4000:]
        out[f'{w}/{half}'] = rec
    return out


@section('module_i')
def _module_i():
    out = {}
    for (w, half), h in HALVES.items():
        rec = {}
        try:
            d, g, i = frames(w, half)
            hh = h['hh']
            ic, gc = crop_col(i), crop_col(g)
            irel, grel = f"{w}/{h['i']}", f"{w}/{h['g']}"
            rec['i_crop_col'] = ic
            rec['i_crop_label'] = meta(irel)['labels'].get(ic)
            rec['g_crop_label'] = meta(grel)['labels'].get(gc)
            il, gl = labels_of(irel, ic), labels_of(grel, gc)
            rec['i_value_label_set'] = meta(irel)['var_value_labels'].get(ic)
            rec['g_value_label_set'] = meta(grel)['var_value_labels'].get(gc)
            codes = sorted(set(il) | set(gl), key=lambda x: float(x) if re.fullmatch(r'-?\d+(\.\d+)?', x) else 1e9)
            rec['label_diffs'] = {c: [il.get(c), gl.get(c)] for c in codes if il.get(c) != gl.get(c)}
            rec['i_label_48'] = il.get('48')
            rec['i_rows'] = int(len(i))
            icode = pd.to_numeric(i[ic], errors='coerce')
            rec['i_code_value_counts_top'] = vc(icode, 70)
            i48 = i[icode.eq(48).fillna(False)].copy()
            rec['i48_rows'] = int(len(i48))
            if 'ag_i01' in i:
                rec['i48_sold_flag'] = vc(i48['ag_i01'])
            ipos = i48[pd.to_numeric(i48.get('ag_i03'), errors='coerce').gt(0).fillna(False)] if 'ag_i03' in i48 else i48.iloc[:0]
            rec['i48_positive_value_rows'] = int(len(ipos))
            rec['i48_positive_value_sum'] = float(pd.to_numeric(ipos.get('ag_i03'), errors='coerce').sum()) if len(ipos) else 0.0
            itexts = [c for c in i.columns if ('oth' in c or c.endswith('_os'))]
            rec['i_text_cols'] = {c: {'label': meta(irel)['labels'].get(c),
                                      'nonblank_all': int(mal._normalise_unit_text(i[c]).notna().sum()),
                                      'nonblank_on_48': int(mal._normalise_unit_text(i48[c]).notna().sum()),
                                      'top_on_48': vc(mal._normalise_unit_text(i48[c]).dropna(), 15)}
                                  for c in itexts}
            # (hh, code) overlap of I with G and with H
            ik = set(zip(i[hh].apply(format_id), icode))
            gk = set(zip(g[hh].apply(format_id), pd.to_numeric(g[gc], errors='coerce')))
            ik = {k for k in ik if pd.notna(k[1])}
            rec['i_hhcode_pairs'] = len(ik)
            rec['i_hhcode_in_g'] = len(ik & gk)
            sold = i[pd.to_numeric(i.get('ag_i03'), errors='coerce').gt(0).fillna(False)] if 'ag_i03' in i else i.iloc[:0]
            sk = {k for k in zip(sold[hh].apply(format_id), pd.to_numeric(sold[ic], errors='coerce')) if pd.notna(k[1])}
            rec['i_sold_hhcode_pairs'] = len(sk)
            rec['i_sold_hhcode_in_g'] = len(sk & gk)
            st = FRAMES.get((w, half, 'hh_state'))
            if st is not None:
                hhs = ipos[hh].apply(format_id) if len(ipos) else pd.Series([], dtype=str)
                cls = np.where(~hhs.isin(st.index), 'no_g48',
                               np.where(hhs.map(st['all']).fillna(False).astype(bool), 'all_g48_cowpea',
                                        np.where(hhs.map(st['any']).fillna(False).astype(bool), 'some_g48_cowpea', 'no_g48_cowpea')))
                rec['i48_positive_by_household_g48_state'] = vc(pd.Series(cls))
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['traceback'] = traceback.format_exc()[-4000:]
        out[f'{w}/{half}'] = rec
    return out


@section('g2010_own_text')
def _g2010():
    rel = '2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta'
    g = read(rel)
    code = pd.to_numeric(g['ag_g0d'], errors='coerce')
    t = mal._normalise_unit_text(g['ag_g0d_os'])
    return {'on_48': vc(t[code.eq(48).fillna(False)], 200),
            'cowpea_letters_any_code': vc(code[letters(t).isin(COWPEA_LETTERS).fillna(False)])}


@section('h1712')
def _h1712():
    out = {}
    union = {}
    for w, half in H_SOURCES:
        rec = {}
        try:
            b = [s for s in mal._SEED_LINE_SOURCES[w] if s['half'] == half][0]
            rel = f"{w}/{b['h']}"
            m = meta(rel)
            h = read(rel)
            rows = np.arange(len(h))
            if w == '2010-11':
                rows = rows[~h.duplicated().to_numpy()]
                h = mal.drop_repeated_records(h)
            h['hhid'] = b['prefix'] + h[b['hhid']].apply(format_id)
            kept = mal._seed_acquisition_block(h.copy(), t=w, source_half=half, row_positions=rows,
                                               source_ref=mal._seed_source_reference(ROOT / rel))
            rec['kept_rows'] = int(len(kept))
            rec['kept_by_kind'] = vc(kept['Acquisition_kind'])
            # raw pyreadstat read for comparison
            tmp = local_copy(rel)
            try:
                raw, _ = pyreadstat.read_dta(str(tmp), apply_value_formats=False)
            finally:
                shutil.rmtree(tmp.parent, ignore_errors=True)
            for col in SIX:
                c = {'present': col in h.columns, 'label': m['labels'].get(col)}
                if col in h.columns:
                    s = h[col]
                    c['dtype_get_dataframe'] = str(s.dtype)
                    c['nonnull_get_dataframe'] = int(s.notna().sum())
                    c['nonblank_get_dataframe'] = int(s.astype('string').str.strip().replace('', pd.NA).notna().sum())
                    r = raw[col]
                    c['dtype_pyreadstat'] = str(r.dtype)
                    c['nonnull_pyreadstat'] = int(r.notna().sum())
                    c['nonblank_pyreadstat'] = int(r.astype('string').str.strip().replace('', pd.NA).notna().sum())
                    c['top_values_pyreadstat'] = vc(r.astype('string').str.strip().replace('', pd.NA).dropna(), 15)
                    k = kept[col] if col in kept else pd.Series(dtype=object)
                    c['nonnull_on_kept'] = int(k.notna().sum())
                    c['nonblank_on_kept'] = int(k.astype('string').str.strip().replace('', pd.NA).notna().sum())
                    parent = col[:-3] if col.endswith('_os') else None
                    if parent and parent in h.columns:
                        pl = labels_of(rel, parent)
                        other = [a for a, v in pl.items() if re.search(r'other|specify', str(v), re.I)]
                        pc = pd.to_numeric(h[parent], errors='coerce')
                        oth = pc.isin([float(x) for x in other if re.fullmatch(r'-?\d+(\.\d+)?', x)])
                        c['parent'] = parent
                        c['parent_label'] = m['labels'].get(parent)
                        c['parent_other_codes'] = {a: pl[a] for a in other}
                        c['parent_value_counts'] = vc(pc, 30)
                        c['rows_parent_is_other'] = int(oth.sum())
                        kp = pd.to_numeric(kept[parent], errors='coerce') if parent in kept else pd.Series(dtype=float)
                        c['kept_rows_parent_is_other'] = int(kp.isin([float(x) for x in other if re.fullmatch(r'-?\d+(\.\d+)?', x)]).sum())
                    if col in ('ag_h26_1b', 'ag_h36_1b'):
                        sib = col[:-1] + 'a'
                        if sib in h.columns:
                            c['sibling'] = sib
                            c['sibling_label'] = m['labels'].get(sib)
                            c['sibling_nonnull_raw'] = int(h[sib].notna().sum())
                            c['sibling_nonnull_kept'] = int(kept[sib].notna().sum()) if sib in kept else None
                            c['sibling_value_counts'] = vc(h[sib], 20)
                rec[col] = c
            for col in [x for x in kept.columns if x.startswith('ag_h')]:
                u = union.setdefault(col, {'nonnull_kept': 0, 'sources': []})
                u['nonnull_kept'] += int(kept[col].notna().sum())
                u['sources'].append(f'{w}/{half}')
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
            rec['traceback'] = traceback.format_exc()[-4000:]
        out[f'{w}/{half}'] = rec
    out['_union_all_null_on_kept'] = {k: v['sources'] for k, v in union.items() if v['nonnull_kept'] == 0}
    out['_union_n_columns'] = len(union)
    return out


print('DIAG DONE', flush=True)
