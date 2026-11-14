"""Fourth read-only diagnostic (after the red team): hh_x02, slot pairing, code 49,
blank H columns, and the 302 / 301 line.

usage: python diag4.py <clone> <outdir>
"""
import json
import re
import shutil
import sys
import tempfile
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
from lsms_library.local_tools import get_dataframe, format_id, _ensure_dvc_pulled, _dvc_cache_path  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

ROOT = countries_root() / 'Malawi'
RES = {'head': head}
COW = {'COWPEAS', 'COW PEAS'}


def dump():
    (outdir / 'diag4.json').write_text(json.dumps(RES, indent=1, default=str))


def vc(s, n=40):
    return {str(k): int(v) for k, v in s.value_counts(dropna=False).head(n).items()}


def norm(s):
    s = pd.Series(s).astype('string').str.strip().str.replace(r'\s+', ' ', regex=True).str.upper()
    return s.where(s.notna() & (s != ''), pd.NA)


def raw_read(rel, usecols=None):
    p = ROOT / rel
    _ensure_dvc_pulled(str(p))
    blob = _dvc_cache_path(str(p))
    tmp = Path(tempfile.mkdtemp()) / Path(rel).name
    shutil.copyfile(blob, tmp)
    try:
        df, m = pyreadstat.read_dta(str(tmp), usecols=usecols, apply_value_formats=False)
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)
    return df, m


def meta(rel):
    p = ROOT / rel
    _ensure_dvc_pulled(str(p))
    blob = _dvc_cache_path(str(p))
    tmp = Path(tempfile.mkdtemp()) / Path(rel).name
    shutil.copyfile(blob, tmp)
    try:
        _, m = pyreadstat.read_dta(str(tmp), metadataonly=True)
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)
    return m


def section(name, fn):
    try:
        RES[name] = fn()
    except Exception as e:  # noqa: BLE001
        RES[name] = {'error': f'{type(e).__name__}: {e}', 'traceback': traceback.format_exc()[-3000:]}
    print(name, json.dumps(RES[name], default=str)[:1500], flush=True)
    dump()


XS = {'2016-17': ('Data/Cross_Sectional', 'cs-17-', True), '2019-20': ('Data/Cross_Sectional', 'cs-19-', False)}


def d_frame(w, dirr, restricted):
    rel = f'{w}/{dirr}/ag_mod_d.dta'
    cols = ['case_id', 'gardenid', 'plotid', 'ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e',
            'ag_d20_oth', 'ag_d20b_oth']
    if restricted:
        m = meta(rel)
        return raw_read(rel, usecols=[c for c in cols if c in m.column_names])[0]
    return get_dataframe(str(ROOT / rel), convert_categoricals=False)


def hh_x02():
    out = {}
    for w, (dirr, prefix, restricted) in XS.items():
        rec = {}
        xrel = f'{w}/{dirr}/' + ('hh_mod_x.dta' if w == '2016-17' else 'HH_MOD_X.dta')
        get_dataframe(str(ROOT / xrel), convert_categoricals=False)   # pulls the blob
        m = meta(xrel)
        labels = dict(zip(m.column_names, m.column_labels))
        rec['hh_x02_label'] = labels.get('hh_x02')
        ls = m.variable_to_label.get('hh_x02')
        vl = {str(int(k)) if isinstance(k, float) and float(k).is_integer() else str(k): v
              for k, v in m.value_labels.get(ls, {}).items()} if ls else {}
        rec['hh_x02_value_labels'] = vl
        x = get_dataframe(str(ROOT / xrel), convert_categoricals=False)
        x['_h'] = x['case_id'].apply(format_id).astype(str)
        code = pd.to_numeric(x['hh_x02'], errors='coerce')
        rec['hh_x02_counts'] = vc(code)
        # season start year from the value label ("2014/2015" -> 2014)
        start = {}
        for k, v in vl.items():
            mm = re.search(r'(20\d\d)\s*[/-]', str(v))
            if mm:
                start[float(k)] = int(mm.group(1))
        rec['label_start_year'] = {str(k): v for k, v in start.items()}
        x['_S_x'] = code.map(start)
        sx = x.drop_duplicates('_h').set_index('_h')['_S_x']
        g = get_dataframe(str(ROOT / w / dirr / 'ag_mod_g.dta'), convert_categoricals=False)
        g['_h'] = g['case_id'].apply(format_id).astype(str)
        mo = pd.to_numeric(g['ag_g05a'], errors='coerce')
        yr = pd.to_numeric(g['ag_g05b'], errors='coerce')
        ok = mo.between(1, 12) & yr.between(2000, 2030)
        g['_S_g'] = np.where(ok, np.where(mo >= 7, yr, yr - 1), np.nan)
        g['_S_x'] = g['_h'].map(sx)
        dated = g[g['_S_g'].notna() & g['_S_x'].notna()]
        rec['dated_lines'] = int(len(dated))
        rec['lines_season_equals_hh_x02'] = int((dated['_S_g'] == dated['_S_x']).sum())
        rec['lines_planted_one_season_before'] = int((dated['_S_g'] == dated['_S_x'] - 1).sum())
        rec['lines_planted_one_season_after'] = int((dated['_S_g'] == dated['_S_x'] + 1).sum())
        # the relabelled lines (single code-48 text, cowpea) and their households
        d = d_frame(w, dirr, restricted)
        for df in (g, d):
            df['_k'] = (df['case_id'].apply(format_id).astype(str) + '|' + df['gardenid'].apply(format_id).astype(str)
                        + '|' + df['plotid'].apply(format_id).astype(str))
        texts = [c for c in ('ag_d20_oth', 'ag_d20b_oth') if c in d.columns]
        t = pd.DataFrame({c: norm(d[c]).values for c in texts}, index=d.index)
        n48 = sum(pd.to_numeric(d[c], errors='coerce').eq(48).astype(int) for c in
                  ('ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e'))
        single = (t.notna().sum(axis=1) == 1) & t['ag_d20_oth'].isin(list(COW)).fillna(False) & (n48 == 1) \
            & ~d['_k'].duplicated(keep=False)
        cowk = set(d.loc[single, '_k'])
        c48 = pd.to_numeric(g['crop_code'], errors='coerce').eq(48).fillna(False)
        rel = g[c48 & g['_k'].isin(cowk)]
        rel = rel[~rel['_k'].duplicated(keep=False)]
        rec['relabelled_lines'] = int(len(rel))
        rd = rel[rel['_S_g'].notna() & rel['_S_x'].notna()]
        rec['relabelled_dated'] = int(len(rd))
        rec['relabelled_season_equals_hh_x02'] = int((rd['_S_g'] == rd['_S_x']).sum())
        rh = set(rel['_h'])
        hg = g[g['_h'].isin(rh) & g['_S_g'].notna() & g['_S_x'].notna()]
        rec['relabelled_households_lines_dated'] = int(len(hg))
        rec['relabelled_households_lines_equal'] = int((hg['_S_g'] == hg['_S_x']).sum())
        # slot pairing on two-text plots: is code 48's own text cowpea?
        if 'ag_d20b_oth' in t:
            two = t.notna().sum(axis=1) == 2
            has49 = sum(pd.to_numeric(d[c], errors='coerce').eq(49).astype(int) for c in
                        ('ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e')) >= 1
            rec['two_text_plots'] = int(two.sum())
            rec['two_text_plots_with_slots_48_and_49'] = int((two & (n48 >= 1) & has49).sum())
            rec['two_text_cowpea_in_ag_d20_oth'] = int((two & t['ag_d20_oth'].isin(list(COW)).fillna(False)).sum())
            rec['two_text_cowpea_in_ag_d20b_oth_only'] = int((two & ~t['ag_d20_oth'].isin(list(COW)).fillna(False)
                                                              & t['ag_d20b_oth'].isin(list(COW)).fillna(False)).sum())
            rec['two_text_both_cowpea'] = int((two & t['ag_d20_oth'].isin(list(COW)).fillna(False)
                                               & t['ag_d20b_oth'].isin(list(COW)).fillna(False)).sum())
            rec['single_text_in_ag_d20b_oth'] = int(((t.notna().sum(axis=1) == 1) & t['ag_d20_oth'].isna()).sum())
            rec['single_text_in_ag_d20b_oth_with_48_slot'] = int(((t.notna().sum(axis=1) == 1)
                                                                  & t['ag_d20_oth'].isna() & (n48 >= 1)).sum())
        # code-49 G lines: how many, and their plot's code-49 text
        c49 = pd.to_numeric(g['crop_code'], errors='coerce').eq(49).fillna(False)
        rec['g_code49_lines'] = int(c49.sum())
        if 'ag_d20b_oth' in t and c49.any():
            tb = pd.Series(t['ag_d20b_oth'].values, index=d['_k'].values)
            tb = tb[~tb.index.duplicated(keep=False)]
            txt = g.loc[c49, '_k'].map(tb)
            rec['g_code49_text_ag_d20b_oth'] = vc(txt, 25)
            rec['g_code49_text_is_cowpea'] = int(txt.isin(list(COW)).sum())
        out[w] = rec
    return out


def h_blank():
    from lsms_library.paths import countries_root as cr
    import importlib.util
    spec = importlib.util.spec_from_file_location('mw4', cr() / 'Malawi' / '_' / 'malawi.py')
    mw = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mw)
    out = {}
    cols = ['ag_h33', 'ag_h42b_os', 'ag_h36b_oth', *sorted(mw._H_EMPTY_IN_RELEASE)]
    for w, halves in mw._SEED_LINE_SOURCES.items():
        for s in halves:
            rel = f"{w}/{s['h']}"
            raw, m = raw_read(rel)
            labels = dict(zip(m.column_names, m.column_labels))
            rec = {}
            for c in cols:
                if c not in raw.columns:
                    continue
                v = raw[c]
                blank = v.isna() | v.astype('string').str.strip().eq('').fillna(False)
                rec[c] = {'label': labels.get(c), 'dtype': str(v.dtype), 'rows': int(len(v)),
                          'nonnull': int(v.notna().sum()), 'nonblank': int((~blank).sum()),
                          'top': vc(v[~blank].astype('string'), 8)}
                parent = c[:-3] if c.endswith('_os') else c[:-4] if c.endswith('_oth') else None
                if parent and parent in raw.columns:
                    ls = m.variable_to_label.get(parent)
                    vl = m.value_labels.get(ls, {}) if ls else {}
                    other = [k for k, lab in vl.items() if re.search(r'other|specify', str(lab), re.I)]
                    pc = pd.to_numeric(raw[parent], errors='coerce')
                    rec[c]['parent'] = parent
                    rec[c]['parent_label'] = labels.get(parent)
                    rec[c]['parent_other_codes'] = {str(k): vl[k] for k in other}
                    rec[c]['parent_other_rows'] = int(pc.isin([float(k) for k in other]).sum())
                    rec[c]['parent_nonnull'] = int(pc.notna().sum())
            out[f"{w}/{s['half']}"] = rec
    return out


def seed_302():
    """The 2016-17 relabelled G lines without a served seed row: their seed answers."""
    from lsms_library.country import Country
    cp = Country('Malawi').crop_production().reset_index()
    pi = Country('Malawi').plot_inputs().reset_index()
    out = {}
    for w in ('2010-11', '2016-17', '2019-20'):
        h = cp[cp['t'].astype(str).eq(w) & cp['crop'].astype(str).eq('Cowpea (Khobwe)')
               & cp['plot_id'].astype(str).ne('Farm-level')]
        s = pi[pi['t'].astype(str).eq(w) & pi['crop'].astype(str).eq('Cowpea (Khobwe)')
               & pi['input'].astype(str).eq('Seed') & pi['plot_id'].astype(str).ne('Farm-level')]
        hk = set(zip(h['i'].astype(str), h['plot_id'].astype(str)))
        sk = set(zip(s['i'].astype(str), s['plot_id'].astype(str)))
        out[w] = {'harvest_rows': int(len(h)), 'seed_rows': int(len(s)),
                  'harvest_without_seed': sorted(list(hk - sk))[:10], 'seed_without_harvest': sorted(list(sk - hk))[:10]}
        if hk - sk:
            gone = Country('Malawi').derivation_inputs('Malawi::plot_inputs::seed-line-no-payload', wave=w).reset_index()
            gone['plot'] = (gone['gardenid'].apply(format_id).astype(str) + '_' + gone['plotid'].apply(format_id).astype(str)) \
                if 'gardenid' in gone else gone.get('ag_g0b')
            out[w]['missing_in_seed_line_no_payload'] = [
                (i, p) for i, p in hk - sk
                if ((gone['i'].astype(str) == i) & (gone['plot'].astype(str) == p)).any()]
            g = gone[[(i, p) in hk - sk for i, p in zip(gone['i'].astype(str), gone['plot'].astype(str))]]
            out[w]['their_seed_answers'] = g[[c for c in ('ag_g04a', 'ag_g04b', 'ag_g0f', 'crop_code')
                                              if c in g]].astype(str).to_dict('records')
    return out


section('hh_x02', hh_x02)
section('h_blank', h_blank)
section('seed_302', seed_302)
print('DIAG4 DONE', flush=True)
