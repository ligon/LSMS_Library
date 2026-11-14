"""Third read-only GH #1228 diagnostic: is the reference season one per interview?

usage: python diag3.py <clone> <outdir>

The release exports no D-0 / G-0 season selection.  What it does carry:
  * Module G planting month / year per line (ag_g05a / ag_g05b), from which a
    line's rainy season S is derived (planted Jul-Dec of year Y -> S = Y;
    Jan-Jun of Y -> S = Y - 1);
  * the agriculture meta file's per-module start dates (Module D, Module G);
  * the household cover's interview date.
Tests, per half: one season per household?  D and G started the same day?
Season against the G start date (a step by date means [RS] is set per
interview).  Plus 2016-17 D20 vs G crop_code_collapsed, the 2024-25 code
48 / 49 / 50 layout, and the #1712 ag_h17 'Other' rows.
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


def dump():
    (outdir / 'diag3.json').write_text(json.dumps(RES, indent=1, default=str))


def vc(s, n=60):
    return {str(k): int(v) for k, v in s.value_counts(dropna=False).head(n).items()}


def local_copy(rel):
    p = ROOT / rel
    _ensure_dvc_pulled(str(p))
    blob = _dvc_cache_path(str(p))
    tmp = Path(tempfile.mkdtemp()) / Path(rel).name
    shutil.copyfile(blob, tmp)
    return tmp


def read_raw(rel, usecols=None, formats=False):
    tmp = local_copy(rel)
    try:
        df, m = pyreadstat.read_dta(str(tmp), usecols=usecols, apply_value_formats=formats)
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)
    return df, m


def to_date(s):
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors='coerce').dt.normalize()
    if pd.api.types.is_numeric_dtype(s):
        x = pd.to_numeric(s, errors='coerce')
        unit = 'ms' if x.dropna().abs().median() > 1e8 else 'D'
        return (pd.Timestamp('1960-01-01') + pd.to_timedelta(x, unit=unit)).dt.normalize()
    return pd.to_datetime(s.astype('string').str.slice(0, 19), errors='coerce').dt.normalize()


HALVES = {
    ('2016-17', 'xs'): dict(g='Data/Cross_Sectional/ag_mod_g.dta', d='Data/Cross_Sectional/ag_mod_d.dta',
                            meta='Data/Cross_Sectional/ag_metadata.dta', cover='Data/Cross_Sectional/hh_mod_a_filt.dta',
                            hh='case_id', cdate=['interviewDate'], restricted=True),
    ('2016-17', 'panel'): dict(g='Data/Panel/ag_mod_g_16.dta', d='Data/Panel/ag_mod_d_16.dta',
                               meta='Data/Panel/ag_meta_16.dta', cover='Data/Panel/hh_mod_a_filt_16.dta',
                               hh='y3_hhid', cdate=['interviewdate_v1', 'interviewdate_v2']),
    ('2019-20', 'xs'): dict(g='Data/Cross_Sectional/ag_mod_g.dta', d='Data/Cross_Sectional/ag_mod_d.dta',
                            meta='Data/Cross_Sectional/ag_mod_meta.dta', cover='Data/Cross_Sectional/hh_mod_a_filt.dta',
                            hh='case_id', cdate=['interviewDate']),
    ('2019-20', 'panel'): dict(g='Data/Panel/ag_mod_g_19.dta', d='Data/Panel/ag_mod_d_19.dta',
                               meta='Data/Panel/ag_meta_19.dta', cover='Data/Panel/hh_mod_a_filt_19.dta',
                               hh='y4_hhid', cdate=['interviewdate_v1', 'interviewdate_v2']),
    ('2024-25', 'xs'): dict(g='Data/Cross_Sectional/ag_mod_g.dta', d='Data/Cross_Sectional/ag_mod_d.dta',
                            meta='Data/Cross_Sectional/ag_mod_meta.dta', cover='Data/Cross_Sectional/hh_mod_a_filt.dta',
                            hh='case_id', cdate=['interviewDate']),
    ('2024-25', 'panel'): dict(g='Data/Panel/ag_mod_g_24.dta', d='Data/Panel/ag_mod_d_24.dta',
                               meta='Data/Panel/ag_meta_24.dta', cover='Data/Panel/hh_mod_a_filt_24.dta',
                               hh='y5_hhid', cdate=['interviewdate_v1', 'interviewdate_v2', 'interviewDate']),
}

RES['season'] = {}
for (w, half), h in HALVES.items():
    rec = {}
    try:
        hh = h['hh']
        g = get_dataframe(str(ROOT / w / h['g']), convert_categoricals=False)
        g['_h'] = g[hh].apply(format_id).astype(str)
        mo = pd.to_numeric(g['ag_g05a'], errors='coerce')
        yr = pd.to_numeric(g['ag_g05b'], errors='coerce')
        ok = mo.between(1, 12) & yr.between(2000, 2030)
        g['_S'] = np.where(ok, np.where(mo >= 7, yr, yr - 1), np.nan)
        rec['g_lines'] = int(len(g))
        rec['g_lines_dated'] = int(ok.sum())
        rec['line_season'] = vc(g['_S'])
        per = g.dropna(subset=['_S']).groupby('_h')['_S'].agg(['nunique', 'min', 'max', 'size'])
        rec['households_dated'] = int(len(per))
        rec['households_one_season'] = int((per['nunique'] == 1).sum())
        rec['households_two_plus'] = int((per['nunique'] > 1).sum())
        mixed = g[g['_h'].isin(per.index[per['nunique'] > 1])]
        rec['mixed_household_line_seasons'] = vc(mixed.groupby('_h')['_S'].agg(lambda s: tuple(sorted(s.unique()))).astype(str), 20)
        # code-48 lines: their household's season set
        c48 = pd.to_numeric(g['crop_code'], errors='coerce').eq(48).fillna(False)
        h48 = set(g.loc[c48, '_h'])
        rec['g48_households'] = len(h48)
        rec['g48_households_one_season'] = int(((per['nunique'] == 1) & per.index.isin(h48)).sum())
        rec['g48_households_two_plus'] = int(((per['nunique'] > 1) & per.index.isin(h48)).sum())
        # modal season per household
        modal = g.dropna(subset=['_S']).groupby('_h')['_S'].agg(lambda s: s.mode().iloc[0])
        # module start dates
        meta = get_dataframe(str(ROOT / w / h['meta']), convert_categoricals=False)
        mcols = list(meta.columns)
        dcol = next((c for c in mcols if re.fullmatch(r'ag_moduleD_start_?date', c, re.I)), None)
        gcol = next((c for c in mcols if re.fullmatch(r'ag_moduleG_start_?date', c, re.I)), None)
        rec['meta_cols'] = {'D': dcol, 'G': gcol, 'hh_in_meta': hh in mcols, 'n_rows': int(len(meta)),
                            'dtype_D': str(meta[dcol].dtype) if dcol else None,
                            'sample_D': [str(x) for x in meta[dcol].dropna().head(3)] if dcol else None}
        if dcol and gcol and hh in mcols:
            meta['_h'] = meta[hh].apply(format_id).astype(str)
            meta['_dD'] = to_date(meta[dcol])
            meta['_dG'] = to_date(meta[gcol])
            mm = meta.drop_duplicates('_h').set_index('_h')
            rec['meta_households'] = int(len(mm))
            both = mm[['_dD', '_dG']].dropna()
            diff = (both['_dG'] - both['_dD']).dt.days
            rec['D_G_start_same_day'] = int((diff == 0).sum())
            rec['D_G_start_diff_days'] = vc(diff.clip(-400, 400), 30)
            rec['D_G_both_dated'] = int(len(both))
            # season vs G start month (the agriculture interview)
            j = pd.concat([modal.rename('S'), mm['_dG'].dt.to_period('M').astype(str).rename('gmonth'),
                           mm['_dD'].dt.to_period('M').astype(str).rename('dmonth')], axis=1, join='inner')
            tab = pd.crosstab(j['gmonth'], j['S'])
            rec['season_by_G_start_month'] = {str(m): {str(int(k)): int(v) for k, v in r.items() if v}
                                              for m, r in tab.iterrows()}
            tabd = pd.crosstab(j['dmonth'], j['S'])
            rec['season_by_D_start_month'] = {str(m): {str(int(k)): int(v) for k, v in r.items() if v}
                                              for m, r in tabd.iterrows()}
            # the G48 households' D/G start dates
            rec['g48_D_G_same_day'] = int(((diff == 0) & diff.index.isin(h48)).sum())
            rec['g48_D_G_dated'] = int(diff.index.isin(h48).sum())
        # cover interview date
        cov = get_dataframe(str(ROOT / w / h['cover']), convert_categoricals=False)
        if hh in cov.columns:
            cov['_h'] = cov[hh].apply(format_id).astype(str)
            for c in h['cdate']:
                if c in cov.columns:
                    dd = to_date(cov[c])
                    j = pd.concat([modal.rename('S'), pd.Series(dd.dt.to_period('M').astype(str).values,
                                                                  index=cov['_h']).groupby(level=0).first().rename('m')],
                                  axis=1, join='inner')
                    tab = pd.crosstab(j['m'], j['S'])
                    rec[f'season_by_cover_{c}_month'] = {str(m): {str(int(k)): int(v) for k, v in r.items() if v}
                                                         for m, r in tab.iterrows()}
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
        rec['traceback'] = traceback.format_exc()[-3000:]
    RES['season'][f'{w}/{half}'] = rec
    print(w, half, {k: rec.get(k) for k in ('g_lines_dated', 'households_dated', 'households_one_season',
                                             'households_two_plus', 'D_G_both_dated', 'D_G_start_same_day',
                                             'error')}, flush=True)
    dump()

# 2016-17: D20 against G's crop_code_collapsed
RES['roster_2016_collapsed'] = {}
for half, (drel, grel, hh, restricted) in {
        'xs': ('Data/Cross_Sectional/ag_mod_d.dta', 'Data/Cross_Sectional/ag_mod_g.dta', 'case_id', True),
        'panel': ('Data/Panel/ag_mod_d_16.dta', 'Data/Panel/ag_mod_g_16.dta', 'y3_hhid', False)}.items():
    rec = {}
    try:
        slots = ['ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e']
        if restricted:
            d, _ = read_raw(f'2016-17/{drel}', usecols=[hh, 'gardenid', 'plotid', *slots])
        else:
            d = get_dataframe(str(ROOT / '2016-17' / drel), convert_categoricals=False)
        g = get_dataframe(str(ROOT / '2016-17' / grel), convert_categoricals=False)
        for df in (d, g):
            df['_k'] = (df[hh].apply(format_id).astype(str) + '|' + df['gardenid'].apply(format_id).astype(str)
                        + '|' + df['plotid'].apply(format_id).astype(str))
        dl = d.melt(id_vars='_k', value_vars=slots, value_name='code')
        dl['code'] = pd.to_numeric(dl['code'], errors='coerce')
        dp = set(zip(dl.dropna(subset=['code'])['_k'], dl.dropna(subset=['code'])['code'].astype(int)))
        for col in ('crop_code', 'crop_code_collapsed'):
            c = pd.to_numeric(g[col], errors='coerce')
            gp = set(zip(g.loc[c.notna(), '_k'], c.dropna().astype(int)))
            rec[col] = {'g_pairs': len(gp), 'in_d': len(gp & dp), 'd_pairs': len(dp), 'd_in_g': len(dp & gp)}
        rec['d20_codes_top'] = vc(dl['code'], 50)
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
    RES['roster_2016_collapsed'][half] = rec
    print('2016 collapsed', half, rec.get('crop_code'), rec.get('crop_code_collapsed'), rec.get('error'), flush=True)
dump()

# 2024-25: codes 48 / 49 / 50 and their texts
RES['ihs6_layout'] = {}
for half, (drel, grel, irel, hh) in {
        'xs': ('Data/Cross_Sectional/ag_mod_d.dta', 'Data/Cross_Sectional/ag_mod_g.dta',
               'Data/Cross_Sectional/ag_mod_i.dta', 'case_id'),
        'panel': ('Data/Panel/ag_mod_d_24.dta', 'Data/Panel/ag_mod_g_24.dta', 'Data/Panel/ag_mod_i_24.dta', 'y5_hhid')}.items():
    rec = {}
    try:
        d = get_dataframe(str(ROOT / '2024-25' / drel), convert_categoricals=False)
        g = get_dataframe(str(ROOT / '2024-25' / grel), convert_categoricals=False)
        i = get_dataframe(str(ROOT / '2024-25' / irel), convert_categoricals=False)
        gc = pd.to_numeric(g['crop_code'], errors='coerce')
        rec['g_rows_by_code_48_49_50'] = {str(k): int((gc == k).sum()) for k in (48, 49, 50)}
        ic = pd.to_numeric(i['crop_code'], errors='coerce')
        rec['i_rows_by_code_48_49_50'] = {str(k): int((ic == k).sum()) for k in (48, 49, 50)}
        rec['i_sold_by_code_48_49_50'] = {str(k): int(((ic == k) & pd.to_numeric(i['ag_i03'], errors='coerce').gt(0)).sum())
                                          for k in (48, 49, 50)}
        slots = ['ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e']
        codes = pd.DataFrame({s: pd.to_numeric(d[s], errors='coerce') for s in slots})
        norm = lambda s: s.astype('string').str.strip().str.replace(r'\s+', ' ', regex=True).str.upper().replace('', pd.NA)  # noqa: E731
        t1, t2 = norm(d['ag_d20_oth']), norm(d['ag_d20b_oth'])
        has48, has49, has50 = codes.eq(48).any(axis=1), codes.eq(49).any(axis=1), codes.eq(50).any(axis=1)
        rec['d_rows'] = int(len(d))
        rec['d_has48_has49_text1_text2'] = vc(has48.astype(int).astype(str) + has49.astype(int).astype(str)
                                              + t1.notna().astype(int).astype(str) + t2.notna().astype(int).astype(str))
        rec['d_has50'] = int(has50.sum())
        rec['text1_on_48_only_plots_cowpea'] = int((has48 & ~has49 & t1.isin(['COW PEAS', 'COWPEAS'])).sum())
        rec['text2_on_49_plots_cowpea'] = int((has49 & t2.isin(['COW PEAS', 'COWPEAS'])).sum())
        rec['text1_any_cowpea'] = int(t1.isin(['COW PEAS', 'COWPEAS']).sum())
        rec['text2_any_cowpea'] = int(t2.isin(['COW PEAS', 'COWPEAS']).sum())
        rec['d_label_ag_d20_oth'] = None
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
        rec['traceback'] = traceback.format_exc()[-2000:]
    RES['ihs6_layout'][half] = rec
    print('ihs6', half, rec, flush=True)
dump()

# #1712: where (if anywhere) is the text of the 21 ag_h17 = 5 rows?
RES['h17'] = {}
try:
    for rel in ('2010-11/Data/Full_Sample/Agriculture/ag_mod_h.dta', '2010-11/Data/Panel/Agriculture/ag_mod_h.dta'):
        rec = {}
        try:
            h, m = read_raw(rel)
            labels = dict(zip(m.column_names, m.column_labels))
            rec['rows'] = int(len(h))
            rec['has_ag_h17_os'] = 'ag_h17_os' in h.columns
            if 'ag_h17_os' in h.columns:
                s = h['ag_h17_os'].astype('string').str.strip().replace('', pd.NA)
                rec['ag_h17_os_nonblank'] = int(s.notna().sum())
            if 'ag_h17' in h.columns:
                other = pd.to_numeric(h['ag_h17'], errors='coerce').eq(5)
                rec['ag_h17_other_rows'] = int(other.sum())
                sub = h[other]
                texts = {}
                for c in h.columns:
                    if h[c].dtype == object:
                        v = sub[c].astype('string').str.strip().replace('', pd.NA).dropna()
                        if len(v):
                            texts[c] = {'label': labels.get(c), 'n': int(len(v)), 'top': vc(v, 8)}
                rec['nonblank_string_columns_on_those_rows'] = texts
                rec['all_os_columns_nonblank_counts'] = {
                    c: int(h[c].astype('string').str.strip().replace('', pd.NA).notna().sum())
                    for c in h.columns if c.endswith('_os')}
        except Exception as e:  # noqa: BLE001
            rec['error'] = f'{type(e).__name__}: {e}'
        RES['h17'][rel] = rec
        print('h17', rel, {k: v for k, v in rec.items() if k != 'nonblank_string_columns_on_those_rows'}, flush=True)
except Exception as e:  # noqa: BLE001
    RES['h17']['error'] = str(e)
dump()
print('DIAG3 DONE', flush=True)
