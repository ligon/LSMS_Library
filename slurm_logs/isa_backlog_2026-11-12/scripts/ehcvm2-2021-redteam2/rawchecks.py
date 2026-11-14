"""Independent checks of the GH #1697 delta on the raw files (aggregates only).

usage: rawchecks.py CLONE OUT.json
Runs at head 9b5585cf2 in a fresh private root.  Every section is guarded and
the JSON is dumped after each one.  No raw rows are written: counts, shares,
quantiles and modes only.
"""
import json, os, sys, time, traceback, warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env

clone, out = sys.argv[1:3]
assert_env(clone)
warnings.simplefilter('ignore')
import numpy as np
import pandas as pd
import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

CR = countries_root()
T21 = CR / 'Togo' / '2021-22' / 'Data'
T18 = CR / 'Togo' / '2018' / 'Data1'
C21 = CR / 'CotedIvoire' / '2021-22' / 'Data'
C18 = CR / 'CotedIvoire' / '2018-19' / 'Data' / 'Menage'
FACTOR = {'Centrale': 1.6725, 'Savanes': 1.338, 'Plateaux': 1.2265, 'Kara': 1.0035,
          'Maritime': 1.0035, 'Grand Lomé': 0.9143}
LOME = 'Grand Lomé'
K_DIV = 'Togo::food_acquired::s07bq03a-regional-factor-divided-out'
K_REC = 'Togo::food_acquired::s07bq03a-floored-count-recovered'
K_WH = 'Togo::food_acquired::s07bq03a-floored-count-withheld'
K_H = 'CotedIvoire::food_acquired::s07bq03a-outlier-replaced-withheld'
K_L = 'CotedIvoire::food_acquired::s07bq03a-partial-triplet-conversion'
K_STRATA = 'CotedIvoire::sample::region-milieu-stratum-upper-region'
res = {}
T0 = time.time()


def dump():
    json.dump(res, open(out, 'w'), indent=1, default=str)


def section(name):
    def deco(fn):
        t0 = time.time()
        try:
            res[name] = fn()
        except Exception as exc:
            res[name] = {'ERROR': f'{type(exc).__name__}: {exc}'[:1000],
                         'tb': traceback.format_exc()[-2500:]}
        res.setdefault('_timing', {})[name] = round(time.time() - t0, 1)
        print(f'[{round(time.time() - T0)}s] section {name}: '
              f'{"ERROR " + res[name]["ERROR"][:200] if isinstance(res[name], dict) and "ERROR" in res[name] else "ok"}',
              flush=True)
        dump()
        return fn
    return deco


def gd(path, labels):
    return get_dataframe(str(path), convert_categoricals=labels)


def num(s):
    return pd.to_numeric(s, errors='coerce').astype(float)


def on_grid(x, step, tol=1e-6):
    k = np.asarray(x, dtype=float) / step
    return np.abs(k - np.round(k)) <= tol * np.maximum(1.0, np.abs(k))


def same(a, b, tol=1e-6):
    a = np.asarray(a, dtype=float); b = np.asarray(b, dtype=float)
    return np.abs(a - b) <= tol * np.maximum(1.0, np.abs(b))


def share(mask, base=None):
    mask = np.asarray(mask, dtype=bool)
    if base is not None:
        base = np.asarray(base, dtype=bool)
        n = int(base.sum())
        return {'n': n, 'k': int((mask & base).sum()), 'share': round(float((mask & base).sum()) / n, 4) if n else None}
    n = len(mask)
    return {'n': n, 'k': int(mask.sum()), 'share': round(float(mask.sum()) / n, 4) if n else None}


def modes(x, nd=4, top=3):
    s = pd.Series(np.round(np.asarray(x, dtype=float), nd))
    return {str(k): int(v) for k, v in s.value_counts().head(top).items()}


def q(x, qs=(0, .1, .5, .9, 1)):
    x = np.asarray(x, dtype=float); x = x[np.isfinite(x)]
    return [round(float(v), 6) for v in np.quantile(x, qs)] if len(x) else None


# ----------------------------------------------------------------- Togo raw
TG = {}


def togo_frames():
    if TG:
        return TG
    s07 = gd(T21 / 's07b_me_tgo2021.dta', False)
    s07l = gd(T21 / 's07b_me_tgo2021.dta', True)
    cov = gd(T21 / 's00_me_tgo2021.dta', False)[['grappe', 'menage']].copy()
    cov['region'] = gd(T21 / 's00_me_tgo2021.dta', True)['s00q01'].astype('string').str.strip().to_numpy()
    s07 = s07.merge(cov[['grappe', 'menage', 'region']], on=['grappe', 'menage'], how='left',
                    validate='many_to_one')
    TG['s07'] = s07
    TG['s07l'] = s07l
    TG['q3'] = num(s07['s07bq03a']).to_numpy()
    TG['q4'] = num(s07['s07bq04']).fillna(0).to_numpy()
    TG['q5'] = num(s07['s07bq05']).fillna(0).to_numpy()
    TG['region'] = s07['region'].astype(str).to_numpy()
    TG['unit'] = num(s07['s07bq03b']).to_numpy()
    TG['count'] = TG['unit'] == 147
    TG['jam'] = (num(s07['s07bq06']) == 5).to_numpy()
    TG['f'] = pd.Series(TG['region']).map(FACTOR).to_numpy(dtype=float)
    TG['lome'] = TG['region'] == LOME
    TG['og'] = (TG['q4'] > 0) | (TG['q5'] > 0)
    TG['feff'] = np.where(TG['lome'] & TG['og'], 1.2, TG['f'])
    return TG


@section('togo_labels_and_regions')
def _():
    t = togo_frames()
    s07, s07l = t['s07'], t['s07l']
    lab06 = pd.crosstab(num(s07['s07bq06']), s07l['s07bq06'].astype(str))
    unit147 = s07l.loc[num(s07['s07bq03b']) == 147, 's07bq03b'].astype(str).value_counts().head(3).to_dict()
    return {'rows': len(s07), 'region_counts': pd.Series(t['region']).value_counts().to_dict(),
            'region_missing': int(pd.isna(s07['region']).sum()),
            's07bq06_code_to_label': {str(int(c)): lab06.columns[lab06.loc[c].to_numpy().argmax()] for c in lab06.index},
            'unit_147_label': unit147,
            'q3_dtype': str(s07['s07bq03a'].dtype), 'q4_dtype': str(s07['s07bq04'].dtype)}


@section('togo_factor_by_region')
def _():
    t = togo_frames()
    q3, q4, q5, f, feff = t['q3'], t['q4'], t['q5'], t['f'], t['feff']
    pos = q3 > 0
    own = pos & t['jam'] & (q4 > 0) & (q5 == 0)
    gift = pos & t['jam'] & (q5 > 0) & (q4 == 0)
    out_ = {}
    for r in FACTOR:
        reg = t['region'] == r
        for cls, cm in (('noncount', ~t['count']), ('count', t['count'])):
            for kind, base in (('own_only_jamais', own), ('gift_only_jamais', gift)):
                b = base & reg & cm
                truth = q4 if kind == 'own_only_jamais' else q5
                exact = same(q3, truth * feff)
                fl = np.abs(q3 - np.floor(truth * feff + 1e-9)) <= 1e-9
                unity = same(q3, truth)
                with np.errstate(divide='ignore', invalid='ignore'):
                    ratio = q3 / truth
                out_[f'{r}|{cls}|{kind}'] = {
                    'n': int(b.sum()), 'factor_used': float(np.unique(feff[b])[0]) if b.any() else None,
                    'exact_x_f': share(exact, b)['share'], 'floor_x_f_not_exact': share(fl & ~exact, b)['share'],
                    'ratio_is_1': share(unity, b)['share'], 'ratio_modes': modes(ratio[b], 6) if b.any() else None}
    return out_


@section('togo_unit_by_unit')
def _():
    """Per unit code: does any unit besides 147 look floored?  (a) own-production-only
    Jamais rows in the three regions with f > 1.1: exact vs floor-only; (b) all positive
    rows in those regions: share of filed s07bq03a that is an integer, and share of
    s07bq03a / f on the quarter grid."""
    t = togo_frames()
    q3, q4, q5, f = t['q3'], t['q4'], t['q5'], t['f']
    big = np.isin(t['region'], ['Centrale', 'Savanes', 'Plateaux'])
    pos = (q3 > 0) & big
    own = pos & t['jam'] & (q4 > 0) & (q5 == 0)
    exact = same(q3, q4 * f)
    fl = (np.abs(q3 - np.floor(q4 * f + 1e-9)) <= 1e-9) & ~exact
    integer = on_grid(q3, 1)
    div_grid = on_grid(q3 / f, 0.25)
    label = t['s07l']['s07bq03b'].astype(str).to_numpy()
    rows = []
    for u in np.unique(t['unit'][pos & np.isfinite(t['unit'])]):
        m = pos & (t['unit'] == u)
        mo = own & (t['unit'] == u)
        if m.sum() < 30:
            continue
        rows.append({'code': int(u), 'label': pd.Series(label[m]).value_counts().index[0],
                     'n_pos': int(m.sum()), 'filed_integer': round(float(integer[m].mean()), 3),
                     'divided_on_quarter_grid': round(float(div_grid[m].mean()), 3),
                     'own_n': int(mo.sum()),
                     'own_exact': round(float(exact[mo].mean()), 3) if mo.any() else None,
                     'own_floor_only': round(float(fl[mo].mean()), 3) if mo.any() else None})
    rows.sort(key=lambda r: -r['filed_integer'])
    suspicious = [r for r in rows if r['code'] != 147 and r['filed_integer'] > 0.5 and r['divided_on_quarter_grid'] < 0.5]
    return {'units': rows[:60], 'n_units': len(rows), 'suspicious_floored_units': suspicious}


@section('togo_purchase_only_grid')
def _():
    """Purchase-only rows have no s07bq04 to check the factor against.  Quarter-grid share of
    the filed and the divided value, by region; 2018 control."""
    t = togo_frames()
    q3, q4, q5 = t['q3'], t['q4'], t['q5']
    po = (q3 > 0) & (q4 == 0) & (q5 == 0) & ~t['count']
    out_ = {}
    for r, fr in FACTOR.items():
        b = po & (t['region'] == r)
        out_[r] = {'n': int(b.sum()), 'filed_grid': share(on_grid(q3, .25), b)['share'],
                   'divided_grid': share(on_grid(q3 / fr, .25), b)['share'],
                   'divided_by_1.2_grid': share(on_grid(q3 / 1.2, .25), b)['share']}
    s18 = gd(T18 / 's07b_me_tgo2018.dta', False)
    c18 = gd(T18 / 's00_me_tgo2018.dta', False)[['grappe', 'menage']].copy()
    c18['s00q01'] = gd(T18 / 's00_me_tgo2018.dta', True)['s00q01'].astype('string').str.strip().to_numpy()
    s18 = s18.merge(c18, on=['grappe', 'menage'], how='left', validate='many_to_one')
    a3, a4, a5 = num(s18['s07bq03a']), num(s18['s07bq04']).fillna(0), num(s18['s07bq05']).fillna(0)
    po18 = (a3 > 0) & (a4 == 0) & (a5 == 0) & (num(s18['s07bq03b']) != 147)
    c2018 = {}
    for r, g in s18[po18].groupby(s18.loc[po18, 's00q01'].astype(str)):
        c2018[r] = {'n': int(len(g)), 'filed_grid': round(float(on_grid(num(g['s07bq03a']), .25).mean()), 4)}
    own18 = (a3 > 0) & (a4 > 0) & (a5 == 0) & (num(s18['s07bq06']) == 5)
    c2018_own = {r: {'n': int(len(g)), 'ratio_is_1': round(float(same(num(g['s07bq03a']), num(g['s07bq04'])).mean()), 4)}
                 for r, g in s18[own18].groupby(s18.loc[own18, 's00q01'].astype(str))}
    return {'2021': out_, '2018_purchase_only': c2018, '2018_own_only_jamais_ratio1': c2018_own}


@section('togo_lome_predicate')
def _():
    """Grand Lomé rows with own production or a gift are divided by 1.2.  How many fit 0.9143
    instead, and what does dividing them by 1.2 do to the purchased quantity?"""
    t = togo_frames()
    q3, q4, q5 = t['q3'], t['q4'], t['q5']
    s07 = t['s07']
    b = t['lome'] & t['og'] & (q3 > 0) & ~t['count']
    g12 = on_grid(q3 / 1.2, .25)
    g91 = on_grid(q3 / 0.9143, .25)
    g12c = on_grid(q3 / 1.2, .01)
    g91c = on_grid(q3 / 0.9143, .01)
    fit91 = b & g91c & ~g12c
    r12 = np.clip(q3 / 1.2 - q4 - q5, 0, None)
    r91 = np.clip(q3 / 0.9143 - q4 - q5, 0, None)
    q7, v8 = num(s07['s07bq07a']).to_numpy(), num(s07['s07bq08']).to_numpy()
    same_unit = ((num(s07['s07bq03b']) == num(s07['s07bq07b'])) & (num(s07['s07bq03c']) == num(s07['s07bq07c']))).to_numpy()
    uv = np.where(same_unit & (q7 > 0), v8 / np.where(q7 > 0, q7, np.nan), np.nan)
    own_only_cnt = t['lome'] & t['count'] & t['jam'] & (q4 > 0) & (q5 == 0) & (q3 > 0)
    return {'lome_og_noncount_pos': int(b.sum()),
            'quarter_grid_at_1.2': share(g12, b), 'quarter_grid_at_0.9143': share(g91, b),
            'cent_grid_at_1.2': share(g12c, b), 'cent_grid_at_0.9143_not_1.2': share(fit91, b),
            'neither_cent_grid': share(~g12c & ~g91c, b),
            'fit91_rows_purchase_under_1.2': int((fit91 & (r12 > 1e-6)).sum()),
            'fit91_rows_purchase_under_0.9143': int((fit91 & (r91 > 1e-6)).sum()),
            'fit91_purchased_qty_sum_1.2_vs_0.9143': [float(r12[fit91].sum()), float(r91[fit91].sum())],
            'fit91_value_at_unit_value_1.2_vs_0.9143_FCFA': [float(np.nansum(r12[fit91] * uv[fit91])),
                                                             float(np.nansum(r91[fit91] * uv[fit91]))],
            'lome_own_only_count_rows': int(own_only_cnt.sum()),
            'lome_own_only_count_exact_x1.2': share(same(q3, q4 * 1.2), own_only_cnt),
            'lome_own_only_count_floor_x1.2_not_exact': share((np.abs(q3 - np.floor(q4 * 1.2 + 1e-9)) <= 1e-9) & ~same(q3, q4 * 1.2), own_only_cnt)}


TGR = {}


def togo_recovered():
    if TGR:
        return TGR
    t = togo_frames()
    c = ll.Country('Togo')
    recs = c.derivations()
    TGR['recover'] = recs[K_DIV].resolve()
    TGR['classify'] = recs[K_WH].resolve()
    TGR['same_fn'] = recs[K_REC].resolve() is TGR['recover']
    args = (t['s07']['s07bq03a'], t['s07']['s07bq03b'], t['region'], t['s07']['s07bq04'], t['s07']['s07bq05'])
    TGR['value'] = np.asarray(TGR['recover'](*args), dtype=float)
    TGR['branch'] = np.asarray(TGR['classify'](*args), dtype=object)
    return TGR


@section('togo_recovery_branches_and_jamais')
def _():
    t, r = togo_frames(), togo_recovered()
    q3, q4, q5, v, br = t['q3'], t['q4'], t['q5'], r['value'], r['branch']
    s06 = num(t['s07']['s07bq06']).to_numpy()
    filed_res = q3 - q4 - q5
    rec_res = v - q4 - q5
    jam = t['jam']
    out_ = {'registry_functions_resolve': True, 'recover_is_count_function_too': r['same_fn'],
            'branch_counts': pd.Series(br).astype(str).value_counts().to_dict(),
            'branch_by_region_count': pd.crosstab(pd.Series(t['region']), pd.Series(br).astype(str) + np.where(t['count'], '|count', '|noncount')).to_dict()}
    left = jam & (rec_res > 1e-9)
    out_['jamais'] = {'n': int(jam.sum()), 'filed_residual_pos': int((jam & (filed_res > 1e-6)).sum()),
                      'recovered_residual_pos': int(left.sum()),
                      'left_by_region': pd.Series(t['region'][left]).value_counts().to_dict(),
                      'left_by_branch': pd.Series(br[left]).astype(str).value_counts().to_dict(),
                      'left_by_count_unit': int((left & t['count']).sum()),
                      'left_residual_q': q(rec_res[left]),
                      'left_value_on_quarter_grid': share(on_grid(v, .25), left)['share'],
                      'left_q4q5_integer': share(on_grid(q4 + q5, 1), left)['share'],
                      'left_filed_over_q4q5_modes': modes(np.where(q4 + q5 > 0, q3 / np.where(q4 + q5 > 0, q4 + q5, 1), np.nan)[left], 4, 6),
                      'left_ratio_over_factor_modes': modes((np.where(q4 + q5 > 0, q3 / np.where(q4 + q5 > 0, q4 + q5, 1), np.nan) / t['feff'])[left], 4, 6),
                      'left_q4q5_zero': int((left & (q4 + q5 == 0)).sum())}
    # 2018 baseline
    s18 = gd(T18 / 's07b_me_tgo2018.dta', False)
    a3, a4, a5 = num(s18['s07bq03a']), num(s18['s07bq04']).fillna(0), num(s18['s07bq05']).fillna(0)
    j18 = num(s18['s07bq06']) == 5
    out_['jamais_2018'] = {'n': int(j18.sum()), 'residual_pos': int((j18 & (a3 - a4 - a5 > 1e-6)).sum())}
    # purchased rows that vanish: a positive filed residual, none after recovery
    van = (filed_res > 1e-6) & ~(rec_res > 1e-9) & ~pd.isna(v)
    out_['vanished_purchased'] = {'n': int(van.sum()),
                                  'by_s07bq06': pd.Series(s06[van]).value_counts().to_dict(),
                                  'by_region': pd.Series(t['region'][van]).value_counts().to_dict(),
                                  'with_last_purchase_value': int((van & (num(t['s07']['s07bq08']).to_numpy() > 0)).sum())}
    # recovered below q4 + q5
    below = (~pd.isna(v)) & (rec_res < -1e-6)
    out_['recovered_below_q4q5'] = {'n': int(below.sum()), 'count_unit': int((below & t['count']).sum()),
                                    'by_region': pd.Series(t['region'][below]).value_counts().to_dict()}
    return out_


@section('togo_integer_recovery_truth')
def _():
    """Ground truth: on Jamais own-production-only or gift-only `unité` rows outside Grand Lomé,
    the household consumed exactly s07bq04 + s07bq05, which carry no factor."""
    t, r = togo_frames(), togo_recovered()
    q3, q4, q5, v, br = t['q3'], t['q4'], t['q5'], r['value'], r['branch']
    base = t['count'] & ~t['lome'] & t['jam'] & ~np.isnan(q3) & (((q4 > 0) & (q5 == 0)) | ((q5 > 0) & (q4 == 0)))
    truth = q4 + q5
    withheld = pd.isna(v)
    right = ~withheld & same(v, truth)
    wrong = ~withheld & ~right
    tint = on_grid(truth, 1)
    filed_consistent = np.abs(q3 - np.floor(truth * t['f'] + 1e-9)) <= 1e-9
    out_ = {'n': int(base.sum()), 'right': int((base & right).sum()), 'wrong': int((base & wrong).sum()),
            'withheld': int((base & withheld).sum()),
            'truth_integer_share': share(tint, base)['share'],
            'filed_equals_floor_truth_x_f': share(filed_consistent, base)['share'],
            'wrong_truth_nonint': int((base & wrong & ~tint).sum()),
            'wrong_truth_int_filed_inconsistent': int((base & wrong & tint & ~filed_consistent).sum()),
            'withheld_truth_nonint': int((base & withheld & ~tint).sum()),
            'by_region': {}}
    for reg in FACTOR:
        if reg == LOME:
            continue
        b = base & (t['region'] == reg)
        out_['by_region'][reg] = {'n': int(b.sum()), 'right': int((b & right).sum()),
                                  'wrong': int((b & wrong).sum()), 'withheld': int((b & withheld).sum()),
                                  'truth_nonint': int((b & ~tint).sum())}
    # error sizes on the wrong rows
    e = (v - truth)[base & wrong]
    out_['wrong_error_q'] = q(e)
    # non-integer shares of the unscaled columns on count rows, every region
    pos4 = t['count'] & (q4 > 0)
    pos5 = t['count'] & (q5 > 0)
    q7 = num(t['s07']['s07bq07a']).to_numpy()
    u7 = num(t['s07']['s07bq07b']).to_numpy()
    pos7 = (u7 == 147) & (q7 > 0)
    out_['count_rows_nonint'] = {'q4': share(~on_grid(q4, 1), pos4), 'q5': share(~on_grid(q5, 1), pos5),
                                 'q7a_unite': share(~on_grid(q7, 1), pos7)}
    items = t['s07l']['s07bq01'].astype(str).to_numpy()
    ni4 = pos4 & ~on_grid(q4, 1)
    out_['q4_nonint_top_items'] = pd.Series(items[ni4]).value_counts().head(8).to_dict()
    out_['q7_nonint_top_items'] = pd.Series(items[pos7 & ~on_grid(q7, 1)]).value_counts().head(8).to_dict()
    return out_


@section('togo_integer_recovery_simulation')
def _():
    """2018's positive `unité` s07bq03a (no factor) as the true counts; scale by each factor,
    floor, and recover with the head function."""
    t, r = togo_frames(), togo_recovered()
    s18 = gd(T18 / 's07b_me_tgo2018.dta', False)
    s18l = gd(T18 / 's07b_me_tgo2018.dta', True)
    a3 = num(s18['s07bq03a']).to_numpy()
    cnt = (num(s18['s07bq03b']).to_numpy() == 147) & (a3 > 0)
    k = a3[cnt]
    kint = on_grid(k, 1)
    items = s18l['s07bq01'].astype(str).to_numpy()[cnt]
    out_ = {'n_2018_count_pos': int(cnt.sum()), 'integer_share': round(float(kint.mean()), 5),
            'nonint_top_items': pd.Series(items[~kint]).value_counts().head(10).to_dict(),
            'nonint_values_q': q(k[~kint]), 'nonint_below_1': int((~kint & (k < 1)).sum()),
            'factors': {}}
    n21 = {reg: int((t['count'] & (t['region'] == reg) & (t['q3'] >= 0)).sum()) for reg in FACTOR}
    expected_wrong = 0.0
    for reg, f in FACTOR.items():
        if reg == LOME:
            continue
        filed = np.floor(k * f + 1e-9)
        n = len(k)
        v = np.asarray(r['recover'](filed, np.full(n, 147), np.full(n, reg, dtype=object),
                                    np.zeros(n), np.zeros(n)), dtype=float)
        wh = np.isnan(v)
        rt = ~wh & (np.abs(v - k) <= 1e-9)
        wr = ~wh & ~rt
        zero = wr & (v == 0)
        out_['factors'][reg] = {'f': f, 'right': round(float(rt.mean()), 5), 'wrong': round(float(wr.mean()), 5),
                                'withheld': round(float(wh.mean()), 5),
                                'wrong_among_int': int((wr & kint).sum()), 'wrong_among_nonint': int((wr & ~kint).sum()),
                                'withheld_among_nonint': int((wh & ~kint).sum()),
                                'wrong_to_zero': int(zero.sum()),
                                'wrong_error_q': q((v - k)[wr]),
                                'n_2021_count_rows': n21[reg]}
        expected_wrong += wr.mean() * n21[reg]
    out_['expected_wrong_in_2021_non_lome'] = round(expected_wrong, 1)
    out_['n_2021_count_rows_non_lome'] = int(sum(v for kk, v in n21.items() if kk != LOME))
    return out_


TS = {}


def togo_served():
    if not TS:
        fa = ll.Country('Togo').food_acquired()
        TS['fa'] = fa
    return TS['fa']


def _tij(df):
    f = df.reset_index()
    for c in ('t', 'i', 'j'):
        f[c] = f[c].astype(str)
    return f


def join_report(inputs_q, served, qcol='Quantity'):
    """inputs_q: frame with t,i,j and 'qty' (input grain); served: frame with t,i,j and Quantity."""
    gi = inputs_q.groupby(['t', 'i', 'j']).agg(qty=('qty', 'sum'), n_in=('qty', 'size'))
    gs = served.groupby(['t', 'i', 'j']).agg(served=(qcol, lambda s: s.sum(min_count=1)), n_served=(qcol, 'size'))
    both = gi.join(gs, how='inner')
    match = np.isclose(both['qty'].astype(float), both['served'].astype(float), rtol=1e-9, atol=1e-9, equal_nan=True)
    shape = (both['n_in'].astype(str) + 'in:' + both['n_served'].astype(str) + 'served').value_counts().to_dict()
    mm = both[~match]
    return {'groups_inputs': int(len(gi)), 'groups_served': int(len(gs)), 'groups_both': int(len(both)),
            'groups_only_inputs': int((~gi.index.isin(gs.index)).sum()),
            'groups_only_served': int((~gs.index.isin(gi.index)).sum()),
            'served_rows_in_groups_only_served': int(gs.loc[~gs.index.isin(gi.index), 'n_served'].sum()),
            'groups_sum_match': int(match.sum()), 'groups_sum_mismatch': int((~match).sum()),
            'shape': shape,
            'mismatch_shape': (mm['n_in'].astype(str) + 'in:' + mm['n_served'].astype(str) + 'served').value_counts().head(6).to_dict(),
            'mismatch_absdiff_q': q((mm['qty'] - mm['served']).abs()) if len(mm) else None,
            'one_to_one_groups': int(((both['n_in'] == 1) & (both['n_served'] == 1)).sum()),
            'one_to_one_match': int((((both['n_in'] == 1) & (both['n_served'] == 1)).to_numpy() & match).sum()),
            'served_rows_keyed': int(len(served))}


@section('togo_served_and_registry_reproduction')
def _():
    fa = togo_served()
    t = fa.index.get_level_values('t').astype(str)
    f = fa[t == '2021-22']
    d = f['Derivation'].fillna('').astype(str)
    s = f.index.get_level_values('s').astype(str)
    out_ = {'index_names': list(fa.index.names), 'rows_2021': int(len(f)),
            'rows_by_s': pd.Series(s).value_counts().to_dict(),
            'purchased_q_na': int(f.loc[s == 'purchased', 'Quantity'].isna().sum()),
            'purchased_e_na': int(f.loc[s == 'purchased', 'Expenditure'].isna().sum()),
            'purchased_e_sum': float(f.loc[s == 'purchased', 'Expenditure'].sum()),
            'index_unique': bool(f.index.is_unique)}
    c = ll.Country('Togo')
    for key in (K_DIV, K_REC, K_WH):
        on = d.str.split('+').apply(lambda ks: key in ks).to_numpy()
        out_[key] = {'served_rows': int(on.sum()), 'only_purchased': bool((s[on] == 'purchased').all()),
                     'q_na': int(f.loc[on, 'Quantity'].isna().sum()), 'e_na': int(f.loc[on, 'Expenditure'].isna().sum())}
        inp = c.derivation_inputs(key, wave='2021-22')
        out_[key]['inputs_rows'] = int(len(inp))
        out_[key]['inputs_index'] = list(inp.index.names)
        out_[key]['inputs_columns'] = list(inp.columns)
        fn = c.derivations()[key].resolve()
        args = (inp['s07bq03a'], inp['s07bq03b'], inp['s00q01'], inp['s07bq04'], inp['s07bq05'])
        if key == K_WH:
            br = pd.Series(np.asarray(fn(*args), dtype=object)).astype(str)
            out_[key]['inputs_branch'] = br.value_counts().to_dict()
            served = _tij(f[on])
            gi = _tij(inp).groupby(['t', 'i', 'j']).size()
            gs = served.groupby(['t', 'i', 'j']).size()
            out_[key]['groups_inputs'] = int(len(gi)); out_[key]['groups_served'] = int(len(gs))
            out_[key]['groups_both'] = int(gi.index.isin(gs.index).sum())
            out_[key]['served_rows_not_in_inputs'] = int(gs[~gs.index.isin(gi.index)].sum())
            # served withheld rows that also carry the divided/recovered key (collision partners)
            out_[key]['served_also_divided_or_recovered'] = int((on & (d.str.contains('divided-out', regex=False) | d.str.contains('count-recovered', regex=False)).to_numpy()).sum())
            continue
        v = np.asarray(fn(*args), dtype=float)
        q4 = num(inp['s07bq04']).fillna(0).to_numpy(); q5 = num(inp['s07bq05']).fillna(0).to_numpy()
        iq = _tij(inp)
        iq['qty'] = np.clip(v - q4 - q5, 0, None)
        out_[key]['inputs_with_zero_residual'] = int((iq['qty'] <= 0).sum())
        out_[key]['inputs_nan_value'] = int(np.isnan(v).sum())
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio = num(inp['s07bq03a']).to_numpy() / v
        out_[key]['filed_over_recovered_modes'] = modes(ratio, 4, 8)
        served = _tij(f[on & ~f['Quantity'].isna().to_numpy()])
        out_[key]['join'] = join_report(iq, served)
        out_[key]['served_withheld_too'] = int((on & d.str.contains('count-withheld', regex=False).to_numpy()).sum())
    # every s07bq03a key's inputs vs 7-day key's: the source rows that serve no purchased row
    return out_


@section('togo_2018_codes')
def _():
    out_ = {}
    s21, s21l = togo_frames()['s07'], togo_frames()['s07l']
    codes = num(s21['s07bq03b'])
    for cde in (659, 660, 662, 114):
        m = (codes == cde).to_numpy()
        out_[f'2021_{cde}'] = {'n': int(m.sum()), 'label': s21l.loc[m, 's07bq03b'].astype(str).value_counts().head(2).to_dict(),
                               'items': s21l.loc[m, 's07bq01'].astype(str).value_counts().head(8).to_dict()}
    s18 = gd(T18 / 's07b_me_tgo2018.dta', False)
    s18l = gd(T18 / 's07b_me_tgo2018.dta', True)
    out_['2018_s07bq03b_label_dtype'] = str(s18l['s07bq03b'].dtype)
    c18 = num(s18['s07bq03b'])
    for cde in (659, 660, 662, 114):
        m = (c18 == cde).to_numpy()
        out_[f'2018_{cde}'] = {'n': int(m.sum()),
                               'label_in_file': s18l.loc[m, 's07bq03b'].astype(str).value_counts().head(2).to_dict(),
                               'items': s18l.loc[m, 's07bq01'].astype(str).value_counts().head(8).to_dict(),
                               'sizes': s18l.loc[m, 's07bq03c'].astype(str).value_counts().head(6).to_dict()}
    nsu = gd(T21 / 'ehcvm_nsu_tgo2021.dta', False)
    nsul = gd(T21 / 'ehcvm_nsu_tgo2021.dta', True)
    out_['nsu_columns'] = list(nsu.columns)[:40]
    prod_cols = [c for c in nsu.columns if 'prod' in c.lower()]
    for col in nsu.columns:
        x = pd.to_numeric(nsu[col], errors='coerce')
        if x.isin([659, 660, 662]).any():
            for cde in (659, 660, 662):
                m = (x == cde).to_numpy()
                if not m.any():
                    continue
                rec = {'n': int(m.sum()), 'label': nsul.loc[m, col].astype(str).value_counts().head(2).to_dict()}
                for pc in prod_cols[:2]:
                    rec[f'by_{pc}'] = nsul.loc[m, pc].astype(str).value_counts().head(8).to_dict()
                out_[f'nsu_{col}_{cde}'] = rec
    fa = togo_served()
    t = fa.index.get_level_values('t').astype(str)
    u = fa.index.get_level_values('u').astype(str)
    j = fa.index.get_level_values('j').astype(str)
    m18 = t == '2018'
    for lab in ('Main', 'Quartier', 'Tête', '659', '660', '662', '114'):
        mm = m18 & pd.Series(u).str.match(rf'^{lab}(\s|$|\()').to_numpy()
        out_[f'served_2018_u_{lab}'] = {'rows': int(mm.sum()), 'u': pd.Series(u[mm]).value_counts().head(6).to_dict(),
                                        'items': pd.Series(j[mm]).value_counts().head(6).to_dict()}
    return out_


# ---------------------------------------------------------------- CIV
CV = {}


def civ_frames():
    if CV:
        return CV
    CV['s21'] = gd(C21 / 's07b_me_civ2021.dta', False)
    CV['s18'] = gd(C18 / 's07b_me_CIV2018.dta', False)
    c = ll.Country('CotedIvoire')
    CV['classify'] = c.derivations()[K_H].resolve()
    CV['same_fn'] = c.derivations()[K_L].resolve() is CV['classify']
    return CV


def decimals(x, tol, most=8):
    x = np.asarray(x, dtype=float)
    out_ = np.full(x.shape, most + 1)
    for d in range(most, -1, -1):
        y = x * 10 ** d
        ok = np.abs(y - np.round(y)) <= tol * np.maximum(1.0, np.abs(y))
        out_ = np.where(ok, d, out_)
    return out_


def classify_alt(x, tol):
    x = np.asarray(x, dtype=float)
    off = (x > 0) & ~on_grid(x, .25, tol)
    d = decimals(np.where(np.isnan(x), 0, x), tol)
    return np.where(off & (d >= 4), 'H', np.where(off, 'L', np.where(x > 0, 'grid', 'none')))


@section('civ_classifier')
def _():
    cv = civ_frames()
    out_ = {'L_and_H_share_one_function': cv['same_fn']}
    for wave, s in (('2021-22', cv['s21']), ('2018-19', cv['s18'])):
        q3 = num(s['s07bq03a']).to_numpy()
        k = pd.Series(np.asarray(cv['classify'](s['s07bq03a']), dtype=object)).astype(str).to_numpy()
        pos = q3 > 0
        rec = {'dtype': str(s['s07bq03a'].dtype), 'positive': int(pos.sum()),
               'head_classes': pd.Series(k[pos]).value_counts().to_dict()}
        for tol in (1e-7, 1e-6, 1e-5):
            a = classify_alt(q3, tol)
            rec[f'alt_tol_{tol}'] = pd.Series(a[pos]).value_counts().to_dict()
        a6 = classify_alt(q3, 1e-6)
        rec['alt_1e-6_agrees_with_head'] = int(((a6 == 'H') == (k == 'replaced'))[pos].sum())
        bins = pd.cut(q3[pos], [0, 1, 10, 100, 1000, np.inf], right=False).astype(str)
        rec['class_by_magnitude'] = pd.crosstab(bins, k[pos]).to_dict()
        # absolute-tolerance decimal count (float64 values): does the relative test hide 4+ decimals at large values?
        d_abs = np.full(q3.shape, 9)
        for d in range(8, -1, -1):
            ok = np.abs(q3 - np.round(q3, d)) <= 1e-7
            d_abs = np.where(ok, d, d_abs)
        offabs = pos & (np.abs(q3 * 4 - np.round(q3 * 4)) > 4e-7)
        rec['abs_tol_H_like'] = int((offabs & (d_abs >= 4)).sum())
        rec['abs_H_but_head_not_H'] = int((offabs & (d_abs >= 4) & (k != 'replaced')).sum())
        rec['abs_H_but_head_not_H_by_mag'] = pd.Series(bins[(offabs & (d_abs >= 4) & (k != 'replaced'))[pos]]).value_counts().to_dict()
        H = k == 'replaced'
        if H.any():
            dd = decimals(q3[H], 1e-6)
            rec['H_decimals'] = pd.Series(dd).value_counts().sort_index().to_dict()
            near = np.abs(q3[H] * 4 - np.round(q3[H] * 4)) <= 1e-4 * np.maximum(1, q3[H] * 4)
            rec['H_within_1e-4_of_quarter'] = int(near.sum())
        q4 = num(s['s07bq04']).fillna(0).to_numpy(); q5 = num(s['s07bq05']).fillna(0).to_numpy()
        q4H = classify_alt(q4, 1e-6); q5H = classify_alt(q5, 1e-6)
        rec['H_rows_q4_pos'] = int((H & (q4 > 0)).sum())
        rec['H_rows_q4_offgrid'] = int((H & (q4 > 0) & np.isin(q4H, ['H', 'L'])).sum())
        rec['H_rows_q4_offgrid_4dec'] = int((H & (q4 > 0) & (q4H == 'H')).sum())
        rec['H_rows_q4_ongrid'] = int((H & (q4 > 0) & (q4H == 'grid')).sum())
        rec['H_rows_q5_pos'] = int((H & (q5 > 0)).sum())
        rec['H_rows_q5_offgrid'] = int((H & (q5 > 0) & np.isin(q5H, ['H', 'L'])).sum())
        rec['H_rows_q5_ongrid'] = int((H & (q5 > 0) & (q5H == 'grid')).sum())
        L = k == 'converted'
        rec['L_rows_q4_pos'] = int((L & (q4 > 0)).sum())
        rec['L_rows_q4_offgrid'] = int((L & (q4 > 0) & np.isin(q4H, ['H', 'L'])).sum())
        G = k == 'None'
        rec['grid_q3_rows_with_q4_Hlike'] = int((pos & G & (q4H == 'H')).sum())
        rec['grid_q3_rows_with_q5_Hlike'] = int((pos & G & (q5H == 'H')).sum())
        rec['grid_q3_rows_with_q4_Llike'] = int((pos & G & (q4H == 'L')).sum())
        out_[wave] = rec
    return out_


CS = {}


def civ_served():
    if not CS:
        CS['fa'] = ll.Country('CotedIvoire').food_acquired()
    return CS['fa']


@section('civ_served_and_registry')
def _():
    cv = civ_frames()
    fa = civ_served()
    t = fa.index.get_level_values('t').astype(str)
    f = fa[t == '2021-22']
    d = f['Derivation'].fillna('').astype(str)
    s = f.index.get_level_values('s').astype(str)
    c = ll.Country('CotedIvoire')
    out_ = {'rows_2021': int(len(f)), 'rows_by_s': pd.Series(s).value_counts().to_dict(),
            'index_unique': bool(f.index.is_unique)}
    for key in (K_H, K_L):
        on = d.str.split('+').apply(lambda ks: key in ks).to_numpy()
        rec = {'served_rows': int(on.sum()), 'by_s': pd.Series(s[on]).value_counts().to_dict(),
               'q_na': int(f.loc[on, 'Quantity'].isna().sum()), 'e_na': int(f.loc[on, 'Expenditure'].isna().sum())}
        inp = c.derivation_inputs(key, wave='2021-22')
        rec['inputs_rows'] = int(len(inp))
        kk = pd.Series(np.asarray(c.derivations()[key].resolve()(inp['s07bq03a']), dtype=object)).astype(str)
        rec['inputs_classes'] = kk.value_counts().to_dict()
        gi = _tij(inp).groupby(['t', 'i', 'j']).size()
        served = _tij(f[on])
        gs = served.groupby(['t', 'i', 'j']).size()
        rec['groups_inputs'] = int(len(gi)); rec['groups_served'] = int(len(gs))
        rec['served_groups_not_in_inputs'] = int((~gs.index.isin(gi.index)).sum())
        rec['served_rows_not_in_inputs'] = int(gs[~gs.index.isin(gi.index)].sum())
        rec['input_groups_not_served'] = int((~gi.index.isin(gs.index)).sum())
        if key == K_L:
            # served values must equal the filed ones on L rows
            q3 = num(inp['s07bq03a']).fillna(0).to_numpy(); q4 = num(inp['s07bq04']).fillna(0).to_numpy()
            q5 = num(inp['s07bq05']).fillna(0).to_numpy()
            iq = _tij(inp); iq['qty'] = np.clip(q3 - q4 - q5, 0, None)
            iq = iq[iq['qty'] > 1e-9]
            sp = _tij(f[on & (s == 'purchased')])
            rec['purchased_join'] = join_report(iq, sp)
            ip = _tij(inp); ip['qty'] = q4
            ip = ip[ip['qty'] > 0]
            rec['produced_join'] = join_report(ip, _tij(f[on & (s == 'produced')]))
        out_[key] = rec
    # withheld served rows from non-H source rows (collision partners)
    held = d.str.contains('outlier-replaced-withheld', regex=False).to_numpy()
    out_['held_rows_total'] = int(held.sum())
    out_['held_rows_without_H_class_key_inputs'] = out_[K_H]['served_rows_not_in_inputs']
    # the wave test's oracle: file sums over non-H rows vs served sums (the rel=1e-4 pin)
    s21 = cv['s21']
    q3 = num(s21['s07bq03a']).astype(float)
    on4 = np.isclose(q3 * 4, (q3 * 4).round(), rtol=1e-6, atol=1e-6)
    three = np.isclose(q3 * 1000, (q3 * 1000).round(), rtol=1e-6, atol=1e-6)
    h = (q3 > 0) & ~on4 & ~three
    x = s21[~h]
    qty = (num(x['s07bq03a']).fillna(0) - num(x['s07bq04']).fillna(0) - num(x['s07bq05']).fillna(0)).clip(lower=0)
    same_u = ((x['s07bq03b'] == x['s07bq07b']) & (x['s07bq03c'] == x['s07bq07c']) & (num(x['s07bq07a']) > 0))
    value = (qty * num(x['s07bq08']) / num(x['s07bq07a'])).where(same_u)
    side = f.groupby(level='s')[['Quantity', 'Expenditure']].sum()
    oracle = {'purchased_Q': float(qty.sum()), 'purchased_E': float(value.sum()),
              'produced_Q': float(num(x['s07bq04']).fillna(0).sum()), 'inkind_Q': float(num(x['s07bq05']).fillna(0).sum())}
    servedsum = {'purchased_Q': float(side.loc['purchased', 'Quantity']), 'purchased_E': float(side.loc['purchased', 'Expenditure']),
                 'produced_Q': float(side.loc['produced', 'Quantity']), 'inkind_Q': float(side.loc['inkind', 'Quantity'])}
    out_['test_oracle_rel_diff'] = {k: (servedsum[k] - oracle[k]) / oracle[k] for k in oracle}
    out_['test_oracle_H_count'] = int(h.sum())
    return out_


@section('civ_strata')
def _():
    c = ll.Country('CotedIvoire')
    rec = c.derivations()[K_STRATA]
    out_ = {'waves': rec.waves, 'function': rec.function, 'inputs': rec.inputs, 'bases': list(rec.bases)}
    for label, kw in (('wave_none', {}), ('wave_2021', {'wave': '2021-22'})):
        inp = c.derivation_inputs(K_STRATA, **kw)
        fn = rec.resolve()
        rebuilt = pd.Series([fn(r, m) for r, m in zip(inp['s00q01'], inp['s00q04'])], index=inp.index)
        out_[label] = {'inputs_rows': int(len(inp)), 't': pd.Series(inp.index.get_level_values('t').astype(str)).value_counts().to_dict()}
        if label == 'wave_2021':
            smp = c.sample()
            st = smp.index.get_level_values('t').astype(str)
            served = smp[st == '2021-22']['strata']
            served.index = served.index.get_level_values('i').astype(str)
            rb = rebuilt.copy(); rb.index = rb.index.get_level_values('i').astype(str)
            common = served.index.intersection(rb.index)
            eq = (served.loc[common].astype('string') == rb.loc[common].astype('string')).fillna(False)
            out_[label].update({'served_households': int(len(served)), 'common': int(len(common)),
                                'match': int(eq.sum()), 'mismatch': int((~eq).sum()),
                                'served_not_in_inputs': int((~served.index.isin(rb.index)).sum()),
                                'served_na': int(served.isna().sum()),
                                'derivation_key_on_served': smp[st == '2021-22']['Derivation'].astype(str).value_counts().to_dict()})
    return out_


dump()
print('RAWCHECKS DONE', flush=True)
