"""GH #850 -- measure the defects in the food-side kg inference, and prototype
the corrected inference IN MEMORY.

Read-only.  Reads warm ``food_acquired`` through the public Country API with
``LSMS_DATA_DIR`` pointed at a SCRATCH root, so no rebuild can touch the shared
cache.  Writes nothing but its own CSVs, in this directory.

Three defects are measured, and the order matters:

(c) VOCABULARY.  ``KNOWN_METRIC`` misses ``Millilitre`` / ``Grams`` /
    ``Litres``, and ``_parse_explicit_metric``'s regexes miss the ``lts`` /
    ``ltrs`` / ``lt`` / ``kgs`` spellings, so labels that state their own
    kilograms fall through to the price-ratio inference.  Fixing (c) SHRINKS
    the inference set and ENLARGES the kg baseline, so (a) and (b) are
    measured on top of it, not before it.
(b) DENOMINATOR.  Step 2 medians ``Expenditure``, not
    ``Expenditure / Quantity``, so the inferred factor is kilograms per
    transaction ROW.
(a) ITEM AXIS.  No step carries ``j``; the result is keyed on ``u`` alone.

Usage
-----
    LSMS_DATA_DIR=<scratch> PYTHONPATH=<worktree> python measure.py [Country ...]
"""
from __future__ import annotations

import os
import re
import sys
import warnings

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library.transformations import (
    KNOWN_METRIC, _get_kg_factors, _is_currency_denominated,
    _parse_explicit_metric, conversion_to_kgs,
)

HERE = os.path.dirname(os.path.abspath(__file__))
COUNTRIES = ['Uganda', 'Malawi', 'Nigeria', 'Ethiopia', 'Tanzania', 'Niger',
             'Mali', 'EthiopiaRHS', 'GhanaLSS']
FLOORS = (3, 5, 10, 20)
DEFAULT_FLOOR = 5            # step-2 support, the _survey_median_factors twin
DEFAULT_BASELINE = 5         # (t,j) baseline support -- the OTHER floor

# The EIGHT-key map ``conversion_to_kgs`` uses for its OWN kg baseline.  It is
# NOT ``KNOWN_METRIC`` -- no litre, ml or cl -- so a litre row can enter the
# baseline only through a survey-supplied ``Quantity_kg``.
LOCAL_UNIT_CONVERSION = {
    'kg': 1, 'kilogram': 1, 'gram': 1 / 1000, 'g': 1 / 1000,
    'pound': 0.453592, 'lbs': 0.453592, 'kilogramme': 1, 'gramm': 1 / 1000,
}

# --- defect (c): the vocabulary the corpus actually uses -------------------
MISSING_BARE = {'millilitre': .001, 'milliliter': .001, 'mili liter': .001,
                'millilitres': .001, 'milligram': 1e-6, 'grams': .001,
                'gramme': .001, 'grammes': .001, 'litres': 1., 'liters': 1.,
                'kilo': 1., 'kilos': 1., 'kgs': 1., 'gm': .001, 'gms': .001}
MISSING_CONTENT = re.compile(
    r'(\d+(?:\.\d+)?)\s*(lts|ltrs|ltr|lt|kgs|gms|grs)\b', re.IGNORECASE)
CONTENT_SCALE = {'lts': 1., 'ltrs': 1., 'ltr': 1., 'lt': 1., 'kgs': 1.,
                 'gms': .001, 'grs': .001}


# ---------------------------------------------------------------------------
# frame prep
# ---------------------------------------------------------------------------

def load(country):
    """The frame the derived transform is handed.

    ``Country._FOOD_DERIVED`` dispatch hands ``transform_fn`` the output of
    ``_aggregate_wave_data(waves, 'food_acquired')`` (``country.py:4470``).
    Checked per country: that frame and ``Country.food_acquired()`` have the
    same length and the same ``u`` vocabulary, so the public reader is a
    faithful stand-in and is used here because it is what a user sees.
    """
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return ll.Country(country).food_acquired()


def prep(df):
    """Flat working frame, mirroring ``conversion_to_kgs``'s own preparation."""
    v = df.copy().replace(0, np.nan)
    flat = v.reset_index()
    flat['u'] = pd.Series([str(x) for x in flat['u']], index=flat.index)
    flat = flat[~_is_currency_denominated(flat['u'])].copy()
    flat['ul'] = flat['u'].str.lower()
    flat['Kgs'] = flat['Quantity'] * flat['ul'].map(LOCAL_UNIT_CONVERSION).astype(float)
    if 'Quantity_kg' in flat.columns:
        flat['Kgs'] = flat['Kgs'].where(flat['Kgs'].notna(), flat['Quantity_kg'])
    else:
        flat['Quantity_kg'] = np.nan
    return flat


def seeded_factors(df, *, wide=False, volume_as_mass=True):
    """``KNOWN_METRIC`` + the explicit-metric label parse.

    With *wide*, additionally seed the metric spellings the library misses --
    the defect-(c) repair, applied here so (a) and (b) are measured on the
    rows that would still need an inference afterwards.
    """
    factors = dict(KNOWN_METRIC)
    if wide:
        factors.update(MISSING_BARE)
    labels = (df.index.get_level_values('u').dropna().unique()
              if 'u' in (df.index.names or []) else df['u'].dropna().unique())
    for u in labels:
        key = str(u).lower()
        if key in factors:
            continue
        kg = _parse_explicit_metric(str(u), volume_as_mass=volume_as_mass)
        if kg is None and wide:
            m = MISSING_CONTENT.search(str(u))
            if m:
                kg = float(m.group(1)) * CONTENT_SCALE[m.group(2).lower()]
        if kg is not None and np.isfinite(kg) and kg > 0:
            factors[key] = kg
    return factors


# ---------------------------------------------------------------------------
# the three inferences
# ---------------------------------------------------------------------------

def infer_old(flat, index=('t', 'i')):
    """Verbatim re-implementation of the live chain, kept as a check on the
    prototype's plumbing (``reimpl_agree_with_live``)."""
    index = list(index)
    pkg = (flat['Expenditure'] / flat['Kgs']).groupby(
        [flat[k] for k in index]).median()
    infer = flat[flat['Quantity_kg'].isna()]
    po = infer.groupby(index + ['u'])['Expenditure'].median()
    return (po / pkg).dropna().groupby('u').median().to_dict()


def infer_b(flat, index=('t', 'i')):
    """Fix (b) only: step 2 medians ``Expenditure / Quantity``."""
    index = list(index)
    pkg = (flat['Expenditure'] / flat['Kgs']).groupby(
        [flat[k] for k in index]).median()
    infer = flat[flat['Quantity_kg'].isna()].copy()
    infer['up'] = infer['Expenditure'] / infer['Quantity']
    po = infer.groupby(index + ['u'])['up'].median()
    return (po / pkg).dropna().groupby('u').median().to_dict()


def infer_c(flat, floor=DEFAULT_FLOOR, baseline_floor=DEFAULT_BASELINE,
            fallback=True):
    """Fixes (a) + (b): baseline at ``(t, j)`` over rows whose kilograms are
    already known, step 2 at ``(t, j, u)`` on ``Expenditure / Quantity``,
    factor keyed ``(j, u)``.

    TWO floors, and they gate different things.  *floor* is the number of
    inference rows behind the ``(t, j, u)`` estimate -- the twin of
    ``SURVEY_MEDIAN_MIN_REPORTS``.  *baseline_floor* is the number of
    kg-known rows behind the ``(t, j)`` price-per-kg reference; without it a
    single household's kg purchase sets the price of an item nationally,
    which is what the largest movers turn out to be.

    Returns ``(per_ju, per_u, support, baseline_n)``.
    """
    ratio = flat['Expenditure'] / flat['Kgs']
    gb = ratio.groupby([flat['t'], flat['j']])
    pkg, nb = gb.median(), gb.count()
    pkg.index.names = nb.index.names = ['t', 'j']
    pkg = pkg.where(nb >= baseline_floor)

    infer = flat[flat['Quantity_kg'].isna()].copy()
    infer['up'] = infer['Expenditure'] / infer['Quantity']
    g = infer.groupby(['t', 'j', 'u'])['up']
    est = (g.median() / pkg).replace([np.inf, -np.inf], np.nan).dropna()
    n = g.count().reindex(est.index)
    sup = n.groupby(['j', 'u']).sum()
    per_ju = est.groupby(['j', 'u']).median()
    per_ju = per_ju[(sup >= floor) & np.isfinite(per_ju) & (per_ju > 0)]
    return per_ju.to_dict(), (infer_b(flat) if fallback else {}), sup, nb


def row_factor(flat, seeded, per_ju=None, per_u=None):
    """Ladder: seeded label -> ``(j, u)`` inference -> ``(u)`` inference."""
    out = flat['ul'].map(seeded).astype(float)
    if per_ju:
        key = pd.Series(list(zip(flat['j'].astype(str), flat['u'])),
                        index=flat.index)
        out = out.where(out.notna(), key.map(per_ju).astype(float))
    if per_u:
        out = out.where(out.notna(), flat['u'].map(per_u).astype(float))
    return out


# ---------------------------------------------------------------------------
# derived tables, recomputed in memory
# ---------------------------------------------------------------------------

def _kg(flat, factor):
    qkg = flat['Quantity'] * factor
    return flat['Quantity_kg'].where(flat['Quantity_kg'].notna(), qkg)


def quantities_kgs(flat, factor):
    qkg = _kg(flat, factor)
    conv = qkg.notna()
    out = pd.DataFrame({'t': flat['t'], 'i': flat['i'], 'j': flat['j'],
                        'u': np.where(conv, 'kg', flat['u'].to_numpy()),
                        's': flat.get('s', 'NA'),
                        'Quantity': np.where(conv, qkg, flat['Quantity'])})
    out = out.replace(0, np.nan).dropna(subset=['Quantity'])
    return out.groupby(['t', 'i', 'j', 'u', 's'])['Quantity'].sum()


def prices_kgvalue(flat, factor):
    qkg = _kg(flat, factor)
    with np.errstate(divide='ignore', invalid='ignore'):
        p = flat['Expenditure'] / qkg
    out = pd.DataFrame({'t': flat['t'], 'i': flat['i'], 'j': flat['j'],
                        'u': np.where(qkg.notna(), 'kg', flat['u'].to_numpy()),
                        's': flat.get('s', 'NA'), 'Price': p})
    out = out.replace([np.inf, -np.inf], np.nan).replace(0, np.nan)
    out = out.dropna(subset=['Price'])
    return out.groupby(['t', 'i', 'j', 'u', 's'])['Price'].median()


# ---------------------------------------------------------------------------
# per-country measurement
# ---------------------------------------------------------------------------

def measure(country):
    df = load(country)
    flat = prep(df)
    seed_now = seeded_factors(df)
    seed_wide = seeded_factors(df, wide=True)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        factors_live = _get_kg_factors(df)
        idx = [n for n in ('t', 'm', 'i') if n in df.index.names]
        live_raw = conversion_to_kgs(df, index=idx)

    is_seeded_now = flat['ul'].isin(seed_now)
    is_seeded_wide = flat['ul'].isin(seed_wide)
    needs_factor = flat['Quantity_kg'].isna()          # no survey kg on the row
    step2 = needs_factor & flat['Expenditure'].notna()
    inf_now = step2 & ~is_seeded_now & flat['Quantity'].notna()
    inf_wide = step2 & ~is_seeded_wide & flat['Quantity'].notna()

    # (i) defect (b): the per-row Quantity distribution on the step-2 rows
    q = flat.loc[inf_wide, 'Quantity']
    row_i = {
        'country': country,
        'rows_total': len(flat),
        'rows_step2': int(step2.sum()),
        'rows_inference_now': int(inf_now.sum()),
        'rows_inference_after_c': int(inf_wide.sum()),
        'rows_c_removes': int((inf_now & ~inf_wide).sum()),
        'share_inference_now': float(inf_now.mean()),
        'share_Q_eq_1': float((q == 1).mean()) if len(q) else np.nan,
        'Q_median': float(q.median()) if len(q) else np.nan,
        'Q_p90': float(q.quantile(0.90)) if len(q) else np.nan,
        'Q_mean': float(q.mean()) if len(q) else np.nan,
    }

    # (ii) defect (a): how many j share each u, and the spread of kg per unit
    per_ju, per_u, sup, nb = infer_c(flat)
    ju = pd.Series(per_ju, dtype=float)
    if len(ju):
        ju.index = pd.MultiIndex.from_tuples(ju.index, names=['j', 'u'])
    rows_ii = []
    counts = flat.loc[inf_wide].groupby('u')['j'].nunique()
    volume = flat.loc[inf_wide].groupby('u').size()
    for u, nj in counts.sort_values(ascending=False).items():
        try:
            vals = ju.xs(u, level='u')
        except (KeyError, IndexError):
            vals = pd.Series(dtype=float)
        rows_ii.append({
            'country': country, 'u': u, 'n_rows': int(volume.get(u, 0)),
            'n_distinct_j': int(nj), 'n_j_with_factor': int(len(vals)),
            'kg_per_unit_pooled_old': float(factors_live.get(str(u).lower(), np.nan)),
            'kg_per_unit_pooled_new': float(per_u.get(u, np.nan)),
            'kg_per_j_min': float(vals.min()) if len(vals) else np.nan,
            'kg_per_j_p25': float(vals.quantile(.25)) if len(vals) else np.nan,
            'kg_per_j_median': float(vals.median()) if len(vals) else np.nan,
            'kg_per_j_p75': float(vals.quantile(.75)) if len(vals) else np.nan,
            'kg_per_j_max': float(vals.max()) if len(vals) else np.nan,
            'ratio_max_min': float(vals.max() / vals.min())
                             if len(vals) > 1 and vals.min() > 0 else np.nan,
        })

    # -- floor sweep, BOTH floors -----------------------------------------
    rows_floor = []
    key = pd.Series(list(zip(flat['j'].astype(str), flat['u'])), index=flat.index)
    for f in FLOORS:
        for bf in FLOORS:
            pj, _pu, _s, _nb = infer_c(flat, floor=f, baseline_floor=bf,
                                       fallback=False)
            served = key.map(pj).notna() & inf_wide
            rows_floor.append({
                'country': country, 'step2_floor': f, 'baseline_floor': bf,
                'cells_ju': len(pj), 'rows_served_by_ju': int(served.sum()),
                'share_of_inference_rows': served.sum() / max(inf_wide.sum(), 1)})

    mine_old, mine_b = infer_old(flat), infer_b(flat)

    # -- (iii) movement, on the rows whose kilograms the factor DECIDES ----
    # Rows carrying a survey ``Quantity_kg`` are excluded: their factor moves
    # but their delivered kilograms do not (``_apply_kg_conversion`` defers).
    affected = (~is_seeded_wide) & needs_factor
    f_old = row_factor(flat, factors_live)
    f_new = row_factor(flat, seed_wide, per_ju, per_u)
    both = f_old.notna() & f_new.notna() & affected
    rel = (f_new / f_old).where(both)
    row_iii = {
        'country': country,
        'n_units_total': int(flat['ul'].nunique()),
        'n_units_inferred_now': int(flat.loc[~is_seeded_now, 'ul'].nunique()),
        'n_units_inferred_after_c': int(flat.loc[~is_seeded_wide, 'ul'].nunique()),
        'rows_affected': int(affected.sum()),
        'share_rows_affected': float(affected.mean()),
        'rows_compared': int(both.sum()),
        'rows_gained': int((f_new.notna() & f_old.isna() & affected).sum()),
        'rows_lost': int((f_old.notna() & f_new.isna() & affected).sum()),
        'share_move_gt_10pct': float(((rel - 1).abs() > .10).sum() / max(both.sum(), 1)),
        'share_move_gt_2x': float(((rel > 2) | (rel < .5)).sum() / max(both.sum(), 1)),
        'share_move_gt_10x': float(((rel > 10) | (rel < .1)).sum() / max(both.sum(), 1)),
        'median_rel_move': float(rel.median()) if both.any() else np.nan,
        'reimpl_keys': len(live_raw),
        'reimpl_agree_with_live': sum(
            1 for k, v in mine_old.items()
            if k in live_raw and np.isclose(live_raw[k], v, rtol=1e-9)),
    }
    # the kg self-test: the one unit whose factor is 1 by definition
    row_iii['kg_selftest_old'] = float(
        {k.lower(): v for k, v in mine_old.items()}.get('kg', np.nan))
    row_iii['kg_selftest_b'] = float(
        {k.lower(): v for k, v in mine_b.items()}.get('kg', np.nan))
    kgvals = pd.Series({k[0]: v for k, v in per_ju.items()
                        if str(k[1]).lower() == 'kg'}, dtype=float)
    row_iii['kg_selftest_c_median'] = float(kgvals.median()) if len(kgvals) else np.nan
    row_iii['kg_selftest_c_n_items'] = int(len(kgvals))
    row_iii['kg_selftest_c_within_10pct'] = (
        float(((kgvals - 1).abs() <= .10).mean()) if len(kgvals) else np.nan)

    qo, qn = quantities_kgs(flat, f_old), quantities_kgs(flat, f_new)
    po_, pn = prices_kgvalue(flat, f_old), prices_kgvalue(flat, f_new)
    uo, un = qo.index.get_level_values('u'), qn.index.get_level_values('u')
    row_iii.update({
        'fq_rows_old': len(qo), 'fq_rows_new': len(qn),
        'fq_kg_rows_old': int((uo == 'kg').sum()),
        'fq_kg_rows_new': int((un == 'kg').sum()),
        'fq_kg_total_old': float(qo[uo == 'kg'].sum()),
        'fq_kg_total_new': float(qn[un == 'kg'].sum()),
        'fp_rows_old': len(po_), 'fp_rows_new': len(pn),
    })

    # top-10 items by expenditure, in the LATEST wave only (a country-wide
    # median pools redenominated currencies; GhanaLSS spans the 2007 cedi redenomination).
    last = sorted(flat['t'].astype(str).unique())[-1]
    fl = flat[flat['t'].astype(str) == last]
    exp = fl.groupby('j')['Expenditure'].sum().sort_values(ascending=False)
    def _med(p):
        s = p.xs(last, level='t', drop_level=False) if last in p.index.get_level_values('t').astype(str) else p
        s = s[s.index.get_level_values('u') == 'kg']
        return s.groupby('j').median()
    mo, mn = _med(po_), _med(pn)
    rows_p = []
    for j in list(exp.head(10).index):
        a, b = mo.get(j, np.nan), mn.get(j, np.nan)
        rows_p.append({'country': country, 't': last, 'j': j,
                       'expenditure_share': float(exp[j] / exp.sum()),
                       'baseline_n': int(nb.get((last, j), 0)) if len(nb) else 0,
                       'median_price_kg_old': float(a) if pd.notna(a) else np.nan,
                       'median_price_kg_new': float(b) if pd.notna(b) else np.nan,
                       'pct_change': float(b / a - 1) * 100
                                     if pd.notna(a) and pd.notna(b) and a else np.nan})
    return row_i, rows_ii, row_iii, rows_p, rows_floor


def main():
    countries = sys.argv[1:] or COUNTRIES
    I, II, III, P, F = [], [], [], [], []
    for c in countries:
        print(f'--- {c}', flush=True)
        try:
            a, b, d, e, f = measure(c)
        except Exception as exc:                              # noqa: BLE001
            print(f'    FAILED: {type(exc).__name__}: {exc}', flush=True)
            continue
        I.append(a); II += b; III.append(d); P += e; F += f
        print(f'    inference now {a["rows_inference_now"]:,} -> after (c) '
              f'{a["rows_inference_after_c"]:,}; Q==1 {a["share_Q_eq_1"]:.1%}; '
              f'kg self-test old={d["kg_selftest_old"]:.3f}; '
              f'>2x {d["share_move_gt_2x"]:.1%} of {d["rows_compared"]:,}',
              flush=True)
        for name, rows in [('measure_i_quantity', I), ('measure_ii_units', II),
                           ('measure_iii_rows', III), ('measure_iii_prices', P),
                           ('measure_floor', F)]:
            pd.DataFrame(rows).to_csv(os.path.join(HERE, name + '.csv'), index=False)
    print('wrote csvs to', HERE)


if __name__ == '__main__':
    main()
