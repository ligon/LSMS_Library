#!/usr/bin/env python
"""GhanaLSS: five sources of food prices, compared (slurm_logs/price_sources/PROTOCOL.org).

Sources present in GhanaLSS (verified, not assumed -- see REPORT.org section 1):

  1 unit_value                  food_prices(units='unitvalue'), s='purchased',
                                currency-denominated ``u`` dropped.  Exists in
                                **2016-17 only**: GLSS1-GLSS6 elicit purchases by
                                VALUE (u='Value', Quantity == Expenditure), so the
                                ratio is 1 by construction.
  2 reported_purchase_price     ABSENT.  food_acquired.Price is 0.0% populated on
                                s='purchased' rows in all seven waves.
  3 own_consumption_valuation   food_prices(units='unitprice'), s='produced' --
                                GLSS Section 8H "For how much would you sell one
                                unit of ...... now?", the unit being the one THE
                                RESPONDENT CHOOSES.  A farmgate (seller-side)
                                valuation.  1991-92 .. 2016-17 (0% in GLSS1/2).
  5 community_price             community_prices(), per-unit price =
                                Price / NumberOfUnits, at the survey's own
                                (t, v, j, u, obs) grain -- up to three vendor
                                observations per item.  Six waves; 2005-06 fielded
                                the price questionnaire but distributes no price
                                file (`asked-not-distributed`).
  4 crop_sale_price             not declared for GhanaLSS (no crop_production).

PROTOCOL EXTENSION.  The protocol's pair list has no
``community_price_vs_own_consumption_valuation``.  GhanaLSS's source 1 exists in
one wave only, so 5-vs-3 is the only pair that tests the marketing margin here,
in four waves.  Orientation follows the protocol's rule (``a`` is the downstream
/ market-side price): a = community_price, b = own_consumption_valuation.

Usage (from anywhere; the shared warm cache is read read-only):

  export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:$WT
  export LSMS_BUILD_WORKERS=1
  taskset -c 3-5 .venv/bin/python analysis.py [OUTDIR]
"""
from __future__ import annotations

import math
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

COMMON = Path('/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/'
              'slurm_logs/price_sources')
if str(COMMON) not in sys.path:
    sys.path.insert(0, str(COMMON))
import common                                                    # noqa: E402

COUNTRY = 'GhanaLSS'
GEO_COLS = ('District', 'Region')      # GhanaLSS cluster_features has Region only
THR_HH = 10                            # household sample statistic (protocol)
THR_HH_LOW = 5                         # sensitivity: a thin household source
THR_MEAS = 1                           # community price is a measurement
PAIRS = [
    # (a = downstream / market side, b)
    ('community_price', 'own_consumption_valuation'),   # protocol EXTENSION
    ('unit_value', 'own_consumption_valuation'),
    ('unit_value', 'community_price'),
]
_MISSING_GEO = {'nan', 'NaN', '<NA>', 'None', ''}


def _s(x):
    """Series of plain ``str``, missing rendered ``'<NA>'``, INDEX PRESERVED.

    Index preservation is load-bearing: these frames are assigned back into a
    filtered DataFrame (``df['u'] = _s(df['u'])``), and a helper that returned a
    fresh ``RangeIndex`` would align on the caller's gappy index and silently
    fill the non-overlapping rows with NaN.
    """
    s = pd.Series(x).astype(object)
    return s.where(s.notna(), '<NA>').map(str)


def _quiet(fn, *a, **kw):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return fn(*a, **kw)


# ---------------------------------------------------------------------------
# source frames
# ---------------------------------------------------------------------------

#: Units for which ``NumberOfUnits`` is unambiguously a quantity OF ``u``.
_METRIC_U = frozenset({'Kg', 'Gram', 'Milligram', 'Tonne',
                       'Liter', 'Milliliter', 'Pound', 'Ounce', 'Gallon'})
#: The wave whose price form leaves QTY free per observation (see below).
_FREE_QTY_WAVE = '2016-17'


def source_community_price(c, log):
    """Source 5.  Per-unit price = Price / NumberOfUnits (the reported basis).

    ``NumberOfUnits`` is the quantity, in ``u``, the reported ``Price`` refers
    to.  In GLSS1-GLSS6 it is either a weighed quantity in a metric ``u``
    (``QUANn``, ``s1stkg``) or the FORM's stated per-item basis (``Basis`` in
    ``harmonize_price_item``: 10 tablets, 6 yards, the 0.17 kg milk tin) --
    both reliable, and verified consistent across waves on maize at ``u='Kg'``
    (REPORT.org section 1).

    GLSS7 is different and it is a DATA PROBLEM, not a convention.  Its form
    is ``PRICE / QTY / UOM`` per observation, with QTY entered by the
    enumerator, and for container units the enumerator often entered the
    container's CONTENT WEIGHT IN GRAMS instead of a count.  The decisive
    case: Millet, ``u='Bowl'``, ``Price = 4.00``, ``NumberOfUnits = 4000``
    (clusters 70735/70789/70791), against a household unit value for
    Millet/Bowl in Northern of EXACTLY 4.00 GHS.  ``Price`` is the price of
    one bowl; the 4000 is the bowl's weight.  ``Price / NumberOfUnits`` there
    is 0.001 GHS and produced log gaps of +8.3 (a ratio of 4,050).

    So for 2016-17 ONLY, an observation is kept when ``u`` is a metric mass /
    volume unit (QTY is then unambiguously a quantity of ``u``) or when
    ``NumberOfUnits == 1`` (the two readings coincide).  The rule needs no
    threshold, and the excluded rows are counted, not hidden.  Recovering them
    by reading ``Price`` as the per-unit price is possible but requires a
    row-by-row judgement (QTY = 2 on a Bowl is ambiguous); that is a data-fix,
    reported rather than improvised here.
    """
    df = _quiet(c.community_prices, currency='column').reset_index()
    n0 = len(df)
    df['u'], df['j'], df['t'], df['v'] = _s(df['u']), _s(df['j']), _s(df['t']), _s(df['v'])
    bad = df['NumberOfUnits'].isna() | (df['NumberOfUnits'] <= 0)
    log(f'  source 5: {n0} rows; dropped {int(bad.sum())} with no usable '
        f'NumberOfUnits ({df.loc[bad].groupby("t").size().to_dict()})')
    df = df[~bad].copy()
    free = (df['t'] == _FREE_QTY_WAVE)
    unusable = free & ~(df['u'].isin(_METRIC_U) | (df['NumberOfUnits'] == 1))
    by_u = (df.loc[unusable].groupby('u').size().sort_values(ascending=False)
              .head(8).to_dict())
    log(f'  source 5: {_FREE_QTY_WAVE} free-QTY screen dropped '
        f'{int(unusable.sum())} of {int(free.sum())} rows whose QTY is a '
        f'content weight, not a count of u; top u: {by_u}')
    df = df[~unusable].copy()
    df['price'] = df['Price'] / df['NumberOfUnits']
    return df[['t', 'v', 'j', 'u', 'price', 'currency']]


def source_food_prices(c, units, s_value, log, drop_currency_units=False):
    """Sources 1 and 3, from ``food_prices(units=...)`` restricted to one ``s``."""
    df = _quiet(c.food_prices, units=units, currency='column').reset_index()
    n0 = len(df)
    df = df[df['s'] == s_value].copy()
    df['u'], df['j'], df['t'], df['v'] = _s(df['u']), _s(df['j']), _s(df['t']), _s(df['v'])
    n1 = len(df)
    if drop_currency_units:
        from lsms_library.transformations import _is_currency_denominated
        cur = _is_currency_denominated(df['u'])
        by_t = df.loc[cur].groupby('t').size().to_dict()
        log(f'  units={units!r} s={s_value!r}: {n0} rows -> {n1} on s; dropped '
            f'{int(cur.sum())} currency-denominated-u rows {by_t}')
        df = df[~cur].copy()
    else:
        log(f'  units={units!r} s={s_value!r}: {n0} rows -> {n1} on s')
    df = df.rename(columns={'Price': 'price'})
    return df[['t', 'v', 'j', 'u', 'price', 'currency']]


# ---------------------------------------------------------------------------
# cells
# ---------------------------------------------------------------------------

def cells_for(frame, source, c, log):
    """``common.cell_medians`` at every ladder rung, with geography attached."""
    if not len(frame):
        return pd.DataFrame(columns=common.CELL_COLS)
    geo, n_missing = common.attach_geo(frame, c, geo_cols=GEO_COLS)
    have = [g for g in GEO_COLS if g in geo.columns]
    miss = geo[geo[have[0]].isna()].groupby('t').size().to_dict() if have else {}
    log(f'  {source}: {len(frame)} priced rows; geography attached '
        f'({have or "none"}); {n_missing} rows whose (t, v) is not in '
        f'cluster_features {miss}')
    for g in have:
        geo[g] = _s(geo[g])
    levels = common.geo_ladder(geo)                      # ['v', ...] finest first
    cells = common.cell_medians(geo, levels, keys=('t', 'j', 'u'), price='price')
    n_before = len(cells)
    bad = (cells['geo_level'] != 'national') & cells['geo'].isin(_MISSING_GEO)
    if bad.any():
        log(f'  {source}: dropped {int(bad.sum())} cells on a missing geography key')
    cells = cells[~bad].copy()
    cur = (geo.groupby('t')['currency']
              .agg(lambda s: sorted(set(_s(s)))[0] if len(set(_s(s))) == 1 else '|'.join(sorted(set(_s(s))))))
    cells['country'] = COUNTRY
    cells['source'] = source
    cells['currency'] = cells['t'].map(cur)
    log(f'  {source}: {n_before} cells at levels {levels + ["national"]}; '
        f'{len(cells)} kept')
    return cells[common.CELL_COLS]


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main(outdir):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []

    def log(msg=''):
        print(msg, flush=True)
        lines.append(str(msg))

    import lsms_library as ll
    log(f'lsms_library: {ll.__file__}')
    c = ll.Country(COUNTRY)
    log(f'waves: {c.waves}')

    # ---- source 2: the independence test is vacuous here -------------------
    log('\n== source 2 (reported_purchase_price): presence test ==')
    fa = _quiet(c.food_acquired)
    r = fa.reset_index()[['t', 's', 'Price']]
    del fa
    pres = r.groupby(['t', 's'])['Price'].agg(n='size', share_populated=lambda s: float(s.notna().mean()))
    log(pres.to_string())
    purch = pres.xs('purchased', level='s')
    log(f'  Price populated on purchased rows: max share over waves = '
        f'{float(purch["share_populated"].max()):.6f}  -> source 2 ABSENT '
        f'(no independence test possible; nothing to drop)')
    del r, pres, purch

    # ---- build the three source frames, one at a time ----------------------
    log('\n== source frames ==')
    frames = {}
    frames['community_price'] = source_community_price(c, log)
    frames['own_consumption_valuation'] = source_food_prices(
        c, 'unitprice', 'produced', log)
    frames['unit_value'] = source_food_prices(
        c, 'unitvalue', 'purchased', log, drop_currency_units=True)

    log('\n== cells ==')
    cells = {}
    for name, fr in frames.items():
        cells[name] = cells_for(fr, name, c, log)
        frames[name] = None                       # release the priced rows
    del frames

    all_cells = pd.concat([d for d in cells.values() if len(d)], ignore_index=True)

    # ---- gaps and models ---------------------------------------------------
    log('\n== pairs ==')
    gap_rows, model_rows = [], []
    for a, b in PAIRS:
        pair = f'{a}_vs_{b}'
        ca, cb = cells[a], cells[b]
        if not len(ca) or not len(cb):
            log(f'  {pair}: one side empty -- skipped')
            continue
        thr_a = THR_MEAS if a == 'community_price' else THR_HH
        thr_b = THR_MEAS if b == 'community_price' else THR_HH
        g = common.match_gaps(ca, cb, threshold_a=thr_a, threshold_b=thr_b,
                              finest_only=True)
        log(f'  {pair}: thresholds a={thr_a} b={thr_b} -> {len(g)} matched cells')
        if len(g):
            g = g.assign(country=COUNTRY, pair=pair)
            gap_rows.append(g[common.GAP_COLS])
            for t in sorted(g['t'].unique()):
                gt = g[g['t'] == t]
                model_rows += common.model_rows(
                    gt, country=COUNTRY, t=t, pair=pair, u_basis='native',
                    threshold_a=thr_a, threshold_b=thr_b)
                log(f'    {t}: {len(gt)} cells, levels '
                    f'{sorted(set(gt["geo_level"]))}, median gap '
                    f'{gt["gap_log"].median():.4f} (ratio '
                    f'{math.exp(gt["gap_log"].median()):.3f})')
        # sensitivity: a thinner household threshold (protocol allows 5)
        thr_a5 = thr_a if a == 'community_price' else THR_HH_LOW
        thr_b5 = thr_b if b == 'community_price' else THR_HH_LOW
        if (thr_a5, thr_b5) != (thr_a, thr_b):
            g5 = common.match_gaps(ca, cb, threshold_a=thr_a5, threshold_b=thr_b5,
                                   finest_only=True)
            log(f'  {pair}: sensitivity thresholds a={thr_a5} b={thr_b5} -> '
                f'{len(g5)} matched cells')
            for t in sorted(g5['t'].unique()) if len(g5) else []:
                model_rows += common.model_rows(
                    g5[g5['t'] == t], country=COUNTRY, t=t, pair=pair,
                    u_basis='native', threshold_a=thr_a5, threshold_b=thr_b5)

    gaps = (pd.concat(gap_rows, ignore_index=True) if gap_rows
            else pd.DataFrame(columns=common.GAP_COLS))
    models = (pd.DataFrame(model_rows) if model_rows
              else pd.DataFrame(columns=common.MODEL_COLS))

    paths = common.write_outputs(outdir, cells=all_cells, gaps=gaps, models=models)
    log('\nwrote: ' + ', '.join(str(p) for p in paths))

    # ---- the by-item table protocol section 5 asks for ---------------------
    if len(gaps):
        log('\n== gap by item (pair x wave x j) ==')
        by_item = (gaps.groupby(['pair', 't', 'j'])
                        .agg(n_cells=('gap_log', 'size'),
                             median_gap=('gap_log', 'median'))
                        .assign(ratio=lambda d: np.exp(d['median_gap']))
                        .reset_index())
        by_item['in_30_50'] = ((by_item['median_gap'] >= common.CLAIM_LO)
                               & (by_item['median_gap'] <= common.CLAIM_HI))
        by_item.to_csv(outdir / 'gaps_by_item.csv', index=False)
        log(by_item.to_string(index=False))

    (outdir / 'analysis.log').write_text('\n'.join(lines) + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else
                  Path(__file__).resolve().parent))
