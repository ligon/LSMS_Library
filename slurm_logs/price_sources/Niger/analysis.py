"""Niger -- five sources of food prices compared (slurm_logs/price_sources/PROTOCOL.org).

Run (cores 21-23, worktree code, private data dir so the shared L2 cache is
never rebuilt under this worktree's hashes):

    export PYTHONPATH=/global/scratch/fsa/fc_jevons/ligon/tmp/pyextra:/global/scratch/fsa/fc_jevons/ligon/tmp/wt-prices-niger
    export LSMS_BUILD_WORKERS=1
    export LSMS_COUNTRIES_ROOT=/global/scratch/fsa/fc_jevons/ligon/tmp/wt-prices-niger/lsms_library/countries
    export LSMS_DATA_DIR=/global/scratch/fsa/fc_jevons/ligon/tmp/lsms_data_prices_niger
    taskset -c 21-23 .venv/bin/python slurm_logs/price_sources/Niger/analysis.py

WHAT NIGER CAN AND CANNOT DO (see REPORT.org section 1)
------------------------------------------------------
Sources 2 (reported_purchase_price) and 3 (own_consumption_valuation) do not
exist: Niger's ``food_acquired`` has no ``Price`` column in any wave, so
``food_prices(units='unitprice')`` raises.  Asserted below, not assumed.

Source 1 (unit_value) exists ONLY in the two EHCVM waves (2018-19, 2021-22);
source 5 (community_price) ONLY in the two ECVMA waves (2011-12, 2014-15).
They never co-occur, so ``unit_value_vs_community_price`` is EMPTY for Niger.
Source 4 (crop_sale_price) exists in all four waves and is the bridge:

    2011-12, 2014-15 -> community_price_vs_crop_sale_price   (market vs farmgate)
    2018-19, 2021-22 -> unit_value_vs_crop_sale_price        (market vs farmgate)

Source 4 DOES have a ``u`` index level (contra the brief and the ledger),
so native-unit matching is possible in every wave.

TWO UNIT BASES
--------------
``native``  -- match on the shared ``u`` Preferred Label.  Primary.
``kg_def``  -- per-kg, using ONLY DEFINITIONAL factors read off the unit name
   (Kg=1, Gramme=0.001, 'Sac de 50 kg'=50, 'Sac de 100 kg'=100).  The library's
   ``_kg_factor_series`` is deliberately NOT used: it infers factors from
   ``Expenditure/Quantity`` ratios, i.e. from source 1 itself (100% of Niger's
   food_acquired rows get an inferred factor), so a per-kg comparison against
   source 1 would be partly a comparison of source 1 with itself.  With
   definitional factors only, 100% of the kg cells rest on exact conversions.
"""
from __future__ import annotations

import math
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import common  # noqa: E402  (slurm_logs/price_sources/common.py)

import lsms_library as ll  # noqa: E402
from lsms_library.local_tools import get_dataframe, id_walk  # noqa: E402

COUNTRY = 'Niger'
OUTDIR = Path(__file__).resolve().parent
WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']
ECVMA, EHCVM = ['2011-12', '2014-15'], ['2018-19', '2021-22']

# Household-side sources are sample statistics (protocol: 10 obs, lowerable to
# 5 for a thin source).  A community price is a MEASUREMENT -- one surveyed
# price per (cluster, item, unit) -- and is usable at 1.
THR_HOUSEHOLD, THR_THIN, THR_MEASUREMENT = 10, 5, 1

# Definitional kg factors: read off the unit NAME, never inferred from a
# price ratio.  Nothing else is converted.
KG_DEFINITIONAL = {'Kg': 1.0, 'Gramme': 0.001,
                   'Sac de 50 kg': 50.0, 'Sac de 100 kg': 100.0}

GEO_PREF = ('v', 'District', 'Region')

log_lines: list[str] = []


def log(msg=''):
    print(msg, flush=True)
    log_lines.append(str(msg))


# ---------------------------------------------------------------------------
# source frames
# ---------------------------------------------------------------------------

def _label_maps():
    """harmonize_food and u label maps, read with an ABSOLUTE search dir.

    ``niger._crop_maps()`` calls ``get_categorical_mapping`` with its default
    relative ``dirs`` (``./``, ``../../_/``, ``../../../_/``), which resolve
    only when cwd is a wave ``_/`` directory -- so it cannot be reused from a
    script that lives outside the country tree.  Same tables, same keys.
    """
    from lsms_library.local_tools import get_categorical_mapping
    d = [str(Path(os.environ['LSMS_COUNTRIES_ROOT']) / COUNTRY / '_') + '/']
    kw = {'Preferred Label': 'Preferred Label'}
    return (get_categorical_mapping(tablename='harmonize_food',
                                    idxvars='Original Label', dirs=d, **kw),
            get_categorical_mapping(tablename='u',
                                    idxvars='Original Label', dirs=d, **kw))


def _map_labels(series, mapping):
    lab = series.astype('string')
    return lab.map(lambda x: mapping.get(x, x) if pd.notna(x) else pd.NA).astype('string')



def source_unit_value(c):
    """S1: Expenditure / Quantity per native u, purchased rows only."""
    fp = c.food_prices(units='unitvalue').reset_index()
    fp = fp[fp['s'].astype(str) == 'purchased']
    out = fp.rename(columns={'Price': 'price'})[['t', 'v', 'j', 'u', 'price']]
    return _clean(out)


def source_community_price(c):
    """S5: the CS07 surveyed consumer price, per ONE native unit.

    ``Price`` is the price recorded for ``Quantity`` units of ``u``
    ('poids du produit du Nieme releve' in the raw CS07 file), so the
    per-unit price is Price / Quantity -- Niger's analogue of the
    ``NumberOfUnits`` basis the protocol names.
    """
    cp = c.community_prices().reset_index()
    q = pd.to_numeric(cp['Quantity'], errors='coerce')
    p = pd.to_numeric(cp['Price'], errors='coerce')
    cp['price'] = p.where(q > 0) / q.where(q > 0)
    return _clean(cp[['t', 'v', 'j', 'u', 'price']])


def _sold_unit_map(wave, c_updated_ids):
    """Raw (i, crop, sold-unit) triples for one wave.

    ``crop_production.u`` is the HARVEST unit (as02eq07b / AS02EQ07B /
    s16cq12b / s16cq16b), but ``Value_sold / Quantity_sold`` is a price per
    SOLD unit, which every wave records in its OWN column that the feature
    does not carry.  This rebuilds the sold unit with the same helpers the
    wave scripts use, so source 4 can be restricted to rows where the two
    agree.
    """
    sys.path.insert(0, str(Path(os.environ['LSMS_COUNTRIES_ROOT']) / COUNTRY / '_'))
    import niger  # noqa: E402
    crop_map, unit_map = _label_maps()
    R = Path(os.environ['LSMS_COUNTRIES_ROOT']) / COUNTRY / wave / 'Data'
    spec = {
        '2011-12': ('NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta',
                    ('hid',), 'as02eq06', 'as02eq12b'),
        '2014-15': ('NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS2E2P2.dta',
                    ('GRAPPE', 'MENAGE'), 'AS02EQ110B', 'AS02EQ12B'),
        '2018-19': ('s16c_me_ner2018.dta',
                    ('grappe', 'menage'), 's16cq04', 's16cq16b'),
        '2021-22': ('s16d_me_ner2021.dta',
                    ('grappe', 'menage'), 's16dq01', 's16dq05b'),
    }[wave]
    fn, idc, cropc, sun = spec
    lab = get_dataframe(str(R / fn), convert_categoricals=True)
    cod = get_dataframe(str(R / fn), convert_categoricals=False)
    if len(idc) == 1:                                   # 2011-12 scalar hid
        hh = cod[idc[0]].apply(lambda x: niger.i(x) if pd.notna(x) else pd.NA)
    else:
        hh = lab.apply(lambda r: niger.i(pd.Series([r[idc[0]], r[idc[1]]],
                                                   index=list(idc))), axis=1)
    out = pd.DataFrame({
        't': wave,
        'i': pd.Series(hh).astype('string').values,
        'crop': _map_labels(lab[cropc], crop_map).values,
        'u_sold': _map_labels(lab[sun], unit_map).values,
    })
    n_all = len(out)
    n_nou = int(out['u_sold'].isna().sum())
    out = out.dropna().drop_duplicates()
    # The API applies id_walk in _finalize_result, so a panel wave's raw
    # household id is NOT the id crop_production carries (Niger 2014-15:
    # raw '100004' -> '10004', the 2011-12 key).  Walk the raw ids the same
    # way or the join silently misses (measured: 38 of 434 households).
    walked = id_walk(out.set_index(['t', 'i']), c_updated_ids).reset_index()
    return walked, n_all, n_nou


def source_crop_sale_price(c):
    """S4: Value_sold / Quantity_sold per row, in the row's native u,
    restricted to rows whose SOLD unit equals the row's harvest unit."""
    cr = c.crop_production().reset_index()
    qs = pd.to_numeric(cr['Quantity_sold'], errors='coerce')
    vs = pd.to_numeric(cr['Value_sold'], errors='coerce')
    cr['price'] = vs.where((qs > 0) & (vs > 0)) / qs.where((qs > 0) & (vs > 0))
    cr = cr[cr['price'].notna()].copy()
    cr['j'] = cr['crop'].astype(str)
    cr['i'] = cr['i'].astype(str)
    cr['u'] = cr['u'].astype(str)
    kept, diag = [], []
    for t, g in cr.groupby('t', observed=True):
        try:
            m, n_all, n_nou = _sold_unit_map(str(t), c.updated_ids)
        except Exception as exc:                       # pragma: no cover
            log(f'   !! {t}: sold-unit map unavailable ({type(exc).__name__}: '
                f'{exc}); source 4 NOT restricted for this wave')
            kept.append(g)
            continue
        m['i'] = m['i'].astype(str)
        m['crop'] = m['crop'].astype(str)
        m['u_sold'] = m['u_sold'].astype(str)
        j = g.merge(m.rename(columns={'crop': 'j', 'u_sold': 'u'}).assign(_ok=1),
                    on=['i', 'j', 'u'], how='left')
        joinable = g.merge(m[['i', 'crop']].drop_duplicates()
                           .rename(columns={'crop': 'j'}).assign(_seen=1),
                           on=['i', 'j'], how='left')['_seen'].notna()
        ok = j['_ok'].notna().to_numpy()
        diag.append((str(t), len(g), int(joinable.sum()), int(ok.sum()), n_all, n_nou))
        kept.append(g[ok])
    log('   source 4 sold-unit restriction (wave | priced rows | (i,crop) seen in '
        'the raw sale block | rows KEPT | raw sale lines | of those with NO sold unit):')
    for row in diag:
        log(f'      {row[0]}  {row[1]:6d}  {row[2]:6d}  {row[3]:6d}'
            f'   kept {row[3] / max(row[1], 1):6.1%}   raw {row[4]:6d} '
            f'no-unit {row[5]:5d}')
    out = pd.concat(kept, ignore_index=True)
    return _clean(out[['t', 'v', 'j', 'u', 'price']])


def _clean(df):
    df = df.copy()
    for col in ('t', 'v', 'j', 'u'):
        df[col] = df[col].astype(str)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df = df[df['price'].notna() & (df['price'] > 0)]
    df = df[(df['u'] != 'nan') & (df['j'] != 'nan') & (df['u'] != 'Manquant')]
    return df.reset_index(drop=True)


def to_kg_basis(df):
    """Per-kg prices using DEFINITIONAL factors only; u collapses to 'kg'."""
    f = df['u'].map(KG_DEFINITIONAL)
    out = df[f.notna()].copy()
    out['price'] = out['price'] / f[f.notna()].to_numpy()
    out['u'] = 'kg'
    return out


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    warnings.simplefilter('ignore')
    assert 'wt-prices-niger' in ll.__file__, ll.__file__
    log(f'lsms_library : {ll.__file__}')
    log(f'countries_root: {os.environ.get("LSMS_COUNTRIES_ROOT")}')
    log(f'data dir      : {os.environ.get("LSMS_DATA_DIR")}')

    c = ll.Country(COUNTRY)

    # -- sources 2 and 3: prove absent, do not assume -----------------------
    log('\n== sources 2 / 3 (reported Price) ==')
    try:
        fp = c.food_prices(units='unitprice')
        n = int(pd.to_numeric(fp.iloc[:, 0], errors='coerce').notna().sum())
        log(f'   food_prices(units="unitprice") returns {fp.shape}, {n} non-null rows.')
        if n == 0:
            log('   -> sources 2 and 3 are ABSENT for Niger: food_acquired has no '
                'Price column in any wave, so the reported-price modes are empty. '
                'No independence test is possible or needed.')
        else:
            log('   -> INVESTIGATE: a reported Price exists after all.')
    except Exception as exc:
        log(f'   food_prices(units="unitprice") raises {type(exc).__name__}: {exc}')
        log('   -> sources 2 and 3 are ABSENT for Niger (no Price column in '
            'food_acquired, any wave).  Nothing to test for independence.')
    fa = c.food_acquired()
    log(f'   food_acquired columns: {list(fa.columns)}   '
        f's values: {sorted(fa.reset_index()["s"].astype(str).unique())}')
    log(f'   s=produced rows exist ({int((fa.reset_index()["s"].astype(str) == "produced").sum())}) '
        'but carry no reported valuation.')

    # -- currency -----------------------------------------------------------
    try:
        cur = c.food_prices(units='unitvalue', currency='column')
        curcol = [x for x in cur.columns if x.lower().startswith('cur')]
        ccode = (cur.reset_index().groupby('t')[curcol[0]].agg(
            lambda s: sorted(set(map(str, s.dropna())))) if curcol else None)
        log(f'\n== currency by wave ==\n{ccode}')
        currency_by_t = {str(k): (v[0] if v else 'XOF') for k, v in ccode.items()} \
            if ccode is not None else {}
    except Exception as exc:
        log(f'\n== currency ==  currency="column" unavailable ({type(exc).__name__}: {exc})')
        currency_by_t = {}
    for t in WAVES:
        currency_by_t.setdefault(t, 'XOF')   # CS07 records prices "en FCFA"

    # -- build the source frames -------------------------------------------
    log('\n== source frames ==')
    src = {}
    src['unit_value'] = source_unit_value(c)
    src['community_price'] = source_community_price(c)
    src['crop_sale_price'] = source_crop_sale_price(c)
    for k, v in src.items():
        log(f'   {k:20s} rows={len(v):7d}  waves={sorted(v.t.unique())}  '
            f'items={v.j.nunique():4d}  units={v.u.nunique():3d}')

    # -- PRE-CHECK (i): unit vocabularies ----------------------------------
    log('\n== PRE-CHECK (i): unit vocabulary across tables, per wave ==')
    precheck_i = []
    for t in WAVES:
        a_name = 'unit_value' if t in EHCVM else 'community_price'
        A, B = src[a_name], src['crop_sale_price']
        ua = A[A.t == t].groupby('u').size()
        ub = B[B.t == t].groupby('u').size()
        inter = sorted(set(ua.index) & set(ub.index))
        log(f'   {t}: {a_name} u={sorted(ua.index)}')
        log(f'            crop_sale_price u={sorted(ub.index)}')
        log(f'            INTERSECTION ({len(inter)}) = {inter}')
        log(f'            rows in intersection: {a_name} '
            f'{int(ua.reindex(inter).sum())}/{int(ua.sum())}  crop '
            f'{int(ub.reindex(inter).sum())}/{int(ub.sum())}')
        precheck_i.append(dict(t=t, side=a_name, n_u_a=len(ua), n_u_b=len(ub),
                               n_inter=len(inter), inter=';'.join(inter),
                               rows_a=int(ua.sum()), rows_a_in=int(ua.reindex(inter).sum()),
                               rows_b=int(ub.sum()), rows_b_in=int(ub.reindex(inter).sum())))

    # -- PRE-CHECK (ii): is the ladder's bottom rung real? -----------------
    log('\n== PRE-CHECK (ii): v coverage in cluster_features / sample ==')
    cf = c.cluster_features().reset_index()
    sm = c.sample().reset_index()
    cf['t'], cf['v'] = cf['t'].astype(str), cf['v'].astype(str)
    sm['t'], sm['v'] = sm['t'].astype(str), sm['v'].astype(str)
    have_geo = [g for g in ('District', 'Region') if g in cf.columns]
    log(f'   cluster_features geography columns available: {have_geo}')
    log('   Latitude non-null by wave: '
        f'{cf.groupby("t")["Latitude"].apply(lambda s: int(s.notna().sum())).to_dict()}'
        '   (Latitude is NOT on the ladder; 2014-15 ships no geovariables)')
    for name, frame in src.items():
        for t in sorted(frame.t.unique()):
            vv = set(frame.loc[frame.t == t, 'v'])
            incf = len(vv & set(cf.loc[cf.t == t, 'v']))
            insm = len(vv & set(sm.loc[sm.t == t, 'v']))
            log(f'   {name:20s} {t}: n_v={len(vv):4d}  in cluster_features={incf:4d}'
                f'  in sample={insm:4d}')

    # -- attach geography ---------------------------------------------------
    geo = {}
    for name, frame in src.items():
        g, miss = common.attach_geo(frame, c, geo_cols=tuple(have_geo))
        log(f'   attach_geo {name}: {miss} rows whose (t, v) has no '
            'cluster_features row (kept, not dropped)')
        geo[name] = g

    # -- cells, gaps, models -----------------------------------------------
    PAIRS = [('community_price', 'crop_sale_price', ECVMA),
             ('unit_value', 'crop_sale_price', EHCVM),
             ('unit_value', 'community_price', [])]   # empty by construction
    cells_rows, gaps_rows, model_rows_all, byitem = [], [], [], []

    for basis in ('native', 'kg_def'):
        frames = {k: (v if basis == 'native' else to_kg_basis(v))
                  for k, v in geo.items()}
        cellcache = {}
        for name, frame in frames.items():
            for t in sorted(frame.t.unique()):
                f = frame[frame.t == t]
                if not len(f):
                    continue
                levels = common.geo_ladder(f, prefer=GEO_PREF)
                cm = common.cell_medians(f, levels)
                cm.insert(0, 'country', COUNTRY)
                cm['source'], cm['currency'] = name, currency_by_t.get(t, 'XOF')
                cellcache[(basis, name, t)] = cm
                if basis == 'native':
                    cells_rows.append(cm)

        for a, b, waves in PAIRS:
            ta = THR_MEASUREMENT if a == 'community_price' else THR_HOUSEHOLD
            tb = THR_MEASUREMENT if b == 'community_price' else THR_THIN
            # source 4 is thin (556-3226 priced rows/wave): threshold 5, said so
            pair = f'{a}_vs_{b}'
            for t in waves:
                ca, cb = cellcache.get((basis, a, t)), cellcache.get((basis, b, t))
                if ca is None or cb is None:
                    continue
                g = common.match_gaps(ca, cb, threshold_a=ta, threshold_b=tb)
                if not len(g):
                    log(f'   {basis:7s} {pair} {t}: 0 matched cells')
                    continue
                g = g.assign(country=COUNTRY, pair=pair)
                if basis == 'native':
                    gaps_rows.append(g)
                model_rows_all += common.model_rows(
                    g, country=COUNTRY, t=t, pair=pair, u_basis=basis,
                    threshold_a=ta, threshold_b=tb)
                log(f'   {basis:7s} {pair} {t}: {len(g):4d} cells, '
                    f'levels={sorted(set(g.geo_level))}, '
                    f'median gap={g.gap_log.median():+.3f} '
                    f'(ratio {math.exp(g.gap_log.median()):.2f})')
                bi = (g.groupby('j', observed=True)['gap_log']
                        .agg(n_cells='size', median_gap='median',
                             q25=lambda s: s.quantile(.25),
                             q75=lambda s: s.quantile(.75))
                        .reset_index())
                bi['ratio'] = np.exp(bi['median_gap'])
                bi.insert(0, 'u_basis', basis)
                bi.insert(0, 't', t)
                bi.insert(0, 'pair', pair)
                byitem.append(bi)

    cells = pd.concat(cells_rows, ignore_index=True) if cells_rows else pd.DataFrame()
    gaps = pd.concat(gaps_rows, ignore_index=True) if gaps_rows else pd.DataFrame()
    models = pd.DataFrame(model_rows_all)
    if len(gaps):
        gaps['country'] = COUNTRY
    for col in common.GAP_COLS:
        if col not in gaps.columns:
            gaps[col] = pd.Series(dtype=float)
    paths = common.write_outputs(OUTDIR, cells=cells, gaps=gaps, models=models)
    log(f'\n== wrote {[str(p) for p in paths]} ==')

    # -- SUPPLEMENTARY (not part of the protocol schemas): the same pairing
    # done at ONE fixed rung at a time (finest_only=False), so the reader can
    # see how many cells each rung actually supports.  Niger's cells are thin
    # enough that no FWL slope is identified; these counts are the evidence.
    rung_rows = []
    for basis in ('native', 'kg_def'):
        frames = {k: (v if basis == 'native' else to_kg_basis(v))
                  for k, v in geo.items()}
        for a, b, waves in PAIRS:
            ta = THR_MEASUREMENT if a == 'community_price' else THR_HOUSEHOLD
            tb = THR_MEASUREMENT if b == 'community_price' else THR_THIN
            for t in waves:
                fa_, fb_ = frames[a][frames[a].t == t], frames[b][frames[b].t == t]
                if not len(fa_) or not len(fb_):
                    continue
                ca = common.cell_medians(fa_, common.geo_ladder(fa_, prefer=GEO_PREF))
                cb = common.cell_medians(fb_, common.geo_ladder(fb_, prefer=GEO_PREF))
                for lvl in ('v', 'District', 'Region', 'national'):
                    g = common.match_gaps(ca[ca.geo_level == lvl],
                                          cb[cb.geo_level == lvl],
                                          threshold_a=ta, threshold_b=tb,
                                          finest_only=False)
                    if not len(g):
                        continue
                    rung_rows.append(dict(
                        country=COUNTRY, t=t, pair=f'{a}_vs_{b}', u_basis=basis,
                        geo_level=lvl, n_cells=len(g), n_items=g.j.nunique(),
                        n_geos=g.geo.nunique(),
                        median_gap=float(g.gap_log.median()),
                        ratio=float(np.exp(g.gap_log.median())),
                        iqr_gap=float(g.gap_log.quantile(.75) - g.gap_log.quantile(.25)),
                        resid_dof=int(len(g) - g.j.nunique() - g.geo.nunique() - 1),
                        items=';'.join(sorted(map(str, g.j.unique())))))
    if rung_rows:
        rr = pd.DataFrame(rung_rows)
        rr.to_csv(OUTDIR / 'gaps_by_rung.csv', index=False)
        log('\n== SUPPLEMENTARY: one fixed rung at a time (finest_only=False) ==')
        with pd.option_context('display.width', 250, 'display.max_columns', 30):
            log(rr.drop(columns='items').to_string(index=False))

    if byitem:
        bi = pd.concat(byitem, ignore_index=True)
        bi.to_csv(OUTDIR / 'gaps_by_item.csv', index=False)
        log(f'   + {OUTDIR / "gaps_by_item.csv"}')
        log('\n== gap BY ITEM (native basis) ==')
        with pd.option_context('display.width', 200, 'display.max_rows', 400):
            log(bi[bi.u_basis == 'native']
                .sort_values(['pair', 't', 'median_gap']).to_string(index=False))
    pd.DataFrame(precheck_i).to_csv(OUTDIR / 'precheck_units.csv', index=False)

    log('\n== models.csv (pooled rows) ==')
    with pd.option_context('display.width', 250, 'display.max_columns', 30):
        log(models[models.geo_level == 'all'].to_string(index=False))
    log('\n== models.csv (per geo level) ==')
    with pd.option_context('display.width', 250, 'display.max_columns', 30):
        log(models[models.geo_level != 'all'].to_string(index=False))

    (OUTDIR / 'analysis.log').write_text('\n'.join(log_lines) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
