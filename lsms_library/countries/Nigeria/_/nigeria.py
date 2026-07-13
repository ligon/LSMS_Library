
import os
import re
import pandas as pd
import numpy as np

if __name__ == '__main__':
    import sys
    sys.path.append('../../_')
    sys.path.append('../../../_')
    from local_tools import format_id
else:
    from lsms_library.local_tools import format_id

Waves = {'2010-11': (),
         '2012-13': (),
         '2015-16': (),
         '2018-19': (),
         '2023-24': ()}

waves = ['2010Q3', '2011Q1', '2012Q3', '2013Q1', '2015Q3', '2016Q1', '2018Q3', '2019Q1', '2023Q3', '2024Q1']

wave_folder_map = {
    '2010Q3': '2010-11',
    '2011Q1': '2010-11',
    '2012Q3': '2012-13',
    '2013Q1': '2012-13',
    '2015Q3': '2015-16',
    '2016Q1': '2015-16',
    '2018Q3': '2018-19',
    '2019Q1': '2018-19',
    '2023Q3': '2023-24',
    '2024Q1': '2023-24',
}


# =====================================================================
# Cluster identity `v`  (GH #323)
# =====================================================================
# `ea` ALONE IS NOT A CLUSTER ID.  In the GHS-Panel it is a serial number
# unique only *within* an LGA, so the same `ea` value recurs in different
# LGAs and states.  Wave 1 is the proof: the design is 500 EAs x 10
# households = 5,000 households, but
#
#     nunique(ea)                = 411      <- 89 real EAs conflated away
#     nunique(state, lga, ea)    = 500      <- the design, recovered
#
# and the household geovariables confirm it independently: lat/lon is
# constant within all 500 (state, lga, ea) groups (0 groups span >1
# coordinate) but VARIES inside 76 of the 411 bare `ea` codes.  Keying on
# `ea` therefore MERGES DISTINCT CLUSTERS, and the household->cluster
# collapse in cluster_features then stamps one arbitrary EA's
# Region/District/Rural onto every household in the merged group:
# 890/5000 households got the wrong District in W1 alone (17.8%), rising
# to 1283/5263 (24.4%) by W4.  That is a class-1 SILENTLY-WRONG defect,
# and it leaks everywhere, because sample() publishes the same broken key
# and _join_v_from_sample() stamps it onto every household table.
#
# THE KEYSPACE IS BUILT FROM CODES, NOT LABELS.  The community
# questionnaire (community_prices) must land in the SAME keyspace as the
# household files so a cluster's prices join its households.  In W2 the
# community file's state/lga carry no Stata value labels while the
# household file's do, so a LABEL-built key gives 0% overlap between the
# two sides; a CODE-built key gives 100%.  Hence every consumer of these
# helpers reads its geography with convert_categoricals=False (the YAML
# path does this with `converted_categoricals:` on the sub-df) and the
# composite is assembled from the raw numeric codes.
#
# THE `Moved` SENTINEL.  From W2 on, a tracked household that left its
# original EA is recorded with ea == 0 -- surfacing as the label 'Moved'
# (W2), '0. Moved' (W3), or a bare 0 (W4).  It is a MISSING-VALUE
# SENTINEL, not a cluster: pooling those households would weld unrelated
# households from across the country into fake clusters like
# (Lagos, ETI-OSA, 'Moved').  They have no sampling cluster, so each is
# given its own singleton id.  A NaN `v` was rejected deliberately: the
# downstream groupby() in roster_to_characteristics / the food-derivation
# pipeline DROPS NaN keys, which would silently delete these households
# (152 / 306 / 166 in W2 / W3 / W4) from every derived table -- trading
# one silent-data-loss bug for another.  An explicit singleton keeps them
# present, keeps their true Region/District/Rural, and can never merge
# them with anyone else.
_MOVED_EA_CODE = 0


def _geo_code(x):
    """Normalize one geographic component to its numeric CODE as a string.

    Handles every rendering the GHS files use for the same underlying code:
    a raw numeric (``1690`` / ``1690.0``), a Stata code-prefixed label
    (``'102. ABA SOUTH'`` -> ``'102'``), and the bare ``'Moved'`` label,
    which is code 0.  Returns ``None`` when the value is missing.
    """
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    m = re.match(r'^(\d+)\s*\.\s*\D', s)   # '102. ABA SOUTH' -> '102'
    if m:
        return m.group(1)
    if re.fullmatch(r'-?\d+(\.0*)?', s):   # 1690 / '1690' / '1690.0'
        return str(int(float(s)))
    if s.lower().lstrip('0. ').startswith('moved'):
        return str(_MOVED_EA_CODE)
    return ' '.join(s.upper().split())     # last-resort: a bare label


def cluster_id(state, lga, ea, hhid=None):
    """The composite EA identity for one household.  See the note above.

    Real EA        -> ``'{state}/{lga}/{ea}'`` (codes)
    'Moved' (ea=0) -> ``'moved-{hhid}'``: no sampling cluster; a singleton
                      that can never be pooled with another household.
    """
    ea_c = _geo_code(ea)
    if ea_c is None:
        return None
    if ea_c == str(_MOVED_EA_CODE):
        hh = format_id(hhid)
        return f'moved-{hh}' if hh is not None else None
    state_c, lga_c = _geo_code(state), _geo_code(lga)
    if state_c is None or lga_c is None:
        return None
    return f'{state_c}/{lga_c}/{ea_c}'


def v(row):
    """`v` formatting function, auto-bound by df_data_grabber.

    Declared in the wave YAML as ``v: [state, lga, ea, hhid]`` (idxvars for
    cluster_features, myvars for sample), so BOTH tables -- and
    community_prices, via cluster_id() -- share one keyspace by
    construction.  They must: sample.v is what _join_v_from_sample() stamps
    onto every household table, and community_prices.v has to join it.
    """
    return cluster_id(row.get('state'), row.get('lga'), row.get('ea'),
                      row.get('hhid'))


# ---------------------------------------------------------------------
# plot_features (GH #167)
# ---------------------------------------------------------------------

# Post-planting quarter assigned as the single `t` for each wave's
# plot_features.  Lasting plot attributes are recorded only in the
# post-planting round (the post-harvest sect11 files are crop-level), so
# each wave contributes exactly one t value.  These are a subset of the
# quarter-based t values used elsewhere for Nigeria (see `waves`).
PP_QUARTER = {
    '2010-11': '2010Q3',
    '2012-13': '2012Q3',
    '2015-16': '2015Q3',
    '2018-19': '2018Q3',
    '2023-24': '2023Q3',
}

# Post-harvest quarter assigned as the single `t` for each wave's
# crop_production.  Crop-level harvest is recorded only in the
# post-harvest round (the secta3* files), so each wave contributes
# exactly one t value -- the PH quarter -- distinct from the PP quarter
# used by plot_features (same survey wave, different round).  plot_id
# values still align across the two rounds (both format_id(plotid)).
PH_QUARTER = {
    '2010-11': '2011Q1',
    '2012-13': '2013Q1',
    '2015-16': '2016Q1',
    '2018-19': '2019Q1',
    '2023-24': '2024Q1',
}


# ---------------------------------------------------------------------
# crop_production (GAP 1) -- item-level reported crop harvest
# ---------------------------------------------------------------------
#
# Natural grain (t, i, plot, crop): one row per crop grown on a plot in
# the post-harvest crop module (secta3*).  Stores REPORTED item-level
# fields only -- Quantity (native harvest qty) + u (native unit),
# Quantity_sold + Value_sold (reported), planting_month + harvest_month
# (item dates), intercropped + perennial flags.  No kg conversion, no
# yield, no main_crop, no shares -- those are transformations.
#
# crop labels (j): cropcode -> Preferred Label via harmonize_food
# (extended with the crop codes; reused food labels where a crop is a
# consumed food so crop_production.j joins food_acquired.j).
# units (u): native production-unit label normalized to a base
# Preferred Label registered in the `u` table.


def harmonized_food_labels():
    """{int itemcode: Preferred Label} from the Code-keyed harmonize_food
    table.  This is the SAME resolver crop_production uses (see
    ``_crop_labels``), exposed for food_acquired so its ``j`` resolves to
    the shared Preferred Label and joins crop_production.crop (GH #443).

    The food itemcodes in the GHS consumption modules are read as integers
    (``item_cd`` is int64 in the source CSVs), while harmonize_food's Code
    column is string-keyed; this returns an INT-keyed dict so a numeric
    ``j`` matches.  Codes with a blank / '---' Preferred Label are dropped
    (they legitimately stay raw)."""
    from lsms_library.local_tools import get_categorical_mapping
    raw = get_categorical_mapping(tablename='harmonize_food', idxvars='Code',
                                  **{'Preferred Label': 'Preferred Label'})
    out = {}
    for k, v in raw.items():
        try:
            ik = int(k)
        except (TypeError, ValueError):
            continue
        if pd.isna(v) or str(v).strip() in ('', '---'):
            continue
        out[ik] = str(v).strip()
    return out


def _crop_labels():
    """{int cropcode: Preferred Label} from harmonize_food (shared with
    food_acquired)."""
    return harmonized_food_labels()


def _strip_code_prefix(s):
    """'1080. MAIZE' -> 'MAIZE'; '130. SACK/BAG' -> 'SACK/BAG'."""
    import re
    s = str(s).strip()
    m = re.match(r'^\s*\d+\.\s*(.*)$', s)
    return (m.group(1) if m else s).strip()


def _canon_unit(s):
    """Normalize a native harvest production-unit label to a size-agnostic
    base Preferred Label registered in the `u` table.  Returns pd.NA for
    blanks / pure-numeric junk."""
    import re
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return pd.NA
    t = _strip_code_prefix(s)
    if t == '' or re.fullmatch(r'[\d.]+', t):
        return pd.NA
    tl = t.lower()
    if 'kilogram' in tl or tl == 'kg' or tl == 'kilograms':
        return 'Kg'
    if 'gram' in tl or tl == 'grams':
        return 'g'
    if 'centilitre' in tl or tl == 'cl':
        return 'cl'
    if 'litre' in tl or tl == 'litres' or tl == 'l':
        return 'l'
    if 'bag' in tl or 'sack' in tl:
        return 'Sack/Bag'
    if 'basket' in tl or 'bin/basket' in tl:
        return 'Basket'
    if 'basin' in tl:
        return 'Basin'
    if 'bowl' in tl:
        return 'Bowl'
    if 'bunch' in tl:
        return 'Bunch'
    if 'bundle' in tl:
        return 'Bundle'
    if 'tuber' in tl:
        return 'Tuber'
    if 'heap' in tl:
        return 'Heap'
    if 'stalk' in tl:
        return 'Stalk'
    if 'wheel' in tl:
        return 'Wheelbarrow'
    if 'pick' in tl:
        return 'Pick-up'
    if 'jerry' in tl or 'keg' in tl:
        return 'Jerry Can'
    if 'derica' in tl or tl == 'tin':
        return 'Derica'
    if 'tiya' in tl:
        return 'Tiya'
    if 'kobiowu' in tl:
        return 'Kobiowu'
    if 'congo' in tl:
        return 'Congo'
    if 'mudu' in tl:
        return 'Mudu'
    if 'packet' in tl or 'sachet' in tl:
        return 'Packet/Sachet'
    if 'piece' in tl:
        return 'Piece'
    if 'paint rubber' in tl:
        return 'Paint rubber'
    if 'milk cup' in tl:
        return 'Milk cup'
    if 'cigarette' in tl:
        return 'Cigarette cup'
    if 'olodo' in tl:
        return 'Olodo'
    if 'other' in tl or 'specify' in tl:
        return 'other specify'
    return pd.NA


def _month_str(month_series, year_series):
    """Build a 'YYYY-MM' month string from numeric month + year columns;
    pd.NA where month is missing/invalid."""
    m = pd.to_numeric(month_series, errors='coerce')
    y = pd.to_numeric(year_series, errors='coerce') if year_series is not None else None
    out = pd.Series(pd.NA, index=m.index, dtype='string')
    valid = m.between(1, 12)
    if y is not None:
        valid = valid & y.notna()
        out.loc[valid] = (y[valid].astype('Int64').astype(str) + '-'
                          + m[valid].astype('Int64').astype(str).str.zfill(2))
    else:
        out.loc[valid] = m[valid].astype('Int64').astype(str).str.zfill(2)
    return out


def crop_production_for_wave(t, frames, crop_labels):
    """Assemble item-level crop_production for one Nigeria GHS-Panel wave.

    Parameters
    ----------
    t : str
        PH-quarter wave id (e.g. '2011Q1'), used as the `t` index.
    frames : list of dict
        Each dict describes one source frame (annual harvest, perennial,
        and/or an hh-crop sold frame) with keys:
            df        : raw DataFrame (convert_categoricals as noted below)
            dec       : decoded-label DataFrame (for crop/unit label decode)
            hhid, plot, crop                 column names (plot may be None)
            qty, unit                        harvest qty / unit columns
            qty_sold, unit_sold, value_sold  reported sale columns (optional)
            plant_m, plant_y                 planting/harvest-start month/yr
            harv_m, harv_y                   harvest-end month/yr (optional)
            intercropped                     bool Series aligned to df (opt)
            perennial                        bool flag for the whole frame
            sold_on                          'plot' | 'hh' join grain for sale
    crop_labels : dict
        {int cropcode: Preferred Label}.

    Returns
    -------
    DataFrame indexed by (t, i, plot, crop) with reported columns.
    Sale columns are populated only for frames where they sit at the
    plot-crop grain; hh-crop sale frames are merged on (i, crop) where
    that is the survey's grain (W3-W5 record sales at hh-crop level, so
    the same sale value applies to every plot row of that hh-crop --
    a reported attribute, not a per-plot allocation).
    """
    pieces = []
    for fr in frames:
        df = fr['df']
        dec = fr['dec']
        n = len(df)
        i = df[fr['hhid']].apply(format_id)
        crop_code = pd.to_numeric(df[fr['crop']], errors='coerce').astype('Int64')
        crop = crop_code.map(crop_labels).astype('string')
        if fr.get('plot') is not None:
            plot = df[fr['plot']].apply(format_id)
        else:
            plot = pd.Series(pd.NA, index=df.index, dtype='string')

        piece = pd.DataFrame({
            'i': i.values,
            'plot': plot.values,
            'crop': crop.values,
        }, index=df.index)

        piece['Quantity'] = (pd.to_numeric(df[fr['qty']], errors='coerce')
                             if fr.get('qty') in df.columns else pd.NA)
        if fr.get('unit') in dec.columns:
            piece['u'] = dec[fr['unit']].map(_canon_unit).astype('string').values
        else:
            piece['u'] = pd.Series(pd.NA, index=df.index, dtype='string').values

        # Reported sale at plot-crop grain (W1/W2); hh-crop sales merged
        # separately below.
        if fr.get('sold_on') == 'plot':
            piece['Quantity_sold'] = (pd.to_numeric(df[fr['qty_sold']], errors='coerce')
                                      if fr.get('qty_sold') in df.columns else pd.NA)
            piece['Value_sold'] = (pd.to_numeric(df[fr['value_sold']], errors='coerce')
                                   if fr.get('value_sold') in df.columns else pd.NA)

        piece['planting_month'] = (
            _month_str(df[fr['plant_m']], df.get(fr.get('plant_y'))).values
            if fr.get('plant_m') in df.columns
            else pd.Series(pd.NA, index=df.index, dtype='string').values)
        piece['harvest_month'] = (
            _month_str(df[fr['harv_m']], df.get(fr.get('harv_y'))).values
            if fr.get('harv_m') in df.columns
            else pd.Series(pd.NA, index=df.index, dtype='string').values)

        if fr.get('intercropped') is not None:
            piece['intercropped'] = fr['intercropped'].reindex(df.index).values
        piece['perennial'] = bool(fr.get('perennial', False))

        pieces.append(piece)

    out = pd.concat(pieces, ignore_index=True)
    out['t'] = t

    # Ensure all canonical columns exist.
    for col in ['Quantity', 'u', 'Quantity_sold', 'Value_sold',
                'planting_month', 'harvest_month', 'intercropped', 'perennial']:
        if col not in out.columns:
            out[col] = pd.NA

    # Drop rows with no crop label resolved (free-text junk codes) and no
    # household.  Keep rows with a crop even if Quantity is NaN (the
    # survey recorded the crop on the plot but no harvest qty).
    out = out[out['i'].notna() & out['crop'].notna()]

    # Dedup on the index grain (a handful of duplicate (i,plot,crop) rows
    # exist in W1); keep the first non-null record.
    out = out.sort_values(['i', 'plot', 'crop'])
    out = out.drop_duplicates(subset=['t', 'i', 'plot', 'crop'], keep='first')

    out = out.set_index(['t', 'i', 'plot', 'crop']).sort_index()
    out = out[['Quantity', 'u', 'Quantity_sold', 'Value_sold',
               'planting_month', 'harvest_month', 'intercropped', 'perennial']]
    return out


# ---------------------------------------------------------------------
# plot_inputs (GAP 2) -- item-level reported agricultural inputs
# ---------------------------------------------------------------------
#
# Natural grain (t, i, plot, input[, crop]): one row per input applied to
# a plot in the post-planting / post-harvest input modules.  `input` is a
# harmonized input-type label (Seed / NPK / Urea / Organic Fertilizer /
# Other Inorganic Fertilizer / Pesticide / Herbicide / Animal Traction /
# Tractor-Machinery) carried on the harmonize_input categorical table.
# A `crop` index level (harmonize_food labels) disambiguates the multiple
# seed rows a plot can carry (one per crop); non-seed inputs are not
# crop-specific and carry crop = 'n/a' (a non-null sentinel so the index
# stays unique and free of null index levels).
#
# Stores REPORTED item-level fields ONLY:
#   input               harmonized input-type label (index)
#   crop                seed's crop (index; 'n/a' for non-seed inputs)
#   Quantity, u         reported quantity + native unit
#   Purchased           bool: any of this input acquired by purchase
#   Quantity_purchased  reported purchased quantity (where recorded)
#   Improved            bool (seed rows): improved / certified seed variety
# NO kg conversion, NO seed_kg / nitrogen_kg / any-use flags, NO fertilizer
# totals -- those are transformations.
#
# The Nigeria GHS input modules organise each input by ACQUISITION CHANNEL
# (left-over / free / commercial-1 / commercial-2), each channel its own
# quantity+unit triplet.  We keep ONE row per (plot[,crop], input) and sum
# the reported native-unit quantities across channels ONLY when they share
# a unit; otherwise we keep the channel with the largest reported quantity
# and carry its unit (mixed-unit channels can't be summed without a kg
# conversion, which is a transformation).  Purchased quantity is the sum of
# the commercial channels.  This keeps the row item-level and reported.

# Harmonized input-type labels (the `input` index axis).  Codes are an
# arbitrary stable enumeration registered in harmonize_input.
INPUT_SEED = 'Seed'
INPUT_NPK = 'NPK'
INPUT_UREA = 'Urea'
INPUT_FERT_OTHER = 'Other Inorganic Fertilizer'
INPUT_ORGANIC = 'Organic Fertilizer'
INPUT_PESTICIDE = 'Pesticide'
INPUT_HERBICIDE = 'Herbicide'
INPUT_ANIMAL = 'Animal Traction'
INPUT_TRACTOR = 'Tractor/Machinery'

NO_CROP = 'n/a'      # crop-level sentinel for non-seed (crop-agnostic) rows


def _seed_unit_label(unit_series, unit_dec):
    """Canonical unit label for a seed-quantity unit column.  `unit_dec`
    is the decoded-label Series (convert_categoricals=True); fall back to
    pd.NA where the unit is blank/junk."""
    if unit_dec is not None:
        return unit_dec.map(_canon_unit).astype('string')
    return pd.Series(pd.NA, index=unit_series.index, dtype='string')


def _pick_channel(channels):
    """Collapse a list of (qty Series, unit Series) acquisition channels
    into ONE (Quantity, u) pair per row, reported-only:

    - If every non-null channel for a row shares the same unit label, sum
      the quantities (legitimate -- same native unit).
    - Otherwise keep the channel with the largest reported quantity and
      its unit (cannot sum across units without a kg conversion factor,
      which is a transformation).

    `channels` is a list of dicts {qty: Series(float), u: Series(string)},
    all aligned to a common index.  Returns (qty Series, u Series)."""
    if not channels:
        return None, None
    idx = channels[0]['qty'].index
    qty_out = pd.Series(pd.NA, index=idx, dtype='Float64')
    u_out = pd.Series(pd.NA, index=idx, dtype='string')

    qmat = pd.concat([c['qty'] for c in channels], axis=1)
    umat = pd.concat([c['u'] for c in channels], axis=1)
    qmat.columns = range(len(channels))
    umat.columns = range(len(channels))

    for ix in idx:
        qrow = qmat.loc[ix]
        urow = umat.loc[ix]
        mask = qrow.notna() & (qrow > 0)
        if not mask.any():
            # No positive quantity; keep first reported unit (if any) so a
            # recorded-but-zero / recorded-without-qty input still surfaces.
            present = urow[urow.notna()]
            if len(present):
                u_out.loc[ix] = present.iloc[0]
            # leave a single reported qty (possibly NaN) so the row exists
            present_q = qrow[qrow.notna()]
            if len(present_q):
                qty_out.loc[ix] = present_q.iloc[0]
            continue
        units = urow[mask].dropna().unique()
        if len(units) == 1:
            qty_out.loc[ix] = qrow[mask].sum()
            u_out.loc[ix] = units[0]
        else:
            # mixed units -> keep the single largest reported channel
            j = qrow[mask].astype(float).idxmax()
            qty_out.loc[ix] = qrow[j]
            u_out.loc[ix] = urow[j]
    return qty_out, u_out


def seed_rows_for_wave(df, dec, hhid, plot, crop, channels, crop_labels,
                       purchased_idx=None, improved=None):
    """Build item-level seed rows (input='Seed') for one wave.

    Parameters
    ----------
    df, dec : raw / decoded seed-module DataFrames (same row order).
    hhid, plot, crop : column names for household, plot, cropcode.
    channels : list of dicts describing each acquisition channel:
        {qty: 'col', unit: 'col', purchased: bool}
        `unit` is decoded via `dec` -> _canon_unit.
    crop_labels : {int cropcode: Preferred Label}.
    purchased_idx : list[int] | None
        Indices into `channels` that are purchase (commercial) channels;
        their summed quantity is Quantity_purchased and Purchased=True when
        positive.  None -> infer from channel['purchased'].
    improved : Series aligned to df (nullable boolean) | None.

    Returns a DataFrame with columns
        [i, plot, input, crop, Quantity, u, Purchased, Quantity_purchased,
         Improved].
    """
    i = df[hhid].apply(format_id)
    plot_id = df[plot].apply(format_id)
    crop_code = pd.to_numeric(df[crop], errors='coerce').astype('Int64')
    crop_lab = crop_code.map(crop_labels).astype('string')

    chan_pairs = []
    purch_qtys = []
    for k, ch in enumerate(channels):
        q = (pd.to_numeric(df[ch['qty']], errors='coerce').astype('Float64')
             if ch['qty'] in df.columns
             else pd.Series(pd.NA, index=df.index, dtype='Float64'))
        u = (_seed_unit_label(df[ch['unit']],
                              dec[ch['unit']] if ch['unit'] in dec.columns else None)
             if ch.get('unit') else pd.Series(pd.NA, index=df.index, dtype='string'))
        q.index = df.index
        u.index = df.index
        chan_pairs.append({'qty': q, 'u': u})
        is_purch = ch.get('purchased', False) if purchased_idx is None else (k in purchased_idx)
        if is_purch:
            purch_qtys.append(q)

    qty, u = _pick_channel(chan_pairs)

    if purch_qtys:
        pq = pd.concat(purch_qtys, axis=1).sum(axis=1, min_count=1)
        pq = pd.Series(pq.values, index=df.index, dtype='Float64')
    else:
        pq = pd.Series(pd.NA, index=df.index, dtype='Float64')
    purchased = pd.Series(pd.NA, index=df.index, dtype='boolean')
    purchased = purchased.where(~(pq > 0), True)
    purchased = purchased.where(~(pq == 0), False)

    out = pd.DataFrame({
        'i': i.values,
        'plot': plot_id.values,
        'input': INPUT_SEED,
        'crop': crop_lab.values,
        'Quantity': pd.Series(qty.values, dtype='Float64'),
        'u': pd.Series(u.values, dtype='string'),
        'Purchased': pd.Series(purchased.values, dtype='boolean'),
        'Quantity_purchased': pd.Series(pq.values, dtype='Float64'),
    })
    if improved is not None:
        out['Improved'] = pd.Series(improved.reindex(df.index).values,
                                    dtype='boolean')
    else:
        out['Improved'] = pd.Series(pd.NA, index=out.index, dtype='boolean')
    # Keep only rows with a household, a plot, and a resolved crop.
    out = out[out['i'].notna() & out['plot'].notna() & out['crop'].notna()]
    return out


def fert_rows_long_typed(df, dec, hhid, plot, channels, type_map):
    """Build fertilizer rows for waves where each acquisition channel
    records a fertilizer TYPE code + a single quantity/unit (W1/W2/W3).

    For each channel (left-over/free/commercial), the row's type comes
    from the channel's 'type' column mapped through `type_map`
    ({code:label}); rows are grouped to ONE row per (plot, type),
    summing same-unit quantities across the channels that resolved to
    that type.

    df  : raw frame (convert_categoricals=False; numeric type/qty codes).
    dec : decoded frame (convert_categoricals=True; unit LABELS).
    channels : list of dicts {qty, unit, type, purchased}.
    Returns DataFrame [i, plot, input, crop, Quantity, u, Purchased,
    Quantity_purchased, Improved].
    """
    i = df[hhid].apply(format_id)
    plot_id = df[plot].apply(format_id)
    base = pd.DataFrame({'i': i.values, 'plot': plot_id.values},
                        index=df.index)

    # Build a per-channel long frame, then split by resolved type label.
    chan_frames = []
    for ch in channels:
        if ch['qty'] not in df.columns or ch['type'] not in df.columns:
            continue
        typ = (pd.to_numeric(df[ch['type']], errors='coerce').astype('Int64')
               .map(type_map).astype('string'))
        q = pd.to_numeric(df[ch['qty']], errors='coerce').astype('Float64')
        u = (dec[ch['unit']].map(_canon_unit).astype('string')
             if ch.get('unit') and ch['unit'] in dec.columns
             else pd.Series('Kg', index=df.index, dtype='string'))
        chan_frames.append(pd.DataFrame({
            'i': base['i'].values, 'plot': base['plot'].values,
            'type': typ.values, 'qty': q.values, 'u': u.values,
            'purchased': bool(ch.get('purchased', False)),
        }, index=df.index))
    if not chan_frames:
        return pd.DataFrame(columns=['i', 'plot', 'input', 'crop', 'Quantity',
                                     'u', 'Purchased', 'Quantity_purchased',
                                     'Improved'])
    long = pd.concat(chan_frames, ignore_index=True)
    long = long[long['type'].notna() & long['i'].notna() & long['plot'].notna()]

    # One row per (i, plot, type): sum same-unit quantities, purchased = any
    # positive purchased-channel quantity.
    recs = []
    for (i_, p_, typ), g in long.groupby(['i', 'plot', 'type']):
        gg = g[g['qty'].notna() & (g['qty'] > 0)]
        if len(gg):
            units = gg['u'].dropna().unique()
            if len(units) == 1:
                qty = gg['qty'].sum(); uu = units[0]
            else:
                row = gg.loc[gg['qty'].astype(float).idxmax()]
                qty = row['qty']; uu = row['u']
        else:
            qty = pd.NA
            uu = g['u'].dropna().iloc[0] if g['u'].notna().any() else pd.NA
        pq = g.loc[g['purchased'] & g['qty'].notna(), 'qty']
        pqsum = pq.sum() if len(pq) else pd.NA
        purchased = (pd.NA if (pqsum is pd.NA or pd.isna(pqsum))
                     else (True if pqsum > 0 else False))
        recs.append({'i': i_, 'plot': p_, 'input': typ, 'crop': NO_CROP,
                     'Quantity': qty, 'u': uu, 'Purchased': purchased,
                     'Quantity_purchased': pqsum, 'Improved': pd.NA})
    out = pd.DataFrame.from_records(recs)
    if len(out):
        out['Quantity'] = out['Quantity'].astype('Float64')
        out['Quantity_purchased'] = out['Quantity_purchased'].astype('Float64')
        out['u'] = out['u'].astype('string')
        out['Purchased'] = out['Purchased'].astype('boolean')
        out['Improved'] = out['Improved'].astype('boolean')
    return out


def fert_rows_wide_typed(df, dec, hhid, plot, specs):
    """Build fertilizer rows for waves where each TYPE has its own
    quantity/unit columns (W4/W5: NPK / Urea / other / organic).

    df  : raw frame (numeric quantity codes).
    dec : decoded frame (unit LABELS).
    specs : list of dicts {input, qty, unit}.  One output row per (plot,
    input) where the type's quantity is recorded.
    """
    i = df[hhid].apply(format_id)
    plot_id = df[plot].apply(format_id)
    pieces = []
    for sp in specs:
        if sp['qty'] not in df.columns:
            continue
        q = pd.to_numeric(df[sp['qty']], errors='coerce').astype('Float64')
        u = (dec[sp['unit']].map(_canon_unit).astype('string')
             if sp.get('unit') and sp['unit'] in dec.columns
             else pd.Series('Kg', index=df.index, dtype='string'))
        piece = pd.DataFrame({
            'i': i.values, 'plot': plot_id.values, 'input': sp['input'],
            'crop': NO_CROP, 'Quantity': q.values, 'u': u.values,
            'Purchased': pd.Series(pd.NA, index=df.index, dtype='boolean').values,
            'Quantity_purchased': pd.Series(pd.NA, index=df.index, dtype='Float64').values,
            'Improved': pd.Series(pd.NA, index=df.index, dtype='boolean').values,
        }, index=df.index)
        # keep rows where the type's quantity was recorded (>0 or non-null)
        piece = piece[piece['Quantity'].notna()]
        pieces.append(piece)
    if not pieces:
        return pd.DataFrame(columns=['i', 'plot', 'input', 'crop', 'Quantity',
                                     'u', 'Purchased', 'Quantity_purchased',
                                     'Improved'])
    out = pd.concat(pieces, ignore_index=True)
    out = out[out['i'].notna() & out['plot'].notna()]
    return out


def chem_rows(df, dec, hhid, plot, specs):
    """Build pesticide / herbicide / animal-traction rows carrying the
    REPORTED quantity (chemical quantity+unit, or animal-traction days).

    df  : raw frame (numeric used/qty codes).
    dec : decoded frame (unit LABELS).
    specs : list of dicts; each must record a reported quantity so the row
    is an item, NOT a bare any-use flag:
        {input, qty, unit}            -> chemical quantity + decoded unit
        {input, qty_cols, u_label}    -> sum of numeric day/count columns
                                         carried with a fixed unit label
    A row is emitted only where a positive reported quantity exists
    (so rows are genuine reported items, never collapse-max use flags).
    """
    i = df[hhid].apply(format_id)
    plot_id = df[plot].apply(format_id)
    pieces = []
    for sp in specs:
        if sp.get('qty_cols'):
            cols = [c for c in sp['qty_cols'] if c in df.columns]
            if cols:
                q = (pd.concat([pd.to_numeric(df[c], errors='coerce')
                                for c in cols], axis=1)
                     .sum(axis=1, min_count=1).astype('Float64'))
            else:
                q = pd.Series(pd.NA, index=df.index, dtype='Float64')
            u = pd.Series(sp.get('u_label'), index=df.index, dtype='string')
        else:
            q = (pd.to_numeric(df[sp['qty']], errors='coerce').astype('Float64')
                 if sp.get('qty') in df.columns
                 else pd.Series(pd.NA, index=df.index, dtype='Float64'))
            if sp.get('unit') and sp['unit'] in dec.columns:
                u = dec[sp['unit']].map(_canon_unit).astype('string')
            else:
                u = pd.Series(pd.NA, index=df.index, dtype='string')
        q.index = df.index
        u.index = df.index
        # Emit ONLY where a positive reported quantity exists.
        emit = (q.fillna(0) > 0)
        piece = pd.DataFrame({
            'i': i.values, 'plot': plot_id.values, 'input': sp['input'],
            'crop': NO_CROP, 'Quantity': q.values, 'u': u.values,
            'Purchased': pd.Series(pd.NA, index=df.index, dtype='boolean').values,
            'Quantity_purchased': pd.Series(pd.NA, index=df.index, dtype='Float64').values,
            'Improved': pd.Series(pd.NA, index=df.index, dtype='boolean').values,
        }, index=df.index)
        piece = piece[emit.values]
        pieces.append(piece)
    if not pieces:
        return pd.DataFrame(columns=['i', 'plot', 'input', 'crop', 'Quantity',
                                     'u', 'Purchased', 'Quantity_purchased',
                                     'Improved'])
    out = pd.concat(pieces, ignore_index=True)
    out = out[out['i'].notna() & out['plot'].notna()]
    return out


def assemble_plot_inputs(t, parts):
    """Concatenate the per-module row frames for one wave, attach `t`,
    drop empties, set the (t, i, plot, input, crop) index and dedup."""
    parts = [p for p in parts if p is not None and len(p)]
    cols = ['i', 'plot', 'input', 'crop', 'Quantity', 'u', 'Purchased',
            'Quantity_purchased', 'Improved']
    if not parts:
        return pd.DataFrame(columns=cols).set_index(
            ['i', 'plot', 'input', 'crop'])
    out = pd.concat(parts, ignore_index=True)
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    out['t'] = t
    out['crop'] = out['crop'].fillna(NO_CROP).astype('string')
    out['input'] = out['input'].astype('string')
    # Dedup on the index grain (defensive: a handful of duplicate
    # (i, plot, input, crop) rows can survive the per-module collapse).
    out = out.sort_values(['i', 'plot', 'input', 'crop'])
    out = out.drop_duplicates(subset=['t', 'i', 'plot', 'input', 'crop'],
                              keep='first')
    out = out.set_index(['t', 'i', 'plot', 'input', 'crop']).sort_index()
    return out[['Quantity', 'u', 'Purchased', 'Quantity_purchased', 'Improved']]


# ---------------------------------------------------------------------
# livestock (GAP 4) -- item-level reported livestock holdings
# ---------------------------------------------------------------------
#
# Natural grain (t, i, animal): one row per species/herd a household
# reports owning, from the GHS-Panel livestock roster sect11i.  This is
# the PRE-collapse roster the WB code reads then throws away down to a
# single household engaged-in-livestock y/n binary (NGA_GHS1.do:992-998
# recodes s11iq1 then `collapse (max) livestock, by(hhid)`).  We keep the
# roster richer: per-animal head counts, acquisitions, sales, and the
# reported per-head reservation value.
#
# `animal` (index) is the harmonize_species Preferred Label, resolved
# in-script from the wave's native animal code (101--123, one stable
# scheme across all five waves), mirroring how crop_production resolves
# crops via harmonize_food.  No `v` level: livestock is in the framework
# `_no_v_join` set, so the grain is exactly (t, i, animal).
#
# Stores REPORTED item-level fields ONLY:
#   HeadCount      head owned/kept now (s11iq2 W1-W3; s11iq2a "kept" W4;
#                  s11iq2 "kept" W5 -- W4/W5 split kept vs owned-subset,
#                  we carry the primary kept/owned-now count)
#   HeadAcquired   head bought to raise this period (s11iq10 W1-W4;
#                  s11iq17 W5)
#   HeadSold       head sold alive this period (s11iq16 W1-W4; s11iq23 W5)
#   Value          reported reservation value of ONE head -- "if you sold
#                  one today, how much would you receive" (s11iq3 W1-W4;
#                  s11iq7 W5).  This is a per-head reported price, NOT a
#                  herd-value total and NOT the WB engaged binary; a herd
#                  value (HeadCount x Value) and TLU are transformations.
# NO TLU, NO herd-value total, NO engaged-in-livestock binary -- those
# are transformations over these rows (their binary = groupby.any()).
#
# Each wave maps to a single t = PP_QUARTER[wave]: sect11i is collected
# in the post-planting round only (matching plot_features / plot_inputs;
# hhid aligns via format_id).


def _species_labels():
    """{int animal_code: Preferred Label} from harmonize_species."""
    from lsms_library.local_tools import get_categorical_mapping
    raw = get_categorical_mapping(tablename='harmonize_species',
                                  idxvars='Code',
                                  **{'Preferred Label': 'Preferred Label'})
    out = {}
    for k, v in raw.items():
        try:
            ik = int(k)
        except (TypeError, ValueError):
            continue
        if pd.isna(v) or str(v).strip() in ('', '---'):
            continue
        out[ik] = str(v).strip()
    return out


def livestock_for_wave(t, df, animal_code, owned, head_now, head_acquired,
                       head_sold, value, species_labels, own_yes=1):
    """Assemble item-level livestock for one Nigeria GHS-Panel wave.

    Parameters
    ----------
    t : str
        PP-quarter wave id (e.g. '2010Q3'), used as the `t` index.
    df : DataFrame
        Raw livestock roster (convert_categoricals=False), one row per
        (household, animal).
    animal_code : str
        Column holding the native animal code (101-123).
    owned : str or None
        Ownership y/n column (s11iq1).  Rows kept only where this == own_yes
        (1).  None keeps every row (W1/W5 files are already filtered to
        owned rows).
    head_now, head_acquired, head_sold, value : str or None
        Reported column names for HeadCount / HeadAcquired / HeadSold /
        Value.  None -> that column is all-NA for the wave.
    species_labels : dict
        {int animal_code: Preferred Label} from harmonize_species.

    Returns
    -------
    DataFrame indexed by (t, i, animal) with reported numeric columns.
    Keeps a row when the animal label resolves AND the household reports
    something for it (any of HeadCount / HeadAcquired / HeadSold / Value
    non-missing-and-nonzero) so the roster grid's never-owned all-zero
    rows (W2-W4 enumerate every animal for every HH) do not bloat the
    feature.  A row with own==yes but all-zero quantities is still kept
    (the household engaged with that species).
    """
    n = len(df)
    i = df['hhid'].apply(format_id)
    code = pd.to_numeric(df[animal_code], errors='coerce').astype('Int64')
    animal = code.map(species_labels).astype('string')

    def num(col):
        if col is not None and col in df.columns:
            return pd.to_numeric(df[col], errors='coerce')
        return pd.Series(pd.NA, index=df.index, dtype='Float64')

    piece = pd.DataFrame({
        'i': i.values,
        'animal': animal.values,
        'HeadCount': num(head_now).astype('Float64').values,
        'HeadAcquired': num(head_acquired).astype('Float64').values,
        'HeadSold': num(head_sold).astype('Float64').values,
        'Value': num(value).astype('Float64').values,
    }, index=df.index)

    if owned is not None and owned in df.columns:
        own = pd.to_numeric(df[owned], errors='coerce')
        piece['_own'] = (own == own_yes).values
    else:
        # File already filtered to owned rows.
        piece['_own'] = True

    piece['t'] = t

    # Keep rows the HH actually engaged with: either flagged owned, or any
    # reported quantity is non-null and nonzero.  Drop the roster-grid
    # not-owned all-zero filler rows (W2-W4) and unresolved animal labels.
    qcols = ['HeadCount', 'HeadAcquired', 'HeadSold', 'Value']
    has_qty = pd.Series(False, index=piece.index)
    for c in qcols:
        v = piece[c]
        has_qty = has_qty | (v.notna() & (v != 0))
    keep = piece['animal'].notna() & piece['i'].notna() & (piece['_own'] | has_qty)
    out = piece[keep].copy()

    # Dedup on the index grain (defensive: a species code can appear twice
    # for one HH in a malformed roster); keep the first/largest record.
    out = out.sort_values(['i', 'animal', 'HeadCount'],
                          ascending=[True, True, False])
    out = out.drop_duplicates(subset=['t', 'i', 'animal'], keep='first')

    out = out.set_index(['t', 'i', 'animal']).sort_index()
    return out[['HeadCount', 'HeadAcquired', 'HeadSold', 'Value']]


# ---------------------------------------------------------------------
# food_coping (#332, Family B: coping-strategies / rCSI)
# ---------------------------------------------------------------------
#
# GHS section 9 ("Food Security") is a coping day-count battery, NOT an
# HFIAS occurrence/frequency scale.  Items s9q1a..s9q1i ask "HOW MANY
# DAYS [in the last 7] HAD TO..." for each coping strategy, coded 0-7.
# Collected in the post-planting round only (sect9_plantingwN.dta), so
# each wave maps to a single t = PP_QUARTER[wave].  The post-harvest
# round has no equivalent battery.  sect9b (W4+) is FIES and is wired
# separately (do not include here).
#
# Canonical rCSI strategy names (LessPreferred, LimitVariety,
# LimitPortion, ReduceMeals, RestrictAdults, BorrowFood) plus the
# survey-specific severe-coping items (NoFood, SleepHungry,
# WholeDayNoFood).  Order follows the questionnaire (s9q1a..s9q1i).
FOOD_COPING_ITEMS = {
    's9q1a': 'LessPreferred',     # rely on less preferred foods
    's9q1b': 'LimitVariety',      # limit variety of foods eaten
    's9q1c': 'LimitPortion',      # limit portion size at meal times
    's9q1d': 'ReduceMeals',       # reduce number of meals eaten
    's9q1e': 'RestrictAdults',    # restrict adult consump for children
    's9q1f': 'BorrowFood',        # borrow food / rely on help
    's9q1g': 'NoFood',            # no food of any kind in household
    's9q1h': 'SleepHungry',       # go to sleep hungry
    's9q1i': 'WholeDayNoFood',    # go a whole day & night without eating
}


def food_coping_for_wave(t, df, id_col='hhid', items=None):
    """Reshape a sect9 coping battery into long-form food_coping.

    Parameters
    ----------
    t : str
        The single post-planting quarter for this wave (e.g. '2010Q3').
    df : DataFrame
        Raw sect9_plantingwN data (one row per household), read with
        ``convert_categoricals=False`` so the day counts stay numeric.
    id_col : str
        Household id column (Nigeria GHS uses 'hhid').
    items : dict
        Mapping of source column -> Strategy name; defaults to
        ``FOOD_COPING_ITEMS``.

    Returns
    -------
    DataFrame indexed by (t, i, Strategy) with an integer ``Days``
    column (0-7).  Rows where the day count is missing are dropped; a
    household with all-missing items contributes no rows.
    """
    items = items or FOOD_COPING_ITEMS
    present = {src: name for src, name in items.items() if src in df.columns}
    if not present:
        raise ValueError(f"food_coping: none of {list(items)} in source for t={t}")

    sub = df[[id_col] + list(present)].copy()
    sub['i'] = sub[id_col].apply(format_id)
    sub = sub.drop(columns=[id_col]).rename(columns=present)

    long = sub.melt(id_vars='i', var_name='Strategy', value_name='Days')
    long['Days'] = pd.to_numeric(long['Days'], errors='coerce')
    long = long.dropna(subset=['Days'])
    long['Days'] = long['Days'].round().astype('Int64')
    long['t'] = t

    long = long.set_index(['t', 'i', 'Strategy']).sort_index()
    return long[['Days']]


HECTARES_PER_ACRE = 0.404686          # 1 acre  = 0.404686 ha
SQM_PER_HECTARE = 10000.0             # 1 ha    = 10,000 m^2

# Native area-unit codes -> descriptive labels (for AreaUnit) and the
# hectare conversion factor where a standard conversion exists.
# Non-standard local units (heaps/ridges/stands/plots, and W5's
# square-foot / football-field) have NO in-repo conversion factor, so
# Area for those rows comes from the GPS measurement only; where GPS is
# missing, Area is NaN and AreaUnit carries the native label.
_AREA_UNIT_LABEL = {
    1: 'heaps', 2: 'ridges', 3: 'stands', 4: 'plots',
    5: 'acres', 6: 'hectares', 7: 'square metres',
    8: 'square foot (100x100)', 9: 'square foot (100x50)',
    10: 'football field',
}
_AREA_UNIT_TO_HA = {        # convertible estimate units only
    5: HECTARES_PER_ACRE,
    6: 1.0,
    7: 1.0 / SQM_PER_HECTARE,
}


def _harmonize_acquire_table():
    """Return {(Wave, Code): Preferred Label} from harmonize_acquire in
    Nigeria/_/categorical_mapping.org.  Wave-keyed because the acquire
    code scheme evolves across waves (GH #167).  Codes absent from the
    table map to NaN (no silent default)."""
    from lsms_library.local_tools import df_from_orgfile

    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, 'categorical_mapping.org'),
        os.path.abspath(os.path.join('..', '..', '_',
                                     'categorical_mapping.org')),
        'categorical_mapping.org',
    ]
    orgfn = next((c for c in candidates if os.path.exists(c)), candidates[0])

    df = df_from_orgfile(orgfn, name='harmonize_acquire',
                         set_columns=True, to_numeric=True)
    out = {}
    for _, row in df.iterrows():
        wv = str(row['Wave']).strip()
        c = row['Code']
        try:
            c = int(c)
        except (TypeError, ValueError):
            pass
        lab = row.get('Preferred Label')
        if pd.isna(lab):
            continue
        out[(wv, c)] = str(lab).strip()
    return out


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a {int Code: Preferred Label} dict from a single-Code-keyed
    table in categorical_mapping.org (harmonize_soil /
    harmonize_tenure_system).  NaN labels -> pd.NA."""
    from lsms_library.local_tools import get_categorical_mapping

    raw = get_categorical_mapping(tablename=tablename, idxvars=key,
                                  **{value: value})
    out = {}
    for k, v in raw.items():
        try:
            int_k = int(k)
        except (TypeError, ValueError):
            int_k = k
        if pd.isna(v) or str(v).strip() in ('---', ''):
            out[int_k] = pd.NA
        else:
            out[int_k] = str(v).strip()
    return out


def _map_codes(series, code_map):
    """Map a numeric (raw-Stata-code) Series through ``code_map``.
    Returns a nullable string Series with NaN where the code is unmapped.
    Sources are loaded with convert_categoricals=False so the codes are
    integers."""
    if series is None:
        return None
    out = pd.to_numeric(series, errors='coerce').astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_features_for_wave(t, area, detail, colmap, geovar=None):
    """Build canonical ``plot_features`` for one Nigeria GHS-Panel wave.

    Nigeria is a post-planting / post-harvest survey; lasting plot
    attributes live only in the post-planting round, so each wave maps
    to a single ``t`` (the PP quarter).  The area question (sect11a1)
    and the plot-detail question (sect11b / sect11b1) are separate
    files joined on (hhid, plotid).

    Parameters
    ----------
    t : str
        PP-quarter wave id (e.g. ``'2010Q3'``), used as the ``t`` index.
    area : pd.DataFrame
        Raw sect11a1 (area) frame, loaded with
        ``get_dataframe(..., convert_categoricals=False)``.
    detail : pd.DataFrame | None
        Raw sect11b / sect11b1 (tenure / soil / irrigation) frame, same
        loading convention.  ``None`` permitted (none of the waves need
        it, but defensive).
    colmap : dict
        Per-wave column-name map.  Keys:
            hhid, plot_id                  (required, in `area`)
            area_est, area_unit, area_gps  (in `area`; area_gps in sqm)
            acquire                        (in `detail`; -> Tenure)
            tenure_system                  (in `detail`; W5 only)
            soil_type                      (in `detail`; W2+ only)
            irrigated                      (in `detail`; W2+ only)
            certificate                    (in `detail`; W2+ -> PlotCertificate)
            erosion                        (in `detail`; W3+ -> ErosionProtection)
            fallow                         (in `detail`; -> Fallow, code 1=fallow)
            fallow_cultivated              (in `detail`; W1 only override:
                                            value 1 forces Fallow=False)
            slope                          (in `geovar`; -> PlotSlope, degrees)
        Omitted / absent columns yield NaN for the corresponding output.
    geovar : pd.DataFrame | None
        Raw plot-geovariables frame (``nga_plotgeovariables_y*``), loaded
        with ``get_dataframe(..., convert_categoricals=False)``, joined on
        (hhid, plotid) to attach ``PlotSlope`` (``srtmslp_nga``).  Absent
        in W5 (no plot-geovariables file released) -> PlotSlope NaN.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares float), ``AreaUnit`` (native unit label),
        ``Tenure``, ``TenureSystem``, ``SoilType`` (str),
        ``Irrigated`` (nullable boolean), ``PlotCertificate`` (nullable
        boolean: holds a land-ownership certificate), ``ErosionProtection``
        (nullable boolean: erosion-control measure present), ``Fallow``
        (nullable boolean: plot left fallow this season) and ``PlotSlope``
        (float, SRTM-derived slope in degrees).  Latitude / Longitude are
        deferred (Nigeria has no decimal-degree parcel coordinates;
        only GPS area in m^2).  ``PlotCertificate`` / ``ErosionProtection``
        / ``Fallow`` are the *reported* per-plot item flags; the HH-level
        ``nb_fallow_plots`` count is a downstream transform, never stored.
    """
    c = colmap
    wave_label = wave_folder_map.get(t, t)   # PP quarter -> 'YYYY-YY'

    acquire_map = _harmonize_acquire_table()
    soil_map = _harmonized_codes('harmonize_soil')
    tenure_system_map = _harmonized_codes('harmonize_tenure_system')

    a = area.copy()
    hh = a[c['hhid']].apply(format_id)
    plot_id = a[c['plot_id']].apply(format_id)

    # --- Area (hectares) ---
    unit_code = (pd.to_numeric(a[c['area_unit']], errors='coerce').astype('Int64')
                 if c.get('area_unit') in a.columns
                 else pd.Series(pd.NA, index=a.index, dtype='Int64'))

    # GPS measurement (square metres) -> hectares.
    area_ha = pd.Series(pd.NA, index=a.index, dtype='Float64')
    if c.get('area_gps') in a.columns:
        gps = pd.to_numeric(a[c['area_gps']], errors='coerce').astype('Float64')
        area_ha = gps / SQM_PER_HECTARE

    # Fall back to the farmer estimate ONLY for convertible units
    # (acres / hectares / square metres).  Non-standard units have no
    # conversion factor -> leave Area NaN (GPS-only).
    if c.get('area_est') in a.columns:
        est = pd.to_numeric(a[c['area_est']], errors='coerce').astype('Float64')
        factor = unit_code.map(_AREA_UNIT_TO_HA).astype('Float64')
        est_ha = est * factor                 # NaN where unit non-convertible
        area_ha = area_ha.where(area_ha.notna(), est_ha)

    # Plausibility clamp: > 1000 ha is a data-entry error for a Nigerian
    # smallholder plot; drop to NaN.
    area_ha = area_ha.where((area_ha <= 1000) | area_ha.isna(), pd.NA)

    # AreaUnit: native reported unit label (documents the unit even where
    # Area is NaN for non-standard units / missing GPS).
    area_unit = unit_code.map(_AREA_UNIT_LABEL).astype('string')

    pieces = pd.DataFrame({
        'i': hh.values,
        'plot_id': plot_id.values,
        'Area': area_ha.values,
        'AreaUnit': area_unit.values,
    })

    # --- Detail columns joined on (hhid, plotid) ---
    tenure = pd.Series(pd.NA, index=a.index, dtype='string')
    tenure_system = pd.Series(pd.NA, index=a.index, dtype='string')
    soil_type = pd.Series(pd.NA, index=a.index, dtype='string')
    irrigated = pd.Series(pd.NA, index=a.index, dtype='boolean')

    def _yes_no_bool(series):
        """Map a 1=Yes / 2=No reported flag to nullable boolean.

        Anything else (3 = "don't know", sentinels, missing) -> NA.
        """
        num = pd.to_numeric(series, errors='coerce')
        out = pd.Series(pd.NA, index=series.index, dtype='boolean')
        out = out.where(~(num == 1), True)
        out = out.where(~(num == 2), False)
        return out

    if detail is not None and not detail.empty:
        d = detail.copy()
        d_hh = d[c['hhid']].apply(format_id)
        d_plot = d[c['plot_id']].apply(format_id)
        det = pd.DataFrame({'i': d_hh.values, 'plot_id': d_plot.values})

        if c.get('acquire') in d.columns:
            acq = pd.to_numeric(d[c['acquire']], errors='coerce').astype('Int64')
            det['Tenure'] = acq.map(
                lambda code: acquire_map.get((wave_label, int(code)))
                if pd.notna(code) else pd.NA).astype('string').values
        if c.get('tenure_system') in d.columns:
            det['TenureSystem'] = _map_codes(d[c['tenure_system']],
                                             tenure_system_map).values
        if c.get('soil_type') in d.columns:
            det['SoilType'] = _map_codes(d[c['soil_type']], soil_map).values
        if c.get('irrigated') in d.columns:
            irr = pd.to_numeric(d[c['irrigated']], errors='coerce')
            # 1 = Yes, 2 = No; 11 (.A sentinel) and anything else -> NaN.
            irr_bool = pd.Series(pd.NA, index=d.index, dtype='boolean')
            irr_bool = irr_bool.where(~(irr == 1), True)
            irr_bool = irr_bool.where(~(irr == 2), False)
            det['Irrigated'] = irr_bool.values

        # PlotCertificate: holds a land-ownership certificate (s11b1q7 /
        # s11b1q8).  1 = Yes, 2 = No, 3 ("don't know") -> NA.
        if c.get('certificate') in d.columns:
            det['PlotCertificate'] = _yes_no_bool(d[c['certificate']]).values

        # ErosionProtection: erosion-control measure on the plot (s11b1q49 /
        # s11b1q66).  1 = Yes, 2 = No.
        if c.get('erosion') in d.columns:
            det['ErosionProtection'] = _yes_no_bool(d[c['erosion']]).values

        # Fallow: plot left fallow this season.  Reported as a per-plot
        # item flag with code 1 = fallow (s11bq17 "left fallow" in W1;
        # s11b1q28 / s11b1q44 main-use code 1 in W2-W5).  Anything else
        # observed -> not fallow; missing -> NA.  In W1 a separate
        # "cultivated this plot?" question (s11bq16, value 1 = yes) takes
        # precedence and forces Fallow = False, matching the WB .do logic
        # (`replace fallow_plot = 0 if s11bq16 == 1`).
        if c.get('fallow') in d.columns:
            fal = pd.to_numeric(d[c['fallow']], errors='coerce')
            fal_bool = pd.Series(pd.NA, index=d.index, dtype='boolean')
            fal_bool = fal_bool.where(fal.isna(), False)   # observed -> False
            fal_bool = fal_bool.where(~(fal == 1), True)    # code 1 -> fallow
            if c.get('fallow_cultivated') in d.columns:
                cult = pd.to_numeric(d[c['fallow_cultivated']],
                                     errors='coerce')
                fal_bool = fal_bool.where(~(cult == 1), False)
            det['Fallow'] = fal_bool.values

        # Detail is unique on (i, plot_id); drop dup detail rows defensively.
        det = det.drop_duplicates(subset=['i', 'plot_id'])
        pieces = pieces.merge(det, on=['i', 'plot_id'], how='left')

    # --- PlotSlope from plot-geovariables (joined on (hhid, plotid)) ---
    if (geovar is not None and not geovar.empty
            and c.get('slope') in geovar.columns):
        g = geovar.copy()
        g_hh = g[c['hhid']].apply(format_id)
        g_plot = g[c['plot_id']].apply(format_id)
        slope = pd.to_numeric(g[c['slope']], errors='coerce').astype('Float64')
        gv = pd.DataFrame({'i': g_hh.values, 'plot_id': g_plot.values,
                           'PlotSlope': slope.values})
        gv = gv.drop_duplicates(subset=['i', 'plot_id'])
        pieces = pieces.merge(gv, on=['i', 'plot_id'], how='left')

    # Ensure all canonical columns exist.
    for col, dtype in (('Tenure', 'string'), ('TenureSystem', 'string'),
                       ('SoilType', 'string'), ('Irrigated', 'boolean'),
                       ('PlotCertificate', 'boolean'),
                       ('ErosionProtection', 'boolean'),
                       ('Fallow', 'boolean'), ('PlotSlope', 'Float64')):
        if col not in pieces.columns:
            pieces[col] = pd.Series(pd.NA, index=pieces.index, dtype=dtype)

    pieces['t'] = t
    pieces = pieces[['t', 'i', 'plot_id', 'Area', 'AreaUnit', 'Tenure',
                     'TenureSystem', 'SoilType', 'Irrigated',
                     'PlotCertificate', 'ErosionProtection', 'Fallow',
                     'PlotSlope']]
    pieces = pieces.set_index(['t', 'i', 'plot_id'])
    return pieces


# ---------------------------------------------------------------------
# anthropometry (GAP 5) -- item-level reported body measures
# ---------------------------------------------------------------------
#
# Natural grain (t, i, pid): one row per measured individual, carrying the
# REPORTED body measures the GHS-Panel anthropometry module collects --
# Weight (kg) and Height (cm) -- plus the individual's Sex and reported Age
# (years) so the row is self-describing for the downstream WHO-2006 z-score
# transform.  Source: the post-harvest anthropometry section
# (sect4a_harvest{wN} W1-W4; sect4b_harvest{w5} W5).  The WB code
# (NGA_GHS1.do:1296-1308) reads these same weight/height vars, merges sex +
# age from the roster, calls `zscore06` to produce haz06/waz06/whz06/bmiz06
# and a `wasting` flag, then keeps only the z-scores.  We keep ONLY the raw
# reported measures: the z-scores are a TRANSFORM (they require the WHO-2006
# reference population and child age-in-months), computed at query time,
# never stored.
#
# `Age_months` is NOT stored: the survey records reported age in YEARS; the
# WB derives age-in-months as (harvest-interview-month - birth-month), a
# transform.  The z-score helper derives months from Age + interview_date.
# MUAC (mid-upper-arm circumference) is NOT recorded in any Nigeria GHS wave
# -> no MUAC column.
#
# Anthropometry is collected in the post-harvest round only, so each wave
# maps to a single t = PH_QUARTER[wave] (2011Q1 / 2013Q1 / 2016Q1 / 2019Q1 /
# 2024Q1), matching the post-harvest slice of household_roster.  The
# individual key aligns exactly with household_roster: i = format_id(hhid),
# pid = str(int(indiv)) (verified 100% (i, pid) overlap on measured rows).
#
# Per-wave weight/height column map (verified against the .dta):
#   W1 2010-11  sect4a_harvestw1  Weight=s4aq52        Height=s4aq53
#   W2 2012-13  sect4a_harvestw2  Weight=s4aq52        Height=s4aq53
#   W3 2015-16  sect4a_harvestw3  Weight=s4aq52        Height=s4aq53
#   W4 2018-19  sect4a_harvestw4  Weight=median(s4aq52_1..3)  Height=median(s4aq53_1..3)
#   W5 2023-24  sect4b_harvestw5  Weight=median(s4bq8a..c)    Height=median(s4bq12a..c)
# W4/W5 took three readings per measure; the WB uses their row median
# (egen rowmedian) -- we mirror that (a measurement-error reduction, not an
# aggregation that crosses the item grain).


def _to_pid(indiv_series):
    """indiv -> canonical pid string ('1'), matching household_roster's pid
    (raw indiv as string).  NA where indiv is missing."""
    num = pd.to_numeric(indiv_series, errors='coerce').astype('Int64')
    return num.astype('string').where(num.notna(), pd.NA)


def _norm_sex(sex_series):
    """Normalize the GHS roster sex variable (s1q2) to canonical 'M' / 'F'.

    Emit the canonical Sex values directly (M / F), matching what
    household_roster surfaces at API time.  The Sex canonical-spelling map
    in data_info.yml is registered under household_roster only, so it is
    NOT applied to anthropometry by _enforce_canonical_spellings; emitting
    M / F here keeps anthropometry's Sex consistent with the roster without
    touching the shared data_info.yml.

    Read with convert_categoricals=False the column is the numeric code
    1 (male) / 2 (female), stable across all five waves; read with labels
    it is 'male' / 'MALE' / '1. MALE' etc.  Handle both: numeric 1/2 ->
    M/F; otherwise strip any 'n. NAME' prefix, titlecase, and map the
    leading letter.  Returns a 'string'-dtype Series, NA where unresolved."""
    num = pd.to_numeric(sex_series, errors='coerce')
    if num.notna().any():
        return num.map({1: 'M', 2: 'F'}).astype('string')
    label = (sex_series.astype('string')
             .str.split('. ').str[-1]
             .str.strip().str.title())
    return label.map({'Male': 'M', 'Female': 'F'}).astype('string')


def anthropometry_for_wave(t, anthro, roster, weight_cols, height_cols,
                           hhid='hhid', indiv='indiv', sex_col='s1q2',
                           age_col='s1q4'):
    """Assemble item-level anthropometry for one Nigeria GHS-Panel PH wave.

    Parameters
    ----------
    t : str
        PH-quarter wave id (e.g. '2011Q1'), used as the `t` index.
    anthro : DataFrame
        Raw post-harvest anthropometry section (convert_categoricals as the
        caller chose), one row per (household, individual).
    roster : DataFrame or None
        Raw post-harvest roster section (sect1_harvest{wN}) supplying Sex +
        reported Age, merged on (hhid, indiv).  None -> Sex/Age all-NA.
    weight_cols, height_cols : list of str
        One or more reported weight / height columns.  A single column is
        used as-is; multiple columns are reduced by their row median (the
        W4/W5 three-reading case, mirroring the WB egen rowmedian).
    hhid, indiv : str
        ID columns in `anthro` (and `roster`).
    sex_col, age_col : str
        Sex / reported-age columns in `roster`.

    Returns
    -------
    DataFrame indexed by (t, i, pid) with columns
    [Weight, Height, Sex, Age].  Keeps a row only where at least one of
    Weight / Height is reported (the module enumerates every household
    member; only measured members carry a body measure).  Stores REPORTED
    fields only -- no z-scores, no wasting/stunting (transforms).
    """
    def _median(df, cols):
        present = [c for c in cols if c in df.columns]
        if not present:
            return pd.Series(np.nan, index=df.index)
        block = df[present].apply(pd.to_numeric, errors='coerce')
        # Row median across the (up to three) readings; NaN where all NaN.
        return block.median(axis=1, skipna=True)

    i = anthro[hhid].apply(format_id)
    pid = _to_pid(anthro[indiv])
    weight = _median(anthro, weight_cols)
    height = _median(anthro, height_cols)

    piece = pd.DataFrame({
        'i': i.values,
        'pid': pid.values,
        'Weight': pd.to_numeric(weight, errors='coerce').astype('Float64').values,
        'Height': pd.to_numeric(height, errors='coerce').astype('Float64').values,
    }, index=anthro.index)

    # Merge Sex + reported Age from the roster on (i, pid).
    if roster is not None and sex_col in roster.columns:
        ri = roster[hhid].apply(format_id)
        rpid = _to_pid(roster[indiv])
        ros = pd.DataFrame({'i': ri.values, 'pid': rpid.values})
        ros['Sex'] = _norm_sex(roster[sex_col]).values
        if age_col in roster.columns:
            ros['Age'] = pd.to_numeric(roster[age_col],
                                       errors='coerce').astype('Float64').values
        else:
            ros['Age'] = pd.Series(pd.NA, index=roster.index, dtype='Float64').values
        ros = ros.dropna(subset=['i', 'pid']).drop_duplicates(subset=['i', 'pid'])
        piece = piece.merge(ros, on=['i', 'pid'], how='left')
    else:
        piece['Sex'] = pd.Series(pd.NA, index=piece.index, dtype='string')
        piece['Age'] = pd.Series(pd.NA, index=piece.index, dtype='Float64')

    piece['t'] = t

    # Keep measured individuals only: at least one of Weight / Height
    # reported, and a resolvable individual key.
    measured = piece['Weight'].notna() | piece['Height'].notna()
    keep = measured & piece['i'].notna() & piece['pid'].notna()
    out = piece[keep].copy()

    # Defensive dedup on the index grain (a member should appear once).
    out = out.drop_duplicates(subset=['t', 'i', 'pid'], keep='first')
    out = out.set_index(['t', 'i', 'pid']).sort_index()
    return out[['Weight', 'Height', 'Sex', 'Age']]


# ---------------------------------------------------------------------
# plot_labor (GAP 3, grain 2) -- item-level reported plot labor by source
# ---------------------------------------------------------------------
#
# Natural grain (t, i, plot, source): one row per labor SOURCE used on a
# plot, source in {family, hired, other}.  Source: the post-harvest plot
# labor roster (secta2_harvest{wN}; W4/W5 split it into secta2a family +
# secta2b hired/other).  This is the construct NGA_GHS{1..5}.do reads then
# collapses to the household totals total_labor_days / total_family_labor_
# days / total_hired_labor_days / hired_labor_value.  We keep the
# PRE-collapse per-(plot, source) rows:
#   PersonDays  reported person-days of that source on the plot.
#               family = Sigma over worker slots of (#workers * days each)
#                        (or days alone where #workers is missing -- mirrors
#                        the WB `replace hh_labordays = days if persons==.`).
#               hired  = Sigma over man/woman/child of (#hired * days each).
#               other  = Sigma over man/woman/child of reported free/exchange
#                        person-days.
#   Wage        cash paid to hired labor on the plot = Sigma over
#               man/woman/child of (reported daily wage * hired days).
#               NaN for family / other (no cash wage reported).
# NO total_labor_days / total_family_labor_days / total_hired_labor_days /
# hired_labor_value -- those are HH / cross-source SUM / median-wage
# transformations over these rows, NEVER stored here.
#
# `source` (index) carries a harmonize_labor_source Preferred Label
# (family / hired / other).  `v` auto-joins from sample() at API time
# (plot_labor is NOT in the framework `_no_v_join` set).  plot (= plotid,
# format_id) aligns with crop_production / plot_inputs on (t, i, plot)
# (post-harvest round; t = PH_QUARTER[wave], matching crop_production).
#
# PP-round plot labor (sect11c1_planting{wN}, W2-W5) is NOT included: it is
# a SECOND round of the same source on the same plot, and folding it onto
# the same (plot, source) row would require a PP+PH SUM (the forbidden
# total) while a separate round level is outside the GAP-3 grain.  Documented
# partial -- the PH roster is present and consistent for all five waves.

LABOR_FAMILY = 'family'
LABOR_HIRED = 'hired'
LABOR_OTHER = 'other'


def _num(df, col):
    """pd.to_numeric on df[col] if present, else an all-NaN float Series."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors='coerce')
    return pd.Series(np.nan, index=df.index, dtype='float64')


def _family_days_wide(df, slots=('a', 'b', 'c', 'd'),
                      id_t='sa2q1{let}1', n_t='sa2q1{let}2', d_t='sa2q1{let}3'):
    """Family person-days from the wide W1-W3 secta2 roster: Sigma over worker
    slots a-d of (#workers * days), using days alone where #workers is
    missing (mirrors NGA_GHS1.do:642-643).  NaN where no slot is filled."""
    total = pd.Series(0.0, index=df.index)
    any_slot = pd.Series(False, index=df.index)
    for let in slots:
        present = _num(df, id_t.format(let=let)).notna()
        n = _num(df, n_t.format(let=let))
        d = _num(df, d_t.format(let=let))
        slot = (n * d).where(n.notna(), d)
        total = total.add(slot.where(present, 0.0).fillna(0.0), fill_value=0.0)
        any_slot = any_slot | present
    return total.where(any_slot, np.nan)


def _mwc_days(df, n_cols, d_cols):
    """Sigma over (man, woman, child) of #persons * days.  n_cols / d_cols are
    3-tuples of (count, days) column names.  NaN where no group is reported."""
    total = pd.Series(0.0, index=df.index)
    any_grp = pd.Series(False, index=df.index)
    for nc, dc in zip(n_cols, d_cols):
        n = _num(df, nc)
        d = _num(df, dc)
        total = total.add((n * d).fillna(0.0), fill_value=0.0)
        any_grp = any_grp | n.notna()
    return total.where(any_grp, np.nan)


def _mwc_days_direct(df, d_cols):
    """Sigma over (man, woman, child) of reported person-days (each a single
    column, the W1-W3 `other` block sa2q12a/b/c).  NaN where none reported."""
    total = pd.Series(0.0, index=df.index)
    any_grp = pd.Series(False, index=df.index)
    for dc in d_cols:
        d = _num(df, dc)
        total = total.add(d.fillna(0.0), fill_value=0.0)
        any_grp = any_grp | d.notna()
    return total.where(any_grp, np.nan)


def _hired_cash(df, n_cols, d_cols, w_cols):
    """Cash paid to hired labor = Sigma over (man, woman, child) of
    (daily wage * hired person-days).  hired person-days for a group =
    #hired * days.  NaN where no group reports both a wage and days."""
    total = pd.Series(0.0, index=df.index)
    any_pay = pd.Series(False, index=df.index)
    for nc, dc, wc in zip(n_cols, d_cols, w_cols):
        n = _num(df, nc)
        d = _num(df, dc)
        w = _num(df, wc)
        days = n * d
        pay = days * w
        total = total.add(pay.fillna(0.0), fill_value=0.0)
        any_pay = any_pay | (days.notna() & w.notna())
    return total.where(any_pay, np.nan)


def _plot_labor_assemble(t, hhid, plot, family, hired, other, wage):
    """Build the (t, i, plot, source) frame from per-plot day/cash Series.

    family / hired / other : person-days Series aligned to the plot rows.
    wage : hired-labor cash Series (NaN for family/other rows).
    Returns DataFrame indexed by (t, i, plot, source) with PersonDays, Wage.
    """
    i = hhid.apply(format_id)
    p = plot.apply(format_id)
    rows = []
    for source, days in ((LABOR_FAMILY, family),
                         (LABOR_HIRED, hired),
                         (LABOR_OTHER, other)):
        piece = pd.DataFrame({
            'i': i.values,
            'plot': p.values,
            'source': source,
            'PersonDays': pd.to_numeric(days, errors='coerce').values,
        })
        if source == LABOR_HIRED:
            piece['Wage'] = pd.to_numeric(wage, errors='coerce').values
        else:
            piece['Wage'] = np.nan
        rows.append(piece)
    out = pd.concat(rows, ignore_index=True)
    out['t'] = t
    # Drop rows with no household / plot key or no reported person-days
    # (a source not used on the plot -> no item row).
    out = out[out['i'].notna() & out['plot'].notna() & out['PersonDays'].notna()]
    out = out.sort_values(['i', 'plot', 'source'])
    out = out.drop_duplicates(subset=['t', 'i', 'plot', 'source'], keep='first')
    out = out.set_index(['t', 'i', 'plot', 'source']).sort_index()
    return out[['PersonDays', 'Wage']]


def plot_labor_wide(t, df):
    """Assemble plot_labor for a W1-W3 wide secta2 PH labor roster
    (sa2q* variable scheme; one row per (hhid, plotid))."""
    family = _family_days_wide(df)
    hired = _mwc_days(df, ('sa2q2', 'sa2q5', 'sa2q8'),
                      ('sa2q3', 'sa2q6', 'sa2q9'))
    other = _mwc_days_direct(df, ('sa2q12a', 'sa2q12b', 'sa2q12c'))
    wage = _hired_cash(df, ('sa2q2', 'sa2q5', 'sa2q8'),
                       ('sa2q3', 'sa2q6', 'sa2q9'),
                       ('sa2q4', 'sa2q7', 'sa2q10'))
    return _plot_labor_assemble(t, df['hhid'], df['plotid'],
                                family, hired, other, wage)


def plot_labor_split(t, fam_df, hired_df, fam_days_col='sa2aq1b',
                     n_cols=('sa2bq2', 'sa2bq5', 'sa2bq8'),
                     d_cols=('sa2bq3', 'sa2bq6', 'sa2bq9'),
                     w_cols=('sa2bq4', 'sa2bq7', 'sa2bq10'),
                     other_n=('sa2bq14a', 'sa2bq14b', 'sa2bq14c'),
                     other_d=('sa2bq15a', 'sa2bq15b', 'sa2bq15c')):
    """Assemble plot_labor for the W4 split PH roster: a long family roster
    (secta2a, one row per family worker, days in `fam_days_col`) summed to
    plot level, joined to the plot-level hired/other roster (secta2b).
    Mirrors NGA_GHS4.do:788-838 (sa2aq*/sa2bq* scheme)."""
    fam = fam_df.copy()
    fam['_i'] = fam['hhid'].apply(format_id)
    fam['_plot'] = fam['plotid'].apply(format_id)
    fam['_days'] = pd.to_numeric(fam.get(fam_days_col), errors='coerce')
    fam_plot = (fam.groupby(['_i', '_plot'])['_days']
                .sum(min_count=1).rename('family').reset_index())

    h = hired_df.copy()
    h['_i'] = h['hhid'].apply(format_id)
    h['_plot'] = h['plotid'].apply(format_id)
    h['hired'] = _mwc_days(h, n_cols, d_cols).values
    h['other'] = _mwc_days(h, other_n, other_d).values
    h['wage'] = _hired_cash(h, n_cols, d_cols, w_cols).values
    h_plot = h[['_i', '_plot', 'hired', 'other', 'wage']].drop_duplicates(
        ['_i', '_plot'])

    merged = fam_plot.merge(h_plot, on=['_i', '_plot'], how='outer')
    hhid = merged['_i']
    plot = merged['_plot']
    # _i / _plot are already format_id-applied; pass through identity so
    # _plot_labor_assemble's format_id is a no-op on the canonical strings.
    family = merged['family']
    hired = merged.get('hired')
    other = merged.get('other')
    wage = merged.get('wage')
    rows = []
    for source, days in ((LABOR_FAMILY, family),
                         (LABOR_HIRED, hired),
                         (LABOR_OTHER, other)):
        piece = pd.DataFrame({
            'i': hhid.values,
            'plot': plot.values,
            'source': source,
            'PersonDays': pd.to_numeric(days, errors='coerce').values
            if days is not None else np.nan,
        })
        piece['Wage'] = (pd.to_numeric(wage, errors='coerce').values
                         if source == LABOR_HIRED and wage is not None else np.nan)
        rows.append(piece)
    out = pd.concat(rows, ignore_index=True)
    out['t'] = t
    out = out[out['i'].notna() & out['plot'].notna() & out['PersonDays'].notna()]
    out = out.sort_values(['i', 'plot', 'source'])
    out = out.drop_duplicates(subset=['t', 'i', 'plot', 'source'], keep='first')
    out = out.set_index(['t', 'i', 'plot', 'source']).sort_index()
    return out[['PersonDays', 'Wage']]


# ---------------------------------------------------------------------
# people_last7days (GAP 3, grain 1) -- individual 7-day activity
# ---------------------------------------------------------------------
#
# Natural grain (t, i, pid): one row per household member, with the
# reported last-7-days labor-activity items the WB code (NGA_GHS{1..5}.do
# labor section) builds:
#   farm_work / SOB_work / wage_work  0/1 did the member do farm work /
#                                     own-business work / wage work in the
#                                     last 7 days
#   farm_hrs / SB_hrs / wage_hrs      hours spent on each (last 7 days)
#   Industry                          harmonized industry of the member's
#                                     wage/main work (harmonize_industry
#                                     Preferred Label; <NA> for non-workers)
#   working_age                       0/1 member is of working age (the WB
#                                     `s3q1==1` / `s4aq1==1` filter)
# Reported per-individual ONLY -- no household rollups.  `v` auto-joins
# from sample() at API time (people_last7days is NOT in `_no_v_join`).
# `pid` matches household_roster's pid (raw indiv as string).
#
# Per-wave source (the labor module):
#   W1 2010-11  sect3_plantingw1   s3q* ; hours via job-type logic (s3q18/30)
#   W2 2012-13  sect3a_plantingw2  s3aq*; hours via s3aq18/31
#   W3 2015-16  sect3_plantingw3   s3q* ; hours direct s3q5b/6b/4b
#   W4 2018-19  sect3_plantingw4   s3q* ; hours direct s3q5b/6b/4b
#   W5 2023-24  sect4a_harvestw5   s4aq*; own scheme (post-harvest module)

# WB industry grouping over the section-3 industry code (s3q14, 1-14):
#   1 -> Agriculture; 2 -> Mining; 3-5 -> Manufacturing; 6 -> Construction;
#   7-14 -> Services.  (No Fishing branch in this code list.)  Registered in
#   harmonize_industry.  W5 uses ISIC-style s4aq41_code ranges instead.
def _industry_from_s3q14(code):
    c = pd.to_numeric(code, errors='coerce')
    out = pd.Series(pd.NA, index=c.index, dtype='string')
    out = out.mask(c == 1, 'Agriculture')
    out = out.mask(c == 2, 'Mining')
    out = out.mask(c.between(3, 5), 'Manufacturing')
    out = out.mask(c == 6, 'Construction')
    out = out.mask(c.between(7, 14), 'Services')
    return out


def _industry_from_isic(code):
    """W5 ISIC-style code (s4aq41_code) -> harmonized industry label,
    mirroring NGA_GHS5.do:1315-1320 ranges."""
    c = pd.to_numeric(code, errors='coerce')
    out = pd.Series(pd.NA, index=c.index, dtype='string')
    out = out.mask((c > 100) & (c < 300), 'Agriculture')
    out = out.mask((c > 300) & (c < 400), 'Fishing')
    out = out.mask((c > 500) & (c < 1000), 'Mining')
    out = out.mask((c >= 1010) & (c <= 4000), 'Manufacturing')
    out = out.mask((c >= 4100) & (c <= 4500), 'Construction')
    out = out.mask((c >= 4501) & (c <= 10000), 'Services')
    return out


def _dummy01(series, yes=1, no=2):
    """Recode a yes/no item to 1.0/0.0 (yes->1, no->0); NaN otherwise."""
    c = pd.to_numeric(series, errors='coerce')
    out = pd.Series(np.nan, index=c.index, dtype='float64')
    out = out.where(~(c == yes), 1.0)
    out = out.where(~(c == no), 0.0)
    return out


def people_last7days_from_s3q(t, df, hhid='hhid', indiv='indiv',
                              farm='s3q5', sob='s3q6', wage='s3q4',
                              working='s3q1', industry='s3q14',
                              hrs_mode='direct',
                              farm_hrs='s3q5b', sb_hrs='s3q6b', wage_hrs='s3q4b',
                              hour_job1='s3q18', hour_job2='s3q30'):
    """Build people_last7days for a section-3 (s3q*/s3aq*) labor module.

    hrs_mode='direct'  hours read straight from per-activity columns
                       (W3/W4 s3q5b/6b/4b).
    hrs_mode='joblogic' hours derived from job-1/job-2 totals (W1/W2):
                       a coarse fallback -- assign the reported job hours to
                       farm if the member did farm work, else to wage work,
                       so per-activity hours are at least non-degenerate.
    """
    farm_work = _dummy01(df[farm]) if farm in df.columns else pd.Series(np.nan, index=df.index)
    sob_work = _dummy01(df[sob]) if sob in df.columns else pd.Series(np.nan, index=df.index)
    wage_work = _dummy01(df[wage]) if wage in df.columns else pd.Series(np.nan, index=df.index)
    working_age = (pd.to_numeric(df[working], errors='coerce') == 1).astype('float64') \
        if working in df.columns else pd.Series(np.nan, index=df.index)

    if hrs_mode == 'direct':
        fh = _num(df, farm_hrs)
        sh = _num(df, sb_hrs)
        wh = _num(df, wage_hrs)
        # WB zeros the activity hours where the activity dummy is 'no'.
        fh = fh.where(~(farm_work == 0), 0.0)
        sh = sh.where(~(sob_work == 0), 0.0)
        wh = wh.where(~(wage_work == 0), 0.0)
    else:  # joblogic (W1/W2): coarse split of reported job hours
        h1 = _num(df, hour_job1)
        h2 = _num(df, hour_job2)
        tot = h1.add(h2, fill_value=0).where(h1.notna() | h2.notna(), np.nan)
        fh = tot.where(farm_work == 1, 0.0)
        wh = tot.where((wage_work == 1) & ~(farm_work == 1), 0.0)
        sh = tot.where((sob_work == 1) & ~(farm_work == 1) & ~(wage_work == 1), 0.0)

    ind = (_industry_from_s3q14(df[industry])
           if industry in df.columns
           else pd.Series(pd.NA, index=df.index, dtype='string'))
    # WB zeros activity items for non-working-age members; mirror by
    # blanking work dummies / hours / industry where working_age==0.
    notwa = (working_age == 0)
    for s in (farm_work, sob_work, wage_work):
        s.loc[notwa] = 0.0
    for s in (fh, sh, wh):
        s.loc[notwa] = 0.0
    ind = ind.mask(notwa, pd.NA)
    # Industry only meaningful for wage workers.
    ind = ind.where(wage_work == 1, pd.NA)

    out = pd.DataFrame({
        'i': df[hhid].apply(format_id).values,
        'pid': _to_pid(df[indiv]).values,
        'farm_work': farm_work.values,
        'SOB_work': sob_work.values,
        'wage_work': wage_work.values,
        'farm_hrs': fh.values,
        'SB_hrs': sh.values,
        'wage_hrs': wh.values,
        'Industry': ind.values,
        'working_age': working_age.values,
    })
    out['t'] = t
    out = out[out['i'].notna() & out['pid'].notna()]
    out = out.drop_duplicates(subset=['t', 'i', 'pid'], keep='first')
    out = out.set_index(['t', 'i', 'pid']).sort_index()
    return out[['farm_work', 'SOB_work', 'wage_work',
                'farm_hrs', 'SB_hrs', 'wage_hrs', 'Industry', 'working_age']]


def people_last7days_from_s4aq(t, df, hhid='hhid', indiv='indiv'):
    """Build people_last7days for the W5 post-harvest labor module
    (sect4a_harvestw5, s4aq* scheme), mirroring NGA_GHS5.do:1299-1337."""
    # farm_work = farmed for hh (s4aq10) OR worked on hh farm (s4aq11)
    fw1 = _dummy01(df['s4aq10']) if 's4aq10' in df.columns else pd.Series(np.nan, index=df.index)
    fw2 = _dummy01(df['s4aq11']) if 's4aq11' in df.columns else pd.Series(np.nan, index=df.index)
    farm_work = pd.Series(np.nan, index=df.index, dtype='float64')
    farm_work = farm_work.where(~((fw1 == 1) | (fw2 == 1)), 1.0)
    farm_work = farm_work.where(~(fw1 == 0), 0.0)
    sob_work = _dummy01(df['s4aq6']) if 's4aq6' in df.columns else pd.Series(np.nan, index=df.index)
    # wage_work: a non-zero occupation code (s4aq40_code), zeroed where no
    # wage work (s4aq32==2).
    occ = pd.to_numeric(df.get('s4aq40_code'), errors='coerce')
    wage_work = pd.Series(np.nan, index=df.index, dtype='float64')
    wage_work = wage_work.where(~(occ == 0), 0.0)
    wage_work = wage_work.where(~(occ > 0), 1.0)
    if 's4aq32' in df.columns:
        wage_work = wage_work.where(~(pd.to_numeric(df['s4aq32'], errors='coerce') == 2), 0.0)
    working_age = (pd.to_numeric(df['s4aq1'], errors='coerce') == 1).astype('float64') \
        if 's4aq1' in df.columns else pd.Series(np.nan, index=df.index)

    farm_hrs = _num(df, 's4aq12')
    sb_hrs = _num(df, 's4aq7')
    wage_hrs = _num(df, 's4aq5')
    if 's4aq11' in df.columns:
        farm_hrs = farm_hrs.where(~(pd.to_numeric(df['s4aq11'], errors='coerce') == 2), 0.0)
    if 's4aq6' in df.columns:
        sb_hrs = sb_hrs.where(~(pd.to_numeric(df['s4aq6'], errors='coerce') == 2), 0.0)
    if 's4aq4' in df.columns:
        wage_hrs = wage_hrs.where(~(pd.to_numeric(df['s4aq4'], errors='coerce') == 2), 0.0)

    ind = (_industry_from_isic(df['s4aq41_code'])
           if 's4aq41_code' in df.columns
           else pd.Series(pd.NA, index=df.index, dtype='string'))
    notwa = (working_age == 0)
    for s in (farm_work, sob_work, wage_work, farm_hrs, sb_hrs, wage_hrs):
        s.loc[notwa] = 0.0
    ind = ind.mask(notwa, pd.NA).where(wage_work == 1, pd.NA)

    out = pd.DataFrame({
        'i': df[hhid].apply(format_id).values,
        'pid': _to_pid(df[indiv]).values,
        'farm_work': farm_work.values,
        'SOB_work': sob_work.values,
        'wage_work': wage_work.values,
        'farm_hrs': farm_hrs.values,
        'SB_hrs': sb_hrs.values,
        'wage_hrs': wage_hrs.values,
        'Industry': ind.values,
        'working_age': working_age.values,
    })
    out['t'] = t
    out = out[out['i'].notna() & out['pid'].notna()]
    out = out.drop_duplicates(subset=['t', 'i', 'pid'], keep='first')
    out = out.set_index(['t', 'i', 'pid']).sort_index()
    return out[['farm_work', 'SOB_work', 'wage_work',
                'farm_hrs', 'SB_hrs', 'wage_hrs', 'Industry', 'working_age']]


# ---------------------------------------------------------------------
# community_prices (GAP C) -- item-level reported community food prices
# ---------------------------------------------------------------------
#
# Natural grain (t, v, j, u): one row per (cluster/EA x food-item x unit)
# from the post-harvest COMMUNITY questionnaire's food-price module
# (Section C8 "Food Prices", file sectc8_harvest{wN}; W3 splits it across
# sectc8a / sectc8b; W5 lives under Post Harvest Wave 5/Community/).  This
# is a CLUSTER-level table: there is no household i.  `v` is the native
# community-questionnaire EA id (`ea`), which maps to the SAME keyspace as
# sample().v (sample publishes the same composite cluster_id; GH #323), so community_prices.v
# joins households via their cluster.  Item-level REPORTED prices only --
# NOT an index, NOT a median/mean across clusters, NOT an HH-median
# imputation (those are transformations over these rows).
#
# Stores the REPORTED field only:
#   Price   the surveyed prevailing price of one unit `u` of item `j` in
#           cluster `v` at the present time (Naira).  W1/W2 sc8q2/c8q2;
#           W3-W5 c8q3 / c8aq3.
#
# Labels (the community-price arm of label unification):
#   j  -> the SHARED harmonize_food Preferred Label, so community_prices.j
#         joins food_acquired.j and crop_production.crop.  W3-W5 carry the
#         consumption-module item_cd scheme already registered in
#         harmonize_food (resolved via Code, exactly like food_acquired).
#         W1/W2 use the community questionnaire's OWN (disjoint) item-code
#         scheme -- mapped here by NAME to the canonical Preferred Label via
#         _W1W2_PRICE_ITEM, reusing existing food labels where the priced
#         good is a consumption-module food and adding only genuinely new
#         price-questionnaire prepared-food/drink items (Moi moi, Akara,
#         soups, Kunu, juices, ...).  Tobacco/cigarettes (non-food) dropped.
#   u  -> the SHARED `u` table base Preferred Label via _canon_unit.  W3-W5
#         record the unit per row (sc8q2/c8q2/c8aq2); W1/W2 do NOT carry a
#         unit column -- the questionnaire fixes one unit per item, so the
#         per-item unit is supplied from _W1W2_PRICE_UNIT (Section C8's
#         column heads: KG / PIECE / BUNCH / LITRE / TIN / BOTTLE / ...).
#
# t = PH_QUARTER[wave] (the price module sits in the post-harvest round,
# matching crop_production / anthropometry).


# W1/W2 community-price questionnaire (Section C8) item codes -> canonical
# harmonize_food Preferred Label.  The 4-digit data codes encode a size
# variant as `base*10 + size` (e.g. 1511 = item 151 size 1), so the build
# resolves a data code by trying the code itself, then `code // 10`.
_W1W2_PRICE_ITEM = {
    1: 'Guinea corn/sorghum', 2: 'Millet', 3: 'Maize', 4: 'Maize',
    5: 'Rice--local', 6: 'Rice--local', 7: 'Rice--imported', 8: 'Bread',
    9: 'Buns/Pofpof/Donuts', 10: 'Biscuits', 11: 'Maize flour',
    20: 'Millet flour', 21: 'Wheat flour', 22: 'Yam flour',
    23: 'Cassava flour', 24: 'Plantain flour', 30: 'Cassava--roots',
    31: 'Yam--roots', 32: 'Gari--white', 33: 'Gari--yellow', 34: 'Cocoyam',
    35: 'Plantains', 36: 'Sweet potatoes', 37: 'Potatoes',
    38: 'Fufu/Cassava dough (akpu)', 50: 'Brown beans', 51: 'Soya beans',
    52: 'Moi moi', 53: 'Akara (bean cake)', 54: 'Kulikuli (groundnut cake)',
    55: 'Locust bean', 60: 'Bambara nut', 61: 'Groundnuts (shelled)',
    62: 'Kola nut', 63: 'Palm kernel', 64: 'Cashew nut', 65: 'Coconut',
    70: 'Palm oil', 71: 'Coconut oil', 72: 'Sheabutter',
    73: 'Butter/Margarine', 74: 'Cheese (wara)', 75: 'Animal fat',
    76: 'Groundnut oil', 78: 'Butter/Margarine', 80: 'Bananas',
    81: 'Watermelon', 82: 'Orange/tangerine', 83: 'Mangoes', 84: 'Pawpaw',
    85: 'Avocado pear', 86: 'Pineapples', 87: 'Pineapple juice',
    88: 'Orange juice', 90: 'Fruit canned', 91: 'Fruit juice canned/Pack',
    101: 'Tomatoes', 102: 'Onions', 103: 'Garden eggs/egg plant',
    104: 'Okra--fresh', 105: 'Okra--dried', 106: 'Fresh Pepper',
    107: 'Cabbage', 108: 'Cucumber',
    109: 'Leaves (Cocoyam, Spinach, etc.)', 111: 'Tomato puree (canned)',
    120: 'Chicken', 121: 'Duck', 122: 'Other domestic poultry',
    123: 'Wild game meat', 125: 'Agricultural eggs', 126: 'Local eggs',
    130: 'Beef', 131: 'Mutton', 132: 'Pork', 133: 'Goat',
    134: 'Wild game meat', 136: 'Canned beef/corned beef',
    140: 'Fish--fresh', 141: 'Fish--frozen', 142: 'Fish--smoked',
    143: 'Fish--dried', 144: 'Fish--fresh', 145: 'Snails',
    146: 'Seafood (lobster, crab, prawns, etc)', 147: 'Canned fish/seafood',
    150: 'Fresh milk', 151: 'Milk powder', 152: 'Baby milk powder',
    153: 'Milk tinned (unsweetened)', 154: 'Fresh milk',
    155: 'Other milk products', 160: 'Coffee',
    161: 'Chocolate drinks (including Milo)', 162: 'Tea',
    170: 'Cooked rice and stew', 171: 'Fufu and soup', 172: 'Tuo and soup',
    173: 'Amala and soup', 174: 'Gari and soup',
    175: 'Pounded yam and soup', 180: 'Sugar', 181: 'Jams', 182: 'Honey',
    183: 'Other sweets and confectionary', 184: 'Ice cream',
    190: 'Condiments (salt, spices, pepper, etc)', 200: 'Bottled water',
    201: 'Sachet water', 202: 'Malt drinks',
    203: 'Soft drinks (Coca Cola, spirit, etc)', 204: 'Kunu',
    611: 'Other vegetables (fresh or canned)',
    612: 'Other vegetables (fresh or canned)',
    900: 'Beer (local and imported)', 901: 'Beer (local and imported)',
    903: 'Palm wine', 904: 'Pito', 905: 'Other alcoholic beverages',
    906: 'Gin',
}

# W1/W2 Section C8 per-item fixed unit (the column head on the printed
# questionnaire -- W1/W2 record no unit column, so the unit is the item's
# canonical sale unit).  Normalized to the shared `u` table base label.
_W1W2_PRICE_UNIT = {
    1: 'Kg', 2: 'Kg', 3: 'Kg', 4: 'Kg', 5: 'Kg', 6: 'Kg', 7: 'Kg',
    8: 'Piece', 9: 'Piece', 10: 'Piece', 11: 'Kg', 20: 'Kg', 21: 'Kg',
    22: 'Kg', 23: 'Kg', 24: 'Kg', 30: 'Piece', 31: 'Piece', 32: 'Kg',
    33: 'Kg', 34: 'Piece', 35: 'Piece', 36: 'Piece', 37: 'Piece', 38: 'Kg',
    50: 'Kg', 51: 'Kg', 52: 'Piece', 53: 'Piece', 54: 'Kg', 55: 'Kg',
    60: 'Kg', 61: 'Kg', 62: 'Kg', 63: 'Kg', 64: 'Kg', 65: 'Piece',
    70: 'l', 71: 'l', 72: 'Kg', 73: 'Kg', 74: 'Kg', 75: 'Kg', 76: 'l',
    78: 'Kg', 80: 'Bunch', 81: 'Piece', 82: 'Piece', 83: 'Piece',
    84: 'Piece', 85: 'Piece', 86: 'Piece', 87: 'Piece', 88: 'Piece',
    90: 'Piece', 91: 'Piece', 101: 'Kg', 102: 'Kg', 103: 'Kg', 104: 'Kg',
    105: 'Kg', 106: 'Kg', 107: 'Piece', 108: 'Piece', 109: 'Bunch',
    111: 'Piece', 120: 'Kg', 121: 'Kg', 122: 'Kg', 123: 'Kg',
    125: 'Crate', 126: 'Piece', 130: 'Kg', 131: 'Kg', 132: 'Kg',
    133: 'Kg', 134: 'Kg', 136: 'Bottle/Can', 140: 'Kg', 141: 'Kg',
    142: 'Kg', 143: 'Kg', 144: 'Kg', 145: 'Kg', 146: 'Kg',
    147: 'Bottle/Can', 150: 'l', 151: 'tin', 152: 'tin', 153: 'tin',
    154: 'Bottle/Can', 155: 'Bottle/Can', 160: 'Sachet', 161: 'tin',
    162: 'Piece', 170: 'Piece', 171: 'Piece', 172: 'Piece', 173: 'Piece',
    174: 'Piece', 175: 'Piece', 180: 'Wrap', 181: 'Bottle/Can',
    182: 'Bottle/Can', 183: 'Bowl', 184: 'l', 190: 'Wrap',
    200: 'Bottle/Can', 201: 'cl', 202: 'cl', 203: 'cl', 204: 'cl',
    611: 'Kg', 612: 'Kg', 900: 'Bottle/Can', 901: 'Bottle/Can',
    903: 'Bottle/Can', 904: 'Bottle/Can', 905: 'Bottle/Can',
    906: 'Bottle/Can',
}


def _resolve_w1w2_price_item(code):
    """W1/W2 community item code -> (Preferred Label, base unit).  Tries the
    code itself, then ``code // 10`` (4-digit codes are ``base*10+size``).
    Returns (None, None) for unmapped / non-food codes."""
    code = int(code)
    for c in (code, code // 10):
        if c in _W1W2_PRICE_ITEM:
            return _W1W2_PRICE_ITEM[c], _W1W2_PRICE_UNIT.get(c)
    return None, None


def community_prices_for_wave(t, frames, mode, crop_labels=None):
    """Assemble item-level community_prices for one Nigeria GHS-Panel wave.

    Parameters
    ----------
    t : str
        PH-quarter wave id (e.g. '2011Q1'), used as the `t` index.
    frames : list of dict
        Each dict describes one source price frame:
            df     raw DataFrame (convert_categoricals=False)
            dec    decoded-label DataFrame (for unit / item decode)
            ea     EA id column name (-> v = cluster_id(state, lga, ea))
            state  state column name (default 'state')
            lga    LGA column name   (default 'lga')
            item   item_cd column name
            price  reported-price column name
            unit   unit column name (W3-W5; None for W1/W2)
    mode : {'codes', 'names'}
        'codes' -> item_cd is the consumption-module scheme already in
                   harmonize_food (W3-W5): resolve via crop_labels (Code).
        'names' -> item_cd is the community questionnaire's own scheme
                   (W1/W2): resolve via _resolve_w1w2_price_item.
    crop_labels : dict, required for mode='codes'
        {int code: Preferred Label} from harmonize_food.

    Returns
    -------
    DataFrame indexed by (t, v, j, u) with the reported `Price` column.
    One row per (cluster, item, unit); the reported price is the MEDIAN of
    the cluster's reported prices when a cluster lists an item more than once
    at the same unit (e.g. sectc8a + sectc8b both list it) -- a within-row
    dedup of the SAME surveyed quantity, never a cross-cluster aggregate.
    """
    pieces = []
    for fr in frames:
        df = fr['df']
        dec = fr['dec']
        # v must land in the SAME keyspace as sample().v, or a cluster's prices
        # join no households at all.  That means the COMPOSITE (state/lga/ea)
        # id, not the bare `ea` serial (GH #323), and it means building it from
        # raw CODES: `df` here is read with convert_categoricals=False, and the
        # community questionnaire carries no value labels on state/lga anyway,
        # so a label-built key would not match the household side (measured: 0%
        # overlap in W2, 100% on codes).  A community record with no usable EA
        # yields v = None and is dropped by the notna() filter below.
        state_col = fr.get('state', 'state')
        lga_col = fr.get('lga', 'lga')
        v = df.apply(
            lambda r: cluster_id(r.get(state_col), r.get(lga_col), r[fr['ea']]),
            axis=1)
        code = pd.to_numeric(df[fr['item']], errors='coerce')
        price = pd.to_numeric(df[fr['price']], errors='coerce')

        if mode == 'codes':
            j = code.astype('Int64').map(crop_labels).astype('string')
            if fr.get('unit') and fr['unit'] in dec.columns:
                u = dec[fr['unit']].map(_canon_unit).astype('string')
            else:
                u = pd.Series(pd.NA, index=df.index, dtype='string')
        else:  # names (W1/W2)
            labs, units = [], []
            for c in code:
                if pd.isna(c):
                    labs.append(pd.NA)
                    units.append(pd.NA)
                    continue
                lab, un = _resolve_w1w2_price_item(c)
                labs.append(lab if lab is not None else pd.NA)
                units.append(un if un is not None else pd.NA)
            j = pd.Series(labs, index=df.index, dtype='string')
            u = pd.Series(units, index=df.index, dtype='string')

        piece = pd.DataFrame({
            'v': v.values,
            'j': j.values,
            'u': u.values,
            'Price': price.values,
        }, index=df.index)
        pieces.append(piece)

    out = pd.concat(pieces, ignore_index=True)
    out['t'] = t

    # Keep only rows with a resolved cluster, item, unit and a positive
    # reported price (price 0 / NaN = item not available / not priced).
    out = out[out['v'].notna() & out['j'].notna() & out['u'].notna()
              & out['Price'].notna() & (out['Price'] > 0)]

    # Collapse same-(cluster,item,unit) duplicates to one row.  A cluster can
    # list the same item/unit more than once (W3 sectc8a+sectc8b overlap;
    # multiple size variants mapping to one base unit) -- keep the FIRST reported
    # price (a genuine REPORTED surveyed value, consistent with every sibling
    # community_prices feature), NOT a computed median.  Within-(t,v,j,u) only,
    # never a cross-cluster aggregate.
    out = (out.groupby(['t', 'v', 'j', 'u'], as_index=False)['Price']
              .first())

    # community_prices is a CLUSTER table -- there is no household i, the
    # natural grain is (t, v, j, u).  But the framework's
    # local_tools.map_index() (run on EVERY read path) unconditionally swaps
    # j -> i whenever a `j` index level is present and an `i` level is NOT.
    # That swap would rename our item level `j` to `i`, drop `j`, and collapse
    # the table to (t, v, u).  To keep `j` intact WITHOUT touching the
    # framework, carry a redundant `i` level (set equal to the cluster v)
    # positioned BEFORE `j`: map_index then sees i present and j after i, so it
    # does NOT swap, and the framework's _normalize_dataframe_index drops the
    # undeclared `i` level (data_scheme declares (t, v, j, u)), leaving the
    # canonical (t, v, j, u) grain.  v already in the index means
    # _join_v_from_sample is skipped; the spurious i==v never reaches the API.
    # (Same framework-compatibility shim Tanzania/Malawi/Ethiopia/Mali
    # community_prices use.)
    out['i'] = out['v']
    out = out.set_index(['t', 'v', 'i', 'j', 'u']).sort_index()
    return out[['Price']]
