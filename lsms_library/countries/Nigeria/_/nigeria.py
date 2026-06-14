
import os
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


def _crop_labels():
    """{int cropcode: Preferred Label} from harmonize_food (shared with
    food_acquired)."""
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


def plot_features_for_wave(t, area, detail, colmap):
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
        Omitted / absent columns yield NaN for the corresponding output.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares float), ``AreaUnit`` (native unit label),
        ``Tenure``, ``TenureSystem``, ``SoilType`` (str) and
        ``Irrigated`` (nullable boolean).  Latitude / Longitude are
        deferred (Nigeria has no decimal-degree parcel coordinates;
        only GPS area in m^2).
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

        # Detail is unique on (i, plot_id); drop dup detail rows defensively.
        det = det.drop_duplicates(subset=['i', 'plot_id'])
        pieces = pieces.merge(det, on=['i', 'plot_id'], how='left')

    # Ensure all canonical columns exist.
    for col, dtype in (('Tenure', 'string'), ('TenureSystem', 'string'),
                       ('SoilType', 'string'), ('Irrigated', 'boolean')):
        if col not in pieces.columns:
            pieces[col] = pd.Series(pd.NA, index=pieces.index, dtype=dtype)

    pieces['t'] = t
    pieces = pieces[['t', 'i', 'plot_id', 'Area', 'AreaUnit', 'Tenure',
                     'TenureSystem', 'SoilType', 'Irrigated']]
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

