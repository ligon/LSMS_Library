
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

