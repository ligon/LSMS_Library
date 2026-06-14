# Formatting  Functions for Mali
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value.iloc[0])+'0'+tools.format_id(value.iloc[1],zeropadding=2)

def pid(value):
    '''
    Formatting person id
    '''
    return tools.format_id(value.iloc[0])+'0'+tools.format_id(value.iloc[1],zeropadding=2)+'0'+tools.format_id(value.iloc[2],zeropadding=2)

def Sex(value):
    '''
    Formatting sex variable
    '''
    if pd.isna(value) or value == 'Manquant':
        return pd.NA
    else:
        return str(value).upper()[0]

def Age(value):
    '''
    Formatting age variable
    '''
    if pd.isna(value) or value == 'Manquant' or value == 'NSP':
        return pd.NA
    elif value =='95 ans & plus':
        return 95
    else:
        return int(value)

def Relationship(value):
    '''
    Formatting relationship variable
    '''
    if pd.isna(value) or value == 'Manquant':
        return pd.NA
    else:
        return value.title()

def Int_t(value):
    '''
    Formatting interview date
    '''   
    if pd.isna(value) or value == 'Manquant':
        return pd.NA
    else:
        return pd.to_datetime(value, errors='coerce').date()
def interview_date(df):
    df['Int_t'] = pd.to_datetime(df['Int_t'])
    return df


# ---------------------------------------------------------------------------
# plot_features (GH #167; EHCVM cluster)
# ---------------------------------------------------------------------------
#
# Mali is the EHCVM reference implementation.  Six more EHCVM countries
# (Niger, Senegal, Burkina_Faso, Benin, Togo, Guinea-Bissau) copy this
# pattern: a single per-wave agriculture-parcel file s16a_me_{iso}{year}
# with a uniform column scheme.  The shared harmonization lives here in
# ``plot_features_for_wave``; each wave's ``_/plot_features.py`` is a
# thin loader that hands the raw DataFrame plus a column map to it.


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a ``{int code -> Preferred Label}`` dict from
    ``categorical_mapping.org`` for one of the plot_features harmonize_*
    tables.  Codes whose Preferred Label is blank / '---' map to NA so
    the corresponding column stays NaN.  Mirrors Uganda's helper."""
    raw = tools.get_categorical_mapping(tablename=tablename, idxvars=key,
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
    """Map a numeric (raw Stata integer-code) Series through ``code_map``,
    returning a string Series with NA where the code is unmapped.  Source
    files must be loaded with ``convert_categoricals=False`` so the codes
    arrive as integers."""
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_features_for_wave(t, source, colmap):
    """Build canonical ``plot_features`` for one Mali EHCVM wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2018-19"``), used as the ``t`` index value.
    source : pd.DataFrame
        Raw s16a agriculture-parcel DataFrame, loaded via
        ``get_dataframe(..., convert_categoricals=False)`` so the
        harmonize_* tables can key on the integer codes.
    colmap : dict
        Column-name map.  Required keys::

            grappe        — cluster id column (with menage builds ``i``)
            menage        — within-cluster household number
            field_no      — within-HH field/champ sequence number
            parcel_no     — within-field parcel sequence number
            area_gps      — GPS-measured parcel area (hectares)
            gps_measured  — 1/2 (Oui/Non) flag for GPS measurement
            area_est      — farmer-estimated area (in area_est_unit)
            area_est_unit — 1=Hectare, 2=m^2
            tenure        — mode-of-tenure question  -> Tenure
            tenure_system — land-document question    -> TenureSystem
            soil_type     — soil-type question        -> SoilType
            water_source  — water-source question     -> Irrigated

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares float), ``AreaUnit`` (always 'hectares'),
        ``Tenure``, ``TenureSystem``, ``SoilType`` (str), and
        ``Irrigated`` (nullable bool).

    Notes
    -----
    * ``i`` is the EHCVM composite household id built with Mali's
      ``i()`` formatter so it matches ``sample().i`` natively.
    * ``plot_id = "{field_no}_{parcel_no}"`` — unique within
      ``(grappe, menage)`` (verified 0 collisions both waves).
    * ``Area`` prefers the GPS measurement (already hectares) where
      ``gps_measured == 1``; otherwise the farmer estimate converted to
      hectares (m^2 / 10000).  No GPS coordinate columns: EHCVM s16a has
      no decimal-degree parcel GPS, so Latitude / Longitude are deferred
      (as in Uganda).
    """
    tenure_map = _harmonized_codes('harmonize_tenure')
    tenure_system_map = _harmonized_codes('harmonize_tenure_system')
    soil_map = _harmonized_codes('harmonize_soil')
    water_map = _harmonized_codes('harmonize_water')

    c = colmap

    # Drop the s16a placeholder rows for non-farming households: one row
    # per household with NO field/parcel number and every plot attribute
    # blank (3286 of 9924 in Mali 2021-22; none in 2018-19).  Keeping
    # them would emit a content-free "nan_nan" plot_id per household and
    # collide once two such households id_walk to the same baseline id.
    src = source[source[c['field_no']].notna() & source[c['parcel_no']].notna()].copy()

    # Household id: EHCVM composite (grappe, menage) via mali.i().
    hh = src.apply(lambda r: i(pd.Series([r[c['grappe']], r[c['menage']]])),
                   axis=1)

    field = src[c['field_no']].apply(tools.format_id)
    parcel = src[c['parcel_no']].apply(tools.format_id)
    plot_id = field.astype(str) + '_' + parcel.astype(str)

    # Area in hectares.  GPS where measured, else farmer estimate
    # converted from its declared unit (1=Hectare ->x1, 2=m^2 ->/10000).
    area_gps = pd.to_numeric(src[c['area_gps']], errors='coerce').astype('Float64')
    gps_flag = src[c['gps_measured']].astype('Int64')

    est_raw = pd.to_numeric(src[c['area_est']], errors='coerce').astype('Float64')
    est_unit = src[c['area_est_unit']].astype('Int64')
    est_ha = est_raw.where(est_unit != 2, est_raw / 10000)

    # Prefer GPS where it was actually measured (flag == 1) and present.
    area_ha = est_ha.copy()
    use_gps = (gps_flag == 1) & area_gps.notna()
    area_ha = area_gps.where(use_gps, area_ha)

    # Plausibility clamp (GH #327): raw EHCVM s16a parcel areas carry
    # data-entry outliers many orders of magnitude too large against sane
    # medians of ~1 ha.  NaN out anything outside the plausible agronomic
    # range — above 1000 ha (a single smallholder parcel above this is an
    # error) or non-positive (zero / negative ha is impossible).  Rows are
    # kept; only the Area value is dropped.  The AreaUnit line below already
    # clears the unit wherever Area becomes NA.
    area_ha = area_ha.where(((area_ha > 0) & (area_ha <= 1000)) | area_ha.isna(), pd.NA)

    area_unit = pd.Series(['hectares'] * len(src), index=src.index, dtype='string')
    area_unit = area_unit.where(area_ha.notna(), pd.NA)

    tenure = _map_codes(src[c['tenure']], tenure_map) \
        if c.get('tenure') in src.columns else pd.Series(pd.NA, index=src.index, dtype='string')
    tenure_system = _map_codes(src[c['tenure_system']], tenure_system_map) \
        if c.get('tenure_system') in src.columns else pd.Series(pd.NA, index=src.index, dtype='string')
    soil_type = _map_codes(src[c['soil_type']], soil_map) \
        if c.get('soil_type') in src.columns else pd.Series(pd.NA, index=src.index, dtype='string')

    irrigated = pd.Series(pd.NA, index=src.index, dtype='boolean')
    if c.get('water_source') in src.columns:
        water_label = _map_codes(src[c['water_source']], water_map)
        irrigated = (water_label == 'Irrigated').astype('boolean')
        irrigated = irrigated.where(water_label.notna(), pd.NA)

    df = pd.DataFrame({
        't':            t,
        'i':            hh.values,
        'plot_id':      plot_id.values,
        'Area':         area_ha.values,
        'AreaUnit':     area_unit.values,
        'Tenure':       tenure.values,
        'TenureSystem': tenure_system.values,
        'SoilType':     soil_type.values,
        'Irrigated':    irrigated.values,
    })
    df = df.set_index(['t', 'i', 'plot_id'])
    return df

# ---------------------------------------------------------------------------
# crop_production (GAP 1; parity loop) — item-level (t, i, plot, crop)
# ---------------------------------------------------------------------------
#
# The crop/harvest module lives in the EACI waves only (2014-15 EACI14,
# 2017-18 EACI17).  The EHCVM waves (2018-19, 2021-22) carry no crop-harvest
# block, so crop_production is wired for the two EACI waves only — exactly
# the two waves the WB MLI_EACI*.do crop sections cover.
#
# Grain: one row per (t, i, plot, crop), where
#   - i     = Mali composite household id (grappe, menage/exploitation),
#   - plot  = "{field_no}_{parcel_no}" for seasonal crops; NaN for perennial
#             trees (the perennial roster s3b/s11f is not on the field grid),
#   - crop  = harmonize_food Preferred Label (food crops REUSE the consumed-
#             food label so crop_production.j joins food_acquired.j).
#
# REPORTED item-level columns only (no harvest_kg / yield / main_crop /
# value-share — those are transformations.py work over these rows):
#   Quantity, u, Quantity_sold, Value_sold,
#   planting_month, harvest_month, intercropped, perennial.
#
# Crop names and harvest units arrive as DECODED Stata labels (strings), so
# the harmonize_food / u tables key on the decoded label (Code column).

# French month name -> month number (2017-18 records month as a label).
_FR_MONTHS = {
    'janvier': 1, 'février': 2, 'fevrier': 2, 'mars': 3, 'avril': 4, 'mai': 5,
    'juin': 6, 'juillet': 7, 'août': 8, 'aout': 8, 'septembre': 9,
    'octobre': 10, 'novembre': 11, 'décembre': 12, 'decembre': 12,
}


def _crop_labels(series):
    """Map a Series of decoded crop names -> harmonize_food Preferred Label."""
    m = tools.get_categorical_mapping(tablename='harmonize_food', idxvars='Code',
                                      **{'Preferred Label': 'Preferred Label'})
    out = series.astype('string').str.strip().map(m)
    return out.astype('string')


def _unit_labels(series):
    """Map a Series of decoded harvest-unit names -> u Preferred Label."""
    m = tools.get_categorical_mapping(tablename='u', idxvars='Code',
                                      **{'Preferred Label': 'Preferred Label'})
    out = series.astype('string').str.strip().map(m)
    return out.astype('string')


def _month_num(series):
    """Coerce a month column (numeric code or French label) to 1-12 / NA."""
    def conv(v):
        if pd.isna(v):
            return pd.NA
        s = str(v).strip()
        if s in ('Manquant', 'Manqant', 'NSP', '99', '99.0', ''):
            return pd.NA
        try:
            n = int(float(s))
            return n if 1 <= n <= 12 else pd.NA
        except (TypeError, ValueError):
            return _FR_MONTHS.get(s.lower(), pd.NA)
    return series.map(conv).astype('Int64')


def crop_production_finalize(df):
    """Common post-processing for a crop_production wave DataFrame.

    Expects raw columns already assembled:
        t, i, plot, crop (decoded), u (decoded), Quantity, Quantity_sold,
        Value_sold, planting_month, harvest_month, intercropped, perennial.
    Maps crop/unit labels, coerces dtypes, sets the (t, i, plot, crop) index,
    and collapses exact-duplicate index rows (a crop reported on the same
    plot more than once in a single questionnaire row-set) by summing the
    reported quantities / values and taking the first of the flags/dates.
    """
    df = df.copy()
    df['crop'] = _crop_labels(df['crop'])
    df['u'] = _unit_labels(df['u'])
    df = df.dropna(subset=['crop'])  # drop "Non exploitée" / unmapped residual

    for c in ('Quantity', 'Quantity_sold', 'Value_sold'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    # 9999 / 99 are the EACI "Manquant" sentinels on the reported quantity
    # columns (cf. WB `harvest_kg=. if s3aq08a==9999`); coerce to NA so they
    # do not contaminate a downstream kg-conversion sum.
    for c in ('Quantity', 'Quantity_sold'):
        df.loc[df[c].isin([99, 9999]), c] = pd.NA
    df['planting_month'] = _month_num(df['planting_month'])
    df['harvest_month'] = _month_num(df['harvest_month'])
    df['intercropped'] = df['intercropped'].astype('boolean')
    df['perennial'] = df['perennial'].astype('boolean')

    # plot may be NA (perennial); fill index with <NA> string so the level is
    # not silently dropped, but keep it as a real pandas NA-able string.
    df['plot'] = df['plot'].astype('string')

    keys = ['t', 'i', 'plot', 'crop']
    # Aggregate any exact (t,i,plot,crop) duplicates: sum reported amounts,
    # first non-null for flags/dates/unit.  Use dropna=False so perennial
    # rows with plot=<NA> are not discarded by groupby.  min_count=1 on the
    # sums keeps an all-NA group as NA (not a spurious 0) — important for the
    # 2017-18 multi-plot crops whose sold qty/value is deliberately NaN.
    g = df.groupby(keys, dropna=False, as_index=True)
    out = pd.DataFrame({
        'Quantity':       g['Quantity'].sum(min_count=1),
        'Quantity_sold':  g['Quantity_sold'].sum(min_count=1),
        'Value_sold':     g['Value_sold'].sum(min_count=1),
        'u':              g['u'].first(),
        'planting_month': g['planting_month'].first(),
        'harvest_month':  g['harvest_month'].first(),
        'intercropped':   g['intercropped'].max(),
        'perennial':      g['perennial'].max(),
    })
    out = out[['Quantity', 'u', 'Quantity_sold', 'Value_sold',
               'planting_month', 'harvest_month', 'intercropped', 'perennial']]
    return out.sort_index()
