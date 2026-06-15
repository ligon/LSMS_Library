# Formatting Functions for Benin
import pandas as pd
import lsms_library.local_tools as tools


def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value.iloc[0])+tools.format_id(value.iloc[1],zeropadding=3)


# ---------------------------------------------------------------------------
# plot_features (GH #167; EHCVM cluster)
# ---------------------------------------------------------------------------
#
# Benin is an EHCVM sibling of the Mali reference implementation.  The
# single 2018-19 wave uses the s16a agriculture-parcel module with the
# uniform EHCVM column scheme.  The shared harmonization lives here in
# ``plot_features_for_wave``; the wave's ``_/plot_features.py`` is a thin
# loader that hands the raw DataFrame plus a column map to it.  ``i`` is
# the Benin EHCVM composite household id built with this module's ``i()``
# formatter so it matches ``sample().i`` natively.


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a ``{int code -> Preferred Label}`` dict from
    ``categorical_mapping.org`` for one of the plot_features harmonize_*
    tables.  Codes whose Preferred Label is blank / '---' map to NA so
    the corresponding column stays NaN.  Mirrors the Mali helper."""
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
    """Build canonical ``plot_features`` for one Benin EHCVM wave.

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
    * ``plot_id = "{field_no}_{parcel_no}"`` — unique within
      ``(grappe, menage)``.
    * ``Area`` prefers the GPS measurement (already hectares) where
      ``gps_measured == 1``; otherwise the farmer estimate converted to
      hectares (m^2 / 10000).  No GPS coordinate columns: EHCVM s16a has
      no decimal-degree parcel GPS, so Latitude / Longitude are deferred
      (as in Uganda / Mali).
    """
    tenure_map = _harmonized_codes('harmonize_tenure')
    tenure_system_map = _harmonized_codes('harmonize_tenure_system')
    soil_map = _harmonized_codes('harmonize_soil')
    water_map = _harmonized_codes('harmonize_water')

    c = colmap

    # Drop the s16a placeholder rows for non-farming households: rows with
    # NO field/parcel number and every plot attribute blank.  Keeping them
    # would emit a content-free "nan_nan" plot_id per household.
    src = source[source[c['field_no']].notna() & source[c['parcel_no']].notna()].copy()

    # Household id: EHCVM composite (grappe, menage) via benin.i().
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
    # data-entry outliers many orders of magnitude too large (e.g. Benin
    # max ~711,000 ha, Guinea-Bissau ~281,000,000 ha) against sane medians
    # of ~1 ha.  NaN out anything outside the plausible agronomic range —
    # above 1000 ha (a single smallholder parcel above this is an error) or
    # non-positive (zero / negative ha is impossible).  Rows are kept; only
    # the Area value is dropped.  The AreaUnit line below already clears the
    # unit wherever Area becomes NA.
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
