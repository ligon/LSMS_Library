
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

