import pandas as pd
import numpy as np
import json
from ligonlibrary.dataframes import from_dta
import pyreadstat
import lsms_library.local_tools as tools


Waves = {'2011-12': (),
         '2014-15': (),
         '2018-19': (),
         '2021-22': ()}


def i(x):
    """Create hhid from grappe + menage with '0' separator + zero-padded menage.

    Detects which wave based on column case:
    - 2014-15 (ECVMA): uppercase columns (GRAPPE, MENAGE) -> no prefix
    - 2018-19/2021-22 (EHCVM): lowercase columns (grappe, menage) -> 'E_' prefix

    Uses '0' separator and zero-padded menage (2 digits) to prevent ID
    collisions (e.g., grappe=1,menage=23 vs grappe=12,menage=3).

    For scalar inputs (2011-12 hid), returns str(int(x)).
    """
    if isinstance(x, pd.Series):
        grappe = tools.format_id(x.iloc[0])
        menage = tools.format_id(x.iloc[1], zeropadding=2)
        if grappe is None or menage is None:
            return None

        # Check column names to detect which wave
        col_names = x.index.tolist()
        is_ehcvm = any(c.islower() for c in str(col_names[0]))

        if is_ehcvm:
            # 2018-19/2021-22 EHCVM: add prefix to prevent matching with ECVMA panel
            return 'E_' + grappe + '0' + menage
        else:
            # 2014-15 ECVMA: no prefix, may include EXTENSION
            if len(x) > 2:
                extension = str(int(x.iloc[2])) if pd.notna(x.iloc[2]) else '0'
                return grappe + '0' + menage + extension
            return grappe + '0' + menage
    return str(int(x))


def panel_ids(df):
    """Construct previous_i for Niger panel linkage.

    Handles two survey programs:
    - ECVMA (2014-15 -> 2011-12): previous_i = str(grappe*100 + menage)
      to match 2011-12's hid = grappe*100+menage format.
    - EHCVM (2021-22 -> 2018-19): previous_i = 'E_' + str(grappe) + str(menage)
      to match 2018-19's EHCVM composite ID format.

    For EHCVM waves, filter to panel households only (in_panel == 1).

    Note: Because ECVMA and EHCVM use different ID namespaces (no prefix vs 'E_'
    prefix), the two programs' panel linkage is independent even though
    local_tools.panel_ids() processes them sequentially.
    """
    if 'in_panel' in df.columns:
        # EHCVM wave (2021-22): filter to panel HHs with valid previous IDs
        # Must match i() format: 'E_' + format_id(grappe) + '0' + format_id(menage, zp=2)
        df = df[df['in_panel'] == 1]
        df = df[df['previous_grappe'].notna() & df['previous_menage'].notna()]
        grappe = df['previous_grappe'].apply(tools.format_id)
        menage = df['previous_menage'].apply(lambda x: tools.format_id(x, zeropadding=2))
        df['previous_i'] = 'E_' + grappe + '0' + menage
    else:
        # ECVMA wave (2014-15): previous_i matches 2011-12 hid = grappe*100+menage
        # 2011-12 uses scalar hid (not composite), so this format is correct as-is
        df = df[df['previous_grappe'].notna() & df['previous_menage'].notna()]
        df['previous_i'] = (
            (df['previous_grappe'].astype(float).astype(int) * 100
             + df['previous_menage'].astype(float).astype(int)).astype(str)
        )

    return df[['previous_i']]


def Age(value):
    '''
    Formatting age variable to numeric.
    '''
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def _household_roster_from_df(df, sex, age, HHID, sex_converter=None, age_converter=None,
                               months_spent='months_spent', Age_ints=None):
    """Inline replacement for lsms.tools.get_household_roster(fn_type=None)."""
    cols = [c for c in [HHID, sex, age, months_spent] if c in df.columns]
    df = df.loc[:, cols].rename(columns={HHID: 'HHID', sex: 'sex', age: 'age',
                                          months_spent: 'months_spent'})
    if sex_converter is not None:
        df['sex'] = df['sex'].apply(sex_converter)
    df = df.dropna(how='any')
    df['sex'] = df['sex'].apply(lambda s: str(s[0]).lower())
    if age_converter is not None:
        df['age'] = df['age'].apply(age_converter)
    df['boys']  = (df['sex'] == 'm') & (df['age'] < 18)
    df['girls'] = (df['sex'] == 'f') & (df['age'] < 18)
    df['men']   = (df['sex'] == 'm') & (df['age'] >= 18)
    df['women'] = (df['sex'] == 'f') & (df['age'] >= 18)
    if Age_ints is None:
        Age_ints = ((0,1),(1,5),(5,10),(10,15),(15,20),(20,30),(30,50),(50,60),(60,100))
    valvars = list({'HHID','girls','boys','men','women'}.intersection(df.columns))
    for lo, hi in Age_ints:
        s, e = lo, hi - 1
        df['Males %02d-%02d' % (s, e)]   = (df['sex'] == 'm') & (df['age'] >= lo) & (df['age'] < hi)
        df['Females %02d-%02d' % (s, e)] = (df['sex'] == 'f') & (df['age'] >= lo) & (df['age'] < hi)
        valvars += ['Males %02d-%02d' % (s, e), 'Females %02d-%02d' % (s, e)]
    try:
        if df['HHID'].iloc[0].split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError):
        pass
    if 'months_spent' in df.columns and df['months_spent'].count() > 0:
        g = df.loc[df['months_spent'] > 0, valvars].groupby('HHID')
    else:
        g = df[valvars].groupby('HHID')
    return g.sum()


def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = _household_roster_from_df(df, sex=sex, age=age, HHID=hhid,
                                       sex_converter=sex_converter,
                                       age_converter=age_converter,
                                       Age_ints=Age_ints)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf

def age_handler(df, interview_date = None, format_interv = None, age = None, dob = None, format_dob  = None, m = None, d = None, y = None, interview_year = None):
    '''
    a function to fill ages with the best available information for age, prioritizes more precise estimates

    Args:
        interview_date : column name containing interview date
        format_interv: argument to be passed into pd.to_datetime(, format=) for interview_date
        age : column name containing age in years
        dob: column name containing date of birth
        format_dob: to be passed into pd.to_datetime(, format=) for date of birth
        m, d, y: pass column names for month, day, and year respectively
        interview_year: column name containing year of interview; please enter an estimation in case an interview date is not found

    Returns:
    dataframe: mutates the dataframe to add an 'age' column and returns the dataframe
    '''

    if interview_date:
        df[interview_date] = pd.to_datetime(df[interview_date], format = format_interv)
    if dob:
        df[dob] = pd.to_datetime(df[dob], format = format_dob)

    def _safe_int(val):
        """Convert to int, returning None for Stata missing codes ('.')."""
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    def fill_func(x):
        if age and pd.notna(x[age]):
            v = _safe_int(x[age])
            return v if v is not None else pd.NA

        #conversion to pd.datetime obj of the date of birth if we are given mdy
        date_of_birth = None
        year_born = None
        if (m and d and y) and (x[[m, d, y]].notna().all()):
            mi, di, yi = _safe_int(x[m]), _safe_int(x[d]), _safe_int(x[y])
            if mi is not None and di is not None and yi is not None:
                date_conv = f'{mi}/{di}/{yi}'
                date_of_birth = pd.to_datetime(date_conv, format='%m/%d/%Y',
                                               errors='coerce')

        if dob and pd.notna(x[dob]):
            date_of_birth = x[dob]

        if pd.notna(date_of_birth):
            year_born = date_of_birth.year
            if interview_date and pd.notna(x[interview_date]):
                return (x[interview_date] - date_of_birth).days / 365.25

        elif (y and pd.notna(x[y])) or pd.notna(year_born):
            used_year = year_born or _safe_int(x[y])
            if used_year is None:
                return pd.NA
            if interview_date and pd.notna(x[interview_date]):
                return x[interview_date].year - used_year
            elif interview_year and pd.notna(x[interview_year]):
                iy = _safe_int(x[interview_year])
                return (iy - used_year) if iy is not None else pd.NA

        else:
            return pd.NA

    df['age'] = df.apply(fill_func, axis = 1)

    return df


# ---------------------------------------------------------------------------
# plot_features (GH #167; EHCVM cluster)
# ---------------------------------------------------------------------------
#
# Niger is an EHCVM sibling of the Mali reference implementation
# (PR #284).  The agriculture-parcel module s16a_me_ner{year}.dta uses
# the same uniform EHCVM column scheme.  The shared harmonization lives
# in ``plot_features_for_wave``; each wave's ``_/plot_features.py`` is a
# thin loader that hands the raw DataFrame plus a column map to it.
#
# NIGER-SPECIFIC delta: 2021-22 s16aq10 adds code 7 = Co-propriétaire,
# which Niger's harmonize_tenure maps to ``owned`` (2018-19 has codes
# 1-6 only, identical to Mali).
#
# i() above already produces the EHCVM composite id ('E_' + grappe + '0'
# + zero-padded menage) when handed a lowercase-named (grappe, menage)
# Series, matching sample().i natively.


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a ``{int code -> Preferred Label}`` dict from
    ``categorical_mapping.org`` for one of the plot_features harmonize_*
    tables.  Codes whose Preferred Label is blank / '---' map to NA so
    the corresponding column stays NaN.  Mirrors the Mali reference."""
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
    """Build canonical ``plot_features`` for one Niger EHCVM wave.

    See ``Mali/_/mali.py:plot_features_for_wave`` for the full contract
    (this is a direct sibling).  Returns a DataFrame indexed by
    ``(t, i, plot_id)`` with columns ``Area`` (hectares float),
    ``AreaUnit`` (always 'hectares'), ``Tenure``, ``TenureSystem``,
    ``SoilType`` (str), and ``Irrigated`` (nullable bool).

    Niger note: ``i`` is built with Niger's ``i()`` formatter from a
    lowercase-named (grappe, menage) Series so the EHCVM 'E_' prefix
    fires and the id matches ``sample().i``.  ``plot_id =
    "{field_no}_{parcel_no}"`` is unique within ``(grappe, menage)``.
    """
    tenure_map = _harmonized_codes('harmonize_tenure')
    tenure_system_map = _harmonized_codes('harmonize_tenure_system')
    soil_map = _harmonized_codes('harmonize_soil')
    water_map = _harmonized_codes('harmonize_water')

    c = colmap

    # Drop s16a placeholder rows for non-farming households: one row per
    # household with NO field/parcel number and every plot attribute
    # blank.  Keeping them would emit a content-free "nan_nan" plot_id
    # per household and collide once two such households id_walk to the
    # same baseline id (Mali had ~3286 such rows in 2021-22; Niger has
    # none in either wave, but the guard is kept for parity / safety).
    src = source[source[c['field_no']].notna() & source[c['parcel_no']].notna()].copy()

    # Household id: EHCVM composite (grappe, menage) via niger.i().  Build
    # a Series with the original lowercase column names as its index so
    # i()'s is_ehcvm detection fires and prefixes 'E_'.
    g_col, m_col = c['grappe'], c['menage']
    hh = src.apply(lambda r: i(pd.Series([r[g_col], r[m_col]],
                                         index=[g_col, m_col])),
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
# crop_production (GAP 1 — item-level crop harvest at (t, i, plot, crop, u))
# ---------------------------------------------------------------------------
#
# One row per REPORTED harvest record: a crop grown on a plot, with the
# reported harvest quantity in its native unit, plus reported sale
# quantity / sale value and the intercropped / perennial flags WHERE the
# instrument records them.  No aggregation — every reported line is kept;
# harvest_kg, yield, value-shares and main_crop are transformations, not
# columns here.
#
# Index = (t, i, plot, crop, u).  ``u`` is carried in the index (as in
# food_acquired) because a single (plot, crop) can be reported in more
# than one unit / harvest line, and collapsing those would be a sum.
# ``plot`` = "{field_no}_{parcel_no}" aligns with plot_features' plot_id.
#
# Crop instrument by wave:
#   2011-12  ecvmaas2e_p2  : plot-crop harvest + sold (qty/value) in one file.
#   2014-15  ECVMA2_AS2E1  : plot-crop harvest + harvest months; sold lives
#                            in ECVMA2_AS2E2 at CROP level (no plot) — joined
#                            to the plot-crop rows by (i, crop) where a
#                            household grows the crop on a single plot,
#                            else left NaN (cannot attribute to one plot).
#   2018-19  s16c          : plot-crop harvest + sold (qty/value) + intercrop.
#   2021-22  s16c (+s16d)  : plot-crop harvest + intercrop in s16c; sold
#                            (qty/value) in s16d at CROP level — joined the
#                            same single-plot way as 2014-15.
# perennial is NOT a separate flag in any Niger harvest module (the
# perennial-crop roster is a distinct instrument not wired here), so the
# ``perennial`` column is emitted all-NaN for parity with the schema.


def _crop_labels(source_codes, source_labels, crop_map):
    """Map a crop column (string labels from convert_categoricals=True)
    through ``harmonize_food`` (keyed on Original Label).  Unmapped labels
    pass through unchanged (kept visible, flagged by the sanity checker)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: crop_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _unit_labels(unit_series, unit_map):
    """Map a harvest-unit column (string labels) through the ``u`` table.
    Unmapped labels pass through unchanged."""
    u = unit_series.astype('string')
    return u.map(lambda x: unit_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _crop_maps():
    crop_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    unit_map = tools.get_categorical_mapping(
        tablename='u', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    return crop_map, unit_map


def _finish_crop_production(df, t):
    """Common tail: tag t, build the (t, i, plot, crop, u) index, coerce
    numeric columns, and guarantee the full schema column set."""
    for col in ['Quantity', 'Quantity_sold', 'Value_sold']:
        df[col] = pd.to_numeric(df.get(col), errors='coerce').astype('Float64')
    # harvest_month: only 2014-15 records it; NaN elsewhere (still carried
    # because >=1 wave has it).  planting_month / perennial are omitted from
    # the schema entirely — no Niger wave records them.
    if 'harvest_month' not in df.columns:
        df['harvest_month'] = pd.NA
    df['harvest_month'] = pd.to_numeric(df['harvest_month'], errors='coerce').astype('Float64')
    if 'intercropped' not in df.columns:
        df['intercropped'] = pd.NA
    df['intercropped'] = df['intercropped'].astype('boolean')
    df['t'] = t
    df['crop'] = df['crop'].astype('string')
    df['u'] = df['u'].astype('string')
    # Drop placeholder rows with no crop recorded (a parcel listed but no
    # crop grown / reported on the line).  These carry no harvest data
    # (crop, Quantity, unit all NA) and are not item-level harvest records.
    df = df[df['crop'].notna()]
    keep = ['t', 'i', 'plot', 'crop', 'u', 'Quantity',
            'Quantity_sold', 'Value_sold', 'harvest_month', 'intercropped']
    df = df[[c for c in keep if c in df.columns]]
    df = df.set_index(['t', 'i', 'plot', 'crop', 'u'])
    return df
