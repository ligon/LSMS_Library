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


# ---------------------------------------------------------------------------
# plot_inputs (GAP 2 — item-level agricultural inputs)
# ---------------------------------------------------------------------------
#
# One row per REPORTED agricultural input applied by a household in a wave:
# its harmonized input identity (Seed / Urea / DAP / NPK / Phosphate / Mixed
# fertilizer / Organic manure / Organic compost / Pesticide / Herbicide /
# Fungicide / Other phytosanitary), reported quantity used + native unit, the
# purchased flag + reported purchased quantity, and — for seed rows — the
# seed's crop (on the shared harmonize_food labels).  No aggregation: every
# reported input line is kept.  seed_kg, nitrogen_kg, inorganic_fertilizer
# any-use flags and fertilizer totals are TRANSFORMATIONS over these rows,
# never columns here (GAP-2 / item-level discipline).
#
# GRAIN = (t, i, input, crop, u).  NOTE: Niger's input modules are at the
# HOUSEHOLD x input grain (ECVMA additionally x crop), NOT plot x input —
# no Niger wave records inputs at the parcel level (the WB .do code that
# emits plot-level seed_kg / nitrogen_kg fabricates a plot allocation via
# indicator = plot_area / total_land_area, which the reported-only rule
# forbids).  So `plot` is NOT in the index; the feature name `plot_inputs`
# follows the GAP-2 ranking name, but the data is the reported HH-level
# input roster.  `crop` is in the index because the ECVMA input roster is
# per-(crop) for every input type, and several seed slots / crops can share
# the same (input, u); it is NaN for non-seed EHCVM rows (those modules
# carry no crop), accepted as a partly-null index level.
#
# Input instrument by wave (all four waves have one; all are item-level
# household input rosters, loaded convert_categoricals=True so the input /
# crop / unit labels arrive as the strings the harmonize_* tables key on):
#   2011-12  ecvmaas2c_p1  : (hid, crop, input-type) — as02cq02 input,
#                            as02cq04 crop, as02cq05a/b qty+unit,
#                            as02cq03 purchased(1/2), as02cq08a purchased qty.
#   2014-15  ECVMA2_AS02CP1: same layout, UPPERCASE columns (AS02CQ*),
#                            i from (GRAPPE, MENAGE).
#   2018-19  s16b_me_ner2018: (grappe, menage, input-type) — s16bq01 input
#                            (crop embedded in seed label -> harmonize_seed_crop),
#                            s16bq03a/b qty+unit, s16bq05 purchased(Oui/Non),
#                            s16bq07a purchased qty.  NO crop column.
#   2021-22  s16b_me_ner2021: same as 2018-19.


def _input_maps():
    """Load the harmonize_input (input-type) and u (unit) string->label
    maps, plus the harmonize_seed_crop map (EHCVM seed-label -> crop).
    Keyed on Original Label, like crop_production's _crop_maps."""
    input_map = tools.get_categorical_mapping(
        tablename='harmonize_input', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    unit_map = tools.get_categorical_mapping(
        tablename='u', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    seed_crop_map = tools.get_categorical_mapping(
        tablename='harmonize_seed_crop', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    return input_map, unit_map, seed_crop_map


def _input_labels(source_labels, input_map):
    """Map an input-type column (string labels from convert_categoricals=True)
    through harmonize_input.  Unmapped labels pass through unchanged (kept
    visible, flagged by the sanity checker)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: input_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _seed_crop_labels(source_labels, seed_crop_map):
    """For EHCVM waves: resolve the crop embedded in a seed-type label
    ('Semences de petit mil' -> 'Mil') via harmonize_seed_crop.  Returns NA
    for any label not in the seed-crop table (i.e. non-seed input rows)."""
    lab = source_labels.astype('string')
    return lab.map(lambda x: seed_crop_map.get(x, pd.NA) if pd.notna(x) else pd.NA).astype('string')


# Sentinel for the `crop` index level on input rows that are NOT
# crop-specific (every fertilizer / pesticide row, and any seed row whose
# crop could not be resolved).  WHY A SENTINEL, NOT NaN: the framework's
# canonical-index de-duplication (_finalize_result, country.py ~L3457) does
# groupby(level=...).first(), and pandas groupby drops rows whose grouping
# KEY is NaN — so a partly-null `crop` index level silently discards every
# non-seed input row.  A non-null token keeps all reported rows and makes
# "no crop dimension" explicit; '(not crop-specific)' is deliberately not a
# food/crop label so it never collides with the harmonize_food `crop`
# values that the seed rows carry.
CROP_NA = '(not crop-specific)'


def _finish_plot_inputs(df, t):
    """Common tail for the wave-level plot_inputs scripts: coerce numeric
    columns, guarantee the full schema column set, build the
    (t, i, input, crop, u) index.  Mirrors _finish_crop_production.

    Drops rows with no input identity (input NA) — a roster placeholder
    line, not a reported input.  The `crop` index level is filled with the
    CROP_NA sentinel wherever no crop applies (non-seed inputs, unresolved
    seed crops) so the index is fully non-null and no row is lost to the
    framework's NaN-key duplicate collapse.  The native unit `u` is filled
    likewise (a reported input may lack a unit), keeping `u` non-null."""
    for col in ['Quantity', 'Quantity_purchased']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
    if 'Purchased' not in df.columns:
        df['Purchased'] = pd.NA
    df['Purchased'] = df['Purchased'].astype('boolean')
    df['t'] = t
    df['input'] = df['input'].astype('string')
    if 'crop' not in df.columns:
        df['crop'] = pd.NA
    df['crop'] = df['crop'].astype('string').fillna(CROP_NA)
    # A reported input may lack a recorded unit (e.g. a count of bags); fill
    # with the existing 'Manquant' (=missing) `u` Preferred Label so the `u`
    # index level is non-null and survives the canonical de-dup collapse.
    df['u'] = df['u'].astype('string').fillna('Manquant')
    df = df[df['input'].notna()]
    keep = ['t', 'i', 'input', 'crop', 'u',
            'Quantity', 'Purchased', 'Quantity_purchased']
    df = df[[c for c in keep if c in df.columns]]
    df = df.set_index(['t', 'i', 'input', 'crop', 'u'])
    return df


# ---------------------------------------------------------------------------
# livestock (GAP 4 — item-level livestock at (t, i, animal))
# ---------------------------------------------------------------------------
#
# One row per REPORTED (household, species) the household owns/raised.  This
# is the raw livestock roster the WB .do code (NER_ECVMA1.do:1103-1108,
# NER_ECVMA2.do:1170-1177) reads and then THROWS AWAY down to a single
# engaged-in-livestock y/n binary (recode as4aq05 ... ; collapse (max)).  We
# keep the pre-collapse roster: head count owned now, head acquired/sold, and
# herd value where the source reports it.  The WB HH binary is recoverable as
# groupby(['t','i']).any() over these rows; TLU is a Σ(head × factor)
# transform — neither is a column here (item-level / reported-values rule).
#
# GRAIN = (t, i, animal): ONE row per (household, canonical species owned).
# NO `v`: 'livestock' is in the framework's _no_v_join set, so the
# household-level rows carry no cluster level and the framework does NOT join
# one from sample().  `animal` is the harmonized species Preferred Label.
#
# SUB-TYPE COLLAPSE (why the wave tail sums within (t, i, animal)).  The
# ECVMA roster lists each animal SUB-TYPE on its own line — bœuf, taureau,
# vache, taurillon, génisse and veau are six separate rows that all
# harmonize to the single species "Cattle" (likewise mouton/brebis ->
# Sheep, bouc/chèvre -> Goats, the three camel sub-types -> Camels, ...).
# Those sub-type lines are NOT independent species items: they are age/sex
# strata of one species the household owns.  So the per-species head count
# IS the natural item grain (a household owning 2 bœuf + 1 vache owns 3
# Cattle), and _finish_livestock SUMS HeadCount / HeadAcquired / HeadSold
# within (t, i, animal).  This is the species-level item, NOT a forbidden
# aggregation: TLU (cross-species Σ head×factor), a herd-value total, and
# the WB engaged-in-livestock binary are the transformations the GAP rule
# forbids as columns — all three are CROSS-species rollups, recovered by a
# transformations fn over these rows (the binary = groupby(['t','i']).any()).
# Summing age/sex strata of ONE species is not a cross-species rollup.  The
# EHCVM waves already report one row per species (codes 1-11), so the sum is
# a no-op there; it only merges the ECVMA sub-type lines.  After the sum the
# (t, i, animal) index is unique, so no rows are lost to the framework's
# canonical-index de-dup collapse (groupby().first()).
#
# Livestock instrument by wave (all four waves have one):
#   2011-12  ecvmaas4a_p2  : as4aq04 species code, as4aq05 owned(1/2) gate,
#                            as4aq11 head owned by HH, as4aq38 head 12mo ago,
#                            as4aq43 head bought, as4aq51 head sold on hoof,
#                            as4aq55 net sale value.  NO current herd value.
#   2014-15  ECVMA2_AS4AP2 : same layout, UPPERCASE columns (AS4AQ*); i from
#                            (GRAPPE, MENAGE, EXTENSION).
#   2018-19  s17_me_ner2018: s17q02 species code, s17q03 owned(1/2) gate (all
#                            rows ==1 — roster pre-filtered to owned species),
#                            s17q06 head owned by HH, s17q08 head bought,
#                            s17q10 head sold on hoof.  NO current herd value.
#   2021-22  s17_me_ner2021: same as 2018-19 EXCEPT the species code lives in
#                            s17q01 (s17q02 absent); 1-11 value scheme is
#                            identical.
#
# Columns by wave: HeadCount (owned now) and HeadSold are in every wave;
# HeadAcquired = number bought in the last 12 months (the single reported
# "acquired" flow, matching EHCVM s17q08 "Combien avez-vous achetés"; births
# and gifts are separate flows, not folded in).  Value = herd value where the
# source records it — NO Niger wave asks a current herd value, so Value is
# all-NaN and therefore OMITTED from the schema entirely (an all-NaN column
# would only trip the sanity checker; per the GAP rule "only include columns
# the source actually records").  The reported sale value (as4aq55) is a
# FLOW value, not a herd stock value, so it is deliberately not emitted as
# Value.


def _species_maps():
    """Load the two species code->Preferred Label dicts (ECVMA 3-digit
    codes, EHCVM 1-11 codes) from categorical_mapping.org.  Keyed on the
    integer Code, like harmonize_tenure / harmonize_soil — so the wave
    scripts load convert_categoricals=False and map the raw integer code."""
    ecvma = _harmonized_codes('harmonize_species_ecvma')
    ehcvm = _harmonized_codes('harmonize_species_ehcvm')
    return ecvma, ehcvm


def _finish_livestock(df, t):
    """Common tail for the wave-level livestock scripts: coerce numeric
    columns, drop unresolved-species placeholder rows, SUM the head counts
    within (t, i, animal) so each (household, canonical species) is one row,
    and build the (t, i, animal) index.  Mirrors _finish_crop_production /
    _finish_plot_inputs in shape, but unlike those it aggregates: the ECVMA
    roster's animal SUB-TYPE lines (bœuf/vache/... all -> Cattle) are
    age/sex strata of one species, so the per-species head count is the
    natural item grain (see module note).  HeadCount / HeadAcquired /
    HeadSold are Float64 (head counts, nullable); the sum uses min_count=1
    so an all-NaN group stays NaN rather than becoming 0.  Value is NOT a
    column: no Niger wave records a current herd value (see module note)."""
    cols = ['HeadCount', 'HeadAcquired', 'HeadSold']
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
    df['t'] = t
    df['animal'] = df['animal'].astype('string')
    # Drop rows with no resolved species (animal NA) — a roster placeholder
    # line, not a reported owned-animal record.  i must also be non-null for
    # a valid household key.
    df = df[df['animal'].notna() & df['i'].notna()]
    # Sum sub-type strata onto the canonical species (no-op for the EHCVM
    # waves, which already report one row per species).  min_count=1 keeps an
    # all-NaN group NaN.
    df = (df.groupby(['t', 'i', 'animal'], dropna=False)[cols]
            .sum(min_count=1)
            .reset_index())
    keep = ['t', 'i', 'animal'] + cols
    df = df[keep]
    df = df.set_index(['t', 'i', 'animal'])
    return df
