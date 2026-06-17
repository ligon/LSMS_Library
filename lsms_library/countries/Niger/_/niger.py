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
    ``AreaUnit`` (always 'hectares'), ``AreaSelfReported`` (reported
    farmer-estimate area in hectares, distinct from the GPS-preferred
    ``Area``), ``Tenure``, ``TenureSystem``, ``PlotCertificate``
    (nullable bool: has a formal land document), ``SoilType`` (str),
    ``SoilFertility`` (reported good/medium/poor str), and ``Irrigated``
    (nullable bool).

    The three reported item attributes added in the 2026-06-14 WB-parity
    audit (GAP 6) — ``AreaSelfReported`` (s16aq09a/b), ``PlotCertificate``
    (s16aq13), ``SoilFertility`` (s16aq20) — are emitted only when the
    matching ``colmap`` keys (``area_est``/``area_est_unit``,
    ``certificate``, ``fertility``) are supplied; otherwise they fall back
    to all-NA so the schema stays stable across waves that lack them.

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

    # AreaSelfReported (WB parity: area_self_reported / self_reported_area).
    # The REPORTED farmer-estimate parcel area (s16aq09a), in hectares,
    # carried distinct from ``Area`` (which prefers GPS where measured).
    # WB keeps the self-reported estimate as its own variable rather than
    # folding it into the GPS-preferred area; we mirror that here so the
    # reported estimate survives even on plots where GPS was used.  Same
    # unit conversion (1=Ha, 2=m^2 -> /10000) and plausibility clamp
    # (GH #327) as ``Area``.  ``est_ha`` was already computed above.
    area_self = est_ha.where(((est_ha > 0) & (est_ha <= 1000)) | est_ha.isna(), pd.NA)

    # PlotCertificate (WB parity: plot_certificate).  Reported bool from
    # s16aq13 "Avez-vous un document légal qui affirme votre possession de
    # cette parcelle?" — codes 1-5 are a formal land document (Titre
    # foncier / Permis d'exploiter / Procès-verbal / Bail / Convention de
    # vente) -> True; code 7 (Aucun) -> False; code 6 (Autre) and missing
    # -> NA.  This is a SECOND, complementary projection of s16aq13: the
    # existing ``TenureSystem`` maps only the three codes (1,2,4) that
    # imply a clear tenure regime, whereas WB's plot_certificate is the
    # binary has-any-document fact over the same question.  Not a transform
    # (no sum/count/PCA) — a direct reported recode of one source column.
    plot_certificate = pd.Series(pd.NA, index=src.index, dtype='boolean')
    if c.get('certificate') in src.columns:
        cert_code = src[c['certificate']].astype('Int64')
        has_doc = cert_code.isin([1, 2, 3, 4, 5])
        plot_certificate = has_doc.astype('boolean')
        # code 7 = Aucun -> False (already via isin); code 6 = Autre and
        # any missing -> NA (not a defensible yes/no).
        plot_certificate = plot_certificate.where(cert_code.isin([1, 2, 3, 4, 5, 7]), pd.NA)

    # SoilFertility (WB parity: a reported soil-quality tag; WB's
    # soil_fertility_index is a banned PCA transform, but s16aq20 records
    # the farmer's REPORTED fertility rating directly).  s16aq20 codes
    # 1=Bonne -> good, 2=Moyenne -> medium, 3=Faible -> poor.  Emitted as
    # a normalized English ordinal string; no parallel categorical table
    # (the three labels are identical across both EHCVM waves).
    fertility_map = {1: 'good', 2: 'medium', 3: 'poor'}
    soil_fertility = pd.Series(pd.NA, index=src.index, dtype='string')
    if c.get('fertility') in src.columns:
        soil_fertility = _map_codes(src[c['fertility']], fertility_map)

    df = pd.DataFrame({
        't':                t,
        'i':                hh.values,
        'plot_id':          plot_id.values,
        'Area':             area_ha.values,
        'AreaUnit':         area_unit.values,
        'AreaSelfReported': area_self.values,
        'Tenure':           tenure.values,
        'TenureSystem':     tenure_system.values,
        'PlotCertificate':  plot_certificate.values,
        'SoilType':         soil_type.values,
        'SoilFertility':    soil_fertility.values,
        'Irrigated':        irrigated.values,
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


# ---------------------------------------------------------------------------
# plot_labor (GAP 3 — item-level plot labor at (t, i, plot, source))
# ---------------------------------------------------------------------------
#
# One row per REPORTED labor SOURCE on a plot.  source in {family, hired,
# other} (the "other" = free / exchange labor exists only in the ECVMA
# waves; the EHCVM s16a parcel module records family vs non-family only, so
# it emits {family, hired}).  COLUMNS, reported values only:
#   PersonDays — reported person-days of that source on the plot.
#   Wage       — cash PAID to hired labor (where the survey records it);
#                NaN for family / other (no cash changes hands).
#
# This is the REPORTED person-day item, NOT the WB collapsed plot total.
# The WB .do (NER_ECVMA1.do:676-858, NER_ECVMA2.do:768-...) reads exactly
# these reported day cells, rowtotals them into PPtotal_/PHtotal_*_labor_days
# and finally total_labor_days / total_family_labor_days /
# total_hired_labor_days / hired_labor_value (a median-wage valuation).  ALL
# of those sums / valuations are TRANSFORMATIONS over these rows, never
# columns here (item-level / reported-values discipline):
#   total_labor_days        = Σ PersonDays over all sources on a plot
#   total_family_labor_days = Σ PersonDays where source=='family'
#   total_hired_labor_days  = Σ PersonDays where source=='hired'
#   hired_labor_value       = median-wage valuation over the hired rows
#
# WITHIN-SOURCE SUM is NOT a forbidden rollup (mirrors livestock summing
# age/sex strata).  The surveys report a labor source as several reported
# day cells — one per family member, or per hired-worker gender category,
# and across the post-planting (PP) and post-harvest (PH) seasons / phases.
# Those cells are strata of the SAME (plot, source) labor item, so the
# per-(plot, source) person-day total IS the natural item grain.  The
# forbidden rollups are the CROSS-source ones (total_labor_days) and the
# wage VALUATION — both recovered by a transformations fn over these rows.
#
# GRAIN = (t, i, plot, source).  `plot` = "{field_no}_{parcel_no}" aligns
# with crop_production's plot key and plot_features' plot_id (verified
# 100% on the EHCVM waves, ~78-92% on the ECVMA waves — labor is reported
# on plots that carry no harvest record, which is expected).  v is NOT baked
# in (household-linked; the framework joins it from sample()).
#
# Plot-labor instrument by wave:
#   2011-12  ecvmaas1_p1 (PP) + ecvmaas1_p2 (PH).  Family days
#            as02aq20b..25b (PP) / as02aq28b..33b,36b..41b (PH); hired
#            as02aq27{a/b-d/e} (PP) / as02aq35,43 (PH) with e=wage; other
#            as02aq26 (PP) / as02aq34,42 (PH).  Day sentinel 99/999, wage
#            sentinel 999999.  plot = concat(hid, as01q03, as01q05).
#   2014-15  ECVMA2_AS2AP1 (PP) + ECVMA2_AS2AP2 (PH).  Same scheme,
#            UPPERCASE; PP plot = (AS01Q01, AS01Q03), PH plot =
#            (AS02AQ01, AS02AQ03); i from (GRAPPE, MENAGE).
#   2018-19  s16a_me_ner2018 — family days s16aq33b_*/35b_*/37b_* (per
#            member, prep/maint/harvest); non-family (hired)
#            s16aq39/41/43 {a=#workers, b=days, c=wage} per gender category.
#            EHCVM has no "other" labor.  plot = (s16aq02, s16aq03).
#   2021-22  s16a_me_ner2021 — identical s16a labor-grid scheme.
#
# A wave with no plot-labor module is silently skipped by the country-level
# concatenator (no parquet emitted); see _/plot_labor.py.


# Canonical labor-source Preferred Labels (the `source` index level).  Tiny
# fixed vocabulary assigned in code (not read from a raw label column), so it
# is an inline mapping rather than a categorical_mapping table.
LABOR_SOURCE_FAMILY = 'family'
LABOR_SOURCE_HIRED = 'hired'
LABOR_SOURCE_OTHER = 'other'


def _coerce_days(series, sentinels=(99, 999)):
    """Numeric person-day cell -> Float64, with the survey's day sentinels
    (99 family / 999 hired-or-other 'no answer') mapped to NA."""
    s = pd.to_numeric(series, errors='coerce').astype('Float64')
    for sv in sentinels:
        s = s.where(s != sv, pd.NA)
    return s


def _coerce_wage(series, sentinel=999999):
    """Numeric wage cell -> Float64, with the 999999 'no answer' sentinel
    mapped to NA.  Used for the cash paid to hired labor."""
    s = pd.to_numeric(series, errors='coerce').astype('Float64')
    return s.where(s != sentinel, pd.NA)


def plot_labor_ehcvm(src, t):
    """Build plot_labor for one EHCVM wave (2018-19 / 2021-22) from its
    s16a parcel module.  EHCVM records family vs non-family (hired) plot
    labor only (no free / exchange 'other' source).  Shared by the two
    EHCVM wave scripts since the s16a labor-grid scheme is identical
    (member-day grids s16aq{33,35,37}b_*, hired grids s16aq{39,41,43}{a,b,c}_*;
    a = #workers, b = days, c = wage).  See the wave scripts' docstrings and
    the module note for the grain / column contract.

    Returns the long (i, plot, source, PersonDays, Wage) frame ready for
    _finish_plot_labor (which sums onto the (t, i, plot, source) grain)."""
    cols = list(src.columns)

    def _num(col):
        return pd.to_numeric(src[col], errors='coerce').astype('Float64')

    hh = src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                         index=['grappe', 'menage'])),
                   axis=1)
    field = src['s16aq02'].apply(tools.format_id)
    parcel = src['s16aq03'].apply(tools.format_id)
    plot = field.astype('string') + '_' + parcel.astype('string')

    # family: sum member-day cells across the three phase grids
    fam_cols = [c for c in cols
                if c.startswith('s16aq33b_')
                or c.startswith('s16aq35b_')
                or c.startswith('s16aq37b_')]
    fam_days = sum(_num(c).fillna(0) for c in fam_cols)
    fam_any = pd.concat([_num(c).notna() for c in fam_cols], axis=1).any(axis=1)
    fam_days = fam_days.where(fam_any.values, pd.NA)
    fam = pd.DataFrame({
        'i': hh.values, 'plot': plot.values,
        'source': LABOR_SOURCE_FAMILY,
        'PersonDays': fam_days.values, 'Wage': pd.NA,
    })

    # hired: Σ(#workers × days) person-days, Σ wage, over phase × category
    hired_days = pd.Series(0.0, index=src.index, dtype='Float64')
    hired_wage = pd.Series(0.0, index=src.index, dtype='Float64')
    any_days = pd.Series(False, index=src.index)
    any_wage = pd.Series(False, index=src.index)
    for base in ['s16aq39', 's16aq41', 's16aq43']:
        for ac in [c for c in cols if c.startswith(base + 'a_')]:
            suf = ac[len(base + 'a'):]   # e.g. '_1' (keeps the underscore)
            bc, cc = base + 'b' + suf, base + 'c' + suf
            workers = _num(ac)
            days = _num(bc) if bc in cols else pd.Series(pd.NA, index=src.index, dtype='Float64')
            wage = _num(cc) if cc in cols else pd.Series(pd.NA, index=src.index, dtype='Float64')
            pd_cell = workers * days
            hired_days = hired_days.add(pd_cell.fillna(0))
            any_days = any_days | pd_cell.notna().values
            hired_wage = hired_wage.add(wage.fillna(0))
            any_wage = any_wage | wage.notna().values
    hired_days = hired_days.where(any_days.values, pd.NA)
    hired_wage = hired_wage.where(any_wage.values, pd.NA)
    hired = pd.DataFrame({
        'i': hh.values, 'plot': plot.values,
        'source': LABOR_SOURCE_HIRED,
        'PersonDays': hired_days.values, 'Wage': hired_wage.values,
    })

    return pd.concat([fam, hired], ignore_index=True)


def _finish_plot_labor(df, t):
    """Common tail for the wave-level plot_labor scripts.

    `df` arrives with columns [i, plot, source, PersonDays, Wage] — one row
    per (plot, source) labor cell already reduced to a person-day count and
    (for hired) a wage.  This SUMS PersonDays / Wage within
    (t, i, plot, source) so the strata (family members, hired-worker gender
    categories, PP/PH seasons) collapse onto the natural (plot, source) item
    grain (see module note — a within-source sum, NOT a cross-source rollup).
    min_count=1 keeps an all-NA group NA rather than 0.  Rows with no plot,
    no source, or no person-days at all are dropped (roster placeholders, not
    reported labor)."""
    df = df.copy()
    df['t'] = t
    df['source'] = df['source'].astype('string')
    df['plot'] = df['plot'].astype('string')
    df['PersonDays'] = pd.to_numeric(df.get('PersonDays'), errors='coerce').astype('Float64')
    if 'Wage' not in df.columns:
        df['Wage'] = pd.NA
    df['Wage'] = pd.to_numeric(df['Wage'], errors='coerce').astype('Float64')
    df = df[df['i'].notna() & df['plot'].notna() & df['source'].notna()]
    df = (df.groupby(['t', 'i', 'plot', 'source'], dropna=False)[['PersonDays', 'Wage']]
            .sum(min_count=1)
            .reset_index())
    # Drop (plot, source) rows that carry no reported labor at all (both
    # PersonDays and Wage NA) — a survey skip, not a reported labor item.
    df = df[df['PersonDays'].notna() | df['Wage'].notna()]
    df = df.set_index(['t', 'i', 'plot', 'source'])
    return df


# ---------------------------------------------------------------------------
# people_last7days (GAP 3 — individual 7-day activity at (t, i, pid))
# ---------------------------------------------------------------------------
#
# One row per individual, carrying the REPORTED 7-day (last-week) labor
# participation the survey records.  Mirrors the target schema in the GAP
# ranking (the per-individual activity feature; Uganda's existing
# `people_last7days` is a legacy HH-level Men/Women/Boys/Girls count and is a
# DIFFERENT, older construct — this is the (t, i, pid) individual feature the
# 6 new countries are meant to gain).  COLUMNS, reported per-individual:
#   farm_work  — worked on own farm/garden/livestock in the last 7 days (bool)
#   SOB_work   — worked in own business / commerce in the last 7 days (bool)
#   wage_work  — worked for a wage / employer in the last 7 days (bool)
#   farm_hrs   — usual weekly hours on farm work (float; ECVMA only)
#   SB_hrs     — usual weekly hours in own business (float; ECVMA only)
#   wage_hrs   — usual weekly hours in wage work (float; ECVMA only)
#   Industry   — broad industry of the (main) job: Agriculture / Fishing /
#                Mining / Manufacturing / Construction / Services (str;
#                ECVMA only — derived from the WB code's section-code ranges)
#   working_age— member is of working age (Age >= 6, the survey threshold)
#
# NO rollups (the WB nb_members_working_age HH total is a transformation, not
# a column here).  Industry is stored as ONE harmonized label rather than the
# WB's six ind_* dummies (a wide encoding of one categorical); the ranges
# come straight from the WB .do (NER_ECVMA1.do:1386-1391) and do not vary
# across waves, so the mapping lives in code (no per-wave label variation to
# harmonize via a categorical table).
#
# 7-day instrument by wave:
#   2011-12  ecvmaind_p1p2 — ms04q03/05/02 (farm/SOB/wage work, 1=Oui),
#            ms04q24 industry section code, ms04q29-31/55-57 month/day/hour
#            per job (av weekly hours = month*day*hour/52), ms04q23/51 the
#            job's occupation code -> farm/SB/wage job classifier, ms01q06a
#            age (working_age = age>=6).  ID = hid-ms01q00.
#   2014-15  ECVMA2_MS04P1 merged to ECVMA2_MS01P1 — MS04Q01/02/03
#            (farm/SOB/wage), MS04Q23 industry, MS04Q25-28/51-53 + week
#            (av weekly hours = month*week*day*hour/52), MS01Q06A age.
#            ID = hhid-MS01Q00 with i from (GRAPPE, MENAGE).
#   2018-19  s04_me_ner2018 — 7-day dummies s04q06 (own farm/garden/
#            livestock/fishing -> farm_work), s04q07 (paid own account ->
#            SOB_work), s04q08 (paid employee/State/employer -> wage_work).
#            The EHCVM 7-day module records hours only for UNPAID domestic
#            tasks (q01-05), not for productive work in an ECVMA-comparable
#            per-activity form, and the industry is recorded as 12-month
#            occupation codes, not the WB 7-day section ranges — so
#            farm_hrs/SB_hrs/wage_hrs and Industry are NA in EHCVM (declared
#            for cross-wave schema parity).  working_age from s01 Age>=6.
#            pid = s01q00a (matches household_roster).
#   2021-22  s04a_me_ner2021 — same s04 7-day dummy scheme (s04q06/07/08).
#
# The two ECVMA waves carry the full schema; the EHCVM waves carry the
# dummies + working_age and leave hours / Industry NA.  Every wave has a
# 7-day labor module, so all four are wired.


# WB industry section-code ranges (NER_ECVMA1.do:1386-1391 / ECVMA2:1409-14).
# The code is a section of the national activity classification; the WB
# buckets it into six broad industries.  Stored as one `Industry` label.
def _industry_label(code_series):
    """Map a numeric activity-section code to a broad Industry Preferred
    Label using the WB .do ranges.  Returns NA where the code is missing /
    out of every range (e.g. the not-working / no-job sentinel)."""
    c = pd.to_numeric(code_series, errors='coerce')
    out = pd.Series(pd.NA, index=c.index, dtype='string')
    out = out.mask((c >= 11) & (c <= 40), 'Agriculture')
    out = out.mask((c == 51) | (c == 52), 'Fishing')
    out = out.mask((c >= 60) & (c <= 72), 'Mining')
    out = out.mask((c >= 81) & (c <= 292), 'Manufacturing')
    out = out.mask((c >= 301) & (c <= 302), 'Construction')
    out = out.mask((c >= 310) & (c <= 430), 'Services')
    return out


def _yn_bool(series, yes=1, no=2):
    """Map a 1=Oui / 2=Non survey item to nullable boolean (other codes,
    e.g. 9 'no answer', -> NA)."""
    s = pd.to_numeric(series, errors='coerce')
    out = pd.Series(pd.NA, index=s.index, dtype='boolean')
    out = out.mask(s == yes, True)
    out = out.mask(s == no, False)
    return out


def people_last7days_ehcvm(s04, s01, t, pid_col='s01q00a', age_col='s01q04a',
                           dob_year_col='s01q03c', survey_year=None):
    """Build people_last7days for one EHCVM wave (2018-19 / 2021-22).

    EHCVM's 7-day labor module (s04) records only the three participation
    DUMMIES in an ECVMA-comparable form:
      s04q06 -> farm_work (worked >=1h on own farm/garden/livestock/fishing)
      s04q07 -> SOB_work  (worked >=1h for own account / own business)
      s04q08 -> wage_work (worked >=1h for an employer / the State / others)
    The module records HOURS only for UNPAID domestic tasks (q01-05), and the
    industry only as 12-month occupation codes (not the WB 7-day section
    ranges), so farm_hrs / SB_hrs / wage_hrs and Industry are left NA (the
    _finish tail fills them).  working_age = roster Age >= 6; Age (`age_col`)
    is merged from the s01 roster on (grappe, menage, `pid_col`) — the same
    person key household_roster uses.  pid = `pid_col`.

    The person key differs by wave: 2018-19 uses ``s01q00a``; 2021-22 uses
    ``membres__id`` (matching each wave's household_roster pid).  Returns the
    (i, pid, farm_work, SOB_work, wage_work, working_age) frame ready for
    _finish_people_last7days.

    AGE: the EHCVM s01 roster fills age-in-years (`age_col`, s01q04a) for only
    ~27% of people and birth-year (`dob_year_col`, s01q03c) for the rest --
    exactly the two inputs household_roster's age_handler resolves.  We mirror
    that: prefer reported years, else survey_year - birth_year (9999 sentinel
    -> NA).  working_age = Age >= 6 (the survey threshold)."""
    key = ['grappe', 'menage', pid_col]
    roster_cols = [age_col, dob_year_col]
    merged = s04[key + ['s04q06', 's04q07', 's04q08']].merge(
        s01[key + roster_cols], on=key, how='left')

    hh = merged.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                            index=['grappe', 'menage'])),
                      axis=1)
    pid = merged[pid_col].apply(tools.format_id)

    age = pd.to_numeric(merged[age_col], errors='coerce')
    if survey_year is not None:
        birth = pd.to_numeric(merged[dob_year_col], errors='coerce')
        birth = birth.where((birth >= 1900) & (birth <= survey_year), pd.NA)
        age_from_dob = survey_year - birth
        age = age.where(age.notna(), age_from_dob)
    working_age = (age >= 6)
    # Keep working_age NA where age is entirely unknown (rather than False).
    working_age = working_age.where(age.notna(), pd.NA)

    df = pd.DataFrame({
        'i': hh.values,
        'pid': pid.values,
        'farm_work': _yn_bool(merged['s04q06']).values,
        'SOB_work': _yn_bool(merged['s04q07']).values,
        'wage_work': _yn_bool(merged['s04q08']).values,
        'working_age': working_age.values,
    })
    return df


def _finish_people_last7days(df, t):
    """Common tail for the wave-level people_last7days scripts: coerce dtypes,
    guarantee the full schema column set (NA where a wave lacks a field),
    drop rows with no individual key, and build the (t, i, pid) index."""
    df = df.copy()
    df['t'] = t
    df['pid'] = df['pid'].astype('string')
    for col in ['farm_work', 'SOB_work', 'wage_work', 'working_age']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype('boolean')
    for col in ['farm_hrs', 'SB_hrs', 'wage_hrs']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
    if 'Industry' not in df.columns:
        df['Industry'] = pd.NA
    df['Industry'] = df['Industry'].astype('string')
    df = df[df['i'].notna() & df['pid'].notna()]
    # One row per (t, i, pid): keep the first reported line per individual
    # (the source rosters are already one-row-per-person; this guards the
    # 2014-15 m:1 labor->roster merge against any stray duplicate).
    keep = ['t', 'i', 'pid', 'farm_work', 'SOB_work', 'wage_work',
            'farm_hrs', 'SB_hrs', 'wage_hrs', 'Industry', 'working_age']
    df = df[keep]
    df = (df.groupby(['t', 'i', 'pid'], dropna=False, as_index=False).first())
    df = df.set_index(['t', 'i', 'pid'])
    return df


# ---------------------------------------------------------------------------
# community_prices (GAP C — item-level community/market food prices)
# ---------------------------------------------------------------------------
#
# One row per REPORTED (cluster/EA × food item × unit) surveyed price from the
# ECVMA *community* price questionnaire (section CS07).  This is the OURS-ONLY
# parity arm (GAP_RANKING.org GAP C): the WB panel surveys no community prices
# — their crop_price_EA is an HH-median imputation, programs.do:31-89 — so this
# is a surveyed price we record where they only impute.  REPORTED values only:
# any median / mean across clusters, or a community->household price join, is a
# TRANSFORMATION over these rows, never a column here.
#
# GRAIN = (t, v, j, u).  This is a CLUSTER-level feature: `v` is the community
# questionnaire's `grappe` (EA), formatted into the SAME v keyspace as
# sample() (`v: grappe`/`v: GRAPPE` there), so community_prices.v == sample().v
# for the same cluster and the surveyed price joins households.  There is NO
# household `i`, so _join_v_from_sample does not apply and `v` IS declared in
# the index (not framework-joined).  `j` is on the SHARED harmonize_food
# Preferred Labels (so a priced food joins food_acquired consumption and the
# GAP-1 crop_production harvest); `u` on the shared `u` table.
#
# WAVE COVERAGE: only the two ECVMA waves carry an EA-level community price
# module.  The EHCVM waves (2018-19, 2021-22) replaced it with a separate
# region/market-level price survey (ehcvm_prix_ner2021: region × milieu ×
# point_de_vente, NO grappe — not the EA grain; and ehcvm_nsu, a region/strata
# p50/mu MEDIAN — a forbidden imputation), and 2018-19 ships no community price
# file at all.  So those two waves are silently absent (the country-level
# concatenator skips a wave with no parquet), NOT faked.
#
# Community-price instrument by wave (both ECVMA; module CS07):
#   2011-12  ecvmacoms07_p1 (passage 1) + ecvmacoms07_p2 (passage 2).
#            grappe; cs07q01 item; THREE (price, qty, unit) triples:
#            (cs07q03, cs07q04, cs07q05), (cs07q06, cs07q07, cs07q08),
#            (cs07q09, cs07q10, cs07q11).  Unit is a STRING column per triple.
#   2014-15  comprixcs07.  grappe; cs07q01 item; ONE unit column cs07q03
#            shared by THREE (price, qty) pairs: (cs07q04, cs07q05),
#            (cs07q06, cs07q07), (cs07q08, cs07q09).
# Both load convert_categoricals=True so the item / unit labels arrive as the
# strings harmonize_food / u key on.
#
# COLUMNS (reported, item-level):
#   Price    — the surveyed price the enumerator recorded for `Quantity` units
#              of the item in unit `u` in that cluster (CFA francs).  The
#              instrument records up to three price observations per
#              (cluster, item, unit); each is kept as its own row (the within-
#              cluster observation index is carried as `obs` so the rows do not
#              collapse).  NOT averaged — an across-observation mean is a
#              transformation.
#   Quantity — the measured quantity that Price was recorded for, in unit `u`
#              (e.g. Price=11000 for Quantity=50, u='Sac de 50 kg').  This is
#              the native price-per-quantity basis the survey gives; the per-kg
#              unit value Price/Quantity (×kg-factor) is a transformation.
# A price of 0 / sentinel (9999 / 99999), and rows whose unit is the
# 'produit absent' / 'manquant' missing-marker, carry no surveyed price and are
# dropped.  `obs` (1/2/3) keeps the multiple within-cluster observations as
# distinct rows without summing.


def _community_prices_maps():
    """Load the harmonize_food (item -> j Preferred Label) and u (unit ->
    Preferred Label) string maps, keyed on Original Label — exactly the two
    shared label tables crop_production / food_acquired use, so a priced food
    joins consumed food and harvested crop on (j, u)."""
    item_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    unit_map = tools.get_categorical_mapping(
        tablename='u', idxvars='Original Label',
        **{'Preferred Label': 'Preferred Label'})
    return item_map, unit_map


def _item_labels(item_series, item_map):
    """Map a community-price item column (string labels from
    convert_categoricals=True) through harmonize_food (keyed on Original
    Label).  Unmapped labels pass through unchanged (kept visible, flagged by
    the sanity checker)."""
    lab = item_series.astype('string')
    return lab.map(lambda x: item_map.get(x, x) if pd.notna(x) else pd.NA).astype('string')


# Price sentinels meaning "no answer / not recorded" in the CS07 grids.
_COMMUNITY_PRICE_SENTINELS = (9999.0, 99999.0)
# Unit Preferred Labels that mark the product as absent / missing in the
# cluster — these rows carry no surveyed price and are dropped.
_COMMUNITY_MISSING_UNITS = {'Manquant'}


def _community_price_triples(df, item_map, unit_map, triples, passage=1):
    """Reshape a wide CS07 community-price frame into long (v, j, u, passage,
    Price, Quantity) rows.

    `triples` is a list of (price_col, qty_col, unit_col) — the unit_col may be
    a single shared column repeated across triples (2014-15) or per-triple
    (2011-12).  The instrument records up to three within-questionnaire price
    observations; each becomes a candidate row, and one reported price per
    (t, v, j, u) is selected downstream in _finish_community_prices (NOT
    averaged).  `v` is the grappe formatted into the sample() keyspace; `j` via
    harmonize_food; `u` via the u table.  `passage` (the field visit: 1=post-
    planting, 2=post-harvest) tags the rows for the post-harvest-first
    selection.  Rows with no usable price (NA / 0 / sentinel) or a
    missing-marker unit are dropped here so they never reach the index."""
    v = df['grappe'].apply(tools.format_id).astype('string')
    j = _item_labels(df['cs07q01'].astype(str).str.strip(), item_map)
    pieces = []
    for pcol, qcol, ucol in triples:
        price = pd.to_numeric(df[pcol], errors='coerce').astype('Float64')
        qty = pd.to_numeric(df[qcol], errors='coerce').astype('Float64')
        # Quantity 0 / 9999 / 99999 are "no answer" markers, not a measured
        # basis — null them (the surveyed Price row is still kept; only the
        # missing quantity basis is dropped, per the reported-only rule).
        qty = qty.where((qty > 0)
                        & (~qty.isin(_COMMUNITY_PRICE_SENTINELS)), pd.NA)
        u = _unit_labels(df[ucol].astype(str).str.strip(), unit_map)
        piece = pd.DataFrame({
            'v': v.values, 'j': j.values, 'u': u.values,
            'Price': price.values, 'Quantity': qty.values,
            'passage': passage,
        })
        pieces.append(piece)
    out = pd.concat(pieces, ignore_index=True)
    # Drop rows with no surveyed price.
    out = out[out['Price'].notna() & (out['Price'] > 0)]
    for sv in _COMMUNITY_PRICE_SENTINELS:
        out = out[out['Price'] != sv]
    out = out[~out['u'].isin(_COMMUNITY_MISSING_UNITS)]
    return out


def _finish_community_prices(df, t):
    """Common tail for the wave-level community_prices scripts: tag t, coerce
    numeric columns, drop rows missing any index key, SELECT one reported price
    per (t, v, j, u), and build the (t, v, j, u) index.

    The CS07 instrument records up to three within-questionnaire price
    observations (2011-12 across two field passages too) per (cluster, item,
    unit).  Mirroring the Mali EACI community_prices reference, ONE reported
    observation is selected per (t, v, j, u) — post-harvest (passage 2) before
    post-planting (passage 1), then questionnaire order via a stable sort — so
    the result is a clean, item-level (t, v, j, u) grain carrying a genuinely
    REPORTED price (NOT an across-observation mean — averaging is a
    transformation).  This matches every sibling community_prices feature
    (Mali / Malawi / Tanzania / Ethiopia / Nigeria), which all use (t, v, j, u)
    with one reported price per cell."""
    df = df.copy()
    for col in ['Price', 'Quantity']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
    df['t'] = t
    df['v'] = df['v'].astype('string')
    df['j'] = df['j'].astype('string')
    df['u'] = df['u'].astype('string')
    if 'passage' not in df.columns:
        df['passage'] = 1
    # Every index level must be non-null (the framework drops NaN-key rows).
    df = df[df['v'].notna() & df['j'].notna() & df['u'].notna()]
    # Select one observation per (t, v, j, u): post-harvest (passage 2) before
    # post-planting (passage 1), then questionnaire order (stable sort).
    df = df.reset_index(drop=True)
    df['_porder'] = df['passage'].map({2: 0, 1: 1}).fillna(2).astype(int)
    df = df.sort_values(['t', 'v', 'j', 'u', '_porder'], kind='stable')
    df = df.drop_duplicates(subset=['t', 'v', 'j', 'u'], keep='first')
    # Redundant i==v level (positioned before j) so the framework's map_index
    # does not swap j->i for an i-less index; _normalize_dataframe_index then
    # drops the undeclared i, leaving the canonical (t,v,j,u) grain.  Mirrors
    # the Mali/Malawi/Tanzania/Ethiopia community_prices shim.
    df['i'] = df['v']
    keep = ['t', 'v', 'i', 'j', 'u', 'Price', 'Quantity']
    df = df[keep]
    df = df.set_index(['t', 'v', 'i', 'j', 'u']).sort_index()
    return df


def interview_date(df):
    """Melt EHCVM per-visit interview start/end timestamps onto a `visit`
    index. q23/q24/q25 a/b = visit 1/2/3 start/end -> int_start/int_end[_v2/_v3].
    Delegates to local_tools.melt_visit_intervals -> 'Interview start' /
    'Interview end'; collapsing `visit` with `first` reproduces the legacy
    single-date table."""
    return tools.melt_visit_intervals(df)
