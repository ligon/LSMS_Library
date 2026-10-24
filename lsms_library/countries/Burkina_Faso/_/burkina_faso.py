#!/usr/bin/env python

import pandas as pd
import numpy as np
import json
from ligonlibrary.dataframes import from_dta
import lsms_library.local_tools as tools


def i(value):
    '''
    Formatting household id from composite (grappe/zd, menage).
    Matches existing convention: str(grappe) + str(menage).rjust(3, '0')
    '''
    return tools.format_id(value.iloc[0]) + tools.format_id(value.iloc[1], zeropadding=3)


def ehcvm_i(grappe, menage):
    '''Canonical EHCVM household id reconciling with ``sample()``.

    The Burkina EHCVM 2018-19 ``sample`` table builds ``i`` as
    ``format_id(grappe) + '0' + format_id(menage, zeropadding=2)`` —
    grappe, a literal ``'0'`` separator, then the menage zero-padded to
    TWO digits.  (Verified empirically: this gives a 100% i-key
    intersection with ``sample().i`` across s00/s01/s04/s16a/s16b/s16c/s17.)

    This deliberately differs from the older :func:`i` above, which uses
    ``format_id(grappe) + format_id(menage, zeropadding=3)`` (no separator,
    3-digit menage).  The two AGREE only for two-digit menage; for the
    ~17% of households with a three-digit menage (100-527) they diverge —
    e.g. grappe=472, menage=141 -> :func:`i` makes ``'472141'`` but
    ``sample()`` (and this helper) make ``'4720141'``.  Using :func:`i`
    for the agriculture/livestock rosters therefore stranded ~17% of
    households off ``sample()`` (the GAP-4 livestock i-key bug).  New
    EHCVM features (livestock, crop_production, plot_inputs, plot_labor,
    people_last7days) MUST use this helper.  ``i`` is left untouched
    because ``food_acquired`` / ``plot_features`` already depend on it.

    Returns None if either component is missing.
    '''
    g = tools.format_id(grappe)
    m = tools.format_id(menage, zeropadding=2)
    if g is None or m is None:
        return None
    return g + '0' + m

# ---------------------------------------------------------------------------
# Household-id NAMESPACES for the independent samples (GH #1244, #1276)
# ---------------------------------------------------------------------------
#
# Every wave builds ``i`` from its cluster SERIAL and the household number, and
# the serials of independent draws overlap: EMC 2014 numbers its ZD 1-900,
# EHCVM-1 its grappes 1-585, the 2021-22 cross-section 586-1185.  So unrelated
# households shared served ids: 583 between 2014 (``zd + menage.zfill(3)``)
# and 2018-19 (``grappe + '0' + menage.zfill(2)``), and 9 between the 2021-22
# cross-section and 2018-19 (2018-19's (59, 612) and the cross-section's
# (590, 612) both spell '590612').  A cross-wave ``groupby('i')`` merged them.
#
# @ligon's ruling on #1244: namespace the samples that are NOT the EHCVM-1
# panel lineage, Niger's way (``niger.i`` prefixes EHCVM ids ``'E_'`` to keep
# them apart from ECVMA's).  The 2018-19 ids and the 2021-22 panel component's
# ids stay as they are: they are linked through ``panel_ids`` / ``updated_ids``.
#
# * ``EMC_`` -- the 2014 Enquete Multisectorielle Continue sample.  The whole
#   acronym, not Niger's one letter: ``E`` would read as EHCVM to anyone who
#   knows Niger's ``E_``.
# * ``CS_`` -- the 2021-22 EHCVM-2 cross-section (WB 6277,
#   ``Data/Cross_Section/``), Malawi's name for the same component (its
#   ``cs_i`` / ``'cs-17-'``) in Niger's spelling.  Burkina has one
#   cross-section sample; a later independent sample needs its own prefix.
#
# Letters cannot occur in the numeric ids, so a prefixed id can never equal an
# unprefixed one, whatever the serials do.
EMC_PREFIX = 'EMC_'
CROSS_SECTION_PREFIX = 'CS_'


def emc_i(zd, menage):
    '''2014 (EMC) household id: ``'EMC_' + zd + menage.zfill(3)``.'''
    z = tools.format_id(zd)
    m = tools.format_id(menage, zeropadding=3)
    if z is None or m is None:
        return None
    return EMC_PREFIX + z + m


def cross_section_i(grappe, menage):
    '''2021-22 cross-section (WB 6277) household id:
    ``'CS_' + grappe + menage.zfill(3)`` -- the country :func:`i` format, so
    the digits after the prefix are the survey's own ``hhid``.'''
    g = tools.format_id(grappe)
    m = tools.format_id(menage, zeropadding=3)
    if g is None or m is None:
        return None
    return CROSS_SECTION_PREFIX + g + m


def cs_i(value):
    '''``data_info.yml`` formatter for a ``Cross_Section/`` file entry:
    ``i: [grappe, menage, {mapping: cs_i}]`` (a per-file override).'''
    return cross_section_i(value.iloc[0], value.iloc[1])


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

# ---------------------------------------------------------------------------
# plot_features (GH #167; EHCVM cluster)
# ---------------------------------------------------------------------------
#
# Burkina Faso copies the Mali EHCVM reference implementation (PR #284):
# a single per-wave agriculture-parcel file s16a_me_bfa{year} with a
# uniform column scheme.  The only country-specific piece is the
# household-id formatter, which MUST match the one the wave's ``sample()``
# uses (GH #460) — and that DIFFERS BY WAVE:
#   * 2018-19: mapping.py overrides ``i()`` to ``ehcvm_i`` (grappe + '0' +
#     2-digit menage), so sample() / household_roster carry 7-char ids for
#     3-digit menage.  plot_features 2018-19 passes ``ehcvm_i``.
#   * 2021-22: mapping.py has NO ``i()`` override, so sample() / roster fall
#     back to the country-level ``i()`` (grappe + 3-digit menage, no '0'
#     separator).  plot_features 2021-22 passes that old formatter.
# ``plot_features_for_wave`` takes ``id_fn`` so each wave's plot_features.py
# selects the formatter matching its own sample().  Hardcoding the old
# ``i()`` (the pre-#460 behavior) stranded ~30% of 2018-19 plot_features
# households (3-digit menage) off sample() — the #460 i-key 0.846 bug.


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a ``{int code -> Preferred Label}`` dict from
    ``categorical_mapping.org`` for one of the plot_features harmonize_*
    tables.  Codes whose Preferred Label is blank / '---' map to NA so
    the corresponding column stays NaN.  Mirrors Mali / Uganda."""
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


def plot_features_for_wave(t, source, colmap, id_fn=None):
    """Build canonical ``plot_features`` for one Burkina Faso EHCVM wave.

    Uses the Mali EHCVM area/tenure pattern with Burkina's household-id
    formatter and optional parcel coordinates. Returns a DataFrame indexed by
    ``(t, i, plot_id)`` with columns ``Area`` (hectares float),
    ``AreaUnit`` ('hectares'), ``Tenure``, ``TenureSystem``,
    ``SoilType`` (str), and ``Irrigated`` (nullable bool).

    Required ``colmap`` keys: grappe, menage, field_no, parcel_no,
    area_gps, gps_measured, area_est, area_est_unit, tenure,
    tenure_system, soil_type, water_source.

    ``id_fn(grappe, menage) -> str`` selects the household-id formatter,
    which MUST match the one the wave's ``sample()`` uses (GH #460).
    The 2018-19 wave's ``mapping.py`` overrides ``i()`` to the canonical
    ``ehcvm_i`` form (``grappe + '0' + 2-digit menage``), so its sample /
    roster carry 7-char ids for 3-digit menage; the 2021-22 wave has no
    such override, so its sample / roster fall back to the country-level
    :func:`i` (``grappe + 3-digit menage``, no separator).  Hardcoding
    :func:`i` here therefore stranded ~30% of 2018-19 plot_features
    households off ``sample()`` (the #460 plot_features 0.846 i-key bug).
    Each wave's ``plot_features.py`` passes the formatter matching its own
    ``sample()``; the default is :func:`ehcvm_i` (the canonical EHCVM id).

    Optional ``latitude`` / ``longitude`` colmap keys supply independent
    nullable-float coordinates on the same retained source rows. Missing keys
    leave that wave unchanged; a declared missing source column raises.
    Burkina 2021-22 records these in s16aq46; 2018-19 has no coordinates.
    s16aq47 is an area, not a coordinate.
    """
    if id_fn is None:
        id_fn = ehcvm_i
    tenure_map = _harmonized_codes('harmonize_tenure')
    tenure_system_map = _harmonized_codes('harmonize_tenure_system')
    soil_map = _harmonized_codes('harmonize_soil')
    water_map = _harmonized_codes('harmonize_water')

    c = colmap

    # Drop s16a placeholder rows for non-farming households: one row per
    # household with NO field/parcel number and every plot attribute
    # blank.  Keeping them would emit a content-free "nan_nan" plot_id.
    src = source[source[c['field_no']].notna() & source[c['parcel_no']].notna()].copy()

    # Household id: composite (grappe, menage) via the wave-supplied
    # formatter (must match the wave's sample(); see docstring, GH #460).
    hh = src.apply(lambda r: id_fn(r[c['grappe']], r[c['menage']]), axis=1)

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
    for source_key, output_name in [('latitude', 'Latitude'),
                                    ('longitude', 'Longitude')]:
        if source_key in c:
            df[output_name] = pd.array(src[c[source_key]], dtype='Float64')
    df = df.set_index(['t', 'i', 'plot_id'])
    return df


def interview_date(df):
    """Melt EHCVM per-visit interview start/end timestamps onto a `visit`
    index. q23/q24/q25 a/b = visit 1/2/3 start/end -> int_start/int_end[_v2/_v3].
    Delegates to local_tools.melt_visit_intervals -> 'Interview start' /
    'Interview end'; collapsing `visit` with `first` reproduces the legacy
    single-date table."""
    return tools.melt_visit_intervals(df)
