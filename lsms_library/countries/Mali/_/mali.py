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
            topography    — topography/slope question -> Topography  (optional)
            soil_fertility— fertility-rating question -> SoilFertility (optional)

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares float), ``AreaUnit`` (always 'hectares'),
        ``SelfReportedArea`` (hectares float — the farmer estimate, kept
        distinct from the GPS-preferred ``Area``), ``Tenure``,
        ``TenureSystem``, ``SoilType`` (str), ``Topography`` (str reported
        slope class), ``SoilFertility`` (str reported fertility rating), and
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
    topo_map = _harmonized_codes('harmonize_topography')
    fert_map = _harmonized_codes('harmonize_soil_fertility')

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

    # Farmer-self-reported parcel area (hectares).  The WB harmonised panel
    # carries `self_reported_area` distinctly from the GPS measure; we keep it
    # as its own REPORTED column rather than only as the GPS fallback folded
    # into Area above.  Same unit conversion (1=Hectare, 2=m^2) and the same
    # plausibility clamp (GH #327) as Area.
    self_area = est_ha.where(((est_ha > 0) & (est_ha <= 1000)) | est_ha.isna(), pd.NA)

    tenure = _map_codes(src[c['tenure']], tenure_map) \
        if c.get('tenure') in src.columns else pd.Series(pd.NA, index=src.index, dtype='string')
    tenure_system = _map_codes(src[c['tenure_system']], tenure_system_map) \
        if c.get('tenure_system') in src.columns else pd.Series(pd.NA, index=src.index, dtype='string')
    soil_type = _map_codes(src[c['soil_type']], soil_map) \
        if c.get('soil_type') in src.columns else pd.Series(pd.NA, index=src.index, dtype='string')

    # Reported per-plot topography/slope class (s16aq19) and the farmer's
    # reported soil-fertility assessment (s16aq20).  These are SURVEYED item
    # attributes — the reported analogues of the WB panel's GAEZ-raster
    # `plot_slope` / `soil_fertility_index` (which are geospatial/PCA
    # transforms, out of scope).  Both columns are optional in the colmap.
    topography = _map_codes(src[c['topography']], topo_map) \
        if c.get('topography') in src.columns else pd.Series(pd.NA, index=src.index, dtype='string')
    soil_fertility = _map_codes(src[c['soil_fertility']], fert_map) \
        if c.get('soil_fertility') in src.columns else pd.Series(pd.NA, index=src.index, dtype='string')

    irrigated = pd.Series(pd.NA, index=src.index, dtype='boolean')
    if c.get('water_source') in src.columns:
        water_label = _map_codes(src[c['water_source']], water_map)
        irrigated = (water_label == 'Irrigated').astype('boolean')
        irrigated = irrigated.where(water_label.notna(), pd.NA)

    df = pd.DataFrame({
        't':                t,
        'i':                hh.values,
        'plot_id':          plot_id.values,
        'Area':             area_ha.values,
        'AreaUnit':         area_unit.values,
        'SelfReportedArea': self_area.values,
        'Tenure':           tenure.values,
        'TenureSystem':     tenure_system.values,
        'SoilType':         soil_type.values,
        'Topography':       topography.values,
        'SoilFertility':    soil_fertility.values,
        'Irrigated':        irrigated.values,
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


# ---------------------------------------------------------------------------
# plot_inputs (GAP 2; parity loop) — item-level (t, i, plot, input)
# ---------------------------------------------------------------------------
#
# One row per input APPLIED to a plot.  The crop/input modules live in the
# EACI waves only (2014-15 EACI14, 2017-18 EACI17); the EHCVM waves
# (2018-19, 2021-22) carry no agriculture-input block (the same wave split as
# crop_production), so plot_inputs is wired for the two EACI waves only.
#
# Grain: one row per (t, i, plot, input), where
#   - i     = Mali composite household id (grappe, menage/exploitation),
#   - plot  = "{field}_{parcel}" — the SAME plot id as crop_production /
#             plot_features (s2cq01_s2cq02 etc.); seed rows additionally key
#             on a crop column where the source records it,
#   - input = harmonize_input Preferred Label (Seed / Urea / DAP / NPK /
#             Other inorganic / Manure / Compost / Other organic / Pesticide /
#             Fungicide / Herbicide / Other phytosanitary).
#
# REPORTED item-level columns only (NO seed_kg / nitrogen_kg / any-use flags /
# fertilizer totals — those are transformations.py rollups over these rows):
#   Quantity, u, Purchased, Quantity_purchased, Improved, crop.
#
# The input types are FIXED questionnaire slots (not a decoded label column),
# so the wave scripts assign the harmonize_input Code directly; this module's
# _input_labels maps that Code -> Preferred Label.  Units arrive as decoded
# Stata labels (the `u` categorical table); the seed's crop is the
# harmonize_food Preferred Label so it joins crop_production / food_acquired.


def _input_labels(series):
    """Map a Series of harmonize_input Codes -> Preferred Label."""
    m = tools.get_categorical_mapping(tablename='harmonize_input', idxvars='Code',
                                      **{'Preferred Label': 'Preferred Label'})
    out = series.astype('string').str.strip().map(m)
    return out.astype('string')


def plot_inputs_finalize(df):
    """Common post-processing for a plot_inputs wave DataFrame.

    Expects raw columns already assembled:
        t, i, plot, input (harmonize_input Code), crop (decoded crop name or
        NA), u (decoded unit label or NA), Quantity, Purchased (nullable
        bool), Quantity_purchased, Improved (nullable bool).
    Maps input/unit/crop labels, coerces dtypes, drops content-free rows
    (an input slot the household did not use -> no quantity, no purchase,
    no improved flag), sets the (t, i, plot, input, crop) index, and
    collapses exact-duplicate index rows by summing the reported quantities
    and taking the first/any of the flags.
    """
    df = df.copy()
    df['input'] = _input_labels(df['input'])
    df['u'] = _unit_labels(df['u'])
    df['crop'] = _crop_labels(df['crop'])
    df = df.dropna(subset=['input'])  # drop unmapped input slots

    for c in ('Quantity', 'Quantity_purchased'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    # 9999 / 999 / 99 are the EACI "Manquant" sentinels on the reported
    # quantity columns (cf. the WB seed_kg block `replace seed_kg=. if
    # s1eq05a>=9999`); coerce to NA so they do not contaminate a downstream
    # kg-conversion sum.
    for c in ('Quantity', 'Quantity_purchased'):
        df.loc[df[c].isin([99, 999, 9999, 99999]), c] = pd.NA
    df['Purchased'] = df['Purchased'].astype('boolean')
    df['Improved'] = df['Improved'].astype('boolean')

    # Drop content-free rows: an input slot for which the household reported
    # nothing positive.  A reported Quantity, a reported purchased quantity,
    # a positive Purchased flag, or a non-NA Improved flag all count as
    # content.  Purchased == False does NOT count (it is the ABSENCE of a
    # purchase — keeping it would emit a content-free row on every plot for a
    # fertilizer type the household neither applied here nor bought).
    has_content = (df['Quantity'].notna() | df['Quantity_purchased'].notna()
                   | df['Purchased'].fillna(False).astype('boolean')
                   | df['Improved'].notna())
    df = df[has_content]

    # plot / crop may be NA; carry as NA-able strings so the index level is
    # not silently dropped.
    df['plot'] = df['plot'].astype('string')
    df['crop'] = df['crop'].astype('string')

    keys = ['t', 'i', 'plot', 'input', 'crop']
    # Collapse any exact (t,i,plot,input,crop) duplicates: sum reported
    # amounts (min_count=1 keeps an all-NA group NA, not a spurious 0),
    # first/any for flags.  dropna=False so plot/crop NA rows survive.
    g = df.groupby(keys, dropna=False, as_index=True)
    out = pd.DataFrame({
        'Quantity':           g['Quantity'].sum(min_count=1),
        'Quantity_purchased': g['Quantity_purchased'].sum(min_count=1),
        'u':                  g['u'].first(),
        'Purchased':          g['Purchased'].max(),
        'Improved':           g['Improved'].max(),
    })
    out = out[['Quantity', 'u', 'Purchased', 'Quantity_purchased', 'Improved']]
    return out.sort_index()


# ---------------------------------------------------------------------------
# livestock (GAP 4; parity loop) — item-level (t, i, animal)
# ---------------------------------------------------------------------------
#
# One row per (household, species owned).  Source: the EACI livestock roster
# that the WB MLI_EACI*.do code reads, recodes to a single engaged-in-
# livestock binary (s4aq03==Oui / s8aq04==Oui, collapse-max per hhid), then
# THROWS AWAY.  We keep the pre-collapse roster — strictly richer than their
# one y/n flag.
#
# Grain: one row per (t, i, animal), where
#   - i      = Mali composite household id (grappe, menage/exploitation),
#   - animal = harmonize_species Preferred Label (the survey's own integer
#              species code s4aq02/s8aq02 -> canonical species).
#
# REPORTED item-level columns only (NO TLU, NO herd-value total, NO
# engaged-in-livestock binary — those are transformations.py rollups over
# these rows; their binary is groupby(['t','i']).any() over these rows):
#   HeadCount        — number currently in the herd (owned + raised),
#   HeadAcquired     — number bought in the recall period,
#   HeadSold         — number owned by the HH sold in the recall period,
#   Value            — gross value of those sales (FCFA) where the source
#                      records it; NaN otherwise (the EACI roster carries NO
#                      current herd-value question, only a sales value).
#
# `animal` is set by the wave script from the numeric species code, mapped
# here to the harmonize_species Preferred Label.  No `v`: the framework's
# _no_v_join set already excludes 'livestock', so the canonical grain is
# (t, i, animal) with no cluster level.

# EACI "Manquant" / refusal sentinels on the reported count/value columns.
_LIVESTOCK_SENTINELS = [99, 999, 9999, 99999, 999999, 9999999]


def _species_labels(series):
    """Map a Series of numeric survey species codes (110, 120, ... 910)
    -> harmonize_species Preferred Label.  The org-table Code column is read
    back as STRING keys ('110', ...), so coerce the raw code to int->str
    before the map; unmapped codes (none expected) become NA."""
    m = tools.get_categorical_mapping(tablename='harmonize_species',
                                      idxvars='Code',
                                      **{'Preferred Label': 'Preferred Label'})
    code = pd.to_numeric(series, errors='coerce').astype('Int64')
    key = code.astype('string').str.replace(r'\.0$', '', regex=True)
    out = key.map(m)
    return out.astype('string').where(out.notna(), pd.NA)


def livestock_finalize(df):
    """Common post-processing for a livestock wave DataFrame.

    Expects raw columns already assembled:
        t, i, animal (numeric species code), HeadCount, HeadAcquired,
        HeadSold, Value.
    Maps the species code -> harmonize_species Preferred Label, coerces the
    reported count/value columns to numeric (clearing the EACI Manquant
    sentinels), drops content-free rows (a species the household does not
    keep — no head count and no transaction), sets the (t, i, animal) index,
    and collapses any exact-duplicate index rows by summing the reported
    amounts (min_count=1 so an all-NA group stays NA, not a spurious 0).
    """
    df = df.copy()
    df['animal'] = _species_labels(df['animal'])
    df = df.dropna(subset=['animal'])  # drop unmapped species codes

    for c in ('HeadCount', 'HeadAcquired', 'HeadSold', 'Value'):
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = pd.to_numeric(df[c], errors='coerce')
        df.loc[df[c].isin(_LIVESTOCK_SENTINELS), c] = pd.NA

    # Drop content-free rows: a species the household reported nothing
    # positive for.  A reported head count, an acquisition, a sale, or a
    # sale value all count as content; an all-zero/all-NA row does not (the
    # roster has a fixed slot per species, with zeros for animals the HH
    # does not keep — keeping them would emit 18 rows per household).
    has_content = (df['HeadCount'].fillna(0).gt(0)
                   | df['HeadAcquired'].fillna(0).gt(0)
                   | df['HeadSold'].fillna(0).gt(0)
                   | df['Value'].fillna(0).gt(0))
    df = df[has_content]

    keys = ['t', 'i', 'animal']
    g = df.groupby(keys, dropna=False, as_index=True)
    out = pd.DataFrame({
        'HeadCount':    g['HeadCount'].sum(min_count=1),
        'HeadAcquired': g['HeadAcquired'].sum(min_count=1),
        'HeadSold':     g['HeadSold'].sum(min_count=1),
        'Value':        g['Value'].sum(min_count=1),
    })
    out = out[['HeadCount', 'HeadAcquired', 'HeadSold', 'Value']]
    return out.sort_index()


# ---------------------------------------------------------------------------
# plot_labor (GAP 3a; parity loop) — item-level (t, i, plot, source)
# ---------------------------------------------------------------------------
#
# One row per (plot, labor source).  source in {family, hired, other}, on
# the shared `harmonize_labor_source` Preferred Labels.  Source: the EACI
# plot-labor rosters that the WB MLI_EACI*.do code reads then collapses to
# the per-plot totals (total_family_labor_days / total_hired_labor_days /
# total_other_labor_days / total_labor_days) and the median-wage valuation
# (hired_labor_value).  We keep the PRE-collapse REPORTED person-days per
# source — strictly richer than the WB collapsed totals, which are
# transformations.py rollups over these item rows.
#
# Two questionnaire passages contribute, both at the plot grain:
#   - post-planting (PP): MLI_EACI1.do:667-748 (lab_roster, s2b vars);
#     MLI_EACI2.do:603-674 (s11e vars).
#   - post-harvest  (PH): MLI_EACI1.do:750-868 (lab_roster2, s2f vars);
#     MLI_EACI2.do:676-769 (s7e vars).
# The wave script computes person-days for each (source, passage,
# gender-split) and hands a long (t, i, plot, source) frame to
# `plot_labor_finalize`, which sums the passages and gender splits to the
# REPORTED person-days per (plot, source) and carries the reported hired
# cash wage.  PersonDays = persons * days-each (s..05a * s..05b style), the
# same product the WB code forms before it sums.
#
# REPORTED item-level columns ONLY:
#   PersonDays — reported person-days of that source applied to the plot,
#                summed over passages (PP+PH) and the man/woman/child
#                demographic splits within a source.
#   Wage       — reported cash paid to HIRED labor (FCFA) where the survey
#                records it; NaN for family / other (no cash wage asked).
# NO total_labor_days / total_family_labor_days / total_hired_labor_days /
# hired_labor_value — all four are transformations.py rollups over these
# rows (total_* = groupby-sum by source; hired_labor_value = median-wage
# valuation over the hired rows).

# EACI "Manquant" / refusal sentinels on the reported day/persons/wage
# columns (cf. MLI_EACI1.do:720-722 `replace = . if ==99|999|99999999`).
_LABOR_SENTINELS = [99, 999, 9999, 99999, 999999, 9999999, 99999999]


def _labor_source_labels(series):
    """Map a Series of harmonize_labor_source Codes (family/hired/other)
    -> Preferred Label."""
    m = tools.get_categorical_mapping(tablename='harmonize_labor_source',
                                      idxvars='Code',
                                      **{'Preferred Label': 'Preferred Label'})
    out = series.astype('string').str.strip().map(m)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_labor_finalize(df):
    """Common post-processing for a plot_labor wave DataFrame.

    Expects raw columns already assembled, one row per
    (t, i, plot, source, <passage/gender split>):
        t, i, plot, source (harmonize_labor_source Code: family/hired/other),
        PersonDays (reported person-days for that split), Wage (reported cash
        wage for hired rows; NA otherwise).
    Maps the source code -> Preferred Label, coerces the reported columns to
    numeric (clearing the EACI Manquant sentinels), drops content-free rows
    (a source/split with no reported person-days and no wage), sets the
    (t, i, plot, source) index, and collapses to ONE row per (plot, source)
    by SUMMING the reported person-days over passages and gender splits
    (min_count=1 so an all-NA group stays NA, not a spurious 0) and summing
    the reported hired wage over the splits.
    """
    df = df.copy()
    df['source'] = _labor_source_labels(df['source'])
    df = df.dropna(subset=['source'])  # drop unmapped source codes

    for c in ('PersonDays', 'Wage'):
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = pd.to_numeric(df[c], errors='coerce')
        df.loc[df[c].isin(_LABOR_SENTINELS), c] = pd.NA

    # Drop content-free rows: a (plot, source, split) for which the survey
    # reported neither person-days nor a wage.  A reported positive
    # PersonDays or a reported positive Wage counts as content; an
    # all-zero/all-NA split does not (the roster has fixed slots per source
    # and gender, with zeros where that source did not work the plot —
    # keeping them would emit empty rows on every plot).
    has_content = (df['PersonDays'].fillna(0).gt(0)
                   | df['Wage'].fillna(0).gt(0))
    df = df[has_content]

    df['plot'] = df['plot'].astype('string')

    keys = ['t', 'i', 'plot', 'source']
    g = df.groupby(keys, dropna=False, as_index=True)
    out = pd.DataFrame({
        'PersonDays': g['PersonDays'].sum(min_count=1),
        'Wage':       g['Wage'].sum(min_count=1),
    })
    out = out[['PersonDays', 'Wage']]
    return out.sort_index()


# ---------------------------------------------------------------------------
# people_last7days (GAP 3b; parity loop) — item-level (t, i, pid)
# ---------------------------------------------------------------------------
#
# One row per individual, mirroring Uganda's per-individual 7-day activity
# feature so the six labor-gap countries match the one we already have.
# Source: the EACI individual labor/time-use module the WB MLI_EACI*.do
# code reads (MLI_EACI1.do:1435-1520, indiv_roster EACIIND_p1 s04 vars;
# MLI_EACI2.do:1340-1427, indiv_labor eaci17_s04p1 s4 vars).
#
# REPORTED per-individual columns ONLY (no rollups):
#   farm_work / SOB_work / wage_work — nullable-bool 7-day activity dummies
#       (worked on the household farm / own non-farm business / for a wage).
#   farm_hrs / SB_hrs / wage_hrs     — average weekly hours in each activity
#       ((months * days * hours) / 52, the WB av_hours construction).
#   Industry — primary-job industry on the `harmonize_industry` Preferred
#       Labels (Agriculture / Fishing / Mining / Manufacturing /
#       Construction / Services), decoded from the WB ind_* dummy split over
#       the primary-job industry code.  Zeroed (-> NA) for self-employment /
#       no-work as the WB code does; NOT gated on wage_work (so a non-wage
#       primary job in a recognized industry still carries a label — a
#       wage-only cut is a transformation over (Industry, wage_work)).
#   working_age — nullable bool (the WB working_age = age >= 6 floor).
# These are per-individual REPORTED values; the household roll-ups
# (nb_members_working_age etc.) are transformations.py, never stored.

# The six WB ind_* dummies map to one canonical Industry label.  Code is the
# WB dummy stem; Preferred Label resolves via the harmonize_industry table.
_INDUSTRY_CODES = ['ind_ag', 'ind_fish', 'ind_mining',
                   'ind_manuf', 'ind_const', 'ind_serv']


def _industry_labels(series):
    """Map a Series of harmonize_industry Codes (ind_ag .. ind_serv)
    -> Preferred Label."""
    m = tools.get_categorical_mapping(tablename='harmonize_industry',
                                      idxvars='Code',
                                      **{'Preferred Label': 'Preferred Label'})
    out = series.astype('string').str.strip().map(m)
    return out.astype('string').where(out.notna(), pd.NA)


def people_last7days_finalize(df):
    """Common post-processing for a people_last7days wave DataFrame.

    Expects raw columns already assembled, one row per (t, i, pid):
        t, i, pid, farm_work, SOB_work, wage_work (nullable bool dummies),
        farm_hrs, SB_hrs, wage_hrs (float weekly hours),
        Industry (harmonize_industry Code or NA), working_age (nullable bool).
    Maps the Industry code -> Preferred Label, coerces dtypes, sets the
    (t, i, pid) index, and (defensively) collapses any exact-duplicate
    (t, i, pid) by taking the any/first of the reported values.
    """
    df = df.copy()
    df['Industry'] = _industry_labels(df['Industry'])

    for c in ('farm_work', 'SOB_work', 'wage_work', 'working_age'):
        df[c] = df[c].astype('boolean')
    for c in ('farm_hrs', 'SB_hrs', 'wage_hrs'):
        df[c] = pd.to_numeric(df[c], errors='coerce')

    df['pid'] = df['pid'].astype('string')
    keys = ['t', 'i', 'pid']
    df = df.dropna(subset=['pid'])

    g = df.groupby(keys, dropna=False, as_index=True)
    out = pd.DataFrame({
        'farm_work':   g['farm_work'].max(),
        'SOB_work':    g['SOB_work'].max(),
        'wage_work':   g['wage_work'].max(),
        'farm_hrs':    g['farm_hrs'].sum(min_count=1),
        'SB_hrs':      g['SB_hrs'].sum(min_count=1),
        'wage_hrs':    g['wage_hrs'].sum(min_count=1),
        'Industry':    g['Industry'].first(),
        'working_age': g['working_age'].max(),
    })
    out = out[['farm_work', 'SOB_work', 'wage_work',
               'farm_hrs', 'SB_hrs', 'wage_hrs', 'Industry', 'working_age']]
    return out.sort_index()


# ---------------------------------------------------------------------------
# community_prices (GAP C; parity loop) — item-level (t, v, j, u)
# ---------------------------------------------------------------------------
#
# One row per (cluster v, food-item j, unit u): the REPORTED surveyed price
# the EACI community price questionnaire records for that good in that
# cluster.  CLUSTER-level — there is NO household i, so v is NATIVE (the
# community questionnaire's grappe, the SAME keyspace as sample().v, so the
# price joins households) and the framework's _join_v_from_sample does not
# fire (it only augments tables that carry i).
#
# The community price module lives in the 2014-15 EACI wave ONLY
# (EACIS04_p1 post-planting passage 1 + EACIS04_p2 post-harvest passage 2,
# rec_type 8, grappe-level, no menage).  The 2017-18 EACI wave dropped the
# community questionnaire; the EHCVM waves (2018-19, 2021-22) collect prices
# only at the region x milieu IHPC grain (ehcvm_prix), which carries no
# grappe key and so cannot join the sample().v cluster keyspace — deferred.
#
# Grain: one row per (t, v, j, u), where
#   - t = wave ('2014-15'),
#   - v = grappe (cluster) as a string — sample().v keyspace,
#   - j = harmonize_food Preferred Label, decoded from the broad item label
#         s04q01 (Riz, Maïs, Oignon frais, ...); REUSES the consumed-food
#         label so community_prices.j joins food_acquired.j / crop_production.j,
#   - u = u-table Preferred Label, decoded from the unit label s04q09
#         (Kilogramme -> Kg, Sac moyen (50 kg) -> Sac moyen, ...).
#
# REPORTED columns only:
#   Price    = s04q11, the surveyed price (FCFA) for a Quantity-sized lot.
#   Quantity = s04q10, the native quantity that Price refers to (e.g. a
#              "Sac moyen" lot is 50, a per-kilogramme price is 1).  The 999
#              sentinel ("non-standard / loose retail lot") is coerced to NA.
#   passage  = 1 (post-planting) / 2 (post-harvest); NOT emitted — used only
#              to pick a single observation per (t, v, j, u) below.
#
# Within a cluster the instrument records a separate price per sale-form
# variety (s04q02: imported/local rice, white/yellow maize, box sizes) and
# per passage.  Several varieties collapse to the same (j, u) — e.g. red vs
# white onion both -> (Oignon, Kg).  The declared index (t, v, j, u) holds
# ONE reported price per cell, so a single observation is SELECTED (never
# averaged — a mean/median across varieties or clusters would be a
# transformation): prefer the more complete post-harvest passage (2) over
# post-planting (1), then the first reported sale-form in questionnaire
# order.  Verified harmless: colliding same-(j,u) prices are near-identical
# (median max/min ratio ~1.13, median CV ~0.09), so the choice does not
# distort the reported price.  Any genuine median / community->household
# price imputation is transformations.py work over these rows.

# s04q09 unit labels that are numeric-coded data artifacts (a handful of
# rows where the unit was entered as a number / the missing sentinel); they
# do not map through the u table and are dropped.
_PRICE_QTY_SENTINELS = {999, 9999}


def community_prices_finalize(df):
    """Post-process a community_prices wave DataFrame.

    Expects raw columns already assembled:
        t, v, j (decoded item label), u (decoded unit label), Price,
        Quantity, passage.
    Maps j/u labels via harmonize_food / u, drops unmapped item/unit rows,
    coerces the Price/Quantity dtypes and the 999 Quantity sentinel, then
    SELECTS one reported price per (t, v, j, u) — post-harvest passage first,
    then questionnaire order — yielding a unique (t, v, j, u) index.
    """
    df = df.copy()
    df['j'] = _crop_labels(df['j'])   # harmonize_food Code -> Preferred Label
    df['u'] = _unit_labels(df['u'])   # u Code -> Preferred Label
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    df.loc[df['Quantity'].isin(_PRICE_QTY_SENTINELS), 'Quantity'] = pd.NA

    df['v'] = df['v'].astype('string')
    # Keep only resolvable, genuinely-priced item-unit rows.
    df = df[df['j'].notna() & df['u'].notna() & df['Price'].notna() & (df['Price'] > 0)]

    # Select one observation per (t, v, j, u): post-harvest (passage 2) before
    # post-planting (passage 1), then questionnaire row order (stable sort).
    df = df.reset_index(drop=True)
    df['_porder'] = df['passage'].map({2: 0, 1: 1}).fillna(2).astype(int)
    df = df.sort_values(['t', 'v', 'j', 'u', '_porder'], kind='stable')
    df = df.drop_duplicates(subset=['t', 'v', 'j', 'u'], keep='first')

    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')

    # community_prices is a CLUSTER table — there is no household i, the
    # natural grain is (t, v, j, u).  But the framework's
    # local_tools.map_index() (run on EVERY read path) unconditionally swaps
    # j -> i whenever a `j` index level is present and an `i` level is NOT.
    # That swap would rename the item level `j` to `i`, drop `j`, and collapse
    # the table to (t, v, u).  To keep `j` intact WITHOUT touching the
    # framework, carry a redundant `i` level (set equal to the cluster v)
    # positioned BEFORE `j`: map_index then sees i present and j after i, so it
    # does NOT swap, and the framework's _normalize_dataframe_index drops the
    # undeclared `i` level (data_scheme declares (t, v, j, u)), leaving the
    # canonical (t, v, j, u) grain.  v already in the index means
    # _join_v_from_sample is skipped, so the spurious i==v never reaches the
    # API.  (Same framework-compatibility shim Tanzania/Malawi/Ethiopia
    # community_prices use — see Tanzania community_prices_for_wave.)
    df['i'] = df['v']
    out = (df.set_index(['t', 'v', 'i', 'j', 'u'])[['Price', 'Quantity']]
             .sort_index())
    assert out.reset_index().duplicated(['t', 'v', 'j', 'u']).sum() == 0, \
        "community_prices: (t, v, j, u) not unique"
    return out
