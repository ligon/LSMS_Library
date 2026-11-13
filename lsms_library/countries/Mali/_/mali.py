# Formatting  Functions for Mali
import re
import pandas as pd
from lsms_library.ehcvm import crop_condition_codes, crop_condition_labels
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import U_UNKNOWN
from lsms_library.derivations import MULTI_SEP, union_keys
from lsms_library.build_transforms import reduce_to_agreed
from collections import defaultdict

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value.iloc[0])+'0'+tools.format_id(value.iloc[1],zeropadding=2)

# Household namespaces (GH #1494, ruling @ligon 2026-11-04).  2014-15 and 2017-18
# are EAC-I; 2018-19 and 2021-22 are EHCVM, a separate programme with no household
# link (panel_ids.py).  Both number their clusters and households from 1, so ``i``
# above spells the same string for unrelated households: 3,613 ids shared between
# 2017-18 and 2018-19, 1,601 between 2014-15 and 2018-19, 3,341 and 1,493 with
# 2021-22.  A cross-wave ``groupby('i')`` merged them.  Member-level roster match
# on those ids is at the level of two unrelated households of one grappe
# (CONTENTS.org, GH #1494).  Letters cannot occur in the numeric ids, so a prefixed
# id can never equal an unprefixed one, whatever the serials do. The later1494
# ruling namespaces ALL2021 raw IDs; only approved panel links map back to bare
# 2018 canonical IDs. Unlinked/unknown/orphan records stay distinct.
# Burkina Faso's ``EMC_`` is the precedent (GH #1244).  One prefix per EAC-I wave, because
# the two waves are not linked either: they visit the same EAs with households selected
# afresh, and both number rural households 3, 6 and 9 on the Lourd form, so 3,560 ids
# recurred between them with a roster match at the level of two different households.
EACI14_PREFIX = 'EACI14_'
EACI17_PREFIX = 'EACI17_'
EHCVM21_PREFIX = 'EHCVM21_'


def eaci14_i(value):
    '''2014-15 household id: ``'EACI14_' + i([grappe, menage])``.'''
    return EACI14_PREFIX + i(value)


def eaci17_i(value):
    '''2017-18 household id: ``'EACI17_' + i([grappe, exploitation])``.'''
    return EACI17_PREFIX + i(value)


def ehcvm21_i(value):
    '''2021-22 RAW household id; approved panel maps return its bare2018 target.

    GH1494 user ruling: every raw record is namespaced, without classifying
    Non/missing/orphan records or changing the native composite formatter.
    '''
    return EHCVM21_PREFIX + i(value)


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

def MonthsSpent(value):
    '''
    Decode the 2014-15 EACI residence-duration question (s01q11) to a
    numeric months-present value in 0..12.

    Unlike the EHCVM waves (2017-18 s1q13, 2018-19 s01q12) whose source
    column is a binary Oui/Non gate, the 2014-15 EACI column is a MONTHS
    COUNT carrying raw French labels: a '36 mois et plus' top-code and a
    'Manquant' missing sentinel alongside integer 0..35.  Left undecoded
    those labels leak into household_roster['MonthsSpent'], and
    roster_to_characteristics' to_numeric(errors='coerce') then turns
    every '36 mois et plus' / 'Manquant' row into NaN, silently dropping
    those members from household_characteristics (GH #502).

    Mapping: '36 mois et plus' -> 12 (capped; resident the whole window),
    'Manquant'/missing -> <NA>, numeric n -> min(n, 12).
    '''
    if pd.isna(value):
        return pd.NA
    s = str(value).strip()
    if s in ('Manquant', 'NSP', ''):
        return pd.NA
    if s.lower().startswith('36 mois'):  # '36 mois et plus' top-code
        return 12
    try:
        n = int(float(s))
    except (TypeError, ValueError):
        return pd.NA
    return min(max(n, 0), 12)

# GH #1045: the country-level ``Int_t`` formatter and ``interview_date(df)``
# hook are gone -- every wave declares a ``datetime:`` spec for Int_t in its
# data_info.yml and the framework parses it at build time
# (lsms_library.datetime_parse).


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

    # The raw2021 namespace is shared by YAML, scripts and derivation inputs.
    if t == '2021-22':
        keys = src[[c['grappe'], c['menage']]].rename(
            columns={c['grappe']: 'grappe', c['menage']: 'menage'})
        hh = ehcvm_household_ids(keys, wave=t)
    else:
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
# crop_production (GAP 1; parity loop) — item-level (t, i, plot_id, crop, condition)
# ---------------------------------------------------------------------------
#
# All four waves are wired.  The two EAC-I waves (2014-15 EACI14, 2017-18
# EACI17) go through crop_production_finalize below.  The two EHCVM waves
# (2018-19 s16c; 2021-22 s16c + s16d) were left unwired by an earlier decision,
# which the rulings of @ligon 2026-11-03 reverse (GH #824); they go through
# finish_ehcvm_crop_production -- see the section after crop_production_finalize.
#
# Grain: one row per (t, i, plot_id, crop), each with its one condition, where
#   - i     = Mali composite household id (grappe, menage/exploitation),
#   - plot  = "{field_no}_{parcel_no}" for seasonal crops; 'Farm-level' for
#             perennial trees (the perennial roster s3b/s11f is not on the
#             field grid, GH #1476) and for a 2017-18 or 2021-22 sale with no
#             single harvest row (GH #1457),
#   - crop  = harmonize_food Preferred Label (food crops REUSE the consumed-
#             food label so crop_production.j joins food_acquired.j).
#
# REPORTED item-level columns only (no harvest_kg / yield / main_crop /
# value-share — those are transformations.py work over these rows):
#   Quantity, u, Quantity_sold, Unit_sold, Value_sold, months_harvested,
#   planting_month, harvest_month, intercropped, perennial
# plus `Derivation` on the perennial (tree) rows -- see
# derive_tree_twelve_month_total, derive_tree_sale_window and
# stamp_tree_derivations below -- and on the 2017-18 and 2021-22 rows that hold a
# household-crop sale (derive_hh_crop_sale_attachment).
#
# Crop names and units arrive as DECODED Stata labels (strings), so the
# harmonize_food / u tables key on the decoded label (Code column).  The
# sold unit is decoded from ITS OWN variable's value labels and passed
# through the SAME u table as the harvest unit (GH #824): in 2017-18 code 10
# is 'Autre' on the harvest set (s7fq13c) and 'Tonne' on the sold set
# (s7gq21b), so a code-level comparison of the two would be wrong.

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


# --- Perennial (tree) rows: a 12-month harvest, and the sale as recorded -----
#     (GH #1267; rulings @ligon 2026-10-23 on PR #1272, revised the same day)
#
# Both EAC-I tree rosters ask the harvest and the quantity sold as an
# AVERAGE PER HARVEST MONTH, beside the NUMBER of months harvested in the
# last 12, and then the sale's value -- printed in BOTH questionnaires as
# "Quel a ete le montant total de cette vente ? (En FCFA)":
#
#   2014-15 3B (EACIS3B_p2.dta)        2017-18 11F (eaci17_s11fp1.dta)
#   3B.09 s3bq09  months harvested     11F.10 s11fq10  months harvested
#   3B.10 s3bq10a/b harvest per month  11F.11 s11fq11a/b harvest per month
#   3B.13 s3bq13a/b sold per month     11F.14 s11fq14a/b sold per month
#   3B.14 s3bq14  "montant total"      11F.15 s11fq15  "montant total"
#
# (11F.15's Stata LABEL says "Montant de la vente mensuelle moyenne"; the
# printed instrument, French p1 pp. 44-45 and English pp. 42-43, says
# "montant total" / "total amount" -- word for word 3B.14.  Both enumerator
# manuals tell the enumerator to record a TOTAL value: 2014-15 3B.14
# "determiner le montant total de la vente", 2017-18 11F.15 "la valeur en F
# CFA de la quantite totale vendue".)
#
# Until GH #1267 the 2014-15 rows served `Quantity = s3bq09`, the month
# COUNT, as the harvest -- a misreading inherited from the WB harmonised
# MLI_EACI1.do:259, which computes `harvest_kg_per = s3bq09 * s3bq10b`
# (months times the UNIT CODE).
#
# Served (rulings @ligon 2026-10-23 on PR #1272; the sale revised after the
# document search, following the Ethiopia #1257 precedent):
#
#   Quantity      = s3bq10a x s3bq09    | s11fq11a x s11fq10
#                   -- the 12-MONTH HARVEST, key tree-twelve-month-total
#                   (the form and the 2017-18 manual ask it per month);
#   Quantity_sold = s3bq13a             | s11fq14a      AS RECORDED
#   Value_sold    = s3bq14              | s11fq15       AS RECORDED
#                   -- key tree-sale-window, whose function is the identity:
#                   the quantity is asked per month, both manuals call the
#                   value a total, and the price levels put the value on the
#                   per-month quantity's footing.  As recorded, Value_sold /
#                   Quantity_sold is a price per Unit_sold; what the
#                   instrument cannot settle is whether the pair is a
#                   monthly or a seasonal figure.  A user who wants the
#                   12-month reading multiplies by months_harvested.
#
# (Rounds 1-3 of the same day served the sold quantity, and then the value,
# x months; the history and the evidence are in Mali/_/CONTENTS.org.)
#
# The per-month answers stay recoverable by their original names through
# Country.derivation_inputs(key), and the month count rides in its own
# column, months_harvested.
#
# The edges:
#   - the existing quantity codes 99 / 9999 / 9999.9 are masked before
#     arithmetic; the baseline tree value mask is ONLY 999999. This window
#     fix excludes the recovered new tree-999998 and field-value masks
#     (ledger current scope sections 3-5). Those values remain recorded;
#     a field's explicit 'Non' zero-sale rule is unchanged.
#     crop_production_finalize does not re-mask a tree row (a genuine
#     harvest total can be 99);
#   - THE HARVEST (the product): a per-month answer of 0 gives 0 whatever
#     the month count; months NA (the 99 code) -> NA: the window is unknown;
#     months == 0 beside a POSITIVE per-month answer -> NA: the two answers
#     contradict each other and no total is formed (the WB do-file serves 0
#     there; a 0 harvest beside a sale would manufacture a zero); a
#     per-month answer that is NA stays NA;
#   - THE SALE (no product): the month count does not touch it.  A sale
#     beside a 0 or missing month count is served as recorded, like every
#     other sale (the Ethiopia section-12 rule: an answer beside a
#     contradicting flag is kept as reported); its key says the window is
#     unsettled, and a user rescaling by months_harvested gets 0 / NA there.
# THE WINDOW MISMATCH ON THE ROW (disclosed, red team 2026-10-23): a tree
# row's Quantity is a 12-month total while its Quantity_sold is per harvest
# month, so Quantity_sold / Quantity is NOT a sold share; the share is
# Quantity_sold x months_harvested / Quantity.  On same-unit tree rows the
# plain ratio's median is 0.375 (258 rows), the rescaled share's 0.917.
# Every tree row carries tree-twelve-month-total, including one whose total
# ends NA; tree-sale-window joins it (MULTI_SEP, sorted) on a tree row with
# a positive Quantity_sold or Value_sold -- a zero sale is zero in any window.

#: Registry keys (Mali/_/derivations.yml), stamped by the wave scripts after
#: the collapse through :func:`stamp_tree_derivations`.
DERIVATION_TREES = 'Mali::crop_production::tree-twelve-month-total'
DERIVATION_TREE_SALES = 'Mali::crop_production::tree-sale-window'

#: (file, crop code, "has this tree" flag, within-cluster household key) of
#: each wave's tree roster.  11F keys the household on `ménage`; it is the
#: `exploitation` of every other 2017-18 file.
_TREE_ROSTERS = {
    '2014-15': ('EACIS3B_p2.dta', 's3bq01', 's3bq03', 'menage'),
    '2017-18': ('eaci17_s11fp1.dta', 's11fq01', 's11fq03', 'ménage'),
}
#: Each wave's tree-roster variables: months harvested, harvest per month
#: and its unit, quantity sold per month and its unit, sale value.
_TREE_VARS = {
    '2014-15': dict(months='s3bq09', q='s3bq10a', u='s3bq10b',
                    qs='s3bq13a', us='s3bq13b', v='s3bq14'),
    '2017-18': dict(months='s11fq10', q='s11fq11a', u='s11fq11b',
                    qs='s11fq14a', us='s11fq14b', v='s11fq15'),
}
#: The raw variables each tree key's ``inputs`` callable returns: the crop,
#: the "has this tree" flag and the month count, then the harvest pair
#: (tree-twelve-month-total) or the sale triple (tree-sale-window).  The two
#: lists partition the roster's measured answers.
_TREE_RAW = {
    '2014-15': ['s3bq01', 's3bq03', 's3bq09', 's3bq10a', 's3bq10b'],
    '2017-18': ['s11fq01', 's11fq03', 's11fq10', 's11fq11a', 's11fq11b'],
}
_TREE_SALE_RAW = {
    '2014-15': ['s3bq01', 's3bq03', 's3bq09', 's3bq13a', 's3bq13b', 's3bq14'],
    '2017-18': ['s11fq01', 's11fq03', 's11fq10', 's11fq14a', 's11fq14b',
                's11fq15'],
}

#: The EAC-I "Manquant" codes on a reported quantity: 99 / 9999 on the
#: integer columns, 9999.9 on 3B's float columns (stored float32, hence
#: isclose).  Shared by crop_production_finalize (field rows) and
#: tree_crop_rows (the recorded tree answers: before the harvest product,
#: and on the sold quantity served as recorded).
_QUANTITY_SENTINELS = (99, 9999)
_QUANTITY_SENTINEL_FLOAT = 9999.9
#: Preserve the baseline tree-value mask (57d255585). The window ruling
#: does not authorize inferring additional value codes: 999998 stays recorded,
#: and field values do not use this tree-only mask. Proposed extensions remain
#: a separate source/policy need (ledger current scope sections 3-6).
_VALUE_SENTINEL = 999999


def _is_quantity_sentinel(series):
    x = pd.to_numeric(series, errors='coerce')
    return x.isin(_QUANTITY_SENTINELS) | np.isclose(
        x.to_numpy(dtype='float64', na_value=np.nan), _QUANTITY_SENTINEL_FLOAT)


def _mask_value_code(series):
    """Tree value as float64 with only the pre-existing 999999 mask.

    Retain baseline policy while correcting the window, not a new sentinel
    inference. Field values never call this helper; 999998 remains recorded.
    """
    x = pd.to_numeric(series, errors='coerce').astype('float64')
    return x.mask(x.eq(_VALUE_SENTINEL))


def producing_trees(src, wave):
    """The rows of a FIXED tree roster that name a tree the household has.

    Both rosters list every species for every household, with the flag
    (s3bq03 / s11fq03) 'Non' for trees it does not have; keep 'Oui' only
    (the WB `drop if s3bq03==2` / `drop if s11fq03==2`).
    """
    _, crop, held, _ = _TREE_ROSTERS[wave]
    src = src[src[crop].notna()]
    return src[src[held].astype('string').str.strip().eq('Oui')]


def _months_harvested(series):
    """3B.09 / 11F.10 -> months harvested in the last 12, Int64 in 0..12.

    2014-15 ``s3bq09`` is numeric (its only value label is 99 = Manquant);
    2017-18 ``s11fq10`` is labelled '1 mois' .. '12 mois', with code 0
    'Aucune récolte (Arbre fruitier n'a pas commencé à produire)'.  Not the
    month-of-year ``_month_num`` parses: 0 is a real answer here.  Anything
    outside 0..12 (the 99 sentinel) is NA.
    """
    def conv(v):
        if pd.isna(v):
            return pd.NA
        s = str(v).strip()
        if s.lower().startswith('aucune'):
            return 0
        m = re.match(r'^(\d+)(?:\.0+)?(?:\s*mois)?$', s)
        if m is None:
            return pd.NA
        n = int(m.group(1))
        return n if 0 <= n <= 12 else pd.NA
    return series.map(conv).astype('Int64')


def derive_tree_twelve_month_total(per_month, months_harvested):
    """A tree crop's 12-month harvest: per-month harvest x months harvested.

    Registered as ``Mali::crop_production::tree-twelve-month-total``.
    ``per_month`` is the harvest the EAC-I tree roster records "en moyenne
    chaque mois" (3B.10 ``s3bq10a`` in 2014-15; 11F.11 ``s11fq11a`` in
    2017-18); ``months_harvested`` is the Int64 month count (3B.09 / 11F.10,
    from :func:`_months_harvested`).  Vectorised; returns float64 (NaN for
    NA) by the harvest edge rule of the module comment above: a 0 answer is
    0; months NA, or months 0 beside a positive answer, is NA; an NA answer
    stays NA.  The missing codes are masked by the caller
    (:func:`tree_crop_rows`).  Not applied to the sale since the revised
    ruling of 2026-10-23 (:func:`derive_tree_sale_window`).
    """
    x = pd.to_numeric(per_month, errors='coerce').astype('float64')
    m = pd.Series(months_harvested).astype('Float64').to_numpy(
        dtype='float64', na_value=np.nan)
    total = x * m                                   # NA months or answer -> NaN
    total = total.mask(x.eq(0), 0.0)                # 0 a month is 0 a year
    total = total.mask(x.gt(0) & (m == 0))          # contradiction: no total
    return total


def derive_tree_sale_window(quantity_sold, value_sold):
    """A tree crop's sale -> the ``Quantity_sold`` / ``Value_sold`` it serves: THE IDENTITY.

    Registered as ``Mali::crop_production::tree-sale-window`` (revised
    ruling @ligon 2026-10-23 on PR #1272, after the Ethiopia #1257
    precedent ``sect12-sale-period``).  ``quantity_sold`` is the quantity
    sold "en moyenne chaque mois" (3B.13 ``s3bq13a`` / 11F.14 ``s11fq14a``)
    and ``value_sold`` the sale value (3B.14 ``s3bq14`` / 11F.15
    ``s11fq15``), both already cleared of their missing codes by the caller
    (:func:`tree_crop_rows`).  Returned unchanged: nothing is multiplied by
    the month count.  What the registered derivation records is the WINDOW,
    not the arithmetic -- the quantity is asked per harvest month, both
    enumerator manuals call the value a total, and the price levels put the
    value on the per-month quantity's footing, so the pair is served as
    recorded beside field rows whose sale is a campaign total -- CLAUDE.md
    "Derived Values": "a value served under a name implying a different
    window -- the function is the identity and the registry is the point".
    No options, by rule.  Returns a DataFrame with columns ``Quantity_sold``
    and ``Value_sold`` (float64), aligned with the inputs.
    """
    return pd.DataFrame({
        'Quantity_sold': pd.to_numeric(quantity_sold, errors='coerce').astype('float64'),
        'Value_sold': pd.to_numeric(value_sold, errors='coerce').astype('float64'),
    })


def tree_crop_rows(src, wave):
    """The measured columns of one wave's served tree rows, from its roster.

    ``src`` is the roster after :func:`producing_trees`.  Quantity is
    :func:`derive_tree_twelve_month_total` of the per-month harvest, first
    cleared of the 99 / 9999 / 9999.9 missing codes; Quantity_sold and
    Value_sold are :func:`derive_tree_sale_window` -- the recorded answers,
    the quantity cleared of the same codes and the baseline tree value of
    999999 (:func:`_mask_value_code`), not
    multiplied by the month count; u and Unit_sold are the decoded Stata
    labels (``crop_production_finalize`` maps both through the ``u`` table);
    months_harvested is the month count.
    """
    v = _TREE_VARS[wave]
    months = _months_harvested(src[v['months']])

    def quantity(col):
        x = pd.to_numeric(src[col], errors='coerce').astype('float64')
        return x.mask(_is_quantity_sentinel(x))

    sale = derive_tree_sale_window(quantity(v['qs']), _mask_value_code(src[v['v']]))
    return pd.DataFrame({
        'months_harvested': months,
        'Quantity':      derive_tree_twelve_month_total(quantity(v['q']), months),
        'u':             src[v['u']],
        'Quantity_sold': sale['Quantity_sold'],
        'Unit_sold':     src[v['us']],
        'Value_sold':    sale['Value_sold'],
    }, index=src.index)


def stamp_tree_derivations(df):
    """The ``Derivation`` column of a finalised crop_production wave frame.

    Called by both EAC-I wave scripts AFTER ``crop_production_finalize``
    (CLAUDE.md "Derived Values", registering step 3), so the stamp rides on
    served rows and their served values.  Every perennial (tree) row
    carries :data:`DERIVATION_TREES`; a tree row with a positive
    Quantity_sold or Value_sold also carries :data:`DERIVATION_TREE_SALES`,
    the two joined with ``MULTI_SEP`` in sorted order (the form
    ``derivations.union_keys`` writes and ``_key_counts`` splits).  A zero
    sale is zero in any window, so it carries only the harvest key.  Field
    rows are NA.  Returns a ``string`` Series on ``df.index``.
    """
    tree = df['perennial'].astype('boolean').fillna(False).to_numpy(dtype=bool)
    sold = ((df['Quantity_sold'] > 0) | (df['Value_sold'] > 0)).astype(
        'boolean').fillna(False).to_numpy(dtype=bool)
    out = pd.Series(pd.NA, index=df.index, dtype='string')
    out[tree] = DERIVATION_TREES
    out[tree & sold] = MULTI_SEP.join(sorted((DERIVATION_TREES,
                                              DERIVATION_TREE_SALES)))
    return out


def _tree_roster_inputs(wave, columns):
    """Raw tree-roster answers ``columns`` of one wave, at (t, i, crop).

    Re-reads the roster through ``get_dataframe``, keeps the rows the wave
    script keeps (:func:`producing_trees`), and returns the ORIGINAL variable
    names as columns, as recorded, indexed by the served ``t``, ``i``
    (pre-``id_walk``; ``Country.derivation_inputs`` re-keys it) and ``crop``
    (the served harmonize_food label) -- so it joins the served perennial
    rows, whose ``plot_id`` is ``'Farm-level'``.
    """
    from pathlib import Path
    from lsms_library.paths import countries_root
    fn, crop, _, hh = _TREE_ROSTERS[wave]
    root = Path(countries_root()) / 'Mali'
    src = tools.get_dataframe(str(root / wave / 'Data' / fn))
    src = producing_trees(src, wave)
    out = src[columns].copy()
    out['t'] = wave
    wave_i = {'2014-15': eaci14_i, '2017-18': eaci17_i}[wave]
    out['i'] = src.apply(lambda r: wave_i(pd.Series([r['grappe'], r[hh]])), axis=1)
    # _crop_labels resolves its table relative to the cwd of a wave script;
    # this callable runs from anywhere, so name the same two rungs (wave _/,
    # then country _/) explicitly.
    crop_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Code',
        dirs=[f'{root / wave / "_"}/', f'{root / "_"}/'],
        **{'Preferred Label': 'Preferred Label'})
    out['crop'] = (src[crop].astype('string').str.strip().map(crop_map)
                   .astype('string').values)
    return out.set_index(['t', 'i', 'crop'])


def inputs_tree_twelve_month_total(wave):
    """Raw harvest answers behind one wave's perennial rows, at (t, i, crop).

    The registry ``inputs`` callable for ``tree-twelve-month-total``: the
    crop, the "has this tree" flag, the month count and the PER-MONTH
    harvest and its unit (:data:`_TREE_RAW`), as recorded.
    """
    return _tree_roster_inputs(wave, _TREE_RAW[wave])


def inputs_tree_sale_window(wave):
    """Raw sale answers behind one wave's perennial rows, at (t, i, crop).

    The registry ``inputs`` callable for ``tree-sale-window``: the crop, the
    "has this tree" flag, the month count, the quantity sold per month, its
    unit and the sale value (:data:`_TREE_SALE_RAW`), as recorded -- the
    month count is there so a user can rescale the sale to 12 months.
    """
    return _tree_roster_inputs(wave, _TREE_SALE_RAW[wave])


def crop_production_finalize(df):
    """Common post-processing for a crop_production wave DataFrame.

    Expects raw columns already assembled:
        t, i, plot, crop (decoded), u (decoded), Quantity, Quantity_sold,
        Value_sold, planting_month, harvest_month, intercropped, perennial,
    and optionally Unit_sold (decoded sold unit), KgFactor_sold (reported
    FIELD kg per sold unit) and months_harvested.
    Maps crop/unit labels -- the sold unit through the SAME ``u`` table as
    the harvest unit (GH #824) -- coerces dtypes, collapses on (t, i, plot, crop),
    and appends ``condition`` to that index as the sentinel (GH #1436).  The
    collapse takes exact-duplicate index rows (a crop reported on the
    same plot more than once in a single questionnaire row-set) by summing
    the reported quantities / values and taking the first of the
    flags/dates.  A group whose rows report the sale in more than one sold
    unit keeps NO sold quantity and NO sold unit (the Niger one-unit rule,
    ``Niger/2021-22/_/crop_production.py``): a sum across units has no
    denominator.  Measured: 0 such groups in either wave.
    """
    df = df.copy()
    df['crop'] = _crop_labels(df['crop'])
    df['u'] = _unit_labels(df['u'])
    # Unit_sold (GH #824): canonicalised HERE, at build time, through the u
    # table -- the read-time auto-apply reaches only a column named `u`
    # (GH #1188).  NA-bearing, never sentinel-filled; 'Manquant' is not in the
    # table and maps to NA (canonical data_info.yml, Unit_sold note).
    if 'Unit_sold' not in df.columns:
        df['Unit_sold'] = pd.NA
    df['Unit_sold'] = _unit_labels(df['Unit_sold'])
    if 'KgFactor_sold' not in df.columns:
        df['KgFactor_sold'] = pd.NA
    df['KgFactor_sold'] = pd.to_numeric(df['KgFactor_sold'], errors='coerce')
    if 'months_harvested' not in df.columns:
        df['months_harvested'] = pd.NA
    df['months_harvested'] = pd.to_numeric(
        df['months_harvested'], errors='coerce').astype('Int64')
    df = df.dropna(subset=['crop'])  # drop "Non exploitée" / unmapped residual

    for c in ('Quantity', 'Quantity_sold', 'Value_sold'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    # 9999 / 99 are the EACI "Manquant" sentinels on the reported quantity
    # columns (cf. WB `harvest_kg=. if s3aq08a==9999`); coerce to NA so they
    # do not contaminate a downstream kg-conversion sum.
    # 9999.9 is the same code on 3B's float columns (stored float32, hence
    # isclose): it occurs once in s3bq10a and once in s3bq13a, and both rows
    # carry 'Manquant' for the matching unit (the first also for the month
    # count s3bq09), so it is not a quantity (GH #1267).
    # FIELD rows only.  A tree row's answers were already masked in
    # tree_crop_rows: its Quantity arrives as a 12-month TOTAL (per-month
    # answer x months, derive_tree_twelve_month_total) whose per-month answer
    # was masked BEFORE the product -- masking the total here would null a
    # genuine total that happens to be 99 or 9999 (none today, measured
    # 2026-10-23) and could never catch 9999 x 2 -- and its Quantity_sold
    # arrives as recorded, already masked (derive_tree_sale_window).
    tree = df['perennial'].astype('boolean').fillna(False).to_numpy(dtype=bool)
    for c in ('Quantity', 'Quantity_sold'):
        sentinel = _is_quantity_sentinel(df[c]).to_numpy(dtype=bool) & ~tree
        df.loc[sentinel, c] = pd.NA
    df['planting_month'] = _month_num(df['planting_month'])
    df['harvest_month'] = _month_num(df['harvest_month'])
    df['intercropped'] = df['intercropped'].astype('boolean')
    df['perennial'] = df['perennial'].astype('boolean')

    # The wave scripts serve no null plot since GH #1476 (a tree row is
    # 'Farm-level'); dropna=False below stays, so a null that does arrive is
    # kept and reported by Site I rather than deleted.
    df['plot_id'] = df['plot_id'].astype('string')

    keys = ['t', 'i', 'plot_id', 'crop']
    # Aggregate any exact (t,i,plot,crop) duplicates: sum reported amounts,
    # first non-null for flags/dates/unit.  dropna=False so a row with a
    # null key is not discarded by groupby.  min_count=1 on the
    # sums keeps an all-NA group as NA (not a spurious 0) — important for the
    # 2017-18 multi-plot crops whose sold qty/value is deliberately NaN.
    g = df.groupby(keys, dropna=False, as_index=True)
    # Positional (same groupby, same order): a NaN plot_id key makes label
    # alignment of the two results fragile.
    mixed_sold_unit = (g['Unit_sold'].nunique() > 1).to_numpy()
    # Factor-only exact agreement at the existing grain; never sum, weight,
    # choose a non-null winner or discard quantity rows (ledger #824 §2/4).
    # The original reducer infers this native wave from the single t index.
    agreed_factor = reduce_to_agreed(
        df.set_index(keys)[['KgFactor_sold']],
        on_conflict='na', na_is_conflict=True,
        country='Mali', table='crop_production')
    out = pd.DataFrame({
        'Quantity':       g['Quantity'].sum(min_count=1),
        'Quantity_sold':  g['Quantity_sold'].sum(min_count=1).mask(mixed_sold_unit),
        'Unit_sold':      g['Unit_sold'].first().mask(mixed_sold_unit),
        'Value_sold':     g['Value_sold'].sum(min_count=1),
        'u':              g['u'].first(),
        'months_harvested': g['months_harvested'].first(),
        'planting_month': g['planting_month'].first(),
        'harvest_month':  g['harvest_month'].first(),
        'intercropped':   g['intercropped'].max(),
        'perennial':      g['perennial'].max(),
    })
    out['Unit_sold'] = out['Unit_sold'].astype('string')
    out['KgFactor_sold'] = (agreed_factor['KgFactor_sold'].reindex(out.index)
                            .mask(mixed_sold_unit))
    out = out[['Quantity', 'u', 'Quantity_sold', 'Unit_sold', 'KgFactor_sold', 'Value_sold',
               'months_harvested', 'planting_month', 'harvest_month',
               'intercropped', 'perennial']]
    # GH #1436: the EHCVM waves serve the `condition` level, and every wave the
    # country script concatenates must carry it.  2014-15's harvest files hold no
    # state.  2017-18 records one (s7fq13b) in a list of its own that no ruling has
    # folded, and it must never go through the EHCVM table, whose codes 3 and 4 it
    # inverts: CONTENTS.org, "serves condition".  After the collapse, so the level
    # moves no row.
    out['condition_native'] = pd.array([pd.NA] * len(out), dtype='string')
    out['condition'] = pd.array([CONDITION_NA] * len(out), dtype='string')
    return out.set_index('condition', append=True).sort_index()


# --- crop_production: the EHCVM waves (GH #824; rulings @ligon 2026-11-03) ----
#
# 2018-19  s16c_me_mli2018.dta  one plot-by-crop file, harvest AND sale on the
#                               line (Niger/2018-19/_/crop_production.py).
# 2021-22  s16c_me_mli2021.dta  plot-by-crop, harvest only (s16cq16a is filled
#                               on 309 of 7,247 rows: GH #1437);
#          s16d_me_mli2021.dta  household-by-crop dispositions, one row per
#                               (household, crop), no plot.
#
# A 2021-22 sale goes on the household's single harvest ROW for that crop where
# there is exactly one (the Niger 2021-22 rule after GH #1360: one served row,
# not merely one plot), and otherwise on ONE row of that household and crop with
# plot_id = 'Farm-level'.  Nothing is dropped and nothing is split:
# derive_hh_crop_sale_attachment.
#
# A finished 2021-22 harvest whose s16cq16a the file lacks takes the sum of its
# s16d dispositions where they share one unit, placed the same way, on the single
# row or the Farm-level row: derive_hh_crop_harvest_total (GH #1437).  Everything
# else stays NA.  A recorded harvest of an unfinished crop keeps its value and takes
# the window key harvest-to-date, in both waves.
#
# Why these waves do not go through crop_production_finalize:
#   - it masks 99 and 9999 as the EACI "Manquant" codes.  s16cq12a carries no
#     such label, and 2018-19 records 99 on three rows as a quantity;
#   - it folds duplicates, and there are none: both s16c files are unique on
#     (grappe, menage, field, parcel, crop) under the crop labels served here,
#     so the index needs no production_obs and nothing here reduces anything;
#   - it silently drops a crop harmonize_food does not name.  Here that raises:
#     1,441 + 1,380 rows of 'Riz Paddy' would otherwise have vanished.
#
# REPORTED ONLY.  Quantity_sold / Value_sold are NA on a line the survey did not
# record them on, 'Non' included: unlike 2017-18, nothing is filled with 0 here
# (Niger's rule).  KgFactor_sold is NA throughout: s16cq16c / s16dq05c, "Estimation
# Quantite totale UML en kg", are TOTALS (s16dq05c == s16dq05a on 495 of 495 rows
# sold in Kg, and == 100 x s16dq05a on 299 of 465 sold in 100 kg sacks), the
# shape that keeps the EAC-I tree equivalents unwired.  s16cq16c is served as the
# harvest's Quantity_kg (GH #1436), only on the row that recorded it: s16d's other
# "c" columns (s16dq02c, s16dq03c, s16dq13c) are the state of the product, so a
# dispositions-derived Quantity has no survey kilograms, and s16dq05c is a sale's: it is
# served as the sale's Quantity_sold_kg, a total, riding with the sale (GH #1560, ruling
# @ligon 2026-11-12, "Treat 2021 as totals"; ehcvm_sold_kilograms).
# planting_month,
# harvest_month, months_harvested and perennial are NA: s16c asks none of them,
# and it lists tree crops (Manguier, Anacarde) on the field grid without saying
# which are perennial.
#
# CONDITION (GH #1436, ruling @ligon 2026-11-04).  "Etat du produit" of the harvest,
# 2018-19 s16cq12c and 2021-22 s16cq16d, is the `condition` level, read by code, with
# the answer as recorded in `condition_native`.  Both waves use the same five codes
# and labels, so one table serves both.  A blank code is the sentinel, and so is a
# row with no s16c line behind it (a Farm-level row) or a harvest derived from s16d,
# whose s16cq16d is blank. The separate sold states s16cq16c/s16dq05d now
# retain their own fold and native answer (GH1528), without supplying harvest state.

#: The canonical structural sentinel for `plot_id`: the record has no plot by
#: design (lsms_library/data_info.yml, top of file).  It is not `unknown_plot`,
#: which says a plot exists and its identifier is missing.  Two kinds of Mali
#: row carry it: an EAC-I tree row, from a roster with no field grid, which has
#: a harvest Quantity (GH #1476); and a 2021-22 household-crop sale, which has
#: none.
FARM_LEVEL = 'Farm-level'

#: Registry key (Mali/_/derivations.yml) on a 2021-22 row that holds a sale which
#: s16d recorded for the household and crop rather than for the plot.
DERIVATION_HH_CROP_SALE = (
    'Mali::crop_production::hh-crop-sale-single-row')

#: Registry keys of the harvest (GH #1437): an identity naming a window, and a sum.
DERIVATION_HARVEST_TO_DATE = 'Mali::crop_production::harvest-to-date'
DERIVATION_HARVEST_DISPOSITIONS = 'Mali::crop_production::harvest-from-dispositions'

#: Registry key on a 2021-22 row with a Quantity_kg (GH #1436): an identity, because
#: the instrument calls s16cq16c an estimate.
DERIVATION_SURVEY_KG_ESTIMATE = 'Mali::crop_production::survey-kg-estimate'

#: Its sibling on a 2021-22 row with a Quantity_sold_kg (GH #1560, ruling 2026-11-12 (c)):
#: s16dq05c is the same "Estimation", on the crop-level sale record.
DERIVATION_SOLD_KG_ESTIMATE = 'Mali::crop_production::sold-kg-estimate'

#: What s16d records of a crop, each with its own quantity <var>a and unit <var>b:
#: consumed, given away, sold, in stock.
_DISPOSITIONS = ('s16dq02', 's16dq03', 's16dq05', 's16dq13')

_SALE_KEYS = ['i', 'crop']
_SALE_COLUMNS = ['Quantity_sold', 'Unit_sold', 'Value_sold']
_EHCVM_INTERCROP = {'Association de cultures': True, 'Pure': False}

#: The canonical sentinel of the `condition` level (lsms_library/data_info.yml): a
#: real value and never NA, because a null index key is deleted by the next groupby.
CONDITION_NA = 'unknown_condition'


def _label_or_refuse(raw, mapped, where):
    """``mapped``, after refusing a recorded label ``where`` does not name."""
    raw = raw.astype('string').str.strip()
    lost = raw[raw.notna() & mapped.isna()]
    if len(lost):
        raise ValueError(f'crop_production: {sorted(set(lost))} are recorded in '
                         f'the source and not in {where}')
    return mapped


def ehcvm_crop_labels(series):
    """s16cq04 / s16dq01 labels -> harmonize_food Preferred Label; refuses a gap."""
    return _label_or_refuse(series, _crop_labels(series),
                            'the harmonize_food table').astype('string')


def ehcvm_unit_labels(series):
    """s16c / s16d unit labels -> ``u`` Preferred Label; refuses a gap.

    NA stays NA: ``u`` is a COLUMN in this table, as in the EAC-I waves, so a
    row with no recorded unit serves NA here and ``Unknown`` through Feature()'s
    promotion.  Writing a literal ``Unknown`` beside the EAC-I nulls is the
    state ``feature._align_to_canonical_levels`` warns about.
    """
    return _label_or_refuse(series, _unit_labels(series),
                            'the u table').astype('string')


def ehcvm_intercropped(series):
    """s16cq07 -> True (Association de cultures) / False (Pure) / NA."""
    raw = series.astype('string').str.strip()
    mapped = raw.map(_EHCVM_INTERCROP)
    return _label_or_refuse(raw, mapped, 'the s16cq07 labels').astype('boolean')


def ehcvm_crop_conditions(codes):
    """Raw "Etat du produit" codes (``convert_categoricals=False``) through
    ``harmonize_crop_condition_ehcvm1`` (GH #1436).  The table is read strictly: a
    missing table must not serve every row as the sentinel.  A blank or off-scheme
    code becomes :data:`CONDITION_NA`."""
    table = tools.get_categorical_mapping(
        tablename='harmonize_crop_condition_ehcvm1', idxvars='Code',
        **{'Preferred Label': 'Preferred Label'})
    table = {int(code): token for code, token in table.items()}
    if sorted(table) != [1, 2, 3, 4, 5]:
        raise ValueError(f'harmonize_crop_condition_ehcvm1: {table}')
    return crop_condition_codes(pd.to_numeric(codes, errors='coerce'), table).fillna(CONDITION_NA)


def ehcvm_crop_condition_labels(labels):
    """The state as the survey recorded it, for ``condition_native``: *labels* is the
    column read with ``convert_categoricals=True``.  A blank stays NA, never a
    sentinel, because a column has no NaN-key hazard (ruling @ligon 2026-11-04,
    GH #1436)."""
    return crop_condition_labels(labels)


def ehcvm_crop_sold_conditions(codes):
    """Own EHCVM sold-state fold, NA-bearing (ledger1528 §§2–4).

    Both own sold dictionaries were checked, including 2021 s16dq05d; no
    harvest answer supplies a sold state and no blank becomes an index sentinel.
    """
    folded = ehcvm_crop_conditions(codes)
    return folded.where(folded != CONDITION_NA, pd.NA)


def ehcvm_household_ids(src, wave='2018-19'):
    """Raw EHCVM keys; canonical panel IDs are applied later by id_walk.

    The legacy default is the unchanged2018 formatter.2021 is explicit at
    every own consumer. Column scalars avoid mixing an integer ID with an
    unrelated fractional quantity in a rowSeries; native answers stay intact.
    """
    if wave == '2018-19':
        return src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']])), axis=1)
    if wave != '2021-22':
        raise ValueError('Mali EHCVM household IDs require 2018-19 or2021-22')
    return pd.Series(
        [ehcvm21_i(pd.Series([g, h], dtype=object))
         for g, h in zip(src['grappe'].array, src['menage'].array)],
        index=src.index,
    )


def ehcvm_plot_ids(src):
    """``{s16cq02}_{s16cq03}``: the spelling of plot_features' ``plot_id``."""
    field = src['s16cq02'].apply(tools.format_id).astype('string')
    parcel = src['s16cq03'].apply(tools.format_id).astype('string')
    return field + '_' + parcel


def ehcvm_is_sale(quantity, value):
    """A sale is a record with a positive sold quantity or a positive value."""
    return (pd.to_numeric(quantity, errors='coerce').gt(0)
            | pd.to_numeric(value, errors='coerce').gt(0))


def derive_hh_crop_sale_attachment(harvest, sales, columns=_SALE_COLUMNS):
    """Place each household-crop sale on the served rows of a wave (GH #824, #1457).

    ``harvest`` holds one row per plot-crop harvest record, with columns
    ``i``, ``crop`` and ``plot_id``.  ``sales`` holds one row per sale, with
    columns ``i``, ``crop``, and ``columns`` (``Quantity_sold``, ``Unit_sold`` and
    ``Value_sold`` by default; 2017-18 adds ``KgFactor_sold``);
    a second sale of one (i, crop) is refused rather than summed.

    A sale of an (i, crop) with EXACTLY ONE harvest row goes on that row, in its
    own ``Unit_sold``, and that row alone carries the key in ``Derivation``.  Any
    other sale -- two or more rows, or none -- becomes ONE new row of that
    (i, crop) with ``plot_id = FARM_LEVEL``, its other columns NA.  The index
    value discloses those, so they carry no key (CLAUDE.md, "exempt by rule").

    No sold quantity or value is split, summed or scaled.  Registered as
    ``Mali::crop_production::hh-crop-sale-single-row``.
    """
    if sales.duplicated(_SALE_KEYS).any():
        raise ValueError('crop_production: more than one sale for a (i, crop)')
    lines = harvest.groupby(_SALE_KEYS).size().rename('_lines').reset_index()
    n = sales[_SALE_KEYS].merge(lines, on=_SALE_KEYS, how='left')['_lines']
    single = n.fillna(0).eq(1).to_numpy()
    columns = list(columns)
    served = harvest.merge(sales.loc[single, _SALE_KEYS + columns],
                           on=_SALE_KEYS, how='left', validate='many_to_one',
                           indicator='_attached')
    derivation = pd.Series(pd.NA, index=served.index, dtype='string')
    derivation[served.pop('_attached').eq('both')] = DERIVATION_HH_CROP_SALE
    served['Derivation'] = derivation
    farm = sales.loc[~single, _SALE_KEYS + columns].copy()
    farm['plot_id'] = FARM_LEVEL
    return pd.concat([served, farm], ignore_index=True)


def stamp_hh_crop_sale(derivation, df):
    """``derivation`` with the attachment key on an EAC-I field row holding an s7g sale.

    2017-18 only, and AFTER ``crop_production_finalize``: a positive sale on a row
    with a plot.  A tree row's plot_id is ``FARM_LEVEL``, and so is a Farm-level
    sale's, which needs no key: its index value discloses it.  A 0 is no sale.
    """
    plotted = df.index.get_level_values('plot_id').to_series(index=df.index).ne(FARM_LEVEL)
    sold = (df['Quantity_sold'] > 0) | (df['Value_sold'] > 0)
    return _with_key(derivation, plotted & sold.astype('boolean').fillna(False),
                     DERIVATION_HH_CROP_SALE)


def _with_key(derivation, mask, key):
    """``derivation`` with ``key`` joined on where ``mask`` holds.

    Two keys on one row are ``MULTI_SEP``-joined in sorted order
    (``derivations.union_keys``), which is how a sale's attachment and a harvest's
    derivation share the column.
    """
    out = derivation.astype('string').copy()
    mask = pd.Series(mask, index=out.index).fillna(False).astype(bool)
    out[mask] = pd.array([union_keys((value, key)) for value in out[mask]], dtype='string')
    return out


def derive_harvest_to_date(quantity):
    """The identity: a recorded harvest of an unfinished crop, served as recorded.

    Registered as ``Mali::crop_production::harvest-to-date``.  Nothing is computed.
    The registry is the point: s16cq12a (2018-19) and s16cq16a (2021-22) ask the
    quantity ALREADY harvested, so on a row whose harvest is not finished the served
    ``Quantity`` covers a part of the season under a name that implies all of it.
    """
    return quantity


def stamp_harvest_to_date(df):
    """Key the rows with ``finished == 'Non'`` and a recorded ``Quantity``."""
    out = df.copy()
    out['Quantity'] = derive_harvest_to_date(pd.to_numeric(out['Quantity'], errors='coerce'))
    derivation = (out['Derivation'] if 'Derivation' in out.columns
                  else pd.Series(pd.NA, index=out.index, dtype='string'))
    unfinished = out['finished'].astype('string').str.strip().eq('Non')
    out['Derivation'] = _with_key(derivation, unfinished & out['Quantity'].notna(),
                                  DERIVATION_HARVEST_TO_DATE)
    return out


def derive_survey_kg_estimate(kilograms, quantity):
    """2021-22 s16cq16c -> ``Quantity_kg``: THE IDENTITY, bar a 0 for a positive harvest.

    Registered as ``Mali::crop_production::survey-kg-estimate``.  Nothing is computed;
    the registry says what the number is, as ``harvest-to-date`` does for a window.  A 0
    beside a positive quantity (3 rows) is no answer, not a harvest of nothing, and is
    NA, so the row falls down the ladder.  Returns float64.
    """
    kg = pd.to_numeric(kilograms, errors='coerce').astype('float64')
    return kg.mask(kg.eq(0) & pd.to_numeric(quantity, errors='coerce').gt(0))


def ehcvm_sold_kilograms(kilograms, quantity_sold):
    """2021-22 s16dq05c -> ``Quantity_sold_kg``: the sale's kilograms, a TOTAL.

    GH #1560 / #1571, ruling @ligon 2026-11-12 ("Treat 2021 as totals"): the released
    label says "Estimation Quantite totale UML en kg", and on all 495 sales in Kg the
    kilograms equal the sold quantity, where both own vague forms instruct a weight per
    ONE unit.  Read literally, as ``Quantity_kg`` is; never divided into a factor, and
    never put in ``KgFactor_sold``, which is kg per ONE sold unit.  A 0 beside a positive
    sold quantity (4 rows) is no answer, not a sale of nothing, and is NA.  The column
    has no code (max 1,150,000, no all-nines value).  Returns float64.
    """
    kg = pd.to_numeric(kilograms, errors='coerce').astype('float64')
    return kg.mask(kg.eq(0) & pd.to_numeric(quantity_sold, errors='coerce').gt(0))


def stamp_sold_kg_estimate(df):
    """Key the rows whose ``Quantity_sold_kg`` holds a value (a Farm-level sale row too).

    :data:`DERIVATION_SOLD_KG_ESTIMATE`, the sibling of the harvest's
    ``survey-kg-estimate`` (ruling @ligon 2026-11-12, "Sold-kg details" (c)).
    """
    out = df.copy()
    out['Derivation'] = _with_key(out['Derivation'], out['Quantity_sold_kg'].notna(),
                                  DERIVATION_SOLD_KG_ESTIMATE)
    return out


def inputs_sold_kg_estimate(wave):
    """The registry ``inputs`` callable of ``sold-kg-estimate``: the s16d sale records
    whose ``Quantity_sold_kg`` holds a value, as recorded, at (t, i, crop)."""
    year = {'2021-22': '2021'}[wave]
    src = _ehcvm_keyed(wave, f's16d_me_mli{year}.dta', 's16dq01')
    keep = (ehcvm_is_sale(src['s16dq05a'], src['s16dq06'])
            & ehcvm_sold_kilograms(src['s16dq05c'], src['s16dq05a']).notna())
    return src.loc[keep.to_numpy(), ['t', 'i', 'crop', 's16dq01', 's16dq04', 's16dq05a',
                                     's16dq05b', 's16dq05c', 's16dq06']].set_index(
        ['t', 'i', 'crop'])


def stamp_survey_kg_estimate(df):
    """Key the rows whose ``Quantity_kg`` holds a value."""
    out = df.copy()
    out['Derivation'] = _with_key(out['Derivation'], out['Quantity_kg'].notna(),
                                  DERIVATION_SURVEY_KG_ESTIMATE)
    return out


def _harvest_totals(lines, dispositions):
    """The (i, crop) pairs whose harvest their dispositions can state, with the sum.

    ``lines`` holds the household's s16c rows of the crop: ``i``, ``crop``,
    ``Quantity`` and ``finished``.  ``dispositions`` holds one row per (i, crop), with
    ``<var>a`` and ``<var>b`` for each of :data:`_DISPOSITIONS`.  A pair qualifies when

    - every s16c row of it says the harvest is finished: a mixed pair's dispositions
      cover what is harvested so far, so they are not a finished total, and an
      unanswered row has not said so;
    - no row of it records a quantity: a recorded harvest is never overridden, and
      a total beside it on a Farm-level row would count it twice;
    - its positive dispositions all state one unit: no factor between units exists.

    Returns ``Quantity``, ``u`` (the dispositions' own unit label) and ``lines`` (how
    many s16c rows the pair has), indexed by (i, crop).
    """
    if dispositions.duplicated(_SALE_KEYS).any():
        raise ValueError('crop_production: more than one s16d record for a (i, crop)')
    long = pd.concat([
        dispositions[_SALE_KEYS].assign(
            quantity=pd.to_numeric(dispositions[f'{var}a'], errors='coerce').astype('float64'),
            unit=dispositions[f'{var}b'].astype('string').str.strip())
        for var in _DISPOSITIONS], ignore_index=True)
    long = long[long['quantity'].gt(0)]
    long['unitless'] = long['unit'].isna()
    stated = long.groupby(_SALE_KEYS).agg(Quantity=('quantity', 'sum'), u=('unit', 'first'),
                                          units=('unit', 'nunique'), unitless=('unitless', 'any'))
    facts = pd.DataFrame({'i': lines['i'], 'crop': lines['crop'],
                          'open': (lines['finished'].astype('string').str.strip().ne('Oui')
                                   .fillna(True).astype(bool)),
                          'recorded': pd.to_numeric(lines['Quantity'], errors='coerce').notna()})
    facts = facts.groupby(_SALE_KEYS).agg(lines=('open', 'size'), open=('open', 'any'),
                                          recorded=('recorded', 'any'))
    pairs = stated.join(facts, how='inner')
    keep = pairs['units'].eq(1) & ~pairs['unitless'] & ~pairs['open'] & ~pairs['recorded']
    return pairs.loc[keep, ['Quantity', 'u', 'lines']]


def derive_hh_crop_harvest_total(served, dispositions):
    """A finished 2021-22 harvest the file lacks, from its dispositions (GH #1437).

    ``served`` is the frame :func:`derive_hh_crop_sale_attachment` returns, with
    ``finished`` beside it.  ``dispositions`` is :func:`ehcvm_dispositions`.  For a
    pair of :func:`_harvest_totals`, ``Quantity`` is the sum of consumed, given, sold
    and in stock, and ``u`` their unit.  It goes where the sale of that pair goes:
    on the single s16c row, or, for two or more rows, on the pair's one Farm-level
    row, created if no sale already made it.  That row carries the key.  It is
    never split among plots.  Every other row keeps what it had.

    Registered as ``Mali::crop_production::harvest-from-dispositions``.
    """
    plots = served['plot_id'].ne(FARM_LEVEL).fillna(True)
    totals = _harvest_totals(served[plots], dispositions)
    out = served.reset_index(drop=True)
    plots = plots.reset_index(drop=True)
    pair = pd.MultiIndex.from_frame(out[_SALE_KEYS])
    target = ((plots & pair.isin(totals.index[totals['lines'].eq(1)]))
              | (~plots & pair.isin(totals.index[totals['lines'].gt(1)])))
    placed = totals.reindex(pair[target])
    out['Quantity'] = pd.to_numeric(out['Quantity'], errors='coerce').astype('float64')
    out.loc[target, 'Quantity'] = placed['Quantity'].to_numpy()
    out['u'] = out['u'].astype('string')
    out.loc[target, 'u'] = pd.array(placed['u'].to_numpy(), dtype='string')
    out['Derivation'] = _with_key(out['Derivation'], target, DERIVATION_HARVEST_DISPOSITIONS)
    several = totals[totals['lines'].gt(1)]
    new = several[~several.index.isin(pair[~plots])].reset_index()[
        _SALE_KEYS + ['Quantity', 'u']]
    new['plot_id'] = FARM_LEVEL
    new['Derivation'] = DERIVATION_HARVEST_DISPOSITIONS
    return pd.concat([out, new], ignore_index=True)


def ehcvm_dispositions(src, wave='2018-19'):
    """s16d, one row per (i, crop), with its dispositions on the served unit labels."""
    out = pd.DataFrame({'i': ehcvm_household_ids(src, wave=wave).astype('string').values,
                        'crop': ehcvm_crop_labels(src['s16dq01']).values})
    for var in _DISPOSITIONS:
        out[f'{var}a'] = pd.to_numeric(src[f'{var}a'], errors='coerce').values
        out[f'{var}b'] = ehcvm_unit_labels(src[f'{var}b']).values
    return out


def finish_ehcvm_crop_production(df, wave):
    """The served frame of an EHCVM wave, indexed (t, i, plot_id, crop, condition).

    The same columns and dtypes as ``crop_production_finalize``, so the country
    concat changes no EAC-I value.  Reduces nothing: a repeated key raises.
    """
    out = df.copy()
    out['t'] = wave
    for col in ('Quantity', 'Quantity_kg', 'Quantity_sold', 'Quantity_sold_kg', 'Value_sold',
                'KgFactor_sold'):
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors='coerce').astype('float64')
    for col in ('months_harvested', 'planting_month', 'harvest_month'):
        out[col] = pd.array([pd.NA] * len(out), dtype='Int64')
    out['perennial'] = pd.array([pd.NA] * len(out), dtype='boolean')
    out['intercropped'] = out['intercropped'].astype('boolean')
    for col in ('u', 'Unit_sold', 'Derivation'):
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = out[col].astype('string')
    for col in ('i', 'plot_id', 'crop'):
        out[col] = out[col].astype('string')
    if out[['i', 'plot_id', 'crop']].isna().any(axis=None):
        raise ValueError(f'crop_production {wave}: a NA household, plot or crop')
    # A Farm-level row has no s16c line behind it, so no state: the sentinel, and NA.
    for col in ('condition', 'condition_native', 'Condition_sold', 'condition_sold_native'):
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = out[col].astype('string')
    out['condition'] = out['condition'].fillna(CONDITION_NA)
    if out.duplicated(['i', 'plot_id', 'crop']).any():
        raise ValueError(f'crop_production {wave}: a repeated (i, plot_id, crop)')
    out = out.set_index(['t', 'i', 'plot_id', 'crop', 'condition'])
    return out[['Quantity', 'Quantity_kg', 'u', 'Quantity_sold', 'Quantity_sold_kg', 'Unit_sold',
                'KgFactor_sold', 'Value_sold', 'months_harvested', 'planting_month',
                'harvest_month', 'intercropped', 'perennial', 'condition_native',
                'Condition_sold', 'condition_sold_native', 'Derivation']].sort_index()


def _ehcvm_crop_map(wave):
    """``harmonize_food`` by named rungs: an ``inputs`` callable runs from any cwd."""
    from pathlib import Path
    from lsms_library.paths import countries_root
    root = Path(countries_root()) / 'Mali'
    return tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Code',
        dirs=[f'{root / wave / "_"}/', f'{root / "_"}/'],
        **{'Preferred Label': 'Preferred Label'})


def _ehcvm_keyed(wave, name, crop):
    """One raw EHCVM crop file, as recorded, with the served ``t``, ``i`` and ``crop``."""
    from pathlib import Path
    from lsms_library.paths import countries_root
    path = Path(countries_root()) / 'Mali' / wave / 'Data' / name
    src = tools.get_dataframe(str(path), convert_categoricals=True)
    out = src.copy()
    out['t'] = wave
    out['i'] = ehcvm_household_ids(src, wave=wave).astype('string')
    out['crop'] = (src[crop].astype('string').str.strip().map(_ehcvm_crop_map(wave))
                   .astype('string'))
    return out


def _eaci17_keyed(name, crop):
    """One raw EAC-I 2017-18 crop file, as recorded, with the served ``t``, ``i``, ``crop``."""
    from pathlib import Path
    from lsms_library.paths import countries_root
    wave = '2017-18'
    src = tools.get_dataframe(str(Path(countries_root()) / 'Mali' / wave / 'Data' / name))
    out = src.copy()
    out['t'] = wave
    out['i'] = src.apply(lambda r: eaci17_i(pd.Series([r['grappe'], r['exploitation']])),
                         axis=1).astype('string')
    out['crop'] = (src[crop].astype('string').str.strip().map(_ehcvm_crop_map(wave))
                   .astype('string'))
    return out


def inputs_hh_crop_sale_single_row(wave):
    """Raw answers behind one wave's keyed rows, at (t, i, crop).

    The registry ``inputs`` callable for
    ``hh-crop-sale-single-row``: the s16d sale as recorded,
    with the field and parcel of the single s16c row it went on, one row per
    keyed served row.  2017-18 is the s7g sale and the s7f row; its household key is
    (grappe, exploitation), and its quantity codes are masked as the build masks them.
    """
    if wave == '2017-18':
        harvest = _eaci17_keyed('eaci17_s7fp2.dta', 's7fq03')
        sold = _eaci17_keyed('eaci17_s7gp2.dta', 's7gq01')
        quantity = pd.to_numeric(sold['s7gq21a'], errors='coerce')
        sold = sold[ehcvm_is_sale(quantity.mask(_is_quantity_sentinel(quantity)), sold['s7gq22'])]
        names = ['s7gq01', 's7gq20', 's7gq21a', 's7gq21b', 's7gq21d', 's7gq22']
        plot = ['s7fq01', 's7fq02']
    else:
        year = {'2021-22': '2021'}[wave]
        harvest = _ehcvm_keyed(wave, f's16c_me_mli{year}.dta', 's16cq04')
        sold = _ehcvm_keyed(wave, f's16d_me_mli{year}.dta', 's16dq01')
        sold = sold[ehcvm_is_sale(sold['s16dq05a'], sold['s16dq06'])]
        names = ['s16dq01', 's16dq04', 's16dq05a', 's16dq05b', 's16dq05c', 's16dq05d', 's16dq06']
        plot = ['s16cq02', 's16cq03']
    lines = harvest.groupby(_SALE_KEYS).size()
    single = lines[lines == 1].index
    harvest = harvest[harvest.set_index(_SALE_KEYS).index.isin(single)]
    out = sold[['t', 'i', 'crop'] + names].merge(
        harvest[_SALE_KEYS + plot], on=_SALE_KEYS, how='inner',
        validate='one_to_one')
    return out.set_index(['t', 'i', 'crop'])


def inputs_harvest_from_dispositions(wave):
    """The s16d answers behind one wave's derived totals, at (t, i, crop).

    The registry ``inputs`` callable for ``harvest-from-dispositions``: the four
    quantities and their units as recorded, one row per keyed served row.
    """
    year = {'2021-22': '2021'}[wave]
    harvest = _ehcvm_keyed(wave, f's16c_me_mli{year}.dta', 's16cq04')
    sold = _ehcvm_keyed(wave, f's16d_me_mli{year}.dta', 's16dq01')
    lines = harvest.assign(Quantity=harvest['s16cq16a'], finished=harvest['s16cq11'])
    totals = _harvest_totals(lines, sold)
    raw = [f'{var}{part}' for var in _DISPOSITIONS for part in 'ab']
    out = sold[sold.set_index(_SALE_KEYS).index.isin(totals.index)]
    return out[['t', 'i', 'crop', 's16dq01'] + raw].set_index(['t', 'i', 'crop'])


#: Per wave: the file, then "finished?", "share left", quantity and unit.  The
#: variable numbers flip between the two waves (CONTENTS.org).
_HARVEST_WINDOW_RAW = {
    '2018-19': ('s16c_me_mli2018.dta', 's16cq10', 's16cq11', 's16cq12a', 's16cq12b'),
    '2021-22': ('s16c_me_mli2021.dta', 's16cq11', 's16cq12', 's16cq16a', 's16cq16b'),
}

#: The window key also covers ``Quantity_kg``, where the wave asks kilograms (GH #1436).
_HARVEST_WINDOW_KG = {'2021-22': ['s16cq16c']}


def inputs_harvest_to_date(wave):
    """The answers behind one wave's window keys, at (t, i, plot_id, crop).

    The registry ``inputs`` callable for ``harvest-to-date``: "finished?", the share
    still to harvest, the quantity and its unit, and the kilograms where the wave asks
    them, as recorded.
    """
    name, finished, left, quantity, unit = _HARVEST_WINDOW_RAW[wave]
    src = _ehcvm_keyed(wave, name, 's16cq04')
    src['plot_id'] = ehcvm_plot_ids(src)
    keep = (src[finished].astype('string').str.strip().eq('Non')
            & pd.to_numeric(src[quantity], errors='coerce').notna())
    return src.loc[keep, ['t', 'i', 'plot_id', 'crop', finished, left, quantity, unit]
                   + _HARVEST_WINDOW_KG.get(wave, [])].set_index(['t', 'i', 'plot_id', 'crop'])


def inputs_survey_kg_estimate(wave):
    """The answers behind one wave's estimate keys, at (t, i, plot_id, crop).

    The registry ``inputs`` callable for ``survey-kg-estimate``: the quantity, its
    unit, the kilograms and the state, as recorded.
    """
    year = {'2021-22': '2021'}[wave]
    src = _ehcvm_keyed(wave, f's16c_me_mli{year}.dta', 's16cq04')
    src['plot_id'] = ehcvm_plot_ids(src)
    keep = derive_survey_kg_estimate(src['s16cq16c'], src['s16cq16a']).notna()
    return src.loc[keep, ['t', 'i', 'plot_id', 'crop', 's16cq16a', 's16cq16b', 's16cq16c',
                          's16cq16d']].set_index(['t', 'i', 'plot_id', 'crop'])


# ---------------------------------------------------------------------------
# plot_inputs (GAP 2; parity loop) — item-level (t, i, plot, input)
# ---------------------------------------------------------------------------
#
# This finalizer handles EACI plot applications (2014-15 EACI14, 2017-18
# EACI17). Earlier no-input wording was incorrect; EACI-only described pre1483 wiring.
# EHCVM household s16b inputs now use their separate source-bound adapter
# and wave wrappers, not this EACI finalizer. Their s16a parcel questions
# remain distinct and unwired; no household allocation or shared grain is
# inferred. See CONTENTS.org and 1483-input-source-contract.md's dated scope.
#
# Grain: one row per (t, i, plot, input), where
#   - i     = Mali composite household id (grappe, menage/exploitation),
#   - plot  = "{field}_{parcel}" — the SAME plot id as crop_production /
#             plot_features (s2cq01_s2cq02 etc.); seed rows additionally key
#             on their crop, and every non-seed row (not crop-specific)
#             carries the CROP_NA sentinel '(not crop-specific)' on `crop`,
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


# Sentinel for the `crop` index level on a plot_inputs row that is NOT
# crop-specific: every fertilizer / pesticide type row and both group
# use-answer rows ('Inorganic Fertilizer?' / 'Phytosanitary products?') --
# NON-SEED rows only.  A seed row is crop-specific: it carries its crop's
# harmonize_food Preferred Label or, where harmonize_food does not map the
# recorded s1eq03b / s11cq03 label, that recorded label itself, exactly as
# niger._crop_labels passes an unmapped crop through (Niger/_/niger.py:440-445;
# measured on a cold Niger build, 0 of 31,029 seed rows carry the marker).
# Inlined verbatim from niger.CROP_NA (Niger/_/niger.py:668; filled as at
# niger.py:693), the spelling all seven EHCVM siblings use (Benin,
# Burkina_Faso, CotedIvoire, Guinea-Bissau, Niger, Senegal, Togo).  WHY A
# SENTINEL, NOT NaN: a NaN on a declared index level is deleted by the first
# groupby that reaches it (pandas groupby(dropna=True)) -- Feature()'s
# assembly, the canonical de-dup, or the user's own aggregation -- and the
# loss is counted nowhere.  Which spelling becomes CANONICAL across
# countries is GH #1119's open decision; this only makes Mali agree with its
# region.
CROP_NA = '(not crop-specific)'


def _input_labels(series):
    """Map a Series of harmonize_input Codes -> Preferred Label."""
    m = tools.get_categorical_mapping(tablename='harmonize_input', idxvars='Code',
                                      **{'Preferred Label': 'Preferred Label'})
    out = series.astype('string').str.strip().map(m)
    return out.astype('string')


def plot_inputs_finalize(df):
    """Common post-processing for a plot_inputs wave DataFrame.

    Expects raw columns already assembled:
        t, i, plot, input (harmonize_input Code), crop (decoded crop name, or
        NA where the row is not crop-specific), u (decoded unit label or
        NA), Quantity, Purchased (nullable bool), Quantity_purchased,
        Improved (nullable bool), and optionally
        Applied (nullable bool; the plot-level group use answer, set only on
        the 'inorganic_fertilizer' / 'phytosanitary' group rows).
    Maps input/unit/crop labels, coerces dtypes, drops content-free rows
    (an input slot the household did not use -> no quantity, no purchase,
    no improved flag, no use answer), keeps a recorded crop label that
    harmonize_food does not map, fills `crop` with CROP_NA on every non-seed
    row, sets the (t, i, plot, input, crop) index, and collapses
    exact-duplicate index rows by summing the reported quantities and taking
    the first/any of the flags.
    """
    df = df.copy()
    df['input'] = _input_labels(df['input'])
    df['u'] = _unit_labels(df['u'])
    # An unmapped recorded crop keeps its own label, as niger._crop_labels
    # does (Niger/_/niger.py:440-445).  Done here, not in _crop_labels, which
    # crop_production shares.  Mali: 'Non-Exploitée' / 'Non exploitée',
    # 'Echalotte', 'Poireaux' (45 seed rows; Mali CONTENTS.org).
    raw_crop = df['crop'].astype('string').str.strip().replace('', pd.NA)
    df['crop'] = _crop_labels(df['crop']).fillna(raw_crop)
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
    # Applied: the survey's plot-level GROUP use answer (inorganic fertilizer /
    # phytosanitary products), served on its own row; NA on every type row.
    if 'Applied' not in df.columns:
        df['Applied'] = pd.NA
    df['Applied'] = df['Applied'].astype('boolean')

    # Drop content-free rows: an input slot for which the household reported
    # nothing positive.  A reported Quantity, a reported purchased quantity,
    # a positive Purchased flag, or a non-NA Improved flag all count as
    # content.  Purchased == False does NOT count (it is the ABSENCE of a
    # purchase — keeping it would emit a content-free row on every plot for a
    # fertilizer type the household neither applied here nor bought).
    has_content = (df['Quantity'].notna() | df['Quantity_purchased'].notna()
                   | df['Purchased'].fillna(False).astype('boolean')
                   | df['Improved'].notna() | df['Applied'].notna())
    df = df[has_content]

    # plot may be NA; carry it as an NA-able string so the index level is not
    # silently dropped.  A NON-SEED row (every fertilizer / pesticide type row,
    # both use-answer rows) is not crop-specific and carries the CROP_NA
    # sentinel, as in Niger/_/niger.py:693.  A seed row is never filled: its
    # crop is crop-specific even when unknown (none is blank today).  Filled
    # BEFORE the collapse; the collapse already grouped NA crops together
    # (dropna=False), so no group changes membership.
    df['plot_id'] = df['plot_id'].astype('string')
    df['crop'] = df['crop'].astype('string')
    df.loc[df['input'].ne('Seed') & df['crop'].isna(), 'crop'] = CROP_NA

    keys = ['t', 'i', 'plot_id', 'input', 'crop']
    # Collapse any exact (t,i,plot,input,crop) duplicates: sum reported
    # amounts (min_count=1 keeps an all-NA group NA, not a spurious 0),
    # first/any for flags.  dropna=False so plot NA rows survive.
    g = df.groupby(keys, dropna=False, as_index=True)
    out = pd.DataFrame({
        'Quantity':           g['Quantity'].sum(min_count=1),
        'Quantity_purchased': g['Quantity_purchased'].sum(min_count=1),
        'u':                  g['u'].first(),
        'Purchased':          g['Purchased'].max(),
        'Improved':           g['Improved'].max(),
        'Applied':            g['Applied'].max(),
    })
    out = out[['Quantity', 'u', 'Purchased', 'Quantity_purchased', 'Improved',
               'Applied']]
    # GH #1118 under the #1041 ruling: a retained record with no unit carries
    # the canonical sentinel, never NaN.  Filled AFTER the collapse so a blank
    # duplicate cannot displace a real unit via g['u'].first().  Measured on
    # the EAC-I sources, every such null is a blank raw unit or the survey's
    # own 'Manquant' label; no substantive label is lost by _unit_labels.
    out['u'] = out['u'].fillna(U_UNKNOWN)
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
#   SalesValue       — gross value of those sales (FCFA) where the source
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
        HeadSold, SalesValue.
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

    for c in ('HeadCount', 'HeadAcquired', 'HeadSold', 'SalesValue'):
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
                   | df['SalesValue'].fillna(0).gt(0))
    df = df[has_content]

    keys = ['t', 'i', 'animal']
    g = df.groupby(keys, dropna=False, as_index=True)
    out = pd.DataFrame({
        'HeadCount':    g['HeadCount'].sum(min_count=1),
        'HeadAcquired': g['HeadAcquired'].sum(min_count=1),
        'HeadSold':     g['HeadSold'].sum(min_count=1),
        'SalesValue':   g['SalesValue'].sum(min_count=1),
    })
    out = out[['HeadCount', 'HeadAcquired', 'HeadSold', 'SalesValue']]
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

    df['plot_id'] = df['plot_id'].astype('string')

    keys = ['t', 'i', 'plot_id', 'source']
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
# community_prices (GAP C; parity loop) - (t, v, j, u, variety, visit, obs)
# ---------------------------------------------------------------------------
#
# One row per SURVEY-RECORDED price: the price the EACI community price
# questionnaire records for one sale-form VARIETY of a food in a cluster, in
# one field passage.  CLUSTER-level — there is NO household i, so v is NATIVE
# (the community questionnaire's grappe, the SAME keyspace as sample().v, so
# the price joins households) and the framework's _join_v_from_sample does not
# fire (it only augments tables that carry i).
#
# The community price module lives in the 2014-15 EACI wave ONLY
# (EACIS04_p1 post-planting passage 1 + EACIS04_p2 post-harvest passage 2,
# rec_type 8, grappe-level, no menage).  The 2017-18 EACI wave dropped the
# community questionnaire.  (The EHCVM waves are NOT all region-grain: 2018-19
# ships a grappe-grain s05_co_mli2018.dta that is unwired -- see CONTENTS.org
# "The EHCVM waves DO ship a price file".)
#
# Grain: one row per (t, v, j, u, variety, visit, obs), where
#   - t       = wave ('2014-15'),
#   - v       = grappe (cluster) as a string — sample().v keyspace,
#   - j       = harmonize_food Preferred Label, decoded from the broad item
#               label s04q01 (Riz, Maïs, Oignon frais, ...); REUSES the
#               consumed-food label so community_prices.j joins
#               food_acquired.j / crop_production.j,
#   - u       = u-table Preferred Label, decoded from the unit label s04q09
#               (Kilogramme -> Kg, Sac moyen (50 kg) -> Sac moyen, ...),
#   - variety = the s04q02 sale-form variety CODE as a string (GH #840):
#               product-prefixed (code // 10 == the s04q01 code, measured on
#               every row of both passages), so it is unique within j and
#               identical across the two passages, whose value LABELS are not
#               (p1 quotes "Aicha", p2 'Aicha'),
#   - visit   = the field passage: 1 post-planting, 2 post-harvest,
#   - obs     = the form's reading slot: 1, 2 or 3 (GH #822).
#
# REPORTED columns only:
#   Price       = s04q05/q08/q11, surveyed FCFA for a Quantity-sized lot.
#   Quantity    = s04q04/q07/q10, the lot's weight in kg, litres for a volume: a
#                 gram or centilitre Poids of 5 or more is divided
#                 (derive_poids_base_unit); other rows are as recorded, whatever
#                 the column head says.  999 / 9999 sentinel is coerced to NA.
#   Description = the s04q02 value label of that passage's file ("Riz importé
#                 vendu en sac", "Boîte de 220g de marque 'Aicha'", ...).
#
# NOTHING IS SELECTED (GH #840; @ligon 2026-10-18, the #871 principle applied to
# the wave script).  Until #840 the declared grain was (t, v, j, u) and one row
# per cell was KEPT -- post-harvest passage first, then questionnaire order --
# from 40,218 priced rows down to 23,646, with a comment here claiming the
# colliding prices were "near-identical".  They were not: Concentré de tomate
# under u='Boîte' is 600 / 125 / 225 FCFA for three varieties (CONTENTS.org).
# The (t, v, j, u) choice the old build made is recoverable from this table:
# per (t, v, j, u), the highest visit, then the lowest variety code
# (questionnaire order), on obs=3. GH #822 restores readings 1/2 on their
# own obs slots while preserving the original reading-3 keep/drop sets.

# s04q09 unit labels that are numeric-coded data artifacts (a handful of
# rows where the unit was entered as a number / the missing sentinel); they
# do not map through the u table and are dropped.
_PRICE_QTY_SENTINELS = {999, 9999}

COMMUNITY_PRICES_INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']

#: Registry key on a row whose recorded Poids was in the row's own gram or centilitre.
DERIVATION_POIDS = 'Mali::community_prices::gram-centilitre-poids'

#: (served unit, passage) -> what divides a Poids in that unit into kg (litres for
#: a volume).  Written per passage because the two passages behave differently and
#: each cell below was measured on its own (CONTENTS.org, GH #1179 memo).
_POIDS_DIVISOR = {('Gramme', 1): 1000.0, ('Gramme', 2): 1000.0,
                  ('Centilitre', 1): 100.0, ('Centilitre', 2): 100.0}
#: At or above this a Poids on such a row is in the row's own unit.  Below 1 it is
#: already kg or litres.  From 1 up to here as recorded is the better reading and
#: not an established one: dividing fits nothing, as recorded fits under half.
_POIDS_NATIVE_FLOOR = 5.0
#: The two passage files' three readings: unit, weight, price.
_COMMUNITY_READINGS = (('s04q03', 's04q04', 's04q05'),
                       ('s04q06', 's04q07', 's04q08'),
                       ('s04q09', 's04q10', 's04q11'))


def derive_poids_base_unit(poids, unit, visit):
    """``Quantity`` from the recorded ``Poids``, in kg (litres for a volume).

    The column is headed "Poids (KG)", and both enumerator manuals say "Le poids et
    l'unite correspondante sont lies": their worked example is 2.25 kg entered Poids
    2250, Unite 2, Gramme.  So on a ``Gramme`` or ``Centilitre`` row a Poids of 5 or
    more is in that unit, and is divided here, in either passage.  Every other value
    stays as recorded: below 1 it is already kg or litres (a 33 cl bottle at 0.33), and
    from 1 up to 5 dividing fits nothing while as recorded fits under half, so it is
    counted rather than decided.  Rows on any other unit stay as recorded.  Registered as
    ``Mali::community_prices::gram-centilitre-poids``.
    """
    poids = pd.to_numeric(poids, errors='coerce')
    divisor = pd.Series([_POIDS_DIVISOR.get(cell) for cell in zip(unit, visit)],
                        index=poids.index, dtype='float64')
    native = poids.ge(_POIDS_NATIVE_FLOOR).fillna(False) & divisor.notna()
    return poids.where(~native, poids / divisor)


def inputs_gram_centilitre_poids(wave):
    """The answers behind one wave's keyed rows, at the served community index.

    The registry ``inputs`` callable for ``gram-centilitre-poids``: the item, the
    variety and all three unit / weight / price triples of the source row, as recorded;
    ``obs`` says which triple the served row read.  Unit codes 2 and 4 are Gramme and
    Centilitre in both passages, where passage 2 omits two of the label sets.
    """
    from pathlib import Path
    from lsms_library.paths import countries_root
    data = Path(countries_root()) / 'Mali' / wave / 'Data'
    crop_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Code',
        dirs=[f'{data.parent / "_"}/', f'{data.parent.parent / "_"}/'],
        **{'Preferred Label': 'Preferred Label'})
    units = {2: 'Gramme', 4: 'Centilitre'}
    raw = ['s04q01', 's04q02'] + [var for triple in _COMMUNITY_READINGS for var in triple]
    pieces = []
    for visit in (1, 2):
        path = str(data / f'EACIS04_p{visit}.dta')
        codes = tools.get_dataframe(path, convert_categoricals=False)
        labels = tools.get_dataframe(path)
        for obs, (unit, weight, price) in enumerate(_COMMUNITY_READINGS, 1):
            keep = (pd.to_numeric(codes[unit], errors='coerce').isin(units)
                    & pd.to_numeric(codes[price], errors='coerce').gt(0)
                    & pd.to_numeric(codes[weight], errors='coerce').ge(_POIDS_NATIVE_FLOOR)
                    & ~pd.to_numeric(codes[weight], errors='coerce').isin(_PRICE_QTY_SENTINELS))
            out = codes.loc[keep, raw].copy()
            out['s04q01'] = labels.loc[keep, 's04q01'].astype('string').str.strip()
            out['s04q02'] = labels.loc[keep, 's04q02'].astype('string').str.strip()
            out['t'] = wave
            out['v'] = codes.loc[keep, 'grappe'].astype('Int64').astype('string')
            out['j'] = out['s04q01'].map(crop_map).astype('string')
            out['u'] = pd.to_numeric(codes.loc[keep, unit]).map(units).astype('string')
            out['variety'] = pd.to_numeric(codes.loc[keep, 's04q02']).astype('Int64').astype('string')
            out['visit'] = visit
            out['obs'] = obs
            pieces.append(out[out['j'].notna()])
    return pd.concat(pieces, ignore_index=True).set_index(COMMUNITY_PRICES_INDEX)


def community_prices_finalize(df):
    """Post-process a community_prices wave DataFrame.

    Expects raw columns already assembled:
        t, v, j (decoded item label), u (decoded unit label), variety (the
        s04q02 code as a string), visit (the passage), obs (reading 1/2/3), Description (the
        s04q02 label), Price, Quantity.
    Maps j/u labels via harmonize_food / u, drops unmapped item/unit rows and
    rows with no positive price, coerces the Price/Quantity dtypes and the
    999 Quantity sentinel, reads a gram or centilitre Poids into kg or litres
    (:func:`derive_poids_base_unit`), and indexes the result by
    (t, v, j, u, variety, visit, obs) -- one row per reported reading. No row
    is selected; a duplicate key is an error (GH #840).
    """
    df = df.copy()
    df['j'] = _crop_labels(df['j'])   # harmonize_food Code -> Preferred Label
    df['u'] = _unit_labels(df['u'])   # u Code -> Preferred Label
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    df.loc[df['Quantity'].isin(_PRICE_QTY_SENTINELS), 'Quantity'] = pd.NA

    df['v'] = df['v'].astype('string')
    df['variety'] = df['variety'].astype('string')
    df['visit'] = pd.to_numeric(df['visit']).astype('int64')
    obs = pd.to_numeric(df['obs'])
    assert obs.isin([1, 2, 3]).all(), "community_prices: obs must be a reading slot 1/2/3"
    df['obs'] = obs.astype('int64')
    df['Description'] = df['Description'].astype('string').str.strip()
    # Keep only resolvable, genuinely-priced item-unit rows.
    df = df[df['j'].notna() & df['u'].notna() & df['Price'].notna() & (df['Price'] > 0)].copy()
    assert df['variety'].notna().all(), "community_prices: a priced row has no s04q02 code"
    poids = df['Quantity']
    df['Quantity'] = derive_poids_base_unit(poids, df['u'], df['visit'])
    df['Derivation'] = pd.Series(pd.NA, index=df.index, dtype='string')
    df.loc[poids.notna() & df['Quantity'].ne(poids), 'Derivation'] = DERIVATION_POIDS

    dup = df.duplicated(COMMUNITY_PRICES_INDEX, keep=False)
    assert not dup.any(), (
        f"community_prices: {int(dup.sum())} rows share a "
        f"{tuple(COMMUNITY_PRICES_INDEX)} key -- the instrument records one "
        "price per (cluster, variety, passage, reading), so this is a broken key, not "
        "a cell to select from (GH #840)")

    # community_prices is a CLUSTER table — there is no household i.  But the
    # framework's local_tools.map_index() (run on EVERY read path) swaps
    # j -> i whenever a `j` index level is present and an `i` level is NOT.
    # Carry a redundant `i` level (set equal to the cluster v) positioned
    # BEFORE `j`: map_index then does NOT swap, and the framework's
    # _normalize_dataframe_index drops the undeclared `i`, leaving the declared
    # grain.  v already in the index means _join_v_from_sample is skipped, so
    # the spurious i==v never reaches the API.  (Same framework-compatibility
    # shim Tanzania/Malawi/Ethiopia community_prices use — see Tanzania
    # community_prices_for_wave.)
    df['i'] = df['v']
    out = (df.set_index(['t', 'v', 'i', 'j', 'u', 'variety', 'visit', 'obs'])
             [['Price', 'Quantity', 'Description', 'Derivation']]
             .sort_index())
    return out
