"""Build plot_features for CotedIvoire EHCVM 2018-19 (GH #167; EHCVM cluster).

CotedIvoire is the one EHCVM country that was missing plot_features.  This
clones the Niger/Mali EHCVM reference (niger.py:plot_features_for_wave,
PR #284) inline so the wave script is self-contained.

Single source file: ../Data/Menage/s16a_me_CIV2018.dta (agriculture-parcel
module).  plot_id = "{field_no}_{parcel_no}" (s16aq02 _ s16aq03); unique
within each (grappe, menage).  The s16a code schemes (tenure s16aq10,
tenure_system / certificate s16aq13, soil s16aq18, water s16aq17, fertility
s16aq20, area est s16aq09a/b, GPS area s16aq47 / flag s16aq45) verified to
match the Niger EHCVM 2018-19 reference; the harmonize_* code tables were
cloned into CotedIvoire's categorical_mapping.org.

i is CotedIvoire's EHCVM composite id (grappe + zero-padded(3) menage; NO
'E_' prefix — CotedIvoire predates the standard EHCVM list), inlined here.
Returns a DataFrame indexed by (t, i, plot_id) with the same canonical
columns as the Niger reference: Area (ha float), AreaUnit, AreaSelfReported,
Tenure, TenureSystem, PlotCertificate (nullable bool), SoilType,
SoilFertility, Irrigated (nullable bool).
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id, get_categorical_mapping


def _i(grappe, menage):
    g = format_id(grappe)
    m = format_id(menage, zeropadding=3)
    if g is None or m is None:
        return None
    return g + m


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a {int code -> Preferred Label} dict; blank / '---' map to NA.
    Inlined copy of niger.py:_harmonized_codes."""
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
    """Map a raw integer-code Series through code_map -> string Series with
    NA where unmapped.  Inlined copy of niger.py:_map_codes."""
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_features_for_wave(t, source, colmap):
    """Build canonical plot_features for one EHCVM wave.  Inlined copy of
    niger.py:plot_features_for_wave (the Mali sibling contract)."""
    tenure_map = _harmonized_codes('harmonize_tenure')
    tenure_system_map = _harmonized_codes('harmonize_tenure_system')
    soil_map = _harmonized_codes('harmonize_soil')
    water_map = _harmonized_codes('harmonize_water')

    c = colmap

    # Drop placeholder rows for non-farming households (no field/parcel).
    src = source[source[c['field_no']].notna() & source[c['parcel_no']].notna()].copy()

    g_col, m_col = c['grappe'], c['menage']
    hh = src.apply(lambda r: _i(r[g_col], r[m_col]), axis=1)

    field = src[c['field_no']].apply(format_id)
    parcel = src[c['parcel_no']].apply(format_id)
    plot_id = field.astype(str) + '_' + parcel.astype(str)

    # Area in hectares.  GPS where measured, else farmer estimate converted
    # from its declared unit (1=Hectare ->x1, 2=m^2 ->/10000).
    area_gps = pd.to_numeric(src[c['area_gps']], errors='coerce').astype('Float64')
    gps_flag = src[c['gps_measured']].astype('Int64')

    est_raw = pd.to_numeric(src[c['area_est']], errors='coerce').astype('Float64')
    est_unit = src[c['area_est_unit']].astype('Int64')
    est_ha = est_raw.where(est_unit != 2, est_raw / 10000)

    area_ha = est_ha.copy()
    use_gps = (gps_flag == 1) & area_gps.notna()
    area_ha = area_gps.where(use_gps, area_ha)

    # Plausibility clamp (GH #327): NaN out implausible areas (>1000 ha or
    # non-positive); rows are kept, only the Area value is dropped.
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

    # AreaSelfReported (WB parity): the reported farmer-estimate parcel area
    # in hectares, carried distinct from GPS-preferred Area.
    area_self = est_ha.where(((est_ha > 0) & (est_ha <= 1000)) | est_ha.isna(), pd.NA)

    # PlotCertificate (WB parity): s16aq13 codes 1-5 = a formal land document
    # -> True; code 7 (Aucun) -> False; code 6 (Autre) and missing -> NA.
    plot_certificate = pd.Series(pd.NA, index=src.index, dtype='boolean')
    if c.get('certificate') in src.columns:
        cert_code = src[c['certificate']].astype('Int64')
        has_doc = cert_code.isin([1, 2, 3, 4, 5])
        plot_certificate = has_doc.astype('boolean')
        plot_certificate = plot_certificate.where(cert_code.isin([1, 2, 3, 4, 5, 7]), pd.NA)

    # SoilFertility (WB parity): s16aq20 1=Bonne -> good, 2=Moyenne -> medium,
    # 3=Faible -> poor (verified identical labels in CotedIvoire).
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
    # Drop rows with no valid household id before indexing.
    df = df[df['i'].notna()]
    df = df.set_index(['t', 'i', 'plot_id'])
    return df


# convert_categoricals=False keeps the integer s16a codes the harmonize_*
# tables key on.
src = get_dataframe('../Data/Menage/s16a_me_CIV2018.dta', convert_categoricals=False)

colmap = dict(
    grappe        = 'grappe',
    menage        = 'menage',
    field_no      = 's16aq02',
    parcel_no     = 's16aq03',
    area_gps      = 's16aq47',
    gps_measured  = 's16aq45',
    area_est      = 's16aq09a',
    area_est_unit = 's16aq09b',
    tenure        = 's16aq10',
    tenure_system = 's16aq13',
    certificate   = 's16aq13',  # WB plot_certificate: has-any-legal-document bool
    soil_type     = 's16aq18',
    water_source  = 's16aq17',
    fertility     = 's16aq20',  # WB soil-quality tag: reported good/medium/poor
)

df = plot_features_for_wave('2018-19', src, colmap)

assert df.index.is_unique, "Non-unique (t, i, plot_id) index in plot_features 2018-19"
assert len(df) > 0, "plot_features 2018-19 produced no rows"

to_parquet(df, 'plot_features.parquet')
