# Shared formatting functions for Albania.
"""Country-level helpers for Albania.

Currently hosts the ``plot_features`` harmonization shared across the four
buildable waves (2002, 2005, 2008, 2012).  Each wave's per-plot agriculture
roster is a clean LONG table (one row per (household, plot)) carrying a plot
area in **square meters**; this module converts area to the canonical
hectares and maps the Albanian acquisition / land-use / legal-document
vocabularies onto the cross-country ``plot_features`` schema
(``lsms_library/data_info.yml``).

Wave source modules (all confirmed plot rosters via variable labels):

  2002  Data/agr_a1_cl.dta                       (mca1_q* ; plot code mca1_q0a)
  2005  Data/agric/part1_roster_a.dta            (p1a_q*  ; plot code p1a_q00)
  2008  Data/Modul_17_id_of_agric_household.sav  (m17_q*  ; plot code m17_q00)
  2012  Data/Modul_17_..._Hh_Q3-9.sav            (M17_Q*  ; NO plot code -> synth)

The panel waves 2003 (w1/w2_hh_farm.dta) and 2004 (w3_hh_farm.dta) carry only
a WIDE household-level farm questionnaire with cryptic, undocumented column
labels ("1 m8a_q03", ...) and no decodable per-plot area / plot identity, so
they are intentionally NOT wired (see CONTENTS.org).
"""
import pandas as pd
import lsms_library.local_tools as tools


# Authoritative wave list.  Mirrors the ``Waves:`` block in this folder's
# ``data_scheme.yml`` and, crucially, EXCLUDES 1996 (Employment & Welfare
# Survey -- not LSMS; the ``1996/`` dir carries only Documentation).  This
# attribute is required because ``Country.waves`` (country.py) consults a
# ``_/{country}.py`` module's ``waves`` BEFORE the data_scheme ``Waves:`` list;
# when the module exists but defines no ``waves``, the property falls through to
# a directory glob that silently re-includes ``1996/`` (it has
# Documentation/SOURCE.org), reviving the documented exclusion.  See GH #445.
waves = ['2002', '2003', '2004', '2005', '2008', '2012']


# --- canonical-value maps ----------------------------------------------------
#
# Tenure: how the household came to hold the plot.  Albania's land was almost
# entirely state-owned until the 1991 privatisation (Law 7501), so the dominant
# category is a country-specific ``privatised`` value (the schema explicitly
# permits extending the spellings list rather than force-fitting).  The rest map
# onto canonical ``inherited`` / ``owned`` (purchased) / ``squatted`` (cleared /
# walked-in) / ``other_tenure``.
_TENURE = {
    'privatised':              'privatised',
    'privatized':              'privatised',   # 2012 spelling
    'privatized (l. 7501)':    'privatised',   # 2012 label text
    'inherited':               'inherited',
    'inhereted':               'inherited',     # 2012 typo in source
    'inhereted (before 1946)': 'inherited',
    'purchased':               'owned',
    'cleared':                 'squatted',
    'tribunal':                'other_tenure',
    'tribunal decision':       'other_tenure',
    'other':                   'other_tenure',
}

# TenureSystem: lasting legal-document / title regime.  Albania records a
# title-document type rather than a tenure *system*; we surface it under
# TenureSystem as a country-specific vocabulary (deed / usufruct / sales
# receipt / will / none / other), per the schema's "countries may extend".
_TENURE_SYSTEM = {
    'deed':                         'deed',
    'deed (from law 7501 in 1991)': 'deed',
    'deed (from before 1946)':      'deed',
    'usufruct':                     'usufruct',
    'sales receipt':                'sales_receipt',
    'will':                         'will',
    'tribunal document':            'tribunal_document',
    'none':                         'none',
    'other':                        'other_tenure_system',
}

# SoilType: Albania records "kind of land" (annual crop / tree crop / pasture /
# forest / pond / other), not a soil texture.  Carried under SoilType verbatim
# (lower-cased, whitespace-normalised) as the only lasting land-character field
# available.
_SOIL = {
    'annual crop land':         'annual crop land',
    'annual crop':              'annual crop land',  # 2005 cropping_use wording
    'tree crop land':           'tree crop',
    'tree crop':                'tree crop',
    'pasture':                  'pasture',
    'pasture/ natural meadow':  'pasture',           # 2005 cropping_use wording
    'forest':                   'forest',
    'pond':                     'pond',
    'left fallow':              'fallow',            # 2005 cropping_use wording
    'rented/loaned to others':  'rented out',        # 2005 cropping_use wording
    'loaned to a relative':     'rented out',
    'given in sharecropping':   'sharecropped out',
    'other':                    'other',
}


def _clean_label(series):
    """Lower-case, whitespace-collapse a string/categorical label Series for
    case-insensitive mapping; NA stays NA."""
    s = series.astype('string')
    s = s.str.strip().str.lower().str.replace(r'\s+', ' ', regex=True)
    return s


def _map_labels(series, mapping):
    """Map cleaned labels through ``mapping``; unmapped non-NA labels pass
    through (already cleaned) so country-specific values are preserved."""
    cleaned = _clean_label(series)
    out = cleaned.map(lambda x: mapping.get(x, x) if pd.notna(x) else pd.NA)
    return out.astype('string')


def plot_features_for_wave(t, src, colmap):
    """Build canonical ``plot_features`` for one Albania wave.

    Parameters
    ----------
    t : str
        Wave id (used as the ``t`` index value).
    src : pd.DataFrame
        Raw per-plot roster, loaded with ``convert_categoricals=True`` so the
        attribute columns arrive as human-readable label strings.
    colmap : dict
        Column-name map.  Keys::

            psu, hh        — together build the canonical household id ``i``
            i_style        — 'psu-hh' (2002/2005/2008) or 'hhid' (2012:
                             psu*100+hh)
            plot_code      — within-HH plot code column, or None to synthesise
                             a 1..n sequence per household (2012)
            area_sqm       — plot area in square meters
            tenure         — acquisition / origin-of-land question (-> Tenure)
            tenure_system  — legal-document / title question (-> TenureSystem)
            soil_type      — kind-of-land question (-> SoilType)
            irrigated      — yes/no irrigation question (-> Irrigated), or None

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with the canonical columns
    that exist for this wave.  ``Area`` is hectares (square meters / 10000);
    ``AreaUnit`` is always ``'sq. meters'`` (the source unit) where Area is
    present, NA otherwise.
    """
    c = colmap
    df = src.copy()

    # Household id (matches each wave's sample.py / mapping.py:i()).
    if c.get('i_style') == 'hhid':
        i = df.apply(
            lambda r: tools.format_id(int(r[c['psu']]) * 100 + int(r[c['hh']])),
            axis=1)
    else:
        i = df.apply(
            lambda r: tools.format_id(r[c['psu']]) + '-' + tools.format_id(r[c['hh']]),
            axis=1)

    # Plot id: real plot code where present, else a 1..n sequence per HH
    # (stable in source row order) for waves whose roster carries no code.
    if c.get('plot_code'):
        plot_id = df[c['plot_code']].apply(tools.format_id)
    else:
        plot_id = (df.groupby(i.values).cumcount() + 1).map(tools.format_id)

    # Area: square meters -> hectares.
    area_sqm = pd.to_numeric(df[c['area_sqm']], errors='coerce').astype('Float64')
    area_ha = area_sqm / 10000
    area_unit = pd.Series('sq. meters', index=df.index, dtype='string')
    area_unit = area_unit.where(area_ha.notna(), pd.NA)

    cols = {
        't':        t,
        'i':        i.values,
        'plot_id':  plot_id.values,
        'Area':     area_ha.values,
        'AreaUnit': area_unit.values,
    }

    if c.get('tenure') and c['tenure'] in df.columns:
        cols['Tenure'] = _map_labels(df[c['tenure']], _TENURE).values
    if c.get('tenure_system') and c['tenure_system'] in df.columns:
        cols['TenureSystem'] = _map_labels(df[c['tenure_system']], _TENURE_SYSTEM).values
    if c.get('soil_type') and c['soil_type'] in df.columns:
        cols['SoilType'] = _map_labels(df[c['soil_type']], _SOIL).values
    if c.get('irrigated') and c['irrigated'] in df.columns:
        flag = _clean_label(df[c['irrigated']])
        irr = flag.map({'yes': True, 'no': False})
        cols['Irrigated'] = irr.where(flag.notna(), pd.NA).astype('boolean').values

    out = pd.DataFrame(cols).set_index(['t', 'i', 'plot_id'])

    # Collapse the rare genuine (i, plot_id) collisions (3 in 2005: two
    # distinct plots share a plot code).  Disambiguate by appending a suffix
    # so each physical plot keeps its own row rather than being dropped.
    if not out.index.is_unique:
        out = out.reset_index()
        dup = out.duplicated(subset=['t', 'i', 'plot_id'], keep=False)
        seq = out.groupby(['t', 'i', 'plot_id']).cumcount().add(1).map(str)
        out.loc[dup, 'plot_id'] = out.loc[dup, 'plot_id'] + '_' + seq[dup]
        out = out.set_index(['t', 'i', 'plot_id'])

    return out
