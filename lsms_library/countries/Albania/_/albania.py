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
import warnings

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


def albania_date_ymd(row):
    """Combine a [year, month, day] row into a Timestamp.

    Used by the ``interview_date`` table.  Albania's cover files record the
    fieldwork interview date as three separate numeric columns -- "Day /
    Month / Year of Interview" (2002 ``m0_q08d/m/y``, 2005 ``m0_q8d/m/y``)
    or "date of enumeration (day/month/year)" (2004 ``m0_q04d/m/y``).
    Declare the columns in **year, month, day** order in the wave's
    data_info.yml ``int_t`` myvar with a trailing
    ``mapping: albania_date_ymd``.

    The month component is numeric (e.g. ``5``); it is handled by building a
    'DAY MONTH YEAR' string and letting ``pd.to_datetime`` parse it (a name
    like 'MAY' would also work).  Returns ``pd.NaT`` when any part is missing
    or the date is unparseable -- so households with no recorded interview
    date (e.g. an empty Visit-2 column) drop out rather than being fabricated.

    The cover files load with ``convert_categoricals=True``, so a numeric
    cell with no Stata value label can arrive as the sentinel string
    ``'***Undefined Label'`` (and day/year as label-typed objects); each
    component is therefore coerced with ``pd.to_numeric`` and any
    non-numeric / undefined value yields ``pd.NaT`` rather than raising.

    Mirrors :func:`lsms_library.countries.Malawi._.malawi.malawi_date_ymd`.
    """
    y, m, d = row.iloc[0], row.iloc[1], row.iloc[2]
    if pd.isna(y) or pd.isna(m) or pd.isna(d):
        return pd.NaT
    # Year and day are always numeric calendar components; coerce defensively
    # (categorical-label objects / '***Undefined Label' sentinels -> NaT).
    y_num = pd.to_numeric(y, errors='coerce')
    d_num = pd.to_numeric(d, errors='coerce')
    if pd.isna(y_num) or pd.isna(d_num):
        return pd.NaT
    # Month may be a numeric code (5 / 5.0) or an English name ('MAY').
    # A numeric month is parsed with an explicit %Y-%m-%d format so a string
    # like '4 5 2002' is never mis-read as April-5 (the day/month order of a
    # bare 'D M Y' string is ambiguous to pd.to_datetime); a month *name*
    # ('MAY') is unambiguous and is parsed without a format.
    m_num = pd.to_numeric(m, errors='coerce')
    if pd.notna(m_num):
        month = int(m_num)
        return pd.to_datetime(
            f"{int(y_num):04d}-{month:02d}-{int(d_num):02d}",
            format="%Y-%m-%d", errors='coerce')
    if isinstance(m, str) and m.strip():
        return pd.to_datetime(
            f"{int(d_num)} {m.strip()} {int(y_num)}", errors='coerce')
    return pd.NaT


def interview_date(df):
    """Melt Albania's per-visit interview dates onto a ``visit`` index level.

    Country-level ``df_edit`` hook for the ``interview_date`` table
    (auto-dispatched by table name; see country.py ``grab_data`` ->
    ``formatting_functions.get(request)``).  Mirrors
    :func:`lsms_library.countries.Malawi._.malawi.interview_date`.

    Per GH #474 the Albania waves previously wired ``Int_t`` to ``m0_date``,
    which the source labels as "Date of last modification" / "data entry
    date" -- a data-entry timestamp, NOT the fieldwork interview date.  The
    real interview date lives in the "Day / Month / Year of Interview"
    question columns, combined by :func:`albania_date_ymd` into ``Int_t``
    (Visit 1) and -- where the wave's cover records a second enumerator visit
    (2002 ``m0_q08*2``) -- ``Int_t_v2`` (Visit 2).

    Input
    -----
    df : pd.DataFrame
        Indexed per the wave's ``idxvars`` (after the framework prepends
        ``t``, i.e. ``(t, i)``), carrying the Visit-1 interview date in a
        ``int_t`` / ``Int_t`` column and -- where the wave records a second
        visit -- the Visit-2 date in ``int_t_v2`` / ``Int_t_v2``.  Both
        casings are accepted defensively (the ``int_t -> Int_t``
        rejected-spelling rewrite has NOT run at this wave-level hook stage).

    Output
    ------
    pd.DataFrame indexed ``(<original idx>, visit)`` -- i.e. ``(t, i,
    visit)`` -- with a single ``Int_t`` datetime column.  ``visit`` is an
    ordinal Int64 level: ``1`` carries the Visit-1 date, ``2`` the Visit-2
    date.  Visit rows whose date is NaT are DROPPED -- never fabricated -- so
    a single-visit wave (2004, 2005) yields only ``visit=1`` rows, and the
    2002 Visit-2 columns (present in the questionnaire but empty in this data
    file) yield no ``visit=2`` rows.

    Delegates to the shared
    :func:`lsms_library.local_tools.melt_visit_intervals` helper with
    ``start_base='int_t'`` and the legacy ``Int_t`` output column (Albania
    records only an interview *date* per visit, no separate start/end).
    """
    return tools.melt_visit_intervals(df, start_base='int_t', out_start='Int_t')


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


# ---------------------------------------------------------------------------
# cluster_features: explicit, CHECKED reduction to cluster grain (GH #323)
# ---------------------------------------------------------------------------

# The 2004 panel follow-up reuses the ORIGINAL 2002 PSU code in m0_q01 for every
# traced household, plus two ADMINISTRATIVE sentinels that are not clusters at
# all.  Verified against 2002 (see CONTENTS.org / .coder/ledger/323-albania.md):
# 0 of the 83 sentinel households key into 2002's (psu, hh), whereas 1713 of the
# 1714 real ones do.
ALBANIA_2004_SENTINEL_PSUS = (995, 999)


def cluster_reduce(df, columns, v='v', wave=None):
    """Reduce a household-/person-grain frame to CLUSTER grain, with a CHECK.

    ``cluster_features`` is declared at cluster grain -- index ``(t, v)`` -- but
    every Albania wave extracts it from a household cover page (2003: from the
    *person* roster).  The extra household level then reaches
    ``_normalize_dataframe_index``, which drops it and collapses the leftover
    duplicate ``(t, v)`` tuples with ``groupby().first()``: a SILENT reduction
    that discards 3,149 / 8,229 / 1,347 / 3,360 rows in 2002 / 2003 / 2004 /
    2005 and, in 2004, silently picked a *mover household's* current district as
    the cluster's district (GH #323).

    This makes the reduction explicit and enforced, rather than implicit and
    silent:

    - a row whose cluster id ``v`` is null is DROPPED (you cannot key a cluster
      row on a null cluster id) -- with a warning saying how many;
    - a declared column must carry exactly ONE distinct non-NA value within a
      cluster.  A cluster that violates this gets ``<NA>`` for the offending
      column and a ``RuntimeWarning`` naming the clusters.  Emitting a value we
      cannot justify would be silently WRONG (class-1); ``<NA>`` is merely
      silently MISSING (class-2), which is strictly safer.

    Returns a frame indexed by ``v`` with exactly ``columns``, one row per
    cluster, and a provably unique index.
    """
    tag = f"Albania/{wave} cluster_features" if wave else "Albania cluster_features"
    out = df.copy()

    n0 = len(out)
    out = out[out[v].notna()]
    if len(out) < n0:
        warnings.warn(
            f"{tag}: dropped {n0 - len(out)} source row(s) with a null cluster "
            f"id ({v}); a cluster row cannot be keyed on a null cluster id "
            f"(GH #323).",
            RuntimeWarning,
        )

    for col in columns:
        # nunique(dropna=True): a cluster mixing a value with NA is fine (the
        # value wins); a cluster mixing two VALUES is not.
        bad = out.groupby(v, observed=True)[col].nunique(dropna=True)
        bad = bad[bad > 1]
        if len(bad):
            warnings.warn(
                f"{tag}: {len(bad)} cluster(s) carry more than one distinct "
                f"{col!r}; emitting <NA> for them rather than silently picking "
                f"one (GH #323).  Clusters: {sorted(bad.index.tolist())[:20]}",
                RuntimeWarning,
            )
            out.loc[out[v].isin(bad.index), col] = pd.NA

    # Every (v, col) is now single-valued, so `first` cannot discard information.
    reduced = (out.groupby(v, observed=True)[list(columns)]
                  .first()
                  .rename_axis(index='v'))
    assert reduced.index.is_unique, f"{tag}: cluster index not unique after reduce"
    return reduced
