import numpy as np
import pandas as pd
from collections import defaultdict
from cfe.df_utils import use_indices
import warnings
import json

if __name__=='__main__':
    import sys
    sys.path.append('../../_')
    sys.path.append('../../../_')
    from local_tools import format_id, get_dataframe
else:
    from lsms_library.local_tools import format_id, get_dataframe

def District(x):
    """Canonical District form across all Uganda waves.

    Pre-2018 waves source ``District`` from numeric Stata columns
    (``h1aq1``, ``h1aq1a``) which df_data_grabber stringifies to
    ``'101.0'``-style float-strings (CLAUDE.md: ``format_id`` is
    auto-applied to ``idxvars`` but NOT to ``myvars``).  Post-2018
    waves source from string columns (``district_name`` / ``district``)
    which need no normalisation but ``format_id`` is a no-op on them.

    Defining a country-level ``District`` formatter routes every
    Uganda ``District`` myvar through the canonical normalizer so
    cross-wave District values share an int-string encoding.
    GH #161.
    """
    return format_id(x)


def v(x):
    """Canonical cluster-id form for Uganda's ``v`` myvar across all tables.

    Uganda declares ``v`` as a ``myvar`` in ``sample`` (and elsewhere)
    rather than an ``idxvar``, so the auto-applied ``format_id`` does
    not fire and numeric cluster codes like ``10120402`` get
    float-stringified to ``'10120402.0'`` -- which silently fails to
    join against ``cluster_features.v`` and ``household_characteristics.v``
    where ``v`` is in ``idxvars`` and *does* go through ``format_id``.

    Defining a country-level ``v`` formatter routes every Uganda
    ``v`` myvar through ``format_id``, restoring the cross-table
    invariant.  ``format_id`` is a no-op on already-canonical strings
    (e.g., the ``parish_name`` strings in 2018-19, 2019-20) and maps
    empty strings to ``None`` (cleaning up the 565-row ``v == ''``
    leak in 2009-10's sample).

    GH #196.
    """
    return format_id(x)


# Data to link household ids across waves
Waves = {'2005-06':(),
         '2009-10':(), # ID of parent household  in ('GSEC1.dta',"HHID",'HHID_parent'), but not clear how to use
         '2010-11':(),
         '2011-12':(),
         '2013-14':('GSEC1.dta','HHID','HHID_old'),
         '2015-16':('gsec1.dta','HHID','hh',lambda s: s.replace('-05-','-04-')),
         '2018-19':('GSEC1.dta','hhid','t0_hhid'),
         '2019-20':('HH/gsec1.dta','hhid','hhidold')}

def harmonized_unit_labels(key='Code', value='Preferred Label'):
    """Return the {Code: Preferred Label} mapping from the ``u`` table
    in ``lsms_library/countries/Uganda/_/categorical_mapping.org``.

    Replaces the older CSV-based pipeline (``unitlabels.py`` emitting
    ``unitlabels.csv``); the org table is now the single source of truth
    for unit-label canonicalisation.  See GH #223 for the cross-country
    roadmap and Tier 1 convention (Malawi, Mali, Nigeria, Senegal,
    Burkina Faso).

    The org table reuses the ``---`` sentinel for empty cells (per
    ``df_from_orgfile``).  To preserve compatibility with the previous
    CSV-based behaviour -- which carried explicit ``'---'`` strings as
    the canonical label for unit codes that exist but lack a meaningful
    label -- we restore those NaN-valued labels to the literal
    ``'---'`` string.  Codes are coerced to ``int`` so the mapping
    matches the float-typed ``u`` index values produced by the wave
    scripts (``hash(1) == hash(1.0)``; a string key would not match).
    """
    from lsms_library.local_tools import get_categorical_mapping

    raw = get_categorical_mapping(tablename='u',
                                  idxvars=key,
                                  **{value: value})

    unitlabels = {}
    for k, v in raw.items():
        try:
            int_k = int(k)
        except (TypeError, ValueError):
            int_k = k
        if pd.isna(v):
            unitlabels[int_k] = '---'
        else:
            unitlabels[int_k] = str(v).strip()
    return unitlabels

def harmonized_food_labels(fn=None,key='Code',value='Preferred Label'):
    """Return the ``{Code: <value>}`` food-label mapping (default ``value``
    is ``Preferred Label``).

    Unit #0 migration (2026-06-14): the canonical food-label table is now
    the ``harmonize_food`` table inside
    ``lsms_library/countries/Uganda/_/categorical_mapping.org`` (formerly
    the standalone ``food_items.org``).  When ``fn`` is ``None`` (the
    default — used by ``food_acquired`` and ``nutrition.org``) the mapping
    is read from that org table via ``get_categorical_mapping``, mirroring
    ``harmonized_unit_labels`` so foods and units share one source-of-truth
    file and become joinable with crop / community-price ``j`` axes.

    A non-``None`` ``fn`` keeps the legacy ``|``-delimited org-CSV reader
    so the *nonfood* path (``nonfood_items.org``) is unaffected.

    Codes are coerced to ``int`` so the mapping matches the integer item
    codes carried in the ``j`` index of ``food_acquired`` (the raw
    ``.dta`` item codes are ``int16``; ``hash(100) == hash(100.0)`` but a
    string key would not match).
    """
    if fn is None:
        from lsms_library.local_tools import get_categorical_mapping

        raw = get_categorical_mapping(tablename='harmonize_food',
                                      idxvars=key,
                                      **{value: value})

        labels = {}
        for k, v in raw.items():
            try:
                int_k = int(k)
            except (TypeError, ValueError):
                int_k = k
            if pd.isna(v):
                continue
            labels[int_k] = str(v).strip()
        return labels

    # Legacy path: explicit standalone org-CSV file (e.g. nonfood_items.org).
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[[key,value]].dropna()
    food_items = food_items.set_index(key)

    return food_items.squeeze().str.strip().to_dict()


def food_acquired(fn,myvars):

    df = get_dataframe(fn,convert_categoricals=False)

    df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})

    # Replace missing unit values
    df['units'] = df['units'].fillna('---')

    df = df.set_index(['HHID','item','units']).dropna(how='all')

    df.index.names = ['i','j','u']  # HHID='i', item='j', units='u'


    # Fix type of hhids if need be
    if df.index.get_level_values('i').dtype ==float:
        fix = dict(zip(df.index.levels[0],df.index.levels[0].astype(int).astype(str)))
        df = df.rename(index=fix,level=0)

    df = df.rename(index=harmonized_food_labels(),level='j')
    unitlabels = harmonized_unit_labels()
    df = df.rename(index=unitlabels,level='u')

    if not 'market' in df.columns:
        df['market'] = df.filter(regex='^market').median(axis=1)

    # Compute unit values
    df['unitvalue_home'] = df['value_home']/df['quantity_home']
    df['unitvalue_away'] = df['value_away']/df['quantity_away']
    df['unitvalue_own'] = df['value_own']/df['quantity_own']
    df['unitvalue_inkind'] = df['value_inkind']/df['quantity_inkind']

    # Get list of units used in current survey
    units = list(set(df.index.get_level_values('u').tolist()))

    unknown_units = set(units).difference(unitlabels.values())
    if len(unknown_units):
        warnings.warn("Dropping some unknown unit codes!")
        print(unknown_units)
        df = df.loc[df.index.isin(unitlabels.values(),level='u')]

    with open('../../_/conversion_to_kgs.json','r') as f:
        conversion_to_kgs = pd.Series(json.load(f))

    conversion_to_kgs.name='Kgs'
    conversion_to_kgs.index.name='u'

    df = df.join(conversion_to_kgs,on='u')
    df = df.astype(float)

    return df


def food_acquired_to_canonical(df):
    """Reshape Uganda wide-form ``food_acquired`` to canonical long form.

    Phase 3 of GH #169 / DESIGN_food_acquired_canonical_2026-05-05.org.

    Inputs
    ------
    df : DataFrame
        Output of :func:`food_acquired` (one row per ``(i, j, u)`` triple)
        plus a ``t`` index level supplied by the caller (so the canonical
        index ``(t, i, j, u, s)`` is producible without inferring the wave
        label here).  Recognized columns:

        * ``value_home``, ``value_away``, ``quantity_home``, ``quantity_away``
          (consumption-location splits of *purchased* food; folded together
          here because the home/away distinction is consumption-location,
          not acquisition-source -- see design doc)
        * ``value_own``, ``quantity_own``  (own production)
        * ``value_inkind``, ``quantity_inkind``  (in-kind receipts)
        * ``market`` (market price per unit ``u``; preserved for ``s='purchased'``)
        * ``farmgate`` (farmgate price per unit ``u``; preserved for ``s='produced'``)

        Other columns (``unitvalue_*``, ``Kgs``, ``market_home``/``_away``/
        ``_own``) are ignored -- ``unitvalue_*`` are derived (= value/quantity)
        and ``Kgs`` is per-unit metadata not in the canonical schema.

    Output
    ------
    DataFrame indexed by canonical ``(t, i, j, u, s)`` with columns
    ``Quantity``, ``Expenditure``, ``Price``.  ``s`` ∈
    ``{'purchased', 'produced', 'inkind'}``.

    Reshape rules
    -------------
    Each input row becomes up to 3 long-form rows:

    * ``s = 'purchased'`` -- ``Quantity = quantity_home + quantity_away``,
      ``Expenditure = value_home + value_away``, ``Price = market``
    * ``s = 'produced'``  -- ``Quantity = quantity_own``,
      ``Expenditure = value_own``, ``Price = farmgate``
    * ``s = 'inkind'``    -- ``Quantity = quantity_inkind``,
      ``Expenditure = value_inkind``, ``Price = NaN`` (Uganda surveys
      do not record an imputed valuation distinct from value_inkind)

    Rows are kept where EITHER ``Quantity > 0`` OR ``Expenditure > 0``.
    Expenditure-only rows (HH reported a food expenditure with no
    quantity — common in Uganda's GSEC15b for food consumed away from
    home) are legitimate data and are carried through with NaN
    ``Quantity``.  Matches the shared
    :func:`lsms_library.transformations.food_acquired_to_canonical` rule.

    Notes
    -----
    - The ``home``/``away`` consumption-location distinction is dropped
      at the canonical layer per DESIGN_food_acquired_canonical_2026-05-05.
      Users who care about it can read the wave-level pre-canonical
      DataFrame directly.
    - ``v`` is intentionally absent -- the framework joins it from
      ``sample()`` at API time.  See CLAUDE.md "## ``sample()`` and
      Cluster Identity".
    - ``Price`` is carried for purchased / produced rows from the
      survey-reported ``market`` / ``farmgate`` columns.  The framework's
      ``food_prices_from_acquired`` currently re-derives Price from
      ``Expenditure / Quantity_kg`` and ignores a stored Price; the
      stored Price preserves the survey-reported information for
      consumers reading the wave parquet directly, and is forward-
      compatible with a future framework change to prefer stored Price
      where available (per DESIGN doc).
    """
    work = df.reset_index()

    # Build the three per-source pieces.
    def _make(source_label, qty, expenditure, price):
        out = pd.DataFrame({
            't': work['t'].values,
            'i': work['i'].values,
            'j': work['j'].values,
            'u': work['u'].values,
            's': source_label,
            'Quantity': pd.to_numeric(qty, errors='coerce').values,
            'Expenditure': pd.to_numeric(expenditure, errors='coerce').values,
            'Price': pd.to_numeric(price, errors='coerce').values,
        })
        return out

    # Purchased: fold home + away.  Sum with min_count=1 so a row with
    # both NaN stays NaN (and is dropped below); a row with one value
    # populated keeps that value.
    purchased_qty = work[['quantity_home', 'quantity_away']].sum(
        axis=1, min_count=1)
    purchased_val = work[['value_home', 'value_away']].sum(
        axis=1, min_count=1)
    purchased_price = (work['market']
                       if 'market' in work.columns else pd.Series(np.nan,
                                                                   index=work.index))

    purchased = _make('purchased', purchased_qty, purchased_val,
                      purchased_price)
    produced  = _make('produced',  work['quantity_own'], work['value_own'],
                      work['farmgate'] if 'farmgate' in work.columns
                      else pd.Series(np.nan, index=work.index))
    inkind    = _make('inkind',    work['quantity_inkind'],
                      work['value_inkind'],
                      pd.Series(np.nan, index=work.index))

    from lsms_library.transformations import _finalize_canonical_food_acquired

    out = pd.concat([purchased, produced, inkind], ignore_index=True)
    # Filter (qty>0 | exp>0; expenditure-only rows kept with NaN Quantity --
    # GH #246 C-2) and aggregate genuine source-data duplicates (e.g. two
    # ``Other (Specify)`` rows under one canonical key) via the shared tail
    # (GH #251): Quantity/Expenditure summed with min_count=1, per-unit
    # Price averaged.
    out = _finalize_canonical_food_acquired(out)
    return out


def nonfood_expenditures(fn='', purchased=None, away=None, produced=None,
                         given=None, item='item', HHID='HHID'):
    """Uganda non-food expenditures from a single .dta file.

    Aggregates across three or four source columns (purchased, away,
    produced, given) at the (HHID, item) level and returns a wide
    matrix (HHID rows x item columns) of total expenditures.

    Replaces the prior lsms.tools.get_food_expenditures-based
    implementation with an inline pandas groupby+sum; the upstream
    lsms dependency is being retired.
    """
    if __name__ == '__main__':
        from local_tools import get_dataframe
    else:
        from lsms_library.local_tools import get_dataframe

    nonfood_items = harmonized_food_labels(
        fn='../../_/nonfood_items.org', key='Code', value='Preferred Label')

    # Read source file via the repo's standard entry point.
    df = get_dataframe(fn, convert_categoricals=False)

    # Gather source columns (skip None entries).
    source_cols = {
        'purchased': purchased,
        'away':      away,
        'produced':  produced,
        'given':     given,
    }
    source_cols = {k: v for k, v in source_cols.items() if v is not None}

    # Project down to the columns we need and rename.
    keep = [HHID, item] + list(source_cols.values())
    df = df[keep].copy()
    rename_map = {HHID: 'HHID', item: 'itmcd'}
    rename_map.update({v: k for k, v in source_cols.items()})
    df = df.rename(columns=rename_map)

    # Coerce itmcd to numeric, drop missing item codes, cast to int.
    df['itmcd'] = pd.to_numeric(df['itmcd'], errors='coerce')
    df = df.dropna(subset=['itmcd'])
    df['itmcd'] = df['itmcd'].astype(int)

    # Handle HHID-as-float-string (see upstream lsms.tools lines 101-108).
    try:
        first = df['HHID'].iloc[0]
        if isinstance(first, str) and first.split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError, IndexError):
        pass

    # Replace itmcd codes with preferred labels BEFORE groupby so that
    # items sharing a label are merged naturally by the groupby.
    # Keep only rows with a recognized item code.
    df['itmcd'] = df['itmcd'].replace(nonfood_items)
    df = df[df['itmcd'].isin(nonfood_items.values())]

    # Sum source columns, groupby HHID+itmcd (now label names).
    active_sources = list(source_cols.keys())
    df['total'] = df[active_sources].sum(axis=1, min_count=1)
    wide = df.groupby(['HHID', 'itmcd'])['total'].sum().unstack('itmcd')
    wide = wide.fillna(0)

    # Match the old output's index/column names.
    wide.index.name = 'j'
    wide.columns.name = 'i'
    return wide


def id_walk(df, updated_ids, hh_index='i'):
    '''
    Updates household IDs in panel data across different waves separately.

    Parameters:
        df (DataFrame): Panel data with a MultiIndex, including 't' for wave and 'i' (default) for household ID.
        updated_ids (dict): A dictionary mapping each wave to another dictionary that maps original household IDs to updated IDs.
            Format:
                {wave_1: {original_id: new_id, ...},
                 wave_2: {original_id: new_id, ...}, ...}
        hh_index (str): Index name for the household ID level (default is 'i').

    Example:
        updated_ids = {
            '2013-14': {'0001-001': '101012150028', '0009-001': '101015620053', '0005-001': '101012150022'},
            '2016-17': {'0001-002': '0001-001', '0003-001': '0005-001', '0005-001': '0009-001'}
        }

        In this example, IDs are updated independently for each wave.
        Because the same original household ID across different waves may not represent the same household.
        Specifically, household '0005-001' in wave '2016-17' corresponds to household '0009-001' from wave '2013-14', not '0005-001' from '2013-14'.

    The function handles these wave-specific mappings separately, ensuring accurate household identification over time.
    '''
    index_names = list(df.index.names or [])
    if not index_names:
        raise ValueError("Dataframe must have a named MultiIndex for id_walk.")
    if 't' not in index_names:
        raise KeyError("Index must contain a 't' level for wave identifiers.")

    household_level = hh_index
    fallback_used = False
    if household_level not in index_names:
        for candidate in ('i', 'j'):
            if candidate in index_names:
                household_level = candidate
                fallback_used = True
                break
        else:
            # fallback to the first non-'t' level (or level 0)
            non_wave_levels = [name for name in index_names if name != 't']
            if not non_wave_levels:
                raise KeyError("Cannot determine household index level for id_walk.")
            household_level = non_wave_levels[0]
            fallback_used = True

    household_level_pos = index_names.index(household_level)

    if fallback_used and household_level != hh_index:
        warnings.warn(
            f"id_walk expected index level '{hh_index}' but found '{household_level}'. "
            "Proceeding with the detected household index."
        )

    #seperate df into different waves:
    dfs = {}
    waves = df.index.get_level_values('t').unique()
    for wave in waves:
        dfs[wave] = df[df.index.get_level_values('t') == wave].copy()
    #update ids for each wave
    for wave, df_wave in dfs.items():
        #update ids
        if wave in updated_ids:
            df_wave = df_wave.rename(index=updated_ids[wave], level=household_level)
            #update the dataframe with the new ids
            dfs[wave] = df_wave
        else:
            continue
    #combine the updated dataframes
    df = pd.concat(dfs.values(), axis=0)

    if 'i' not in df.index.names or household_level != 'i':
        df.index = df.index.set_names('i', level=household_level_pos)

    # df= df.rename(index=updated_ids,level=['t', household_level])
    df.attrs['id_converted'] = True
    return df


# ---------------------------------------------------------------------
# plot_features (GH #167 Phase 1 pilot)
# ---------------------------------------------------------------------

ACRES_PER_HECTARE = 2.471053814671653  # 1 ha = 2.471... acres
HECTARES_PER_ACRE = 1.0 / ACRES_PER_HECTARE  # 0.404686 ha / acre


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a Code -> Preferred Label dict from categorical_mapping.org.

    Mirrors the shape of ``harmonized_unit_labels`` but returns integer
    keys directly (no '---' sentinel restoration; for our use NaN
    Preferred Labels mean "leave the column NaN").
    """
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


def _harmonize_acquire_table():
    """Load harmonize_acquire from categorical_mapping.org and return
    a {(Wave, File, Code): Preferred Label} dict for three-key lookup.

    The acquire codes mean different things across waves (GH #167), so
    the table is wave-keyed.  Codes absent from the table map to NaN
    (no silent default)."""
    from lsms_library.local_tools import df_from_orgfile

    # Resolve org file relative to this module (search ../../_ first so
    # wave-script CWDs still find it).
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, 'categorical_mapping.org'),
        os.path.abspath(os.path.join('..', '..', '_', 'categorical_mapping.org')),
        'categorical_mapping.org',
    ]
    orgfn = next((c for c in candidates if os.path.exists(c)), candidates[0])

    df = df_from_orgfile(orgfn, name='harmonize_acquire',
                         set_columns=True, to_numeric=True)
    out = {}
    for _, row in df.iterrows():
        wv = str(row['Wave']).strip()
        f = str(row['File']).strip()
        c = row['Code']
        try:
            c = int(c)
        except (TypeError, ValueError):
            pass
        lab = row.get('Preferred Label')
        if pd.isna(lab):
            continue
        out[(wv, f, c)] = str(lab).strip()
    return out


def _map_codes(series, code_map):
    """Map a categorical or numeric Series through ``code_map`` (a
    {int: str} dict).  Returns a string Series with NaN where the
    code is not in the map."""
    if series is None:
        return None
    # Stata categoricals come through as strings if convert_categoricals
    # is True.  We need the underlying integer code for the map lookup.
    if pd.api.types.is_categorical_dtype(series) or series.dtype == object:
        # Reverse map: build {label: code} from the categorical, then
        # invert.  Easier path: re-read the value labels from the raw
        # data.  But for our purposes, the harmonized tables were
        # written against the integer codes; if the wave loaded
        # categoricals, the label string IS the value.  So we map
        # by code only when the dtype is numeric; otherwise treat the
        # string value as already canonical (lowercased + spelling
        # normalized via the harmonize table's *value* column).
        # ---
        # Simpler approach: build a reverse lookup of label_lower -> code,
        # then look up.  For tenure/soil/water tables, the value_labels
        # are the source-survey labels, not our preferred labels — so
        # the simple path doesn't work.  We require an int-keyed series.
        # If we received strings, attempt to recover the int code by
        # forcing convert_categoricals=False in the caller, OR by
        # parsing the label.  For now, raise — callers must pass
        # numeric codes.
        raise TypeError(
            f"_map_codes expects a numeric Series (raw Stata codes); "
            f"got dtype={series.dtype}.  Re-load the source with "
            f"convert_categoricals=False, or pass the integer-coded "
            f"underlying values.")
    # Numeric: convert to nullable Int64 (NaN-safe) and map
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_features_for_wave(t, source_2a, source_2b, colmap):
    """Build canonical ``plot_features`` for one Uganda UNPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2013-14"``), used as the ``t`` index value.
    source_2a, source_2b : pd.DataFrame | None
        Raw AGSEC2A (owned parcels) and AGSEC2B (use-rights parcels)
        DataFrames, loaded via ``get_dataframe(..., convert_categoricals=False)``
        so the categorical columns carry integer codes.  ``None`` is
        permitted when a wave lacks one of the source files.
    colmap : dict
        Per-wave column-name map.  Required keys (for any source the
        caller passes):
            hhid           — household id column
            parcel_id      — within-HH parcel sequence column
        Optional keys (NaN where omitted or absent in source):
            area_gps       — GPS-measured parcel area (acres)
            area_est       — farmer-estimated parcel area (acres)
            tenure_system  — Tenure System question (a2aq7 / s2aq7 / ...)
            acquire        — How-acquired question (a2aq8 / s2aq8 / ...)
            soil_type      — Soil-type question
            water_source   — Main-water-source question
        Each value is the column name in the corresponding source df.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares, float), ``AreaUnit`` (str, always 'acres'),
        ``Tenure`` (str), ``TenureSystem`` (str), ``SoilType`` (str),
        and ``Irrigated`` (bool nullable).  GPS columns (Latitude /
        Longitude) are reserved in the canonical Columns block but not
        emitted here — Uganda's DMS encoding in AGSEC2A is
        non-standard and mostly NaN; revisit in a follow-up.
    """
    tenure_system_map = _harmonized_codes('harmonize_tenure_system')
    soil_map = _harmonized_codes('harmonize_soil')
    water_map = _harmonized_codes('harmonize_water')
    acquire_map = _harmonize_acquire_table()

    pieces = []
    for letter, src in (('A', source_2a), ('B', source_2b)):
        if src is None or src.empty:
            continue

        # Build the per-row canonical frame
        c = colmap  # alias
        hh = src[c['hhid']].apply(format_id)
        parcel = src[c['parcel_id']].apply(format_id)
        plot_id = parcel.astype(str) + f'_{letter}'

        # Area: prefer GPS-measured, fall back to farmer estimate.
        area_acres = pd.Series(pd.NA, index=src.index, dtype='Float64')
        if c.get('area_gps') in src.columns:
            area_acres = pd.to_numeric(src[c['area_gps']], errors='coerce').astype('Float64')
        if c.get('area_est') in src.columns:
            est = pd.to_numeric(src[c['area_est']], errors='coerce').astype('Float64')
            area_acres = area_acres.where(area_acres.notna(), est)
        # Plausibility clamp: > 2500 acres (~1000 ha) is a data-entry
        # error for Ugandan smallholder parcels (observed 8093 ha in
        # 2005-06); drop to NaN so AreaUnit follows, rather than poison
        # area-weighted aggregates downstream (GH #167).
        area_acres = area_acres.where((area_acres <= 2500) | area_acres.isna(), pd.NA)
        area_ha = area_acres * HECTARES_PER_ACRE

        area_unit = pd.Series(['acres'] * len(src), index=src.index, dtype='string')
        # Where area is NaN, leave AreaUnit NaN too (no measurement = no unit).
        area_unit = area_unit.where(area_acres.notna(), pd.NA)

        # TenureSystem (Freehold/Leasehold/Mailo/Customary/...)
        tenure_system = pd.Series(pd.NA, index=src.index, dtype='string')
        ts_col = c.get('tenure_system')
        if ts_col and ts_col in src.columns:
            tenure_system = _map_codes(src[ts_col], tenure_system_map)

        # Tenure: wave-keyed acquire-mode code -> canonical tenure.  The
        # same raw code means different things across waves (e.g. 2B
        # code 1 = 'purchased' in 2005-06 but 'agreement' in 2009-15),
        # so the lookup is keyed on (wave, File, Code).  Unmapped codes
        # ('Do not know', etc.) and absent acquire columns stay NaN --
        # NO silent file-letter default (GH #167; the old default
        # mislabelled ~85% of 2005-06 2B rows and made 2A content-free).
        acq_col = c.get('acquire')
        tenure = pd.Series(pd.NA, index=src.index, dtype='string')
        if acq_col and acq_col in src.columns:
            acq = src[acq_col].astype('Int64')
            tenure = acq.map(lambda code: acquire_map.get((t, f'2{letter}', int(code)))
                             if pd.notna(code) else pd.NA).astype('string')

        # SoilType
        soil_type = pd.Series(pd.NA, index=src.index, dtype='string')
        soil_col = c.get('soil_type')
        if soil_col and soil_col in src.columns:
            soil_type = _map_codes(src[soil_col], soil_map)

        # Irrigated boolean derived from water_source
        irrigated = pd.Series(pd.NA, index=src.index, dtype='boolean')
        water_col = c.get('water_source')
        if water_col and water_col in src.columns:
            water_label = _map_codes(src[water_col], water_map)
            irrigated = (water_label == 'Irrigated').astype('boolean')
            # Where water_label is NaN, leave irrigated as NaN too
            irrigated = irrigated.where(water_label.notna(), pd.NA)

        # GPS deferred for v1.  Uganda's DMS encoding in AGSEC2A is
        # non-standard (Minutes ranges 0-99, Seconds 0-999) and the
        # columns are mostly NaN; revisit when a maintainer with
        # Uganda-specific knowledge can confirm the encoding.
        # Canonical Latitude / Longitude in data_info.yml stay
        # reserved for future countries (e.g. Ethiopia ESS) that
        # have decimal-degree plot GPS.

        piece = pd.DataFrame({
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
        pieces.append(piece)

    if not pieces:
        return pd.DataFrame(
            columns=['Area','AreaUnit','Tenure','TenureSystem',
                     'SoilType','Irrigated'])

    df = pd.concat(pieces, ignore_index=True)
    df = df.set_index(['t', 'i', 'plot_id'])
    return df



# ----------------------------------------------------------------------
# crop_production  (GAP 1 — item-level post-harvest crop module)
# ----------------------------------------------------------------------
#
# Grain: (t, i, plot, j, u, season).  One row per *reported* harvest
# record for a crop on a plot.  Stores REPORTED values only — Quantity
# (native harvest unit u), Quantity_sold, Value_sold, harvest_month and
# the intercropped / perennial flags.  No harvest_kg / yield / main_crop /
# value-share — those are transformations over these item rows.
#
# Source: AGSEC5A (season 1) + AGSEC5B (season 2), the UNPS post-harvest
# crop module.  Column names AND the unit/condition column semantics drift
# across waves (see slurm notes in the wave scripts), so each wave passes
# an explicit colmap.  Some newer waves (2018-19, 2019-20) record two
# harvest "conditions" per (plot, crop) in parallel _1 / _2 column sets;
# we emit one row per non-empty condition rather than summing them.
#
# plot id mirrors the WB harmonised plot_id = hhid-parcel-plot; its parcel
# component (hhid-parcel) is the same parcel that plot_features keys on
# (plot_features uses the coarser parcel grain with an _A/_B source tag).

_CROP_TABLE = 'harmonize_crop'
_HARVEST_UNIT_TABLE = 'harvest_units'


def _crop_label_map():
    return _harmonized_codes(_CROP_TABLE)


def _harvest_unit_map():
    return _harmonized_codes(_HARVEST_UNIT_TABLE)


def _to_int_code(series):
    """Coerce a (possibly categorical/float/str) code column to Int64."""
    if series is None:
        return None
    if pd.api.types.is_categorical_dtype(series):
        # When convert_categoricals=False the categories ARE the codes;
        # otherwise fall back to numeric coercion of the string form.
        try:
            return series.astype('Int64')
        except (TypeError, ValueError):
            return pd.to_numeric(series.astype(str), errors='coerce').astype('Int64')
    return pd.to_numeric(series, errors='coerce').astype('Int64')


def crop_production_for_wave(t, df5a, df5b, df4a, colmap):
    """Build canonical ``crop_production`` for one Uganda UNPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2013-14"``).
    df5a, df5b : pd.DataFrame | None
        Raw AGSEC5A (season 1) / AGSEC5B (season 2) post-harvest crop
        modules, loaded with ``convert_categoricals=False`` so code
        columns carry integer codes.  ``None`` permitted.
    df4a : pd.DataFrame | None
        Raw AGSEC4A plot-crop roster (for the intercropped flag and,
        where available, the perennial flag).  ``None`` permitted; when
        absent the flags are NaN.
    colmap : dict
        Per-(season) column maps keyed by ``'A'`` / ``'B'``.  Each value
        is a dict with keys:
            hhid, parcel, plot, crop       — id columns
            conditions : list of dicts, one per parallel harvest-record
                         set, each with keys:
                qty           — reported harvest quantity column
                unit          — reported harvest unit code column (or None)
                qty_sold      — reported quantity sold column (or None)
                value_sold    — reported sale value column (or None)
                month         — harvest-end month code column (or None)
        plus an optional top-level key ``cf`` listing per-condition CF
        columns (unused for storage; documented for transformations).
    intercrop_map : (passed via colmap['intercrop']) optional dict
            file_hhid, file_parcel, file_plot, flag, [perennial]
        describing how to read the intercropped flag from ``df4a``.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot, j, u, season)`` with columns
        ``Quantity`` (Float64), ``Quantity_sold`` (Float64),
        ``Value_sold`` (Float64), ``harvest_month`` (Int64 1-12) and
        ``intercropped`` (boolean).  The ``perennial`` / ``planting_month``
        lookups are wired but not emitted — no current Uganda wave
        populates them cleanly (they would be all-null).
    """
    crop_map = _crop_label_map()
    unit_map = _harvest_unit_map()

    # --- intercropped / perennial / planting from AGSEC4A (plot-crop) ---
    inter_lookup = {}      # (hh, parcel, plot) -> bool   (plot-level flag)
    perennial_lookup = {}  # (hh, parcel, plot, crop) -> bool
    planting_lookup = {}   # (hh, parcel, plot, crop) -> Int month
    ic = colmap.get('intercrop')
    if df4a is not None and ic is not None:
        hh4 = df4a[ic['hhid']].apply(format_id)
        pa4 = df4a[ic['parcel']].apply(format_id)
        pl4 = df4a[ic['plot']].apply(format_id)
        key3 = list(zip(hh4, pa4, pl4))
        if ic.get('flag') and ic['flag'] in df4a.columns:
            flagcode = _to_int_code(df4a[ic['flag']])
            # 1 = mono/No, 2 = Yes  (recode mirrors WB: 2 -> True)
            for k, c in zip(key3, flagcode):
                if pd.notna(c):
                    inter_lookup[k] = bool(int(c) == 2)
        if ic.get('crop') and ic['crop'] in df4a.columns:
            crop4 = _to_int_code(df4a[ic['crop']])
            key4 = list(zip(hh4, pa4, pl4, crop4))
            if ic.get('perennial') and ic['perennial'] in df4a.columns:
                per = _to_int_code(df4a[ic['perennial']])
                for k, c in zip(key4, per):
                    if pd.notna(c):
                        perennial_lookup[k] = bool(int(c) == 2)
            if ic.get('planting_month') and ic['planting_month'] in df4a.columns:
                pm = _to_int_code(df4a[ic['planting_month']])
                for k, m in zip(key4, pm):
                    if pd.notna(m) and 1 <= int(m) <= 12:
                        planting_lookup[k] = int(m)

    pieces = []
    for season, df5 in (('A', df5a), ('B', df5b)):
        if df5 is None or len(df5) == 0:
            continue
        cm = colmap.get(season)
        if cm is None:
            continue

        hh = df5[cm['hhid']].apply(format_id)
        parcel = df5[cm['parcel']].apply(format_id)
        plot = df5[cm['plot']].apply(format_id) if cm.get('plot') and cm['plot'] in df5.columns else pd.Series(['']*len(df5), index=df5.index)
        plot_id = hh.astype(str) + '-' + parcel.astype(str) + '-' + plot.astype(str)
        crop_code = _to_int_code(df5[cm['crop']])
        j = crop_code.map(lambda c: crop_map.get(int(c), pd.NA) if pd.notna(c) else pd.NA)

        for cond in cm['conditions']:
            qcol = cond.get('qty')
            if not qcol or qcol not in df5.columns:
                continue
            qty = pd.to_numeric(df5[qcol], errors='coerce')

            # reported native unit
            if cond.get('unit') and cond['unit'] in df5.columns:
                ucode = _to_int_code(df5[cond['unit']])
                u = ucode.map(lambda c: unit_map.get(int(c), pd.NA) if pd.notna(c) else pd.NA)
            else:
                u = pd.Series([pd.NA]*len(df5), index=df5.index, dtype='object')

            qsold = pd.to_numeric(df5[cond['qty_sold']], errors='coerce') if cond.get('qty_sold') in df5.columns else pd.Series([pd.NA]*len(df5), index=df5.index)
            vsold = pd.to_numeric(df5[cond['value_sold']], errors='coerce') if cond.get('value_sold') in df5.columns else pd.Series([pd.NA]*len(df5), index=df5.index)

            if cond.get('month') and cond['month'] in df5.columns:
                hm = _to_int_code(df5[cond['month']])
                hm = hm.where((hm >= 1) & (hm <= 12), pd.NA)
            else:
                hm = pd.Series([pd.NA]*len(df5), index=df5.index, dtype='Int64')

            piece = pd.DataFrame({
                't':             t,
                'i':             hh.values,
                'plot':          plot_id.values,
                'j':             j.values,
                'u':             u.values,
                'season':        season,
                'Quantity':      qty.values,
                'Quantity_sold': qsold.values,
                'Value_sold':    vsold.values,
                'harvest_month': hm.values,
            })
            # intercropped flag (plot-level) joined from AGSEC4A.  The
            # perennial_lookup / planting_lookup hooks exist for future
            # waves but no current Uganda wave populates them cleanly, so
            # those columns are not emitted (they would be all-null).
            k3 = list(zip(hh.values, parcel.values, plot.values))
            piece['intercropped'] = [inter_lookup.get(k, pd.NA) for k in k3]
            pieces.append(piece)

    cols = ['Quantity', 'Quantity_sold', 'Value_sold', 'harvest_month',
            'intercropped']
    if not pieces:
        return pd.DataFrame(columns=cols)

    df = pd.concat(pieces, ignore_index=True)

    # Drop rows with no crop label and no quantity at all (empty source
    # rows / land-status placeholders with nothing reported).
    df = df[df['j'].notna()]
    # Keep rows even when Quantity is NaN but a sale was reported; drop
    # only when ALL reported measures are missing.
    measure_cols = ['Quantity', 'Quantity_sold', 'Value_sold']
    df = df[df[measure_cols].notna().any(axis=1)]

    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').astype('Float64')
    df['Quantity_sold'] = pd.to_numeric(df['Quantity_sold'], errors='coerce').astype('Float64')
    df['Value_sold'] = pd.to_numeric(df['Value_sold'], errors='coerce').astype('Float64')
    df['harvest_month'] = pd.to_numeric(df['harvest_month'], errors='coerce').astype('Int64')
    df['intercropped'] = df['intercropped'].astype('boolean')

    # u may be NaN (e.g. 2018-19 harvest side has no unit label); fill
    # with a sentinel so it can be an index level without null-index
    # failures, but ONLY where Quantity is present (a reported quantity
    # with no unit).  Where there's no quantity at all, leave the unit
    # sentinel too.
    df['u'] = df['u'].astype('object').where(df['u'].notna(), 'Unknown')

    df = df.set_index(['t', 'i', 'plot', 'j', 'u', 'season'])
    # Collapse exact-duplicate index tuples (same plot/crop/unit/season
    # reported twice) by summing the reported quantities — this is NOT an
    # aggregation across distinct items, just de-duplication of repeated
    # identical source rows so the index is unique.
    if not df.index.is_unique:
        num = df[['Quantity', 'Quantity_sold', 'Value_sold']].groupby(level=df.index.names).sum(min_count=1)
        firstcols = df[['harvest_month', 'intercropped']].groupby(level=df.index.names).first()
        df = num.join(firstcols)
    return df


# Per-wave column maps for crop_production_for_wave.  The harvest UNIT is
# the column whose value labels decode to Kg/Sack/Bunch (the harvest_units
# scheme) — empirically a5aq6c for 2009-16 (NOT a5aq6b, which is the
# Fresh/Dry condition; the WB .do's A5aq6b/A5aq6c unit/condition rename is
# inverted for these actual UNPS files).  2018-19's harvest side carries
# no unit label (-> u='Unknown'); 2019-20 keeps WB names s5aq06b_1.
CROP_COLMAPS = {
    '2009-10': {
        'A': {'hhid': 'HHID', 'parcel': 'a5aq1', 'plot': 'a5aq3', 'crop': 'a5aq5',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': None}]},
        'B': {'hhid': 'HHID', 'parcel': 'a5bq1', 'plot': 'a5bq3', 'crop': 'a5bq5',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': None}]},
        # 2009-10 AGSEC4A uses a non-standard column layout (a4aq1/a4aq2/
        # a4aq4, no parcel/plot/cropID in the form the join needs), so the
        # intercrop flag is not cleanly joinable -> intercropped is NaN
        # this wave.
        'intercrop': None,
    },
    '2010-11': {
        'A': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': None}]},
        'B': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': None}]},
        'intercrop': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
                      'flag': 'a4aq3', 'crop': 'cropID'},
    },
    '2011-12': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': 'a5aq6f'}]},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': 'a5bq6f'}]},
        'intercrop': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                      'flag': 'a4aq3', 'crop': 'cropID'},
    },
    '2013-14': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': 'a5aq6f'}]},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': 'a5bq6f'}]},
        'intercrop': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                      'flag': 'a4aq16', 'crop': 'cropID'},
    },
    '2015-16': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': 'a5aq6f'}]},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': 'a5bq6f'}]},
        'intercrop': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                      'flag': 'a4aq16', 'crop': 'cropID'},
    },
    '2018-19': {
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 's5aq06a_1', 'unit': None,
                              'qty_sold': 's5aq07a_1', 'value_sold': 's5aq08_1',
                              'month': 's5aq06f_1'}]},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 's5bq06a_1', 'unit': None,
                              'qty_sold': 's5bq07a_1', 'value_sold': 's5bq08_1',
                              'month': 's5bq06f_1'}]},
        'intercrop': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                      'flag': 's4aq16', 'crop': 'cropID'},
    },
    '2019-20': {
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [
                  {'qty': 's5aq06a_1', 'unit': 's5aq06b_1',
                   'qty_sold': 's5aq07a_1', 'value_sold': 's5aq08_1',
                   'month': 's5aq06f_1'},
                  {'qty': 's5aq06a_2', 'unit': 's5aq06b_2',
                   'qty_sold': 's5aq07a_2', 'value_sold': 's5aq08_2',
                   'month': 's5aq06f_2'},
              ]},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 's5bq06a_1', 'unit': 's5bq06b_1',
                              'qty_sold': 's5bq07a_1', 'value_sold': 's5bq08_1',
                              'month': 's5bq06f_1'}]},
        'intercrop': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                      'flag': 's4aq16', 'crop': 'cropID'},
    },
}


# ---------------------------------------------------------------------------
# plot_inputs  (GAP 2 — item-level agricultural inputs)
# ---------------------------------------------------------------------------
#
# One row per REPORTED input applied to a plot, grain (t, i, plot, input).
# Source: AGSEC3A (season-1 plot-input module) + AGSEC3B (season-2) for the
# fertilizer / pesticide blocks, and AGSEC4A (plot-crop roster) for the seed
# block.  ``plot`` mirrors the crop_production / WB harmonised plot_id =
# hhid-parcel-plot, so a plot_inputs row joins crop_production on
# (t, i, plot) and plot_features on the parcel component.
#
# The UNPS plot-input module records four input *blocks* per plot, each a
# fixed column group:
#   organic fertilizer   used / qty / purchased? / purchased-qty / purch-value
#   inorganic fertilizer used / type / qty / purchased? / purch-qty / value
#   pesticide/herbicide  used / type / unit / qty / purchased? / purch-qty / value
# and the seed block (AGSEC4A, plot-crop grain):
#   seed                 qty / unit / seed-type(Trad/Improved) / improved-type
#                        / purchase-value [/ purchased? in 2009-10/2010-11]
#
# ``input`` carries the FINEST identity the source records, via the
# harmonize_input table (Code | Preferred Label).  Fertilizer/pesticide
# sub-types live in distinct Code ranges so one table disambiguates:
#   10           Seed
#   20           Organic Fertilizer
#   30 / 31..34  Inorganic Fertilizer  (Nitrate/Phosphate/Potash/Mixed)
#   40 / 41..49  Pesticide             (Insecticide/Fungicide/...)
# Reported attribute columns: Quantity + native unit ``u``, Purchased (bool),
# Quantity_purchased, Improved (bool, seed rows only), crop (j, seed rows
# where the source records the seed's crop, on harmonize_crop labels).
# NO seed_kg / nitrogen_kg / any-use flags — those are transformations.

_INPUT_TABLE = 'harmonize_input'
# pesticide unit scheme is a tiny 1=Kg / 2=Litres code (a3aq24a / a3aq28a),
# distinct from the UNPS harvest/seed unit scheme reused for seed via
# harvest_units.  Stored in its own harmonize_pesticide_unit table.
_PEST_UNIT_TABLE = 'harmonize_pesticide_unit'

# input-block -> harmonize_input base Code.  Inorganic/pesticide refine by
# adding the source type code (1..4 / 1..6,96 -> 96 folds to 9) so e.g.
# Nitrate inorganic = 31, Fungicide pesticide = 43.
_INPUT_BASE = {'seed': 10, 'organic': 20, 'inorganic': 30, 'pesticide': 40}


def _input_label_map():
    return _harmonized_codes(_INPUT_TABLE)


def _pest_unit_map():
    return _harmonized_codes(_PEST_UNIT_TABLE)


def _input_code(block, type_code):
    """Resolve the harmonize_input Code for a block + native type code.

    ``type_code`` may be NaN (block used but type unreported -> base code).
    Pesticide ``96`` ("Other") folds to base+9 so the table stays compact.
    """
    base = _INPUT_BASE[block]
    if block in ('inorganic', 'pesticide') and pd.notna(type_code):
        tc = int(type_code)
        if tc == 96:
            tc = 9
        if 1 <= tc <= 9:
            return base + tc
    return base


def _recode_yes(series):
    """Map a 1/2 (Yes/No) coded column to a nullable boolean (1->True,
    2->False; everything else -> NA).  UNPS uses 1=Yes, 2=No."""
    code = _to_int_code(series)
    out = pd.Series(pd.NA, index=series.index, dtype='boolean')
    out[code == 1] = True
    out[code == 2] = False
    return out


def plot_inputs_for_wave(t, df3a, df3b, df4a, colmap):
    """Build canonical ``plot_inputs`` for one Uganda UNPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2011-12"``).
    df3a, df3b : pd.DataFrame | None
        Raw AGSEC3A (season 1) / AGSEC3B (season 2) plot-input modules,
        loaded with ``convert_categoricals=False`` so code columns carry
        integer codes.  ``None`` permitted (season absent).
    df4a : pd.DataFrame | None
        Raw AGSEC4A plot-crop roster, for the seed block.  ``None``
        permitted; when absent no seed rows are emitted.
    colmap : dict
        Per-wave column map; see ``INPUT_COLMAPS``.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot, input)`` with columns
    ``Quantity`` (Float64), ``u`` (object, native unit label), ``Purchased``
    (boolean), ``Quantity_purchased`` (Float64), ``Improved`` (boolean,
    seed rows), and ``j`` (object crop label, seed rows where recorded).
    Reported values only; missing-in-wave columns are NaN.
    """
    input_map = _input_label_map()
    unit_map = _harvest_unit_map()        # seed unit reuses harvest scheme
    pest_unit_map = _pest_unit_map()
    crop_map = _crop_label_map()

    pieces = []

    # ---- fertilizer / pesticide blocks from AGSEC3A / AGSEC3B ----
    for season, df3 in (('A', df3a), ('B', df3b)):
        if df3 is None or len(df3) == 0:
            continue
        cm = (colmap.get(season) or {}).get('inputs')
        if not cm:
            continue
        hh = df3[cm['hhid']].apply(format_id)
        parcel = df3[cm['parcel']].apply(format_id)
        plot = (df3[cm['plot']].apply(format_id)
                if cm.get('plot') and cm['plot'] in df3.columns
                else pd.Series([''] * len(df3), index=df3.index))
        plot_id = hh.astype(str) + '-' + parcel.astype(str) + '-' + plot.astype(str)

        for block in ('organic', 'inorganic', 'pesticide'):
            b = cm.get(block)
            if not b:
                continue
            # A block "applies" to a plot when its used-flag is Yes, OR
            # (no used-flag column, e.g. inorganic in 2011+) when a type
            # or quantity is reported.
            type_code = (_to_int_code(df3[b['type']])
                         if b.get('type') and b['type'] in df3.columns
                         else pd.Series([pd.NA] * len(df3), index=df3.index, dtype='Int64'))
            qty = (pd.to_numeric(df3[b['qty']], errors='coerce')
                   if b.get('qty') and b['qty'] in df3.columns
                   else pd.Series([np.nan] * len(df3), index=df3.index))

            if b.get('used') and b['used'] in df3.columns:
                used = _recode_yes(df3[b['used']])
                applied = (used == True)
            else:
                applied = type_code.notna() | qty.notna()

            if not applied.any():
                continue

            # native unit: pesticide carries a 1=Kg/2=Litre unit column;
            # organic/inorganic are implicitly kg (no unit column).
            if b.get('unit') and b['unit'] in df3.columns:
                ucode = _to_int_code(df3[b['unit']])
                u = ucode.map(lambda c: pest_unit_map.get(int(c), pd.NA)
                              if pd.notna(c) else pd.NA)
            elif block in ('organic', 'inorganic'):
                u = pd.Series(['Kg'] * len(df3), index=df3.index, dtype='object')
            else:
                u = pd.Series([pd.NA] * len(df3), index=df3.index, dtype='object')

            purchased = (_recode_yes(df3[b['purchased']])
                         if b.get('purchased') and b['purchased'] in df3.columns
                         else pd.Series([pd.NA] * len(df3), index=df3.index, dtype='boolean'))
            qpur = (pd.to_numeric(df3[b['purchased_qty']], errors='coerce')
                    if b.get('purchased_qty') and b['purchased_qty'] in df3.columns
                    else pd.Series([np.nan] * len(df3), index=df3.index))

            input_code = type_code.map(lambda c: _input_code(block, c))
            input_label = input_code.map(lambda c: input_map.get(int(c), pd.NA)
                                         if pd.notna(c) else pd.NA)

            piece = pd.DataFrame({
                't': t,
                'i': hh.values,
                'plot': plot_id.values,
                'input': input_label.values,
                'Quantity': qty.values,
                'u': u.values,
                'Purchased': purchased.values,
                'Quantity_purchased': qpur.values,
                'Improved': pd.Series([pd.NA] * len(df3), dtype='boolean').values,
                'j': pd.Series([pd.NA] * len(df3), dtype='object').values,
            })
            piece = piece[applied.values]
            pieces.append(piece)

    # ---- seed block from AGSEC4A (plot-crop grain) ----
    sc = colmap.get('seed')
    if df4a is not None and len(df4a) and sc:
        hh = df4a[sc['hhid']].apply(format_id)
        parcel = df4a[sc['parcel']].apply(format_id)
        plot = (df4a[sc['plot']].apply(format_id)
                if sc.get('plot') and sc['plot'] in df4a.columns
                else pd.Series([''] * len(df4a), index=df4a.index))
        plot_id = hh.astype(str) + '-' + parcel.astype(str) + '-' + plot.astype(str)

        crop_code = (_to_int_code(df4a[sc['crop']])
                     if sc.get('crop') and sc['crop'] in df4a.columns
                     else pd.Series([pd.NA] * len(df4a), index=df4a.index, dtype='Int64'))
        j = crop_code.map(lambda c: crop_map.get(int(c), pd.NA)
                          if pd.notna(c) else pd.NA)

        qty = (pd.to_numeric(df4a[sc['qty']], errors='coerce')
               if sc.get('qty') and sc['qty'] in df4a.columns
               else pd.Series([np.nan] * len(df4a), index=df4a.index))
        if sc.get('unit') and sc['unit'] in df4a.columns:
            ucode = _to_int_code(df4a[sc['unit']])
            u = ucode.map(lambda c: unit_map.get(int(c), pd.NA)
                          if pd.notna(c) else pd.NA)
        else:
            u = pd.Series([pd.NA] * len(df4a), index=df4a.index, dtype='object')

        # Improved: seed-type 1=Traditional, 2=Improved.
        stype = (_to_int_code(df4a[sc['seed_type']])
                 if sc.get('seed_type') and sc['seed_type'] in df4a.columns
                 else pd.Series([pd.NA] * len(df4a), index=df4a.index, dtype='Int64'))
        improved = pd.Series(pd.NA, index=df4a.index, dtype='boolean')
        improved[stype == 2] = True
        improved[stype == 1] = False

        purchased = (_recode_yes(df4a[sc['purchased']])
                     if sc.get('purchased') and sc['purchased'] in df4a.columns
                     else pd.Series([pd.NA] * len(df4a), index=df4a.index, dtype='boolean'))
        qpur = (pd.to_numeric(df4a[sc['purchased_qty']], errors='coerce')
                if sc.get('purchased_qty') and sc['purchased_qty'] in df4a.columns
                else pd.Series([np.nan] * len(df4a), index=df4a.index))

        seed_label = input_map.get(_INPUT_BASE['seed'], 'Seed')
        piece = pd.DataFrame({
            't': t,
            'i': hh.values,
            'plot': plot_id.values,
            'input': seed_label,
            'Quantity': qty.values,
            'u': u.values,
            'Purchased': purchased.values,
            'Quantity_purchased': qpur.values,
            'Improved': improved.values,
            'j': j.values,
        })
        # Keep a seed row when any seed measure is reported (qty, improved,
        # purchased, or a crop label) — a plot-crop with a recorded seed.
        keep = (piece['Quantity'].notna() | piece['Improved'].notna()
                | piece['Purchased'].notna() | piece['j'].notna())
        pieces.append(piece[keep.values])

    cols = ['Quantity', 'u', 'Purchased', 'Quantity_purchased', 'Improved']
    if not pieces:
        return pd.DataFrame(columns=cols)

    df = pd.concat(pieces, ignore_index=True)
    df = df[df['input'].notna()]

    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').astype('Float64')
    df['Quantity_purchased'] = pd.to_numeric(df['Quantity_purchased'], errors='coerce').astype('Float64')
    df['Purchased'] = df['Purchased'].astype('boolean')
    df['Improved'] = df['Improved'].astype('boolean')
    # u may be NaN (a reported pesticide with no unit label); sentinel so it
    # is not an issue for downstream code that strings the column.
    df['u'] = df['u'].astype('object').where(df['u'].notna(), 'Unknown')
    # j is the seed's crop (on harmonize_crop labels) for seed rows, and a
    # 'n/a' sentinel for fertilizer/pesticide rows (no crop linkage).  It is
    # an INDEX level so per-crop seed rows on a multi-crop plot stay distinct
    # (the maintainer's "plot-crop seed grain") rather than collapsing —
    # 37.9% of seeded plots in 2011-12 carry >1 crop.  The sentinel keeps the
    # level non-null so it is a valid index level.
    df['j'] = df['j'].astype('object').where(df['j'].notna(), 'n/a')

    df = df.set_index(['t', 'i', 'plot', 'input', 'j'])
    # Collapse only EXACT-duplicate (t,i,plot,input,j) tuples — the same input
    # identity reported twice for the same plot-crop (e.g. a fertilizer block
    # appearing in both AGSEC3A passes, or a seed row repeated).  This is
    # de-duplication of the index grain, NOT cross-item aggregation: Quantity
    # / purchased quantity sum, flags/unit take first.
    if not df.index.is_unique:
        num = df[['Quantity', 'Quantity_purchased']].groupby(level=df.index.names).sum(min_count=1)
        flags = df[['Purchased', 'Improved']].groupby(level=df.index.names).max()
        us = df[['u']].groupby(level=df.index.names).first()
        df = num.join(flags).join(us)
        df = df[cols]
    return df


# Per-wave column maps for plot_inputs_for_wave.
#
# Two questionnaire vintages:
#   2009-10 / 2010-11 (older numbering):
#     organic   used a3aq4  qty a3aq5  purch a3aq6  pqty a3aq7
#     inorganic used a3aq14 type a3aq15 qty a3aq16 purch a3aq17 pqty a3aq18
#     pesticide used a3aq26 type a3aq27 unit a3aq28a qty a3aq28b purch a3aq29 pqty a3aq30
#     seed (AGSEC4A): NO qty/unit; purch a4aq10, seed_type a4aq13  (qty absent)
#   2011-12 / 2013-14 / 2015-16 / 2018-19 / 2019-20 (newer numbering, s-prefix
#   for 2018+):
#     organic   used .4  qty .5  purch .6  pqty .7
#     inorganic used .13 type .14 qty .15 purch .16 pqty .17
#     pesticide used .22 type .23 unit .24a qty .24b purch .25 pqty .26
#     seed (AGSEC4A): qty .11a unit .11b seed_type .13  (purch via value .15;
#                     no explicit purchased y/n -> Purchased NA)
INPUT_COLMAPS = {
    '2009-10': {
        'A': {'hhid': 'HHID', 'parcel': 'a3aq1', 'plot': 'a3aq3',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'a3aq1', 'plot': 'a3aq3',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq14', 'type': 'a3aq15', 'qty': 'a3aq16', 'purchased': 'a3aq17', 'purchased_qty': 'a3aq18'},
                  'pesticide': {'used': 'a3aq26', 'type': 'a3aq27', 'unit': 'a3aq28a', 'qty': 'a3aq28b', 'purchased': 'a3aq29', 'purchased_qty': 'a3aq30'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'a3bq1', 'plot': 'a3bq3',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'a3bq1', 'plot': 'a3bq3',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq14', 'type': 'a3bq15', 'qty': 'a3bq16', 'purchased': 'a3bq17', 'purchased_qty': 'a3bq18'},
                  'pesticide': {'used': 'a3bq26', 'type': 'a3bq27', 'unit': 'a3bq28a', 'qty': 'a3bq28b', 'purchased': 'a3bq29', 'purchased_qty': 'a3bq30'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'a4aq2', 'plot': 'a4aq4', 'crop': 'a4aq6',
                 'purchased': 'a4aq10', 'seed_type': 'a4aq13'},
    },
    '2010-11': {
        'A': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq14', 'type': 'a3aq15', 'qty': 'a3aq16', 'purchased': 'a3aq17', 'purchased_qty': 'a3aq18'},
                  'pesticide': {'used': 'a3aq26', 'type': 'a3aq27', 'unit': 'a3aq28a', 'qty': 'a3aq28b', 'purchased': 'a3aq29', 'purchased_qty': 'a3aq30'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq14', 'type': 'a3bq15', 'qty': 'a3bq16', 'purchased': 'a3bq17', 'purchased_qty': 'a3bq18'},
                  'pesticide': {'used': 'a3bq26', 'type': 'a3bq27', 'unit': 'a3bq28a', 'qty': 'a3bq28b', 'purchased': 'a3bq29', 'purchased_qty': 'a3bq30'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid', 'crop': 'cropID',
                 'purchased': 'a4aq10', 'seed_type': 'a4aq13'},
    },
    '2011-12': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq13', 'type': 'a3aq14', 'qty': 'a3aq15', 'purchased': 'a3aq16', 'purchased_qty': 'a3aq17'},
                  'pesticide': {'used': 'a3aq22', 'type': 'a3aq23', 'unit': 'a3aq24a', 'qty': 'a3aq24b', 'purchased': 'a3aq25', 'purchased_qty': 'a3aq26'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq13', 'type': 'a3bq14', 'qty': 'a3bq15', 'purchased': 'a3bq16', 'purchased_qty': 'a3bq17'},
                  'pesticide': {'used': 'a3bq22', 'type': 'a3bq23', 'unit': 'a3bq24a', 'qty': 'a3bq24b', 'purchased': 'a3bq25', 'purchased_qty': 'a3bq26'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
                 'qty': 'a4aq11a', 'unit': 'a4aq11b', 'seed_type': 'a4aq13'},
    },
    '2013-14': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq13', 'type': 'a3aq14', 'qty': 'a3aq15', 'purchased': 'a3aq16', 'purchased_qty': 'a3aq17'},
                  'pesticide': {'used': 'a3aq22', 'type': 'a3aq23', 'unit': 'a3aq24a', 'qty': 'a3aq24b', 'purchased': 'a3aq25', 'purchased_qty': 'a3aq26'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq13', 'type': 'a3bq14', 'qty': 'a3bq15', 'purchased': 'a3bq16', 'purchased_qty': 'a3bq17'},
                  'pesticide': {'used': 'a3bq22', 'type': 'a3bq23', 'unit': 'a3bq24a', 'qty': 'a3bq24b', 'purchased': 'a3bq25', 'purchased_qty': 'a3bq26'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
                 'qty': 'a4aq11a', 'unit': 'a4aq11b', 'seed_type': 'a4aq13'},
    },
    '2015-16': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq13', 'type': 'a3aq14', 'qty': 'a3aq15', 'purchased': 'a3aq16', 'purchased_qty': 'a3aq17'},
                  'pesticide': {'used': 'a3aq22', 'type': 'a3aq23', 'unit': 'a3aq24a', 'qty': 'a3aq24b', 'purchased': 'a3aq25', 'purchased_qty': 'a3aq26'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq13', 'type': 'a3bq14', 'qty': 'a3bq15', 'purchased': 'a3bq16', 'purchased_qty': 'a3bq17'},
                  'pesticide': {'used': 'a3bq22', 'type': 'a3bq23', 'unit': 'a3bq24a', 'qty': 'a3bq24b', 'purchased': 'a3bq25', 'purchased_qty': 'a3bq26'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
                 'qty': 'a4aq11a', 'unit': 'a4aq11b', 'seed_type': 'a4aq13'},
    },
    '2018-19': {
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                  'organic':   {'used': 's3aq04', 'qty': 's3aq05', 'purchased': 's3aq06', 'purchased_qty': 's3aq07'},
                  'inorganic': {'used': 's3aq13', 'type': 's3aq14', 'qty': 's3aq15', 'purchased': 's3aq16', 'purchased_qty': 's3aq17'},
                  'pesticide': {'used': 's3aq22', 'type': 's3aq23', 'unit': 's3aq24a', 'qty': 's3aq24b', 'purchased': 's3aq25', 'purchased_qty': 's3aq26'},
              }},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                  'organic':   {'used': 's3bq04', 'qty': 's3bq05', 'purchased': 's3bq06', 'purchased_qty': 's3bq07'},
                  'inorganic': {'used': 's3bq13', 'type': 's3bq14', 'qty': 's3bq15', 'purchased': 's3bq16', 'purchased_qty': 's3bq17'},
                  'pesticide': {'used': 's3bq22', 'type': 's3bq23', 'unit': 's3bq24a', 'qty': 's3bq24b', 'purchased': 's3bq25', 'purchased_qty': 's3bq26'},
              }},
        'seed': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
                 'qty': 's4aq11a', 'unit': 's4aq11b', 'seed_type': 's4aq13'},
    },
    '2019-20': {
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                  'organic':   {'used': 's3aq04', 'qty': 's3aq05', 'purchased': 's3aq06', 'purchased_qty': 's3aq07'},
                  'inorganic': {'used': 's3aq13', 'type': 's3aq14', 'qty': 's3aq15', 'purchased': 's3aq16', 'purchased_qty': 's3aq17'},
                  'pesticide': {'used': 's3aq22', 'type': 's3aq23', 'unit': 's3aq24a', 'qty': 's3aq24b', 'purchased': 's3aq25', 'purchased_qty': 's3aq26'},
              }},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                  'organic':   {'used': 's3bq04', 'qty': 's3bq05', 'purchased': 's3bq06', 'purchased_qty': 's3bq07'},
                  'inorganic': {'used': 's3bq13', 'type': 's3bq14', 'qty': 's3bq15', 'purchased': 's3bq16', 'purchased_qty': 's3bq17'},
                  'pesticide': {'used': 's3bq22', 'type': 's3bq23', 'unit': 's3bq24a', 'qty': 's3bq24b', 'purchased': 's3bq25', 'purchased_qty': 's3bq26'},
              }},
        'seed': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
                 'qty': 's4aq11a', 'unit': 's4aq11b', 'seed_type': 's4aq13'},
    },
}
