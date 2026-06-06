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

def harmonized_food_labels(fn='../../_/food_items.org',key='Code',value='Preferred Label'):
    # Harmonized food labels
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

