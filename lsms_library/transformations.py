#!/usr/bin/env python3

"""
A collection of mappings to transform dataframes.
"""
import pandas as pd
import numpy as np
from pandas import concat, get_dummies, MultiIndex
from cfe.df_utils import use_indices
from .local_tools import format_id


# Canonical values for the food_acquired ``s`` (acquisition source) index
# level.  See ``slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org``
# and GH #169.  ``data_info.yml`` does not currently support enumerated
# value constraints on index levels, so the enumeration lives here and is
# enforced in code by :func:`validate_acquisition_source`.
S_VALUES = ('purchased', 'produced', 'inkind', 'other')


def validate_acquisition_source(df: pd.DataFrame) -> None:
    """Raise ``ValueError`` if ``s`` index level contains non-canonical values.

    No-op when ``s`` is not in the index.  Called from
    :meth:`Country._finalize_result` for tables that carry an ``s`` level
    (currently just ``food_acquired`` and its derivatives).
    """
    if 's' not in (df.index.names or []):
        return
    observed = set(df.index.get_level_values('s').dropna().unique())
    invalid = observed - set(S_VALUES)
    if invalid:
        raise ValueError(
            f"Non-canonical values in 's' index level: {sorted(invalid)}. "
            f"Allowed values are {S_VALUES}.  See "
            f"slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org."
        )


def food_acquired_to_canonical(df: pd.DataFrame, drop_columns=('visit',)) -> pd.DataFrame:
    """Reshape wide-form ``food_acquired`` into canonical long-form on ``s``.

    Designed for use as a wave-level ``mapping.py`` post-processor.  The
    typical pattern in a country's wave file is::

        from lsms_library.transformations import food_acquired_to_canonical as food_acquired

    Inputs (post-data_grabber):
      - DataFrame with a multi-level index that includes ``(t, i, j, u)``
        and optionally ``v`` (cluster) and ``visit`` (EHCVM passages).
      - Columns: ``Quantity`` (TOTAL acquired in unit ``u``), ``Expenditure``
        (monetary value of purchases; may be NaN), ``Produced`` (subset of
        Quantity from own production; may be NaN/0).

    Output:
      - Index: ``(t, v, i, j, u, s)`` if ``v`` was present in the input,
        else ``(t, i, j, u, s)``; ``s`` ∈ ``{'purchased', 'produced'}``.
        Per the post-2026-04-10 v-from-sample design, script-path waves
        omit ``v`` and let ``_join_v_from_sample`` add it at API time;
        YAML-path waves that already carry ``v`` keep it through.
      - Columns: ``Quantity``, ``Expenditure``.  ``Price`` is api-derived.

    Reshape rules:
      - Each input row becomes up to two long-form rows:

        * ``s='purchased'``: ``Quantity = (Total - Produced)`` clipped at 0;
          ``Expenditure`` as observed.
        * ``s='produced'``: ``Quantity = Produced``; ``Expenditure = NaN``
          (note: the wave-level groupby in ``Wave.food_acquired`` may
          coerce all-NaN sums to 0.0 — semantically equivalent for produced
          rows).

      - Rows with no measurements after the split are dropped.
      - Index columns named in ``drop_columns`` are removed before the
        long-form set_index, allowing per-country quirks (EHCVM ``visit``
        is dropped by default — it's a sample split, not a repeated
        measure; see ``slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org``).

    Empirical sanity: Benin 2018-19 (commit 27e3d963) confirmed Produced ≤
    Quantity in 100% of rows where both populated (2,597 rows), supporting
    the (Total - Produced) = purchased decomposition.

    Parameters
    ----------
    df : pd.DataFrame
        Wide-form food_acquired DataFrame from the wave-level data_grabber.
    drop_columns : iterable of str, optional
        Index/column names to drop before the canonical reshape.  Default
        ``('visit',)`` covers the EHCVM ``vague`` case.  Pass an empty
        tuple to keep all index levels (e.g., for pre-EHCVM single-passage
        waves where ``visit`` is absent — the function silently skips
        missing columns).

    Returns
    -------
    pd.DataFrame
        Canonical long-form food_acquired with index ``(t, v, i, j, u, s)``.
    """
    work = df.reset_index()

    for col in drop_columns:
        if col in work.columns:
            work = work.drop(columns=[col])

    # Purchased = Total - Produced, clipped at zero (a few survey rows
    # have Produced slightly > Quantity due to rounding; treat those as
    # purchased=0 rather than negative).
    purchased_qty = (work['Quantity'].fillna(0)
                     - work['Produced'].fillna(0)).clip(lower=0)

    has_v = 'v' in work.columns
    base_levels = ['t', 'v', 'i', 'j', 'u'] if has_v else ['t', 'i', 'j', 'u']

    def _row_dict(source, qty, expenditure):
        d = {lvl: work[lvl].values for lvl in base_levels}
        d['s'] = source
        d['Quantity'] = qty
        d['Expenditure'] = expenditure
        return d

    purchased = pd.DataFrame(_row_dict('purchased',
                                       purchased_qty.values,
                                       work['Expenditure'].values))
    purchased = purchased[(purchased['Quantity'] > 0)
                          | (purchased['Expenditure'] > 0)]

    produced = pd.DataFrame(_row_dict('produced',
                                      work['Produced'].values,
                                      np.nan))
    produced = produced[produced['Quantity'].fillna(0) > 0]

    out = pd.concat([purchased, produced], ignore_index=True)
    out = out.set_index(base_levels + ['s'])
    return out


def age_intervals(age, age_cuts=(4, 9, 14, 19, 31, 51)):
    """Bucket ages into half-open intervals for household_characteristics.

    ``age_cuts`` is a strictly increasing tuple of **interior breakpoints**,
    each strictly positive.  Breakpoints may be fractional (e.g. ``0.5`` to
    separate neonates from older infants).  The tuple partitions ages into
    ``len(age_cuts) + 1`` half-open buckets:

        [0, c_0), [c_0, c_1), ..., [c_{n-1}, inf)

    The default ``(4, 9, 14, 19, 31, 51)`` reproduces the demographic
    buckets `00-03`, `04-08`, ..., `51+` used throughout the library.

    Back-compat: earlier releases took ``(0, 4, 9, ...)`` with a leading
    zero.  A leading zero is now stripped with a :class:`DeprecationWarning`
    so legacy callers keep producing identical buckets.

    Parameters
    ----------
    age : array-like
        Numeric ages.  Negative values fall outside the first bucket and
        become NaN.
    age_cuts : tuple of positive numbers, strictly increasing
        Interior breakpoints between buckets.

    Returns
    -------
    pandas.Categorical
        Half-open ``[a, b)`` intervals, one per input age.
    """
    cuts = list(age_cuts)
    if cuts and cuts[0] == 0:
        import warnings
        warnings.warn(
            "age_cuts with a leading 0 is deprecated; pass interior "
            "breakpoints only (the first bucket always starts at 0). "
            "Buckets are unchanged; drop the leading 0 to silence this.",
            DeprecationWarning,
            stacklevel=2,
        )
        cuts = cuts[1:]
    if not cuts:
        raise ValueError("age_cuts must contain at least one positive breakpoint")
    if any(c <= 0 for c in cuts):
        raise ValueError(f"age_cuts breakpoints must be > 0; got {age_cuts}")
    if any(a >= b for a, b in zip(cuts, cuts[1:])):
        raise ValueError(f"age_cuts must be strictly increasing; got {age_cuts}")
    bins = [0, *cuts, np.inf]
    return pd.cut(age, bins, duplicates='drop', right=False)

def dummies(df,cols,suffix=False):
    """From a dataframe df, construct an array of indicator (dummy) variables,
    with a column for every unique row df[cols]. Note that the list cols can
    include names of levels of multiindices.

    The optional argument =suffix=, if provided as a string, will append suffix
    to column names of dummy variables. If suffix=True, then the string '_d'
    will be appended.
    """
    idxcols = list(set(df.index.names).intersection(cols))
    colcols = list(set(cols).difference(idxcols))

    v = concat([use_indices(df,idxcols),df[colcols]],axis=1)

    usecols = []
    for s in idxcols+colcols:
        usecols.append(v[s].squeeze())

    tuples = pd.Series(list(zip(*usecols)),index=v.index)

    v = get_dummies(tuples).astype(int)

    if suffix==True:
        suffix = '_d'

    if suffix!=False and len(suffix)>0:
        columns = [tuple([str(c)+suffix for c in t]) for t in v.columns]
    else:
        columns = v.columns

    v.columns = MultiIndex.from_tuples(columns,names=idxcols+colcols)

    return v

def _is_int_bound(x):
    """True if ``x`` is an integer-valued finite number."""
    try:
        return np.isfinite(x) and float(x).is_integer()
    except (TypeError, ValueError):
        return False


def _fmt_bound(x):
    """Render a numeric bound for use inside an explicit ``[a, b)`` label."""
    if _is_int_bound(x):
        return str(int(x))
    return f"{x:g}"


def format_interval(interval, compact=True):
    """Render a ``pd.Interval`` as a human-readable column-name label.

    Two styles, selected by the ``compact`` flag:

    * **Compact (default)** — matches the historical ``household_characteristics``
      column names.  Finite integer bounds span-1 apart or wider collapse to
      ``"{lo:02d}-{hi-1:02d}"`` (e.g. ``[0, 4)`` → ``"00-03"``); the
      unbounded-top bucket renders as ``"{lo:02d}+"`` (e.g. ``[51, inf)`` →
      ``"51+"``).
    * **Explicit** — half-open interval notation ``"[lo, hi)"`` with
      ``"{lo}+"`` for the unbounded top bucket.  Used whenever any bound is
      fractional (spans shorter than a year).

    :func:`roster_to_characteristics` picks ``compact=True`` when all
    ``age_cuts`` are integers and ``False`` otherwise, so column names stay
    consistent within a single call.
    """
    lo, hi = interval.left, interval.right
    if hi == np.inf:
        return f"{int(lo):02d}+" if _is_int_bound(lo) else f"{_fmt_bound(lo)}+"
    if compact and _is_int_bound(lo) and _is_int_bound(hi) and (hi - lo) >= 1:
        return f"{int(lo):02d}-{int(hi) - 1:02d}"
    return f"[{_fmt_bound(lo)}, {_fmt_bound(hi)})"

def roster_to_characteristics(df, age_cuts=(4, 9, 14, 19, 31, 51), drop='pid', final_index=['t', 'v', 'i']):
    """Collapse a household roster into household-level sex × age counts.

    Drives the derived ``household_characteristics`` table: takes a
    person-level roster indexed by (at least) ``('t', 'v', 'i', 'pid')``,
    buckets each person into a ``sex_age`` category, and returns a
    household-level DataFrame with one integer column per bucket plus a
    ``log HSize`` column (log of household size). Called automatically
    via :data:`lsms_library.country._ROSTER_DERIVED` when a user asks
    ``Country(name).household_characteristics()``.

    Parameters
    ----------
    df : pandas.DataFrame
        Household roster with ``Sex`` and ``Age`` columns (case-insensitive).
    age_cuts : tuple of positive numbers, strictly increasing
        Interior breakpoints separating age buckets.  See
        :func:`age_intervals` for the full semantics; the default
        ``(4, 9, 14, 19, 31, 51)`` produces the historical buckets
        ``00-03``, ``04-08``, ``09-13``, ``14-18``, ``19-30``, ``31-50``,
        ``51+``.  Fractional breakpoints (e.g. ``(0.5, 1, 5)``) are
        allowed and switch the label format from compact ``00-03``-style
        to explicit half-open ``[lo, hi)``-style.
    drop : str
        Index level to drop before aggregation (typically ``'pid'``).
    final_index : list[str]
        Final groupby level; defaults to the household key ``('t', 'v',
        'i')`` but ``Country._finalize_result`` can pass a different
        tuple when the roster's actual index differs.

    Returns
    -------
    pandas.DataFrame
        Household-level counts with one column per sex × age bucket and
        a ``log HSize`` column.
    """
    roster_df = df.copy()
    roster_df.columns = roster_df.columns.str.lower()
    # Clean stringified NA sentinels that leak through from to_parquet/astype(str)
    _na_strings = {'<NA>', 'None', 'nan', ''}
    for col in ('sex', 'age'):
        if col in roster_df.columns:
            roster_df[col] = roster_df[col].replace({s: pd.NA for s in _na_strings})
    # Filter on monthsspent if available: exclude departed members
    # (NaN = question not asked, typically "Left permanently") and
    # members with 0 months of residence in the past 12 months —
    # EXCEPT infants (age < 1), who haven't lived anywhere for 12
    # months but are current household members.  age_handler() can
    # return fractional years (e.g. 0.50 for a 6-month-old) when
    # DOB is available, so the threshold is < 1 not == 0.
    # This matches the replication's lsms.tools.get_household_roster
    # which did dropna(how='any') on [HHID, sex, age, months_spent],
    # plus the stricter exclusion of non-infant zero-month members.
    # Resolve months of residence from whichever column is available.
    # Uganda uses MonthsSpent (months present, 0-12).  East African
    # surveys (Ethiopia, Tanzania, Malawi) use MonthsAway (months
    # absent, 0-12); convert to months-present for a uniform filter.
    ms = None
    if 'monthsspent' in roster_df.columns:
        ms = pd.to_numeric(roster_df['monthsspent'], errors='coerce')
    elif 'monthsaway' in roster_df.columns:
        ma = pd.to_numeric(roster_df['monthsaway'], errors='coerce')
        ms = 12 - ma
        ms = ms.clip(lower=0)  # guard against >12 outliers
    elif 'weeksaway' in roster_df.columns:
        wa = pd.to_numeric(roster_df['weeksaway'], errors='coerce')
        ms = 12 - (wa / (52 / 12))
        ms = ms.clip(lower=0)
    if ms is not None:
        age = pd.to_numeric(roster_df['age'], errors='coerce')
        keep = ms.notna() & ((ms > 0) | (age < 1))
        roster_df = roster_df[keep]
    roster_df = roster_df.dropna(subset=['sex', 'age'])
    roster_df['age_interval'] = age_intervals(roster_df['age'], age_cuts)
    # All-integer breakpoints (including the 0 left edge and open inf) → compact
    # "00-03"-style labels that preserve historical column names; any fractional
    # breakpoint triggers explicit "[lo, hi)"-style labels uniformly.
    _compact = all(_is_int_bound(c) for c in age_cuts if c != 0)
    roster_df['sex_age'] = roster_df.apply(
        lambda x: f"{x['sex']} {format_interval(x['age_interval'], compact=_compact)}" if not pd.isna(x['age_interval']) else f"{x['sex']} NA",
        axis=1
    )
    roster_df = dummies(roster_df,['sex_age'])
    roster_df.index = roster_df.index.droplevel(drop)
    # Pandas' ``groupby(...)`` default is ``dropna=True``, which silently
    # excludes rows whose index has NaN in any of ``final_index``.  We
    # keep that drop (the canonical (t, v, i) key assumes v is
    # meaningful) but warn loudly with the per-wave count so the loss
    # isn't invisible to the caller.  GH #197.
    idx_df = roster_df.index.to_frame(index=False)
    nan_mask = idx_df[final_index].isna().any(axis=1)
    n_nan_rows = int(nan_mask.sum())
    if n_nan_rows > 0:
        import warnings as _warnings
        nan_idx = idx_df.loc[nan_mask]
        # Per-wave row count keyed off ``t`` (the typical user-visible
        # axis); HH count is a row-count proxy since (t, i) uniquely
        # identifies a household.
        if 't' in nan_idx.columns:
            per_wave = (
                nan_idx.assign(_one=1)
                .groupby('t', dropna=True)['_one']
                .sum()
                .sort_index()
                .to_dict()
            )
        else:
            per_wave = {'<no-t>': n_nan_rows}
        _warnings.warn(
            f"household_characteristics: dropped {n_nan_rows} roster rows "
            f"with NaN in one of {final_index} (typically v) "
            f"-- per-wave: {per_wave}.  These are usually movers / "
            f"split-offs whose sample() row lacks a cluster code; see "
            f"GH #197.",
            UserWarning,
            stacklevel=3,
        )
    result = roster_df.groupby(level=final_index).sum()
    result['log HSize'] = np.log(result.sum(axis=1))
    result.columns = result.columns.get_level_values(0)
    return result

def fill_v_with_coord_bin(df, target='v', lat='_lat', lon='_lon',
                          grid_degrees=0.05, prefix='@'):
    """Fill blank ``target`` entries with a synthetic ``{prefix}lat,lon`` label.

    For rows where ``target`` is NA or the empty string, build a synthetic
    cluster identifier from the modified coordinates: bin ``lat``/``lon`` to
    a ``grid_degrees`` grid and format as ``{prefix}{lat:+.2f},{lon:+.2f}``.
    Rows whose ``lat`` or ``lon`` is itself missing keep their missing
    ``target`` — nothing can be synthesised from unknown coordinates.

    Intended as a ``derived:`` transformer for ``sample()`` in panels where
    some households (movers, split-offs) fall outside the original sampling
    frame and therefore have no panel-EA code, yet do carry modified GPS
    coordinates so a within-wave spatial cluster can still be expressed.
    The ``@`` prefix keeps the synthetic labels distinct from real
    numeric-string EA codes, so downstream joins to the community
    questionnaire naturally don't match them.

    Parameters
    ----------
    df : pandas.DataFrame
        Post-merge frame containing ``target``, ``lat``, and ``lon``
        columns.  Target may be the empty string or NA on rows to fill.
    target : str
        Name of the column to fill (default ``'v'``).
    lat, lon : str
        Names of the latitude/longitude columns (defaults ``'_lat'`` /
        ``'_lon'`` — the leading underscore is a convention for temporary
        columns that will be removed by a ``drop:`` clause).
    grid_degrees : float
        Grid resolution in decimal degrees.  Default 0.05 ≈ 5.5 km at the
        equator, which matches the rural coordinate-jitter applied by
        LSMS-ISA geo files so two physically co-located households
        reliably share a bin.
    prefix : str
        Prefix distinguishing synthetic labels from real EA codes; default
        ``'@'``.  Joins against real-EA tables (e.g. a community
        questionnaire keyed by the original EA code) will not match
        synthetic labels — by construction, not by accident.

    Returns
    -------
    pandas.DataFrame
        Copy of ``df`` with ``target`` populated where coordinates allow.
    """
    if target not in df.columns:
        raise ValueError(f"{target!r} not in DataFrame columns; cannot coalesce")
    for col in (lat, lon):
        if col not in df.columns:
            raise ValueError(
                f"{col!r} not in DataFrame columns; cannot synthesise "
                f"{target!r} from coordinates"
            )
    out = df.copy()
    v = out[target]
    # A "blank" target is NA or the empty string — we treat them the same for
    # fill purposes, and normalise both to pd.NA in the output when we can't
    # fill (so downstream consumers see one missing sentinel, not two).
    blank = v.isna() | (v.astype('string').fillna('') == '')
    lat_vals = pd.to_numeric(out[lat], errors='coerce')
    lon_vals = pd.to_numeric(out[lon], errors='coerce')
    coords_ok = lat_vals.notna() & lon_vals.notna()
    fill_mask = blank & coords_ok
    cant_fill = blank & ~coords_ok
    lat_bin = (lat_vals / grid_degrees).round() * grid_degrees
    lon_bin = (lon_vals / grid_degrees).round() * grid_degrees
    synthetic = (
        prefix
        + lat_bin.map(lambda x: f'{x:+.2f}' if pd.notna(x) else '')
        + ','
        + lon_bin.map(lambda x: f'{x:+.2f}' if pd.notna(x) else '')
    ).astype('string')
    # String dtype throughout so the union is homogeneous.
    result = v.astype('string')
    result = result.mask(fill_mask, synthetic)
    result = result.mask(cant_fill, pd.NA)
    # Also normalise any pre-existing empty strings that weren't filled.
    result = result.replace({'': pd.NA})
    out[target] = result
    return out


# Registry of transformers usable from a ``derived:`` block in
# ``data_info.yml``.  Each entry is ``kind: callable(df, target=<col>, **kwargs)``
# — the dispatcher supplies ``target`` from the derived-block key; everything
# else comes from the YAML.  Add new transformers here; document their
# kwargs in the callable's docstring.
_DERIVED_TRANSFORMERS = {
    'coalesce_coord_bin': fill_v_with_coord_bin,
}


def apply_derived(df, derived_spec):
    """Apply a ``derived:`` block from ``data_info.yml`` to a DataFrame.

    ``derived_spec`` is ``{output_col: {kind: <name>, **kwargs}}``; each
    entry dispatches to the transformer registered under ``kind`` in
    :data:`_DERIVED_TRANSFORMERS`.  The target column name is forwarded
    to the transformer as ``target=<output_col>`` and must not appear
    in the YAML kwargs.  Unknown ``kind`` raises ``ValueError`` with
    the list of registered transformers so mistakes surface at read
    time, not in a mysterious downstream KeyError.
    """
    if not derived_spec:
        return df
    for output_col, step in derived_spec.items():
        step = dict(step)                          # don't mutate caller
        kind = step.pop('kind', None)
        if kind is None:
            raise ValueError(
                f"derived: entry for {output_col!r} missing required 'kind:' key"
            )
        if 'target' in step:
            raise ValueError(
                f"derived: entry for {output_col!r} must not set 'target:' "
                f"(target is taken from the block key)"
            )
        fn = _DERIVED_TRANSFORMERS.get(kind)
        if fn is None:
            registered = sorted(_DERIVED_TRANSFORMERS)
            raise ValueError(
                f"derived: unknown kind {kind!r} for column {output_col!r}; "
                f"registered transformers: {registered}"
            )
        df = fn(df, target=output_col, **step)
    return df


def conversion_to_kgs(df, price = ['Expenditure'], quantity = 'Quantity', index=['t','m','i'], unit_col = 'u'):
    """Infer local-unit → kg conversion factors from price ratios.

    For each unit that does not appear in :data:`KNOWN_METRIC`, this
    function computes a factor by assuming the *price per kilogram*
    should be roughly constant across units for the same item/market.
    That is: if a "bunch" of item j trades at roughly 2× the unit value
    of a kg of item j, the inferred factor is 2 kg per bunch.

    The mechanics: expenditure is divided by quantity to get a per-unit
    price, grouped to the ``index`` level (default ``('t','m','i')``),
    then the median across rows is compared to the unit-wise median to
    back out kg per unit. Used by :func:`_get_kg_factors` as a fallback
    when a survey doesn't ship its own conversion table.

    Parameters
    ----------
    df : pandas.DataFrame
        Food-acquired frame with ``Expenditure`` and ``Quantity``
        columns and ``u`` (or ``unit_col``) in the index.
    price : list[str]
        Column(s) interpreted as expenditure for the ratio calculation.
    quantity : str
        Column interpreted as quantity.
    index : list[str]
        Groupby levels for the per-item/period median step.
    unit_col : str
        Name of the unit index level; renamed to ``u`` if different.

    Returns
    -------
    dict[str, float]
        Mapping of (lowercased) unit label → inferred kg factor.
        Units already in :data:`KNOWN_METRIC` or that cannot be inferred
        are absent from the output.
    """
    v = df.copy()
    v = v.replace(0, np.nan)
    unit_conversion = {
        'kg': 1,
        'kilogram': 1,
        'gram': 1 / 1000,
        'g': 1 / 1000,
        'pound': 0.453592,
        'lbs': 0.453592,
        'kilogramme': 1,
        'gramm': 1 / 1000
    }
    #convert the value type in index level 'u' to be string
    v = v.reset_index(unit_col)
    if unit_col != 'u':
        v = v.rename(columns={unit_col: 'u'})
    # Vectorize: ``astype(str)`` followed by ``.str.lower()`` handles any
    # underlying dtype (object with NaN, pyarrow string with pd.NA,
    # Categorical from a .dta read), whereas the previous row-by-row
    # ``.apply`` could surface a Python float for an NA cell despite the
    # earlier ``astype(str)`` (pandas 2.x AttributeError: 'float' object
    # has no attribute 'lower').  Unknown units map to NaN, matching the
    # original ``unit_conversion.get(..., np.nan)`` semantics.
    factors = v['u'].astype(str).str.lower().map(unit_conversion).astype(float)
    v['Kgs'] = v[quantity] * factors
    v = v.set_index('u', append=True)
    pkg = v[price].divide(v['Kgs'], axis=0)
    pkg = pkg.groupby(index).median().median(axis=1)
    po = v[price].groupby(index + ['u']).median().median(axis=1)
    kgper = (po / pkg).dropna()
    kgper = kgper.groupby('u').median()
    #convert to dict
    kgper = kgper.to_dict()
    return kgper


# ---------------------------------------------------------------------------
# Derived food tables from food_acquired
# ---------------------------------------------------------------------------

# Column aliases: map legacy (Tanzania, etc.) names to the canonical names
# used by the transformation functions below.
_COLUMN_ALIASES = {
    'value_purchase': 'Expenditure',
    'expenditure': 'Expenditure',
    'quant_ttl_consume': 'Quantity',
    'quantity_consumed': 'Quantity',
    'quant_purchase': 'Quantity',  # fallback if quant_ttl_consume absent
}

# Column names that should be promoted to the 'u' index level
_UNIT_COLUMN_ALIASES = ['u', 'units', 'u_consumed', 'unit']


def _normalize_columns(df):
    """Rename legacy food_acquired columns to canonical names if needed.

    Also promotes a unit column to the 'u' index level when 'u' is not
    already in the index.
    """
    renames = {}
    for old, new in _COLUMN_ALIASES.items():
        if new not in df.columns and old in df.columns and new not in renames.values():
            renames[old] = new
    if renames:
        df = df.rename(columns=renames)

    # Promote unit column to 'u' index level if not already present
    if 'u' not in df.index.names:
        for col in _UNIT_COLUMN_ALIASES:
            if col in df.columns:
                df = df.rename(columns={col: 'u'}).set_index('u', append=True)
                break
            elif col in df.index.names and col != 'u':
                df.index = df.index.rename({col: 'u'})
                break

    return df

KNOWN_METRIC = {
    'kg': 1, 'kilogram': 1, 'kilogramme': 1,
    'g': 1/1000, 'gram': 1/1000, 'gramm': 1/1000,
    'l': 1, 'litre': 1, 'liter': 1,
    'ml': 1/1000, 'cl': 1/100,
    'pound': 0.453592, 'lbs': 0.453592,
}


def _get_kg_factors(df):
    """Build a combined kg-per-unit mapping from known metric units
    and price-ratio inference on the data."""
    factors = dict(KNOWN_METRIC)

    # Infer additional factors from price ratios where possible
    if 'Expenditure' in df.columns and 'Quantity' in df.columns:
        # Determine which index levels are available for grouping
        idx_names = list(df.index.names)
        group_levels = [n for n in ['t', 'm', 'i'] if n in idx_names]
        if group_levels:
            try:
                inferred = conversion_to_kgs(df, index=group_levels)
                # Inferred factors fill in where known metric doesn't cover
                for unit, factor in inferred.items():
                    if unit.lower() not in factors and np.isfinite(factor) and factor > 0:
                        factors[unit.lower()] = factor
            except (ValueError, ZeroDivisionError, KeyError):
                # Inference is best-effort; numeric / lookup failure means
                # we proceed with the known-metric factors only.  Programmer
                # bugs (TypeError, AttributeError) propagate.
                pass

    return factors


def _apply_kg_conversion(df, factors):
    """Convert Quantity to kg using the factors dict.
    Returns a copy with a 'Quantity_kg' column added."""
    v = df.copy()
    if 'u' in v.index.names:
        units = v.index.get_level_values('u').astype(str).str.lower()
    else:
        return v

    v['Quantity_kg'] = v['Quantity'] * units.map(factors)
    return v


def food_expenditures_from_acquired(df):
    """Derive food expenditures from food_acquired.

    Returns a DataFrame of total expenditure per household × item × period,
    summed over units.
    """
    df = _normalize_columns(df)
    if 'Expenditure' not in df.columns:
        raise ValueError("food_acquired must have an 'Expenditure' column")

    idx_names = list(df.index.names)
    group_by = [n for n in ['t', 'v', 'i', 'j'] if n in idx_names]

    x = df[['Expenditure']].replace(0, np.nan).dropna()
    x = x.groupby(group_by).sum()
    return x


def food_quantities_from_acquired(df):
    """Derive food quantities (in kg) from food_acquired.

    Uses known metric conversions and price-ratio inference to convert
    local units to kg, then sums per household × item × period.
    """
    df = _normalize_columns(df)
    if 'Quantity' not in df.columns:
        raise ValueError("food_acquired must have a 'Quantity' column")

    factors = _get_kg_factors(df)
    v = _apply_kg_conversion(df, factors)

    idx_names = list(v.index.names)
    group_by = [n for n in ['t', 'v', 'i', 'j'] if n in idx_names]

    q = v[['Quantity_kg']].rename(columns={'Quantity_kg': 'Quantity'})
    q = q.replace(0, np.nan).dropna()
    q = q.groupby(group_by).sum()
    return q


def food_prices_from_acquired(df):
    """Derive food prices (per kg) from food_acquired.

    Unit values are computed as Expenditure / Quantity_kg and returned
    at the natural grain of the input (typically ``(t, v, i, j, u)`` or
    a subset).  It is the analyst's responsibility to compute medians /
    means across whatever dimension they care about — returning the raw
    per-observation prices preserves information and lets the downstream
    analysis choose its own aggregation.
    """
    df = _normalize_columns(df)
    if 'Expenditure' not in df.columns or 'Quantity' not in df.columns:
        raise ValueError("food_acquired must have 'Expenditure' and 'Quantity' columns")

    factors = _get_kg_factors(df)
    v = _apply_kg_conversion(df, factors)

    v['price_per_kg'] = v['Expenditure'] / v['Quantity_kg']
    v = v[['price_per_kg']].replace([0, np.inf, -np.inf], np.nan).dropna()

    return v.rename(columns={'price_per_kg': 'Price'})


def legacy_locality(country):
    """Reproduce the pre-deprecation output of Country(X).locality().

    Returns a DataFrame indexed by (i, t, m) with a single column
    ``Parish``, where m is the region label and ``Parish`` is the
    parish/cluster identifier (formerly named ``v`` in the deprecated
    interface, renamed in GH #151 to avoid collision with the cluster
    ``v`` used everywhere else in the API).

    Implemented by joining sample() and cluster_features() — both
    first-class tables that carry the same information.

    This exists as a compatibility shim for callers migrating off the
    deprecated locality() method. New code should use sample() and
    cluster_features() directly.

    Parameters
    ----------
    country : Country
        The Country instance (e.g., ``ll.Country('Uganda')``).

    Returns
    -------
    pd.DataFrame
        DataFrame with MultiIndex (i, t, m) and a single column 'Parish'.
    """
    sample = country.sample().reset_index()
    cluster = country.cluster_features().reset_index()
    # Bring cluster-level Region under a non-colliding name so the merge
    # doesn't pandas-suffix it.  Since b87028d4 (HH-level Region in
    # sample()), both tables carry a 'Region' column; matching
    # _add_market_index (e8f79e93) we treat sample.Region as primary
    # and cluster.Region as the fallback for HHs whose sample row is
    # NaN (3227a39f).
    cluster_subset = cluster[['t', 'v', 'Region']].rename(
        columns={'Region': '_cluster_Region'}
    )
    loc = sample.merge(cluster_subset, on=['t', 'v'], how='left')
    if 'Region' in loc.columns:
        loc['m'] = loc['Region'].fillna(loc['_cluster_Region'])
    else:
        loc['m'] = loc['_cluster_Region']
    loc = loc.drop(columns=[c for c in ('Region', '_cluster_Region')
                            if c in loc.columns])
    # Rename v -> Parish to avoid semantic collision with the cluster 'v'
    # used elsewhere in the API.
    loc = loc.rename(columns={'v': 'Parish'})
    return loc.set_index(['i', 't', 'm'])[['Parish']].sort_index()
