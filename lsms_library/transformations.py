#!/usr/bin/env python3

"""
A collection of mappings to transform dataframes.
"""
import re

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

    # Optional exact per-row kg factor (e.g. Nigeria's s10bq2_cvn, GH #378):
    # one factor per input row, applied to whichever source quantity this is,
    # producing a *summable* per-source Quantity_kg.  Valid here because the
    # melt is pre-collapse (one row, one factor); the lossy step would be
    # summing a factor, which we never do.  The factor itself is not carried
    # past the melt -- only the resulting Quantity_kg.
    kg_factor = (pd.to_numeric(work['kg_factor'], errors='coerce').values
                 if 'kg_factor' in work.columns else None)

    def _row_dict(source, qty, expenditure):
        d = {lvl: work[lvl].values for lvl in base_levels}
        d['s'] = source
        d['Quantity'] = qty
        d['Expenditure'] = expenditure
        if kg_factor is not None:
            d['Quantity_kg'] = qty * kg_factor
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


def _finalize_canonical_food_acquired(out: pd.DataFrame,
                                      *,
                                      index_levels=('t', 'i', 'j', 'u', 's'),
                                      dedupe: bool = True) -> pd.DataFrame:
    """Shared filter (+ optional dedupe) tail for the country-level
    ``food_acquired_to_canonical`` builders (Uganda / Tanzania / Malawi).

    Extracted per GH #251 so the canonical filter rule lives in one place
    rather than being copied into each country file (the 2026-05-08
    expenditure-only-row fix had to touch all of them).

    Filter
        Keep rows where ``Quantity > 0`` OR ``Expenditure > 0``.  An
        expenditure-only row (food reported by value but not quantity) is
        legitimate and is carried through with NaN ``Quantity``.

    Dedupe (``dedupe=True``)
        Genuine source-data duplicates on a canonical key — e.g. two
        ``Other (Specify)`` rows that lump distinct foods under one
        ``(t, i, j, u, s)`` — are aggregated: ``Quantity`` and
        ``Expenditure`` summed with ``min_count=1`` (an all-NaN group stays
        NaN rather than coercing to 0), and a per-unit ``Price`` (when the
        column is present) averaged across the duplicate rows.  When
        ``dedupe=False`` the rows are kept as-is (the caller's pre-melt
        already yields unique keys) and only the filter + reindex apply.

    Note
        ``food_acquired_to_canonical`` (this module) intentionally does NOT
        route through this helper: it filters per source *before* the
        purchased/produced concat and its index carries an optional ``v``
        level, so its tail has a different shape.  See #251.

    Parameters
    ----------
    out : pd.DataFrame
        Flat long-form frame with the canonical levels (``t, i, j, u, s``)
        as *columns* plus ``Quantity``, ``Expenditure``, and optionally
        ``Price``.
    index_levels : iterable of str
        Levels to group / index on.  Default ``('t', 'i', 'j', 'u', 's')``.
    dedupe : bool
        Aggregate duplicate canonical keys (True) or just filter + reindex
        (False).

    Returns
    -------
    pd.DataFrame
        Indexed on ``index_levels`` and sorted.
    """
    qty_ok = out['Quantity'].notna() & (out['Quantity'] > 0)
    exp_ok = out['Expenditure'].notna() & (out['Expenditure'] > 0)
    out = out[qty_ok | exp_ok]

    levels = list(index_levels)
    if dedupe:
        aggs = {
            'Quantity': ('Quantity', lambda s: s.sum(min_count=1)),
            'Expenditure': ('Expenditure', lambda s: s.sum(min_count=1)),
        }
        if 'Price' in out.columns:
            # Price is per-unit, so it averages (not sums) across dup rows.
            aggs['Price'] = ('Price', 'mean')
        if 'Quantity_kg' in out.columns:
            # Exact per-row kg (GH #378) is a *quantity*, so it sums like
            # Quantity -- this is what lets different-size rows of one
            # (item, unit) collapse without losing the exact kg total.
            aggs['Quantity_kg'] = ('Quantity_kg', lambda s: s.sum(min_count=1))
        out = out.groupby(levels, dropna=False).agg(**aggs)
    else:
        out = out.set_index(levels)

    return out.sort_index()


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

def roster_to_characteristics(df, age_cuts=(4, 9, 14, 19, 31, 51), drop='pid',
                              final_index=['t', 'v', 'i'],
                              mover_sentinel='Mover'):
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
    mover_sentinel : str or None, default ``'Mover'``
        Value to substitute for NaN entries in any ``final_index``
        level (in practice always ``v``) before the household
        groupby.  When non-None, mover / split-off households with a
        NaN cluster code survive as a distinct bucket identifiable
        by ``index.get_level_values('v') == mover_sentinel``.  Pass
        ``None`` to recover the legacy GH #197 behavior where the
        groupby silently drops these households.

        Default flipped from drop-via-NaN to keep-with-sentinel in
        GH #268.  ``sample()`` typically carries a valid ``Region``
        for movers even when ``v`` is missing, so dropping them at
        this stage loses households we /can/ still assign to a
        market.  Callers who want the strict legacy drop pass
        ``mover_sentinel=None``.

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
    # Pandas' ``groupby(...)`` default is ``dropna=True``, so rows whose
    # index has NaN in any of ``final_index`` are silently excluded.
    # The NaN is almost always in ``v``: ``v`` is joined onto the
    # roster post-hoc via ``_join_v_from_sample`` and ``sample``'s
    # ``v`` column is NaN for movers / split-offs that lack a cluster
    # code.  GH #197 flagged the silent drop; GH #268 makes the
    # keep-with-sentinel path the default (movers survive as a
    # distinguishable ``v == mover_sentinel`` bucket) and turns the
    # legacy drop into an opt-in via ``mover_sentinel=None``.
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
        if mover_sentinel is None:
            _warnings.warn(
                f"household_characteristics: dropped {n_nan_rows} roster "
                f"rows with NaN in one of {final_index} (typically v) "
                f"-- per-wave: {per_wave}.  These are usually movers / "
                f"split-offs whose sample() row lacks a cluster code; "
                f"the groupby below silently excludes them.  To keep "
                f"them as an identifiable bucket, pass a non-None "
                f"``mover_sentinel`` (default ``'Mover'``).  See "
                f"GH #197, GH #268.",
                UserWarning,
                stacklevel=3,
            )
        else:
            # GH #268: fill NaN in every final_index level with the
            # sentinel so movers survive the household groupby as a
            # distinguishable bucket rather than getting dropped.
            for level in final_index:
                if idx_df[level].isna().any():
                    idx_df[level] = idx_df[level].fillna(mover_sentinel)
            roster_df.index = pd.MultiIndex.from_frame(idx_df)
            _warnings.warn(
                f"household_characteristics: replaced {n_nan_rows} roster "
                f"rows with NaN in one of {final_index} (typically v) by "
                f"sentinel {mover_sentinel!r} -- per-wave: {per_wave}.  "
                f"These are usually movers / split-offs whose sample() row "
                f"lacks a cluster code.  Filter on "
                f"index.get_level_values('v') == {mover_sentinel!r} to "
                f"recover the legacy drop (GH #197, GH #268).",
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
    # Rows with an exact per-row Quantity_kg (e.g. Malawi cfactor units, GH
    # #378) serve as kg *references* for the price-per-kg baseline -- exactly
    # as they did when they were build-time-converted to u='kg' -- but they
    # are excluded from the per-unit inference below (their kg is known, not
    # inferred).  This keeps the data-driven factors for genuinely-unknown
    # units identical before/after the Quantity_kg migration.
    if 'Quantity_kg' in v.columns:
        v['Kgs'] = v['Kgs'].where(v['Kgs'].notna(), v['Quantity_kg'])
    v = v.set_index('u', append=True)
    pkg = v[price].divide(v['Kgs'], axis=0)
    pkg = pkg.groupby(index).median().median(axis=1)
    v_infer = (v[v['Quantity_kg'].isna()] if 'Quantity_kg' in v.columns else v)
    po = v_infer[price].groupby(index + ['u']).median().median(axis=1)
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

# Subset of ``KNOWN_METRIC`` whose factors only hold under the implicit
# ``1 litre = 1 kg`` assumption (specific-gravity-1 approximation).
# Stripped from the factor map when ``volume_as_mass=False`` is requested
# at the public API.
_FLUID_UNITS = ('l', 'litre', 'liter', 'ml', 'cl')

# Explicit-metric pattern triples: (regex, scale, is_volume).
# ``regex`` matches the numeric prefix; ``scale`` converts to kg (or kg-
# equivalent under ``volume_as_mass=True``); ``is_volume=True`` means the
# pattern is gated by the ``volume_as_mass`` kwarg.
#
# Order matters: kg before g (so "50 kg" doesn't first match "5" "0kg"),
# and the more specific patterns (``ml``, ``litre``) before less
# specific (``l``).  Decimal numbers (``0.5 kg``) are accepted.  Word
# boundaries on the unit token prevent ``20gallon`` from matching the
# ``g`` pattern.
_EXPLICIT_METRIC_PATTERNS = (
    (re.compile(r'(\d+(?:\.\d+)?)\s*(?:kg|kilogram|kilogramme)\b',
                re.IGNORECASE), 1, False),
    (re.compile(r'(\d+(?:\.\d+)?)\s*(?:gram|gramme|grams|grammes|gr|g)\b',
                re.IGNORECASE), 1/1000, False),
    (re.compile(r'(\d+(?:\.\d+)?)\s*(?:lbs?|pounds?)\b',
                re.IGNORECASE), 0.453592, False),
    (re.compile(r'(\d+(?:\.\d+)?)\s*ml\b',
                re.IGNORECASE), 1/1000, True),
    (re.compile(r'(\d+(?:\.\d+)?)\s*cl\b',
                re.IGNORECASE), 1/100, True),
    (re.compile(r'(\d+(?:\.\d+)?)\s*(?:litres?|liters?|l)\b',
                re.IGNORECASE), 1, True),
)


def _parse_explicit_metric(s, *, volume_as_mass=True):
    """Extract a kg factor from a unit label that names its own metric content.

    Returns the kg-per-unit factor, or ``None`` if no metric content is
    found in *s*.  When *volume_as_mass* is False, volume-based patterns
    (litre, ml, cl) decline to match -- letting the caller (typically
    :func:`_get_kg_factors`) fall back to the price-ratio inference path
    for those units rather than asserting ``1 L = 1 kg`` by fiat.

    Examples
    --------
    >>> _parse_explicit_metric('50 kg Bag')
    50.0
    >>> _parse_explicit_metric('500 g packet')
    0.5
    >>> _parse_explicit_metric('1L Carton')
    1.0
    >>> _parse_explicit_metric('500 ml Bottle')
    0.5
    >>> _parse_explicit_metric('500 ml Bottle', volume_as_mass=False) is None
    True
    >>> _parse_explicit_metric('Heap (Small)') is None
    True
    >>> _parse_explicit_metric('2 lbs sack')
    0.907184
    """
    if not isinstance(s, str):
        return None
    for pattern, scale, is_volume in _EXPLICIT_METRIC_PATTERNS:
        if is_volume and not volume_as_mass:
            continue
        m = pattern.search(s)
        if m:
            try:
                return float(m.group(1)) * scale
            except (ValueError, IndexError):
                return None
    return None


def _get_kg_factors(df, *, volume_as_mass=True):
    """Build a combined kg-per-unit mapping from known metric units,
    explicit-metric label parsing, and price-ratio inference on the data.

    Parameters
    ----------
    df : pd.DataFrame
        food_acquired-shaped DataFrame; expected to have ``u`` in the
        index (or columns).
    volume_as_mass : bool, default True
        When True (default), treat ``1 litre = 1 kg`` for fluid units
        (litre, ml, cl) -- a specific-gravity-1 approximation that's
        roughly right for water-based foods (milk, juice, soup) and
        moderately wrong for cooking oil and alcohol.  When False, fluid
        units are removed from the hand-coded factor map and from the
        explicit-metric label parser; their kg conversion (if any) then
        comes from data-driven price-ratio inference, which empirically
        recovers the actual specific gravity per (item, region, time)
        when enough kg-reporting households share the cell.
    """
    factors = dict(KNOWN_METRIC)
    if not volume_as_mass:
        for u in _FLUID_UNITS:
            factors.pop(u, None)

    # Explicit-metric parser: derive factors from labels that name their
    # own metric content (e.g. '50 kg Bag', '500 g Packet', '1L Carton').
    # Lower-case the keys so they match the lower-cased lookup in
    # ``_apply_kg_conversion``.  Don't override factors already in
    # ``KNOWN_METRIC`` -- those are exact-match tokens that shouldn't
    # be re-derived via the parser.
    if 'u' in df.index.names:
        for u in df.index.get_level_values('u').dropna().unique():
            key = str(u).lower()
            if key in factors:
                continue
            kg = _parse_explicit_metric(str(u), volume_as_mass=volume_as_mass)
            if kg is not None and np.isfinite(kg) and kg > 0:
                factors[key] = kg

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
    Returns a copy with a 'Quantity_kg' column added.

    An *exact, survey-provided* per-row ``Quantity_kg`` (e.g. Nigeria's
    ``s10bq2_cvn``, GH #378 / DESIGN_per_row_kg_quantity) takes precedence
    where it is present and non-null; the unit→factor map only fills the
    rows that lack it.  Carried as a summable quantity (not a factor) because
    the canonical index has no size level -- see the design doc."""
    v = df.copy()
    if 'u' in v.index.names:
        units = v.index.get_level_values('u').astype(str).str.lower()
    else:
        return v

    factor_kg = v['Quantity'] * units.map(factors)
    if 'Quantity_kg' in v.columns:
        # Precomputed exact kg wins; fall back to the factor estimate only
        # where the survey didn't supply one.
        v['Quantity_kg'] = v['Quantity_kg'].where(v['Quantity_kg'].notna(),
                                                   factor_kg)
    else:
        v['Quantity_kg'] = factor_kg
    return v


def food_expenditures_from_acquired(df):
    """Derive food expenditures from food_acquired.

    Returns a DataFrame of total expenditure per household × item × period
    × acquisition source (when ``s`` is present in the input index),
    summed over units.

    Phase 4 of GH #169 / DESIGN_food_acquired_canonical_2026-05-05.org
    extends the group-by to preserve the ``s`` (acquisition-source) index
    level.  Users who want the legacy collapsed view call
    ``food_expenditures.groupby(level=['t','v','i','j']).sum()``
    explicitly.
    """
    df = _normalize_columns(df)
    if 'Expenditure' not in df.columns:
        raise ValueError("food_acquired must have an 'Expenditure' column")

    idx_names = list(df.index.names)
    # Preserve `s` in the output (Phase 4).  `u` is dropped — Expenditure
    # is currency-denominated, so summing across units is meaningful.
    # `v` is also omitted: with pandas's default ``dropna=True``, a
    # groupby that includes ``v`` silently drops HH whose cluster ID is
    # unrecoverable in ``sample()``, even though their food-expenditure
    # data is valid.  ``_finalize_result`` re-joins ``v`` from sample at
    # API time and ``_add_market_index`` resolves Region HH-level when
    # asked, so dropping ``v`` here loses no information.  Closes #246
    # part (C-2) NaN-``v`` regression.
    group_by = [n for n in ['t', 'i', 'j', 's'] if n in idx_names]

    x = df[['Expenditure']].replace(0, np.nan).dropna()
    x = x.groupby(group_by).sum()
    return x


def food_quantities_from_acquired(df, units='kgs', *, volume_as_mass=True):
    """Derive food quantities from food_acquired.

    Parameters
    ----------
    df : pd.DataFrame
        food_acquired DataFrame with a ``Quantity`` column and a ``u``
        index level naming the unit each row's quantity is in.
    units : {'kgs', 'units'}, default 'kgs'
        Aggregation basis:

        - ``'kgs'`` (default): convert ``Quantity`` to kilograms where
          the unit's kg factor is known (via :func:`_get_kg_factors`);
          tag those rows with ``u='kg'``.  Rows whose unit lacks a
          factor (e.g. ``u='Value'`` for LCU-only goods such as
          "meals in restaurants", or ``u='tin'`` when no per-tin kg
          conversion is known) are *carried through* with their native
          ``Quantity`` and original ``u`` label, NOT dropped.  The output
          is therefore mixed-physical-unit; the ``u`` index distinguishes
          kg from native rows.  Consumers wanting purely-kg rows do
          ``df.xs('kg', level='u')``.
        - ``'units'``: sum native ``Quantity`` per ``(t, v, i, j, u, s)``,
          no kg conversion attempted.

    Returns
    -------
    pd.DataFrame
        Single-column ``Quantity`` DataFrame with ``u`` and ``s``
        retained in the index (Phase 4 of GH #169 preserves the
        acquisition-source axis).

    Notes
    -----
    The carry rule for unconvertible units in ``'kgs'`` mode is the
    Phase-4 design call recorded in
    ``slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org``.  It
    differs from the original implementation, which silently dropped
    unconvertible rows from ``food_quantities``.

    The output's group-by preserves both ``u`` (the per-row unit; ``'kg'``
    for converted rows, native otherwise) and ``s`` (acquisition source),
    per the GH #169 canonical schema.  Pre-canonical waves where ``s`` is
    absent silently skip the ``s`` level.
    """
    valid_units = {'kgs', 'units'}
    if units not in valid_units:
        raise ValueError(
            f"food_quantities units= must be one of {sorted(valid_units)}, "
            f"got {units!r}"
        )

    df = _normalize_columns(df)
    if 'Quantity' not in df.columns:
        raise ValueError("food_acquired must have a 'Quantity' column")

    idx_names = list(df.index.names)
    if 'u' not in idx_names:
        # No u index level → can't tag units; fall back to a single-bucket
        # aggregation per (t, i, j, s).  `v` is omitted from the group_by
        # to avoid silently dropping HH whose cluster ID is unrecoverable;
        # ``_finalize_result`` re-joins ``v`` from sample at API time.
        group_by = [n for n in ['t', 'i', 'j', 's'] if n in idx_names]
        q = df[['Quantity']].replace(0, np.nan).dropna()
        if group_by:
            q = q.groupby(group_by).sum()
        return q

    if units == 'units':
        # `v` omitted; see `food_expenditures_from_acquired` for rationale.
        group_by = [n for n in ['t', 'i', 'j', 'u', 's'] if n in idx_names]
        q = df[['Quantity']].replace(0, np.nan).dropna()
        q = q.groupby(group_by).sum()
        return q

    # units == 'kgs': carry rule
    factors = _get_kg_factors(df, volume_as_mass=volume_as_mass)
    v = _apply_kg_conversion(df, factors)

    # Per-row: where Quantity_kg is non-NaN, use it and tag u='kg';
    # otherwise carry native Quantity with native u.  Subsumes the
    # Phase-4 (b9df8fb4) s-preserving group-by below.
    converted = v['Quantity_kg'].notna().to_numpy()
    qty_kg = v['Quantity_kg'].to_numpy()
    qty_native = v['Quantity'].to_numpy()
    qty = np.where(converted, qty_kg, qty_native)

    u_native = v.index.get_level_values('u').astype(str).to_numpy()
    u_new = np.where(converted, 'kg', u_native)

    # Rebuild index with the new u column.
    new_levels = []
    for name in v.index.names:
        if name == 'u':
            new_levels.append(u_new)
        else:
            new_levels.append(v.index.get_level_values(name).to_numpy())
    new_idx = pd.MultiIndex.from_arrays(new_levels, names=v.index.names)

    out = pd.DataFrame({'Quantity': qty}, index=new_idx)
    out = out.replace(0, np.nan).dropna()
    # `v` omitted; see `food_expenditures_from_acquired` for rationale.
    group_by = [n for n in ['t', 'i', 'j', 'u', 's'] if n in out.index.names]
    out = out.groupby(group_by).sum()
    return out


def food_prices_from_acquired(df, units='kgvalue', *, volume_as_mass=True):
    """Derive food prices from food_acquired.

    Returned at the natural grain of the input (``(t, v, i, j, u, s)``
    after the Phase 3 canonical reshape, or a legacy subset) — analysts
    compute medians / means across whatever dimension they care about,
    so per-observation Price preserves information.

    Parameters
    ----------
    df : pd.DataFrame
        food_acquired DataFrame.  Must have ``Quantity``; needs
        ``Expenditure`` for the ``*value`` modes and ``Price`` for
        the ``*price`` modes.
    units : {'kgvalue', 'kgprice', 'unitvalue', 'unitprice'}, default 'kgvalue'
        Which Price to compute, varying across two axes (denominator and
        source):

        - ``'kgvalue'`` (default): ``Expenditure / Quantity_kg``
          (currency / kg, derived).  Backward-compatible with the
          pre-Phase-4 implementation.
        - ``'unitvalue'``: ``Expenditure / Quantity`` (currency / native u,
          derived).  For ``u='Value'`` rows (LCU-only goods) the formula
          gives 1 — *Kwacha per Kwacha* — mathematically correct but
          analytically useless; consumers should filter on ``u`` before
          aggregating.
        - ``'kgprice'``: reported ``Price`` × kg_factor (currency / kg).
          NaN where ``Price`` is missing or ``u`` is unconvertible to kg.
        - ``'unitprice'``: reported ``Price`` (currency / native u).
          NaN where the survey did not record a unit price.  This
          mode reflects the canonical ``food_acquired.Price`` column —
          market price for ``s='purchased'``, farmgate for
          ``s='produced'``, imputed for ``s='inkind'``.

        See ``slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org``.

    Returns
    -------
    pd.DataFrame
        Single-column ``Price`` DataFrame at the natural grain of the
        input (typically ``(t, v, i, j, u, s)`` after the s-axis
        migration).  Zero / infinite / NaN prices are dropped.

    Notes
    -----
    The ``'kgvalue'`` default deliberately departs from the term-of-art
    "unit value" common in the literature (e.g. Deaton 1988, 1997),
    which usually means ``Expenditure / Quantity`` standardized to kg.
    The ``'kgvalue'`` / ``'unitvalue'`` naming makes the denominator
    explicit at the cost of mild inconsistency with prior usage; consult
    the docstring before substituting one for "unit value" in literature
    review or reproduction work.

    No silent fallback between modes.  ``'unitprice'`` returns NaN where
    Price is missing rather than falling back to ``'unitvalue'``; a
    caller wanting "best available" combines results explicitly with
    their own provenance tracking.

    For canonical s-axis input, only ``s='purchased'`` rows have a
    meaningful ``Expenditure`` (the Phase-3 helpers set produced/inkind
    Expenditure to NaN).  Under ``'kgvalue'`` / ``'unitvalue'`` those
    rows become NaN-after-divide and drop out.  Under ``'kgprice'`` /
    ``'unitprice'`` produced/inkind rows survive iff the wave script
    populated the survey-reported ``Price`` column upstream (Uganda
    Phase 3 path).  This function does not synthesize a Price.
    """
    valid_units = {'kgvalue', 'kgprice', 'unitvalue', 'unitprice'}
    if units not in valid_units:
        raise ValueError(
            f"food_prices units= must be one of {sorted(valid_units)}, "
            f"got {units!r}"
        )

    df = _normalize_columns(df)

    # Validate inputs per mode.
    if units in {'kgvalue', 'unitvalue'}:
        missing = [c for c in ('Expenditure', 'Quantity') if c not in df.columns]
        if missing:
            raise ValueError(
                f"food_prices(units={units!r}) requires {missing} on food_acquired"
            )
    if units in {'kgprice', 'unitprice'}:
        if 'Price' not in df.columns:
            # No reported Price column at all → empty frame with right schema.
            empty = df.iloc[0:0].copy()
            empty['Price'] = np.array([], dtype='float64')
            return empty[['Price']]

    if units == 'kgvalue':
        factors = _get_kg_factors(df, volume_as_mass=volume_as_mass)
        v = _apply_kg_conversion(df, factors)
        with np.errstate(divide='ignore', invalid='ignore'):
            v = v.assign(Price=v['Expenditure'] / v['Quantity_kg'])
    elif units == 'unitvalue':
        with np.errstate(divide='ignore', invalid='ignore'):
            v = df.assign(Price=df['Expenditure'] / df['Quantity'])
    elif units == 'unitprice':
        v = df.copy()
        # Price column already populated.
    elif units == 'kgprice':
        factors = _get_kg_factors(df, volume_as_mass=volume_as_mass)
        if 'u' in df.index.names:
            u_lower = df.index.get_level_values('u').astype(str).str.lower()
            kg_per_unit = pd.Series(u_lower.map(factors).values, index=df.index)
            with np.errstate(divide='ignore', invalid='ignore'):
                v = df.assign(Price=df['Price'] / kg_per_unit)
        else:
            # No u index → can't convert; emit NaN
            v = df.assign(Price=np.nan)

    v = v[['Price']].replace([0, np.inf, -np.inf], np.nan).dropna()
    return v


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


# ===========================================================================
# Parity TRANSFORMS — analyst-callable aggregates over the item features
# ===========================================================================
#
# These functions embody the library's "aggregation happens in code, not
# data" rule: each CONSUMES one or more *item-level* features (crop_production,
# plot_inputs, plot_labor, livestock, plot_features, household_roster) and
# RETURNS the corresponding *aggregate* — harvest_kg, yield_kg, total labor
# days, nitrogen, seed, TLU, dependency ratio, farm size, plot counts.
#
# They are deliberately NOT registered in ``_FOOD_DERIVED`` / ``_ROSTER_DERIVED``
# and are NOT auto-surfaced as Country features.  An analyst calls them
# explicitly::
#
#     import lsms_library as ll
#     from lsms_library.transformations import harvest_kg, yield_kg
#     cp = ll.Country('Uganda').crop_production()
#     hk = harvest_kg(cp)                       # (t,i,plot,j) -> Harvest_kg
#     pf = ll.Country('Uganda').plot_features()
#     y  = yield_kg(cp, pf)                      # ... -> Yield_kg per ha
#
# Each reproduces the matching column in the World Bank LSMS-ISA harmonised
# panel (Household / Plot / Plotcrop / Individual datasets) to within a
# documented tolerance.  Where our convention or unit-conversion coverage
# diverges from theirs we *note* the divergence rather than force a match —
# their panel is a useful cross-check, not the canonical answer.
#
# Parity context: slurm_logs/2026-06-13_wb_incidence_map/GAP_RANKING.org
# (GAPs 1-4 item layer; GAPs 6-8 + below-the-line transforms).


# Plot-level index names emitted by the various countries' item features.
# ``plot`` is the canonical (Uganda) name; ``plot_id`` is the Tanzania /
# Ethiopia / Malawi name.  The transforms accept either.
_PLOT_LEVELS = ('plot', 'plot_id')


def _resolve_plot_level(names):
    """Return whichever of :data:`_PLOT_LEVELS` is present in ``names``.

    Lets a transform group by the plot grain regardless of whether the
    country's feature names its plot level ``plot`` (Uganda) or ``plot_id``
    (Tanzania, Ethiopia, …).  Returns ``None`` when neither is present.
    """
    for name in _PLOT_LEVELS:
        if name in (names or []):
            return name
    return None


def _with_u_in_index(df):
    """Ensure ``u`` is an index level; promote a ``u`` *column* if needed.

    Countries differ: some item features carry the unit as a ``u`` index
    level (Uganda ``crop_production``), others as a ``u`` column (Tanzania
    ``crop_production``, every country's ``plot_inputs``).  The shared
    :func:`_get_kg_factors` machinery wants ``u`` in the index, so this
    returns a frame guaranteed to have it there — and a flag telling the
    caller whether a column was promoted (so it can map the result back to
    the original index).  Raises ``ValueError`` if no ``u`` is found.
    """
    if 'u' in (df.index.names or []):
        return df, False
    if 'u' in df.columns:
        return df.set_index('u', append=True), True
    raise ValueError("expected a 'u' unit level or column")


def _kg_factor_series(df, *, volume_as_mass=True):
    """Map each row's unit ``u`` to its kg-per-unit factor.

    Thin wrapper over :func:`_get_kg_factors` (the same machinery
    :func:`food_quantities_from_acquired` uses): build the unit→factor
    mapping from this frame, then look each row's unit up in it.  Returns a
    float Series aligned to ``df.index`` whose entries are NaN for units
    with no known/inferable kg factor (the caller decides whether to drop
    or carry those rows).

    Accepts ``u`` as either an index level or a column; raises
    ``ValueError`` if absent.
    """
    work, promoted = _with_u_in_index(df)
    factors = _get_kg_factors(work, volume_as_mass=volume_as_mass)
    units = work.index.get_level_values('u').astype(str).str.lower()
    out = pd.Series(units.map(factors).astype(float), index=work.index)
    if promoted:
        out.index = df.index
    return out


def harvest_kg(crop_production, *, volume_as_mass=True, carry_native=False):
    """Total harvested kilograms per (t, i, plot, j) from ``crop_production``.

    MECHANICAL reduction (GAP 1 → WB ``Plotcrop``/``Plot`` ``harvest_kg``).
    For each reported harvest row, convert the native-unit ``Quantity`` to
    kilograms using the shared unit→kg machinery (:func:`_get_kg_factors`,
    via :func:`_kg_factor_series` — the same factors
    :func:`food_quantities_from_acquired` applies to ``food_acquired``),
    then sum within each plot-crop.

    Parameters
    ----------
    crop_production : pd.DataFrame
        The ``crop_production`` item feature, indexed by
        ``(t, i, plot, j, u, season)`` (``v`` may also be present — it is
        ignored for the reduction and dropped from the output grain) with a
        reported ``Quantity`` column in native unit ``u``.
    volume_as_mass : bool, default True
        Forwarded to :func:`_get_kg_factors`; treat ``1 litre = 1 kg`` for
        fluid units (juice/beer harvest rows) when True.
    carry_native : bool, default False
        When False (default, matching the WB construct), rows whose ``u``
        has no known kg factor contribute NOTHING to the sum — ``Harvest_kg``
        reflects only the convertible quantity, and a plot-crop with no
        convertible rows is absent from the result.  When True, those rows
        are carried at their native ``Quantity`` (a deliberate over-count
        used only for sensitivity analysis); the default False is the
        parity-faithful choice.

    Returns
    -------
    pd.DataFrame
        One ``Harvest_kg`` column indexed by ``(t, i, plot, j)`` (whichever
        of those levels are present), summed over native units and season.

    Notes
    -----
    Divergence from WB: the WB Uganda code applies survey-provided
    conversion tables that cover bespoke local containers ("Sack (100 kgs)",
    "Basket (Unspecified)", regional "Heap"/"Bunch" sizes).  Our shared
    factor map only resolves units that *name* their metric content (e.g.
    "Basket (10 kg)", "Nice cup (60g)") plus the hand-coded metric tokens,
    so on Uganda it converts ~21% of crop rows.  ``Harvest_kg`` is therefore
    a *lower bound* on the WB figure for plots dominated by non-metric
    containers; magnitudes agree on metric-reported plots.  Extending
    coverage is a per-country ``u``-table (``harvest_units``) job, not a
    change to this transform.
    """
    df = crop_production.copy()
    if 'Quantity' not in df.columns:
        raise ValueError("crop_production must have a 'Quantity' column")

    qty = pd.to_numeric(df['Quantity'], errors='coerce')
    kg_per_unit = _kg_factor_series(df, volume_as_mass=volume_as_mass)
    kg = qty * kg_per_unit
    if carry_native:
        kg = kg.where(kg.notna(), qty)

    out = pd.DataFrame({'Harvest_kg': kg})
    out = out.replace(0, np.nan).dropna()
    plot_level = _resolve_plot_level(out.index.names)
    group_by = [n for n in ['t', 'i', plot_level, 'j']
                if n is not None and n in out.index.names]
    res = out.groupby(group_by).sum()
    # Normalise the plot level name to 'plot' so downstream (yield_kg) and
    # cross-country callers see one schema regardless of source naming.
    if plot_level == 'plot_id':
        res.index = res.index.rename({'plot_id': 'plot'})
    return res


def _parcel_from_crop_plot(plot, i):
    """Extract the parcel token from a ``crop_production`` plot id.

    Uganda's ``crop_production.plot`` is ``{hhid}-{parcel}-{plot}`` (e.g.
    ``'1021000108-1-2'``).  Strip the leading ``{hhid}-`` and return the
    first remaining ``-``-delimited token (the parcel).  Returns the plot id
    unchanged when it doesn't carry the ``{hhid}-`` prefix (other countries'
    vocabularies), so the parcel join degrades to a plot join rather than
    erroring.
    """
    s = str(plot)
    prefix = f"{i}-"
    if s.startswith(prefix):
        return s[len(prefix):].split('-', 1)[0]
    return s


def _parcel_from_feature_plot(plot_id):
    """Extract the parcel token from a ``plot_features`` plot id.

    Uganda's ``plot_features.plot_id`` is ``{parcel}_{suffix}`` (e.g.
    ``'1_A'`` / ``'2_B'`` where the suffix flags AGSEC2A-owned vs
    AGSEC2B-rented).  Return the part before the first ``_``.  No-op when
    there is no ``_`` (other vocabularies).
    """
    return str(plot_id).split('_', 1)[0]


def yield_kg(crop_production, plot_features, *, area_col='Area',
             volume_as_mass=True, on='parcel'):
    """Harvested kilograms per unit plot area (WB ``yield_kg``).

    MECHANICAL reduction (GAP 1).  Sums :func:`harvest_kg` and plot area to a
    common land grain, then divides.  Matches the WB ``yield_kg`` (harvest
    summed across crops on a plot/parcel ÷ that land unit's GPS area).

    Parameters
    ----------
    crop_production : pd.DataFrame
        ``crop_production`` item feature (see :func:`harvest_kg`).
    plot_features : pd.DataFrame
        ``plot_features`` item feature carrying a plot area column
        (default ``'Area'``).  Indexed by ``(t, i, plot_id, ...)``.
    area_col : str, default 'Area'
        Name of the area column in ``plot_features``.
    volume_as_mass : bool, default True
        Forwarded to :func:`harvest_kg`.
    on : {'parcel', 'plot'}, default 'parcel'
        Land grain to join on.

        - ``'parcel'`` (default): reconcile the two plot vocabularies to
          their common *parcel* key — ``crop_production.plot`` is
          ``{hhid}-{parcel}-{plot}`` while ``plot_features.plot_id`` is
          ``{parcel}_{suffix}``, and both encode the same parcel.  Harvest
          is summed over the parcel's crops and plots, area over the
          parcel's land sub-units (the AGSEC2A/2B split), and divided.  This
          is the WB-faithful grain (their ``plot_id_merge`` = ``hhid-parcel``)
          and on Uganda the parcel key matches on 100% of crop rows.
        - ``'plot'``: join on the literal ``plot`` value verbatim.  Use only
          where the two features already share a plot vocabulary; on Uganda
          the literal ids never match and the result is empty.

    Returns
    -------
    pd.DataFrame
        One ``Yield_kg`` column indexed by ``(t, i, parcel)`` (or
        ``(t, i, plot)`` for ``on='plot'``) — kilograms per area-unit (the
        area unit is whatever ``plot_features.AreaUnit`` records; Uganda
        stores hectare-equivalent areas, so ``Yield_kg`` is kg/ha).

    Notes
    -----
    Inherits the unit-conversion coverage caveat of :func:`harvest_kg`
    (only metric-named ``u`` rows convert), so ``Yield_kg`` is a *lower
    bound* on the WB figure for parcels dominated by non-metric containers.
    Where the harvest converts and the area key matches, magnitudes track
    the WB ``yield_kg`` distribution.
    """
    if on not in {'parcel', 'plot'}:
        raise ValueError(f"yield_kg on= must be 'parcel' or 'plot', got {on!r}")

    hk = harvest_kg(crop_production, volume_as_mass=volume_as_mass).reset_index()
    if 'plot' not in hk.columns:
        raise ValueError("harvest_kg must yield a 'plot' level to join area")

    pf = plot_features.reset_index()
    plot_key = 'plot' if 'plot' in pf.columns else (
        'plot_id' if 'plot_id' in pf.columns else None)
    if plot_key is None:
        raise ValueError("plot_features must carry a 'plot' or 'plot_id' level")
    if area_col not in pf.columns:
        raise ValueError(f"plot_features must have a {area_col!r} column")

    base = [k for k in ['t', 'i'] if k in hk.columns and k in pf.columns]

    if on == 'parcel':
        hk['_land'] = [
            _parcel_from_crop_plot(p, i)
            for p, i in zip(hk['plot'],
                            hk['i'] if 'i' in hk.columns else [''] * len(hk))
        ]
        pf['_land'] = pf[plot_key].map(_parcel_from_feature_plot)
    else:
        hk['_land'] = hk['plot'].astype(str)
        pf['_land'] = pf[plot_key].astype(str)

    keys = base + ['_land']
    harvest = hk.groupby(keys, dropna=False)['Harvest_kg'].sum().reset_index()

    pf['_Area'] = pd.to_numeric(pf[area_col], errors='coerce')
    pf = pf[pf['_Area'] > 0]
    area = pf.groupby(keys, dropna=False)['_Area'].sum().reset_index()

    merged = harvest.merge(area, on=keys, how='inner')
    with np.errstate(divide='ignore', invalid='ignore'):
        merged['Yield_kg'] = merged['Harvest_kg'] / merged['_Area']
    merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=['Yield_kg'])
    out = merged.rename(columns={'_land': 'parcel' if on == 'parcel' else 'plot'})
    idx = base + ['parcel' if on == 'parcel' else 'plot']
    return out.set_index(idx)[['Yield_kg']].sort_index()


def _labor_days_by_source(plot_labor, source=None, *,
                          days_col='PersonDays'):
    """Sum reported person-days over plots, optionally filtered to a source.

    Shared backend for :func:`total_labor_days`,
    :func:`total_family_labor_days`, :func:`total_hired_labor_days`.  Reduces
    ``plot_labor`` (grain ``(t, i, plot, source, season)``) to the household
    grain ``(t, i)`` by summing ``PersonDays`` across every plot, season, and
    (when ``source`` is None) labor source.
    """
    df = plot_labor
    if days_col not in df.columns:
        raise ValueError(f"plot_labor must have a {days_col!r} column")
    if source is not None:
        if 'source' not in (df.index.names or []):
            raise ValueError("plot_labor must have a 'source' index level")
        df = df.xs(source, level='source', drop_level=True)
    days = pd.to_numeric(df[days_col], errors='coerce')
    out = pd.DataFrame({'_days': days}).dropna()
    group_by = [n for n in ['t', 'i'] if n in out.index.names]
    return out.groupby(group_by)['_days'].sum()


def total_labor_days(plot_labor):
    """Total person-days of all labor per household (WB ``total_labor_days``).

    MECHANICAL reduction (GAP 3).  Sums ``plot_labor.PersonDays`` over every
    plot, season, AND source for each household.

    Parameters
    ----------
    plot_labor : pd.DataFrame
        ``plot_labor`` item feature, grain ``(t, i, plot, source, season)``,
        with a reported ``PersonDays`` column.

    Returns
    -------
    pd.DataFrame
        One ``Total_labor_days`` column indexed by ``(t, i)``.

    Notes
    -----
    WB sums to the *plot* grain (``total_labor_days`` lives on the Plot
    dataset, one row per plot-season); this returns the *household* total.
    Group by plot in the caller (or compare against the WB Plot column
    summed to HH) to align grains.  Coverage caveat: Uganda 2018-19/2019-20
    record only hired person-days (no family roster in-repo), so those
    waves' totals are hired-only — note when comparing to WB, which derives
    family days from a separate file there.
    """
    s = _labor_days_by_source(plot_labor, source=None)
    return s.to_frame('Total_labor_days').sort_index()


def total_family_labor_days(plot_labor):
    """Family (household) person-days per HH (WB ``total_family_labor_days``).

    MECHANICAL reduction (GAP 3).  As :func:`total_labor_days` but restricted
    to ``source == 'family'`` rows.
    """
    s = _labor_days_by_source(plot_labor, source='family')
    return s.to_frame('Total_family_labor_days').sort_index()


def total_hired_labor_days(plot_labor):
    """Hired (paid) person-days per HH (WB ``total_hired_labor_days``).

    MECHANICAL reduction (GAP 3).  As :func:`total_labor_days` but restricted
    to ``source == 'hired'`` rows.
    """
    s = _labor_days_by_source(plot_labor, source='hired')
    return s.to_frame('Total_hired_labor_days').sort_index()


# Nitrogen content (kg N per kg of product) by fertilizer identity.  The WB
# .do nitrogen blocks (e.g. ETH_ESS1.do:833-836) weight each fertilizer's
# physical kg by its N share.  Our ``plot_inputs.input`` records nutrient
# CLASS (the finest identity the UNPS questionnaire captures — it asks
# "nitrate / phosphate / potash / mixed", not urea/DAP/NPK product names),
# so we map class→N-share rather than product→N-share:
#   - 'Nitrate Fertilizer'   : straight-N product (CAN/urea-class) ~ 0.46 N
#   - 'Phosphate Fertilizer' : P-class (e.g. TSP/SSP), no nitrogen
#   - 'Potash Fertilizer'    : K-class (MOP/SOP), no nitrogen
#   - 'Mixed Fertilizer'     : blended NPK; nominal N share ~0.20 (a
#                              compound-fertilizer mid-point — NPK 17-17-17
#                              through DAP-rich blends)
#   - 'Inorganic Fertilizer' : unrefined inorganic (class not recorded);
#                              nominal ~0.20, same as mixed
# Organic fertilizer carries no inorganic-N credit in the WB construct.
# These are documented nominal shares for the class vocabulary, NOT the WB
# product-level table — see the divergence note in :func:`nitrogen_kg`.
#
# Where a country DOES record the fertilizer *product* (Tanzania / Ethiopia /
# Malawi ``plot_inputs.input`` ∈ {Urea, CAN, DAP, NPK, SA, TSP, …}) we key off
# the standard product N-shares the WB .do nitrogen blocks use, so the
# transform reproduces their figure directly on those countries:
#   Urea 46% N; CAN (calcium ammonium nitrate) 26%; SA / sulphate of ammonia
#   21%; DAP (di-ammonium phosphate) 18%; NPK 17-17-17 ≈ 17%; TSP/SSP/MOP
#   carry no nitrogen.
_NITROGEN_CONTENT = {
    # nutrient-class vocabulary (Uganda)
    'nitrate fertilizer': 0.46,
    'phosphate fertilizer': 0.0,
    'potash fertilizer': 0.0,
    'mixed fertilizer': 0.20,
    'inorganic fertilizer': 0.20,
    # product vocabulary (Tanzania / Ethiopia / Malawi / Nigeria)
    'urea': 0.46,
    'can': 0.26,
    'sa': 0.21,
    'dap': 0.18,
    'npk': 0.17,
    'tsp': 0.0,
    'ssp': 0.0,
    'mop': 0.0,
    'other fertilizer': 0.0,
}


def nitrogen_kg(plot_inputs, *, nitrogen_content=None, volume_as_mass=True):
    """Kilograms of nitrogen applied per plot (WB ``nitrogen_kg``).

    MECHANICAL reduction (GAP 2).  For each fertilizer input row, convert
    the reported ``Quantity`` (native unit ``u``) to kilograms, multiply by
    the fertilizer's nitrogen share, and sum per plot.

    Parameters
    ----------
    plot_inputs : pd.DataFrame
        ``plot_inputs`` item feature, grain ``(t, i, plot, input, j)``, with
        a reported ``Quantity`` column and a ``u`` column (Uganda stores the
        input unit as a *column* ``u``, not an index level).
    nitrogen_content : dict[str, float], optional
        Override the class→N-share map (lowercased input label → kg N per kg
        product).  Defaults to :data:`_NITROGEN_CONTENT`.
    volume_as_mass : bool, default True
        Forwarded to the unit→kg conversion.

    Returns
    -------
    pd.DataFrame
        One ``Nitrogen_kg`` column indexed by ``(t, i, plot)``.  Plots with
        fertilizer input but no convertible-unit row sum to 0; plots with no
        fertilizer at all are absent.

    Notes
    -----
    Divergence from WB: the WB code keys N-share off the *product* a survey
    records (urea 0.46, DAP 0.18, NPK 0.17, …).  The UNPS questionnaire (and
    therefore our ``plot_inputs.input``) records only nutrient *class*, so we
    apply nominal class shares (:data:`_NITROGEN_CONTENT`).  ``Nitrogen_kg``
    will diverge from the WB figure on plots whose product mix differs from
    the class nominal, and shares the unit-conversion coverage caveat of
    :func:`harvest_kg` (only metric-named ``u`` rows convert).  Pass an
    explicit ``nitrogen_content`` to reproduce a particular country's table.
    """
    content = _NITROGEN_CONTENT if nitrogen_content is None else nitrogen_content
    df = plot_inputs.copy()
    if 'Quantity' not in df.columns:
        raise ValueError("plot_inputs must have a 'Quantity' column")

    # ``input`` is an index level in the canonical grain.
    if 'input' in (df.index.names or []):
        inp = df.index.get_level_values('input').astype(str).str.lower()
    elif 'input' in df.columns:
        inp = df['input'].astype(str).str.lower()
    else:
        raise ValueError("plot_inputs must have an 'input' level or column")
    n_share = pd.Series(inp, index=df.index).map(content).astype(float)

    # Resolve a kg quantity.  Uganda keeps the unit in a *column* ``u``; the
    # shared factor machinery wants ``u`` in the index, so temporarily
    # promote it when needed.
    if 'u' in (df.index.names or []):
        kg_per_unit = _kg_factor_series(df, volume_as_mass=volume_as_mass)
    elif 'u' in df.columns:
        tmp = df.set_index('u', append=True)
        kg_per_unit = _kg_factor_series(tmp, volume_as_mass=volume_as_mass)
        kg_per_unit.index = df.index
    else:
        raise ValueError("plot_inputs must carry a 'u' unit level or column")

    qty = pd.to_numeric(df['Quantity'], errors='coerce')
    n_kg = qty * kg_per_unit * n_share
    # Keep only rows with a defined N share (fertilizer rows); seed /
    # pesticide rows map to NaN and drop.
    n_kg = n_kg[n_share.notna()]

    out = pd.DataFrame({'Nitrogen_kg': n_kg}).dropna()
    plot_level = _resolve_plot_level(out.index.names)
    group_by = [n for n in ['t', 'i', plot_level]
                if n is not None and n in out.index.names]
    res = out.groupby(group_by).sum()
    if plot_level == 'plot_id':
        res.index = res.index.rename({'plot_id': 'plot'})
    return res


def seed_kg(plot_inputs, *, seed_label='Seed', volume_as_mass=True):
    """Kilograms of seed applied per plot (WB ``seed_kg``).

    MECHANICAL reduction (GAP 2).  Filters ``plot_inputs`` to seed rows,
    converts the reported ``Quantity`` (native unit ``u``) to kilograms, and
    sums per plot.

    Parameters
    ----------
    plot_inputs : pd.DataFrame
        ``plot_inputs`` item feature (see :func:`nitrogen_kg`).  Seed rows
        are identified by ``input == seed_label``.
    seed_label : str, default 'Seed'
        The ``input`` value marking seed rows (Uganda's ``harmonize_input``
        Preferred Label).
    volume_as_mass : bool, default True
        Forwarded to the unit→kg conversion.

    Returns
    -------
    pd.DataFrame
        One ``Seed_kg`` column indexed by ``(t, i, plot)``.

    Notes
    -----
    Coverage: Uganda 2009-10/2010-11 seed rows record no quantity (only
    purchased-y/n + seed type), so those waves contribute no ``Seed_kg`` —
    matching the WB ``seed_kg`` which is also NaN there.  Otherwise shares
    the metric-unit conversion caveat of :func:`harvest_kg`.
    """
    df = plot_inputs
    if 'Quantity' not in df.columns:
        raise ValueError("plot_inputs must have a 'Quantity' column")
    if 'input' in (df.index.names or []):
        mask = (df.index.get_level_values('input').astype(str) == seed_label)
        seed = df[mask]
    elif 'input' in df.columns:
        seed = df[df['input'].astype(str) == seed_label]
    else:
        raise ValueError("plot_inputs must have an 'input' level or column")

    seed = seed.copy()
    if 'u' in (seed.index.names or []):
        kg_per_unit = _kg_factor_series(seed, volume_as_mass=volume_as_mass)
    elif 'u' in seed.columns:
        tmp = seed.set_index('u', append=True)
        kg_per_unit = _kg_factor_series(tmp, volume_as_mass=volume_as_mass)
        kg_per_unit.index = seed.index
    else:
        raise ValueError("plot_inputs must carry a 'u' unit level or column")

    qty = pd.to_numeric(seed['Quantity'], errors='coerce')
    kg = qty * kg_per_unit
    out = pd.DataFrame({'Seed_kg': kg}).dropna()
    plot_level = _resolve_plot_level(out.index.names)
    group_by = [n for n in ['t', 'i', plot_level]
                if n is not None and n in out.index.names]
    res = out.groupby(group_by).sum()
    if plot_level == 'plot_id':
        res.index = res.index.rename({'plot_id': 'plot'})
    return res


# Tropical Livestock Unit (TLU) factors — head → cattle-equivalent (1 TLU =
# one 250 kg adult bovine), the FAO/ILCA convention widely used in LSMS-ISA
# work (e.g. Jahnke 1982; the World Bank "Livestock data innovation" tables).
# Keyed by our ``livestock.animal`` Preferred Labels (harmonize_species).
_TLU_FACTORS = {
    'cattle': 0.70,
    'donkeys': 0.50,
    'horses': 0.80,
    'pigs': 0.20,
    'sheep': 0.10,
    'goats': 0.10,
    'chicken': 0.01,
    'other poultry': 0.01,
    'rabbits': 0.01,
    'bees': 0.0,
}


def tlu(livestock, *, tlu_factors=None, head_col='HeadCount'):
    """Tropical Livestock Units owned per household.

    MECHANICAL reduction (GAP 4).  Σ(HeadCount × species-TLU factor) over the
    ``livestock`` roster.  TLU normalises a mixed herd to 250-kg-bovine
    equivalents (:data:`_TLU_FACTORS`).

    Parameters
    ----------
    livestock : pd.DataFrame
        ``livestock`` item feature, grain ``(t, i, animal)``, with a head-
        count column.
    tlu_factors : dict[str, float], optional
        Override the species→TLU map (lowercased species label → factor).
        Defaults to :data:`_TLU_FACTORS`.
    head_col : str, default 'HeadCount'
        Head-count column to weight.

    Returns
    -------
    pd.DataFrame
        One ``TLU`` column indexed by ``(t, i)``.

    Notes
    -----
    The WB panel ships NO TLU column (its ``livestock`` is a bare
    engaged-y/n binary — see :func:`livestock_engaged`), so this transform
    is sanity-checked on magnitudes, not against a WB column.  A typical
    smallholder herd lands at roughly 1-5 TLU; values are bounded below by 0.
    Species absent from the factor map contribute nothing (and emit no
    error — callers wanting strict coverage check the species set first).
    """
    factors = _TLU_FACTORS if tlu_factors is None else tlu_factors
    df = livestock
    if head_col not in df.columns:
        raise ValueError(f"livestock must have a {head_col!r} column")
    if 'animal' in (df.index.names or []):
        animal = df.index.get_level_values('animal').astype(str).str.lower()
    elif 'animal' in df.columns:
        animal = df['animal'].astype(str).str.lower()
    else:
        raise ValueError("livestock must have an 'animal' level or column")

    head = pd.to_numeric(df[head_col], errors='coerce')
    weight = pd.Series(animal, index=df.index).map(factors).astype(float)
    contrib = head * weight
    out = pd.DataFrame({'TLU': contrib}).dropna()
    group_by = [n for n in ['t', 'i'] if n in out.index.names]
    return out.groupby(group_by).sum()


def livestock_engaged(livestock):
    """Household engaged-in-livestock indicator (WB ``livestock`` binary).

    MECHANICAL reduction (GAP 4).  A household is engaged iff it has any row
    in the ``livestock`` roster: ``groupby(['t','i']).any()``.

    Parameters
    ----------
    livestock : pd.DataFrame
        ``livestock`` item feature, grain ``(t, i, animal)``.

    Returns
    -------
    pd.DataFrame
        One boolean ``Livestock`` column indexed by ``(t, i)`` — True for
        every HH present in the roster.

    Notes
    -----
    The WB ``livestock`` column is ``'Yes'``/``'No'``; map this boolean to
    those strings (``.map({True: 'Yes', False: 'No'})``) to compare.  Because
    our roster only contains rows for households that own *something*, every
    HH in the output is True — the ``'No'`` households are those *absent*
    from the roster (present elsewhere in the survey).  An analyst recovers
    the full Yes/No vector by reindexing against the household universe
    (e.g. ``sample()``) and filling absent HH with False.
    """
    df = livestock
    group_by = [n for n in ['t', 'i'] if n in (df.index.names or [])]
    if not group_by:
        raise ValueError("livestock must have 't' and/or 'i' index levels")
    # Any row present for the HH ⇒ engaged.
    flag = (df.assign(_one=True)
              .groupby(group_by)['_one'].any())
    return flag.to_frame('Livestock').sort_index()


def dependency_ratio(household_roster, *, working_age=(15, 64),
                     age_col='Age'):
    """Household dependency ratio (WB ``hh_dependency_ratio``).

    MECHANICAL reduction (below-the-line, GAP_RANKING Area 3).  Per
    household: ``dependents / working-age members``, where dependents are
    members younger than the working-age band's lower bound or older than its
    upper bound.

    Parameters
    ----------
    household_roster : pd.DataFrame
        ``household_roster`` item feature with an ``Age`` column, indexed by
        at least ``(t, i, pid)``.
    working_age : (int, int), default (15, 64)
        Inclusive ``[lo, hi]`` working-age band.  Members with
        ``lo <= Age <= hi`` are the denominator; everyone else is a
        dependent.  The default 15-64 matches the standard demographic
        definition the WB uses.
    age_col : str, default 'Age'
        Age column name (case-insensitive lookup falls back to lowercase).

    Returns
    -------
    pd.DataFrame
        One float ``Dependency_ratio`` column indexed by ``(t, i)``.
        Households with zero working-age members yield ``inf`` (all
        dependents, no earner) and are DROPPED — the WB column is likewise
        undefined there; an analyst wanting them kept reindexes afterward.

    Notes
    -----
    Reproduces ``hh_dependency_ratio`` (e.g. ETH_ESS1.do:957-961) up to the
    age-bucketing precision of ``Age``.  ``age_handler`` may return a
    fractional ``Age`` when DOB is available, so the band test uses ``<`` /
    ``>`` on continuous ages, not integer bins.
    """
    df = household_roster
    col = age_col if age_col in df.columns else age_col.lower()
    if col not in df.columns:
        raise ValueError(f"household_roster must have an {age_col!r} column")
    lo, hi = working_age

    age = pd.to_numeric(df[col], errors='coerce')
    work = df.assign(
        _dep=((age < lo) | (age > hi)).astype('float'),
        _work=((age >= lo) & (age <= hi)).astype('float'),
    )
    # Members with NaN age contribute to neither count.
    work.loc[age.isna(), ['_dep', '_work']] = np.nan

    group_by = [n for n in ['t', 'i'] if n in (df.index.names or [])]
    if not group_by:
        raise ValueError("household_roster must have 't' and/or 'i' levels")
    g = work.groupby(group_by)[['_dep', '_work']].sum()
    with np.errstate(divide='ignore', invalid='ignore'):
        ratio = g['_dep'] / g['_work']
    ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
    return ratio.to_frame('Dependency_ratio').sort_index()


def farm_size(plot_features, *, area_col='Area'):
    """Total cultivated/owned plot area per household (WB ``farm_size``).

    MECHANICAL reduction (GAP 6 below-the-line).  Σ plot ``Area`` per
    household over ``plot_features``.

    Parameters
    ----------
    plot_features : pd.DataFrame
        ``plot_features`` item feature, grain ``(t, i, plot_id, ...)``, with
        a plot area column.
    area_col : str, default 'Area'
        Area column to sum.

    Returns
    -------
    pd.DataFrame
        One ``Farm_size`` column indexed by ``(t, i)`` — total area in the
        ``plot_features.AreaUnit`` (Uganda: hectare-equivalent).

    Notes
    -----
    WB stores ``farm_size`` repeated per plot on the Plot dataset; this
    returns the HH total once.  Reindex/broadcast to plot grain to compare
    directly.  Plots with NaN/zero area drop from the sum.
    """
    df = plot_features
    if area_col not in df.columns:
        raise ValueError(f"plot_features must have a {area_col!r} column")
    area = pd.to_numeric(df[area_col], errors='coerce')
    out = pd.DataFrame({'_Area': area})
    out = out[out['_Area'] > 0]
    group_by = [n for n in ['t', 'i'] if n in (df.index.names or [])]
    if not group_by:
        raise ValueError("plot_features must have 't' and/or 'i' levels")
    return (out.groupby(group_by)['_Area'].sum()
               .to_frame('Farm_size').sort_index())


def nb_plots(plot_features):
    """Number of plots per household (WB ``nb_plots``).

    MECHANICAL reduction (GAP 6 below-the-line).  Counts ``plot_features``
    rows per household.

    Parameters
    ----------
    plot_features : pd.DataFrame
        ``plot_features`` item feature, grain ``(t, i, plot_id, ...)``.

    Returns
    -------
    pd.DataFrame
        One integer ``Nb_plots`` column indexed by ``(t, i)``.

    Notes
    -----
    The WB Uganda Household dataset leaves ``nb_plots`` empty (the count
    lives implicitly in the Plot table), so for Uganda this is a
    sanity-check on plot multiplicity rather than a column match; other WB
    countries populate ``nb_plots`` directly.
    """
    df = plot_features
    group_by = [n for n in ['t', 'i'] if n in (df.index.names or [])]
    if not group_by:
        raise ValueError("plot_features must have 't' and/or 'i' levels")
    count = df.groupby(group_by).size()
    return count.to_frame('Nb_plots').sort_index()


# ===========================================================================
# Phase 2 — METHODOLOGY transforms
#
# These encode a *specific analytic method* (a price-imputation ladder, a PCA
# factor score, a WHO reference z-score) rather than a pure mechanical
# reduction.  Each documents the method choice in its docstring; they are
# meant to be human-reviewed.  Like the Phase-1 transforms they are
# ANALYST-CALLABLE functions that consume our item features and return the
# aggregate — they are NOT registered in ``_FOOD_DERIVED`` / ``_ROSTER_DERIVED``
# and are NOT auto-surfaced as Country features.
# ===========================================================================


def median_price_valuation(item_df, geo_levels, *,
                           value_col='Value_sold',
                           qty_col='Quantity_sold',
                           kg_qty=None,
                           quantity_col='Quantity',
                           item_keys=('j',),
                           threshold=10,
                           volume_as_mass=True,
                           price_col='_unit_price',
                           out_col='Value'):
    """Value item rows at a geography-ladder *median unit price* (WB valuation).

    METHODOLOGY transform (GAP 7).  Reproduces the World Bank LSMS-ISA
    ``valuation_median_crops`` ladder (Reproduction_v2 ``programs.do:4-103``):
    impute a unit price for every item row from the prices *actually observed*
    in sale transactions, taking the median within the smallest geographic
    cell that clears a minimum-observation threshold, then multiplying that
    imputed price by each row's physical quantity to get a value.

    Method (the choice being encoded — documented for review)
    ---------------------------------------------------------
    1.  *Observed unit price* ``p = value_col / kg_qty`` per item row, where
        ``kg_qty`` is the sold quantity in kilograms.  Rows with a zero or
        missing price are excluded from the median pool (matching the WB
        ``replace crop_price_temp = . if ==0``), but still *receive* an
        imputed price in step 3.
    2.  *Median ladder*.  For each ``(geo_cell, *item_keys)`` group, count the
        non-missing observed prices ``n``.  Walking ``geo_levels`` from finest
        to coarsest, then a final national level, the imputed price for a row
        is the **median observed price of the finest cell whose count ≥
        threshold**.  This is exactly the WB cascade: EA → admin_4 → admin_3 →
        admin_2 → admin_1 → national, where the cell's median is *adopted only
        if it has ≥10 priced observations* and no finer cell already qualified.
        The national median is the unconditional fallback (the WB
        ``replace ... if ten_obs_n==0``).
    3.  *Valuation*.  ``out_col = imputed_price × quantity_col`` for every row
        (the WB ``harvest_value = crop_price * harvest_kg``).

    Why a ladder of medians rather than each household's own price?  The WB
    construct deliberately values *all* output — including the home-consumed
    share that was never sold — at a common local market price, so that two
    households facing the same market are valued identically regardless of how
    much each happened to sell.  The ≥10-obs threshold trades spatial
    resolution for a stable median: a cell speaks for itself only when enough
    sales back it; otherwise it borrows its parent's price.

    Parameters
    ----------
    item_df : pd.DataFrame
        An item feature carrying, per row, a sale value, a sold quantity, and
        a physical quantity to value — e.g. ``crop_production`` (harvest
        valuation, drives WB ``harvest_value_LCU``), the seed rows of
        ``plot_inputs`` (``seed_value``), fertilizer rows
        (``inorganic_fertilizer_value``), or ``plot_labor`` hired rows
        (``hired_labor_value``, where the "price" is a wage and the "quantity"
        is days).  Must carry the geography keys named in ``geo_levels`` as
        index levels or columns.
    geo_levels : sequence of str
        Geography keys ordered **finest → coarsest** (e.g.
        ``['v', 'District', 'Region']`` for Uganda — ``v`` is the EA/cluster
        analogue of the WB ``ea_id``).  Each must resolve to an index level or
        a column of ``item_df``.  A final unconditional national level is
        always appended internally, so the caller need not list it.  The
        median within a cell is taken over ``(geo_level, *item_keys)``.
    value_col : str, default 'Value_sold'
        Sale-value column (numerator of the observed unit price).
    qty_col : str, default 'Quantity_sold'
        Sold-quantity column, in the row's native unit ``u``.  Used only when
        ``kg_qty`` is not supplied: the sold quantity is converted to kg via
        the shared unit machinery so the unit price is per-kg and comparable
        across rows reported in different containers.
    kg_qty : pd.Series, optional
        Pre-computed sold quantity in kilograms, aligned to ``item_df.index``.
        Supply this when the caller has already converted (or when the price
        basis is not per-kg — e.g. wages per labor-day, where you pass the
        days Series and leave ``quantity_col`` as the days column).  When
        omitted, ``qty_col`` is converted to kg via :func:`_kg_factor_series`.
    quantity_col : str, default 'Quantity'
        The physical quantity each row is valued at in step 3 (harvest kg,
        seed kg, fertilizer kg, labor days).  Must already be in the SAME unit
        as the price denominator (kg for the default per-kg price); convert
        upstream (e.g. with :func:`harvest_kg`'s machinery) if needed.  Pass a
        Series via ``kg_qty``-style alignment is *not* supported here — give a
        column name.
    item_keys : sequence of str, default ('j',)
        Item identity keys the median is stratified by (the WB ``cropvar``;
        for seeds the WB adds ``improved`` — pass ``('j', 'improved')``).
        Resolve to index levels or columns.
    threshold : int, default 10
        Minimum count of priced observations for a cell's median to be
        adopted (the WB ``ten_obs`` ≥ 10).
    volume_as_mass : bool, default True
        Forwarded to the kg conversion of ``qty_col`` when ``kg_qty`` is None.
    price_col : str, default '_unit_price'
        Name for the imputed-price column added to the returned frame.
    out_col : str, default 'Value'
        Name for the valued column (``price × quantity``).

    Returns
    -------
    pd.DataFrame
        ``item_df``'s index with two added columns: ``price_col`` (the imputed
        ladder median unit price) and ``out_col`` (``price × quantity_col``).
        One row per input row — the caller groups to whatever HH/plot grain
        the target WB column lives at (e.g. ``.groupby(['t','i','plot']).sum()``
        for ``harvest_value_LCU``).  Rows whose ``quantity_col`` is missing get
        a missing ``out_col``.

    Notes
    -----
    Divergence from WB: their ladder is keyed on survey ``admin_1..admin_4``
    codes; we accept whatever geography the caller supplies from our
    ``cluster_features`` / ``sample`` (``v``, ``District``, ``Region``), which
    are the same nesting at possibly different label granularity.  The imputed
    *price* therefore matches the WB to the extent the geography nesting and
    the sold-price pool coincide; the resulting *value* additionally inherits
    any unit-conversion coverage caveat of the kg quantities it multiplies
    (see :func:`harvest_kg`).  ``*_value`` columns are LCU; deflation to USD
    is a separate step the WB does downstream (CPI × Atlas FX) and is out of
    scope here.
    """
    df = item_df.copy()

    def _series(name):
        """Resolve ``name`` to a Series aligned to df.index (level or column)."""
        if name in (df.index.names or []):
            return pd.Series(df.index.get_level_values(name), index=df.index)
        if name in df.columns:
            return df[name]
        raise ValueError(f"{name!r} is neither an index level nor a column")

    if value_col not in df.columns:
        raise ValueError(f"item_df must have a {value_col!r} column")
    if quantity_col not in df.columns:
        raise ValueError(f"item_df must have a {quantity_col!r} column")

    value = pd.to_numeric(df[value_col], errors='coerce')

    # Observed sold quantity in kg (the price denominator).
    if kg_qty is not None:
        sold_kg = pd.to_numeric(kg_qty, errors='coerce').reindex(df.index)
    else:
        if qty_col not in df.columns:
            raise ValueError(f"item_df must have a {qty_col!r} column "
                             "(or pass kg_qty)")
        sold_native = pd.to_numeric(df[qty_col], errors='coerce')
        sold_kg = sold_native * _kg_factor_series(
            df, volume_as_mass=volume_as_mass)

    # Step 1: observed unit price; zero/inf priced rows excluded from the pool.
    with np.errstate(divide='ignore', invalid='ignore'):
        price = value / sold_kg
    price = price.replace([np.inf, -np.inf], np.nan)
    price = price.where(price > 0)

    item_key_series = [_series(k).astype(str) for k in item_keys]

    # Step 2: median ladder.  Start with everyone unassigned; for each geo
    # level finest→coarsest (then national), fill any still-unassigned row
    # whose cell clears the threshold with that cell's median observed price.
    imputed = pd.Series(np.nan, index=df.index, dtype='float64')
    ladder = list(geo_levels) + [None]  # None ⇒ national (no geo grouping)
    for level in ladder:
        keys = ([] if level is None else [_series(level).astype(str)]) \
            + item_key_series
        grouped = price.groupby(keys)
        cell_median = grouped.transform('median')
        cell_count = price.notna().groupby(keys).transform('sum')
        qualifies = cell_count >= threshold
        take = imputed.isna() & qualifies & cell_median.notna()
        imputed = imputed.where(~take, cell_median)
    # National median is the unconditional fallback (WB ten_obs_n==0 branch):
    # any row still unassigned after the threshold cascade gets the national
    # median regardless of count, mirroring the final WB ``replace``.
    nat_median = price.groupby(item_key_series).transform('median')
    imputed = imputed.where(imputed.notna(), nat_median)

    # Step 3: value every row at its imputed price.
    quantity = pd.to_numeric(df[quantity_col], errors='coerce')
    out = pd.DataFrame(index=df.index)
    out[price_col] = imputed
    out[out_col] = imputed * quantity
    return out


def asset_index(assets, split='hh', *, value_col=None, ag_items=None,
                n_components=1):
    """First-principal-component asset index (WB ``ag/hh_asset_index``).

    METHODOLOGY transform (GAP 8).  Reproduces the World Bank
    ``factor d_*, pcf`` + ``predict`` construct (e.g. ETH_ESS1.do:1077-1102):
    reshape the long item-level ``assets`` ownership into a household ×
    asset-type ownership matrix, extract its **first principal component**,
    and return each household's score on that component as a single wealth
    index.

    Method (the choice being encoded — documented for review)
    ---------------------------------------------------------
    Stata's ``factor ..., pcf`` is *principal-component factoring*: the factor
    loadings are the eigenvectors of the **correlation** matrix, and
    ``predict`` returns the standardised first-component score.  We reproduce
    that exactly with scikit-learn:

      1. Build a household × asset-type binary matrix ``D`` of ownership
         dummies (1 if the HH owns ≥1 of that asset type, else 0).  An asset
         type the HH never reports is a structural 0, matching the WB
         ``reshape wide`` + implicit-zero behaviour.
      2. Drop asset-type columns with no variation (all-0 or all-1) — a
         constant column has zero correlation contribution and Stata's
         ``factor`` silently ignores it.
      3. **Standardise** each column to mean 0 / unit variance, then run PCA;
         operating on standardised columns makes PCA factor the correlation
         matrix, which is what ``pcf`` does.
      4. The index is the projection onto the first principal component
         (``predict`` after ``factor``).  Its sign is arbitrary (PCA sign is
         not identified); we orient it so the component correlates
         non-negatively with the *number of assets owned* — a higher index
         means more assets, the conventional reading.

    The score is per ``(t, i)`` and computed **within each wave ``t``
    separately** (the WB factors each survey round on its own data), so scores
    are not comparable in level across waves — only ranks within a wave are.

    Parameters
    ----------
    assets : pd.DataFrame
        The ``assets`` item feature at grain ``(t, i, j)`` where ``j`` is the
        asset type.  Ownership is inferred from whichever signal is present:
        a count/``Quantity`` column (``> 0`` ⇒ owned), else ``value_col`` /
        ``Value`` (``> 0`` or non-missing ⇒ owned), else mere row presence.
    split : {'hh', 'ag'}, default 'hh'
        Which index the WB builds.

        - ``'ag'``  → ``ag_asset_index``: restrict to *agricultural* asset
          types (``ag_items``); the WB keeps only farm-equipment item codes.
        - ``'hh'``  → ``hh_asset_index``: the *household* (non-ag) asset
          types — every asset type NOT in ``ag_items``.

        When ``ag_items`` is None the split is a no-op (the index is built
        over all asset types) and a single index is returned; pass the
        country's ag asset-type labels to actually split.
    value_col : str, optional
        Column to read ownership from when no count column is present.
        Defaults to trying ``'Quantity'`` then ``'Value'``.
    ag_items : collection of str, optional
        Asset-type labels (``j`` values) that count as agricultural.  Used to
        partition for ``split``.  Country-specific (the WB hard-codes item
        codes per survey); supply the country's mapping.
    n_components : int, default 1
        Number of leading components to return (``1`` = the WB single index).
        ``>1`` returns ``Asset_index_1..n`` for diagnostics.

    Returns
    -------
    pd.DataFrame
        One ``Asset_index`` column (or ``Asset_index_1..n``) indexed by
        ``(t, i)``.  Standardised within wave (mean ≈ 0, sd ≈ 1), matching the
        WB score whose std is ≈ 1.

    Notes
    -----
    Divergence from WB: scikit-learn's eigen-decomposition and Stata's
    ``pcf`` agree on the component *direction* up to sign and numerical
    precision, so the index correlates ~1 with the WB column **in rank**, but
    absolute values differ by the arbitrary sign (we fix it via the
    asset-count orientation above) and by tiny scaling/standardisation
    conventions.  Compare via Spearman rank, not equality.  Households absent
    from the ``assets`` roster (own nothing of the relevant class) are absent
    from the result; reindex against the HH universe and fill with the wave
    minimum if you need them.
    """
    from sklearn.decomposition import PCA  # local import; heavy dep

    df = assets
    names = df.index.names or []
    if 'j' not in names:
        raise ValueError("assets must have a 'j' (asset type) index level")
    if not ({'t', 'i'} & set(names)):
        raise ValueError("assets must have 't' and/or 'i' index levels")

    # Ownership signal, row-wise.
    if 'Quantity' in df.columns:
        owned = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0) > 0
    else:
        col = value_col
        if col is None:
            col = 'Value' if 'Value' in df.columns else None
        if col is not None and col in df.columns:
            v = pd.to_numeric(df[col], errors='coerce')
            owned = v.fillna(0) > 0
        else:
            # No magnitude column: mere presence ⇒ owned.
            owned = pd.Series(True, index=df.index)
    owned = owned.astype(int)

    # Partition asset types for the ag/hh split.
    j = df.index.get_level_values('j').astype(str)
    if ag_items is not None:
        ag_set = {str(x) for x in ag_items}
        is_ag = pd.Series(j.isin(ag_set), index=df.index)
        if split == 'ag':
            owned = owned[is_ag.values]
        elif split == 'hh':
            owned = owned[(~is_ag).values]
        else:
            raise ValueError("split must be 'ag' or 'hh'")

    work = owned.to_frame('_own')

    # Reshape to (t,i) × j ownership matrix, one wave at a time.
    hh_levels = [n for n in ['t', 'i'] if n in names]
    scores = []
    for t_val, chunk in work.groupby(level='t'):
        wide = (chunk['_own']
                .groupby(level=[n for n in hh_levels + ['j']])
                .max()
                .unstack('j')
                .fillna(0))
        # Drop no-variation columns (Stata factor ignores constants).
        varying = wide.loc[:, wide.nunique() > 1]
        if varying.shape[1] < n_components or varying.shape[0] <= n_components:
            continue
        # Standardise columns → PCA factors the correlation matrix (pcf).
        mu = varying.mean()
        sd = varying.std(ddof=0).replace(0, np.nan)
        Z = ((varying - mu) / sd).fillna(0)
        pca = PCA(n_components=n_components)
        comp = pca.fit_transform(Z.values)
        # Orient component 1 so more-assets ⇒ higher index.
        asset_count = varying.sum(axis=1).values
        if n_components >= 1 and np.corrcoef(comp[:, 0], asset_count)[0, 1] < 0:
            comp = -comp
        cols = (['Asset_index'] if n_components == 1
                else [f'Asset_index_{k+1}' for k in range(n_components)])
        scores.append(pd.DataFrame(comp, index=varying.index, columns=cols))

    if not scores:
        return pd.DataFrame(columns=(['Asset_index'] if n_components == 1
                                     else [f'Asset_index_{k+1}'
                                           for k in range(n_components)]))
    return concat(scores).sort_index()


# WHO Child Growth Standards (2006) flagging bounds — the implausible-value
# cutoffs ``zscore06`` applies before returning a z-score (WHO igrowup macro,
# also Stata ``zanthro``).  A z outside these is set missing (a measurement
# error), not winsorised.
_WHO_FLAG_BOUNDS = {
    'haz06': (-6.0, 6.0),
    'waz06': (-6.0, 5.0),
    'whz06': (-5.0, 5.0),
    'bmiz06': (-5.0, 5.0),
}


def anthropometry_zscores(anthropometry, who_reference, *,
                          weight_col='Weight', height_col='Height',
                          age_col='Age_months', sex_col='Sex',
                          male_value='M', female_value='F',
                          flag=True):
    """WHO-2006 child anthropometric z-scores (WB ``haz06/waz06/whz06/bmiz06``).

    METHODOLOGY transform (GAP 5).  Reproduces the Stata ``zscore06`` /
    ``zanthro`` call the WB applies (e.g. ETH_ESS1.do:1217-1235) using the WHO
    2006 Child Growth Standards: from a child's weight, height/length, age and
    sex it computes height-for-age (HAZ), weight-for-age (WAZ),
    weight-for-height (WHZ) and BMI-for-age (BMIZ) z-scores, plus the
    derived ``stunting`` (HAZ < −2) and ``wasting`` (WHZ < −2) indicators.

    Method (the choice being encoded — documented for review)
    ---------------------------------------------------------
    The WHO standards distribute each measure as a **Box-Cox / LMS** family
    indexed by sex and age (or, for weight-for-height, by sex and height).
    Given the reference parameters ``L`` (Box-Cox power), ``M`` (median) and
    ``S`` (coefficient of variation) for a child's ``(sex, age)`` cell, the
    z-score of a measurement ``y`` is

        z = ((y / M) ** L − 1) / (L · S)            (L ≠ 0)
        z = ln(y / M) / S                            (L = 0)

    For the WHO 2006 standards, when ``|z| > 3`` the score is recomputed off
    the SD at ±3 to tame the heavy Box-Cox tail (the WHO "restricted" formula
    that ``zscore06`` implements):

        z = 3 + (y − C₃) / (C₃ − C₂)   for z > 3,   where Cₖ = M·(1+L·S·k)**(1/L)
        z = −3 + (y − C₋₃) / (C₋₂ − C₋₃)   for z < −3.

    This is the WHO-recommended construction and is exactly what the Stata
    ``zscore06`` macro does; encoding *that* method (rather than a plain
    normal-approximation z) is the choice being documented here.  With
    ``flag=True`` (default) z-scores outside the WHO biological-plausibility
    bounds (:data:`_WHO_FLAG_BOUNDS`) are set missing, matching the WB's
    flagged-out implausible measurements.

    Reference data (NOT vendored)
    -----------------------------
    The WHO LMS reference tables are *not* shipped with the library (they are
    a multi-file WHO dataset under WHO's own terms).  The caller supplies them
    via ``who_reference`` — see that parameter.  This keeps the transform
    additive and honest: it encodes the WHO-2006 *method* and leaves the
    reference lookup table as an explicit, swappable input, exactly as the
    Stata macro relies on the externally-installed WHO ado reference.

    Parameters
    ----------
    anthropometry : pd.DataFrame
        The ``anthropometry`` item feature at grain ``(t, i, v, pid)`` with
        ``Weight`` (kg) and ``Height`` (cm) columns.  It must ALSO carry the
        child's age in months and sex; where the feature itself lacks them
        (e.g. Tanzania ``anthropometry`` has no ``Age_months``), the analyst
        joins ``Age_months`` and ``Sex`` from ``household_roster`` first — the
        WB code likewise merges the roster for ``age``/``female`` before the
        ``zscore06`` call.
    who_reference : dict[str, pd.DataFrame] or callable
        The WHO 2006 LMS parameter tables.  Either:

        - a dict mapping indicator → DataFrame with columns
          ``['sex', 'x', 'L', 'M', 'S']`` where ``x`` is age-in-months for
          ``'haz'``/``'waz'``/``'bmiz'`` and height-in-cm for ``'whz'``, and
          ``sex`` is ``1``/``2`` (male/female, WHO convention) — keys
          ``'haz'``, ``'waz'``, ``'whz'``, ``'bmiz'`` (indicators with no
          table provided are skipped); or
        - a callable ``who_reference(indicator, sex, x) -> (L, M, S)`` for
          callers who back the lookup with ``pygrowup`` or the WHO igrowup
          tables directly.

        The igrowup tables are obtainable from
        https://www.who.int/tools/child-growth-standards/software (the same
        reference the Stata ``zscore06`` macro installs).
    weight_col, height_col : str
        Measurement columns (kg, cm).
    age_col : str, default 'Age_months'
        Child age in months.
    sex_col : str, default 'Sex'
        Child sex column, values ``male_value`` / ``female_value``.
    male_value, female_value : default 'M' / 'F'
        Sex labels in ``sex_col`` mapped to WHO ``1`` / ``2``.
    flag : bool, default True
        Apply the WHO biological-plausibility flagging (set implausible
        z-scores missing).

    Returns
    -------
    pd.DataFrame
        ``anthropometry``'s index plus columns ``haz06``, ``waz06``,
        ``whz06``, ``bmiz06`` (whichever the reference supplies) and the
        derived booleans ``stunting`` (``haz06 < -2``) and ``wasting``
        (``whz06 < -2``).  Rows outside the WHO age domain (typically
        0-60 months) get missing z-scores.

    Notes
    -----
    Divergence from WB: identical *method* to ``zscore06``; numeric agreement
    is to the precision of the supplied reference table and the linear
    interpolation between its tabulated ``x`` points (WHO tables are at
    integer months / 0.1 cm — we interpolate linearly in ``x``, as the WHO
    macro does).  Because the reference is an explicit input, results match
    the WB exactly when fed the same WHO igrowup tables.  ``bmiz06`` uses
    ``BMI = weight / (height/100) ** 2``.
    """
    df = anthropometry
    for c in (weight_col, height_col, age_col, sex_col):
        if c not in df.columns:
            raise ValueError(
                f"anthropometry must carry a {c!r} column (join Age_months / "
                "Sex from household_roster when the feature lacks them)")

    weight = pd.to_numeric(df[weight_col], errors='coerce')
    height = pd.to_numeric(df[height_col], errors='coerce')
    age = pd.to_numeric(df[age_col], errors='coerce')
    sex_raw = df[sex_col].astype(str)
    sex = pd.Series(np.nan, index=df.index, dtype='float64')
    sex[sex_raw == str(male_value)] = 1.0
    sex[sex_raw == str(female_value)] = 2.0
    bmi = weight / (height / 100.0) ** 2

    # indicator → (measurement Series, x-axis Series)
    specs = {
        'haz': (height, age),
        'waz': (weight, age),
        'whz': (weight, height),
        'bmiz': (bmi, age),
    }

    def _lms(indicator, sx, xv):
        """Vectorised (L, M, S) lookup with linear interpolation in x."""
        if callable(who_reference):
            L = np.empty(len(sx)); M = np.empty(len(sx)); S = np.empty(len(sx))
            for idx in range(len(sx)):
                if np.isnan(sx[idx]) or np.isnan(xv[idx]):
                    L[idx] = M[idx] = S[idx] = np.nan
                    continue
                try:
                    l, m, s = who_reference(indicator, sx[idx], xv[idx])
                except Exception:
                    l = m = s = np.nan
                L[idx], M[idx], S[idx] = l, m, s
            return L, M, S
        table = who_reference.get(indicator)
        if table is None:
            return None
        L = np.full(len(sx), np.nan)
        M = np.full(len(sx), np.nan)
        S = np.full(len(sx), np.nan)
        for sex_code in (1.0, 2.0):
            sub = table[table['sex'].astype(float) == sex_code].sort_values('x')
            if sub.empty:
                continue
            mask = sx == sex_code
            xs = sub['x'].to_numpy(dtype=float)
            xq = xv[mask]
            # linear interpolation; out-of-domain → NaN
            inb = (xq >= xs.min()) & (xq <= xs.max())
            for param, dest in (('L', L), ('M', M), ('S', S)):
                yp = sub[param].to_numpy(dtype=float)
                vals = np.interp(xq, xs, yp)
                vals = np.where(inb, vals, np.nan)
                dest_idx = np.where(mask)[0]
                dest[dest_idx] = vals
        return L, M, S

    def _zscore(y, L, M, S):
        y = y.to_numpy(dtype=float)
        with np.errstate(divide='ignore', invalid='ignore'):
            z = np.where(L != 0,
                         ((y / M) ** L - 1.0) / (L * S),
                         np.log(y / M) / S)
            # WHO restricted formula in the tails (|z| > 3).
            def cutoff(k):
                return M * (1.0 + L * S * k) ** (1.0 / L)
            sd3pos = cutoff(3) - cutoff(2)
            sd3neg = cutoff(-2) - cutoff(-3)
            zhi = 3.0 + (y - cutoff(3)) / sd3pos
            zlo = -3.0 + (y - cutoff(-3)) / sd3neg
            z = np.where(z > 3.0, zhi, z)
            z = np.where(z < -3.0, zlo, z)
        return z

    out = pd.DataFrame(index=df.index)
    sx = sex.to_numpy(dtype=float)
    name_map = {'haz': 'haz06', 'waz': 'waz06', 'whz': 'whz06',
                'bmiz': 'bmiz06'}
    for indicator, (meas, xaxis) in specs.items():
        lms = _lms(indicator, sx, xaxis.to_numpy(dtype=float))
        if lms is None:
            continue
        L, M, S = lms
        z = _zscore(meas, L, M, S)
        col = name_map[indicator]
        if flag:
            lo, hi = _WHO_FLAG_BOUNDS[col]
            z = np.where((z >= lo) & (z <= hi), z, np.nan)
        out[col] = z

    if 'haz06' in out.columns:
        out['stunting'] = (out['haz06'] < -2).where(out['haz06'].notna())
    if 'whz06' in out.columns:
        out['wasting'] = (out['whz06'] < -2).where(out['whz06'].notna())
    return out
