#!/usr/bin/env python3
"""Build-path transforms: functions whose output is *baked into* the
harmonized L2 parquet at cache-build time.

Deliberately separated from :mod:`lsms_library.transformations` (which holds
*read-path* transforms applied dynamically on every API call and never
cached).  Making the build/read boundary structural lets the v0.8.0
content-hash cache layer version build-path transform code precisely -- see
the PR #531 discussion.  Everything here runs between raw source and the
written parquet:

  - ``food_acquired_to_canonical`` / ``_finalize_canonical_food_acquired``:
    the food_acquired reshape, called from per-country build scripts.
  - ``apply_derived`` (+ ``fill_v_with_coord_bin`` and the
    ``_DERIVED_TRANSFORMERS`` registry): the ``derived:`` block applied by
    ``Wave.grab_data`` during extraction.
  - ``reduce_to_agreed`` / ``collapse_to_cluster_grain`` / ``add_visit_level``:
    the EXPLICIT grain helpers (GH #323).  A country's ``mapping.py`` calls
    them BY NAME; core never dispatches them.

These names are re-exported from ``lsms_library.transformations`` for
backward compatibility with the country scripts that import them from there.
"""
import os
import warnings

import numpy as np
import pandas as pd

from ._build_registry import build_transform


@build_transform(tables=['food_acquired'])
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


@build_transform(tables=['food_acquired'])
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


@build_transform()
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


@build_transform()
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


# ---------------------------------------------------------------------------
# Explicit grain helpers (GH #323)
#
# POLICY.  ``SkunkWorks/grain_aggregation_policy.org`` puts the boundary at the
# ACCESS PATH: ``country.py`` / ``feature.py`` / ``local_tools.py`` NEVER reduce
# grain -- they return the maximal grain the instruments support.  Aggregation
# is always EXPLICITLY invoked, by the analyst or by the country.  The helpers
# below are the country's instrument: a wave's ``_/mapping.py`` imports one BY
# NAME and hands it the frame.  That is the country aggregating, visibly, at
# build time -- not core aggregating behind the caller's back.
#
# They are therefore NOT a declared-reducer mechanism.  There is no
# ``aggregation:`` YAML key and no dispatch table; core cannot reach these
# functions.  A country that wants a collapse writes the call.
#
# The contract every reducer here upholds: a reduction that would DESTROY an
# observed value is not performed quietly.  It raises, or (when the caller has
# explicitly accepted the loss) it NAs the conflicted cells and warns loudly.
# A helper that quietly picks a winner would only relocate the #323 bug.
#
# CACHE NOTE.  These run at build time, so their output is baked into the L2
# parquet and their CODE must be versioned by the content hash.  It is, via
# ``_build_registry.framework_imports_fingerprint``: ``Wave._input_hash`` /
# ``Country._table_cache_hash`` parse every ``_/ *.py`` build module (mapping.py
# included), resolve its ``from lsms_library... import`` targets, and fold their
# closures in -- so editing a reducer invalidates exactly the waves that call
# it.  ``add_visit_level`` additionally carries a ``@build_transform`` tag (it is
# food_acquired-scoped, like its sibling ``food_acquired_to_canonical``); the
# generic reducers deliberately do NOT, because an all-tables tag would fold
# them into EVERY table's fingerprint in every country -- invalidating the whole
# library's cache for a helper most countries never call, with no correctness
# gain over the import-closure fold above.
# ---------------------------------------------------------------------------

_GRAIN_STRICT_ENV = 'LSMS_GRAIN_STRICT'


class GrainConflict(ValueError):
    """Rows sharing an index tuple carry DIFFERENT values; collapsing them
    would destroy an observed value.  A ``ValueError`` subclass, so a caller
    that catches ``ValueError`` still catches this."""


class GrainConflictWarning(UserWarning):
    """A reducer was told to proceed through a conflict (``on_conflict='na'``)
    and blanked the conflicted cells.  Loud by construction: the values it
    dropped are real data (GH #323)."""


def _grain_strict() -> bool:
    return os.environ.get(_GRAIN_STRICT_ENV, '').strip().lower() not in ('', '0', 'false', 'no')


def _conflict_message(conflicts: pd.DataFrame, levels: list, verb: str) -> str:
    """Human-readable, actionable account of a grain conflict.

    ``conflicts`` is a boolean frame (group x column): True where a group holds
    more than one distinct value.  Names the columns, the counts and a handful
    of offending group keys -- enough to go look at the source.
    """
    per_col = {c: int(conflicts[c].sum()) for c in conflicts.columns if conflicts[c].any()}
    bad = conflicts.any(axis=1)
    examples = [tuple(k) if isinstance(k, tuple) else (k,) for k in conflicts.index[bad][:5]]
    return (
        f"grain conflict on {levels}: {int(bad.sum())} of {len(conflicts)} group(s) "
        f"carry more than one distinct value.  Conflicted groups per column: "
        f"{per_col}.  Offending groups e.g. {examples}.  {verb}  "
        f"Either the payload is genuinely finer-grained than the declared index "
        f"(it does not belong in this table), the identifier is broken / a level "
        f"is missing, or the source needs cleaning.  If the loss is understood and "
        f"accepted, pass on_conflict='na' to blank exactly these cells (and say why "
        f"at the call site).  GH #323; SkunkWorks/grain_aggregation_policy.org."
    )


def reduce_to_agreed(df: pd.DataFrame, *, on_conflict: str = 'raise',
                     na_is_conflict: bool = False) -> pd.DataFrame:
    """Collapse rows sharing an index tuple, keeping ONLY values they AGREE on.

    The honest reducer for a table whose declared grain is COARSER than the
    source it is extracted from -- e.g. ``cluster_features``, declared ``(t, v)``
    but read off a household-level cover page, where each cluster's attributes
    are meant to be redundant copies across its households.

    Call it from the country's ``_/mapping.py`` instead of letting the frame
    reach core's duplicate-index fallback (``groupby().first()``), which is
    silent, takes the first NON-NULL value PER COLUMN (so where the "redundant"
    copies disagree it can synthesise a row that exists in no source), and hides
    the loss behind the cache it poisons (GH #323).

    Contract
    --------
    * **Lossless or loud.**  A group whose rows agree collapses to that agreed
      row.  A group whose rows DISAGREE is never resolved by picking a winner:
      by default this RAISES :class:`GrainConflict`, naming the columns and the
      offending groups.  With ``on_conflict='na'`` the conflicted cells are set
      missing (loudly missing beats quietly wrong) and a
      :class:`GrainConflictWarning` is emitted -- the caller has to have asked
      for that, in writing, at the call site.  ``LSMS_GRAIN_STRICT=1`` in the
      environment escalates ``'na'`` back to a raise.
    * **NaN is absence, not contradiction** (default).  A group where one row
      says ``Dakar`` and another says nothing keeps ``Dakar``: no observed value
      is discarded, so the completion is lossless.  Pass ``na_is_conflict=True``
      for the stricter reading (a missing report is itself a disagreement).
    * **NaN index keys survive.**  The groupby uses ``dropna=False``, so a row
      whose declared index level is NaN is not deleted here.  (What core does
      with it downstream is core's business -- see #323 Site 3.)
    * **A unique index is returned untouched** -- same rows, same order, same
      dtypes.  Nothing to collapse.

    Args:
        df: the frame as extracted, indexed by the declared (possibly
            non-unique) levels.
        on_conflict: ``'raise'`` (default) or ``'na'``.
        na_is_conflict: treat a NaN alongside an observed value as a
            disagreement rather than as a missing report.

    Returns:
        One row per index tuple.

    Raises:
        GrainConflict: on disagreement, unless ``on_conflict='na'``.
        ValueError: on an unknown ``on_conflict``, or a non-empty frame with no
            named index levels (a caller who forgot to set the index would
            otherwise get a silent no-op).
    """
    if on_conflict not in ('raise', 'na'):
        raise ValueError(f"on_conflict must be 'raise' or 'na', not {on_conflict!r}")
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"reduce_to_agreed expects a DataFrame, got {type(df).__name__}")
    if df.empty:
        return df                       # e.g. grab_data's "no data loaded" frame
    levels = [n for n in df.index.names if n is not None]
    if not levels:
        raise ValueError(
            "reduce_to_agreed needs the declared index set (df.index has no named "
            "levels); set_index the declared levels before reducing."
        )
    if df.index.is_unique:
        return df                       # nothing to collapse -- keep rows/order/dtypes

    cols = list(df.columns)
    grouped = df.groupby(level=levels, observed=True, dropna=False)
    # GroupBy.first() == first NON-NULL per column, which IS the agreed value
    # wherever nunique <= 1.  We never rely on it where nunique > 1.
    reduced = grouped[cols].first()
    nunique = grouped[cols].nunique(dropna=not na_is_conflict).reindex(
        index=reduced.index, columns=cols)
    conflicts = nunique > 1

    if bool(conflicts.to_numpy().any()):
        if on_conflict == 'raise' or _grain_strict():
            raise GrainConflict(_conflict_message(
                conflicts, levels,
                "Collapsing would silently discard the difference." if on_conflict == 'raise'
                else f"on_conflict='na' was requested but {_GRAIN_STRICT_ENV} is set.",
            ))
        warnings.warn(
            _conflict_message(conflicts, levels,
                              "on_conflict='na': blanking those cells rather than "
                              "picking an arbitrary winner."),
            GrainConflictWarning, stacklevel=2)
        reduced = reduced.mask(conflicts)

    return reduced


def collapse_to_cluster_grain(df: pd.DataFrame, *, on_conflict: str = 'raise',
                              na_is_conflict: bool = False) -> pd.DataFrame:
    """Reduce a HOUSEHOLD-grain ``cluster_features`` extraction to CLUSTER grain.

    The named, discoverable case of :func:`reduce_to_agreed` -- and by far the
    commonest: many countries source ``cluster_features`` from the household
    cover file (one row per HOUSEHOLD) while the table is declared at ``(t, v)``,
    so the frame arrives ~12x inflated and core used to collapse it with a silent
    ``groupby().first()``, correct only by the ACCIDENT that the attributes
    happen to be constant within the cluster (GH #323).

    This makes the correction explicit and ENFORCES the invariant its
    correctness rests on -- every attribute single-valued within a cluster.  A
    cluster code that is unique only WITHIN a district, a re-coded region, a
    grappe straddling urban and rural: any of these now raises and names the
    offending clusters instead of keeping one row's value at random.  ("Invariant
    by construction of the sampling design" is prose; prose is not enforcement.)

    Use it as a wave-module ``df_edit`` hook::

        # countries/<C>/<wave>/_/mapping.py
        from lsms_library.transformations import (
            collapse_to_cluster_grain as cluster_features,
        )

    See :func:`reduce_to_agreed` for the full contract (including
    ``on_conflict='na'`` and ``LSMS_GRAIN_STRICT``).  Policy:
    ``SkunkWorks/grain_aggregation_policy.org``.
    """
    return reduce_to_agreed(df, on_conflict=on_conflict, na_is_conflict=na_is_conflict)


@build_transform(tables=['food_acquired'])
def add_visit_level(df: pd.DataFrame, visit: int = 1) -> pd.DataFrame:
    """Append a constant ``visit`` (recall-occasion) level to a food_acquired frame.

    For a country whose ``food_acquired`` index carries a ``visit`` level because
    SOME wave repeats the consumption recall, but whose OTHER waves ask it
    exactly once.  Those single-recall waves get ``visit = 1`` so every wave
    shares one index shape and the country-level concat aligns.

    Motivating case (GH #323): Burkina Faso's 2014 EMC wave is a CONTINUOUS
    survey that revisits the same 10,800 households in four quarterly passages,
    each with its own independent 7-day recall (``visit = 1..4``); its EHCVM
    waves (2018-19, 2021-22) field the module once.  Without the level, the four
    passages collide on one ``(t, v, i, j, u, s)`` tuple and the additive
    reducer SUMS them into a single bogus "7-day" figure.

    This ADDS a level -- it never reduces grain -- which is exactly the
    union-of-levels direction the grain policy asks for: carry the finer
    instrument's detail, let the analyst collapse it.

    Note ``visit`` is NOT EHCVM's ``vague``, which is a sample split (which
    households are surveyed when), not a repeated measure;
    ``food_acquired_to_canonical`` drops that upstream, and reusing it as
    ``visit`` would falsely imply a second recall.

    Raises:
        ValueError: if ``visit`` is already present (in the index or the
            columns) -- silently stamping a second one would corrupt the index.
    """
    if 'visit' in (df.index.names or []) or 'visit' in df.columns:
        raise ValueError(
            "add_visit_level: 'visit' is already present on this frame; stamping "
            "a constant over an existing recall-occasion level would corrupt it."
        )
    return df.assign(visit=visit).set_index('visit', append=True)
