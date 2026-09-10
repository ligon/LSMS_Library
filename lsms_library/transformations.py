#!/usr/bin/env python3

"""
A collection of mappings to transform dataframes.
"""
import re
import warnings
from collections.abc import Mapping

import pandas as pd
import numpy as np
from pandas import concat, get_dummies, MultiIndex
from cfe.df_utils import use_indices
from .local_tools import format_id

# Build-path transforms live in build_transforms.py (their output is baked
# into the cached L2 parquet).  Re-exported here so the country build
# scripts that do `from lsms_library.transformations import ...` keep working.
from .build_transforms import (  # noqa: F401  (re-export for back-compat)
    food_acquired_to_canonical,
    _finalize_canonical_food_acquired,
    fill_v_with_coord_bin,
    apply_derived,
    _DERIVED_TRANSFORMERS,
    # Explicit grain helpers (GH #323).  They live in build_transforms because
    # a country's mapping.py calls them at BUILD time and their output is baked
    # into the L2 parquet; they are re-exported here because that is where the
    # country scripts import from, and because the grain policy
    # (SkunkWorks/grain_aggregation_policy.org) names transformations.py as the
    # home of caller-invoked aggregation.  Both modules are caller-invoked; only
    # the ACCESS PATH (country.py / feature.py / local_tools.py) is forbidden to
    # reduce grain, and none of these is reachable from it.
    reduce_to_agreed,
    collapse_to_cluster_grain,
    add_visit_level,
    GrainConflict,
    GrainConflictWarning,
)


# Canonical values for the food_acquired ``s`` (acquisition source) index
# level.  See ``slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org``
# and GH #169.  ``data_info.yml`` does not currently support enumerated
# value constraints on index levels, so the enumeration lives here and is
# enforced in code by :func:`validate_acquisition_source`, which runs on every
# read from :meth:`Country._finalize_result`.
#
# This is the SINGLE definition of "canonical s".  A country build that needs
# the list (e.g. to filter its raw source codes) must ``import S_VALUES`` --
# never re-spell it locally.  EthiopiaRHS did exactly that and drifted to a
# narrower 3-tuple that silently omitted 'other' (GH #537).
S_VALUES = ('purchased', 'produced', 'inkind', 'other')

# Warn when this share of the rows that *should* have produced a price
# instead get dropped as 0 / inf / NaN by ``food_prices_from_acquired``
# (GH #591).  A price that cannot be computed is data loss; dropping the
# row makes the loss invisible (the row does not come back as NaN -- it
# ceases to exist), which is how Nigeria shipped six waves of
# food_prices built on 0.5% of the data, all graded `sane`.
#
# Calibration (all 20 cached food_acquired parquets, 2026-07): the share of
# expenditure-bearing rows lost at this step is 74% for Nigeria (pre-#591),
# 7.9% for Cambodia (same Quantity==0-with-Expenditure>0 signature, tracked
# separately), 0.2-1.9% for the EHCVM family / Serbia / Uganda / Tanzania /
# GhanaSPS, and 0 elsewhere.  5% therefore fires on a genuine defect and
# stays quiet on the ordinary long tail of unpriceable rows.
PRICE_LOSS_WARN_THRESHOLD = 0.05


class UnpriceableRowsWarning(UserWarning):
    """Emitted when ``food_prices`` drops a large share of priceable rows.

    Subclasses ``UserWarning`` so it can be filtered / promoted to an error
    (``warnings.simplefilter('error', UnpriceableRowsWarning)``) independently
    of the library's other warnings.
    """


class ValuationLadderWarning(UserWarning):
    """The own-production valuation ladder ran on fewer geographic rungs than
    the canonical ``v`` -> ``District`` -> ``Region`` -> national one.

    Emitted by the ``Country.food_expenditures(valuation=...)`` path when the
    country's ``cluster_features`` does not carry one of the coarser rungs (or
    the frame carries no ``v`` at all), naming which rungs were available.  A
    shorter ladder is not an error -- ``median_price_valuation``'s national
    fallback is unconditional, so every row still gets a price -- but it means
    the imputed price is drawn from a coarser market than the caller may
    assume, so it is said out loud rather than inferred from silence.
    """


class ShippedFactorWarning(UserWarning):
    """A ``shipped_factors`` table did not join the way its author meant.

    Two situations, both of which used to be SILENT and both of which look
    exactly like a working table from the counts alone:

    * the table matched **zero** rows -- almost always a vocabulary mismatch
      on a key (``j`` as a raw WB crop code against a decoded label, a numeric
      ``74.0`` against ``'74'``, ``t`` as an int against ``'2019-20'``);
    * the table is keyed on a level *crop_production* does not carry, so that
      level is DROPPED and the factor is applied across it -- a one-region
      table going national, a one-condition table serving every condition.
      The multi-row form of this already raised (the surviving keys become
      ambiguous); the single-row form did not, so the more careless loader
      got the weaker signal.  This makes both loud.

    Its own class so it can be filtered or promoted to an error
    (``warnings.simplefilter('error', ShippedFactorWarning)``) independently
    of the library's other warnings.
    """


class UnitLabelCollisionWarning(UserWarning):
    """Two ``u`` spellings that differ only in case got DIFFERENT kg factors.

    ``conversion_to_kgs`` groups by the raw ``u`` label, so ``Calebasse`` and
    ``calebasse`` are two units; ``_get_kg_factors`` then lower-cases and
    keeps whichever it merges first.  The winner is deterministic (the
    inference returns a sorted mapping) but ARBITRARY, and the two factors
    can differ several-fold.

    Measured over the 19 cached ``food_acquired`` tables (GH #770 review):
    16 collision groups in 3 countries, and in every one the two factors
    DIFFER.  Five are INERT -- three ``kg`` and two ``litre``, keys that
    ``KNOWN_METRIC`` seeds, so neither inferred value is ever used --
    leaving **11 live groups in 3 countries**: Burkina Faso 7, Malawi 2,
    Mali 2.  Burkina Faso's ``Calebasse`` 0.35 vs ``calebasse`` 1.432 is
    4.1x; Mali's ``Unité`` 1.2 vs ``unité`` 0.286 is 4.2x.  Only the live
    groups warn.

    Panama is NOT in that list, and was the case that surfaced this:
    ``'Value' -> 0.3489`` and ``'value' -> 2.3405`` was a 6.7x
    iteration-order accident.  Both spellings are currency-denominated, so
    GH #770 removes them from the inference and the collision with them --
    which is why the corpus census above is taken AFTER that fix and still
    finds 11.

    This warns rather than merging, because merging would CHANGE the served
    factors for those countries -- a data change, not a diagnostic.  The fix
    belongs in the country's own unit vocabulary: canonicalise the spelling
    where the label is minted, exactly as GH #770 did for currency labels.
    """


def _as_float(s):
    """Series -> float64 ndarray with NaN for missing (pd.NA-safe)."""
    return pd.to_numeric(s, errors='coerce').to_numpy(dtype='float64',
                                                      na_value=np.nan)


def _drop_unpriceable(v, units, expected, threshold=PRICE_LOSS_WARN_THRESHOLD):
    """Drop rows with a 0 / +-inf / NaN Price -- loudly (GH #591).

    Splits the dropped rows by cause before discarding them, records the
    tally on ``result.attrs['price_rows_dropped']``, and emits ONE
    aggregated :class:`UnpriceableRowsWarning` per call (never one per row)
    when the loss exceeds *threshold* of the rows that ought to have
    produced a price.

    Causes
    ------
    ``inf``
        ``Price = Expenditure / 0``: the survey recorded money spent but a
        zero (or zero-after-kg-conversion) quantity.  ALWAYS a data defect
        -- either a 0-as-missing sentinel or, as in Nigeria, a quantity
        variable bound to the wrong survey question.
    ``nan``
        Quantity missing, or a unit with no kg factor (the ``kg*`` modes).
    ``zero``
        Zero Expenditure (``*value``) or a zero reported Price (``*price``).

    Currency rows are NOT one of the causes -- they get their own bucket
    ----------------------------------------------------------------------
    A currency-denominated unit (:data:`_CURRENCY_DENOMINATED_UNITS`, e.g.
    ``u='Value'``) has no per-label kg factor *by design* (GH #770), so in
    the ``kg*`` modes every such row would land in ``nan``.  Counting them
    as loss destroys the signal this warning exists to carry: measured on
    GhanaLSS ``kgvalue``, 3,492,915 of the 3,493,339 lost rows are
    by-design currency and **424** are everything else -- 79.84% reported
    versus 0.048% real.  A warning pinned at ~79.8% forever cannot report
    the next defect in its own cell (a new 10,000-row break moves it to
    80.07%), which is the repo's own GH #323 failure with a warning
    attached.

    So currency rows in the ``kg*`` modes are excluded from BOTH ``lost``
    and ``expected`` and tallied separately as ``currency``: they were
    never expected to produce a kg price.  ``expected_gross`` keeps the
    unadjusted denominator so nothing is hidden.  The ``unit*`` modes need
    no adjustment -- a currency row is perfectly priceable there
    (``Expenditure / Quantity``), so it stays in the denominator.

    Parameters
    ----------
    v : DataFrame
        Frame carrying the computed ``Price`` column.
    units : str
        The ``food_prices`` mode, for the message.
    expected : boolean ndarray
        Rows that OUGHT to have produced a price -- the denominator of the
        loss fraction.  Supplied by the caller from the *inputs* (Expenditure
        > 0 for the ``*value`` modes; a non-null REPORTED Price for the
        ``*price`` modes), never from the computed Price, so a conversion
        failure counts as a loss rather than silently shrinking the base.
    """
    price = _as_float(v['Price'])

    is_inf = np.isinf(price)
    is_nan = np.isnan(price)
    is_zero = (price == 0)
    dropped = is_inf | is_nan | is_zero

    expected = np.asarray(expected, dtype=bool)

    # Currency-denominated rows have no kg factor BY DESIGN (GH #770), so in
    # the kg* modes they are not rows that "ought to have produced a price".
    # Give them their own bucket and take them out of both sides of the
    # fraction, so what remains is the residual REAL loss -- the only number
    # a threshold can usefully bite on.  In the unit* modes a currency row is
    # priceable (Expenditure / Quantity), so nothing is excluded there.
    if units in ('kgvalue', 'kgprice') and 'u' in (v.index.names or []):
        by_design = _is_currency_denominated(
            v.index.get_level_values('u')) & expected
    else:
        by_design = np.zeros(len(v), dtype=bool)
    effective = expected & ~by_design

    lost = int((dropped & effective).sum())
    n_expected = int(effective.sum())
    tally = {
        'units': units,
        'rows_in': int(len(v)),
        # Denominator of `lost_fraction`: expected MINUS by-design currency.
        'expected': n_expected,
        # The unadjusted denominator, so the adjustment is auditable.
        'expected_gross': int(expected.sum()),
        # Dropped by design because the unit is currency-denominated; NOT
        # counted in `dropped_expected` / `lost_fraction`.
        'currency': int((dropped & by_design).sum()),
        'currency_expected': int(by_design.sum()),
        'dropped_total': int(dropped.sum()),
        'dropped_expected': lost,
        'inf': int((is_inf & effective).sum()),
        'nan': int((is_nan & effective).sum()),
        'zero': int((is_zero & effective).sum()),
        'lost_fraction': (lost / n_expected) if n_expected else 0.0,
    }

    if n_expected and lost / n_expected > threshold:
        currency_note = (
            f"  A further {tally['currency']:,} row(s) were dropped because "
            f"their unit is CURRENCY-DENOMINATED (u='Value': the survey "
            f"deliberately elicited value, not a physical amount).  Those "
            f"are BY DESIGN -- the factor is 1/price at (j, t, m) and no "
            f"per-label constant can represent it (GH #770) -- so they are "
            f"EXCLUDED from the count and the fraction above, which report "
            f"the residual real loss."
            if tally['currency'] else ""
        )
        warnings.warn(
            f"food_prices(units={units!r}): dropped {lost:,} of {n_expected:,} "
            f"priceable rows ({lost / n_expected:.1%}) because Price was "
            f"inf ({tally['inf']:,}), NaN ({tally['nan']:,}) or zero "
            f"({tally['zero']:,}).  These rows are DROPPED, not returned as "
            f"NaN, so the gap is otherwise invisible.  The causes are "
            f"DIFFERENT and only the first is always a defect: an inf Price "
            f"means the survey recorded an Expenditure against a ZERO "
            f"Quantity -- typically a 0-as-missing sentinel or a quantity "
            f"variable mapped to the wrong survey question (GH #591).  A NaN "
            f"Price means the quantity was missing OR the unit has no kg "
            f"factor.{currency_note}  See GH #591, GH #770.",
            UnpriceableRowsWarning,
            stacklevel=3,
        )

    out = v[~dropped][['Price']]
    out.attrs = dict(v.attrs)
    out.attrs['price_rows_dropped'] = tally
    return out


def validate_acquisition_source(df: pd.DataFrame) -> None:
    """Raise ``ValueError`` if the ``s`` index level is non-canonical.

    No-op when ``s`` is not in the index.  Called from
    :meth:`lsms_library.country.Country._finalize_result`, so it runs on the
    frame the user actually receives, for every table carrying an ``s`` level
    (``food_acquired`` and the three ``_FOOD_DERIVED`` tables built from it).

    **What this is.**  A *post-condition regression guard*, not a bug detector.
    It passes on 100% of current data: as of 2026-07-12, all 80 (country x food
    table) frames -- 31.9M rows across the 20 countries with a ``food_acquired``
    -- are already canonical, with zero NaN.  It finds nothing today, and that
    is the expected steady state.  Do not read a green run as evidence that a
    number is right; read it as evidence that nobody has *broken* ``s`` yet.

    **Why it is worth its cost anyway.**  19 of those 20 countries set ``s`` as
    a bare string literal in a build script (``purch['s'] = 'purchased'``).  A
    single typo -- ``'Purchased'``, ``'purchase'``, ``'own-production'`` --
    would mint a phantom category and silently split rows across the
    ``(t, v, i, j, u, s)`` index, so ``food_expenditures`` would quietly drop or
    double-count a source.  That is a silently-wrong-number class of bug, and it
    is exactly what this guard turns into a loud crash.  A crash is a gift.

    **NaN is a violation, deliberately.**  The original implementation called
    ``.dropna()`` before comparing, which made it structurally blind to a NaN
    ``s`` -- the *dominant* real-world non-conformity, since an unmapped source
    code (EthiopiaRHS/1989 has 677 of them upstream) becomes NaN rather than a
    bad string.  A row whose acquisition source is unknown does not belong on
    the ``s`` axis: it must be mapped to a canonical value, mapped to ``other``,
    or dropped at the wave level -- explicitly, by the country's build.

    See ``slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org`` and GH
    #169 (which introduced ``S_VALUES``) / GH #537 (which wired this in -- it
    had zero call sites from birth despite a docstring claiming otherwise).
    """
    names = list(df.index.names or [])
    if 's' not in names:
        return

    # This runs on EVERY read (_finalize_result), including Feature(), which
    # assembles all 20 food countries in one call -- so the clean case has to be
    # cheap.  The old form, get_level_values('s').unique(), is O(#rows) and cost
    # 877 ms on GhanaLSS food_acquired (5.26M rows).  Instead:
    #
    #   * unique values come off the MultiIndex *levels*, already deduplicated
    #     -> O(#unique), microseconds;
    #   * NaN is read off the level *codes* (pandas encodes a missing entry as
    #     code -1 and never stores NaN in `levels`) -> one vectorised int
    #     compare.  This is also precisely why a levels-based scan -- and the
    #     original `.dropna()` scan -- could not see a NaN `s` at all.
    #
    # `levels` can retain entries for rows filtered out upstream, so a bad value
    # found there is only a *suspicion*; we pay the O(#rows) prune to confirm it
    # and avoid a false positive.  That cost is only ever paid on the unhappy
    # path.  Net: 877 ms -> ~10 ms on GhanaLSS when the data is clean.
    pos = names.index('s')
    allowed = set(S_VALUES)
    if isinstance(df.index, pd.MultiIndex):
        n_missing = int((np.asarray(df.index.codes[pos]) == -1).sum())
        invalid = set(df.index.levels[pos]) - allowed
        if invalid:  # suspicion only -- confirm against rows actually present
            invalid = set(df.index.remove_unused_levels().levels[pos]) - allowed
    else:
        level = df.index.get_level_values('s')
        n_missing = int(pd.isna(level).sum())
        invalid = set(level.dropna().unique()) - allowed

    problems = []
    if invalid:
        problems.append(f"non-canonical value(s) {sorted(map(repr, invalid))}")
    if n_missing:
        problems.append(f"{n_missing} row(s) with a missing (NaN) value")
    if problems:
        raise ValueError(
            f"Invalid 's' (acquisition source) index level: {'; '.join(problems)}. "
            f"Allowed values are {S_VALUES} and NaN is not permitted -- map the "
            f"offending rows to a canonical source (or to 'other') in the wave's "
            f"build, or drop them there explicitly.  See "
            f"slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org."
        )


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

    Notes
    -----
    **Residence filter.**  Non-resident members are dropped using whichever
    residence-duration column the roster carries — ``MonthsSpent`` (months
    present), ``MonthsAway`` (→ ``12 - value``) or ``WeeksAway``
    (→ ``12 - weeks/(52/12)``); see ``CLAUDE.md`` §"MonthsSpent /
    MonthsAway / WeeksAway".  ``df`` here is the *country-level* concat of
    every wave, so those columns are unioned across waves: a column
    supplied by one wave appears (all-NaN) on the others.  The resolution
    is therefore a **per-row coalesce** across the three sources, and a
    wave with no usable residence datum at all falls back to counting every
    roster member — the documented behaviour for a country with no
    residence column, applied per-wave.  Without those two rules an entire
    wave's ``household_characteristics`` silently vanishes while its
    ``household_roster`` is fully populated.
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
    # Ethiopia W4-W5 and Cambodia use WeeksAway instead.
    #
    # The resolution is a per-ROW coalesce, not a per-column choice: this
    # frame is the *country-level* concat of every wave, so a column
    # contributed by one wave is unioned onto all the others.  Ethiopia
    # carries BOTH MonthsAway (W1-W3) and WeeksAway (W4-W5), disjointly
    # populated -- picking the first column *present* would resolve W4-W5
    # to an all-NaN MonthsAway and delete both waves.  Priority order on
    # the (rare) rows where more than one source is populated:
    # MonthsSpent (asked directly) > MonthsAway > WeeksAway (coarser).
    # Work positionally (numpy) rather than through pandas alignment: the
    # roster index is a MultiIndex with duplicate entries in the wild.
    def _numeric(col):
        return pd.to_numeric(roster_df[col], errors='coerce').astype('float64').to_numpy()

    ms = None
    _sources = []
    if 'monthsspent' in roster_df.columns:
        _sources.append(_numeric('monthsspent'))
    if 'monthsaway' in roster_df.columns:
        # months present = 12 - months away; clip guards >12 outliers
        _sources.append(np.clip(12 - _numeric('monthsaway'), 0, None))
    if 'weeksaway' in roster_df.columns:
        _sources.append(np.clip(12 - (_numeric('weeksaway') / (52 / 12)), 0, None))
    if _sources:
        ms = _sources[0]
        for _alt in _sources[1:]:
            ms = np.where(np.isnan(ms), _alt, ms)
    if ms is not None:
        age = _numeric('age')
        with np.errstate(invalid='ignore'):
            keep = ~np.isnan(ms) & ((ms > 0) | (age < 1))
        # A wave can end up with a residence column that is entirely NaN
        # *within that wave* -- either because the survey never asked the
        # question (CotedIvoire 1985-89, Mali 2021-22: the column exists in
        # this frame only because the country-level concat unioned it in
        # from a later wave) or because the labels did not parse.  ``keep``
        # is then all-False for the wave and household_characteristics
        # silently returns NOTHING for it while household_roster is fully
        # populated.  Guard per ``t``: a wave with no usable residence datum
        # at all reverts to the documented pre-MonthsSpent behaviour --
        # "countries without any residence column are unaffected -- the old
        # count-everyone behavior continues" (CLAUDE.md) -- applied at wave
        # granularity.  Waves that DO have residence data are filtered
        # exactly as before, so the Uganda drift the filter was introduced
        # to fix stays fixed.
        has_ms = ~np.isnan(ms)
        idx_names = roster_df.index.names or []
        if 't' in idx_names:
            t_vals = pd.Index(roster_df.index.get_level_values('t'))
            wave_has_ms = (
                pd.Series(has_ms)
                .groupby(t_vals, dropna=False)
                .transform('any')
                .to_numpy()
            )
        else:
            wave_has_ms = np.full(len(roster_df), bool(has_ms.any()))
        if not wave_has_ms.all():
            import warnings as _warnings
            if 't' in idx_names:
                _dead = sorted(
                    {str(t) for t, ok in zip(t_vals, wave_has_ms) if not ok}
                )
            else:
                _dead = ['<no-t>']
            _warnings.warn(
                f"household_characteristics: no usable residence duration "
                f"(MonthsSpent / MonthsAway / WeeksAway) for wave(s) {_dead}; "
                f"counting every roster member there instead of filtering to "
                f"resident members.  The column is present in the country-level "
                f"frame only because another wave supplies it.  Waves with "
                f"residence data are filtered as usual.",
                UserWarning,
                stacklevel=3,
            )
        keep = keep | ~wave_has_ms
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


# Unit labels denominated in local currency rather than in a physical
# amount (GH #770).  Matched case-insensitively against the lower-cased
# ``u`` label.
#
# These are EXCLUDED from the price-ratio inference in
# :func:`conversion_to_kgs` and from the merge in :func:`_get_kg_factors`,
# so no kg factor is produced for them and ``Quantity_kg`` stays NaN.
#
# WHY -- and the name matters.  ``u='Value'`` is NOT a "non-physical" unit:
# Value = Quantity x Price, so ``Value / Price = Quantity`` and a kg factor
# for it *does* exist -- it is ``1/price``.  Prices are the units in which
# value measures quantity.  The defect is one of GRANULARITY, not of
# existence: :func:`conversion_to_kgs` returns ONE factor per unit label,
# while ``1/price`` varies over ``(j, t, m)``.  A per-label constant cannot
# represent it, so the inference must not pretend otherwise.  Calling these
# labels "non-physical" would teach the next reader the very error this
# constant fixes.
#
# NaN is therefore the honest INTERIM state, not a terminal verdict: the
# factor is recoverable per ``(j, t, m)`` from a price source, which is the
# separate ``source=`` work (Step 2 of #770), not this.
#
# THE LIBRARY RECOGNISES ONE CANONICAL SENTINEL; COUNTRIES CANONICALISE
# ONTO IT.  This set is not, and must not become, an enumeration of the
# corpus's currency-ish labels -- that would be an unbounded blocklist of
# world currencies running beside a canonical label that already exists.
# ``'Value'`` IS that label: ``country._RESERVED_U_SENTINELS`` documents it
# as the marker for LCU-only goods, amounts are in local currency units by
# default, and ``currency=`` (``lsms_library/currency.py``) is the lever for
# currency *representation*.  So the NAME of a currency is redundant with
# the country and the wave and does not belong in a ``u`` label at all.
#
# A country whose survey elicits value rather than quantity therefore emits
# ``u='Value'`` where the label is MINTED -- in its wave script or its
# ``categorical_mapping.org`` -- and needs no edit here.  Two did exactly
# that in GH #770: EthiopiaRHS's ``harmonize_unit`` code 30 (was ``Birr``)
# and Serbia 2007's ``mera == 'dinar'``.  The alternative, adding ``birr``
# and ``dinar`` here, was proposed and REJECTED.
#
# The rename must land where the label is minted, NOT in an API-time
# categorical mapping: the derived-food dispatch is
# ``_aggregate_wave_data`` -> ``transform_fn`` -> ``_finalize_result``
# (``country.py:4155-4161``), so ``conversion_to_kgs`` below sees the RAW
# ``u`` and an API-time rename would fire after the factor was inferred.
#
# Why a set at all, rather than a detector?  The obvious detector
# (``Quantity == Expenditure``) tests the SYMPTOM rather than the cause, and
# a survey that elicits value need not follow that convention: Panama 1997
# is 0.5% (99.1% of those rows carry the undecoded 7.70 sentinel of GH
# #777) and Serbia's rows carry ``Quantity`` NaN outright.  A sentinel the
# country declares is checkable; an inferred one is a guess.  If a detector
# is ever founded on the real cause, note that compiled-regex module
# constants must NOT be used: they land in the hashed import closure and are
# un-serialisable (GH #780).
#
# (An earlier version of this comment claimed the corpus holds "662 distinct
# unit labels ... only ``value`` qualifies".  Both halves were false -- the
# figure is ~1,636 across the four ``u``-bearing tables, and two countries
# were minting currency labels of their own.  The claim is not corrected
# here but RETIRED: a count of the corpus rots, whereas "one canonical
# sentinel, countries canonicalise onto it" is bounded and stays true as
# countries are added.)
#
# Kept in sync with ``lsms_library.country._RESERVED_U_SENTINELS`` (the
# capital-``V`` spelling of the same concept, GH #361) by
# ``tests/test_u_sentinel_protection.py``; a plain import would be circular
# (``country`` imports ``transformations``).
_CURRENCY_DENOMINATED_UNITS = frozenset({'value'})


def _is_currency_denominated(labels):
    """Boolean ndarray over raw ``u`` labels, matched case-insensitively.

    Accepts a Series, an Index or a plain sequence; always returns a numpy
    bool array so the caller need not care which it passed (``pd.Index.isin``
    already returns an ndarray, ``Series.isin`` a Series).
    """
    return np.asarray(
        pd.Index(labels).astype(str).str.lower().isin(
            _CURRENCY_DENOMINATED_UNITS),
        dtype=bool,
    )


#: Floor on the number of step-2 reports behind a ``(t, j, u)`` estimate
#: (GH #850, D4).  Set equal to :data:`SURVEY_MEDIAN_MIN_REPORTS` on purpose:
#: it is the same quantity on the food side that the crop side's
#: ``survey_median`` layer already gates, and the floor sweep in
#: ``slurm_logs/gh850_design/DESIGN.org`` shows coverage is nearly flat in it
#: (moving it from 3 to 20 costs one or two percent of inference rows in every
#: country), so consistency decides what the data does not.
#:
#: Spelled as a literal rather than as ``= SURVEY_MEDIAN_MIN_REPORTS`` only
#: because that constant is defined further down the module;
#: ``tests/test_food_kg_inference.py`` pins the two equal so the tie cannot rot.
FOOD_KG_MIN_REPORTS = 5

#: Floor on the number of kg-known rows behind a ``(t, j)`` price-per-kg
#: baseline (GH #850, D5).  THIS is the floor that binds -- it decides whether
#: one household's single kilogram purchase gets to set an item's price
#: nationally, and the largest price movements measured for #850 sat on the
#: thinnest baselines.  The food side gated nothing at all before #850.
FOOD_KG_MIN_BASELINE = 5

#: A baseline of :data:`FOOD_KG_MIN_BASELINE_TIGHT` to
#: :data:`FOOD_KG_MIN_BASELINE` - 1 reports is accepted WHEN THE REPORTS AGREE
#: -- max/min of the per-report price per kilogram within the ``(t, j)`` cell
#: at or below :data:`FOOD_KG_TIGHT_TOLERANCE` (GH #850, D5).
#:
#: WHY max/min AND NOT AN IQR.  Because at N = 3 or 4 an interquartile range
#: DISCARDS THE EXTREMES, and the extremes are the whole question.  With three
#: reports the interpolated quartiles sit between the points; the statistic is
#: driven by the middle of a sample too small to have a middle, and a single
#: wild report -- the exact failure the floor exists to catch -- barely moves
#: it.  ``max/min`` reads the whole spread, is scale-free, is defined
#: identically at N = 3 and at N = 400, and states the sentence the exception
#: is for: "the reports lie within 25% of each other".
#:
#: AN EARLIER VERSION OF THIS PARAGRAPH ARGUED THE POINT WITH A NUMBER, AND
#: THE NUMBER WAS A SCALE ERROR.  It said an ``IQR/median`` gate "admits 21 of
#: Uganda's 28 in-band cells at a tolerance of 1.10 and 26 at 2.00 -- a
#: statistic that barely moves ... is not discriminating".  Those counts
#: reproduce, but 1.10 and 2.00 are not the same tolerance for the two
#: statistics: ``max/min <= 1.25`` means the reports lie within about +/-12%,
#: while ``IQR/median <= 1.10`` means the interquartile range is 110% OF THE
#: MEDIAN -- an enormously looser gate.  Swept at its own scale the IQR gate
#: discriminates perfectly well: at ``IQR/median <= 0.10`` Uganda admits 2
#: cells whose ``max/min`` p90 is 1.32.  The comparison was between
#: incommensurable scales and a reviewer could falsify it in ten minutes.
#: The choice stands on the ground stated above; the evidence for it does not,
#: and is retired rather than repaired (GH #850 red team, 2026-09-10).
#:
#: WHAT IT BUYS, MEASURED.  Little, and that is the point.  The in-band cells
#: are few (10 to 33 per country, of 253 to 831), and taken UNCONDITIONALLY
#: they would lift Uganda's inference coverage by 9.5 points -- but their
#: spreads run 1.2x to 25x, so nearly all of that would come from baselines
#: whose own reports disagree by an order of magnitude.  At 1.25 the exception
#: admits 0 to 3 cells per country and 0 to +0.7 points of coverage, and the
#: admitted cells' median spread is 1.00 to 1.25.  It is a narrow, principled
#: admission, not a coverage lever; a tolerance of 2.00 would be the floor
#: giving up.
FOOD_KG_MIN_BASELINE_TIGHT = 3
FOOD_KG_TIGHT_TOLERANCE = 1.25

#: Cross-wave spread above which a delivered ``(item, unit)`` factor is
#: REPORTED as thin evidence (GH #850, D6, 2026-09-10).  A reporting
#: threshold, never a screen: no row is refused for exceeding it.
#:
#: The delivered factor has no ``t`` axis -- it is the median of the per-wave
#: estimates, because a container's weight should not move between waves.
#: That is a defensible design and an UNVERIFIED assumption on any particular
#: cell, and the corpus is not reassuring about it: for cells estimated in
#: three or more waves the median max/min across waves is 2.2 (Uganda), 2.5
#: (Malawi) and 3.1 (Nigeria).  Two is the natural place to report from --
#: below it the pooling is doing what it claims (averaging noise), above it
#: the waves are describing different objects and the median is a compromise
#: between them.
#:
#: Niger makes the case concrete: ``(Mil, Tiya)`` is estimated in ONE of the
#: two waves it serves, because 2018-19 has no kg-known millet row at all, and
#: 4,886 rows are served a factor measured on a wave they are not in.
FOOD_KG_WAVE_SPREAD_REPORT = 2.0

#: The eight-key map that decides which rows may ANCHOR the price-per-kg
#: baseline.  Deliberately NOT :data:`KNOWN_METRIC`: no litre, ml or cl, so a
#: volume row can enter the baseline only through a survey-supplied
#: ``Quantity_kg``, and the ``1 L = 1 kg`` approximation never propagates into
#: OTHER units' factors through the reference price.
#:
#: It was a local literal inside :func:`conversion_to_kgs` until GH #850 named
#: it.  Naming it is the whole change: widening it to the metric spellings
#: #850 added to :data:`KNOWN_METRIC` (``Grams``, ``Gramme``, ``Millilitre``)
#: would enlarge the baseline and move every inferred factor, which is a
#: measured step nobody has taken -- so it is a stated follow-up, not a
#: side effect.
_BASELINE_UNIT_CONVERSION = {
    'kg': 1, 'kilogram': 1, 'gram': 1 / 1000, 'g': 1 / 1000,
    'pound': 0.453592, 'lbs': 0.453592, 'kilogramme': 1, 'gramm': 1 / 1000,
}


def _kg_inference_frame(df, quantity, unit_col):
    """Shared preparation for both arms of :func:`conversion_to_kgs`.

    Returns a copy of *df* with zeros NaN-ed, currency-denominated labels
    dropped (GH #770), ``u`` restored to the index under its canonical name,
    and a ``Kgs`` column holding each row's KNOWN kilograms -- from
    :data:`_BASELINE_UNIT_CONVERSION` where the label is metric, else from a
    survey-supplied ``Quantity_kg``.
    """
    v = df.copy()
    v = v.replace(0, np.nan)
    #convert the value type in index level 'u' to be string
    v = v.reset_index(unit_col)
    if unit_col != 'u':
        v = v.rename(columns={unit_col: 'u'})
    # Currency-denominated labels leave the computation ENTIRELY, not just
    # the ``v_infer`` step below (GH #770).  They must not contribute to the
    # ``pkg`` price-per-kg baseline either -- a ``Value`` row carrying a
    # non-null ``Quantity_kg`` would otherwise enter it through the
    # ``.where(..., Quantity_kg)`` fill and move OTHER units' factors.  The
    # acceptance criterion is that exactly one key disappears.
    v = v[~_is_currency_denominated(v['u'])]
    # Vectorize: ``astype(str)`` followed by ``.str.lower()`` handles any
    # underlying dtype (object with NaN, pyarrow string with pd.NA,
    # Categorical from a .dta read), whereas the previous row-by-row
    # ``.apply`` could surface a Python float for an NA cell despite the
    # earlier ``astype(str)`` (pandas 2.x AttributeError: 'float' object
    # has no attribute 'lower').  Unknown units map to NaN, matching the
    # original ``unit_conversion.get(..., np.nan)`` semantics.
    factors = v['u'].astype(str).str.lower().map(
        _BASELINE_UNIT_CONVERSION).astype(float)
    v['Kgs'] = v[quantity] * factors
    # Rows with an exact per-row Quantity_kg (e.g. Malawi cfactor units, GH
    # #378) serve as kg *references* for the price-per-kg baseline -- exactly
    # as they did when they were build-time-converted to u='kg' -- but they
    # are excluded from the per-unit inference below (their kg is known, not
    # inferred).  This keeps the data-driven factors for genuinely-unknown
    # units identical before/after the Quantity_kg migration.
    if 'Quantity_kg' in v.columns:
        v['Kgs'] = v['Kgs'].where(v['Kgs'].notna(), v['Quantity_kg'])
    return v.set_index('u', append=True)


#: Levels of ``conversion_to_kgs``'s ``index`` that survive into the ITEM
#: arm's baseline group.  The household level ``i`` is deliberately dropped
#: (the item replaces it), and so is the market level ``m`` -- a market axis
#: would re-thin the very cell the floors exist to protect, and ``m`` is not
#: present at derive time anyway (it is minted by ``_add_market_index``
#: inside ``_finalize_result``, after the derived transform has run).
_WAVE_LEVELS = ('country', 't')


def _level_series(df, name):
    """Index level *name* as an object Series on ``df``'s own index."""
    return pd.Series(df.index.get_level_values(name), index=df.index,
                     dtype=object)


def conversion_to_kgs(df, price = ['Expenditure'], quantity = 'Quantity',
                      index=['t','m','i'], unit_col = 'u', *, item_col=None,
                      min_reports=FOOD_KG_MIN_REPORTS,
                      min_baseline=FOOD_KG_MIN_BASELINE,
                      min_baseline_tight=FOOD_KG_MIN_BASELINE_TIGHT,
                      tight_tolerance=FOOD_KG_TIGHT_TOLERANCE,
                      baseline_max_spread=None,
                      _detail=False):
    """Infer local-unit -> kg conversion factors from price ratios.

    For each unit that does not appear in :data:`KNOWN_METRIC`, this
    function computes a factor by assuming the *price per kilogram*
    should be roughly constant across units for the same item/market.
    That is: if a "bunch" of item j trades at roughly 2x the unit value
    of a kg of item j, the inferred factor is 2 kg per bunch.

    The mechanics, in three steps:

    1. **the baseline** -- each row's price per kilogram
       (``price / Kgs``) is medianed over a group, giving a reference
       price for a kilogram;
    2. **step 2** -- each row's price per UNIT (``price / quantity``) is
       medianed over the same group plus ``u``, over rows whose kilograms
       are not already known;
    3. **the factor** -- their ratio, medianed over the remaining axes.

    Two things about that chain were wrong before GH #850, and both are
    fixed here.

    **Step 2 used to median ``price`` itself**, not ``price / quantity``,
    while this docstring described the result as a per-unit price.  The
    factor it returned was therefore kilograms per transaction ROW, equal to
    kilograms per unit only where ``Quantity == 1`` -- between 4.6% and 41.8%
    of the corpus's rows.  The self-test is the cheap way to see it: asked
    what a kilogram weighs, the old chain answered between 0.87 and 2.0
    depending on the country, and the deviation tracked the median quantity
    on the rows it looked at.

    **No step carried the ITEM**, so one factor per unit label served a whole
    country: Malawi's ``Piece`` is shared by 133 food items, and the 72 the
    data can estimate separately run from 1 g to 1.8 kg.  Pass
    ``item_col='j'`` for the item-keyed estimate; the default ``None``
    reproduces the old keys (and the old return type), with the corrected
    arithmetic.

    Labels in :data:`_CURRENCY_DENOMINATED_UNITS` (e.g. ``u='Value'``) are
    dropped from the whole computation before anything is inferred, so they
    never appear as a key of the result: their factor is ``1/price`` at
    ``(j, t, m)`` and a per-label constant cannot represent it (GH #770).

    The item arm's grouping, and why it is not ``index + ['j']``
    ------------------------------------------------------------
    The baseline is keyed ``(t, item)`` -- the WAVE levels of *index* plus
    the item, with the household level dropped.  Appending ``j`` to *index*
    instead would ask one household to have bought item ``j`` both in
    kilograms and in bunches within one wave, which is precisely why the
    household key fails as an item key.  The precedent for dropping the
    household is on the crop side: :func:`_survey_median_factors` groups
    ``(country, t, u, condition)`` and never carries ``i``.

    Two floors gate the item arm, and they gate different things:
    *min_reports* is the number of step-2 rows behind a ``(t, item, u)``
    estimate (the twin of :data:`SURVEY_MEDIAN_MIN_REPORTS`), and
    *min_baseline* is the number of kg-known rows behind the ``(t, item)``
    price-per-kg reference.  The second is the one that binds.  Neither
    applies to the ``item_col=None`` arm, which is ungated exactly as it was
    before #850 -- it is the FALLBACK rung, and gating a fallback would
    leave rows with nothing.

    *min_reports* is applied PER WAVE, to each ``(t, item, u)`` estimate, and
    a wave that cannot clear it contributes nothing to the cross-wave median.
    It was applied to the support SUMMED over waves until the GH #850 red team
    measured it (2026-09-10), which meant one report per wave in five waves
    cleared a floor that four reports in a single wave did not.  The crop
    side's :func:`_survey_median_factors` gates per wave; so does this now.

    A THIRD gate exists and is OFF by default: *baseline_max_spread* refuses a
    ``(t, item)`` baseline whose own reports disagree by more than the given
    factor, measured as ``p90/p10`` where the cell has 10 or more reports and
    ``max/min`` below that.  The strict rung is otherwise ungated, and
    measured on the corpus it admits cells whose reports differ by up to 9e7x
    (median max/min 7.5 in Uganda, 178 in Malawi, 500 in Ethiopia).  Left off
    pending a measured default; see the sweep in
    ``.coder/ledger/850-food-kg-item-axis-impl.md``.

    WHAT THE ITEM ARM IS AND IS NOT.  It is exactly as good as the item's own
    kg-labelled rows.  Where those are sound it is right (Niger's cowpea comes
    back at 1.11 kg per tiya against an answer key of 1.17); where they are
    not, it is wrong AND LOCALISED, whereas the u-pooled factor it replaces
    was wrong everywhere and diluted.  Niger's millet is the worked example:
    nine ``Kg`` rows in one wave price millet at 2,286 FCFA/kg against a
    retail 250-450, and the item arm hands the resulting 0.33 kg/tiya to all
    10,107 of that item's tiya rows.  Removing a dilution is not the same
    thing as removing an error, and a movement is not a correction until
    something outside the estimate says so.

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
        Groupby levels for the per-item/period median step.  At derive time
        this is ``['t', 'i']`` -- ``m`` is minted by ``_add_market_index``
        inside ``_finalize_result``, which runs AFTER the derived transform.
    unit_col : str
        Name of the unit index level; renamed to ``u`` if different.
    item_col : str or None, default None
        Index level naming the item.  ``None`` keeps the pre-#850 keys and
        the ``dict[str, float]`` return.  ``'j'`` keys the result on
        ``(item, unit)``.
    min_reports : int, default :data:`FOOD_KG_MIN_REPORTS`
        Floor on the step-2 support behind a ``(t, item, u)`` estimate.
    min_baseline : int, default :data:`FOOD_KG_MIN_BASELINE`
        Floor on the kg-known rows behind a ``(t, item)`` baseline.
    min_baseline_tight, tight_tolerance
        The dispersion-gated exception to *min_baseline*: a baseline with
        between *min_baseline_tight* and ``min_baseline - 1`` reports is
        accepted when the max/min of its per-report price per kilogram is at
        or below *tight_tolerance*.  See :data:`FOOD_KG_TIGHT_TOLERANCE` for
        why the spread is a range and not an IQR.
    baseline_max_spread : float or None, default None
        When given, a ``(t, item)`` baseline whose reports disagree by more
        than this factor is REFUSED however many of them there are --
        ``p90/p10`` at N >= 10, ``max/min`` below it.  Off by default; the
        tight rung is unaffected (its own tolerance is stricter).

    Returns
    -------
    dict[str, float] or dict[tuple[str, str], float]
        Mapping of unit label -> inferred kg factor when ``item_col is
        None``; of ``(item, unit)`` -> factor when it is given.  Keys are the
        RAW ``u`` label with its case PRESERVED -- the grouping is on the raw
        label, so ``Calebasse`` and ``calebasse`` come back as two entries
        with two factors.  (The docstring used to say "(lowercased)", which
        was false; :func:`_get_kg_factors` is where the lower-casing happens,
        and it reports the resulting clashes -- see
        :class:`UnitLabelCollisionWarning`.)  Units already in
        :data:`KNOWN_METRIC`, units in :data:`_CURRENCY_DENOMINATED_UNITS`,
        or units that cannot be inferred are absent from the output.

        With ``_detail=True`` the item arm returns the underlying frame
        instead -- indexed ``(item, unit)`` with columns ``kg_per_unit``,
        ``support``, ``n_waves``, ``wave_spread`` and ``baseline_tight``.
        Private: it exists so :func:`food_kg_factors` can report WHICH rung,
        WHICH baseline and HOW MANY WAVES served a row without estimating
        twice.
    """
    v = _kg_inference_frame(df, quantity, unit_col)
    v_infer = (v[v['Quantity_kg'].isna()] if 'Quantity_kg' in v.columns else v)

    if item_col is None:
        pkg = v[price].divide(v['Kgs'], axis=0)
        pkg = pkg.groupby(index).median().median(axis=1)
        # GH #850 defect (b): the per-unit price the docstring promises.
        # ``price`` may name several columns, so divide before the groupby
        # and keep the existing "median across price columns" reduction.
        po = (v_infer[price].divide(v_infer[quantity], axis=0)
              .groupby(index + ['u']).median().median(axis=1))
        kgper = (po / pkg).dropna()
        kgper = kgper.groupby('u').median()
        #convert to dict
        return kgper.to_dict()

    # --- the item arm ------------------------------------------------------
    if item_col not in (v.index.names or []):
        raise KeyError(
            f"conversion_to_kgs(item_col={item_col!r}): no such index level; "
            f"have {list(v.index.names)}")

    wave = [n for n in index if n in _WAVE_LEVELS]
    base_keys = wave + [item_col]

    # ``median(axis=1)`` BEFORE the groupby rather than after it: the item arm
    # needs the per-ROW price per kilogram (for the dispersion gate), and with
    # the single ``price`` column every caller uses the two orders agree.
    ppk = v[price].divide(v['Kgs'], axis=0).median(axis=1).replace(
        [np.inf, -np.inf], np.nan)
    # ``dropna=False`` -- pandas would otherwise DELETE an NA-keyed group
    # (CLAUDE.md, "Grain Collapse" 3b) and under-report the support behind
    # the cells that remain.  NA keys are dropped from the DELIVERED map
    # below instead, so such a row falls to the unit rung rather than being
    # served a factor pooled over "no item".
    gb = ppk.groupby([_level_series(v, k) for k in base_keys], dropna=False)
    base = pd.DataFrame({'median': gb.median(), 'n': gb.count(),
                         'hi': gb.max(), 'lo': gb.min()})
    base.index.names = base_keys
    spread = base['hi'] / base['lo']
    strict = base['n'] >= min_baseline
    if baseline_max_spread is not None:
        # OFF BY DEFAULT and measured before it is proposed (GH #850 red team,
        # 2026-09-10).  The strict rung has no dispersion gate at all, so a
        # 4-report cell whose reports differ by 26% is refused while a
        # 5-report cell whose reports differ by nine million times is admitted
        # without comment -- and the ungated rung governs 35-98% of rows while
        # the tolerance the design argued over governs 0-0.7.
        #
        # The statistic switches with N for the reason the tight gate does
        # not: with 10 or more reports a robust interdecile spread is
        # available and a single wild report should not condemn the cell,
        # while below 10 there are not enough points for a decile and the
        # extremes are the question.  Quantiles are computed ONLY when the
        # gate is on, so the default path pays nothing.
        lo10 = gb.quantile(0.10)
        hi90 = gb.quantile(0.90)
        lo10.index.names = hi90.index.names = base_keys
        wide = pd.Series(np.where(base['n'] >= 10, hi90 / lo10, spread),
                         index=base.index)
        strict = strict & (wide <= baseline_max_spread)
    # The tight rung is NOT re-gated: ``tight_tolerance`` (1.25) is stricter
    # than any *baseline_max_spread* worth proposing, so the extra condition
    # would be a no-op wearing a second name.
    tight = ((base['n'] >= min_baseline_tight) & (base['n'] < min_baseline)
             & (spread <= tight_tolerance))
    pkg = base['median'].where(strict | tight)

    up = v_infer[price].divide(v_infer[quantity], axis=0).median(
        axis=1).replace([np.inf, -np.inf], np.nan)
    g = up.groupby([_level_series(v_infer, k) for k in base_keys + ['u']],
                   dropna=False)
    po, n = g.median(), g.count()
    po.index.names = n.index.names = base_keys + ['u']
    # Explicit positional alignment rather than pandas's leading-level
    # broadcast, so the estimator does not depend on that behaviour.
    ref = pkg.reindex(po.index.droplevel('u')).to_numpy()
    est = (po / ref).replace([np.inf, -np.inf], np.nan)
    keep = est.notna().to_numpy()
    est, n = est[keep], n[keep]

    # *min_reports* gates the PER-WAVE estimate, which is what its docstring
    # has always said and what the crop side's ``_survey_median_factors``
    # does.  Until the GH #850 red team measured it, the screen was applied to
    # the support SUMMED over waves -- so one report per wave in five waves
    # cleared a floor that four reports in a single wave did not.  Filtering
    # here, before the cross-wave median, means a wave that cannot support an
    # estimate contributes none, rather than lending its row count to waves
    # that can.
    qualifies = (n >= min_reports).to_numpy()
    est, n = est[qualifies], n[qualifies]
    was_tight = (tight.reindex(est.index.droplevel('u'))
                 == True).to_numpy(dtype=bool)  # noqa: E712

    ju = [item_col, 'u']
    grp = est.groupby(ju)
    # D6 disclosure.  The delivered factor is a median ACROSS WAVES, and
    # nothing used to say across how many or how far apart -- so a cell
    # estimated in one of the two waves it serves, and a cell whose two
    # estimates differ threefold, were indistinguishable from a cell measured
    # the same way twice.  ``support`` answers "how many reports"; these two
    # answer "did the waves agree", which is the other half of the same
    # question and the half ``baseline_tight`` set the precedent for.
    detail = pd.DataFrame({'kg_per_unit': grp.median(),
                           'support': n.groupby(ju).sum(),
                           'n_waves': grp.size(),
                           'wave_spread': grp.max() / grp.min()})
    strict_est = est[~was_tight]
    if len(strict_est):
        strict_med = strict_est.groupby(ju).median()
        strict_ok = _valid_factor(strict_med) > 0
        strict_keys = strict_med.index[strict_ok]
    else:
        strict_keys = detail.index[:0]
    detail = detail[np.isfinite(detail['kg_per_unit'])
                    & (detail['kg_per_unit'] > 0)]
    # A factor whose evidence includes NO strict baseline exists ONLY because
    # of the dispersion-gated exception, which is the question a consumer is
    # actually asking ("would this row have fallen to the unit rung?").  The
    # looser reading -- "some contributing baseline was tight" -- was measured
    # too and flags 1.5x to 2.5x as many cells for a weaker claim.
    detail['baseline_tight'] = ~detail.index.isin(strict_keys)
    # An NA item or an NA unit is not an item or a unit: drop those keys so
    # the rows they came from fall to the coarser rung.
    ok = pd.notna(detail.index.get_level_values(item_col)) & pd.notna(
        detail.index.get_level_values('u'))
    detail = detail[ok]
    if _detail:
        return detail
    return detail['kg_per_unit'].to_dict()


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

# Unit labels that STATE their own metric content, mapped to kilograms per
# one such unit.  Matched against the lower-cased ``u`` label, exactly.
#
# THESE ARE READ, NEVER INFERRED.  A label in here never reaches the
# price-ratio inference, so every spelling missing from this dict is a label
# whose kilograms the library invents when the questionnaire already stated
# them.  That is not hypothetical: before GH #850 the corpus's ``Millilitre``
# rows were served at 0.743 kg in Malawi and 0.266 kg in GhanaLSS against a
# true 0.001, ``Grams`` at 0.706 kg, and Mali's ``Gramme`` at 0.708 kg --
# 708x -- because the plural, the French spelling and the ``milli`` prefix
# were all absent (measured in ``slurm_logs/gh850_design/DESIGN.org``,
# "Ground truth A").
#
# The plural / French / abbreviated spellings below were added in GH #850 and
# are NOT a general licence to grow this dict by guessing: each one is a label
# a country in the corpus actually mints.  A spelling nobody uses costs a line
# here and buys nothing; a spelling somebody uses and is missing costs an
# invented weight.
KNOWN_METRIC = {
    'kg': 1, 'kilogram': 1, 'kilogramme': 1,
    'kgs': 1, 'kilograms': 1, 'kilogrammes': 1, 'kilo': 1, 'kilos': 1,
    'g': 1/1000, 'gram': 1/1000, 'gramm': 1/1000,
    'grams': 1/1000, 'gramme': 1/1000, 'grammes': 1/1000,
    'gm': 1/1000, 'gms': 1/1000,
    'milligram': 1e-6,
    'l': 1, 'litre': 1, 'liter': 1, 'litres': 1, 'liters': 1,
    'ml': 1/1000, 'cl': 1/100,
    'millilitre': 1/1000, 'milliliter': 1/1000,
    'millilitres': 1/1000, 'milliliters': 1/1000, 'mili liter': 1/1000,
    'pound': 0.453592, 'lbs': 0.453592,
}

# Subset of ``KNOWN_METRIC`` whose factors only hold under the implicit
# ``1 litre = 1 kg`` assumption (specific-gravity-1 approximation).
# Stripped from the factor map when ``volume_as_mass=False`` is requested
# at the public API.
#
# EVERY volume spelling added to ``KNOWN_METRIC`` must be added here too, or
# ``volume_as_mass=False`` silently keeps asserting 1 L = 1 kg for the new
# spelling while declining it for the old one.  ``tests/test_volume_as_mass_kwarg.py``
# pins the correspondence.
_FLUID_UNITS = ('l', 'litre', 'liter', 'litres', 'liters',
                'ml', 'cl',
                'millilitre', 'milliliter', 'millilitres', 'milliliters',
                'mili liter')

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
#
# THE WORD BOUNDARY IS WHERE THIS WENT WRONG (GH #850).  ``\b`` after a
# literal ``kg`` or ``l`` does not merely permit the plural -- it FORBIDS it,
# so every one of Uganda's own container labels spelled with the plural
# declined to match and was sent to the price-ratio inference instead:
#
#   ``Sack (50 kgs)``       -> None, and served as 4.4 kg
#   ``Tin (Debe) (20 lts)`` -> None, and served as 3.3 kg
#   ``Cup/Mug(0.5lt)``      -> None, and served as 1.0 kg
#
# while ``Basket (10 kg)`` and ``Bottle(750ml)`` matched all along.  The
# repair is to spell the plural / abbreviated tokens INSIDE the alternation
# rather than to relax the boundary: ``\b`` is what stops ``20gallon``
# matching the ``g`` pattern, and dropping it would trade one silent error
# for another.  Longest alternative first within each group, so ``ltr`` is
# preferred to ``lt`` and ``kilogramme`` to ``kg``.
_EXPLICIT_METRIC_PATTERNS = (
    (re.compile(r'(\d+(?:\.\d+)?)\s*(?:kilogrammes?|kilograms?|kgs?)\b',
                re.IGNORECASE), 1, False),
    (re.compile(r'(\d+(?:\.\d+)?)\s*(?:grammes?|grams?|gms?|grs?|g)\b',
                re.IGNORECASE), 1/1000, False),
    (re.compile(r'(\d+(?:\.\d+)?)\s*(?:lbs?|pounds?)\b',
                re.IGNORECASE), 0.453592, False),
    (re.compile(r'(\d+(?:\.\d+)?)\s*ml\b',
                re.IGNORECASE), 1/1000, True),
    (re.compile(r'(\d+(?:\.\d+)?)\s*cl\b',
                re.IGNORECASE), 1/100, True),
    (re.compile(r'(\d+(?:\.\d+)?)\s*(?:litres?|liters?|ltrs?|lts?|l)\b',
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

    The plural / abbreviated spellings Uganda's own container labels use
    (GH #850); each of these returned ``None`` before that fix.

    >>> _parse_explicit_metric('Sack (50 kgs)')
    50.0
    >>> _parse_explicit_metric('Tin (Debe) (20 lts)')
    20.0
    >>> _parse_explicit_metric('Cup/Mug(0.5lt)')
    0.5
    >>> _parse_explicit_metric('Jerrican (5 ltrs)')
    5.0
    >>> _parse_explicit_metric('Tin (Debe) (20 lts)', volume_as_mass=False) is None
    True
    >>> _parse_explicit_metric('20gallon drum') is None
    True
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


def _seeded_kg_factors(df, *, volume_as_mass=True):
    """The kg factors that are READ rather than inferred.

    :data:`KNOWN_METRIC` plus whatever :func:`_parse_explicit_metric` can
    take off the frame's own ``u`` labels.  Split out of
    :func:`_get_kg_factors` for GH #850 so :func:`food_kg_factors` can seed
    the same way without also running the u-keyed price-ratio inference.

    Keys are lower-cased, matching the lookup in
    :func:`_apply_kg_conversion`.  ``KNOWN_METRIC`` wins over the parser --
    those are exact-match tokens that should not be re-derived from a
    substring.
    """
    factors = dict(KNOWN_METRIC)
    if not volume_as_mass:
        for u in _FLUID_UNITS:
            factors.pop(u, None)

    if 'u' in (df.index.names or []):
        labels = df.index.get_level_values('u').dropna().unique()
    elif 'u' in df.columns:
        labels = df['u'].dropna().unique()
    else:
        labels = ()
    for u in labels:
        key = str(u).lower()
        if key in factors:
            continue
        kg = _parse_explicit_metric(str(u), volume_as_mass=volume_as_mass)
        if kg is not None and np.isfinite(kg) and kg > 0:
            factors[key] = kg
    return factors


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
    factors = _seeded_kg_factors(df, volume_as_mass=volume_as_mass)

    # Infer additional factors from price ratios where possible
    if 'Expenditure' in df.columns and 'Quantity' in df.columns:
        # Determine which index levels are available for grouping
        idx_names = list(df.index.names)
        group_levels = [n for n in ['t', 'm', 'i'] if n in idx_names]
        if group_levels:
            try:
                inferred = conversion_to_kgs(df, index=group_levels)
                # Inferred factors fill in where known metric doesn't cover.
                # Currency-denominated labels are already dropped inside
                # ``conversion_to_kgs``; re-checking here is belt and braces
                # against a future caller of that function with different
                # arguments, or a future inference path.
                #
                # SCOPE, stated exactly (review NIT 1): this guard is on the
                # INFERENCE only.  ``factors`` also gets entries from
                # ``KNOWN_METRIC`` and from the explicit-metric label parser
                # above, and neither is checked -- a label that both named a
                # currency and named metric content (``"Value (500g)"``)
                # would still be given 0.5 by the parser.  No such label
                # exists in the corpus, so this is a documented gap, not a
                # live defect; do not read the guard as "the key can never
                # come back".
                #
                # ``inferred`` is keyed on the RAW label, so case variants of
                # one unit arrive as two entries and the first merged wins.
                # Report that rather than resolving it silently: see
                # :class:`UnitLabelCollisionWarning` for why merging would be
                # a data change.  ``won`` records the label actually used.
                seeded = frozenset(factors)   # KNOWN_METRIC + label parser
                won: dict[str, tuple[str, float]] = {}
                collisions: dict[str, list[tuple[str, float]]] = {}
                for unit, factor in inferred.items():
                    key = unit.lower()
                    if key in _CURRENCY_DENOMINATED_UNITS:
                        continue
                    if not (np.isfinite(factor) and factor > 0):
                        continue
                    # Only a key the INFERENCE decides can be a collision:
                    # if KNOWN_METRIC or the explicit-metric parser already
                    # seeded it, neither inferred value is used and the
                    # disagreement is inert (every corpus ``kg`` collision is
                    # this case).  Reporting those would be noise.
                    decided_here = key not in seeded
                    if decided_here and key in won and won[key][1] != factor:
                        collisions.setdefault(key, [won[key]]).append(
                            (unit, float(factor)))
                    if key not in factors:
                        factors[key] = factor
                    if decided_here:
                        won.setdefault(key, (unit, float(factor)))
                if collisions:
                    detail = '; '.join(
                        f"{k!r}: " + ', '.join(f"{lbl!r}->{f:.6g}"
                                               for lbl, f in v)
                        + f" (using {won[k][0]!r})"
                        for k, v in sorted(collisions.items()))
                    warnings.warn(
                        f"_get_kg_factors: {len(collisions)} unit label(s) "
                        f"differ only in CASE but were inferred DIFFERENT kg "
                        f"factors; the lower-cased lookup keeps one "
                        f"arbitrarily.  {detail}.  Fix this in the country's "
                        f"unit vocabulary by canonicalising the spelling "
                        f"where the label is minted -- do not rely on which "
                        f"one wins here.",
                        UnitLabelCollisionWarning,
                        stacklevel=2,
                    )
            except (ValueError, ZeroDivisionError, KeyError):
                # Inference is best-effort; numeric / lookup failure means
                # we proceed with the known-metric factors only.  Programmer
                # bugs (TypeError, AttributeError) propagate.
                pass

    return factors


#: The layers :func:`food_kg_factors` resolves a row's kilograms through, in
#: precedence order.  ``none`` is a layer, not a failure: a row it serves
#: carries its NATIVE quantity into ``food_quantities(units='kgs')`` and is
#: COUNTED rather than silently absent.
#:
#: A SIBLING OF :data:`KG_FACTOR_LAYERS`, NOT A REPLACEMENT.  The crop
#: ladder's layers mean what they have always meant and must not be reordered,
#: renamed or merged with these; the two tuples answer the same question about
#: two different tables, and a row's provenance is only interpretable against
#: the ladder that produced it.
#:
#: - ``survey_kg``  -- the survey supplied this row's kilograms outright
#:   (``Quantity_kg``, GH #378).  It beats every inference, and always has.
#: - ``metric``     -- the LABEL states the kilograms: :data:`KNOWN_METRIC`,
#:   or :func:`_parse_explicit_metric` reading "50 kg Bag" off the label.
#: - ``item_unit``  -- the price-ratio inference for this row's OWN
#:   ``(item, unit)`` cell.
#: - ``item_unit_tight`` -- the same, but the cell exists only because of the
#:   dispersion-gated baseline exception (:data:`FOOD_KG_TIGHT_TOLERANCE`).
#:   Separated because it is thinner evidence and a consumer is entitled to
#:   drop it without dropping the rung.
#: - ``unit``       -- the fallback: the u-keyed inference, pooled over every
#:   item sharing the label.  This is what the library served for EVERY row
#:   before GH #850, and between a fifth and a half of the inferred rows still
#:   land here, which is the argument for reporting it.
#: - ``none``       -- no factor.
FOOD_KG_FACTOR_LAYERS = ('survey_kg', 'metric', 'item_unit',
                         'item_unit_tight', 'unit', 'none')


def food_kg_factors(df, *, volume_as_mass=True, item_col='j',
                    min_reports=FOOD_KG_MIN_REPORTS,
                    min_baseline=FOOD_KG_MIN_BASELINE,
                    min_baseline_tight=FOOD_KG_MIN_BASELINE_TIGHT,
                    tight_tolerance=FOOD_KG_TIGHT_TOLERANCE,
                    baseline_max_spread=None):
    """Per-ROW kg-per-unit for a ``food_acquired`` frame, with provenance.

    The food twin of :func:`harvest_kg_factors`, and deliberately the same
    shape: one row per input row, a resolved ``kg_per_unit``, a
    ``KgFactorSource`` drawn from :data:`FOOD_KG_FACTOR_LAYERS`, the
    per-layer candidates beside it, and counts in
    ``attrs['kg_factor_sources']`` that PARTITION the frame.

    Why per-row at all (GH #850): once the inference carries an item axis, a
    factor is no longer a property of the unit label, so there is no dict to
    return.  And the ladder is now worth reporting -- a row served by its own
    ``(j, u)`` cell and a row served by the u-pooled fallback were
    indistinguishable before, and after this change they differ by more than
    they did before it.

    Parameters
    ----------
    df : pandas.DataFrame
        ``food_acquired``-shaped, with ``u`` in the index and ``Quantity`` /
        ``Expenditure`` columns.  Without ``Expenditure`` there is nothing to
        take a price ratio of, and only the ``survey_kg`` / ``metric`` layers
        can serve a row.
    item_col : str, default ``'j'``
        The item level.  Absent from the frame -> the item rungs serve
        nothing and every inferred row falls to ``unit``; the layers still
        partition.
    volume_as_mass, min_reports, min_baseline, min_baseline_tight, tight_tolerance
        Passed through; see :func:`conversion_to_kgs`.

    Returns
    -------
    pandas.DataFrame
        Indexed like *df*, with columns ``kg_per_unit``, ``KgFactorSource``,
        ``kg_survey``, ``kg_metric``, ``kg_item_unit`` and ``kg_unit``.
        ``attrs['kg_factor_sources']`` maps each layer to its row count.

    Notes
    -----
    WHAT THIS DOES NOT FIX, stated where a user will meet it.  The only
    external answer key the corpus has for a food unit is Mali's own
    questionnaire, and against it the corrected inference is BETTER and still
    WRONG: for a stated 100 / 50 / 25 kg sack it serves a median 50.0 / 34.4 /
    14.1 kg per row (0.50x / 0.69x / 0.56x of truth, up from 0.33x / 0.52x /
    0.21x).  ``Gramme`` is now exact, but only because GH #850 taught the
    label parser to READ it -- no inference was involved.  Mali's
    ``CONTENTS.org`` advice ("use native units, or ``u = 'Kg'``") therefore
    survives this change.  A price-ratio inference cannot be argued into
    being a conversion table; where a country ships one, use it (the crop
    side's ``shipped`` layer, GH #852/#854).

    ``kg_per_unit`` on a ``survey_kg`` row is the factor IMPLIED by the
    survey's own kilograms, ``Quantity_kg / Quantity``, so that
    ``Quantity * kg_per_unit`` reproduces the delivered kilograms on every
    row rather than on all but those.  Where that quotient cannot be formed
    (a missing or zero ``Quantity``) the row keeps the ladder's factor and
    still reports ``survey_kg``: the kilograms DO come from the survey, and
    the factor column is there for the price-per-kg conversion, which would
    otherwise lose a row it can serve.
    """
    idx_names = list(df.index.names or [])
    seeded = _seeded_kg_factors(df, volume_as_mass=volume_as_mass)

    if 'u' in idx_names:
        units = df.index.get_level_values('u').astype(str).str.lower()
    elif 'u' in df.columns:
        units = df['u'].astype(str).str.lower()
    else:
        raise ValueError("food_kg_factors: expected a 'u' unit level or column")
    kg_metric = _valid_factor(pd.Series(np.asarray(units)).map(seeded))

    kg_item = np.full(len(df), np.nan)
    kg_tight = np.zeros(len(df), dtype=bool)
    kg_waves = np.full(len(df), np.nan)
    kg_wave_spread = np.full(len(df), np.nan)
    kg_unit = np.full(len(df), np.nan)
    have_price = ('Expenditure' in df.columns and 'Quantity' in df.columns)
    if have_price:
        group_levels = [n for n in ('t', 'm', 'i') if n in idx_names]
        if group_levels:
            try:
                per_u = conversion_to_kgs(df, index=group_levels)
            except (ValueError, ZeroDivisionError, KeyError):
                per_u = {}
            # The u rung is keyed on the RAW label, exactly as
            # ``_get_kg_factors`` consumes it; lower-case here so a case
            # variant does not silently miss.
            lowered = {}
            for lbl, f in per_u.items():
                if np.isfinite(f) and f > 0:
                    lowered.setdefault(str(lbl).lower(), float(f))
            kg_unit = _valid_factor(pd.Series(np.asarray(units)).map(lowered))

            if item_col in idx_names or item_col in df.columns:
                try:
                    detail = conversion_to_kgs(
                        df, index=group_levels, item_col=item_col,
                        min_reports=min_reports, min_baseline=min_baseline,
                        min_baseline_tight=min_baseline_tight,
                        tight_tolerance=tight_tolerance,
                        baseline_max_spread=baseline_max_spread,
                        _detail=True)
                except (ValueError, ZeroDivisionError, KeyError):
                    detail = None
                if detail is not None and len(detail):
                    items = (df.index.get_level_values(item_col)
                             if item_col in idx_names else df[item_col])
                    raw_u = (df.index.get_level_values('u')
                             if 'u' in idx_names else df['u'])
                    key = pd.MultiIndex.from_arrays(
                        [_normalise_join_key(items),
                         _normalise_join_key(raw_u)])
                    ref = detail.copy()
                    ref.index = pd.MultiIndex.from_arrays(
                        [_normalise_join_key(
                            ref.index.get_level_values(item_col)),
                         _normalise_join_key(ref.index.get_level_values('u'))])
                    # Two raw labels differing only in case normalise onto one
                    # key; keep the first, exactly as the lower-cased lookup
                    # in ``_get_kg_factors`` does (which already reports the
                    # clash -- see :class:`UnitLabelCollisionWarning`; a
                    # second warning here would be the same news twice).
                    ref = ref[~ref.index.duplicated()]
                    hit = ref.reindex(key)
                    kg_item = _valid_factor(hit['kg_per_unit'])
                    kg_tight = (hit['baseline_tight'] == True).to_numpy(  # noqa: E712
                        dtype=bool)
                    kg_waves = _as_float(hit['n_waves'])
                    kg_wave_spread = _as_float(hit['wave_spread'])

    kg_survey = np.full(len(df), np.nan)
    from_survey = np.zeros(len(df), dtype=bool)
    if 'Quantity_kg' in df.columns:
        qkg = _as_float(df['Quantity_kg'])
        from_survey = np.isfinite(qkg)
        with np.errstate(divide='ignore', invalid='ignore'):
            kg_survey = _valid_factor(qkg / _as_float(df['Quantity']))

    item_ok = ~np.isnan(kg_item)
    resolved = np.where(
        ~np.isnan(kg_survey), kg_survey,
        np.where(~np.isnan(kg_metric), kg_metric,
                 np.where(item_ok, kg_item, kg_unit)))
    source = np.where(
        from_survey, 'survey_kg',
        np.where(~np.isnan(kg_metric), 'metric',
                 np.where(item_ok & ~kg_tight, 'item_unit',
                          np.where(item_ok, 'item_unit_tight',
                                   np.where(~np.isnan(kg_unit), 'unit',
                                            'none')))))

    out = pd.DataFrame({'kg_per_unit': resolved,
                        'KgFactorSource': source,
                        'kg_survey': kg_survey,
                        'kg_metric': kg_metric,
                        'kg_item_unit': kg_item,
                        'kg_item_n_waves': kg_waves,
                        'kg_item_wave_spread': kg_wave_spread,
                        'kg_unit': kg_unit},
                       index=df.index)
    out.attrs['kg_factor_sources'] = {
        layer: int((source == layer).sum()) for layer in FOOD_KG_FACTOR_LAYERS}
    # D6's missing half.  The delivered factor is a median ACROSS WAVES, so a
    # row can be served by a cell estimated in one of the waves it covers, or
    # by one whose per-wave estimates disagree threefold -- and until the
    # GH #850 red team asked, nothing in the output said which.  Counted over
    # the rows the item rungs actually SERVE, because a spread on a cell that
    # lost the rank is not a fact about any delivered number.
    served_item = np.isin(source, ('item_unit', 'item_unit_tight'))
    wide = served_item & (kg_wave_spread > FOOD_KG_WAVE_SPREAD_REPORT)
    out.attrs['kg_factor_wave_spread'] = {
        'threshold': FOOD_KG_WAVE_SPREAD_REPORT,
        'rows_served_by_item_rung': int(served_item.sum()),
        'rows_over_threshold': int(wide.sum()),
        'rows_single_wave': int((served_item & (kg_waves == 1)).sum()),
    }
    return out


def _apply_kg_conversion(df, factors):
    """Convert Quantity to kg using the factors dict.
    Returns a copy with a 'Quantity_kg' column added.

    An *exact, survey-provided* per-row ``Quantity_kg`` (e.g. Nigeria's
    ``s10bq2_cvn``, GH #378 / DESIGN_per_row_kg_quantity) takes precedence
    where it is present and non-null; the unit→factor map only fills the
    rows that lack it.  Carried as a summable quantity (not a factor) because
    the canonical index has no size level -- see the design doc.

    *factors* is either the ``{unit: kg}`` dict this function has always
    taken, or -- since GH #850 -- a PER-ROW Series of kg-per-unit, which is
    what a factor with an item axis has to be.  A Series is consumed
    POSITIONALLY (``to_numpy``), not by label: the canonical food index is
    not unique (a household buys the same item in the same unit from two
    sources in one wave), so label alignment would fan the frame out.  The
    caller must pass a Series built from THIS frame --
    :func:`food_kg_factors` returns one."""
    v = df.copy()
    if 'u' in v.index.names:
        units = v.index.get_level_values('u').astype(str).str.lower()
    else:
        return v

    if isinstance(factors, pd.Series):
        if len(factors) != len(v):
            raise ValueError(
                f"_apply_kg_conversion: per-row factors have {len(factors)} "
                f"rows but the frame has {len(v)}; a Series is applied "
                f"positionally and must be built from this frame")
        per_unit = _as_float(factors)
    else:
        per_unit = pd.to_numeric(pd.Series(units.map(factors)),
                                 errors='coerce').to_numpy(dtype='float64',
                                                           na_value=np.nan)
    # Arithmetic on ndarrays, not on Series: the canonical food index is not
    # unique, and two Series sharing a non-unique index align rather than
    # multiply row by row.
    factor_kg = pd.Series(_as_float(v['Quantity']) * per_unit, index=v.index)
    if 'Quantity_kg' in v.columns:
        # Precomputed exact kg wins; fall back to the factor estimate only
        # where the survey didn't supply one.
        v['Quantity_kg'] = v['Quantity_kg'].where(v['Quantity_kg'].notna(),
                                                   factor_kg)
    else:
        v['Quantity_kg'] = factor_kg
    return v


# ---------------------------------------------------------------------------
# Own-production / in-kind valuation (GH #585).  OPT-IN, read-time, never
# stored.  See ``food_acquired_valued`` for the method and its bias.
# ---------------------------------------------------------------------------

#: Rungs :func:`food_acquired_valued` knows how to run, in the order EPAR's
#: consumption repo runs them (own price first, then the spatial ladder).
VALUATION_RUNGS = ('own_price', 'median_price')

#: Values ``ValuationSource`` can take.  The four partition the input rows.
VALUATION_SOURCES = ('reported', 'own_price', 'median_price', 'none')

#: The geographic ladder the ``Country`` path asks for, finest -> coarsest.
#: ``v`` rides on ``food_acquired``'s own index; the rest come from
#: ``cluster_features``.  A national rung is always appended by
#: :func:`median_price_valuation` itself.
VALUATION_GEO_LEVELS = ('v', 'District', 'Region')


def _normalize_valuation_arg(valuation):
    """Validate ``valuation=`` and return it as a tuple of rung names."""
    if valuation is None:
        return ()
    rungs = (valuation,) if isinstance(valuation, str) else tuple(valuation)
    if not rungs:
        raise ValueError(
            "valuation= must be a rung name, a non-empty sequence of them, "
            "or None; got an empty sequence.  Pass None for no valuation."
        )
    unknown = [r for r in rungs if r not in VALUATION_RUNGS]
    if unknown:
        raise ValueError(
            f"valuation= rung(s) {unknown} unknown; must be drawn from "
            f"{list(VALUATION_RUNGS)}."
        )
    if len(set(rungs)) != len(rungs):
        raise ValueError(f"valuation= names a rung twice: {list(rungs)}.")
    return rungs


def _level_or_column(df, name):
    """``name`` as a numpy array aligned positionally to ``df``'s rows."""
    if name in (df.index.names or []):
        return np.asarray(df.index.get_level_values(name))
    if name in df.columns:
        return df[name].to_numpy()
    return None


def _geo_key_tokens(values):
    """String group keys in which every MISSING value is UNIQUE.

    ``median_price_valuation`` stringifies its group keys, so a null cluster id
    would otherwise collapse to the literal ``'nan'`` and -- at ``threshold``
    rows or more -- qualify as a geographic cell that does not exist (Uganda
    carries 328 such ``food_acquired`` rows, 0.09%).  Giving each null its own
    token caps its cell count at 1, so it can never clear any
    ``threshold >= 2``; the row still receives the national price, exactly as a
    row with no observed price does.
    """
    arr = pd.Series(values)
    out = arr.astype('object').where(arr.notna())
    missing = out.isna().to_numpy()
    out = out.astype(str).to_numpy(dtype=object)
    if missing.any():
        out[missing] = [f'__nogeo_{k}__' for k in np.flatnonzero(missing)]
    return out.astype(str)


def food_acquired_valued(df, valuation, *, geo=None, threshold=10,
                         value_col='Expenditure', quantity_col='Quantity'):
    """Value ``food_acquired``'s unvalued non-purchased rows, with PROVENANCE.

    METHODOLOGY transform (GH #585).  The item-grain half of
    ``food_expenditures(basis='total', valuation=...)``, exposed on its own so
    the imputation can be AUDITED row by row -- which rung served each row, and
    at what unit price.  Every row of *df* gets a row here, on the same index
    and in the same order.

    **This is opt-in and it is never the default, for a measured reason.**
    Every rung below prices own-consumption at a PURCHASE transaction, and a
    purchase sits above the farm gate by the marketing margin.  Our five-source
    price study measured the gap: in Uganda, own-consumption valued at the
    purchase price sits about **23% above what a sale actually fetched**
    (``slurm_logs/price_sources/SYNTHESIS.org:750-753``; the transitive
    223-cell set, ratio 1.234).  That number is **Uganda-only** -- GhanaLSS
    ships no sale-price source, so no comparable figure exists for it.  The
    UNPS interviewer manual is explicit that column 9 "should be valued at farm
    gate/producer price ... excludes any cost transport and marketing services"
    (``SYNTHESIS.org:732-736``), so a consumption aggregate built this way
    carries part of a margin the instrument intended to exclude.  Deaton &
    Zaidi (2002, *Guidelines for Constructing Consumption Aggregates*, LSMS
    WP 135) treat the producer/market choice as the two defensible readings and
    warn that the market one inflates the aggregate relative to purchases.
    Choose it deliberately; do not reach for it because ``basis='total'``
    looked incomplete.

    Rungs, in precedence order (``ValuationSource`` records which one served)

    ``reported``
        The row already carries a non-zero ``value_col``.  Never re-valued --
        including a value the survey itself imputed for ``s='inkind'``
        (``lsms_library/data_info.yml:602``).  Zero counts as missing, exactly
        as :func:`food_expenditures_from_acquired`'s own ``replace(0, nan)``
        does, so a zero-valued produced row IS a candidate.
    ``own_price``
        **The same household's own purchase unit value for the same
        ``(t, j, u)``**: the sum of that household's purchased ``value_col``
        over the sum of its purchased ``quantity_col``, in that wave, for that
        item, in that unit.  No cross-household information whatever.  This is
        EPAR's consumption repo's FIRST rung (``EthiopiaW5_...do:423``,
        ``UgandaW4_...do:693``), conditional there too on the consumed unit
        matching the purchased one.  Note that EPAR's *Ag* repo does the
        opposite -- it keeps the household's own price as a parallel
        ``value_harvest_hh`` series rather than a rung -- so "EPAR's ladder" is
        ambiguous and this docstring says which (``LEARNINGS.org`` L5 item 4).
    ``median_price``
        The geographic median-price ladder, run by :func:`median_price_valuation`
        on a price pool built **only from ``s == 'purchased'`` rows**
        (``value_col / quantity_col``).  The ladder walks *geo* finest ->
        coarsest and takes the median of the finest cell with at least
        *threshold* priced observations, with an unconditional national
        fallback.  Cells are ``(geo, t, j, u)``: ``t`` is in the item key
        because ``median_price_valuation`` has no wave axis of its own, and
        without it the national rung would pool currency across a decade.
    ``none``
        No rung produced a price.  Counted, never hidden.  Also covers a
        ``purchased`` row with no recorded outlay, which is a data defect
        rather than an unvalued acquisition and is deliberately not a
        candidate.

    Parameters
    ----------
    df : pd.DataFrame
        ``food_acquired``, canonical index ``(t, v, i, j, u, s)``
        (``data_info.yml:51``); ``v`` optional.  Needs ``s`` as an index level
        or column -- without an acquisition source there is nothing to value.
    valuation : str or sequence of str
        Rungs to run, IN THE ORDER GIVEN; each fills only rows still unvalued.
        A scalar means **exactly that rung** -- ``'median_price'`` runs the
        spatial ladder alone, it does NOT quietly run ``'own_price'`` first.
        The composed form ``('own_price', 'median_price')`` is EPAR's
        consumption-repo ordering and is what you want if you want theirs.
    geo : pd.DataFrame, optional
        Coarser geographic rungs, indexed by ``(t, v)`` -- i.e.
        ``cluster_features``' own grain -- with columns ordered **finest ->
        coarsest** (e.g. ``[['District', 'Region']]``).  The ``v`` level of
        *df*'s own index, when present, is prepended as the finest rung.  With
        ``geo=None`` the ladder is ``v`` (if present) then national.
    threshold : int, default 10
        Minimum priced observations for a geographic cell's median to be
        adopted; forwarded to :func:`median_price_valuation` (the WB / EPAR-Ag
        gate of >= 10).
    value_col, quantity_col : str
        Column names on *df*.

    Returns
    -------
    pd.DataFrame
        Indexed like *df*, with

        ``Expenditure`` (i.e. *value_col*)
            the reported value where there was one, the imputed value
            otherwise, NaN where no rung could serve.
        ``ValuationSource``
            one of :data:`VALUATION_SOURCES`.
        ``valuation_price``
            the unit price the winning rung used, per the row's own ``u``
            (NaN for ``reported`` and ``none``).  Deliberately NOT called
            ``Price``: that name is the survey's own reported unit price, and
            this one is CONSTRUCTED.

        Two records ride on ``.attrs``:

        ``valuation_sources``
            ``{source: n_rows}`` over the INPUT rows.  The four
            :data:`VALUATION_SOURCES` partition the frame and sum to
            ``len(df)``.  The extra ``candidates`` key counts the rows that
            were ELIGIBLE for valuation (non-purchased, no reported value) and
            is deliberately outside that partition, since such a row is still
            served by one of the four.  POOLED on a cross-country
            :class:`~lsms_library.feature.Feature` frame -- read it as a total,
            never as coverage.
        ``valuation``
            the tuple of rungs actually run, and ``valuation_geo_levels`` the
            ladder they ran on.

    Notes
    -----
    **The price basis is the row's NATIVE unit, not kilograms.**  ``u`` is an
    item key, so a produced row is valued at the purchase price of the very
    label it was reported in and unit alignment is exact by construction.  A
    kg-normalised basis would pool more finely-split labels (Malawi carries
    ``Kilogramme``, ``Kilogram``, ``Kg`` and ``kg`` as four labels in one wave)
    but would inherit the open kg-inference defect of GH #850, and it buys
    nothing measurable: national-rung coverage at ``threshold=10`` is 94.2%
    (Malawi) / 76.5% (Uganda) on the native key, and lower-casing ``u`` moves
    it 0.00pp in both.  A ``u='Value'`` row prices at 1 currency-unit per
    currency-unit, which is the right answer for it.

    **Read that 94.2% as COVERAGE, not as accuracy, and read the
    value-weighted complement beside it.**  ``median_price_valuation``'s
    national rung is an *unconditional* fallback, so ``threshold`` gates the
    geographic cells and gates nothing at the top of the ladder.  Measured on
    Malawi (red-team, 2026-09-09), by share of the imputed MONEY rather than of
    the rows: **6.48% of the imputed value is priced off fewer than ten
    purchase observations nationwide, and 2.85% off exactly one.**  The 5.8% of
    candidate rows below the gate are not a random 5.8% of the money.  The
    concrete case: a single 2016-17 purchase sets "Small Animal - Rabbit, Mice,
    Etc." (``u='Whole'``) at 100,000 MWK, which is then applied to 195 rows and
    contributes 2.12% of Malawi's entire delta.  The median pool is 533
    observations, so the bulk is well supported -- but a thin ``(t, j, u)``
    cell is thin for every household in it, so this tail is *systematic* rather
    than an outlier.  It is COUNTED here, never clipped; ``valuation_price`` is
    returned per row so a caller can gate on it.

    Whether the national rung should carry a gate of its own is a **follow-up,
    deliberately not decided here**.  @ligon has ruled on the parallel
    food-side inference (GH #850) that a thin baseline is a floor of 5 with a
    dispersion-gated exception at 3-4; adopting the same rule at this rung is
    the obvious candidate, and it would change returned numbers, so it belongs
    in its own change.

    Nothing here is cached.  ``food_expenditures`` is derived at read time
    (``Country._FOOD_DERIVED``) and this runs inside that derivation, so no
    parquet ever holds an imputed value.
    """
    rungs = _normalize_valuation_arg(valuation)
    if not rungs:
        raise ValueError("food_acquired_valued: valuation= is required; "
                         "pass a rung name or a sequence of them.")

    df = _normalize_columns(df)
    for col in (value_col, quantity_col):
        if col not in df.columns:
            raise ValueError(f"food_acquired must have a {col!r} column to "
                             f"value own-production rows")

    s = _level_or_column(df, 's')
    if s is None:
        raise ValueError(
            "food_acquired has no 's' (acquisition source) level, so there is "
            "no own-production row to value.  Call with valuation=None."
        )
    s = pd.Series(s).astype(str).to_numpy()

    n = len(df)
    value = pd.to_numeric(df[value_col], errors='coerce').to_numpy(
        dtype='float64', na_value=np.nan)
    value = np.where(value == 0, np.nan, value)
    qty = pd.to_numeric(df[quantity_col], errors='coerce').to_numpy(
        dtype='float64', na_value=np.nan)

    purchased = (s == 'purchased')
    reported = ~np.isnan(value)
    # A candidate is a NON-purchased acquisition carrying no value.  A
    # purchased row with no recorded outlay is a defect, not an unvalued
    # acquisition, and is deliberately excluded (see the ``none`` rung).
    candidate = (~purchased) & (~reported)

    # The purchase-side price pool: rows that are a purchase AND carry both a
    # positive value and a positive quantity.
    pool = purchased & (~np.isnan(value)) & (qty > 0)

    t = pd.Series(_level_or_column(df, 't')).astype(str).to_numpy()
    j = pd.Series(_level_or_column(df, 'j')).astype(str).to_numpy()
    u_raw = _level_or_column(df, 'u')
    if u_raw is None:
        raise ValueError("food_acquired has no 'u' level; the purchase price "
                         "the valuation uses is per unit, so 'u' is required.")
    u = pd.Series(u_raw).astype(str).to_numpy()

    price = np.full(n, np.nan)
    source = np.where(reported, 'reported', 'none').astype(object)

    geo_levels_used = ()
    # IN THE ORDER GIVEN -- each rung fills only rows still unvalued, so the
    # order is the precedence.  `('own_price', 'median_price')` is EPAR's
    # consumption-repo ordering; the reverse is a different construct, not a
    # spelling of the same one.
    for rung in rungs:
        if rung == 'own_price':
            offered = _own_price_rung(t, df, j, u, value, qty, pool)
        else:
            offered, geo_levels_used = _median_price_rung(
                df, t, j, u, value, qty, pool, geo=geo, threshold=threshold)
        take = candidate & np.isnan(price) & (~np.isnan(offered)) & (qty > 0)
        price = np.where(take, offered, price)
        source = np.where(take, rung, source)

    valued = np.where(np.isnan(price), value, price * qty)

    out = pd.DataFrame({value_col: valued,
                        'ValuationSource': source.astype(str),
                        'valuation_price': price},
                       index=df.index)
    counts = {src: int((out['ValuationSource'].to_numpy() == src).sum())
              for src in VALUATION_SOURCES}
    # NOT a fifth source and deliberately not part of the partition: a
    # candidate row is still SERVED by one of the four.
    counts['candidates'] = int(candidate.sum())
    out.attrs['valuation_sources'] = counts
    out.attrs['valuation'] = rungs
    out.attrs['valuation_geo_levels'] = list(geo_levels_used)
    return out


def _own_price_rung(t, df, j, u, value, qty, pool):
    """Unit price from the SAME household's purchases of the same (t, j, u).

    ``sum(value) / sum(quantity)`` over that household's pooled purchase rows,
    so a household that bought an item twice contributes one quantity-weighted
    price rather than two.  Returns NaN where the household made no usable
    purchase of that item in that unit in that wave -- no cross-household
    information is ever consulted.

    ``i`` is resolved as an index level OR a column, like every other key, and
    its absence RAISES.  It used to be looked up on ``df.index`` alone, so a
    frame carrying ``i`` as a column got an all-NaN offer and ``own_price: 0``
    with no signal at all -- while a missing ``u`` or ``s`` raised.  Only the
    household axis degraded quietly, which is the one place a silent zero is
    indistinguishable from an honest "no household ever bought what it grew".
    """
    i = _level_or_column(df, 'i')
    if i is None:
        raise ValueError(
            "food_acquired has no 'i' (household) level or column, so the "
            "own_price rung -- which is defined as the SAME household's "
            "purchase price -- has no household to look up.  Use "
            "valuation='median_price' on a frame with no household axis."
        )
    i = pd.Series(i).astype(str).to_numpy()
    keys = ['_t', '_i', '_j', '_u']
    work = pd.DataFrame({'_t': t, '_i': i, '_j': j, '_u': u,
                         '_v': value, '_q': qty})
    agg = work[pool].groupby(keys, sort=False, dropna=False)[['_v', '_q']].sum()
    with np.errstate(divide='ignore', invalid='ignore'):
        p = agg['_v'] / agg['_q']
    p = p.replace([np.inf, -np.inf], np.nan)
    p = p.where(p > 0)
    lookup = pd.MultiIndex.from_frame(work[keys])
    return p.reindex(lookup).to_numpy(dtype='float64')


def _median_price_rung(df, t, j, u, value, qty, pool, *, geo, threshold):
    """The geography median-price ladder, via :func:`median_price_valuation`.

    Builds the frame that function wants on a fresh ``RangeIndex`` (``df``'s
    own index may carry duplicate labels, which its internal ``reindex`` would
    refuse) and maps the imputed price back positionally.  The price pool is
    masked to *pool* -- purchased rows only -- so no already-imputed in-kind
    value can become a "price" and feed itself back in.
    """
    n = len(df)
    work = pd.DataFrame({
        '_pool_value': np.where(pool, value, np.nan),
        '_pool_qty': np.where(pool, qty, np.nan),
        '_qty': qty,
        '_t': t, '_j': j, '_u': u,
    })

    ladder = []
    v = _level_or_column(df, 'v')
    if v is not None:
        work['_g_v'] = _geo_key_tokens(v)
        ladder.append('_g_v')
        used = ['v']
    else:
        used = []
    if geo is not None and len(geo.columns):
        gnames = list(geo.index.names or [])
        if not {'t', 'v'} <= set(gnames):
            raise ValueError(
                "geo= must be indexed by (t, v) -- cluster_features' own "
                f"grain; got index names {gnames}."
            )
        if v is None:
            raise ValueError(
                "geo= was supplied but food_acquired carries no 'v' level, so "
                "the coarser rungs cannot be joined to it."
            )
        g = geo.reorder_levels(['t', 'v'] + [x for x in gnames
                                             if x not in ('t', 'v')])
        if g.index.nlevels > 2:
            g = g.groupby(level=['t', 'v']).first()
        g = g[~g.index.duplicated()]
        target = pd.MultiIndex.from_arrays(
            [pd.Series(t).to_numpy(),
             pd.Series(v).astype('object').to_numpy()], names=['t', 'v'])
        joined = g.reindex(target)
        # A rung that RESOLVES TO NOTHING was not available, whatever
        # ``cluster_features`` had a column for.  Without this count the
        # ladder can silently degrade to ``v`` -> national while
        # ``attrs['valuation_geo_levels']`` still advertises the full one.
        # Two causes, and the warning cannot tell them apart, so it names
        # both: (a) the cluster is absent from the geo frame -- watch for a
        # ``v`` dtype/spelling mismatch, a live hazard here since
        # ``format_id`` is applied to idxvars but not myvars (CLAUDE.md,
        # Gotchas); (b) the cluster IS present and the column is simply null
        # for it.  Measured 2026-09-09: Malawi 100% on both rungs; Uganda
        # 99.73% of ``(t, v)`` present but ``District`` null for 27.8% of
        # 2010-11 and 33.0% of 2011-12 clusters -- i.e. cause (b), a real gap
        # in the country's own cluster_features, not a join bug.
        has_v = pd.Series(v).notna().to_numpy()
        for k, col in enumerate(geo.columns):
            vals = joined[col].to_numpy()
            missed = int((has_v & pd.isna(vals)).sum())
            if missed:
                warnings.warn(
                    f"food_acquired valuation: the {str(col)!r} rung resolved "
                    f"to nothing for {missed:,} of {int(has_v.sum()):,} rows "
                    f"that DO carry a cluster id -- either those clusters are "
                    f"absent from the geo frame (check that the 'v' labels on "
                    f"both sides are the same dtype and spelling) or "
                    f"cluster_features carries a null {str(col)!r} for them.  "
                    f"Those rows fall through to the next rung, so the ladder "
                    f"is effectively shorter for them than "
                    f"attrs['valuation_geo_levels'] says.",
                    ValuationLadderWarning, stacklevel=3)
            work[f'_g{k}'] = _geo_key_tokens(vals)
            ladder.append(f'_g{k}')
            used.append(str(col))

    res = median_price_valuation(
        work, ladder,
        value_col='_pool_value',
        kg_qty=work['_pool_qty'],
        quantity_col='_qty',
        item_keys=('_t', '_j', '_u'),
        threshold=threshold,
        price_col='_vp', out_col='_vv',
    )
    return res['_vp'].to_numpy(dtype='float64'), used


def food_expenditures_from_acquired(df, basis='purchased', *,
                                    valuation=None, geo=None,
                                    threshold=10):
    """Derive food expenditures from food_acquired.

    Returns a DataFrame of expenditure per household × item × period ×
    acquisition source (when ``s`` is present in the input index), summed
    over units.

    Parameters
    ----------
    df : pd.DataFrame
        food_acquired with an ``Expenditure`` column.
    basis : {'purchased', 'total'}, default 'purchased'
        Which acquisition sources contribute to ``Expenditure`` (GH #575):

        - ``'purchased'`` (default): **cash outlay only** — keep rows with
          ``s == 'purchased'``.  Own-production / in-kind / other sources
          carry no cash expenditure, so they are excluded.  This is
          *consistent across countries* regardless of whether the source
          happened to record an imputed produced/in-kind value: some waves
          (e.g. Serbia, GhanaSPS) populate ``Expenditure`` on produced/
          in-kind rows, others (e.g. Guatemala, and every country built via
          the stock ``food_acquired_to_canonical``) leave it NaN.  Filtering
          to ``purchased`` removes that cross-country divergence.
        - ``'total'``: **all recorded acquisition value** — sum
          ``Expenditure`` across every ``s``.  Where the source recorded a
          produced/in-kind value it is included; where it did not, ``'total'``
          equals ``'purchased'`` for that country -- unless ``valuation=``
          is passed, which is the ONLY way this function ever fabricates a
          value.
    valuation : str or sequence of str, optional
        **Opt-in** own-production / in-kind valuation (GH #585).  ``None``
        (the default) is today's behaviour exactly: no value is fabricated.
        Otherwise the rungs named are run, in the order given, over the
        non-purchased rows that carry no value -- see
        :func:`food_acquired_valued` for the rungs, the provenance, and
        **the measured purchase-side bias, which you should read before using
        this**: every rung prices own-consumption at a purchase transaction,
        and in Uganda own-consumption so valued sits about 23% above what a
        sale actually fetched (``slurm_logs/price_sources/SYNTHESIS.org:750-753``,
        Uganda-only), while the interviewer manual asks for the farm-gate
        price (``:732-736``).  Requires ``basis='total'``; under
        ``basis='purchased'`` there is no non-purchased row left in the output
        to value, so it raises rather than silently doing nothing.

        A scalar names EXACTLY that rung: ``'median_price'`` runs the spatial
        ladder alone and does not quietly run ``'own_price'`` first.  Pass
        ``('own_price', 'median_price')`` for EPAR's consumption-repo
        ordering.
    geo : pd.DataFrame, optional
        Coarser geographic rungs for the ``'median_price'`` ladder, indexed by
        ``(t, v)`` with columns ordered finest -> coarsest.  See
        :func:`food_acquired_valued`.
    threshold : int, default 10
        Minimum priced observations for a geographic cell's median to be
        adopted.  Forwarded to :func:`median_price_valuation`.

    Notes
    -----
    Phase 4 of GH #169 preserves the ``s`` (acquisition-source) level in the
    output.  Users who want the legacy collapsed view call
    ``food_expenditures.groupby(level=['t','v','i','j']).sum()`` explicitly;
    ``basis='purchased'`` then yields the purchased total, ``'total'`` the
    sum across recorded sources.

    When the input has no ``s`` level (pre-canonical waves) the two bases
    coincide — there is no source split to filter on.

    With ``valuation=``, the returned frame carries the per-source tallies on
    ``.attrs['valuation_sources']`` (plus ``'valuation'`` and
    ``'valuation_geo_levels'``), mirroring ``harvest_kg``'s
    ``attrs['kg_factor_sources']``.  The per-row ``ValuationSource`` column is
    NOT on this frame: the output is summed over ``u``, and one ``(t, i, j, s)``
    cell can mix rungs, so a per-row label has nowhere unique to land.  Call
    :func:`food_acquired_valued` directly for the item-grain frame that carries
    it.

    A :class:`~lsms_library.feature.Feature` call forwards ``valuation=`` (it
    forwards by signature) and its VALUES are exact -- Malawi sums to the same
    426,562,493.98 either way.  Its ``attrs`` depend on how many countries were
    asked for, and the boundary is worth stating precisely because a reader who
    tests the general claim on one country would see it contradicted:

    - **one country** -- a ``concat`` of a single frame, nothing to disagree
      with, so the tallies COME THROUGH;
    - **more than one** -- the frames' ``attrs`` disagree by construction, one
      record per country, which lands in the ``{}`` row of the propagation rule
      (``CLAUDE.md``, "Panel ID Transitive Chains"), so
      ``attrs['valuation_sources']`` is ABSENT.

    Pooling the tallies across countries is deliberately NOT built: a pooled
    ``median_price: 280623`` would say nothing about WHICH country was imputed,
    the same objection ``harvest_kg``'s docstring makes about its own pooled
    counts.  Ask per country, or group the item-grain frame.
    """
    valid_basis = {'purchased', 'total'}
    if basis not in valid_basis:
        raise ValueError(
            f"food_expenditures basis= must be one of {sorted(valid_basis)}, "
            f"got {basis!r}"
        )

    rungs = _normalize_valuation_arg(valuation)
    if rungs and basis != 'total':
        raise ValueError(
            f"food_expenditures valuation={valuation!r} requires "
            f"basis='total'; got basis={basis!r}.  Valuing own-production "
            "rows is pointless under basis='purchased', which drops them."
        )

    df = _normalize_columns(df)
    if 'Expenditure' not in df.columns:
        raise ValueError("food_acquired must have an 'Expenditure' column")

    valuation_attrs = {}
    if rungs:
        # Everything above this line is untouched by the valuation and
        # everything below runs identically on the valued frame, so
        # ``valuation=None`` takes the original code path verbatim.
        valued = food_acquired_valued(df, rungs, geo=geo, threshold=threshold)
        valuation_attrs = {k: valued.attrs[k] for k in
                           ('valuation_sources', 'valuation',
                            'valuation_geo_levels')}
        df = df.assign(Expenditure=valued['Expenditure'].to_numpy())

    idx_names = list(df.index.names)

    # Drop zero / missing Expenditure.  Unlike the same-shaped drop in
    # food_prices_from_acquired (GH #591), this one is NOT silent data loss
    # and deliberately gets no warning: a zero expenditure contributes zero
    # to every downstream sum, so dropping the row is a sparsity convention,
    # not the destruction of a number the user needed.  (The price drop is
    # different in kind: there the row carried a POSITIVE expenditure and
    # the price was uncomputable, so the row's information dies with it.)
    x = df[['Expenditure']].replace(0, np.nan).dropna()
    if basis == 'purchased' and 's' in idx_names:
        # Cash outlay only — drop non-purchased sources so the figure means
        # the same thing across countries (#575).
        x = x[x.index.get_level_values('s').astype(str) == 'purchased']

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
    x = x.groupby(group_by).sum()
    x.attrs.update(valuation_attrs)
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
    kgf = food_kg_factors(df, volume_as_mass=volume_as_mass)
    v = _apply_kg_conversion(df, kgf['kg_per_unit'])

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
    # Stamped AFTER the aggregation: ``out`` is a fresh frame, so anything
    # attached upstream is gone by here.  The counts are over the INPUT rows
    # (they partition ``df``), not over the returned ones -- the returned
    # frame is a sum over units and sources and has no per-layer identity.
    out.attrs['kg_factor_sources'] = dict(kgf.attrs['kg_factor_sources'])
    out.attrs['kg_factor_wave_spread'] = dict(
        kgf.attrs['kg_factor_wave_spread'])
    return out


def food_prices_from_acquired(df, units='kgvalue', *, volume_as_mass=True):
    """Derive food prices from food_acquired.

    Returned at the canonical ``(t, i, j, u, s)`` grain (``v`` is omitted
    and re-joined from ``sample()`` by ``_finalize_result`` at API time).
    Any extra recall level present in the input — notably GhanaLSS's
    ``visit`` — is aggregated out here via the median Price, mirroring how
    ``food_quantities_from_acquired`` / ``food_expenditures_from_acquired``
    collapse it with ``.sum()`` (price is per-unit, not additive, so median
    rather than sum).  This keeps the three ``_FOOD_DERIVED`` transforms at
    the same country-level grain so ``Feature('food_prices')`` no longer has
    to drop a leaked ``visit`` level (and silently keep one arbitrary
    visit's price) before the cross-country concat — see GH #517.

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
        Single-column ``Price`` DataFrame at the canonical
        ``(t, i, j, u, s)`` grain (``v`` re-joined downstream; any extra
        recall level such as ``visit`` is collapsed via median Price).
        Zero / infinite / NaN prices are dropped.

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

    kgf = None
    if units == 'kgvalue':
        kgf = food_kg_factors(df, volume_as_mass=volume_as_mass)
        v = _apply_kg_conversion(df, kgf['kg_per_unit'])
        with np.errstate(divide='ignore', invalid='ignore'):
            v = v.assign(Price=v['Expenditure'] / v['Quantity_kg'])
    elif units == 'unitvalue':
        with np.errstate(divide='ignore', invalid='ignore'):
            v = df.assign(Price=df['Expenditure'] / df['Quantity'])
    elif units == 'unitprice':
        v = df.copy()
        # Price column already populated.
    elif units == 'kgprice':
        if 'u' in df.index.names:
            # The SAME per-row factor the 'kgvalue' branch converts with
            # (GH #850): before that, this branch mapped ``u -> factor``
            # through an independent dict, so the two branches could
            # disagree on a row.
            kgf = food_kg_factors(df, volume_as_mass=volume_as_mass)
            kg_per_unit = pd.Series(_as_float(kgf['kg_per_unit']),
                                    index=df.index)
            with np.errstate(divide='ignore', invalid='ignore'):
                v = df.assign(
                    Price=pd.Series(_as_float(df['Price'])
                                    / _as_float(kg_per_unit),
                                    index=df.index))
        else:
            # No u index → can't convert; emit NaN
            v = df.assign(Price=np.nan)

    # Zero / infinite / NaN prices are dropped -- but NOT silently (GH #591).
    # The denominator of the loss is taken from the INPUTS: the rows that
    # ought to have produced a price.  ``.replace(...).dropna()`` used to do
    # this in one wordless expression, which is how ~99% of Nigeria's price
    # rows vanished without a trace.  NaN-and-keep was considered and
    # rejected: it would change the shape of food_prices for every country
    # (Uganda +6,456 rows, Serbia +3,767, Tanzania +714, GhanaSPS +883) and
    # downstream demand code assumes a dense frame.
    if units in ('kgvalue', 'unitvalue'):
        expected = _as_float(df['Expenditure']) > 0
    else:                                   # 'kgprice' / 'unitprice'
        expected = ~np.isnan(_as_float(df['Price']))
    v = _drop_unpriceable(v, units, expected)

    # Aggregate over any non-canonical recall level (e.g. GhanaLSS's
    # ``visit`` — ~12 repeated recall visits per month) at the country
    # level, mirroring what ``food_quantities_from_acquired`` and
    # ``food_expenditures_from_acquired`` do with ``.sum()``.  Price is
    # per-unit and therefore NOT additive, so we collapse with the median
    # rather than the sum.  ``v`` is omitted from the group-by (see the
    # rationale in ``food_expenditures_from_acquired``): it is re-joined
    # from ``sample()`` by ``_finalize_result`` at API time, and dropping
    # it here avoids silently discarding HH whose cluster ID is
    # unrecoverable.  Without this, ``visit`` leaked to
    # ``Feature.food_prices`` where ``_collapse_duplicate_index`` kept one
    # arbitrary visit's price via ``groupby().first()`` (GH #517).
    group_by = [n for n in ['t', 'i', 'j', 'u', 's'] if n in v.index.names]
    if group_by:
        tally = v.attrs.get('price_rows_dropped')
        v = v.groupby(group_by).median()
        # groupby drops attrs (pandas 2.x/3.x) -- keep the drop tally so a
        # caller can audit the loss without parsing the warning text.
        if tally is not None:
            v.attrs['price_rows_dropped'] = tally
    # Same reasoning as in ``food_quantities_from_acquired``: stamped last,
    # counted over the INPUT rows.  Absent on the modes that use no factor
    # ('unitvalue', 'unitprice') rather than present and zero -- a mode that
    # never consults the ladder has no ladder to report.
    if kgf is not None:
        v.attrs['kg_factor_sources'] = dict(kgf.attrs['kg_factor_sources'])
        v.attrs['kg_factor_wave_spread'] = dict(
            kgf.attrs['kg_factor_wave_spread'])
    return v


def legacy_area_output(country):
    """Reproduce the retired EthiopiaRHS Country(X).area_output() (t,i) table.

    The HH-level wide ``area_output`` table (#277) was retired in favour of the
    item-level ``crop_production`` (t, i, j) feature (#438), which subsumes it.
    This shim re-widens ``crop_production`` back to the old (t, i) shape with
    per-crop ``{Crop}_kg`` (production) and ``{Crop}_ha`` (area) columns, so
    callers migrating off ``area_output()`` keep working.

    Note: ``crop_production`` covers MORE waves than the original ``area_output``
    (R1-R4 + R6 + R7, not just R6/R7), so this shim now returns those extra
    waves too.  New code should use ``crop_production()`` directly.

    Parameters
    ----------
    country : Country
        The Country instance (e.g., ``ll.Country('EthiopiaRHS')``).

    Returns
    -------
    pd.DataFrame
        DataFrame indexed by (t, i) with ``{Crop}_kg`` / ``{Crop}_ha`` columns
        (crop label spaces removed, e.g. ``WhiteTeff_kg``).
    """
    cp = country.crop_production().reset_index()
    idx = [c for c in ('t', 'i') if c in cp.columns]
    # crop label -> compact column stem ('White Teff' -> 'WhiteTeff')
    cp['stem'] = cp['j'].astype(str).str.replace(' ', '', regex=False)
    kg = cp.pivot_table(index=idx, columns='stem', values='Quantity',
                        aggfunc='first')
    ha = cp.pivot_table(index=idx, columns='stem', values='Area_ha',
                        aggfunc='first')
    kg.columns = [f'{c}_kg' for c in kg.columns]
    ha.columns = [f'{c}_ha' for c in ha.columns]
    out = kg.join(ha, how='outer').sort_index(axis=1)
    out.columns.name = None
    return out


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
#
# A second point of comparison, with the same non-canonical stance, is the
# EPAR (Evans School, UW) LSMS-ISA indicator curation, Technical Report #335:
# slurm_logs/2026-09-09_epar_curation/{EPAR_PROJECT,LEARNINGS}.org.  Its
# median-price ladders (crop sale price, livestock, own-consumed food) select
# the finest geographic cell with >= 10 observations exactly as
# ``median_price_valuation`` does; the one construction delta was that EPAR's
# medians are weighted (by population-raked survey weights) and ours were not.
# ``median_price_valuation(weight_col=...)`` now opts in to a weighted median
# -- a strict generalisation (equal weights reproduce the unweighted median
# exactly), the default stays unweighted, and we still do not RAKE the
# weights.
# EPAR also winsorises at the 1st/99th percentile; that is not reproduced
# here, by design (count, never clip; within-wave mean-one weights).


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


#: Largest kg-per-unit a REPORTED ``KgFactor`` may claim before it is read as
#: a data-entry error rather than a container.  Derived from the corpus's own
#: unit tables, not guessed: the heaviest container any of them NAMES is
#: Uganda's ``Sack (120 kgs)`` (``Uganda/_/categorical_mapping.org:278``),
#: with Niger's ``Sac de 100 kg`` and Malawi's 90 kg bag below it.  (Two
#: larger-looking greps are false positives: Uganda's ``0.125kg`` is
#: margarine, a food item, and Niger's ``2687`` is prose about 2.687 kg.)
#: 120 x 2 headroom, rounded -> 250, which leaves room for a country to ship
#: a heavier container than any yet seen while still rejecting the observed
#: errors by two orders of magnitude (Uganda's worst reported factor is
#: 260 000, and its 99.9th percentile is 2 018 -- a year, not a weight).
KG_FACTOR_MAX = 250.0

#: A reported factor on the kilogram unit must be 1: a "kg" that weighs
#: something other than a kilogram is a mis-keyed field, not a unit.  Allowed
#: within 1% so a rounded 0.999/1.001 survives.
KG_UNIT_TOLERANCE = 0.01

#: Reported factors falling in this INCLUSIVE band, integral, on a non-kg
#: unit are read as the adjacent YEAR column having been keyed into the
#: factor field -- the signature behind Uganda's Beans 2 017 and Soybean
#: 2 018.  Currently SUBSUMED by :data:`KG_FACTOR_MAX` (every year exceeds
#: 250), so it rejects nothing the cap would not; it is kept because it
#: documents the signature and stays correct if the cap is ever raised.
_KG_FACTOR_YEAR_BAND = (1990, 2030)

#: The kilogram unit's accepted spellings, READ OFF the module's own metric
#: table rather than hardcoded: the ``KNOWN_METRIC`` keys whose factor is
#: exactly 1, minus the fluid units (a litre is also 1 under
#: ``volume_as_mass`` but is not the kilogram unit).  Matching is on the
#: lower-cased label, exactly as :func:`_kg_factor_series` matches.
_KG_UNIT_KEYS = frozenset(
    k for k, v in KNOWN_METRIC.items() if v == 1) - frozenset(_FLUID_UNITS)


def _screen_reported_factors(df, reported):
    """Reject REPORTED factors that are data-entry errors, not weights.

    Uganda's first wired wave made the need concrete: 86 rows with a reported
    factor above 200 contributed 46.97M of a 57.9M kg national total, and the
    top ones are recognisable mis-keys -- a Sorghum row with quantity 147 and
    factor 147 000 (the quantity re-keyed, x1000), a ``u='Kg'`` row claiming
    260 000, and factors of 2 017 / 2 018 (the adjacent year column).

    Three rules, each counted under ``reported_implausible``:

    1. the row's unit IS the kilogram (:data:`_KG_UNIT_KEYS`) and the factor
       is not within :data:`KG_UNIT_TOLERANCE` of 1;
    2. the factor exceeds :data:`KG_FACTOR_MAX`;
    3. the factor is an integral calendar year in
       :data:`_KG_FACTOR_YEAR_BAND` on a non-kg unit.

    A rejected value is NEVER clipped or rescaled -- inventing a weight is
    the failure this whole design exists to avoid.  The row falls through to
    ``shipped`` / ``survey_median`` / ``inferred`` / ``none`` exactly as if it
    had never reported, and its rejected value enters no median.

    ALSO SCREENS THE ``shipped`` LAYER, by design and not by accident: the
    same three rules, one implementation.  Note which frame the unit comes
    from -- ``df``, the row being served, never the factor table -- so a
    shipped factor is judged against the unit it is being APPLIED to.  A
    shipped 300 on a ``Kg`` row is exactly as wrong as a reported one.  Its
    rejections are counted separately, under ``shipped_implausible``.

    Returns ``(screened, rejected_mask)``.
    """
    rep = _valid_factor(reported)
    have = ~np.isnan(rep)

    u = _level_or_column(df, 'u')
    if u is None:
        is_kg = np.zeros(len(rep), dtype=bool)
    else:
        text = pd.Series(u).astype(str).str.strip().str.lower().to_numpy()
        is_kg = np.isin(text, sorted(_KG_UNIT_KEYS))

    lo, hi = _KG_FACTOR_YEAR_BAND
    kg_not_one = have & is_kg & (np.abs(rep - 1.0) > KG_UNIT_TOLERANCE)
    too_heavy = have & (rep > KG_FACTOR_MAX)
    year_like = (have & ~is_kg & (rep == np.round(rep))
                 & (rep >= lo) & (rep <= hi))

    rejected = kg_not_one | too_heavy | year_like
    return np.where(rejected, np.nan, rep), rejected


#: The canonical ``u`` value meaning "NO UNIT WAS RECORDED".  Uganda mints it
#: wherever a wave ships no harvest-unit label (``uganda.py:1386``, ``:1859``;
#: ``Uganda/_/data_scheme.yml`` documents it for ``crop_production.u``).
#:
#: It is NOT a unit, and the ``survey_median`` layer must never treat it as
#: one: a median of reported factors taken across "no unit recorded" rows is a
#: number about nothing, and using it to fill OTHER such rows fabricates
#: weights -- the exact opposite of what the sentinel is for.
#:
#: DECLARED HERE BECAUSE NOTHING DECLARES IT MACHINE-READABLY.  Checked at
#: 28f9243f: ``data_info.yml`` has an ``Unknown`` sentinel, but it belongs to
#: the EDUCATION vocabulary (``Columns.household_roster.Education``), not to
#: ``u``; and there is no ``niger.py:U_NA`` in the tree (``rg U_NA`` is
#: empty).  Niger's missing-unit sentinel is the literal string ``Manquant``
#: (``niger.py:602``, ``_COMMUNITY_MISSING_UNITS`` at ``:1183``) and has NOT
#: been relabelled onto ``Unknown``, so it is listed below rather than
#: assumed away.  When GH #847 lands a single canonical declaration, delete
#: both of these and read it from there.
U_UNKNOWN = 'Unknown'

#: Every live spelling of "no unit recorded", lower-cased.  A NaN ``u`` counts
#: too, and is handled separately in :func:`_unit_sentinel_mask`.
_U_SENTINELS = frozenset({U_UNKNOWN.lower(), 'manquant'})


def _unit_sentinel_mask(df):
    """Boolean mask: rows whose ``u`` records NO unit at all.

    True for a NaN ``u`` and for any spelling in :data:`_U_SENTINELS`.  These
    rows are excluded from the ``survey_median`` layer on both sides -- they
    neither contribute to a median nor receive one -- and from the
    disagreement audit, where a reported factor on such a row has nothing to
    be compared against.
    """
    u = _level_or_column(df, 'u')
    if u is None:
        return np.zeros(len(df), dtype=bool)
    col = pd.Series(u)
    missing = col.isna().to_numpy()
    text = col.astype(str).str.strip().str.lower().to_numpy()
    return missing | np.isin(text, sorted(_U_SENTINELS))


#: Minimum number of REPORTED ``KgFactor`` values a group must carry before
#: :func:`harvest_kg` will use their MEDIAN to fill that group's unreported
#: rows (layer *survey_median*).  Five is a starting value, not a measured
#: one -- it is low enough that a unit a wave asks about at all will usually
#: clear it, and high enough that one enumerator's keystroke does not become
#: a whole unit's weight.  Exposed as a ``min_reports=`` kwarg on
#: :func:`harvest_kg` / :func:`harvest_kg_factors` so callers (and tests) can
#: move it without monkeypatching, and so a country whose module is thin can
#: be examined at a lower N without changing the default for everyone.
SURVEY_MEDIAN_MIN_REPORTS = 5

#: Relative gap above which two layers' factors for the SAME row are reported
#: as DISAGREEING by :func:`harvest_kg_factors`.  Measured against the
#: *reported* factor (the survey's own number is the reference, not the
#: library's inference).
KG_FACTOR_DISAGREEMENT_TOLERANCE = 0.10

#: The layers :func:`harvest_kg_factors` resolves a row's factor through, in
#: precedence order.  ``none`` is a layer, not a failure: a row it serves
#: contributes nothing to ``Harvest_kg`` and is COUNTED rather than silently
#: absent.
#:
#: ``shipped`` sits BELOW ``reported`` and ABOVE ``survey_median``: an
#: externally shipped conversion table (the World Bank's Ethiopia
#: ``Crop_CF_Wave{2..5}``, Malawi's IHS5 files -- GH #852, #854) is evidence
#: about what a container weighs, but it is not THIS row's enumerator writing
#: down what THIS harvest weighed, so it never displaces a plausible reported
#: factor; and it is externally sourced rather than a pooling of the survey's
#: own reports, so it outranks ``survey_median``.
#:
#: The counts over these layers PARTITION the frame and sum to ``len(df)``, so
#: every key is present on every call -- ``shipped: 0`` when no table is
#: passed.
KG_FACTOR_LAYERS = ('reported', 'shipped', 'survey_median', 'inferred',
                    'none')


def _level_or_column(df, name):
    """Return index level or column *name* as an ndarray, or ``None``.

    ``crop_production`` carries ``u`` as an index level in some countries and
    as a column in others (see :func:`_with_u_in_index`), and ``condition``
    is an index level where it exists at all.  Grouping code must not care
    which.
    """
    if name in (df.index.names or []):
        return df.index.get_level_values(name).to_numpy()
    if name in df.columns:
        return df[name].to_numpy()
    return None


def _valid_factor(values):
    """Float ndarray with NaN wherever the factor is unusable.

    A kg factor must be finite and strictly positive.  Zero, negative and
    infinite reports are DISCARDED, not clipped: they count neither as a
    reported factor, nor toward a group's median, nor toward the ``N`` that
    licenses that median.
    """
    arr = _as_float(pd.Series(values))
    return np.where(np.isfinite(arr) & (arr > 0), arr, np.nan)


def _survey_median_factors(df, reported, *, min_reports, sentinel=None):
    """Median of REPORTED factors for a row's own ``(u, condition)`` group.

    The survey's own weights filling the survey's own gaps.  For each row,
    take the median of the *reported* ``KgFactor`` of every row sharing its
    unit and condition within the same country-wave, provided at least
    *min_reports* of them reported one; where that group is too thin, fall
    back to the same country-wave's ``u`` alone.  Returns NaN where neither
    group clears the bar.

    Grouping notes
    --------------
    * ``groupby(..., dropna=False)``.  The default ``dropna=True`` DELETES
      rows whose group key is NA (CLAUDE.md, "Grain Collapse" §3b), and both
      ``u`` and ``condition`` can be NA in a raw frame.  A row with an NA key
      must fall through to the inferred layer, not vanish.
    * ``count`` counts non-NaN, and *reported* has already had its invalid
      values NaN-ed by :func:`_valid_factor`, so ``N`` is the number of
      USABLE reports -- which is what licenses the median.
    * The median of a group INCLUDES a reporting row itself.  That is
      deliberate: it is also the reference the disagreement audit compares
      that row's own report against.
    * Rows flagged by *sentinel* (see :func:`_unit_sentinel_mask`) are
      excluded from BOTH SIDES: their reports do not enter any median, and
      they receive none.  ``u='Unknown'`` is the absence of a unit, not a
      unit, so a "group" of such rows pools containers of unrelated sizes.
      Such a row therefore gets kilograms from its OWN reported ``KgFactor``
      or from nothing at all.
    """
    if sentinel is None:
        sentinel = np.zeros(len(df), dtype=bool)
    reported = np.where(sentinel, np.nan, reported)
    keys = {}
    for name in ('country', 't', 'u', 'condition'):
        col = _level_or_column(df, name)
        if col is not None:
            keys[name] = pd.Series(col).astype(object)

    wave = [k for k in ('country', 't') if k in keys]
    fine = wave + [k for k in ('u', 'condition') if k in keys]
    coarse = wave + [k for k in ('u',) if k in keys]

    rep = pd.Series(reported, index=pd.RangeIndex(len(df)))
    out = np.full(len(df), np.nan)
    seen = []
    for group in (fine, coarse):
        if not group or group in seen:
            continue
        seen.append(list(group))
        by = [keys[k].set_axis(rep.index) for k in group]
        g = rep.groupby(by, dropna=False)
        med = g.transform('median').astype('float64').to_numpy()
        n = g.transform('count').astype('float64').to_numpy()
        cand = np.where(n >= min_reports, med, np.nan)
        out = np.where(np.isnan(out), cand, out)
    return _valid_factor(np.where(sentinel, np.nan, out))


#: Index levels (or columns) a ``shipped_factors`` table may be keyed on.  The
#: join uses the ones present in BOTH the factor table and the
#: ``crop_production`` frame; the loader decides which levels its table can
#: honestly key on, and this transform joins on the overlap.
#:
#: ``country`` is in the set for a reason worth stating: a cross-country
#: :class:`~lsms_library.feature.Feature` frame carries a ``country`` level,
#: and a table loaded for ONE country would otherwise be applied to every
#: country's rows.  Keying on ``country`` lets a loader fence its own table;
#: it costs nothing when the level is absent.
#:
#: ``region`` is spelled in LOWER CASE and matched exactly.
#: ``cluster_features`` ships ``Region``, so a caller resolving region onto
#: ``crop_production`` must rename it -- accepting both spellings silently
#: would hide a mis-keyed join, which is the failure mode this whole layer is
#: most exposed to.
SHIPPED_FACTOR_JOIN_LEVELS = ('country', 't', 'j', 'u', 'condition', 'region')

#: Private stand-in for a NA join key.  ``astype(str)`` would render NaN as the
#: literal ``'nan'`` and let it collide with a genuine ``'nan'`` label; this
#: string cannot occur in survey text.
_KEY_NA = '\x00<NA>'


def _normalise_join_key(values):
    """Stripped, lower-cased join key with NA on :data:`_KEY_NA`.

    Applied identically to BOTH sides of the shipped-factor join, and BEFORE
    the duplicate check -- so a collision the normalisation itself creates
    (a table carrying both ``'Kg'`` and ``'kg'``) is REFUSED rather than
    silently resolved.
    """
    col = pd.Series(np.asarray(values, dtype=object))
    missing = col.isna().to_numpy()
    text = col.astype(str).str.strip().str.lower().to_numpy()
    return np.where(missing, _KEY_NA, text)


def _shipped_table_key(shipped_factors, name):
    """Read join key *name* off a shipped-factor table's index or columns."""
    if name in (shipped_factors.index.names or []):
        return shipped_factors.index.get_level_values(name).to_numpy()
    return shipped_factors[name].to_numpy()


def _shipped_factor_lookup(df, shipped_factors):
    """Each row's EXTERNALLY SHIPPED kg-per-unit factor, and its ``Source``.

    *shipped_factors* is a table the CALLER loaded -- nothing here discovers
    it (see :func:`harvest_kg_factors`).  Canonical shape: a ``KgFactor``
    column (kg per ONE unit of the row's ``u``, the same meaning the canonical
    ``crop_production.KgFactor`` carries) and an optional ``Source`` string
    column, keyed -- as index levels or as columns -- on some subset of
    :data:`SHIPPED_FACTOR_JOIN_LEVELS`.

    The join is on the keys present in BOTH the table and *df*.  A key the
    table declares but *df* does not carry is DROPPED from the join, which is
    the condition-aware behaviour: where the table carries ``condition`` and
    the crop row has one, they join on it; where either lacks it, they join
    ignoring it.  A dropped key is now ANNOUNCED
    (:class:`ShippedFactorWarning`) rather than applied in silence: a
    one-region table going national and a one-condition table serving every
    condition are exactly the cases that used to pass unremarked while their
    multi-row equivalents raised.  Dropping a key can of course make the
    remaining ones ambiguous, and that is when the duplicate refusal below
    fires instead -- the intended outcome, since collapsing a region- or
    condition-keyed table onto a coarser grain is a decision for the loader.

    ``u`` is REQUIRED among the table's keys.  A kg-per-unit factor that does
    not say which unit is not a factor: a ``j``-only table would hand a crop's
    single number to a sack, a basket and a tin alike.

    A table with no ``country`` key applied to a pooled
    :class:`~lsms_library.feature.Feature` frame is applied to EVERY country's
    rows (that is the dropped-key rule again, and it now warns).

    REFUSES an ambiguous table.  Two rows sharing a join key are two different
    answers to the same question; averaging them, or letting a
    ``groupby().first()`` pick one, is the grain collapse ``CLAUDE.md``
    forbids and the failure GH #852 names outright (Ethiopia's
    ``Crop_CF_Wave5`` ships crop 74 / unit 62 twice, at 4.34 and 6.125, and
    the right answer -- keep 4.34 -- comes from W3 data, not from a rule this
    transform could apply).  De-duplication is the loader's deliberate act.

    STRICTER than the core's grain-collapse rule, deliberately: it refuses
    IDENTICAL duplicate rows too, not only disagreeing ones.  Core stays
    silent on a lossless de-dup because it is reducing data it was handed; a
    factor table is a small curated artefact, and a loader that has not looked
    at its own duplicates has not looked at its table.  A raw WB file with
    repeated rows will trip this -- ``drop_duplicates()`` in the loader is the
    expected answer, and writing it is the act of noticing.

    A SILENT ZERO-MATCH is this layer's dominant failure mode.  Keys are
    compared as stripped, lower-cased text, so any vocabulary or type
    mismatch simply matches nothing: ``j`` as a WB crop code ``74`` against a
    decoded ``'Enset'``; a numeric ``74.0`` against ``'74'``; a ``region``
    code ``1`` against ``1.0``; ``t`` as an int against ``'2019-20'``.  A
    non-empty table that matches zero rows now raises a
    :class:`ShippedFactorWarning`, and the count to read is
    ``attrs['kg_factor_sources']['shipped_matched']`` -- rows the table
    supplied a finite factor for, taken BEFORE the sentinel, the screen and
    the rank.  Do NOT read ``counts['shipped']`` as the tell: it is post-rank
    and is legitimately 0 when a perfectly-keyed table is outranked on every
    row by ``reported``.

    A NA key is not a wildcard.  NaN is normalised to a private sentinel on
    both sides, so it matches NaN and only NaN -- deliberately unlike
    ``pd.merge``'s null-matching, which ``CLAUDE.md`` flags as a hazard: here
    it is explicit and symmetric ("condition not applicable" on both sides),
    not incidental.

    Returns ``(values, source)``: a float ndarray and an object ndarray, both
    aligned POSITIONALLY to *df* (``crop_production`` indexes repeat, so an
    index-aligned merge would be wrong as well as slow).  ``source`` is
    ``pd.NA`` wherever no usable factor was found (pandas 3.0 renders that as
    NaN when the array becomes a column, so read it with ``pd.isna``).
    """
    sf = shipped_factors
    if not isinstance(sf, pd.DataFrame):
        raise ValueError(
            "shipped_factors must be a DataFrame with a 'KgFactor' column; "
            f"got {type(sf).__name__}")
    if 'KgFactor' not in sf.columns:
        raise ValueError(
            "shipped_factors must carry a 'KgFactor' column (kg per ONE unit "
            f"of the row's u); got columns {list(sf.columns)}")

    index_names = [n for n in (sf.index.names or []) if n is not None]
    table_keys = tuple(k for k in SHIPPED_FACTOR_JOIN_LEVELS
                       if k in index_names or k in sf.columns)
    if not table_keys:
        raise ValueError(
            "shipped_factors is keyed on none of "
            f"{list(SHIPPED_FACTOR_JOIN_LEVELS)}: it carries index levels "
            f"{index_names} and columns {list(sf.columns)}.  Key the table on "
            "the levels it should join on -- at minimum the unit 'u'.")
    if 'u' not in table_keys:
        # ENFORCED, not merely asked for.  A kg-PER-UNIT factor that does not
        # say which unit is not a factor; a `j`-only table would hand a crop's
        # single number to a sack, a basket and a tin alike.  The message
        # above claims this rule, so the code has to have it.
        raise ValueError(
            f"shipped_factors is keyed on {list(table_keys)} but not on 'u'. "
            "A kg-per-unit factor with no unit is not a factor -- it would be "
            "applied to every container the crop was ever measured in.  Key "
            "the table on 'u' (plus whatever else it can honestly key on).")

    keys = tuple(k for k in table_keys if _level_or_column(df, k) is not None)
    dropped = tuple(k for k in table_keys if k not in keys)
    if not keys:
        raise ValueError(
            f"shipped_factors is keyed on {list(table_keys)}, none of which "
            "crop_production carries as an index level or a column, so there "
            "is nothing to join on.  Resolve the missing level onto "
            "crop_production first (e.g. join cluster_features and rename its "
            "'Region' to 'region') -- this transform never re-enters sample() "
            "or cluster_features() itself.")

    table_cols = [_normalise_join_key(_shipped_table_key(sf, k)) for k in keys]
    table_tuples = list(zip(*table_cols))
    duplicated = pd.MultiIndex.from_arrays(table_cols).duplicated(keep=False)
    if duplicated.any():
        examples = sorted({table_tuples[i] for i in np.flatnonzero(duplicated)})
        because = ''
        if dropped:
            because = (f"  {list(dropped)} could not be joined on because "
                       "crop_production carries no such level, which is what "
                       "left the remaining keys ambiguous -- collapse that "
                       "axis in the loader.")
        raise ValueError(
            f"shipped_factors is ambiguous on the join keys {list(keys)}: "
            f"{len(examples)} duplicated key(s), e.g. {examples[:5]}."
            f"{because}  De-duplicate the table in the LOADER, with a stated "
            "rule; this transform refuses to average two factors that "
            "disagree, or to take the first of them.")

    values = _valid_factor(sf['KgFactor'])
    if 'Source' in sf.columns:
        sources = sf['Source'].astype(object).to_numpy()
    else:
        sources = np.full(len(sf), pd.NA, dtype=object)
    factor_of = dict(zip(table_tuples, values))
    source_of = dict(zip(table_tuples, sources))

    if dropped:
        warnings.warn(
            f"shipped_factors is keyed on {list(dropped)}, which "
            "crop_production does not carry, so that level was DROPPED from "
            f"the join and the table was applied across it (joining on "
            f"{list(keys)} alone).  A one-region table therefore goes "
            "national, and a one-condition table serves every condition.  "
            "Resolve the level onto crop_production first, or reduce the "
            "table to the coarser grain deliberately.",
            ShippedFactorWarning, stacklevel=3)

    row_cols = [_normalise_join_key(_level_or_column(df, k)) for k in keys]
    out = np.full(len(df), np.nan)
    out_source = np.full(len(df), pd.NA, dtype=object)
    for pos, key in enumerate(zip(*row_cols)):
        value = factor_of.get(key, np.nan)
        if not np.isnan(value):
            out[pos] = value
            out_source[pos] = source_of[key]

    matched = int(np.isfinite(out).sum())
    if len(sf) and not matched:
        if not np.isfinite(values).any():
            # Not a keying problem: the table's own values are unusable.  A
            # `.dta` whose conversion column read back as TEXT lands here, and
            # so does one that is all zero / negative.  Saying "check your
            # keys" would send the loader author to the wrong end of the file.
            reason = (f"none of its {len(sf)} row(s) carries a usable "
                      "KgFactor (finite and > 0) -- check that the column "
                      "parsed as numeric")
        else:
            reason = ("check that the table's keys use the same labels as "
                      "the frame (j decoded, u canonical).  Joined on "
                      f"{list(keys)}; the table offers {len(sf)} row(s)")
        warnings.warn(
            f"shipped_factors matched 0 of {len(df)} rows; {reason}.",
            ShippedFactorWarning, stacklevel=3)
    return out, out_source, matched


def harvest_kg_factors(crop_production, *, volume_as_mass=True,
                       min_reports=SURVEY_MEDIAN_MIN_REPORTS,
                       shipped_factors=None):
    """Per-row kg-per-unit factor for ``crop_production``, and its PROVENANCE.

    The factor half of :func:`harvest_kg`, exposed on its own so the layers
    can be AUDITED -- most usefully, the survey's own reported weights
    against the library's inferred ones for the same unit.  Every row of
    *crop_production* gets a row here, in the same order and on the same
    index.

    Layers, in precedence order (``KgFactorSource`` records which one served
    each row)

    ``reported``
        The row's own ``KgFactor`` column -- what the instrument wrote down
        (UNPS ``a5?q6d``, "conversion factor into kg"), where it is finite,
        > 0 and PLAUSIBLE (:func:`_screen_reported_factors`: a kilogram unit
        must weigh 1, nothing may exceed :data:`KG_FACTOR_MAX`, and an
        integral calendar year on a non-kg unit is the adjacent-column
        signature).  A rejected value is never clipped or rescaled -- the row
        falls through to the layers below exactly as if it had not reported,
        and the rejection is counted under ``reported_implausible``.
        ``KgFactor`` is REPORTED, never constructed: see the canonical schema
        note in ``lsms_library/data_info.yml``.
    ``shipped``
        A factor looked up in *shipped_factors* -- an EXTERNALLY SHIPPED
        conversion table the caller passed in (the World Bank's Ethiopia
        ``Crop_CF_Wave{2..5}``, Malawi's IHS5 crop files: GH #852, #854).
        Screened by the SAME :func:`_screen_reported_factors`, with its
        rejections counted under ``shipped_implausible``; a rejected shipped
        factor is likewise never clipped, and the row falls through.
        Ranked below ``reported`` because a table is not this row's
        enumerator, and above ``survey_median`` because it is external
        evidence rather than a pooling of the survey's own reports.  See
        *shipped_factors* below for the table's shape and the join.
    ``survey_median``
        The median reported ``KgFactor`` of the same ``(u, condition)``
        within the same country-wave, where at least *min_reports* rows
        report one; falling back to ``u`` alone.  The survey's own weights
        filling the survey's own gaps -- see :func:`_survey_median_factors`.
    ``inferred``
        The shared unit→kg machinery, unchanged: :func:`_kg_factor_series`
        → :func:`_get_kg_factors` (hand-coded metric tokens, the
        explicit-metric label parser, then price-ratio inference).  This is
        the ONLY layer that acts on a frame with no ``KgFactor`` column, so
        such a frame gets exactly the factors it got before this function
        existed.
    ``none``
        No layer produced a usable factor.  Counted, not hidden: these rows
        contribute nothing to ``Harvest_kg``.

    Parameters
    ----------
    crop_production : pd.DataFrame
        The ``crop_production`` item feature.  Needs ``Quantity`` only for
        the price-ratio branch of the inferred layer; needs ``u`` as an
        index level or a column.  ``KgFactor``, ``condition`` and
        ``country`` are all optional.
    volume_as_mass : bool, default True
        Forwarded to :func:`_get_kg_factors` (1 litre = 1 kg for fluids).
    min_reports : int, default :data:`SURVEY_MEDIAN_MIN_REPORTS`
        ``N`` for the ``survey_median`` layer.  One number is applied to
        every group, but the groups are per country-wave, so a thin module
        does not borrow a thick one's licence.
    shipped_factors : pd.DataFrame, optional
        An externally shipped kg-per-unit table.  ``KgFactor`` column (kg per
        ONE unit of the row's ``u``) plus an optional ``Source`` string,
        keyed -- as index levels or as columns -- on some subset of
        :data:`SHIPPED_FACTOR_JOIN_LEVELS`
        ``('country', 't', 'j', 'u', 'condition', 'region')``.  The loader
        decides which levels its table can honestly key on; the join uses
        those present in BOTH the table and *crop_production*.

        NOTHING AUTO-DISCOVERS A COUNTRY'S TABLE, and the result is never
        stored.  The intended call shape, once the loaders of GH #852 / #854
        exist, is explicit::

            harvest_kg(cp, shipped_factors=ethiopia.crop_conversion_factors(...))

        A ``shipped_factors='auto'`` registry is a live design question for
        @ligon; the options and the trade-off are written up in
        ``.coder/ledger/harvest-kg-shipped-factors.md``, section 6.

        ``region`` must be resolved by the CALLER onto *crop_production* (join
        ``cluster_features`` and rename its ``Region`` to ``region``); this
        function never re-enters ``sample()`` or ``cluster_features()``.  Note
        also that ``crop_production.j`` is the DECODED crop label, not the WB
        crop code -- a table read straight from ``Crop_CF_Wave4.dta`` carries
        ``74``, which matches no ``j``, and the ``shipped: 0`` count is the
        only tell.  Decode in the loader.

        An AMBIGUOUS table (two rows sharing a join key) raises ``ValueError``
        naming the duplicates: de-duplicating is the loader's deliberate act,
        never an average taken here.

        This is NOT Malawi's shelled/unshelled ratio interpolation (EPAR's
        ``EPAR_UW_conversionfactors.do:381-390``): inferring one condition's
        factor from the other's is a later, separate layer with its own N
        threshold (LEARNINGS L9, GH #854).  This layer only looks a factor up.

    Returns
    -------
    pd.DataFrame
        Indexed like *crop_production*, with

        ``kg_per_unit``
            the resolved factor (NaN for ``none`` rows).  Deliberately NOT
            called ``KgFactor``: that name is reserved for the survey's own
            reported number, and this column is CONSTRUCTED.
        ``KgFactorSource``
            one of :data:`KG_FACTOR_LAYERS`.
        ``kg_reported`` / ``kg_shipped`` / ``kg_survey_median`` /
        ``kg_inferred``
            what each layer offered for that row, whether or not it won.
            ``kg_reported`` and ``kg_shipped`` are POST-screen, so a rejected
            value reads NaN here and ``kg_reported_rejected`` /
            ``kg_shipped_rejected`` is True; the raw value is untouched in the
            input frame's own ``KgFactor`` column (and in the caller's own
            factor table) -- which is what makes the disagreement auditable
            per unit (``groupby('u')`` on this frame).
        ``kg_shipped_source``
            the shipped table's own ``Source`` string for the row, where it
            has one -- Malawi's IHS5 ladder stamps a provenance string on
            every factor and it is worth carrying through.  MISSING wherever
            no usable shipped factor was found, including where the screen
            rejected it -- read it with ``pd.isna``, since pandas 3.0
            normalises both ``None`` and ``pd.NA`` to NaN in an object
            column.

        Two tallies ride on ``.attrs``:

        ``kg_factor_sources``
            ``{layer: n_rows}`` over the INPUT rows.  The five layers of
            :data:`KG_FACTOR_LAYERS` partition the frame and sum to
            ``len(crop_production)`` -- ``shipped`` included, and therefore
            present as ``0`` when no table is passed.  Three further keys ride
            OUTSIDE that partition, since a row they count is still served by
            one of the five: ``reported_implausible`` and
            ``shipped_implausible`` count rows whose offered factor the screen
            REJECTED, and ``shipped_matched`` counts rows the shipped table
            supplied a finite factor for at all -- a JOIN diagnostic taken
            before the sentinel, the screen and the rank, and the right number
            to check when a table looks like it did nothing.  POOLED: on a
            cross-country
            :class:`~lsms_library.feature.Feature` frame these counts run over
            every country at once, so ``reported: 40000`` says nothing about
            WHICH country reported.  Read it as a total, never as coverage;
            the per-row frame answers the real question with one
            ``groupby('country')``.
        ``kg_factor_disagreement``
            For ``reported_vs_shipped``, ``reported_vs_survey_median`` and
            ``reported_vs_inferred``:
            ``{'both': n, 'disagree': k, 'share': k/n or None}``, where
            ``both`` counts rows for which BOTH layers produced a usable
            factor and ``disagree`` counts those differing from the
            *reported* factor by more than
            :data:`KG_FACTOR_DISAGREEMENT_TOLERANCE` in relative terms.
            This is the audit hook: a large ``reported_vs_inferred`` share
            means the library's factor table and the instrument disagree
            about what a unit weighs, and the instrument is the one that was
            there.  ``reported_vs_shipped`` asks the same question of an
            externally shipped table, which is precisely the audit GH #852
            wants: does the World Bank's conversion table agree with what the
            enumerator wrote down?

    Notes
    -----
    Only the reported and survey-median layers are validity-filtered
    (finite, > 0).  The inferred layer is used exactly as
    :func:`_kg_factor_series` returns it, so that a frame with no
    ``KgFactor`` column reproduces the previous behaviour bit for bit rather
    than merely closely.  ``_get_kg_factors`` already refuses non-finite and
    non-positive values at every point where it can mint one, so the
    asymmetry is a guarantee about identity, not a hole.
    """
    df = crop_production
    inferred = _kg_factor_series(df, volume_as_mass=volume_as_mass)
    inferred_arr = inferred.to_numpy(dtype='float64', na_value=np.nan)

    if 'KgFactor' in df.columns:
        reported, rejected = _screen_reported_factors(df, df['KgFactor'])
    else:
        reported = np.full(len(df), np.nan)
        rejected = np.zeros(len(df), dtype=bool)

    sentinel = _unit_sentinel_mask(df)

    if shipped_factors is None:
        shipped = np.full(len(df), np.nan)
        shipped_rejected = np.zeros(len(df), dtype=bool)
        shipped_source = np.full(len(df), pd.NA, dtype=object)
        shipped_matched = 0
    else:
        offered, shipped_source, shipped_matched = _shipped_factor_lookup(
            df, shipped_factors)
        # The missing-unit sentinel excludes this layer too, and BEFORE the
        # screen.  A shipped table is a kg-PER-UNIT table, so a row that
        # recorded no unit has nothing for it to be per; serving one would
        # fabricate a weight, which is the failure U_UNKNOWN exists to
        # prevent.  Excluding first keeps a sentinel row out of
        # `shipped_implausible`, which counts rows where the shipped layer
        # OFFERED an implausible factor -- not rows that would otherwise have
        # been served, since `reported` may outrank the offer anyway.
        offered = np.where(sentinel, np.nan, offered)
        shipped_source = np.where(sentinel, pd.NA, shipped_source)
        shipped, shipped_rejected = _screen_reported_factors(df, offered)
        shipped_source = np.where(np.isnan(shipped), pd.NA, shipped_source)

    if np.isnan(reported).all():
        # Nothing reported -> nothing to take a median of.  Skipping the
        # groupby is not just an optimisation: it keeps the no-KgFactor path
        # free of any pandas operation that could perturb the result.
        survey_median = np.full(len(df), np.nan)
    else:
        survey_median = _survey_median_factors(df, reported,
                                               min_reports=min_reports,
                                               sentinel=sentinel)

    rep_ok = ~np.isnan(reported)
    shp_ok = ~np.isnan(shipped)
    med_ok = ~np.isnan(survey_median)
    inf_ok = ~np.isnan(inferred_arr)

    resolved = np.where(
        rep_ok, reported,
        np.where(shp_ok, shipped,
                 np.where(med_ok, survey_median, inferred_arr)))
    source = np.where(
        rep_ok, 'reported',
        np.where(shp_ok, 'shipped',
                 np.where(med_ok, 'survey_median',
                          np.where(inf_ok, 'inferred', 'none'))))

    out = pd.DataFrame(
        {'kg_per_unit': resolved,
         'KgFactorSource': source,
         'kg_reported': reported,
         'kg_reported_rejected': rejected,
         'kg_shipped': shipped,
         'kg_shipped_rejected': shipped_rejected,
         'kg_shipped_source': shipped_source,
         'kg_survey_median': survey_median,
         'kg_inferred': inferred_arr},
        index=df.index)

    counts = {layer: int((source == layer).sum()) for layer in KG_FACTOR_LAYERS}
    # NOT layers, and deliberately not part of the partition: a rejected row
    # is still SERVED by one of the five (usually `none` or `inferred`), so
    # counting it there as well would double-count it.  The five layers sum
    # to len(df); these two ride alongside as screen counts, and both are
    # present on every call -- `shipped_implausible` is 0 when no table was
    # passed, exactly as `reported_implausible` is 0 without a KgFactor
    # column.
    counts['reported_implausible'] = int(rejected.sum())
    counts['shipped_implausible'] = int(shipped_rejected.sum())
    # A JOIN diagnostic, taken BEFORE the sentinel, the screen and the rank --
    # so it answers "did my table key correctly?", which none of the other
    # counts can.  `shipped` is post-rank and reads 0 for a perfectly-keyed
    # table whose rows `reported` happens to win; `shipped_implausible` reads
    # 0 for a table that matched nothing at all.  Only this one separates a
    # mis-keyed table from a table that was simply outranked.
    counts['shipped_matched'] = shipped_matched
    out.attrs['kg_factor_sources'] = counts
    # A row with no unit recorded is excluded from the audit as well: there
    # is nothing for its reported factor to be compared against, since no
    # per-unit factor -- inferred or median -- can exist for a non-unit.
    audited = np.where(sentinel, np.nan, reported)
    out.attrs['kg_factor_disagreement'] = {
        'reported_vs_shipped': _disagreement(audited, shipped),
        'reported_vs_survey_median': _disagreement(audited, survey_median),
        'reported_vs_inferred': _disagreement(audited, inferred_arr),
        'tolerance': KG_FACTOR_DISAGREEMENT_TOLERANCE,
    }
    return out


def _disagreement(reference, other,
                  tolerance=KG_FACTOR_DISAGREEMENT_TOLERANCE):
    """How often two layers that BOTH have a factor for a row disagree.

    ``both`` is the denominator -- rows where both layers produced a usable
    (finite, > 0) factor; a row only one layer can serve says nothing about
    agreement.  The gap is relative to *reference* (the reported factor),
    because the question being asked is "how wrong is the library's number
    where the survey told us the answer", not the symmetric one.
    """
    ref = _valid_factor(reference)
    oth = _valid_factor(other)
    both = ~np.isnan(ref) & ~np.isnan(oth)
    n = int(both.sum())
    if not n:
        return {'both': 0, 'disagree': 0, 'share': None}
    gap = np.abs(oth[both] - ref[both]) / ref[both]
    k = int((gap > tolerance).sum())
    return {'both': n, 'disagree': k, 'share': k / n}


def harvest_kg(crop_production, *, volume_as_mass=True, carry_native=False,
               min_reports=SURVEY_MEDIAN_MIN_REPORTS, shipped_factors=None):
    """Total harvested kilograms per (t, i, plot, j) from ``crop_production``.

    MECHANICAL reduction (GAP 1 → WB ``Plotcrop``/``Plot`` ``harvest_kg``).
    For each reported harvest row, convert the native-unit ``Quantity`` to
    kilograms, then sum within each plot-crop.

    Each row's kg-per-unit factor is built by a LAYERED procedure, in
    precedence order: the row's own **reported** ``KgFactor`` (what the
    instrument wrote down, e.g. UNPS ``a5?q6d``); a **shipped** factor from
    an externally supplied conversion table, when the caller passes one as
    *shipped_factors*; the **median of reported**
    factors for the same ``(u, condition)`` in the same country-wave, where
    at least ``min_reports`` rows report one; the library's **inferred**
    factor from the shared unit→kg machinery (:func:`_get_kg_factors` via
    :func:`_kg_factor_series` — the same factors
    :func:`food_quantities_from_acquired` applies to ``food_acquired``); and
    otherwise **none**, in which case the row contributes nothing.  The
    survey's own number wins wherever it exists, on the same principle by
    which ``food_acquired``'s exact per-row ``Quantity_kg`` already beats an
    inferred factor.

    Which layer served each row is DISCLOSED, not assumed: the counts ride
    on ``result.attrs['kg_factor_sources']`` and the per-row provenance is
    available from :func:`harvest_kg_factors`, which also reports how often
    the survey's factor and the library's inferred one DISAGREE.  A frame
    with no ``KgFactor`` column can only reach the inferred layer, so it
    gets exactly the result it got before the column existed.

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
    min_reports : int, default :data:`SURVEY_MEDIAN_MIN_REPORTS`
        How many rows of a ``(u, condition)`` group must carry a reported
        ``KgFactor`` before their median is used to fill that group's
        unreported rows.  Inert on a frame with no ``KgFactor`` column.
    shipped_factors : pd.DataFrame, optional
        An externally shipped kg-per-unit table, forwarded verbatim to
        :func:`harvest_kg_factors` -- see there for its shape, the join keys,
        the ambiguity refusal, and why nothing auto-discovers it.  Omitted
        (the default), this whole layer is inert: every ``Harvest_kg`` value
        is bit for bit what it was before the layer existed.  (The counts dict
        gains three zero-valued keys and :func:`harvest_kg_factors` gains
        three columns -- forced by the partition invariant, and no number
        moves.)  Example::

            harvest_kg(
                cp,
                shipped_factors=ethiopia.crop_conversion_factors(...))
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

        ``result.attrs['kg_factor_sources']`` is ``{layer: n_rows}`` over the
        INPUT rows and sums to ``len(crop_production)``.  It counts rows by
        the layer that gave them a FACTOR, not by whether they contributed:
        a ``reported`` row whose ``Quantity`` is 0 or NaN is counted
        ``reported`` and still drops out of the sum below.
        ``result.attrs['kg_factor_disagreement']`` carries the layer-pair
        audit described in :func:`harvest_kg_factors`.

    Notes
    -----
    Divergence from WB: the WB Uganda code applies survey-provided
    conversion tables that cover bespoke local containers ("Sack (100 kgs)",
    "Basket (Unspecified)", regional "Heap"/"Bunch" sizes).  Our shared
    factor map only resolves units that *name* their metric content (e.g.
    "Basket (10 kg)", "Nice cup (60g)") plus the hand-coded metric tokens,
    so on Uganda it converts ~21% of crop rows *through that layer alone*
    (28 147 of 133 683, measured 2026-09-09).  ``Harvest_kg`` is therefore a
    *lower bound* on the WB figure for plots dominated by non-metric
    containers; magnitudes agree on metric-reported plots.

    Closing that gap is a per-country job in three forms, none of them a
    change to this transform: extend the country's ``u``-table
    (``harvest_units``); wire the instrument's own conversion factor into
    the optional ``KgFactor`` column, which the ``reported`` and
    ``survey_median`` layers above then consume; or -- where the survey ships
    a conversion TABLE rather than a per-row factor -- load that table and
    pass it as *shipped_factors*.  The third is the one Ethiopia and Malawi
    need (GH #852, #854): their tables are keyed on crop x unit x region, so
    they can never be a per-row column, and the de-duplication and region
    resolution they require are decisions that belong at the call site rather
    than baked into a parquet.  The second reaches rows the
    first cannot -- Uganda's 2018-19 season-A harvest side ships a
    100%-populated ``a5aq6d`` conversion factor and NO harvest-unit code at
    all, so its 7 153 rows sit at ``u='Unknown'`` and no unit table can ever
    convert them.

    The ``survey_median`` layer REFUSES the missing-unit sentinel, on both
    sides.  ``u='Unknown'`` (:data:`U_UNKNOWN`) is the absence of a unit, not
    a unit, so a "group" of such rows pools containers of unrelated sizes: a
    median over them is a number about nothing, and filling other such rows
    with it would fabricate weights.  A row with no unit recorded therefore
    gets kilograms from its OWN reported ``KgFactor`` or from nothing at all
    -- which is why wiring ``KgFactor`` is the only thing that can ever
    convert Uganda's 2018-19 season-A rows.
    """
    df = crop_production.copy()
    if 'Quantity' not in df.columns:
        raise ValueError("crop_production must have a 'Quantity' column")

    qty = pd.to_numeric(df['Quantity'], errors='coerce')
    factors = harvest_kg_factors(df, volume_as_mass=volume_as_mass,
                                 min_reports=min_reports,
                                 shipped_factors=shipped_factors)
    kg_per_unit = factors['kg_per_unit']
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
    # ``groupby`` drops ``attrs``, so the tallies are re-stashed here -- the
    # same idiom ``food_prices`` uses for ``price_rows_dropped``.
    res.attrs['kg_factor_sources'] = factors.attrs['kg_factor_sources']
    res.attrs['kg_factor_disagreement'] = factors.attrs[
        'kg_factor_disagreement']
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
             volume_as_mass=True, on='parcel',
             min_reports=SURVEY_MEDIAN_MIN_REPORTS, shipped_factors=None):
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
    min_reports : int, default :data:`SURVEY_MEDIAN_MIN_REPORTS`
        Forwarded to :func:`harvest_kg`.
    shipped_factors : pd.DataFrame, optional
        Forwarded to :func:`harvest_kg` -- an externally shipped
        kg-per-unit table (see :func:`harvest_kg_factors`).  **Without it a
        caller asking for yields gets no ``shipped`` layer**, because
        ``yield_kg`` calls ``harvest_kg`` internally; that gap was the one
        concrete cost of the explicit-loader design and this kwarg closes
        it (`.coder/ledger/harvest-kg-shipped-factors.md` section 6 Q1/Q3)::

            yield_kg(cp, pf, shipped_factors=ethiopia.crop_conversion_factors())
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

    hk = harvest_kg(crop_production, volume_as_mass=volume_as_mass,
                    min_reports=min_reports,
                    shipped_factors=shipped_factors).reset_index()
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
class NutrientCoverageWarning(UserWarning):
    """A fertilizer input label resolved to NO nitrogen share.

    :func:`nitrogen_kg` keys its nutrient share off the ``plot_inputs.input``
    label, and a label the map does not recognise contributes NOTHING to the
    total -- silently, and with the right shape: the frame comes back
    non-empty, finite and non-negative, so every naive assertion passes.
    That is the "right shape, no content" failure CLAUDE.md devotes a section
    to (``null_read_audit``), and it happened: before the label
    normalisation landed, Malawi's four canonical fertilizer labels (``NPK
    Fertilizer``, ``Urea Fertilizer``, ``CAN Fertilizer``, ``DAP
    Fertilizer`` -- 53,210 rows) matched none of the map's keys, the ONLY
    label that did match was ``Other Fertilizer`` at a nominal share of 0.0,
    and ``nitrogen_kg(Malawi)`` returned 331 rows of exactly 0.0.

    Fires for an unmatched label that *names itself* a fertilizer input --
    the corpus's own vocabulary, :data:`_FERTILIZER_LABEL_HINTS`
    (fertilizer / fertiliser / manure / compost, plus the Portuguese
    ``adubo`` and French ``engrais``) -- and NOT for ``Seed`` / ``Pesticide``
    / ``Herbicide``, which are correctly outside a nitrogen map and are the
    bulk of the rows (Malawi ``Seed`` alone is 112,296).  A warning nobody
    reads is how the original defect survived, so the firehose form is
    deliberately not used.  Escalates to a louder message when the whole
    returned column is empty or identically zero.

    The full per-label tally always rides on
    ``result.attrs['nitrogen_input_match']`` whether or not this warns --
    that is the coverage check, and it is the twin of
    ``attrs['kg_factor_sources']['shipped_matched']`` in
    :func:`harvest_kg_factors`: a JOIN diagnostic, so "did my labels key
    correctly?" has an answer that does not depend on a warning firing.
    """


class PlotGrainMismatchWarning(UserWarning):
    """Two plot-level features share no land key, so the join produced nothing.

    The corpus runs two plot vocabularies (see :data:`_PLOT_LEVELS` and
    :func:`_parcel_from_crop_plot`), and a transform that joins across them
    on the wrong grain returns an EMPTY frame rather than an error.
    Measured: ``fertilizer_rate(Uganda, on='plot')`` returned ``(0, 1)``
    silently, because the ag modules key plots ``{hhid}-{parcel}-{plot}``
    while ``plot_features`` keys them ``{parcel}_{suffix}``; the same call
    with ``on='parcel'`` returns 509 rows.
    """


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


# Word stems that mark an input label as a FERTILIZER in the corpus's own
# vocabularies -- English (``NPK Fertilizer``, ``Organic Fertilizer``,
# ``Manure``, ``Compost``), Portuguese (Guinea-Bissau's ``Adubos organicos``
# / ``Adubos inorganicos - ureia``) and French.  Used ONLY to decide whether
# an unmatched label is worth warning about; it never affects a nutrient
# share, so a stem missing from this tuple costs a warning, never a number.
_FERTILIZER_LABEL_HINTS = ('fertilizer', 'fertiliser', 'manure', 'compost',
                           'adubo', 'engrais')


def _normalise_input_label(label):
    """Candidate lookup keys for a ``plot_inputs.input`` label, best first.

    ``_NITROGEN_CONTENT`` mixes two naming conventions -- bare product names
    (``urea``, ``npk``, ``dap``) and class names that carry the word
    (``phosphate fertilizer``, ``other fertilizer``) -- because the countries
    do.  Malawi writes ``Urea Fertilizer`` where Benin writes ``Urea``, and
    Senegal writes ``Phosphate`` where the map's key is ``phosphate
    fertilizer``.  Matching on exact lower-cased equality alone therefore
    misses in BOTH directions, and the failure is silent (an unmatched label
    contributes 0 to a nitrogen total).

    So three candidates are tried in order: the normalised label itself,
    then with a trailing ``' fertilizer'`` / ``' fertiliser'`` REMOVED, then
    with ``' fertilizer'`` ADDED.  Exactness is preserved -- no substring or
    fuzzy matching -- so ``Organic Fertilizer`` still resolves to nothing
    (``organic`` is not a key, and asserting a nominal N share for manure
    would be inventing a number), and ``D Compound Fertilizer`` and
    Ethiopia's ``NPS`` likewise stay unmatched and get named in the warning.

    Measured over the 11 warm ``plot_inputs`` tables (2026-09-09): the strip
    rule is what Malawi needs (53,210 rows across NPK / Urea / CAN / DAP
    Fertilizer, previously all unmatched); the add rule recovers the bare
    ``Phosphate`` label in six EHCVM countries (349 rows, nominal share 0.0,
    so it moves no number -- it moves them out of the unmatched tally, which
    is what makes the tally readable).
    """
    base = ' '.join(str(label).lower().split())
    out = [base]
    for suffix in (' fertilizer', ' fertiliser'):
        if base.endswith(suffix):
            out.append(base[:-len(suffix)])
    if not base.endswith(' fertilizer'):
        out.append(base + ' fertilizer')
    return out


def _nitrogen_shares(labels, content):
    """Map input labels to nutrient shares, and tally what did not match.

    Returns ``(share_series, tally)``.  ``tally`` mirrors
    ``harvest_kg_factors``'s ``shipped_matched``: a JOIN diagnostic, taken
    before anything else can hide it.
    """
    resolved = {}
    for label in pd.unique(pd.Series(labels).astype(str)):
        for key in _normalise_input_label(label):
            if key in content:
                resolved[label] = float(content[key])
                break
    share = pd.Series(labels).astype(str).map(resolved).astype(float)
    counts = pd.Series(labels).astype(str).value_counts()
    unmatched = {str(k): int(v) for k, v in counts.items()
                 if str(k) not in resolved}
    tally = {
        'matched_rows': int(sum(v for k, v in counts.items()
                                if str(k) in resolved)),
        'unmatched_rows': int(sum(unmatched.values())),
        'matched_labels': sorted(resolved),
        'unmatched_labels': dict(sorted(unmatched.items(),
                                        key=lambda kv: -kv[1])),
    }
    return share, tally


def _warn_nitrogen_coverage(tally, total_mass, n_rows):
    """Emit :class:`NutrientCoverageWarning` where coverage failed."""
    suspicious = {k: v for k, v in tally['unmatched_labels'].items()
                  if any(h in k.lower() for h in _FERTILIZER_LABEL_HINTS)}
    zero_mass = (n_rows == 0) or not (total_mass > 0)
    if not suspicious and not zero_mass:
        return
    named = ', '.join(f"{k!r} ({v:,} rows)" for k, v in
                      list(suspicious.items())[:8]) or 'none'
    if zero_mass:
        warnings.warn(
            "nitrogen_kg resolved a nitrogen share for "
            f"{tally['matched_rows']:,} row(s) but the result is "
            f"{'empty' if n_rows == 0 else 'identically zero'} -- every "
            "fertilizer label either went unmatched or carries a nominal "
            f"share of 0. Unmatched fertilizer-shaped labels: {named}. "
            "This has the right SHAPE and no content: len() > 0, finite and "
            ">= 0 all pass on a column of zeros. Check "
            "attrs['nitrogen_input_match'] and pass nitrogen_content= with "
            "this country's own labels.",
            NutrientCoverageWarning, stacklevel=3)
    else:
        warnings.warn(
            f"nitrogen_kg: fertilizer-shaped input label(s) resolved to NO "
            f"nitrogen share and contribute nothing to the total: {named}. "
            "That may be correct (organic manure and compost carry no "
            "nominal share in _NITROGEN_CONTENT, deliberately -- asserting "
            "one would invent a number) but it is not visible in the "
            "returned frame. Full tally on "
            "attrs['nitrogen_input_match'].",
            NutrientCoverageWarning, stacklevel=3)


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
        fertilizer at all are absent.  ``attrs['nitrogen_input_match']``
        carries the label-join tally -- ``matched_rows`` /
        ``unmatched_rows`` / ``matched_labels`` / ``unmatched_labels``
        (label -> row count, descending).

    Notes
    -----
    **Label matching is normalised, and what it cannot resolve is LOUD**
    (2026-09-09).  The share is keyed off the ``input`` label through
    :func:`_normalise_input_label`, which tries the normalised label, then
    the label with a trailing ``' fertilizer'`` removed, then with one
    added -- because the corpus writes the same product three ways
    (``Urea`` in Benin, ``Urea Fertilizer`` in Malawi, ``Phosphate`` where
    the key is ``phosphate fertilizer``).  Exact-equality matching alone
    silently zeroed **53,210 Malawi rows** (``NPK`` / ``Urea`` / ``CAN`` /
    ``DAP Fertilizer``), leaving ``Other Fertilizer`` at a nominal 0.0 as
    the only match and returning 331 plots of exactly ``0.0`` -- a frame
    that is non-empty, finite and non-negative, so nothing caught it.
    A label that still resolves to nothing and *names itself* a fertilizer
    input now raises :class:`NutrientCoverageWarning`; the full tally rides
    on ``attrs`` either way.  Matching stays EXACT after normalisation: no
    substring or fuzzy match, so ``Organic Fertilizer``, ``D Compound
    Fertilizer`` and Ethiopia's ``NPS`` stay unmatched and get named rather
    than being assigned an invented nominal share.
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
        inp = df.index.get_level_values('input').astype(str)
    elif 'input' in df.columns:
        inp = df['input'].astype(str)
    else:
        raise ValueError("plot_inputs must have an 'input' level or column")
    # Labels are passed through VERBATIM (not pre-lowered) so the tally and
    # the warning name them as the country writes them; the normalisation
    # lives in _normalise_input_label, where the three candidate spellings
    # are documented together.
    n_share, _match_tally = _nitrogen_shares(inp, content)
    n_share.index = df.index

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
    # A JOIN diagnostic, always present, taken before any reduction can hide
    # it -- the twin of harvest_kg_factors' `shipped_matched`.
    res.attrs['nitrogen_input_match'] = _match_tally
    _warn_nitrogen_coverage(_match_tally,
                            float(res['Nitrogen_kg'].sum()) if len(res) else 0.0,
                            len(res))
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


# WFP standard reduced-Coping-Strategies-Index weights, keyed on our
# canonical ``food_coping`` Strategy labels.  Malawi, Burkina Faso and Mali
# field exactly this five-item battery (see e.g.
# ``Malawi/_/data_scheme.yml:162-164``: "a=LessPreferred, b=LimitPortion,
# c=ReduceMeals, d=RestrictAdults, e=BorrowFood").  EPAR's own Malawi rename
# (``Malawi IHS/Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:4520-4524``) maps
# hh_h02a..e -> strategy1..5 in EXACTLY that order, each rename line's own
# comment naming its severity weight (1/1/1/3/2); ``:4525`` sums them
# (``rcsi = strategy1 + strategy2 + strategy3 + 3*strategy4 +
# 2*strategy5``), and ``:4519`` cites the coefficients' own source: "Weights
# from The Coping Strategies Index: Field Methods Manual (2008)" -- so the
# weights below are that manual's, read through EPAR's Malawi renaming, not
# an unsourced positional list.
_RCSI_WFP_WEIGHTS = {
    'LessPreferred': 1,
    'BorrowFood': 2,
    'LimitPortion': 1,
    'RestrictAdults': 3,
    'ReduceMeals': 1,
}


def rcsi(food_coping, *, weights=None):
    """Reduced Coping Strategies Index (WFP rCSI; WB/EPAR ``rcsi``).

    MECHANICAL reduction over the ``food_coping`` item table: a weighted sum
    of day-counts across the coping strategies, ``Σ weight[Strategy] ×
    Days``, one score per household-wave.

    Parameters
    ----------
    food_coping : pd.DataFrame
        ``food_coping`` item feature, grain ``(t, i, Strategy)``, with an
        integer ``Days`` column (0-7, days in the past 7 the strategy was
        used).
    weights : dict[str, float], optional
        Strategy label -> weight.  Defaults to :data:`_RCSI_WFP_WEIGHTS`,
        the standard five-strategy WFP formula (``LessPreferred +
        LimitPortion + ReduceMeals + 3×RestrictAdults + 2×BorrowFood``).

        **The strategy set is a property of the questionnaire, not a
        universal constant** -- do not treat the five-term formula as the
        definition.  Ethiopia and Tanzania field an 8-item battery (the
        standard five plus ``LimitVariety`` / ``NoFood`` /
        ``WholeDay``/``WholeDayWithout``: ``Ethiopia/_/ethiopia.py:785-794``,
        ``Tanzania/_/data_scheme.yml:95-98``); Nigeria fields 9
        (``Nigeria/_/nigeria.py`` ``FOOD_COPING_ITEMS``, adding
        ``SleepHungry`` / ``WholeDayNoFood``).  EPAR's OWN Tanzania rCSI is
        an eight-term variant with different weights on the extra items
        (``Tanzania NPS/Tanzania NPS Wave 5/EPAR_UW_Tanzania_NPS_W5.do:2617``,
        ``hh_h02a + hh_h02b + hh_h02c + hh_h02d + 3*hh_h02e + hh_h02f*2 +
        hh_h02g*4 + hh_h02h*4``) -- proof that a wider battery does not
        collapse to the five-term formula by just ignoring the extra
        columns.  The check is symmetric: a ``Strategy`` label present in
        ``food_coping`` but absent from ``weights`` raises
        :class:`ValueError` naming it (never silently scored on just the
        terms it recognises), and a ``weights`` key that names a label
        ``food_coping`` never carries ALSO raises (never silently scored on
        fewer terms than the formula claims -- a country fielding only 4 of
        the 5 standard strategies is equally "not the standard five").  Pass
        an explicit ``weights`` dict naming every label present, and no
        others, for Ethiopia, Tanzania and Nigeria.

    Returns
    -------
    pd.DataFrame
        One float ``rCSI`` column indexed by ``(t, i)``.

    Notes
    -----
    **Missing strategy rows are NEVER filled with zero.** A household using
    a strategy zero days is a valid, STORED response (``Days=0``);
    ``food_coping`` is sparse only where a strategy question went
    unanswered or its response was out-of-domain and dropped.  Evidence:

    - Ethiopia's own wave-builder docstring
      (``Ethiopia/_/ethiopia.py:814-815``): "Rows with a missing Days value
      are dropped (the strategy was not answered for that household)."
    - Nigeria's (``Nigeria/_/nigeria.py:985-987``): "Rows where the day
      count is missing are dropped; a household with all-missing items
      contributes no rows."
    - Malawi's audited row counts (``Malawi/_/CONTENTS.org:24-35``): rows
      are *exactly* 5x each wave's Module-H household count in 2010-11
      (12,271 x 5 = 61,355) and short by a handful in the other three
      waves (6 / 3 / 13 rows) -- the out-of-range day-counts ``malawi.py``
      coerces to NaN and drops (``Malawi/_/malawi.py:1204,1212``), never
      zero-filled.

    So a household present for some weighted strategies and absent for
    another had that strategy UNANSWERED, not zero, and is excluded from
    the score entirely -- never scored on a truncated sum.  Any ``(t, i)``
    missing so much as one of the strategies named in ``weights`` is
    dropped, not imputed.
    """
    df = food_coping
    names = list(df.index.names or [])
    if 'Strategy' not in names:
        raise ValueError("food_coping must have a 'Strategy' index level")
    if 'Days' not in df.columns:
        raise ValueError("food_coping must have a 'Days' column")
    group_by = [n for n in ['t', 'i'] if n in names]
    if not group_by:
        raise ValueError("food_coping must have 't' and/or 'i' index levels")

    w = dict(_RCSI_WFP_WEIGHTS) if weights is None else dict(weights)

    strategy = df.index.get_level_values('Strategy').astype(str)
    present = sorted(set(strategy))
    unknown = sorted(set(present) - set(w))
    if unknown:
        raise ValueError(
            f"rcsi: unrecognised Strategy label(s) {unknown} in food_coping "
            "-- the five-term WFP formula is not universal (Ethiopia/"
            "Tanzania field an 8-item battery, Nigeria 9; EPAR's own "
            "Tanzania rCSI is an eight-term variant with different "
            "weights). Pass weights= naming every Strategy label present."
        )
    # Symmetric with the check above: a weight naming a Strategy this table
    # never carries is exactly as wrong as an unrecognised Strategy -- the
    # strategy set (and therefore the term count) is a property of the
    # questionnaire, not a default to fall back on for a narrower battery.
    missing = sorted(set(w) - set(present))
    if missing:
        raise ValueError(
            f"rcsi: weights name Strategy label(s) {missing} that food_coping "
            "does not carry at all -- pass a weights= dict matching exactly "
            "the Strategy labels present, not a superset."
        )

    used = list(w)

    days = pd.to_numeric(df['Days'], errors='coerce')
    s = pd.Series(days.to_numpy(), index=df.index)
    wide = s.unstack('Strategy')
    if list(wide.index.names) != group_by:
        # The canonical grain is (t, i, Strategy), but Country.food_coping()
        # arrives with the joined cluster level ``v`` as well, so this branch
        # fires on every real-country call.  ``v`` is one-to-one with
        # (t, i) (measured on Malawi, red-team 2026-09-09), so ``first()``
        # is lossless there; it would not be if a country ever served two
        # rows per (t, i, Strategy), which the canonical schema forbids.
        wide = wide.groupby(level=group_by).first()
    wide = wide.reindex(columns=used)

    # Missing strategy rows mean "not answered" / dropped-invalid (see
    # Notes above) -- exclude the household from the score entirely rather
    # than imputing a zero.
    complete = wide.dropna(how='any')
    weight_vec = pd.Series({k: w[k] for k in used}, dtype=float)
    score = complete[used].mul(weight_vec, axis=1).sum(axis=1)
    return score.to_frame('rCSI').sort_index()


def rcsi_phase(rcsi, *, cutoffs=(3, 18, 42)):
    """WFP rCSI severity phase, in partitioning closed intervals.

    Parameters
    ----------
    rcsi : pd.Series or pd.DataFrame
        rCSI score(s) -- e.g. the ``rCSI`` column returned by
        :func:`rcsi` (the frame's single column is used if a DataFrame is
        passed).
    cutoffs : (int, int, int), default (3, 18, 42)
        Three ascending cut points splitting the non-negative score line
        into four phases: ``[0, c1]``, ``(c1, c2]``, ``(c2, c3]``,
        ``(c3, inf)``.

    Returns
    -------
    pd.Series
        Ordered categorical (``'Phase 1'`` .. ``'Phase 4'``), same index as
        ``rcsi``.

    Notes
    -----
    Deliberately closed-interval and PARTITIONING -- every real number
    (hence every integer score) lands in exactly one phase.  This avoids a
    real defect in EPAR's own cutoffs
    (``Malawi IHS/Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:4530-4531``):
    phase 2 there is ``rcsi <= 18`` and phase 3 is
    ``rcsi > 19 & rcsi <= 42`` -- an ``rcsi`` of exactly 19 satisfies
    neither test and falls in NO phase, even though the phase-3 LABEL one
    line later (``:4535``) claims the range is "(19 - 42)", i.e. the label
    and the test contradict each other.  Here ``cutoffs=(3, 18, 42)``
    default gives ``[0, 3]``, ``(3, 18]``, ``(18, 42]``, ``(42, inf)`` --
    19 lands in the third phase (``(18, 42]``), matching the LABEL EPAR
    intended rather than the gap its test actually produced.
    """
    c1, c2, c3 = cutoffs
    if isinstance(rcsi, pd.DataFrame):
        values = rcsi.iloc[:, 0]
    else:
        values = rcsi
    values = pd.to_numeric(values, errors='coerce')

    labels = ['Phase 1', 'Phase 2', 'Phase 3', 'Phase 4']
    bins = [-np.inf, c1, c2, c3, np.inf]
    phase = pd.cut(values, bins=bins, labels=labels, ordered=True,
                   right=True)
    phase.name = 'rCSI_phase'
    return phase


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


# ---------------------------------------------------------------------------
# GH #863 -- the remaining EPAR-parity transforms (WORKPLAN.org Phase 2).
#
# PLACEMENT NOTE: three of the six below (``livestock_sales_value``, ``hdds``,
# ``fcs``) are METHODOLOGY transforms and by classification belong under the
# "Phase 2 -- METHODOLOGY transforms" banner further down.  They sit here, in
# one contiguous block, purely for concurrent-edit hygiene (the harvest_kg
# family and ``median_price_valuation`` were being edited in parallel when
# this landed).  Each docstring carries its own MECHANICAL / METHODOLOGY
# classification -- read that, not the position in the file.
# ---------------------------------------------------------------------------

# Crop-identity level names emitted by the various countries' crop features.
# ``j`` is the canonical (Uganda, Niger, Togo) name; ``crop`` is Malawi's.
# The two vocabularies are the same axis (both the harmonize_crop ->
# harmonize_food Preferred Label), which is why ``data_info.yml``'s
# ``index_info`` does not yet register the plot-level ag features: "their
# per-country index NAMES diverge (plot vs plot_id, crop vs j)".  Twin of
# :data:`_PLOT_LEVELS`.
_CROP_LEVELS = ('j', 'crop')


def _resolve_crop_level(names):
    """Return whichever of :data:`_CROP_LEVELS` is present in ``names``.

    Twin of :func:`_resolve_plot_level`: lets a transform group by the crop
    grain regardless of whether the country's feature names its crop level
    ``j`` (Uganda) or ``crop`` (Malawi).  Returns ``None`` when neither is
    present.
    """
    for name in _CROP_LEVELS:
        if name in (names or []):
            return name
    return None


def gross_crop_revenue(crop_production, *, by=None, value_col='Value_sold'):
    """Reported crop SALE revenue per household (EPAR ``value_crop_sales``).

    MECHANICAL reduction.  Sums ``crop_production.Value_sold`` -- the sale
    value the household actually REPORTED -- to the ``(t, i)`` grain, or to
    ``(t, i, crop)`` with ``by='j'``.

    Parameters
    ----------
    crop_production : pd.DataFrame
        ``crop_production`` item feature carrying a reported sale-value
        column.  Grain varies by country: ``(t, i, plot, j, u, condition,
        season)`` in Uganda, ``(t, i, plot, crop, u)`` in Malawi.
    by : {None, 'j', 'crop'}, optional
        ``None`` (default) reduces all the way to ``(t, i)``.  Any of
        ``'j'`` / ``'crop'`` keeps the crop level, whichever name this
        country uses for it (resolved via :func:`_resolve_crop_level`, so
        ``by='j'`` works on Malawi's ``crop`` level too) -- the output level
        keeps the table's own name.
    value_col : str, default 'Value_sold'
        Reported sale-value column to sum.

    Returns
    -------
    pd.DataFrame
        One ``Gross_crop_revenue`` column, in nominal local currency,
        indexed by ``(t, i)`` (or ``(t, i, <crop level>)``).

    Notes
    -----
    **This is NOT EPAR's "gross value of crop production", and the two must
    not be compared as if they were.**  EPAR's construct
    (``Uganda UNPS Wave 5/EPAR_UW_Uganda_UNPS_W5.do``, GROSS CROP REVENUE
    section) values the *entire* harvest -- including the share never sold,
    which for a smallholder panel is most of it -- at a median unit price
    imputed from observed sales on a geographic ladder.  That is a
    METHODOLOGY transform, and the library already has it:
    :func:`median_price_valuation`, whose ladder (finest geographic cell
    with >= 10 priced observations, then a national fallback) is the same
    one EPAR and the WB both use.  Compose the two -- value the harvest with
    ``median_price_valuation``, then sum -- to get their quantity.  This
    function deliberately reports only what the instrument wrote down.

    **No de-duplication, by design (count, never clip).**  Where a country
    attached a household-crop sale total to more than one physical row, a
    plain sum would double-count it.  It does not: measured on the warm
    corpus (2026-09-09), at Uganda's finest key ``(t, i, plot, j, season)``
    only 963 of 44,598 groups hold more than one non-zero sale row, and in
    just **37** of those do all rows share ONE ``Value_sold`` -- the
    stamping shape.  926 carry genuinely differing values, which is what
    the instrument implies: UNPS question 6 is compound (how much, in what
    unit, in what condition) and the sale questions ride the SAME record, so
    each condition row carries its own quantity-sold and value-sold
    (``Uganda/_/CONTENTS.org`` §"Harvest condition", and the slot-2 column
    list ``s5bq07a_2`` / ``s5bq08_2``).  Summing across ``u`` / ``condition``
    / ``season`` is therefore correct, and 895 exact ``(t, i, crop)``
    repeats out of 45,592 rows (1.9% of value) are ordinary coincidence --
    two plots of one crop sold in equal quantity for equal money.

    **On Malawi the risk runs the OTHER way: this is a systematic LOWER
    BOUND, not a double count.**  Its sale is a HOUSEHOLD-crop question that
    ``data_scheme.yml`` records as "attached to single-plot crops only", and
    the consequence is measurable: of 87,440 ``(t, i, crop)`` groups grown
    on ONE plot, 23,206 (**26.5%**) carry a sale row, against 514 of 19,311
    (**2.7%**) for crops grown on more than one plot.  A crop spread over
    two or more plots is almost never credited with a sale at all, by
    instrument design -- so Malawi's ``Gross_crop_revenue`` UNDER-states
    sales, and the shortfall is concentrated in exactly the households
    farming the most land.  Do not read a Malawi total as complete.

    A household present in ``crop_production`` that reported no sale at all
    is ABSENT from the output, never 0 -- ``sum(min_count=1)``, the same
    never-zero-fill rule as :func:`rcsi`.  Reindex against ``sample()`` if
    you want the non-sellers as explicit zeros.
    """
    df = crop_production
    if value_col not in df.columns:
        raise ValueError(
            f"crop_production must have a {value_col!r} column")
    names = list(df.index.names or [])
    group_by = [n for n in ['t', 'i'] if n in names]
    if not group_by:
        raise ValueError("crop_production must have 't' and/or 'i' levels")

    if by is not None:
        if by not in _CROP_LEVELS:
            raise ValueError(
                f"gross_crop_revenue by= must be one of {_CROP_LEVELS} or "
                f"None, got {by!r}")
        crop_level = _resolve_crop_level(names)
        if crop_level is None:
            raise ValueError(
                "crop_production has no crop level to group by (looked for "
                f"{_CROP_LEVELS})")
        group_by = group_by + [crop_level]

    value = pd.to_numeric(df[value_col], errors='coerce')
    out = pd.DataFrame({'Gross_crop_revenue': value})
    return (out.groupby(group_by, dropna=False)['Gross_crop_revenue']
               .sum(min_count=1)
               .dropna()
               .to_frame('Gross_crop_revenue')
               .sort_index())


def livestock_sales_value(livestock, *, price='reported',
                     price_col='ValuePerAnimal',
                      sales_value_col='SalesValue',
                      head_col='HeadSold'):
    """Value of livestock SOLD ALIVE (one term of EPAR's ``livestock_income``).

    **Named for what it returns, not for EPAR's construct.**  EPAR's
    ``livestock_income`` is a NET income over seven terms; this is the
    single ``value_lvstck_sold`` term, a GROSS SALES VALUE, and the function
    name matches its output column ``Livestock_sales_value`` so the call
    site and the column cannot drift apart.  See the Notes for the six terms
    that are missing and why none of them is computable here.

    METHODOLOGY transform.  ``HeadSold x price`` per ``(t, i, animal)``,
    where the price basis is chosen EXPLICITLY by the caller -- because the
    library's per-head value column is not one price concept but two, and
    which one a row carries depends on the country and the wave.

    Parameters
    ----------
    livestock : pd.DataFrame
        ``livestock`` item feature, grain ``(t, i, animal)``, with a
        ``HeadSold`` column.
    price : {'reported', 'sales_value'} or pd.Series, default 'reported'
        The valuation basis.

        - ``'reported'`` -- multiply ``HeadSold`` by ``price_col``
          (``ValuePerAnimal``) AS THE COUNTRY CARRIES IT.  **Read the
          instrument note below before using this**: in most countries that
          is a reservation price, not a transaction price.
        - ``'sales_value'`` -- do not value anything; return the country's
          own reported ``SalesValue`` (gross value of animals sold in the
          recall window).  Where this column exists it is a REPORTED
          number and should be preferred to any construction, exactly as
          ``crop_production.KgFactor`` and ``food_acquired.Quantity_kg`` are
          preferred over inferred factors.  Raises if the country has no
          such column.
        - a ``pd.Series`` -- a caller-supplied per-animal price, indexed by
          ``(t, animal)`` or by ``animal`` alone (e.g. the output of a
          :func:`median_price_valuation`-style ladder over sale
          transactions, which is what EPAR does:
          ``Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:3320-3346`` walks
          ea -> ta -> district -> region -> country, adopting each cell's
          weighted median ``price_per_animal`` at N >= 10).  Every
          ``(t, animal)`` with ``HeadSold > 0`` must be priced or a
          ``ValueError`` names the gaps -- no silent partial valuation.
    price_col, sales_value_col, head_col : str
        Column names, overridable for a country that spells them
        differently.

    Returns
    -------
    pd.DataFrame
        Indexed by ``(t, i, animal)`` with

        - ``Livestock_sales_value`` -- value of the head sold, nominal local
          currency.  ADDITIVE over ``animal``: ``groupby(['t','i']).sum()``
          gives the household total.
        - ``ValuationSource`` -- per-row provenance, one of
          ``'reported_per_head'`` / ``'reported_sales_value'`` /
          ``'supplied_price'``.

        Rows whose ``HeadSold`` or price is missing are DROPPED, not zeroed
        (the never-zero-fill rule).  ``HeadSold == 0`` with a price is a
        genuine zero and is KEPT.

    Notes
    -----
    **What this is a term OF, not a replacement for.**  EPAR's
    ``livestock_income`` (``EPAR_UW_Malawi_IHS_W1.do:5959``) is

        ``value_slaughtered + value_lvstck_sold - value_livestock_purchases
        + (milk + eggs + other products + manure sales)
        - (hired labour + fodder + vaccine costs)``

    i.e. a NET income.  This function computes **only** the
    ``value_lvstck_sold`` term (``:3345``).  The other six are not
    computable from our ``livestock`` schema and saying so is the point:

    - *slaughtered* -- no head-slaughtered count in any country's
      ``livestock``;
    - *purchases* -- ``HeadAcquired`` is a head count with no purchase
      value attached;
    - *products* (milk, eggs, manure) and *expenses* (fodder, water,
      vaccines, hired labour) -- no such columns anywhere in the schema.
      ``FINDINGS_taxonomy.org`` (LIVESTOCK INCOME row) records both as
      THEIRS-ONLY at the item layer.  GhanaSPS's ``data_scheme.yml:406-414``
      notes it holds per-species expense and revenue questions that are
      deliberately unwired because they have no canonical home.

    So the returned number is a GROSS SALES VALUE.  Do not label it
    "livestock income" in published output without the six missing terms.

    **The price column is two different questions.**  Per
    ``lsms_library/data_info.yml`` (``Columns: livestock: ValuePerAnimal``),
    both variants are per head, but they are not the same economics:

    - RESERVATION price -- "if you would sell one of the [ANIMAL] today,
      how much would you receive?", reported whether or not anything was
      sold.  **Nigeria** (``s11iq3`` W1-W4, ``s11iq7`` W5 --
      ``Nigeria/_/data_scheme.yml:337-343``), **Malawi** (``ag_r04`` --
      ``Malawi/_/CONTENTS.org:336-344``), **Uganda 2009-10 / 2010-11**
      (``a6aq6``).
    - REALISED average -- "what was, on average, the value of each sold?",
      defined only where the household sold.  **Uganda 2011-12 onward**
      (``a6aq14b`` / ``s6aq14b``), which dropped the sell-today question;
      its non-null rate falls from ~96% to ~21% at that wave and every
      non-null row from then on is conditional on a sale.

    A reservation price is a *willingness-to-accept*, systematically
    unequal to the price a household actually got.  Valuing sales with it
    is a modelling choice, which is why ``price=`` has no silent default
    that hides which one you used.

    **Two countries report the sale value directly and should not be
    constructed at all**: **Ethiopia** (``ls_s8aq60``, "total value of sales
    of [LIVESTOCK] in the last 12 months", ``Ethiopia/_/data_scheme.yml:
    196-206``) and **Mali** (``s4aq24`` / ``s8b1q14``, "valeur brute des
    ventes") carry ``SalesValue`` and no per-head price.  Use
    ``price='sales_value'`` there.  **GhanaSPS has no REPORTED price basis
    at all**: its value question is a HERD total (``HerdValue``, "current
    value of these animals if you sold all of them"), and its revenue
    questions bundle animals with products, so neither ``ValuePerAnimal``
    nor ``SalesValue`` is declared -- deliberately
    (``GhanaSPS/_/data_scheme.yml:406-414``).  It DOES declare ``HeadSold``
    (``:465``, ``optional: true``), so the only basis that works there is a
    caller-supplied ``price=`` Series; neither ``'reported'`` nor
    ``'sales_value'`` will resolve.
    """
    df = livestock
    if head_col not in df.columns:
        raise ValueError(
            f"livestock must have a {head_col!r} column -- "
            "livestock_sales_value values head SOLD, and a roster without "
            "it carries no sale flow "
            "at all. Fourteen countries declare HeadSold (grep "
            "'^    HeadSold:' over countries/*/_/data_scheme.yml for the "
            "current list); on GhanaSPS it is declared `optional: true`, so "
            "it can be present and all-null in a wave.")
    names = list(df.index.names or [])
    if 'animal' in names:
        animal = pd.Series(
            df.index.get_level_values('animal').astype(str), index=df.index)
    elif 'animal' in df.columns:
        animal = df['animal'].astype(str)
    else:
        raise ValueError("livestock must have an 'animal' level or column")

    head = pd.to_numeric(df[head_col], errors='coerce')

    if isinstance(price, pd.Series):
        source = 'supplied_price'
        wave = (pd.Series(df.index.get_level_values('t'), index=df.index)
                if 't' in names else None)
        pnames = list(price.index.names or [])
        keyed_by_wave = (wave is not None and len(pnames) == 2
                         and set(pnames) == {'t', 'animal'})
        if keyed_by_wave:
            key = pd.MultiIndex.from_arrays(
                [wave.to_numpy(), animal.to_numpy()], names=['t', 'animal'])
            lookup = price.reorder_levels(['t', 'animal'])
            unit = pd.Series(lookup.reindex(key).to_numpy(), index=df.index)
        elif len(pnames) <= 1:
            unit = animal.map(price).astype(float)
        else:
            raise ValueError(
                "livestock_sales_value price= Series must be indexed by "
                f"('t', 'animal') or by 'animal' alone, got {pnames!r}")
        unit = pd.to_numeric(unit, errors='coerce')
        # Symmetric with rcsi's unrecognised-label check: a sale we cannot
        # price is a hole in the aggregate, never a silent omission.
        gaps = unit.isna() & head.notna() & (head > 0)
        if gaps.any():
            missing = sorted(set(
                zip(wave[gaps] if wave is not None else ['?'] * int(gaps.sum()),
                    animal[gaps])))[:10]
            raise ValueError(
                f"livestock_sales_value: {int(gaps.sum())} row(s) with "
                f"{head_col} > 0 have no supplied price; first missing keys "
                f"{missing}. Supply a price for every (t, animal) that sold, "
                "or filter the frame first -- a partial valuation would "
                "under-report the aggregate silently.")
        value = head * unit
    elif price == 'sales_value':
        source = 'reported_sales_value'
        if sales_value_col not in df.columns:
            raise ValueError(
                f"livestock has no {sales_value_col!r} column -- "
                "price='sales_value' returns the country's OWN reported sale "
                "total and cannot be synthesised. Ethiopia and Mali carry it; "
                "Uganda, Malawi and Nigeria carry ValuePerAnimal instead "
                "(use price='reported'); GhanaSPS carries neither.")
        value = pd.to_numeric(df[sales_value_col], errors='coerce')
    elif price == 'reported':
        source = 'reported_per_head'
        if price_col not in df.columns:
            raise ValueError(
                f"livestock has no {price_col!r} column -- price='reported' "
                "needs a per-head value. Ethiopia and Mali report SalesValue "
                "instead (use price='sales_value'); GhanaSPS reports only a "
                "herd total and supports neither.")
        unit = pd.to_numeric(df[price_col], errors='coerce')
        value = head * unit
    else:
        raise ValueError(
            "livestock_sales_value price= must be 'reported', "
            f"'sales_value', or a pd.Series of per-animal prices, got "
            f"{price!r}")

    out = pd.DataFrame({'Livestock_sales_value': value})
    out = out.dropna(subset=['Livestock_sales_value'])
    out['ValuationSource'] = source
    # Canonical (t, i, animal) level order, like every other transform in
    # this block.  The input frame's own order is NOT preserved: warm Malawi
    # arrives as (i, t, animal), and returning that would make a .join() or
    # .reindex() against gross_crop_revenue / crop_diversity -- which build
    # an explicit ['t', 'i'] group list -- silently misalign.
    names = list(out.index.names or [])
    canonical = [n for n in ('t', 'i', 'animal') if n in names]
    if canonical and canonical != names[:len(canonical)]:
        out = out.reorder_levels(canonical + [n for n in names
                                              if n not in canonical])
    return out.sort_index()


# The twelve FAO household dietary-diversity food groups, in the order of
# Table 1 of *Guidelines for Measuring Household and Individual Dietary
# Diversity* (Kennedy, Ballard & Dop, FAO 2011).  These are GROUP NAMES
# only -- the library ships NO item->group mapping, because which of a
# country's `j` labels belong in which group is a per-country curation over
# that country's own `food_items` / `harmonize_food` table, not a constant.
HDDS_GROUPS = (
    'Cereals',
    'White roots and tubers',
    'Vegetables',
    'Fruits',
    'Meat',
    'Eggs',
    'Fish and other seafood',
    'Legumes, nuts and seeds',
    'Milk and milk products',
    'Oils and fats',
    'Sweets',
    'Spices, condiments and beverages',
)

# WFP standard Food Consumption Score weights (WFP VAM, *Food Consumption
# Analysis: Calculation and Use of the Food Consumption Score in Food
# Security Analysis*, Technical Guidance Sheet, 2008), eight groups.
# Condiments are a ninth group carrying weight 0 in that sheet and are
# deliberately NOT included here: a country whose mapping needs them passes
# ``weights={**FCS_WFP_WEIGHTS, 'Condiments': 0.0}`` explicitly, so the
# decision to score a ninth group is visible in the call.
FCS_WFP_WEIGHTS = {
    'Main staples': 2.0,     # cereals AND tubers -- one group in the FCS
    'Pulses': 3.0,
    'Vegetables': 1.0,
    'Fruit': 1.0,
    'Meat and fish': 4.0,
    'Milk': 4.0,
    'Sugar': 0.5,
    'Oil': 0.5,
}


def _resolve_food_groups(groups, canonical, present, *, what):
    """Validate a ``{group: [j, ...]}`` mapping and invert it to ``{j: group}``.

    Shared by :func:`hdds` and :func:`fcs`.  Three checks, all raising:

    1.  The mapping's key set must equal ``canonical`` EXACTLY.  An empty
        list is the way to say "this survey fields nothing in this group" --
        explicitly, which is the point.  (Same symmetric rule as
        :func:`rcsi`: a formula that claims 12 groups and silently scores 9
        is not the formula.)
    2.  **No ``j`` may appear in two groups.**  The mapping is a PARTITION.
        This is not a hypothetical: EPAR's own Tanzania HDDS ``recode``
        (``Tanzania NPS Wave 5/EPAR_UW_Tanzania_NPS_W5.do:2538-2560``)
        assigns itemcode ``704`` to both FRUITS and SWEETS and itemcode
        ``1003`` to both OILS AND FATS and SPICES/CONDIMENTS/BEVERAGES.
        Stata's ``recode`` takes the FIRST matching rule, so 704 silently
        became FRUITS and 1003 silently became OILS -- the second assignment
        never fired, and nothing in the code says so.  A ``dict`` keyed the
        other way round (``{j: group}``) could not even express the defect,
        because Python would dedupe the key; that is why the mapping is
        taken group-first.
    3.  Every ``j`` present in the table must be mapped.  An unmapped item
        would silently deflate the score.  (The converse is NOT checked: a
        mapping may name items a particular slice does not contain -- a
        single-wave or single-region frame legitimately lacks most of a
        175-item vocabulary.)
    """
    if not isinstance(groups, Mapping):
        raise TypeError(
            f"{what}: groups= must be a mapping {{group: [j, ...]}}, got "
            f"{type(groups).__name__}")
    canonical = list(canonical)
    keys = set(groups)
    unknown = sorted(keys - set(canonical))
    missing = sorted(set(canonical) - keys)
    if unknown or missing:
        raise ValueError(
            f"{what}: groups= must name exactly the {len(canonical)} "
            f"groups {canonical}. "
            + (f"Unrecognised: {unknown}. " if unknown else '')
            + (f"Not named: {missing} -- pass an empty list for a group this "
               "survey does not field, rather than omitting it."
               if missing else ''))

    seen = {}
    duplicated = {}
    for group, items in groups.items():
        if isinstance(items, str):
            raise TypeError(
                f"{what}: groups[{group!r}] must be a sequence of j labels, "
                "not a bare string")
        for item in items:
            item = str(item)
            if item in seen and seen[item] != group:
                duplicated.setdefault(item, [seen[item]]).append(group)
            else:
                seen[item] = group
    if duplicated:
        pairs = {k: v for k, v in sorted(duplicated.items())}
        raise ValueError(
            f"{what}: groups= is not a partition -- item(s) {pairs} appear in "
            "more than one food group. EPAR's own Tanzania mapping has "
            "exactly this defect (itemcodes 704 and 1003, W5.do:2538-2560), "
            "and Stata's `recode` silently kept the first assignment. Decide "
            "which group each item belongs to.")

    unmapped = sorted(set(map(str, present)) - set(seen))
    if unmapped:
        shown = unmapped[:15]
        raise ValueError(
            f"{what}: {len(unmapped)} food item(s) present in the table are "
            f"not mapped to any group, e.g. {shown}. An unmapped item is "
            "silently excluded from the score, so the mapping must cover the "
            "table's whole j vocabulary (filter the frame first if you mean "
            "to exclude them).")
    return seen


def _consumed_mask(df, *, what):
    """Boolean: this ``food_acquired`` row records a positive acquisition.

    ``Quantity > 0`` OR ``Expenditure > 0``.  Both are checked because a
    country can record an expenditure with no quantity (a purchase in an
    unrecorded unit) and vice versa (own production).  Measured on the warm
    Malawi ``food_acquired`` (971,531 rows, 2026-09-09): 971,165 rows have
    ``Quantity > 0``, 302 have exactly 0 and 64 are null -- so the mask is
    "essentially every row", and it exists to catch the 366 that are not.
    """
    cols = [c for c in ('Quantity', 'Expenditure') if c in df.columns]
    if not cols:
        raise ValueError(
            f"{what}: food_acquired must carry 'Quantity' and/or "
            "'Expenditure' to establish that an item was acquired at all")
    mask = pd.Series(False, index=df.index)
    for col in cols:
        mask = mask | (pd.to_numeric(df[col], errors='coerce') > 0)
    return mask


def hdds(food_acquired, *, groups):
    """Household Dietary Diversity Score (FAO HDDS; EPAR ``number_foodgroup``).

    METHODOLOGY transform.  Counts how many of the twelve FAO food groups
    (:data:`HDDS_GROUPS`) the household acquired ANY of during the food
    module's recall window.

    Parameters
    ----------
    food_acquired : pd.DataFrame
        ``food_acquired`` item feature, grain ``(t, v, i, j, u, s)``, with
        ``Quantity`` and/or ``Expenditure``.  Rows are counted across ALL
        acquisition sources ``s`` -- purchased, produced, in-kind and other
        are all consumption.
    groups : Mapping[str, Sequence[str]]
        **Required.**  ``{group name: [j label, ...]}`` covering exactly the
        twelve :data:`HDDS_GROUPS` and every ``j`` present in the frame.
        The ``j`` labels are whatever the frame carries -- so if you called
        ``food_acquired(labels='Aggregate')`` they are the country's
        Aggregate labels, and if you did not they are its fine
        ``Preferred Label`` items (Uganda: 175 of them).  See the
        ``labels=`` contract in
        ``.claude/skills/add-feature/food-acquired/aggregate-labels/``.

    Returns
    -------
    pd.DataFrame
        One integer ``HDDS`` column (0-12) indexed by ``(t, i)``.

    Notes
    -----
    **The library ships no default mapping, deliberately.**  Which of a
    country's ``j`` labels is a "vegetable" is a curation over that
    country's own ``food_items`` / ``harmonize_food`` table, and
    ``LEARNINGS.org`` L8 is explicit that EPAR's group mapping is "a
    starting point to re-derive, not copy".  A shipped default would be
    silently wrong for 15 of the 16 food countries.  What IS shipped is the
    group vocabulary and the partition check.

    **Divergences from the FAO construct, both real:**

    1.  *Recall window.*  FAO's HDDS (Kennedy, Ballard & Dop 2011) is a
        **24-hour** recall.  LSMS food modules are typically **7-day**, so
        an HDDS computed here is systematically HIGHER than the FAO number
        and is not comparable to a published FAO HDDS.  EPAR's own variable
        label says so out loud -- "number of food groups individual consumed
        last week" (``W5.do``, HOUSEHOLD'S DIET DIVERSITY SCORE).  The
        window is the country's, not this function's; check the country's
        food module before quoting the score.
    2.  *Acquisition vs. consumption.*  ``food_acquired`` records what
        entered the household.  Where the country's module is a consumption
        recall (Malawi's ``hh_mod_g1``, "consumed in the past 7 days",
        split by source) the two coincide; where it is a pure purchase
        module, food eaten from own stores registers no row and the score is
        a lower bound.  This function cannot tell the difference -- the
        analyst must.

    A household with rows but no positive acquisition scores 0; a household
    with no rows at all is absent from the output (never zero-filled).
    """
    df = food_acquired
    names = list(df.index.names or [])
    if 'j' in names:
        item = pd.Series(df.index.get_level_values('j').astype(str),
                         index=df.index)
    elif 'j' in df.columns:
        item = df['j'].astype(str)
    else:
        raise ValueError("hdds: food_acquired must have a 'j' level or column")
    group_by = [n for n in ['t', 'i'] if n in names]
    if not group_by:
        raise ValueError("hdds: food_acquired must have 't' and/or 'i' levels")

    mapping = _resolve_food_groups(groups, HDDS_GROUPS, set(item),
                                   what='hdds')
    consumed = _consumed_mask(df, what='hdds')

    work = pd.DataFrame({'_group': item.map(mapping),
                         '_any': consumed}).reset_index()
    per_group = work.groupby(group_by + ['_group'], dropna=False)['_any'].any()
    score = per_group.groupby(group_by).sum().astype('int64')
    return score.to_frame('HDDS').sort_index()


def fcs(food_acquired, *, groups, days, weights=None, cap=7):
    """WFP Food Consumption Score (EPAR ``fcs``).

    METHODOLOGY transform.  ``Sigma_g weight[g] x min(days consumed in group
    g, 7)`` per household, over the eight WFP food groups.

    Parameters
    ----------
    food_acquired : pd.DataFrame
        ``food_acquired`` item feature (see :func:`hdds`).
    groups : Mapping[str, Sequence[str]]
        **Required.**  ``{group: [j label, ...]}`` naming exactly the keys of
        ``weights`` (default: the eight :data:`FCS_WFP_WEIGHTS` groups) and
        covering every ``j`` in the frame.  Same partition rules as
        :func:`hdds`.
    days : str
        **Required, and there is deliberately no default.**  Name of the
        column giving the number of days in the past 7 that the item was
        consumed.

        **No country's ``food_acquired`` carries such a column today.**  The
        canonical schema (``lsms_library/data_info.yml``, ``Columns:
        food_acquired``) declares ``Quantity`` / ``Expenditure`` /
        ``Price`` and nothing else, and the only ``Days`` column anywhere in
        the corpus belongs to ``food_coping``.  So ``fcs`` is UNUSABLE on a
        shipped table until a consumption-frequency column is wired, and
        that is the honest state of it -- an FCS is a frequency score, and
        deriving frequency from an acquisition amount would be inventing
        data.  Where the source has the question (Tanzania's
        ``hh_sec_j3.hh_j08``, which EPAR reads at ``W5.do:2574``, is
        currently unwired on our side), wire it as a column and pass its
        name here.
    weights : Mapping[str, float], optional
        Group -> weight.  Defaults to :data:`FCS_WFP_WEIGHTS`.
    cap : int, default 7
        Per-group day cap.  A group's item-days are summed, then capped:
        eating rice on 5 days and maize on 4 is 7 staple-days, not 9.

    Returns
    -------
    pd.DataFrame
        One float ``FCS`` column indexed by ``(t, i)``.

    Notes
    -----
    Follows WFP's Technical Guidance Sheet (2008): sum the days within each
    group, cap the group total at 7, weight, sum.  **EPAR's Tanzania
    implementation diverges from that** and the divergence is worth naming:
    rather than summing cereals and tubers and capping, it blends them
    (``W5.do:2579-2582``) as ``7`` if either is 7, else their sum if either
    is 0, else ``(max + min(sum, 7)) / 2`` -- an averaging rule with no
    citation in the sheet.  It also leaves its item-group 2 unweighted, so
    that group contributes nothing to the score at all.  Neither behaviour
    is reproduced here.

    The score is NOT thresholded into WFP's poor / borderline / acceptable
    bands (21 / 35) by this function -- the same separation as
    :func:`rcsi` / :func:`rcsi_phase`, so an analyst can apply the
    oil-and-sugar-adjusted cutoffs (28 / 42) where the country's diet
    warrants them without recomputing the score.

    Unlike :func:`rcsi`, a group with no rows for a household scores 0 days
    rather than dropping the household: in a 7-day frequency recall, "no
    row" is the instrument's own zero.  That is only true if the country's
    module really is a consumption recall over the whole item list -- see
    :func:`hdds`'s divergence note 2.
    """
    df = food_acquired
    names = list(df.index.names or [])
    w = dict(FCS_WFP_WEIGHTS) if weights is None else dict(weights)
    if not w:
        raise ValueError("fcs: weights= is empty")
    if days is None or days not in df.columns:
        raise ValueError(
            f"fcs: food_acquired has no {days!r} column. The FCS is a "
            "CONSUMPTION-FREQUENCY score and no country's food_acquired "
            "currently carries a days-consumed column (the canonical schema "
            "declares Quantity/Expenditure/Price only; the corpus's only "
            "Days column is food_coping's). Wire the source question -- e.g. "
            "Tanzania hh_sec_j3.hh_j08 -- and pass its name as days=; do not "
            "substitute a quantity.")
    if 'j' in names:
        item = pd.Series(df.index.get_level_values('j').astype(str),
                         index=df.index)
    elif 'j' in df.columns:
        item = df['j'].astype(str)
    else:
        raise ValueError("fcs: food_acquired must have a 'j' level or column")
    group_by = [n for n in ['t', 'i'] if n in names]
    if not group_by:
        raise ValueError("fcs: food_acquired must have 't' and/or 'i' levels")

    mapping = _resolve_food_groups(groups, list(w), set(item), what='fcs')

    work = pd.DataFrame({
        '_group': item.map(mapping),
        '_days': pd.to_numeric(df[days], errors='coerce'),
    }).reset_index()
    work = work.dropna(subset=['_days'])
    per_group = (work.groupby(group_by + ['_group'], dropna=False)['_days']
                     .sum(min_count=1)
                     .clip(upper=cap))
    weight_vec = pd.Series(
        per_group.index.get_level_values('_group').map(w).to_numpy(),
        index=per_group.index, dtype=float)
    contrib = per_group * weight_vec
    score = contrib.groupby(group_by).sum(min_count=1).dropna()
    return score.to_frame('FCS').sort_index()


def fertilizer_rate(plot_inputs, plot_features, *, nutrient='N',
                    area_col='Area', volume_as_mass=True, on='parcel'):
    """Fertilizer applied per unit plot area (EPAR "rate of fertilizer
    application").

    MECHANICAL reduction.  Divides a per-plot fertilizer mass by that plot's
    area, on the same land grain -- the same grain-bridge shape as
    :func:`yield_kg`.

    Parameters
    ----------
    plot_inputs : pd.DataFrame
        ``plot_inputs`` item feature (see :func:`nitrogen_kg`): an
        ``input`` level, a ``Quantity`` and a unit ``u``.
    plot_features : pd.DataFrame
        ``plot_features`` item feature, grain ``(t, i, plot_id, ...)``,
        carrying ``Area`` -- **hectares, per the canonical schema**
        ("plot area in hectares (canonical unit)",
        ``lsms_library/data_info.yml``, ``Columns: plot_features: Area``).
        So the returned rate is kg/ha wherever the country honours that.
    nutrient : {'N', 'product'}, default 'N'
        - ``'N'``: numerator is :func:`nitrogen_kg` -- kilograms of
          NITROGEN, i.e. product kg times the nutrient share of
          :data:`_NITROGEN_CONTENT`.  Output column
          ``Nitrogen_kg_per_ha``.
        - ``'product'``: numerator is raw fertilizer PRODUCT kg -- the same
          rows, same unit conversion, N-share forced to 1.0.  Output column
          ``Fertilizer_kg_per_ha``.  Note this covers the INORGANIC
          vocabulary only: ``_NITROGEN_CONTENT``'s keys are the nutrient
          classes (nitrate / phosphate / potash / mixed) and the product
          names (urea, CAN, SA, DAP, NPK, TSP, SSP, MOP) -- manure and
          other organic inputs carry no key and contribute nothing to
          either basis.
    area_col : str, default 'Area'
        Plot-area column in ``plot_features``.
    volume_as_mass : bool, default True
        Forwarded to the unit->kg conversion.
    on : {'parcel', 'plot'}, default 'parcel'
        Land grain to join the two features on.  **The default matches
        :func:`yield_kg`'s**, and that agreement is deliberate: the two
        transforms bridge the same two plot vocabularies, and a pair of
        sibling functions that default differently is a trap.  It was one --
        with ``on='plot'`` as the default, ``fertilizer_rate(Uganda)``
        returned an EMPTY frame and said nothing (see
        :class:`PlotGrainMismatchWarning`).

        - ``'parcel'`` (default): reconcile the two plot vocabularies to
          their common parcel key first, exactly as :func:`yield_kg` does
          (``_parcel_from_crop_plot`` / ``_parcel_from_feature_plot``, both
          no-ops on a vocabulary that carries no ``{hhid}-`` prefix or
          ``_suffix``).  Uganda needs it: ``{hhid}-{parcel}-{plot}`` on the
          ag modules vs. ``{parcel}_{suffix}`` on ``plot_features``, which
          share no literal key at all -- 0 rows on ``'plot'``, 509 on
          ``'parcel'``.  Measured no-op on Malawi, whose two features
          already share the literal key (66,298 of 66,368 distinct
          ``(t, i, plot)`` keys match, 99.9%, identically under either
          setting).
        - ``'plot'``: join the literal plot key verbatim.  Use where the two
          features are known to share a vocabulary and the parcel
          extraction would be wrong.

    Returns
    -------
    pd.DataFrame
        One column -- ``Nitrogen_kg_per_ha`` or ``Fertilizer_kg_per_ha``,
        named for the numerator so the basis cannot be lost -- indexed by
        ``(t, i, plot)`` (or ``(t, i, parcel)`` for ``on='parcel'``).

    Notes
    -----
    Divergences from EPAR's rate, both in the denominator and the
    numerator, and neither is a bug on either side:

    - *Denominator.*  EPAR's is hectares **planted**, itself imputed as
      parcel area times a reported planted share ("Plot area size = Parcel
      area size x (planted share)", its readme).  Ours is the plot area the
      instrument reported and the country converted to hectares
      (``plot_features.Area``, ``required``).  A plot only partly planted
      therefore gets a LOWER rate here than in EPAR's table.
    - *Numerator.*  EPAR's rate is over fertilizer PRODUCT; the WB
      harmonised panel's ``nitrogen_kg`` is over the nutrient.  Hence
      ``nutrient=``, with no default that hides which one you got.
    - Inherits :func:`nitrogen_kg`'s unit-conversion coverage caveat (only
      metric-named ``u`` rows convert) and its class-nominal N shares, so
      on a country recording nutrient class rather than product the rate is
      a nominal, not a measured, nitrogen figure.

    Plots with fertilizer but zero or missing area are dropped (an infinite
    rate is not a rate).
    """
    if nutrient not in {'N', 'product'}:
        raise ValueError(
            f"fertilizer_rate nutrient= must be 'N' or 'product', got "
            f"{nutrient!r}")
    if on not in {'plot', 'parcel'}:
        raise ValueError(
            f"fertilizer_rate on= must be 'plot' or 'parcel', got {on!r}")

    if nutrient == 'N':
        num = nitrogen_kg(plot_inputs, volume_as_mass=volume_as_mass)
        num_col, out_col = 'Nitrogen_kg', 'Nitrogen_kg_per_ha'
    else:
        # Same rows, same conversion, nutrient share forced to one: this is
        # a REUSE of nitrogen_kg's machinery, not a second implementation of
        # the unit conversion.
        num = nitrogen_kg(plot_inputs,
                          nitrogen_content={k: 1.0 for k in _NITROGEN_CONTENT},
                          volume_as_mass=volume_as_mass)
        num = num.rename(columns={'Nitrogen_kg': 'Fertilizer_kg'})
        num_col, out_col = 'Fertilizer_kg', 'Fertilizer_kg_per_ha'

    num = num.reset_index()
    if 'plot' not in num.columns:
        raise ValueError(
            "fertilizer_rate: the fertilizer numerator has no 'plot' level "
            "to join area on")

    pf = plot_features.reset_index()
    plot_key = 'plot' if 'plot' in pf.columns else (
        'plot_id' if 'plot_id' in pf.columns else None)
    if plot_key is None:
        raise ValueError(
            "plot_features must carry a 'plot' or 'plot_id' level")
    if area_col not in pf.columns:
        raise ValueError(f"plot_features must have a {area_col!r} column")

    base = [k for k in ['t', 'i'] if k in num.columns and k in pf.columns]

    if on == 'parcel':
        num['_land'] = [
            _parcel_from_crop_plot(p, i)
            for p, i in zip(num['plot'],
                            num['i'] if 'i' in num.columns
                            else [''] * len(num))
        ]
        pf['_land'] = pf[plot_key].map(_parcel_from_feature_plot)
    else:
        num['_land'] = num['plot'].astype(str)
        pf['_land'] = pf[plot_key].astype(str)

    keys = base + ['_land']
    fert = num.groupby(keys, dropna=False)[num_col].sum().reset_index()

    pf['_Area'] = pd.to_numeric(pf[area_col], errors='coerce')
    pf = pf[pf['_Area'] > 0]
    area = pf.groupby(keys, dropna=False)['_Area'].sum().reset_index()

    merged = fert.merge(area, on=keys, how='inner')
    if len(merged) == 0 and len(fert) and len(area):
        # Silent emptiness is the failure this guard exists for: both sides
        # have rows, the inner merge matched none, and the result is a
        # correctly-shaped frame with nothing in it.
        warnings.warn(
            f"fertilizer_rate(on={on!r}): the {len(fert):,} fertilizer land "
            f"key(s) and the {len(area):,} area land key(s) have NO value in "
            "common, so the result is empty. The two features use different "
            "plot vocabularies -- e.g. Uganda's ag modules key plots "
            "'{hhid}-{parcel}-{plot}' while plot_features keys them "
            f"'{{parcel}}_{{suffix}}'. Examples -- fertilizer: "
            f"{sorted(map(str, fert['_land'].unique()))[:3]}; area: "
            f"{sorted(map(str, area['_land'].unique()))[:3]}. Try "
            f"on={'plot' if on == 'parcel' else 'parcel'!r}.",
            PlotGrainMismatchWarning, stacklevel=2)
    with np.errstate(divide='ignore', invalid='ignore'):
        merged[out_col] = merged[num_col] / merged['_Area']
    merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=[out_col])
    land_name = 'parcel' if on == 'parcel' else 'plot'
    out = merged.rename(columns={'_land': land_name})
    return out.set_index(base + [land_name])[[out_col]].sort_index()


def crop_diversity(crop_production, *, weight='count', value_col='Value_sold'):
    """Shannon crop-diversity index per household (EPAR ``sdi``).

    METHODOLOGY transform.  ``H = -Sigma_c p_c ln p_c`` over the crops a
    household grew, where ``p_c`` is crop ``c``'s share of whatever
    ``weight`` says the shares are shares OF.

    Parameters
    ----------
    crop_production : pd.DataFrame
        ``crop_production`` item feature, with a crop level named ``j`` or
        ``crop`` (resolved by :func:`_resolve_crop_level`).
    weight : {'count', 'value', 'area'}, default 'count'
        What the shares are taken over.

        - ``'count'`` -- **the only basis available in every country.**
          ``p_c`` is crop ``c``'s share of the household's crop
          OCCURRENCES, an occurrence being one distinct combination of the
          land and season levels the table carries: ``(plot, season)`` in
          Uganda, ``(plot,)`` in Malawi, falling back to raw rows only if
          the table has neither.  De-duplicating to that grain is
          load-bearing, not tidiness: ``crop_production`` carries one row
          per ``(u, condition)`` as well, so counting raw rows would score
          a maize crop reported in two conditions as twice the maize.
        - ``'value'`` -- ``p_c`` is crop ``c``'s share of the household's
          reported ``Value_sold``.  **Covers SOLD output only**, so a
          household that sold one crop and ate three scores ``H = 0``:
          maximally specialised on the sales margin, which is a real fact
          about the household but is NOT its cropping diversity.  Use it as
          a marketing-concentration measure, not as a substitute for the
          area basis.
        - ``'area'`` -- raises :class:`NotImplementedError`.  See below.

    value_col : str, default 'Value_sold'
        Column used when ``weight='value'``.

    Returns
    -------
    pd.DataFrame
        One float ``Crop_diversity`` column indexed by ``(t, i)``,
        non-negative, in NATS (natural log).  A household growing one crop
        scores exactly 0.

    Raises
    ------
    NotImplementedError
        For ``weight='area'``.  The area basis needs per-crop area PLANTED,
        and ``crop_production`` carries no such column in any country -- no
        ``AreaShare``, no ``Area_planted`` (``LEARNINGS.org`` L8 and the
        taxonomy's item-gap list record this).  ``plot_features.Area`` is
        the whole plot's area, not crop ``c``'s share of it, and splitting
        it evenly across the plot's crops would MANUFACTURE the shares the
        index is made of -- on an intercropped plot the even split is the
        maximum-diversity answer by construction.  The fix is a column
        (per-crop planted area or share), not a transform.

    Notes
    -----
    **Sign.**  EPAR's ``sdi`` is ``Sigma p ln p`` -- it never negates
    (``Uganda UNPS Wave 5/EPAR_UW_Uganda_UNPS_W5.do:3603,3617``), so its
    published index is NON-POSITIVE and equals ``-H``.  Ours is the
    conventional Shannon ``H >= 0``.  Compare as ``sdi == -Crop_diversity``,
    or a reader will think one of the two is broken.

    **Basis.**  EPAR weights by area PLANTED (``area_plan``), dropping rows
    with zero planted area -- which, its own comment notes, silently
    excludes permanent/tree crops unless they are a plot's only crop.  That
    weight is refused here rather than approximated (see ``weight='area'``
    above).

    **What one "occurrence" is, stated exactly, because it is easy to
    misread.**  The ``'count'`` basis takes shares over distinct
    ``(t, i, crop, <plot level>, season)`` tuples -- so a crop grown on
    three plots DOES contribute three terms, exactly as in EPAR's plot-crop
    row basis.  What is de-duplicated away is only the ``u`` / ``condition``
    multiplicity WITHIN one land-season unit (a maize harvest reported in
    two conditions is one occurrence, not two).  It is NOT an index over
    distinct crops: a household growing maize on four land-season units and
    beans on one scores ``H = 0.5004``, not ``ln 2 = 0.6931``.  Pinned by
    ``tests/test_863_transforms.py::
    test_crop_diversity_count_separates_plots_and_seasons``.

    **Not comparable across countries on the ``'count'`` basis.**  The
    occurrence key uses whichever of the land and season levels the
    country's ``crop_production`` carries -- ``(plot, season)`` for Uganda,
    ``(plot,)`` for Malawi, which has no ``season`` level at all.  A Uganda
    household with an asymmetric crop mix across its two seasons therefore
    scores differently from an otherwise identical Malawi household, purely
    because one instrument records a season and the other does not.  On a
    cross-country :class:`~lsms_library.feature.Feature` frame, compare
    within a country, or use ``weight='value'`` (whose shares have no land
    or season key) and accept its sold-only bias.
    """
    if weight not in {'count', 'value', 'area'}:
        raise ValueError(
            "crop_diversity weight= must be 'count', 'value' or 'area', got "
            f"{weight!r}")
    df = crop_production
    names = list(df.index.names or [])
    crop_level = _resolve_crop_level(names)
    if crop_level is None:
        raise ValueError(
            f"crop_production has no crop level (looked for {_CROP_LEVELS})")
    group_by = [n for n in ['t', 'i'] if n in names]
    if not group_by:
        raise ValueError("crop_production must have 't' and/or 'i' levels")

    if weight == 'area':
        raise NotImplementedError(
            "crop_diversity(weight='area') needs a per-crop planted AREA and "
            "crop_production carries none in any country -- there is no "
            "AreaShare / Area_planted column in the canonical schema. "
            "plot_features.Area is the whole plot's area, and splitting it "
            "evenly across a plot's crops would manufacture the very shares "
            "the index measures. Use weight='count' (available everywhere), "
            "or wire a planted-area column first.")

    flat = df.reset_index()
    if weight == 'value':
        if value_col not in flat.columns:
            raise ValueError(
                f"crop_production must have a {value_col!r} column for "
                "weight='value'")
        flat['_w'] = pd.to_numeric(flat[value_col], errors='coerce')
        flat = flat[flat['_w'] > 0]
        share_of = flat.groupby(group_by + [crop_level],
                               dropna=False)['_w'].sum(min_count=1)
    else:
        plot_level = _resolve_plot_level(names)
        occurrence = [n for n in [plot_level, 'season']
                      if n is not None and n in flat.columns]
        if occurrence:
            dedup = flat.drop_duplicates(
                subset=group_by + [crop_level] + occurrence)
        else:
            dedup = flat
        share_of = dedup.groupby(group_by + [crop_level],
                                 dropna=False).size().astype(float)

    share_of = share_of.dropna()
    share_of = share_of[share_of > 0]
    total = share_of.groupby(group_by).sum()
    denom = total.reindex(share_of.index.droplevel(crop_level)).to_numpy()
    p = share_of / denom
    with np.errstate(divide='ignore', invalid='ignore'):
        term = p * np.log(p)
    h = -term.groupby(group_by).sum(min_count=1).dropna()
    # -0.0 for the single-crop case reads badly; normalise it away.
    h = h + 0.0
    return h.to_frame('Crop_diversity').sort_index()


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


def _weighted_median(values, weights, keys):
    """Per-group weighted median of ``values``, weighted by ``weights``.

    Sort a group's values ascending; let ``W`` be its total weight and ``C``
    the running cumulative weight.  The weighted median is the value at the
    first row where ``C >= W/2`` -- EXCEPT where ``C == W/2`` exactly at that
    row (compared with a relative tolerance), in which case it is the mean of
    that value and the next row's.

    That tie rule is what makes ``weight_col`` a strict GENERALISATION of the
    unweighted path: under equal positive weights the exception fires exactly
    when the group has an even count (the half-total lands squarely on the
    lower central row) and returns the average of the two central values,
    while an odd count reaches ``W/2`` strictly inside the middle row and
    returns it.  So equal weights reproduce ``Series.median()`` exactly, in
    BOTH parities.  Dropping the exception would give the *lower* weighted
    median, which agrees with ``Series.median()`` only for odd counts.

    A row takes part only if its value is non-missing AND its weight is
    non-missing and strictly positive; a group with no such row gets NaN.
    ``keys`` is a list of Series aligned to ``values.index``, already
    stringified by the caller (so there are no NA group labels).
    """
    usable = (values.notna() & weights.notna() & (weights > 0)).astype(bool)
    out = pd.Series(np.nan, index=values.index, dtype='float64')
    if not usable.any():
        return out

    kcols = [f'_k{n}' for n in range(len(keys))]
    # Built from numpy arrays on a fresh RangeIndex: ``values.index`` may
    # carry duplicate labels (item rows repeat a household), and a
    # DataFrame assembled from Series would try to align on it.
    sub = pd.DataFrame({
        '_v': values[usable].to_numpy(dtype='float64'),
        '_w': weights[usable].to_numpy(dtype='float64'),
    })
    for col, key in zip(kcols, keys):
        sub[col] = np.asarray(key[usable])
    sub = sub.sort_values(kcols + ['_v'], kind='stable')

    by = sub.groupby(kcols, sort=False, dropna=False)
    cum = by['_w'].cumsum()
    total = by['_w'].transform('sum')
    half = 0.5 * total
    # ``cumsum`` accumulates sequentially and ``sum`` pairwise, so at the
    # exact tie the equal-weights case produces they can differ by an ulp.
    # Both comparisons carry the same relative slack, so a tie is recognised
    # as a tie rather than skipped or split.
    reached = cum >= half * (1 - 1e-12)
    tie = np.isclose(cum.to_numpy(), half.to_numpy(), rtol=1e-12, atol=0.0)

    # The next row within the group (NaN at the group's last row).  An exact
    # tie cannot occur there -- C == W/2 == W would need W == 0, and every
    # participating weight is strictly positive -- so the mask only guards
    # against reading across a group boundary.
    nxt = sub['_v'].shift(-1).where(by.cumcount(ascending=False) != 0)
    stat = pd.Series(
        np.where(tie & nxt.notna().to_numpy(),
                 (sub['_v'].to_numpy() + nxt.to_numpy()) / 2.0,
                 sub['_v'].to_numpy()),
        index=sub.index)

    # Weights are strictly positive, so ``cum`` strictly increases within a
    # group and ``reached`` is monotone: the FIRST True is the median row.
    # ``groupby().first()`` skips NaN, so it returns exactly that row's stat.
    sub['_m'] = stat.where(reached)
    per_group = sub.groupby(kcols, sort=False, dropna=False)['_m'].first()

    if len(kcols) == 1:
        lookup = pd.Index(np.asarray(keys[0]))
    else:
        lookup = pd.MultiIndex.from_arrays([np.asarray(k) for k in keys])
    return pd.Series(per_group.reindex(lookup).to_numpy(dtype='float64'),
                     index=values.index)


def median_price_valuation(item_df, geo_levels, *,
                           value_col='Value_sold',
                           qty_col='Quantity_sold',
                           kg_qty=None,
                           quantity_col='Quantity',
                           item_keys=('j',),
                           threshold=10,
                           weight_col=None,
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
        usable observed prices ``n``.  Walking ``geo_levels`` from finest
        to coarsest, then a final national level, the imputed price for a row
        is the **median observed price of the finest cell whose count ≥
        threshold**.  This is exactly the WB cascade: EA → admin_4 → admin_3 →
        admin_2 → admin_1 → national, where the cell's median is *adopted only
        if it has ≥10 priced observations* and no finer cell already qualified.
        The national median is the unconditional fallback (the WB
        ``replace ... if ten_obs_n==0``).  With ``weight_col`` the cell
        statistic becomes a **weighted** median (see that parameter); the
        ladder, the threshold and the fallback are unchanged.
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
    weight_col : str, optional
        When given (an index level or a column of ``item_df``), every cell
        median becomes a **weighted median**.  Sort the cell's observed prices
        ascending; let ``W`` be the total weight and ``C`` the cumulative
        weight.  The cell's price is the one at the first row where
        ``C >= W/2``, EXCEPT where ``C == W/2`` exactly at that row (to within
        a relative tolerance), in which case it is the mean of that price and
        the next row's.  ``None`` (the default) runs the unweighted path
        unchanged.

        The weight is per *row* -- the household weight repeated on each of its
        item rows, as EPAR's ``[aw=weight]`` is.  A quantity-weighted median
        (EPAR Nigeria ``W4.do:696``, ``gen weight=qty*weight_pop_rururb``) is
        had by passing a column you derived that way.

        *Observation counting is unaffected*: ``n`` stays a COUNT OF ROWS, not
        a sum of weights, so ``threshold`` means the same thing on both paths.

        *Null and non-positive weights.*  A priced row whose weight is missing
        or ``<= 0`` takes no part in the weighted median AND is not counted
        toward ``threshold`` -- a row counts for a cell exactly when it can
        speak for it.  It still *receives* an imputed price, precisely as a
        row with a missing price does.  A warning names how many rows this is.

        *Relation to the unweighted path.*  ``weight_col`` is a strict
        GENERALISATION of it: under equal (or all-equal positive) weights the
        weighted median reproduces ``Series.median()`` EXACTLY, for BOTH odd
        and even cell counts.  The exact-tie exception above is what buys
        that -- with equal weights it fires precisely on an even count and
        averages the two central prices, exactly as the unweighted path does;
        an odd count reaches ``W/2`` strictly inside the middle row and
        returns it.  Pinned in both parities by
        ``tests/test_median_price_valuation.py``.  See :func:`_weighted_median`.
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
    Prior art (convergence, and the deltas).  Three teams reached the same
    selection rule independently -- the WB panel's ``valuation_median_crops``,
    EPAR (Evans School, UW) Technical Report #335 in both its crop and
    livestock ladders and again in its consumption repo, and this function:
    *the median price of the finest geographic cell with >= threshold
    observations*.  EPAR iterates broad-to-narrow with an unconditional
    overwrite, which selects the same cell.  The verified deltas:
      - EPAR's medians are WEIGHTED, by its population-raked
        ``weight_pop_rururb`` (Uganda ``W5.do:482-483``, Malawi
        ``W1.do:504-505``, Nigeria ``W4.do:695-696``); ours is unweighted
        unless ``weight_col`` is passed.  We do not rake weights.
      - EPAR GATES its national rung on the same threshold; ours is an
        unconditional fallback, so no row is left unvalued.
      - EPAR's LIVESTOCK ladder is the opposite idiom: narrow-to-broad fill
        with a preference for the household's OWN observed price, ending
        unconditional.  "EPAR's ladder" is ambiguous -- say which.
      - EPAR's CONSUMPTION repo gates at ``obs > 10``, i.e. N >= 11, where its
        Ag repo gates ``obs > 9``, i.e. N >= 10 (our default).
    See ``slurm_logs/2026-09-09_epar_curation/LEARNINGS.org`` L5.

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

    # Optional survey weight (EPAR's ``[aw=weight]``).  A priced row whose
    # weight is missing or non-positive cannot speak for its cell, so it is
    # excluded from BOTH the weighted median and the observation count --
    # the same bargain the unweighted path strikes with an unpriced row,
    # which is likewise pooled nowhere yet still valued in step 3.
    weight = None
    usable = price.notna()
    if weight_col is not None:
        weight = pd.to_numeric(_series(weight_col), errors='coerce')
        # ``.astype(bool)`` so a nullable Int64/Float64 weight cannot turn
        # the count below into a nullable Int64 sum.
        usable = (price.notna() & weight.notna() & (weight > 0)).astype(bool)
        n_dropped = int((price.notna() & ~usable).sum())
        if n_dropped:
            warnings.warn(
                f"median_price_valuation(weight_col={weight_col!r}): "
                f"{n_dropped:,} of {int(price.notna().sum()):,} priced rows "
                f"have a missing or non-positive weight.  They take no part "
                f"in the weighted medians and are NOT counted toward "
                f"threshold={threshold}; they are still valued at whichever "
                f"ladder price their cell ends up with.",
                UserWarning,
                stacklevel=2,
            )

    def _cell_median(keys):
        """Cell statistic: the plain median, or the weighted median."""
        if weight is None:
            return price.groupby(keys).transform('median')
        return _weighted_median(price, weight, keys)

    # Step 2: median ladder.  Start with everyone unassigned; for each geo
    # level finest→coarsest (then national), fill any still-unassigned row
    # whose cell clears the threshold with that cell's median observed price.
    imputed = pd.Series(np.nan, index=df.index, dtype='float64')
    ladder = list(geo_levels) + [None]  # None ⇒ national (no geo grouping)
    for level in ladder:
        keys = ([] if level is None else [_series(level).astype(str)]) \
            + item_key_series
        cell_median = _cell_median(keys)
        # A COUNT OF ROWS, never a sum of weights -- so ``threshold`` means
        # the same thing on the weighted and unweighted paths.
        cell_count = usable.groupby(keys).transform('sum')
        qualifies = cell_count >= threshold
        take = imputed.isna() & qualifies & cell_median.notna()
        imputed = imputed.where(~take, cell_median)
    # National median is the unconditional fallback (WB ten_obs_n==0 branch):
    # any row still unassigned after the threshold cascade gets the national
    # median regardless of count, mirroring the final WB ``replace``.
    nat_median = _cell_median(item_key_series)
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
