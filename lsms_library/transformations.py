#!/usr/bin/env python3

"""
A collection of mappings to transform dataframes.
"""
import re
import warnings

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

    Labels in :data:`_CURRENCY_DENOMINATED_UNITS` (e.g. ``u='Value'``) are
    dropped from the whole computation before anything is inferred, so they
    never appear as a key of the result: their factor is ``1/price`` at
    ``(j, t, m)`` and a per-label constant cannot represent it (GH #770).

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
        Mapping of unit label → inferred kg factor.  Keys are the RAW ``u``
        label with its case PRESERVED -- the grouping is on the raw label,
        so ``Calebasse`` and ``calebasse`` come back as two entries with two
        factors.  (The docstring used to say "(lowercased)", which was
        false; :func:`_get_kg_factors` is where the lower-casing happens,
        and it reports the resulting clashes -- see
        :class:`UnitLabelCollisionWarning`.)  Units already in
        :data:`KNOWN_METRIC`, units in :data:`_CURRENCY_DENOMINATED_UNITS`,
        or units that cannot be inferred are absent from the output.
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
            offered = _own_price_rung(t, df.index, j, u, value, qty, pool)
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


def _own_price_rung(t, index, j, u, value, qty, pool):
    """Unit price from the SAME household's purchases of the same (t, j, u).

    ``sum(value) / sum(quantity)`` over that household's pooled purchase rows,
    so a household that bought an item twice contributes one quantity-weighted
    price rather than two.  Returns NaN where the household made no usable
    purchase of that item in that unit in that wave -- no cross-household
    information is ever consulted.
    """
    i = _level_or_column(pd.DataFrame(index=index), 'i')
    if i is None:
        # No household axis: there is no "same household" to look up.
        return np.full(len(t), np.nan)
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

    Those tallies reach the caller on the ``Country`` path only.  A
    cross-country :class:`~lsms_library.feature.Feature` call forwards
    ``valuation=`` (it forwards by signature) and the VALUES are correct, but
    ``Feature`` assembles by ``concat`` over frames whose ``attrs`` disagree by
    construction -- one record per country -- which lands in the ``{}`` row of
    the propagation rule (``CLAUDE.md``, "Panel ID Transitive Chains"), so
    ``attrs['valuation_sources']`` is absent there.  Pooling the tallies across
    countries is deliberately NOT built: pooled counts would say nothing about
    WHICH country was imputed, which is the same objection ``harvest_kg``'s
    docstring makes about its own pooled counts.  Ask per country, or group the
    item-grain frame.
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
    ``survey_median`` / ``inferred`` / ``none`` exactly as if it had never
    reported, and its rejected value enters no median.

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
KG_FACTOR_LAYERS = ('reported', 'survey_median', 'inferred', 'none')


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


def harvest_kg_factors(crop_production, *, volume_as_mass=True,
                       min_reports=SURVEY_MEDIAN_MIN_REPORTS):
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
        ``kg_reported`` / ``kg_survey_median`` / ``kg_inferred``
            what each layer offered for that row, whether or not it won.
            ``kg_reported`` is POST-screen, so a rejected report reads NaN
            here and ``kg_reported_rejected`` is True; the raw value is
            untouched in the input frame's own ``KgFactor`` column --
            which is what makes the disagreement auditable per unit
            (``groupby('u')`` on this frame).

        Two tallies ride on ``.attrs``:

        ``kg_factor_sources``
            ``{layer: n_rows}`` over the INPUT rows.  The four layers of
            :data:`KG_FACTOR_LAYERS` partition the frame and sum to
            ``len(crop_production)``; the extra ``reported_implausible`` key
            counts rows whose report the screen REJECTED and is deliberately
            outside that partition, since such a row is still served by one
            of the four.  POOLED: on a cross-country
            :class:`~lsms_library.feature.Feature` frame these counts run over
            every country at once, so ``reported: 40000`` says nothing about
            WHICH country reported.  Read it as a total, never as coverage;
            the per-row frame answers the real question with one
            ``groupby('country')``.
        ``kg_factor_disagreement``
            For ``reported_vs_survey_median`` and ``reported_vs_inferred``:
            ``{'both': n, 'disagree': k, 'share': k/n or None}``, where
            ``both`` counts rows for which BOTH layers produced a usable
            factor and ``disagree`` counts those differing from the
            *reported* factor by more than
            :data:`KG_FACTOR_DISAGREEMENT_TOLERANCE` in relative terms.
            This is the audit hook: a large ``reported_vs_inferred`` share
            means the library's factor table and the instrument disagree
            about what a unit weighs, and the instrument is the one that was
            there.

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
    med_ok = ~np.isnan(survey_median)
    inf_ok = ~np.isnan(inferred_arr)

    resolved = np.where(rep_ok, reported,
                        np.where(med_ok, survey_median, inferred_arr))
    source = np.where(rep_ok, 'reported',
                      np.where(med_ok, 'survey_median',
                               np.where(inf_ok, 'inferred', 'none')))

    out = pd.DataFrame(
        {'kg_per_unit': resolved,
         'KgFactorSource': source,
         'kg_reported': reported,
         'kg_reported_rejected': rejected,
         'kg_survey_median': survey_median,
         'kg_inferred': inferred_arr},
        index=df.index)

    counts = {layer: int((source == layer).sum()) for layer in KG_FACTOR_LAYERS}
    # NOT a fifth layer, and deliberately not part of the partition: a
    # rejected row is still SERVED by one of the four (usually `none` or
    # `inferred`), so counting it here as well would double-count it.  The
    # four layers sum to len(df); this rides alongside as a screen count.
    counts['reported_implausible'] = int(rejected.sum())
    out.attrs['kg_factor_sources'] = counts
    # A row with no unit recorded is excluded from the audit as well: there
    # is nothing for its reported factor to be compared against, since no
    # per-unit factor -- inferred or median -- can exist for a non-unit.
    audited = np.where(sentinel, np.nan, reported)
    out.attrs['kg_factor_disagreement'] = {
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
               min_reports=SURVEY_MEDIAN_MIN_REPORTS):
    """Total harvested kilograms per (t, i, plot, j) from ``crop_production``.

    MECHANICAL reduction (GAP 1 → WB ``Plotcrop``/``Plot`` ``harvest_kg``).
    For each reported harvest row, convert the native-unit ``Quantity`` to
    kilograms, then sum within each plot-crop.

    Each row's kg-per-unit factor is built by a LAYERED procedure, in
    precedence order: the row's own **reported** ``KgFactor`` (what the
    instrument wrote down, e.g. UNPS ``a5?q6d``); the **median of reported**
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

    Closing that gap is a per-country CONFIG job in two forms, neither of
    them a change to this transform: extend the country's ``u``-table
    (``harvest_units``), or wire the instrument's own conversion factor into
    the optional ``KgFactor`` column, which the ``reported`` and
    ``survey_median`` layers above then consume.  The second reaches rows the
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
                                 min_reports=min_reports)
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
