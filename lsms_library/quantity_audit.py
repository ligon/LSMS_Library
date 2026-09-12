"""SITE Q -- a value that is PRESENT, NON-NULL and IMPOSSIBLE.

The library's content guards, in the order they were built, each check one
thing and stop:

* ``Country._assert_built_required_columns`` / the ``dfs:`` sub-df check --
  is the declared column **present**?
* ``country._audit_index_collapse`` (GH #323) -- is the declared index
  **unique**?
* ``null_read_audit`` Sites R and B (GH #699) -- does the column hold
  **anything at all**?

None of them asks whether what it holds is **possible**.  Tanzania's
``crop_production`` 2020-21 reports a single household harvesting
**7,500,000 kg of coconuts** from one plot -- 7,500 tonnes, on a smallholding.
The value is present, non-null, correctly typed, on a unique index, in a table
that grades ``sane``, and nothing anywhere says a word about it (GH #857).

WHAT THIS DOES NOT DO -- and it is the first thing to say.  It **counts and
NAMES**.  It never clips, never rescales, never NaNs, never drops.  That is
the same contract ``transformations._screen_reported_factors`` states for the
reported ``KgFactor`` ("A rejected value is NEVER clipped or rescaled --
inventing a weight is the failure this whole design exists to avoid"), and it
is not a stylistic echo: an unclipped implausible value is a *defect you can
still see*, while a clipped one is a defect that has been given a plausible
disguise and a signature.  :func:`check_quantities` returns its input frame
unchanged, always and by contract.  The EPAR do-files' remedy for this exact
Tanzania wave -- ``replace kg_harvest = . if strmatch(hhid, "0015-001-001") &
plot_id==4`` (``EPAR_UW_Tanzania_NPS_W5.do:578``) -- is doubly out of bounds
here: it deletes, and it deletes from a hand-written list of household ids.

THE RULE, AND WHY EVERY NUMBER IN IT.  A row is reported when

    Quantity > QUANTITY_OUTLIER_K x reference(row)

where ``reference`` is the :data:`QUANTITY_REFERENCE_QUANTILE` (0.90) of the
row's **comparison cell**, lifted to a floor of one native unit
(:data:`QUANTITY_REFERENCE_FLOOR`).  The cell is the finest of ``(t, u, j)``
-> ``(t, u)`` -> ``(t)`` that holds at least :data:`QUANTITY_MIN_CELL`
non-null quantities and whose quantile is strictly positive.  The ladder is
``transformations._survey_median_factors``' fine-to-coarse shape, and like it
the ladder **never pools across ``t``**: a thin module does not borrow a thick
one's licence.

*Why the 90th percentile and not the 99th.*  MEASURED, and the binding case is
Tanzania's own second-worst row.  In the cell ``(2020-21, kg, Timber)``, n=37::

    p99 = 2,632,000   <- 66% of the very row under test: eaten by the outlier
    p95 =    70,800   <- dragged up by the SECOND outlier (200,000)
    p90 =    23,088   <- clean; the 4,000,000 row is 173x it

A percentile at rank ``q*(n-1)`` is immune to the top ``(1-q)*(n-1)`` values,
so at the 30-row floor p99 is immune to nothing, p95 to 1.45 values and p90 to
2.9.  Under a p99 reference the 4,000,000 kg Timber row GH #857 names does not
fire at any threshold that leaves the rest of the corpus quiet.

*Why the cell must hold 30 rows, its quantile must exceed zero, AND the
reference is floored at one unit.*  A thin cell cannot manufacture a reference,
and a cell whose 90th percentile is zero carries no scale at all.  Cost of
omitting the zero condition, measured: six Malawi rows of 2-30 units fire at
ratio ``inf`` -- a 2 kg cassava harvest named as implausible, precisely the
false positive that teaches people to ignore a guard.  But strict positivity
is not enough, and the red team found where it fails: Uganda 2010-11 pools
every crop sharing ``u='Others specify'`` into a ``(t, u)`` cell whose p90 is
**0.6**, which is positive, and eight ordinary harvests were named at 66x to
500x it.  The floor is why :data:`QUANTITY_REFERENCE_FLOOR` exists; it removed
four of those eight and divided the other four by their true size instead of
by 0.6.  The remaining four (Rice 300, Ground Nuts 150, Sugarcane 150,
Pumpkins 105 against a reference of one unit) are a UNIT-LABEL defect, not a
quantity defect, and are left visible rather than tuned away.

*Why K = 100 -- and the honest part: THERE IS NO GAP.*  ``null_read_audit``
could set its trigger "in the gap" because one existed (corpus max 28.3% of a
frame's columns all-null, every known-bad parse >= 40%).  Here there is none.
Over the 595,811 rows with a reference (fifteen warm countries; a further
30,876 have none, and are never judged) the ratio ``Quantity / reference``
runs::

    p90 = 1.0    p99 = 3.6    p99.9 = 21.0    p99.99 = 233.7   max = 14,286

-- smooth, with no discontinuity near the top.  So 100 is a **stated
tolerance, not a discovered boundary**, and it is stated as: two orders of
magnitude above what nine in ten comparable rows report; five times the
corpus's own 99.9th percentile of that ratio; and below 173.3, where the
4,000,000 kg Timber row sits, so the rows the issue names actually fire.  The
sensitivity is smooth and is recorded so the choice stays re-checkable --
K=30 -> 417 rows, K=50 -> 261, K=100 -> 151, K=150 -> 90, K=300 -> 49.

WHAT IT FIRES ON TODAY -- **every** country holding a warm L2-country
``crop_production`` parquet.  (The first version of this table listed five and
was read as the corpus; the measuring script had a hardcoded country list
while its own README said it globbed ``data_root()``.  It globs now.)

=============  =========  ======  ==========
country             rows   fired   audit cost
=============  =========  ======  ==========
Benin              9,056       2        5 ms
Burkina_Faso      15,593       1        6 ms
CotedIvoire       22,223      16        9 ms
Ethiopia          85,519      10       44 ms
EthiopiaRHS       17,523       2        2 ms
GhanaSPS          25,109       7       10 ms
Guinea-Bissau     10,579       1        4 ms
Malawi           131,379      16       61 ms
Mali              35,060      18       20 ms
Niger             47,077      28       19 ms
Nigeria           62,844       9       35 ms
Senegal            8,428       1        5 ms
Tanzania          14,126      14       10 ms
Togo              11,565       1        5 ms
Uganda           130,606      25       77 ms
**total**    **626,687** **151**
=============  =========  ======  ==========

151 of 626,687 rows (0.024%), appearing only on the country being read, is a
work queue rather than a firehose -- the same reading ``null_read_audit`` gives
its own 86 Site-B findings.  Two rows larger than GH #857's headline case turn
up in countries nobody had looked at: **Benin 2018-19 Coton 12,500,000 kg**
(``i=220065``, plot 1_2, x1,785.7) and **CotedIvoire 2018-19 Cocoa 1,500,000**
(``i=600009``, x750).

COST, measured INSIDE the read rather than by differencing two timed reads on
a loaded machine (a spy on the hook, best of five, warm cache): Uganda 83 ms of
a 2,157 ms warm ``crop_production`` read (4%), Malawi 65 ms of 1,391 ms (5%),
Tanzania 9 ms of 643 ms (1%).  The first implementation cost 196 ms on Uganda
and was rewritten (:func:`_group_stats`, :func:`_axis`) to 65-83 ms after the
red team measured the read-path delta.

DELIBERATELY NO ALLOWLIST, for the reason ``country._grain_strict`` gives: an
allowlist is the same disease with a registry.  A cell that is loud and
legitimate stays loud until the data or the config can say why.

WHAT A SENTINEL DOES TO A DISTRIBUTIONAL SCREEN -- the useful case, because
the corpus supplies it twice with opposite outcomes.

Uganda used to carry 3,097 rows whose ``Quantity`` was exactly ``99999``, the
2009-10 missing-value sentinel.  Before GH #861 stripped them at the source,
**seven** of them fired here, across **two** cells (2009-10 Millet /
``u='Unknown'``, p90 2.8, and Sorghum / ``Plastic Basin (15 lts)``, p90 12) --
the two cells the sentinel did NOT saturate.  In every other cell it appeared
in, 99999 *was* the 90th percentile, so the rows were not distributional
outliers by any honest reading and the screen said nothing.  (An earlier draft
of this file said "nine, in one cell"; two of those nine were genuine
Sugarcane rows.  Corrected by the red team, 2026-09-09.)  Since #861 landed,
Uganda fires 25 rather than 36 and none of them is a sentinel.

Niger is the same signature still unfixed: **eleven** rows of exactly
``999999`` in 2011-12, ``u='Unknown'``, Mil / Sorgho / Niebe / Riz Paddy,
which DO fire (x14,285.7 on the ``t`` rung) because eleven rows cannot
saturate a wave.  They are 11 of Niger's 28 firings and are the corpus's
largest ratio.  A REPEATED SENTINEL IS NOT A LONE EXTREME: whether this screen
sees one is an accident of how often it was keyed, which is exactly why
stripping it is a separate job with a separate issue.

Separately, Malawi's ``50 kg Bag`` rows reporting 2015 / 2016 / 2018 / 2019
bags are the calendar-year-keyed-into-a-numeric-field signature that
``transformations._KG_FACTOR_YEAR_BAND`` documents for reported factors; four
are caught here by the ratio alone, and no second rule is added for them --
that would be scope creep, and it is recorded in the ledger instead.

CACHE.  Like ``null_read_audit`` this is pure reporting -- it provably returns
its input unchanged -- so its entry points are listed in
``_build_registry._EXCLUDED_CALLABLES`` and its ledger in
``_EXCLUDED_CONSTANTS``.  Measured both ways: with the exclusions, adding this
module and its call site moved **0 of 12** probed ``Country._table_cache_hash``
values and **0 of 5** ``build_transforms_fingerprint`` values.
"""

from __future__ import annotations

import os
import warnings
from typing import Any

import numpy as np
import pandas as pd

__all__ = [
    "QuantityImplausibleError",
    "QuantityImplausibleWarning",
    "QUANTITY_OUTLIER_K",
    "QUANTITY_MIN_CELL",
    "QUANTITY_REFERENCE_QUANTILE",
    "QUANTITY_REFERENCE_FLOOR",
    "audit_quantities",
    "check_quantities",
    "quantity_reports",
]


_QUANTITY_STRICT_ENV = "LSMS_QUANTITY_STRICT"

#: How many times its comparison cell's :data:`QUANTITY_REFERENCE_QUANTILE` a
#: value must exceed before it is reported.  See the module docstring for the
#: measurement -- and for the statement that the ratio distribution has no gap,
#: so this is a stated tolerance rather than a discovered boundary.
QUANTITY_OUTLIER_K = 100.0

#: Smallest comparison cell that may supply a reference.  A thinner cell falls
#: to the next rung of the ladder; if no rung qualifies the row is not judged.
QUANTITY_MIN_CELL = 30

#: The cell statistic.  0.90 rather than 0.99/0.95 because at the 30-row floor
#: only the 90th percentile is immune to the two or three contaminating values
#: a defective cell actually carries -- measured on Tanzania's Timber cell.
QUANTITY_REFERENCE_QUANTILE = 0.90

#: Smallest reference the ladder may hand back, in the row's OWN unit.  A cell
#: whose 90th percentile is below one unit is not describing a scale -- it is
#: describing a junk unit label under which nine rows in ten record less than
#: one of whatever it is.  Uganda 2010-11 pooled every crop sharing
#: ``u='Others specify'`` into a cell with p90 = 0.6, and eight ordinary
#: harvests (Rice 300, Ground Nuts 150, ... Sugarcane 66) were named at 66x to
#: 500x it.  Strict positivity did not catch them: 0.6 is positive.  The floor
#: is stated in native units deliberately -- there is no cross-unit conversion
#: here and inventing one would be the very thing ``harvest_kg`` exists to do
#: carefully.
QUANTITY_REFERENCE_FLOOR = 1.0

#: WHICH columns of WHICH tables are screened.  A registry of *what is looked
#: at*, NOT an allowlist of cells excused from looking -- the distinction the
#: no-allowlist rule turns on, and the same kind of mapping as
#: ``country._ADDITIVE_MEASURE_COLUMNS``.  ``food_acquired.Quantity`` is the
#: obvious extension and is deliberately absent: it is unmeasured, and it is
#: 5.26M rows on GhanaLSS alone.
_SCREENED_COLUMNS: dict[str, tuple[str, ...]] = {
    "crop_production": ("Quantity",),
}

#: Alternate spellings of the identifying axes, in preference order.  All of
#: these are live: ``j`` (Tanzania, Uganda, Ethiopia) vs ``crop`` (Malawi,
#: Nigeria); ``plot`` vs ``plot_id``; and ``u`` is an index LEVEL in
#: Uganda/Malawi/Ethiopia but a COLUMN in Tanzania/Nigeria.
_AXES: dict[str, tuple[str, ...]] = {
    "t": ("t",),
    "u": ("u",),
    "j": ("j", "crop", "item"),
    "i": ("i",),
    "plot": ("plot", "plot_id", "parcel"),
}

#: How many offending rows a single warning names before it says "+N more".
_MAX_NAMED = 12

#: Guard on the folded group code.  Only reached by a frame with absurdly many
#: distinct labels; the widest product measured in the corpus is 4,590.
_MAX_COMBINED_WIDTH = 1 << 40


class QuantityImplausibleError(RuntimeError):
    """A screened quantity is present, non-null and impossible.

    Raised instead of the default warning when ``LSMS_QUANTITY_STRICT`` is set.
    """


class QuantityImplausibleWarning(RuntimeWarning):
    """A screened quantity is present, non-null and impossible.

    Its own class -- not a bare ``RuntimeWarning`` -- so callers, tests and CI
    can target it precisely:
    ``warnings.simplefilter("error", QuantityImplausibleWarning)``.
    """


def _quantity_strict() -> bool:
    """Whether an implausible value should RAISE rather than warn.

    Warn by default, for the reason ``country._grain_strict`` and
    ``null_read_audit._read_strict`` both give: a guard that breaks a working
    corpus on the day it lands gets reverted, and a revert is how the bug class
    survives.

    ITS OWN LEVER, deliberately -- not ``LSMS_READ_STRICT``.  ``CLAUDE.md``'s
    rule is that "a destroyed row and an empty column are different concerns
    and must ratchet separately"; a value that is present, non-null and
    impossible is a THIRD concern.  Folding it into ``LSMS_READ_STRICT`` would
    gate that lever's adoption in CI -- already blocked on the wave-vs-country
    granularity of ``optional:`` -- on every Malawi bag-of-maize outlier too.
    Identical ``{1,true,yes}`` spelling, so knowing one is knowing all three.
    """
    return os.environ.get(_QUANTITY_STRICT_ENV, "").lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# Reading the frame.  Index level or column, under any of its live spellings.
# ---------------------------------------------------------------------------

def _axis(df: pd.DataFrame, axis: str) -> tuple[np.ndarray, np.ndarray] | None:
    """``(codes, categories)`` for *axis*, or ``None`` if the frame has none.

    The alternate-aware twin of ``transformations._level_or_column``: it looks
    for each spelling in :data:`_AXES` as an index level first, then as a
    column.  It hands back INTEGER CODES rather than the labels themselves,
    because both things this module does with an axis -- grouping, and naming
    the handful of rows that fire -- want codes, and materialising 130k object
    labels to do either is pure waste.  A ``MultiIndex`` and a categorical
    column already carry codes; everything else is factorised once.  A code of
    ``-1`` means the label is missing, and missing is ITS OWN group -- the rows
    with no unit label are among the likeliest to be defective, and exempting
    them silently would be the opposite of the point.

    Measured on Uganda (130,606 rows): reading the three grouping axes cost
    21.6 ms as object arrays plus 33 ms to factorise them; off the MultiIndex
    codes both numbers are ~0.
    """
    names = list(df.index.names or [])
    for name in _AXES.get(axis, (axis,)):
        if name in names:
            position = names.index(name)
            if isinstance(df.index, pd.MultiIndex):
                return (np.asarray(df.index.codes[position], dtype=np.int64),
                        np.asarray(df.index.levels[position], dtype=object))
            return _factorized(df.index)
        if name in df.columns:
            column = df[name]
            if isinstance(column.dtype, pd.CategoricalDtype):
                return (np.asarray(column.cat.codes, dtype=np.int64),
                        np.asarray(column.cat.categories, dtype=object))
            return _factorized(column)
    return None


def _factorized(values: Any) -> tuple[np.ndarray, np.ndarray]:
    """``(codes, categories)`` for anything without codes of its own."""
    codes, categories = pd.factorize(np.asarray(values, dtype=object),
                                     use_na_sentinel=False)
    return codes.astype(np.int64), np.asarray(categories, dtype=object)


def _label(axis: tuple[np.ndarray, np.ndarray] | None, position: int
           ) -> str | None:
    """The *position*-th label of *axis*, or ``None`` for missing / absent."""
    if axis is None:
        return None
    code = int(axis[0][position])
    if code < 0:
        return None
    return str(axis[1][code])


def _numeric(series: pd.Series) -> np.ndarray:
    """*series* as a positional ``float64`` array, ``pd.NA`` and junk -> ``nan``.

    Not decoration: ``Quantity`` is frequently a nullable masked dtype, and a
    bare ``>`` between a masked array and a float array raises or yields
    ``pd.NA`` rather than ``False``.
    """
    values = pd.to_numeric(series, errors="coerce")
    return values.to_numpy(dtype="float64", na_value=np.nan)


def _group_stats(values: np.ndarray, codes: np.ndarray,
                 quantile: float) -> tuple[np.ndarray, np.ndarray]:
    """Per-ROW ``(quantile of its group, size of its group)``.

    A hand-rolled group-quantile rather than ``groupby(...).transform``, and
    the reason is measured: this audit runs on the READ path of every
    ``crop_production`` call, and the pandas spelling cost 184-228 ms on Uganda
    (130,606 rows, three rungs), a 1.4x-1.6x slowdown of the whole warm read.
    One ``lexsort`` per rung plus flat indexing does the same arithmetic --
    linear interpolation between order statistics, exactly ``Series.quantile``'s
    default -- and is pinned against pandas by
    ``tests/test_quantity_screen.py::test_group_quantile_matches_pandas``.

    Only FINITE values enter a group's quantile or its count: ``NaN`` is
    missing and an infinity is not a measurement, so neither may define the
    scale other rows are judged against.  Both still get a reference from their
    own group, and are still tested against it.
    """
    n = len(values)
    stat = np.full(n, np.nan)
    size = np.zeros(n, dtype=np.int64)
    finite = np.isfinite(values)
    if not finite.any():
        return stat, size

    keep = np.flatnonzero(finite)
    order = keep[np.lexsort((values[keep], codes[keep]))]
    sorted_values = values[order]
    sorted_codes = codes[order]

    first = np.flatnonzero(np.concatenate(
        ([True], sorted_codes[1:] != sorted_codes[:-1])))
    counts = np.diff(np.concatenate((first, [len(order)])))
    position = quantile * (counts - 1)
    low = np.floor(position).astype(np.int64)
    high = np.ceil(position).astype(np.int64)
    frac = position - low
    group_stat = (sorted_values[first + low] * (1.0 - frac)
                  + sorted_values[first + high] * frac)
    group_codes = sorted_codes[first]

    slot = np.clip(np.searchsorted(group_codes, codes), 0,
                   max(len(group_codes) - 1, 0))
    hit = group_codes[slot] == codes
    stat[hit] = group_stat[slot[hit]]
    size[hit] = counts[slot[hit]]
    return stat, size


def _cell_reference(values: np.ndarray,
                    keys: dict[str, tuple[np.ndarray, np.ndarray]]
                    ) -> tuple[np.ndarray, np.ndarray]:
    """``(reference, rung)`` for every row.

    Walks the ladder ``(t, u, j)`` -> ``(t, u)`` -> ``(t)``, taking the first
    rung that offers a cell with at least :data:`QUANTITY_MIN_CELL` finite
    values and a strictly positive quantile, then lifting that quantile to
    :data:`QUANTITY_REFERENCE_FLOOR`.  A row no rung can serve keeps a ``NaN``
    reference and is never judged -- silence, not a guess.

    *keys* maps an axis to the ``(codes, categories)`` pair :func:`_axis`
    returns; the rungs are folded out of those codes arithmetically, so no axis
    is grouped or factorised more than once however many rungs use it.
    """
    n = len(values)
    # Shift by one so a missing label (code -1) folds like any other group.
    axis_codes = {a: keys[a][0] + 1 for a in keys}
    axis_width = {a: int(len(keys[a][1])) + 1 for a in keys}

    base = [k for k in ("t",) if k in keys]
    rungs: list[tuple[str, list[str]]] = []
    for cols in (base + [k for k in ("u", "j") if k in keys],
                 base + [k for k in ("u",) if k in keys],
                 base):
        name = ",".join(cols) if cols else "whole-frame"
        if name not in [seen for seen, _ in rungs]:
            rungs.append((name, cols))

    reference = np.full(n, np.nan)
    rung = np.empty(n, dtype=object)
    for name, cols in rungs:
        todo = np.isnan(reference)
        if not todo.any():
            break
        codes = np.zeros(n, dtype=np.int64)
        width = 1
        for col in cols:
            codes = codes * axis_width[col] + axis_codes[col]
            width *= axis_width[col]
            if width > _MAX_COMBINED_WIDTH:
                # Re-compress rather than risk an int64 overflow on a frame
                # with pathologically many labels.  Never reached in the
                # corpus (widest product measured: Malawi, 4,590).
                codes = np.unique(codes, return_inverse=True)[1].astype(np.int64)
                width = int(codes.max()) + 1 if n else 1
        stat, size = _group_stats(values, codes, QUANTITY_REFERENCE_QUANTILE)
        take = todo & (size >= QUANTITY_MIN_CELL) & (stat > 0)
        reference[take] = np.maximum(stat[take], QUANTITY_REFERENCE_FLOOR)
        rung[take] = name
    return reference, rung


# ---------------------------------------------------------------------------
# Site Q -- the delivered table.  Is a screened value impossible?
# ---------------------------------------------------------------------------

def audit_quantities(df: Any, column: str, *, country: str, table: str
                     ) -> list[dict[str, Any]]:
    """Measure which values of *column* are implausible against their cell.

    Returns one report per wave (per ``t``; one report for the whole frame when
    the frame carries no ``t``), each NAMING up to every offending row it
    found -- household, plot, crop, unit, value, reference and ratio.  Returns
    ``[]`` when nothing fires, which is the case for 99.98% of the corpus's
    ``crop_production`` rows.

    Deliberately silent on: a frame without *column*; an empty frame; a row
    whose value is null or non-finite; and a row for which no rung of the
    ladder offers a cell (see :func:`_cell_reference`).
    """
    if not isinstance(df, pd.DataFrame) or df.empty or column not in df.columns:
        return []
    q = _numeric(df[column])
    if not np.isfinite(q).any():
        return []

    named = {axis: _axis(df, axis) for axis in ("t", "i", "plot", "j", "u")}
    keys = {axis: named[axis] for axis in ("t", "u", "j")
            if named[axis] is not None}
    reference, rung = _cell_reference(q, keys)

    with np.errstate(invalid="ignore", divide="ignore"):
        ratio = q / reference
        flagged = ratio > QUANTITY_OUTLIER_K
    if not flagged.any():
        return []

    positions = np.flatnonzero(flagged)

    by_wave: dict[Any, list[dict[str, Any]]] = {}
    for pos in positions:
        offender = {
            "i": _label(named["i"], pos),
            "plot": _label(named["plot"], pos),
            "j": _label(named["j"], pos),
            "u": _label(named["u"], pos),
            "value": float(q[pos]),
            "reference": float(reference[pos]),
            "ratio": round(float(ratio[pos]), 1),
            "rung": None if rung[pos] is None else str(rung[pos]),
        }
        wave = _label(named["t"], pos)
        by_wave.setdefault(wave, []).append(offender)

    wave_sizes: dict[Any, int] = {}
    if named["t"] is not None:
        codes, categories = named["t"]
        sizes = np.bincount(codes + 1, minlength=len(categories) + 1)
        wave_sizes = {str(categories[k]): int(sizes[k + 1])
                      for k in range(len(categories))}

    reports: list[dict[str, Any]] = []
    for wave in sorted(by_wave, key=lambda w: (w is None, w)):
        offenders = sorted(by_wave[wave], key=lambda o: -o["ratio"])
        reports.append({
            "site": "built-table-quantity",
            "country": country,
            "table": table,
            "column": column,
            "wave": wave,
            "rows": int(wave_sizes.get(wave, len(df))),
            "n_implausible": len(offenders),
            "max_ratio": offenders[0]["ratio"],
            "k": QUANTITY_OUTLIER_K,
            "reference_quantile": QUANTITY_REFERENCE_QUANTILE,
            "min_cell": QUANTITY_MIN_CELL,
            "reference_floor": QUANTITY_REFERENCE_FLOOR,
            "offenders": offenders,
        })
    return reports


# ---------------------------------------------------------------------------
# Reporting -- one choke point, mirroring null_read_audit._emit_null_read_report
# ---------------------------------------------------------------------------

def _format_offender(offender: dict[str, Any]) -> str:
    parts = [f"i={offender['i']}"]
    if offender["plot"] is not None:
        parts.append(f"plot={offender['plot']}")
    if offender["j"] is not None:
        parts.append(offender["j"])
    if offender["u"] is not None:
        parts.append(f"u={offender['u']}")
    parts.append(f"{offender['value']:,.10g} (x{offender['ratio']:,.1f} its "
                 f"cell's p{QUANTITY_REFERENCE_QUANTILE * 100:.0f} of "
                 f"{offender['reference']:,.10g}, cell keyed on "
                 f"{offender['rung']})")
    return " ".join(parts)


def _format_quantity_report(report: dict[str, Any]) -> str:
    where = f"{report['country']}/{report['table']}"
    if report.get("wave"):
        where = f"{where} {report['wave']}"
    offenders = report["offenders"]
    shown = offenders[:_MAX_NAMED]
    more = len(offenders) - len(shown)
    listed = "; ".join(_format_offender(o) for o in shown)
    return (
        f"{where}: {report['n_implausible']:,} of {report['rows']:,} row(s) "
        f"report a '{report['column']}' more than {report['k']:,.0f}x the "
        f"{report['reference_quantile'] * 100:.0f}th percentile of comparable rows "
        f"(same wave, unit and crop where at least {report['min_cell']} such "
        f"rows exist): {listed}{f' (+{more} more)' if more else ''}. "
        f"NOTHING HAS BEEN CHANGED -- these rows are returned exactly as they "
        f"are stored, because clipping an implausible harvest would hide the "
        f"defect rather than fix it. Read this as a work queue: check the "
        f"wave's source column and its unit label (a quantity keyed into a "
        f"container field, a calendar year keyed into a quantity field, and a "
        f"missing-value sentinel that was never stripped have each been the "
        f"answer in this corpus). Use quantity_reports() to get the rows "
        f"programmatically. Set {_QUANTITY_STRICT_ENV}=1 to make this fatal."
    )


# Reports filed during this process, keyed by (country, table).  A module-level
# ledger and not ``df.attrs``, for the reason GH #323 documents: pandas drops
# ``attrs`` whenever its inputs disagree, so routing a finding through attrs
# would silently lose exactly the findings this exists to surface.
_QUANTITY_LEDGER: dict[tuple[str, str], list[dict[str, Any]]] = {}


def _record_quantity_report(report: dict[str, Any]) -> None:
    """File a report for :func:`quantity_reports`, then emit it."""
    key = (str(report.get("country") or "?"), str(report.get("table") or "?"))
    existing = _QUANTITY_LEDGER.setdefault(key, [])
    if report not in existing:
        existing.append(report)
    _emit_quantity_report(report)


def _emit_quantity_report(report: dict[str, Any]) -> None:
    """Raise (strict) or warn (default).  The single choke point."""
    msg = _format_quantity_report(report)
    if _quantity_strict():
        raise QuantityImplausibleError(msg)
    warnings.warn(msg, QuantityImplausibleWarning, stacklevel=2)


def quantity_reports(country: str | None = None,
                     table: str | None = None) -> list[dict]:
    """Implausible-value reports filed during this process.

    Public read-only accessor -- the twin of ``null_read_audit`` 's
    :func:`~lsms_library.null_read_audit.null_read_reports` and of
    ``country.grain_reports`` -- for tests, for ``bench/feature_audit``, and
    for an analyst who wants the offending rows as data rather than as prose.
    Each report carries an ``offenders`` list naming every flagged row, even
    though the warning text shows only the first few.
    """
    out: list[dict[str, Any]] = []
    for (c, t), reports in _QUANTITY_LEDGER.items():
        if country is not None and c != country:
            continue
        if table is not None and t != table:
            continue
        out.extend(reports)
    return out


# ---------------------------------------------------------------------------
# The one entry point the framework calls.  See the CACHE note in the module
# docstring: this and :func:`audit_quantities` are excluded from the build
# fingerprint because they provably cannot change a returned value.
# ---------------------------------------------------------------------------

def check_quantities(df: Any, *, country: str, table: str) -> Any:
    """Site Q.  Audit a built table's screened quantity columns.

    Returns *df* unchanged -- always, and by contract.  Call it for its effect,
    not its value.  Tables not named in :data:`_SCREENED_COLUMNS` cost one
    dictionary lookup.
    """
    columns = _SCREENED_COLUMNS.get(str(table), ())
    if not columns:
        return df
    try:
        reports: list[dict[str, Any]] = []
        for column in columns:
            reports.extend(audit_quantities(df, column, country=country,
                                            table=table))
    except QuantityImplausibleError:
        raise
    except Exception as exc:                                   # noqa: BLE001
        # An instrument that fails silently and reports clean is the disease
        # this module exists to cure, so say so rather than swallow it.  A
        # broken audit must never break a read, though: the read is the job.
        warnings.warn(
            f"{country}/{table}: quantity plausibility audit could not run "
            f"({type(exc).__name__}: {exc}). The table itself is unaffected, "
            f"but its quantities are NOT known to be plausible.",
            QuantityImplausibleWarning, stacklevel=2)
        return df
    for report in reports:
        _record_quantity_report(report)
    return df


def _clear_quantity_reports() -> None:
    """Drop every filed report.  For tests only -- the ledger is process-wide
    and a test that asserts on it must not inherit another test's findings."""
    _QUANTITY_LEDGER.clear()
