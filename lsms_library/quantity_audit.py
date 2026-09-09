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
row's **comparison cell** -- the finest of ``(t, u, j)`` -> ``(t, u)`` ->
``(t)`` that holds at least :data:`QUANTITY_MIN_CELL` non-null quantities and
whose quantile is strictly positive.  The ladder is
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

*Why the cell must hold 30 rows and its quantile must exceed zero.*  A thin
cell cannot manufacture a reference, and a cell whose 90th percentile is zero
carries no scale at all, so it cannot support a plausibility judgement.  Cost
of omitting the second condition, measured: six Malawi rows of 2-30 units fire
at ratio ``inf`` -- a 2 kg cassava harvest named as implausible, which is
precisely the false positive that teaches people to ignore a guard.

*Why K = 100 -- and the honest part: THERE IS NO GAP.*  ``null_read_audit``
could set its trigger "in the gap" because one existed (corpus max 28.3% of a
frame's columns all-null, every known-bad parse >= 40%).  Here there is none.
Over 277,277 rows with a reference (Tanzania + Malawi + Nigeria + Ethiopia,
warm parquets) the ratio ``Quantity / reference`` runs::

    p90 = 1.0    p99 = 3.7    p99.9 = 20.0    p99.99 = 173.7    max = 11,029

-- smooth, with no discontinuity near the top.  So 100 is a **stated
tolerance, not a discovered boundary**, and it is stated as: two orders of
magnitude above what nine in ten comparable rows report; five times the
corpus's own 99.9th percentile of that ratio; and below 173.3, where the
4,000,000 kg Timber row sits, so the rows the issue names actually fire.  The
sensitivity is smooth and is recorded so the choice stays re-checkable --
K=50 -> 94 rows, K=100 -> 49, K=150 -> 34, K=300 -> 21, on the same four
countries.

WHAT IT FIRES ON TODAY -- every country whose ``crop_production`` was warm
(``pd.read_parquet`` on the L2-country parquet, no rebuild):

===========  ===========  ==============  ===========
country      rows         fired, K=100    audit cost
===========  ===========  ==============  ===========
Tanzania          14,126              14        43 ms
Uganda           133,683              36       257 ms
Malawi           131,379              16       225 ms
Nigeria           62,844               9       118 ms
Ethiopia          85,519              10       165 ms
**total**    **427,551**   **85** (0.02%)
===========  ===========  ==============  ===========

Tens of rows corpus-wide, appearing only on the country being read, is a work
queue rather than a firehose -- the same reading ``null_read_audit`` gives its
own 86 Site-B findings.  The cost is the same order as Site B's (196 ms on the
largest built table): 257 ms on the largest ``crop_production`` in the corpus.

DELIBERATELY NO ALLOWLIST, for the reason ``country._grain_strict`` gives: an
allowlist is the same disease with a registry.  A cell that is loud and
legitimate stays loud until the data or the config can say why.

TWO SIGNATURES IT SEES, AND ONE IT MOSTLY DOES NOT -- correctly.  Uganda
carries **3,097** rows whose ``Quantity`` is exactly ``99999``, the 2009-10
missing-value sentinel (GH #861).  **Nine** of them fire here.  That is not a
miss: the sentinel saturates most of the ``(t, u, j)`` cells it appears in, so
in those cells 99999 *is* the 90th percentile and the rows are not
distributional outliers by any honest reading; the nine that fire are the ones
that leaked into a cell they do not dominate (2009-10 Millet, ``u='Unknown'``,
whose p90 is 2.8).  A REPEATED SENTINEL IS NOT A LONE EXTREME -- different
defect, different fix, and #861 owns it.  Expect this country's count to move
when #861 lands.

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

def _axis(df: pd.DataFrame, axis: str) -> pd.Series | None:
    """The values of *axis* as a positional Series, or ``None``.

    The alternate-aware twin of ``transformations._level_or_column``: it looks
    for each spelling in :data:`_AXES` as an index level first, then as a
    column.  ``astype(object)`` because ``u`` and ``j`` arrive as pandas
    categoricals from ``.dta`` and a categorical groupby key would form the
    whole unobserved cross-product (``CLAUDE.md`` §Gotchas).
    """
    names = list(df.index.names or [])
    for name in _AXES.get(axis, (axis,)):
        if name in names:
            values = df.index.get_level_values(name)
            return pd.Series(np.asarray(values, dtype=object),
                             index=pd.RangeIndex(len(df)))
        if name in df.columns:
            return pd.Series(np.asarray(df[name], dtype=object),
                             index=pd.RangeIndex(len(df)))
    return None


def _numeric(series: pd.Series) -> pd.Series:
    """*series* as positional float64, ``pd.NA`` and junk becoming ``nan``.

    Not decoration: ``Quantity`` is frequently a nullable masked dtype, and a
    bare ``>`` between a masked array and a float array raises or yields
    ``pd.NA`` rather than ``False``.
    """
    values = pd.to_numeric(series, errors="coerce")
    return pd.Series(values.to_numpy(dtype="float64", na_value=np.nan),
                     index=pd.RangeIndex(len(series)))


def _codes(keys: list[pd.Series]) -> np.ndarray:
    """One integer group code per row, from *keys* taken together.

    ``use_na_sentinel=False`` is the load-bearing part: a missing ``u`` or
    ``j`` becomes its OWN group rather than being dropped.  The rows with no
    unit label are among the likeliest to be defective, and exempting them
    silently would be the opposite of the point.

    Collapsing the keys to one ``int64`` label array is also modestly faster
    than ``groupby`` over a list of object-dtype Series -- MEASURED on Malawi
    (131,379 rows, the three-key rung, best of five): 26.3 ms -> 19.7 ms, a
    1.33x speedup, of which this function itself costs 10.5 ms.  A 1.3x, not
    the order of magnitude one might assume; it is kept because it is free and
    because the NA-group semantics above are wanted regardless.
    """
    codes = np.zeros(len(keys[0]) if keys else 0, dtype=np.int64)
    for key in keys:
        labels, uniques = pd.factorize(key, use_na_sentinel=False)
        codes = codes * max(len(uniques), 1) + labels.astype(np.int64)
    return codes


def _cell_reference(q: pd.Series, keys: dict[str, pd.Series]
                    ) -> tuple[pd.Series, pd.Series]:
    """``(reference, rung)`` for every row of *q*.

    Walks the ladder ``(t, u, j)`` -> ``(t, u)`` -> ``(t)``, taking the first
    rung that offers a cell with at least :data:`QUANTITY_MIN_CELL` non-null
    values and a strictly positive quantile.  A row no rung can serve keeps a
    ``NaN`` reference and is never judged -- silence, not a guess.
    """
    base = [k for k in ("t",) if k in keys]
    rungs: list[tuple[str, list[str]]] = []
    for cols in (base + [k for k in ("u", "j") if k in keys],
                 base + [k for k in ("u",) if k in keys],
                 base):
        name = ",".join(cols) if cols else "whole-frame"
        if name not in [n for n, _ in rungs]:
            rungs.append((name, cols))

    reference = pd.Series(np.nan, index=q.index)
    rung = pd.Series(pd.NA, index=q.index, dtype=object)
    for name, cols in rungs:
        if not reference.isna().any():
            break
        grouped = q.groupby(_codes([keys[c] for c in cols]) if cols
                            else np.zeros(len(q), dtype=np.int64))
        stat = grouped.transform("quantile", QUANTITY_REFERENCE_QUANTILE)
        count = grouped.transform("count")
        candidate = stat.where((count >= QUANTITY_MIN_CELL) & (stat > 0))
        take = reference.isna() & candidate.notna()
        reference = reference.where(~take, candidate)
        rung = rung.where(~take, name)
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
    if not np.isfinite(q.to_numpy()).any():
        return []

    keys = {}
    for axis in ("t", "u", "j"):
        series = _axis(df, axis)
        if series is not None:
            keys[axis] = series
    reference, rung = _cell_reference(q, keys)

    ratio = q / reference
    flagged = (ratio > QUANTITY_OUTLIER_K).fillna(False).to_numpy()
    if not flagged.any():
        return []

    named = {axis: _axis(df, axis) for axis in ("t", "i", "plot", "j", "u")}
    positions = np.flatnonzero(flagged)

    by_wave: dict[Any, list[dict[str, Any]]] = {}
    for pos in positions:
        offender = {
            "i": None if named["i"] is None else str(named["i"].iat[pos]),
            "plot": None if named["plot"] is None else str(named["plot"].iat[pos]),
            "j": None if named["j"] is None else str(named["j"].iat[pos]),
            "u": None if named["u"] is None else str(named["u"].iat[pos]),
            "value": float(q.iat[pos]),
            "reference": float(reference.iat[pos]),
            "ratio": round(float(ratio.iat[pos]), 1),
            "rung": None if pd.isna(rung.iat[pos]) else str(rung.iat[pos]),
        }
        wave = None if named["t"] is None else str(named["t"].iat[pos])
        by_wave.setdefault(wave, []).append(offender)

    wave_sizes: dict[Any, int] = {}
    if named["t"] is not None:
        wave_sizes = named["t"].astype(str).value_counts().to_dict()

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
