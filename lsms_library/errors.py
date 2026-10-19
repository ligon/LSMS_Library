"""Shared exception types for lsms_library.

Kept dependency-free (a leaf module) so both ``country`` and ``feature`` can
import it without a circular reference.
"""
from __future__ import annotations


class LabelUnavailableError(KeyError):
    """A country cannot honour a ``labels=X`` relabeling request.

    Raised by :meth:`Country._relabel_j` when the country has no food-label
    table, or its table lacks the requested column (e.g. ``labels='Aggregate'``
    against an EHCVM country that only curates ``Original Label``).

    Subclasses :class:`KeyError` so existing ``except KeyError`` handlers -- and
    direct ``Country(...).food_*(labels=...)`` callers -- keep catching it
    unchanged.  ``Feature`` catches it *specifically* to degrade gracefully:
    drop the country with one aggregated warning plus a ``df.attrs`` marker,
    rather than conflating a country that simply never curated the label with a
    genuine per-country build failure (a malformed table, a real bug, ...).
    """


class UnitsUnavailableError(ValueError):
    """A country cannot honour a ``units=X`` request on a derived food table.

    Raised by :func:`lsms_library.transformations.food_prices_from_acquired`
    when the requested basis needs a column the country's ``food_acquired``
    does not carry -- ``Price`` for ``units='kgprice'`` / ``'unitprice'`` (the
    instrument records no unit price), ``Expenditure`` / ``Quantity`` for the
    ``*value`` modes.  Before GH #828 the ``*price`` case returned an empty
    ``(0, 1)`` frame silently, so "the survey never asks a price" looked
    exactly like "a filter dropped every row", and ``Feature`` concatenated
    the empties.

    The twin of :class:`LabelUnavailableError` for ``units=``: ``Feature``
    catches it *specifically* and drops the country with one aggregated
    warning plus ``df.attrs['units_unavailable']`` (Contract B), rather than
    conflating it with a genuine build failure.  Subclasses :class:`ValueError`
    -- what the ``*value`` modes raised for a missing column before -- so
    existing ``except ValueError`` handlers keep catching it.

    Attributes: ``country`` (``None`` until :class:`~lsms_library.Country`
    names it), ``units``, ``missing`` (the absent column names) and
    ``table``.
    """

    def __init__(self, *, units: str, missing, country: str | None = None,
                 table: str = 'food_prices'):
        self.units = units
        self.missing = list(missing)
        self.country = country
        self.table = table
        who = f"{country}: " if country else ""
        super().__init__(
            f"{who}{table}(units={units!r}) needs {self.missing} on "
            f"food_acquired, which {country or 'this country'} does not carry "
            f"-- the survey records no such column, so this basis cannot be "
            f"served (not an empty result: there is nothing to compute it "
            f"from).  Use a mode whose inputs are present."
        )

    def for_country(self, country: str) -> "UnitsUnavailableError":
        """The same error, naming *country* (raised by ``Country``)."""
        return type(self)(units=self.units, missing=self.missing,
                          country=country, table=self.table)


class PeriodUnavailableError(ValueError):
    """A country cannot honour a ``period=`` request for some of its waves.

    Raised by :func:`lsms_library.recall.to_period` (and so by
    ``Country(...).food_expenditures(period=...)`` /
    ``food_quantities(period=...)``) when the frame holds a wave ``t`` whose
    recall window is not established: no record, ``basis='not-recorded'`` or
    another weak rung, a window that varies inside the wave
    (``within_wave_variation``), or a record missing ``recall_days`` /
    ``n_asks``.  GH #851 Phase II.

    The third member of the Contract B family, after
    :class:`LabelUnavailableError` and :class:`UnitsUnavailableError`:
    ``Country`` raises, ``Feature`` catches it *specifically* and degrades --
    at the grain of the WAVE, not the country: it re-calls the country with
    ``waves=convertible`` and drops only the unknown cells, with one
    aggregated warning and ``df.attrs['period_unavailable']``.  Subclasses
    :class:`ValueError` for the same reason ``UnitsUnavailableError`` does.

    Attributes: ``country``, ``period``, ``table``, ``unavailable`` (a
    ``{t: reason}`` dict) and ``convertible`` (the sorted ``t`` values in the
    same frame whose window IS established -- pass them as ``waves=`` to get
    the converted subset).
    """

    def __init__(self, *, country: str, period: str, table: str,
                 unavailable: dict, convertible):
        self.country = country
        self.period = period
        self.table = table
        self.unavailable = dict(unavailable)
        self.convertible = sorted(convertible)
        why = "; ".join(f"{t}: {r}" for t, r in sorted(self.unavailable.items()))
        hint = (f"Pass waves={self.convertible!r} to convert those, or omit "
                f"period= to get the recorded window."
                if self.convertible else
                "No wave of this frame converts; omit period= to get the "
                "recorded window.")
        super().__init__(
            f"{country}: {table}(period={period!r}) needs each wave's recall "
            f"window, and it is not established for "
            f"{len(self.unavailable)} wave(s) -- {why}.  Nothing was "
            f"converted: scaling only the known waves would return one "
            f"column on two clocks.  {hint}  See ll.recall({country!r}).")
