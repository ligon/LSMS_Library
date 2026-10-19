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


class UnregisteredCountryError(ValueError):
    """``Country(name)`` named a directory that is not a registered country.

    A country is REGISTERED when its directory under ``countries_root()``
    carries a ``_/data_scheme.yml`` -- exactly the set
    :func:`lsms_library.countries` returns.  A directory without one is staged
    microdata nobody has wired up yet (Bulgaria, Nicaragua, ... -- GH #915): it
    declares no tables, so a ``Country`` built on it could only fail later and
    further from the cause.  ``Country()`` therefore refuses it here, by
    decision (@ligon, 2026-10-18).

    Code that legitimately operates on a directory *being built* --
    acquisition (``data_access.discover_waves``), provenance stamping
    (``scripts/backfill_wave_provenance.py``), cache maintenance and other
    scaffolding -- passes ``Country(name, allow_unregistered=True)``.

    Subclasses :class:`ValueError` -- what ``Country()`` raises for a name with
    no directory at all -- so existing ``except ValueError`` handlers keep
    catching it.  Attributes: ``country`` (the name) and ``path`` (the
    directory, whose ``_/data_scheme.yml`` is missing).
    """

    def __init__(self, country: str, path):
        self.country = country
        self.path = path
        super().__init__(
            f"{country!r} is a directory under {path.parent} but not a "
            f"registered country: it has no _/data_scheme.yml, so it declares "
            f"no tables and is not in lsms_library.countries().  (Staged "
            f"microdata that has not been wired up yet; see the add-feature "
            f"skill.)  Code operating on a directory being built -- "
            f"acquisition, provenance stamping, scaffolding -- may pass "
            f"Country({country!r}, allow_unregistered=True)."
        )
