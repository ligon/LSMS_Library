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
