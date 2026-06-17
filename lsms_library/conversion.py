"""Convert nominal local-currency monetary values to a comparable basis.

Phase 2 of currency handling.  Phase 1 (:mod:`lsms_library.currency`) tags
every monetary value with =(currency, t)=; this module turns that tag into a
conversion by joining a per-=(Country, Date)= factor table and dividing the
monetary columns.

The factor table is the pure org-table
=lsms_library/conversion/conversion_factors.org= (read via
:func:`df_from_orgfile`).  Every factor column is "LCU per target unit", so

    converted = nominal_LCU / factor.

The base year is encoded in the column name (e.g. ``PPP-2017`` already bakes in
the local-CPI deflation), so :func:`convert` is a single division with no
``base=``/``deflator=`` knobs -- a new base year is a new column.

Design: =slurm_logs/DESIGN_currency_conversion_2026-06-17.org=.
"""
from __future__ import annotations

import re
import warnings
from functools import lru_cache
from importlib.resources import files

import pandas as pd

from .currency import (
    CURRENCY_LEVEL,
    _all_monetary_columns,
)
from .local_tools import df_from_orgfile

#: Columns of conversion_factors.org that are not conversion targets.
_NON_TARGET_COLUMNS = frozenset({"Country", "Currency", "Date", "CPI"})

_FACTORS_TABLE = "conversion_factors"


def _factors_path():
    return files("lsms_library") / "conversion" / "conversion_factors.org"


@lru_cache(maxsize=1)
def _load_factors() -> pd.DataFrame:
    """The conversion-factor table indexed by ``(Country, Date)``."""
    df = df_from_orgfile(str(_factors_path()), name=_FACTORS_TABLE)
    df["Date"] = df["Date"].astype(int)
    return df.set_index(["Country", "Date"]).sort_index()


def conversion_targets() -> list[str]:
    """Conversion-target column names available in the factor table."""
    return [c for c in _load_factors().columns if c not in _NON_TARGET_COLUMNS]


def _wave_to_year(wave) -> int | None:
    """First 4-digit calendar year in a wave label ('2005-06' -> 2005)."""
    m = re.search(r"\d{4}", str(wave))
    return int(m.group()) if m else None


def convert(df: pd.DataFrame, to: str, *, country: str | None = None,
            columns=None) -> pd.DataFrame:
    """Convert the monetary columns of *df* to the *to* basis.

    Parameters
    ----------
    df : pandas.DataFrame
        A finalized (phase-1-labelled) frame with a ``t`` (wave) index level.
    to : str
        A conversion target -- a column of ``conversion_factors.org``
        (e.g. ``'FX'``, ``'PPP-2017'``, ``'USD-real-2017'``).
    country : str, optional
        The country, when *df* has no ``country`` index level (single-country
        frames).  Falls back to ``df.attrs['country']``.
    columns : iterable of str, optional
        Monetary columns to scale.  Defaults to the columns of *df* that are
        known monetary names (see :func:`currency._all_monetary_columns`).

    Returns
    -------
    pandas.DataFrame
        A copy with the monetary columns divided by the per-row factor, the
        ``currency`` label set to *to*, and provenance in ``attrs['conversion']``.
        Rows whose ``(country, year)`` factor is missing -- or whose wave is a
        pre-reform redenomination override -- become ``pd.NA`` (with a warning),
        never a silently wrong number.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df

    targets = conversion_targets()
    if to not in targets:
        raise ValueError(f"Unknown conversion target {to!r}; "
                         f"available: {targets}")

    names = list(df.index.names)
    if "t" not in names:
        raise ValueError("convert() needs a 't' (wave) index level")

    # Monetary columns actually present in this frame.
    if columns is None:
        columns = _all_monetary_columns()
    money = [c for c in df.columns if c in set(columns)]
    if not money:
        warnings.warn(f"convert(to={to!r}): no monetary columns found in frame; "
                      "returning unchanged")
        return df

    # Per-row country.
    if "country" in names:
        countries = df.index.get_level_values("country").to_numpy()
    else:
        ctry = country if country is not None else df.attrs.get("country")
        if ctry is None:
            raise ValueError(
                "convert() cannot determine the country: pass country=, or use "
                "a frame with a 'country' index level / df.attrs['country']."
            )
        countries = [ctry] * len(df)

    waves = df.index.get_level_values("t").to_numpy()
    factors = _load_factors()

    factor_vals = []
    missing = set()
    for ctry, wave in zip(countries, waves):
        # Pre-reform redenomination waves (GhanaLSS <= 2005-06, Tajikistan 1999,
        # ...) carry CONTEMPORANEOUS old-currency factors in the table (keyed on
        # the wave year), so they convert here too; a genuinely absent or NaN
        # factor (e.g. Tajikistan 1999 PPP, no 1999 CPI) -> NA + warning.
        year = _wave_to_year(wave)
        try:
            val = factors.at[(ctry, year), to]
        except KeyError:
            val = pd.NA
        if pd.isna(val):
            missing.add((ctry, year))
            factor_vals.append(pd.NA)
        else:
            factor_vals.append(val)

    factor = pd.Series(pd.array(factor_vals, dtype="Float64"), index=df.index)
    if missing:
        warnings.warn(
            f"convert(to={to!r}): no factor for {len(missing)} (country, year) "
            f"pair(s) -> NA: {sorted(missing)[:5]}"
        )

    saved_attrs = dict(df.attrs)
    out = df.copy()
    for col in money:
        out[col] = out[col] / factor

    # Relabel the basis: prefer the existing currency representation.
    if CURRENCY_LEVEL in names:
        out = out.rename(index={c: to for c in
                               out.index.get_level_values(CURRENCY_LEVEL).unique()},
                         level=CURRENCY_LEVEL)
    elif CURRENCY_LEVEL in out.columns:
        out[CURRENCY_LEVEL] = to
    else:
        out[CURRENCY_LEVEL] = to

    out.attrs = saved_attrs
    out.attrs["conversion"] = {
        "to": to,
        "source": "lsms_library/conversion/conversion_factors.org",
    }
    return out
