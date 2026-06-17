"""Convert nominal local-currency monetary values to a comparable basis.

Phase 2 of currency handling.  Phase 1 (:mod:`lsms_library.currency`) tags
every monetary value with =(currency, t)=; this module turns that tag into a
conversion by looking up a per-=(Country, Date)= factor and dividing the
monetary columns.

The factor table is the pure org-table
=lsms_library/conversion/conversion_factors.org= (read via
:func:`df_from_orgfile`).  Every factor column is "LCU per target unit", so

    converted = nominal_LCU / factor.

The base year is encoded in the column name (e.g. ``PPP-2017`` already bakes in
the local-CPI deflation), so :func:`convert` is a single division with no
``base=``/``deflator=`` knobs -- a new base year is a new column.

**Time matching is dynamic / as-of.**  The factor table's ``Date`` column is an
ISO-8601 partial date at any resolution (``2019``, ``2019-08``, ``2019-08-15``),
resolved to the start of the period (``2019`` -> ``2019-01-01``).  Each row is
matched to the *most recent factor at or before its interview date*
(``merge_asof(direction='backward')``), so a household interviewed in 2019
within a ``2018-19`` wave gets the 2019 factor while one interviewed in 2018
gets 2018 -- per-household, exact, and progressively refinable (drop in monthly
or daily rows and the as-of join prefers them automatically).  Households with
no ``interview_date`` (and the 6 countries lacking the table) fall back to the
wave's nominal date (wave year -> Jan 1), reproducing the historical behaviour.

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
_NON_TARGET_COLUMNS = frozenset({"Country", "Currency", "Date", "_date", "CPI"})

_FACTORS_TABLE = "conversion_factors"


def _factors_path():
    return files("lsms_library") / "conversion" / "conversion_factors.org"


def _parse_date(v) -> pd.Timestamp:
    """ISO-8601 partial date -> start-of-period Timestamp.

    ``2019`` -> 2019-01-01, ``2019-08`` -> 2019-08-01, ``2019-08-15`` -> that
    day.  Accepts ints (a pure-year column read by df_from_orgfile becomes
    int64) and strings; returns ``NaT`` for blanks.
    """
    s = str(v).strip()
    if s in ("", "nan", "None", "<NA>", "---", "NaT"):
        return pd.NaT
    if re.fullmatch(r"\d+(\.0+)?", s):                 # '2019' or '2019.0'
        return pd.Timestamp(int(s.split(".")[0]), 1, 1)
    parts = s.split("-")
    if len(parts) == 2:                                # YYYY-MM
        return pd.Timestamp(int(parts[0]), int(parts[1]), 1)
    return pd.Timestamp(s)                             # YYYY-MM-DD


@lru_cache(maxsize=1)
def _load_factors() -> pd.DataFrame:
    """The conversion-factor table with a parsed ``_date`` (period start)."""
    df = df_from_orgfile(str(_factors_path()), name=_FACTORS_TABLE)
    df["_date"] = df["Date"].map(_parse_date)
    return df.dropna(subset=["_date"]).sort_values("_date").reset_index(drop=True)


def conversion_targets() -> list[str]:
    """Conversion-target column names available in the factor table."""
    return [c for c in _load_factors().columns if c not in _NON_TARGET_COLUMNS]


def _wave_to_year(wave) -> int | None:
    """First 4-digit calendar year in a wave label ('2005-06' -> 2005)."""
    m = re.search(r"\d{4}", str(wave))
    return int(m.group()) if m else None


@lru_cache(maxsize=None)
def _interview_dates(country: str) -> dict:
    """``{(i, t): Timestamp}`` of household interview dates for *country*.

    Loaded from ``Country(country).interview_date()``; ``{}`` when the country
    has no ``interview_date`` table or it cannot be built (callers then fall
    back to the wave's nominal date).
    """
    try:
        from . import Country
        c = Country(country, preload_panel_ids=False)
        if "interview_date" not in c.data_scheme:
            return {}
        idf = c.interview_date()
        if not isinstance(idf, pd.DataFrame) or idf.empty:
            return {}
        flat = idf.reset_index()
        datecol = next((col for col in ("Int_t", "int_t")
                        if col in flat.columns), None)
        if datecol is None:
            datecol = next((col for col in flat.columns
                            if pd.api.types.is_datetime64_any_dtype(flat[col])), None)
        if datecol is None or "i" not in flat.columns or "t" not in flat.columns:
            return {}
        flat = flat.dropna(subset=[datecol]).drop_duplicates(subset=["i", "t"])
        return {(str(r.i), str(r.t)): pd.Timestamp(getattr(r, datecol))
                for r in flat.itertuples()}
    except Exception as exc:  # noqa: BLE001 -- best-effort; fall back to wave dates
        logger_msg = f"interview_date unavailable for {country!r} ({exc}); " \
                     "falling back to wave-nominal dates"
        warnings.warn(logger_msg)
        return {}


def _query_dates(countries, households, waves, interview_dates) -> list:
    """Per-row as-of query date: interview date if known, else wave -> Jan 1."""
    out = []
    for ctry, hh, wave in zip(countries, households, waves):
        idates = interview_dates.get(ctry, {})
        d = idates.get((str(hh), str(wave))) if hh is not None else None
        if d is None or pd.isna(d):
            yr = _wave_to_year(wave)
            d = pd.Timestamp(yr, 1, 1) if yr is not None else pd.NaT
        out.append(d)
    return out


def convert(df: pd.DataFrame, to: str, *, country: str | None = None,
            columns=None, interview_dates=None) -> pd.DataFrame:
    """Convert the monetary columns of *df* to the *to* basis (as-of by date).

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
    interview_dates : mapping, optional
        ``{country: {(i, t): Timestamp}}`` (or a flat ``{(i, t): Timestamp}``)
        of household interview dates.  Defaults to auto-loading each country's
        ``interview_date`` table; pass ``{}`` to force the wave-nominal-date
        fallback (and skip the load -- handy in tests).

    Returns
    -------
    pandas.DataFrame
        A copy with the monetary columns divided by the per-row as-of factor,
        the ``currency`` label set to *to*, and provenance in
        ``attrs['conversion']``.  A row with no factor at or before its query
        date (or a NaN factor cell, e.g. Tajikistan 1999 PPP) becomes ``pd.NA``
        with a warning -- never a silently wrong number.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df

    targets = conversion_targets()
    if to not in targets:
        raise ValueError(f"Unknown conversion target {to!r}; available: {targets}")

    names = list(df.index.names)
    if "t" not in names:
        raise ValueError("convert() needs a 't' (wave) index level")

    if columns is None:
        columns = _all_monetary_columns()
    money = [c for c in df.columns if c in set(columns)]
    if not money:
        warnings.warn(f"convert(to={to!r}): no monetary columns found in frame; "
                      "returning unchanged")
        return df

    # Per-row country.
    if "country" in names:
        countries = [str(c) for c in df.index.get_level_values("country")]
    else:
        ctry = country if country is not None else df.attrs.get("country")
        if ctry is None:
            raise ValueError(
                "convert() cannot determine the country: pass country=, or use "
                "a frame with a 'country' index level / df.attrs['country']."
            )
        countries = [str(ctry)] * len(df)

    waves = [str(w) for w in df.index.get_level_values("t")]
    households = (list(df.index.get_level_values("i")) if "i" in names
                  else [None] * len(df))

    # Normalise interview_dates to {country: {(i, t): Timestamp}}.
    if interview_dates is None:
        idmap = {c: _interview_dates(c) for c in set(countries)}
    elif interview_dates and not isinstance(next(iter(interview_dates)), str):
        idmap = {c: dict(interview_dates) for c in set(countries)}  # flat (i,t) map
    else:
        idmap = dict(interview_dates)

    qdates = _query_dates(countries, households, waves, idmap)

    # As-of (backward) join: most recent factor at or before each query date.
    factors = _load_factors()
    left = pd.DataFrame({"_c": countries, "_q": pd.to_datetime(qdates),
                         "_pos": range(len(df))})
    right = factors[["Country", "_date", to]].rename(columns={"Country": "_c"})
    right = right.dropna(subset=["_date"]).sort_values("_date")
    valid = left.dropna(subset=["_q"]).sort_values("_q")
    if not valid.empty and not right.empty:
        merged = pd.merge_asof(valid, right, left_on="_q", right_on="_date",
                               by="_c", direction="backward")
        fac_by_pos = dict(zip(merged["_pos"], merged[to]))
    else:
        fac_by_pos = {}

    factor_vals = [fac_by_pos.get(p, pd.NA) for p in range(len(df))]
    factor = pd.Series(pd.array([v if pd.notna(v) else pd.NA for v in factor_vals],
                                dtype="Float64"), index=df.index)

    n_missing = int(factor.isna().sum())
    if n_missing:
        examples = sorted({(c, q.year if pd.notna(q) else None)
                           for c, q, f in zip(countries, qdates, factor_vals)
                           if pd.isna(f)})[:5]
        warnings.warn(
            f"convert(to={to!r}): no factor for {n_missing} row(s) at/before "
            f"their query date -> NA; e.g. {examples}"
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
    else:
        out[CURRENCY_LEVEL] = to

    out.attrs = saved_attrs
    out.attrs["conversion"] = {
        "to": to,
        "source": "lsms_library/conversion/conversion_factors.org",
    }
    return out
