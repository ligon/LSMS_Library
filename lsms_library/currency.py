"""(country, wave) -> ISO 4217 currency labeling for monetary tables.

Phase 1 ("label only") of the currency story.  Monetary columns in the
harmonized tables (``food_expenditures.Expenditure``, ``food_prices.Price``,
``assets.Value``, ``plot_labor.Wage``, ...) are in nominal local currency
units.  Within a single country they are commensurable; across countries
(``ll.Feature('food_expenditures')``) they are not -- NGN, UGX, XOF, ... at
wildly different scales with no label.  This module is the single read-time
mechanism that attaches an ISO 4217 alpha-3 code, and the join key for the
planned FX / PPP / CPI conversion layer (NOT built yet).

Source of truth: the ``Currency:`` section of ``lsms_library/data_info.yml``.
Currency is keyed on **(country, wave)**, not country alone -- two in-sample
redenominations (GhanaLSS GHC->GHS at 2007, Tajikistan TJR->TJS at 2000)
changed both the scale and the ISO code mid-survey -- and is **not injective**
with country (the 8 EHCVM/WAEMU countries all share XOF).

Design notes
------------
- Applied at API read time (in ``Country._finalize_result``), *after* the
  parquet cache read, so cached parquet shapes are never rewritten and no cache
  invalidation is needed.
- Off by default for single-country ``Country(...)`` calls (backward compatible
  -- adding an index level would change the grain the CFE demand estimator
  merges/reorders on); on (``'index'``) by default for ``Feature(...)``.
- The set of monetary columns is the union of a built-in seed
  (:data:`_DEFAULT_MONETARY`) and any ``monetary:`` flags declared in
  ``data_info.yml``'s ``Columns`` section or a country's ``data_scheme.yml`` --
  declarative and extensible without editing this module.
"""
from __future__ import annotations

from functools import lru_cache
from importlib.resources import files

import pandas as pd
import yaml

from .paths import countries_root
from .yaml_utils import load_yaml

#: Name of the currency index level / column attached to monetary tables.
CURRENCY_LEVEL = "currency"

#: Accepted ``currency=`` representation modes (``None`` means "omit").
_VALID_MODES = frozenset({"index", "column"})

#: Built-in seed of monetary columns per table, denominated in local currency.
#: Unioned at runtime with ``monetary: true`` flags in ``data_info.yml``'s
#: ``Columns`` section and each country's ``data_scheme.yml``.  The derived food
#: tables (``food_expenditures`` / ``food_prices``) are not in any
#: ``data_scheme.yml``, so the seed is the only place they can be declared
#: monetary.  Deflated / real columns are deliberately omitted (e.g.
#: EthiopiaRHS ``consumption.RealConsumptionPC``).
_DEFAULT_MONETARY: dict[str, frozenset[str]] = {
    "food_acquired": frozenset({"Expenditure", "Price"}),
    "food_expenditures": frozenset({"Expenditure"}),
    "food_prices": frozenset({"Price"}),
    "community_prices": frozenset({"Price"}),
    "assets": frozenset({"Value", "Purchase Price"}),
    "livestock": frozenset({"Value", "Purchase Price"}),
    "crop_production": frozenset({"Value_sold"}),
    "plot_labor": frozenset({"Wage"}),
    "earnings": frozenset({"Earnings"}),
    "income": frozenset({"income", "TotalIncome", "CropIncome",
                         "LivestockIncome", "OffFarmIncome", "WageIncome"}),
}


@lru_cache(maxsize=1)
def _load_data_info() -> dict:
    """Parse the global ``data_info.yml`` once."""
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache(maxsize=None)
def _load_country_scheme(country: str) -> dict:
    """The ``Data Scheme`` mapping from *country*'s ``data_scheme.yml``."""
    path = countries_root() / country / "_" / "data_scheme.yml"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = load_yaml(f)
    if not isinstance(data, dict):
        return {}
    scheme = data.get("Data Scheme", {})
    return scheme if isinstance(scheme, dict) else {}


def currency_for(country: str, wave: str | None = None):
    """ISO 4217 alpha-3 code for *country* (and optional *wave*).

    Resolves the ``Currency:`` section of ``data_info.yml``.  A scalar entry is
    constant across waves; a ``{default, overrides}`` mapping applies the
    per-wave ``overrides[wave]`` when it matches, else ``default``.

    Returns :data:`pandas.NA` when the country is unknown (or, for a mapping
    with no ``default``, the wave is unknown).

    >>> currency_for('Uganda')            # doctest: +SKIP
    'UGX'
    >>> currency_for('GhanaLSS', '2005-06')   # doctest: +SKIP
    'GHC'
    >>> currency_for('GhanaLSS', '2016-17')   # doctest: +SKIP
    'GHS'
    """
    spec = _load_data_info().get("Currency", {}).get(country)
    if isinstance(spec, str):
        return spec
    if isinstance(spec, dict):
        overrides = spec.get("overrides") or {}
        if wave is not None and wave in overrides:
            return overrides[wave]
        return spec.get("default", pd.NA)
    return pd.NA


@lru_cache(maxsize=None)
def _monetary_columns(table: str, country: str | None = None) -> frozenset[str]:
    """Column names in *table* denominated in local currency.

    Union of the built-in seed (:data:`_DEFAULT_MONETARY`), ``monetary: true``
    flags in ``data_info.yml``'s ``Columns`` section, and (when *country* is
    given) the country's ``data_scheme.yml``.  A column may set
    ``monetary: false`` in either YAML source to opt out (e.g. a real / deflated
    column); an explicit ``false`` wins.
    """
    cols: set[str] = set(_DEFAULT_MONETARY.get(table, frozenset()))
    opt_out: set[str] = set()

    def _scan(columns_dict) -> None:
        if not isinstance(columns_dict, dict):
            return
        for col, meta in columns_dict.items():
            if not isinstance(meta, dict):
                continue
            flag = meta.get("monetary")
            if flag is True:
                cols.add(col)
            elif flag is False:
                opt_out.add(col)

    _scan(_load_data_info().get("Columns", {}).get(table))
    if country:
        _scan(_load_country_scheme(country).get(table))

    return frozenset(cols - opt_out)


def is_monetary_table(table: str, country: str | None = None) -> bool:
    """Whether *table* carries any local-currency column (for *country*)."""
    return bool(_monetary_columns(table, country))


@lru_cache(maxsize=1)
def _all_monetary_columns() -> frozenset[str]:
    """Union of every monetary column name across all tables.

    Used by the standalone :func:`lsms_library.conversion.convert`, which sees
    a labelled frame but not the table name, to decide which columns to scale.
    """
    cols: set[str] = set()
    for names in _DEFAULT_MONETARY.values():
        cols |= set(names)
    for spec in _load_data_info().get("Columns", {}).values():
        if isinstance(spec, dict):
            for col, meta in spec.items():
                if isinstance(meta, dict) and meta.get("monetary") is True:
                    cols.add(col)
    return frozenset(cols)


def _redenomination_waves(country: str) -> frozenset[str]:
    """Waves of *country* whose ISO code is a redenomination override.

    These are pre-reform waves (e.g. GhanaLSS <= 2005-06 in GHC) whose nominal
    values are in the historical currency unit; the conversion layer declines
    to convert them in v1 (see conversion_factors.org).
    """
    spec = _load_data_info().get("Currency", {}).get(country)
    if isinstance(spec, dict):
        return frozenset((spec.get("overrides") or {}).keys())
    return frozenset()


def attach_currency(df: pd.DataFrame, country: str, table: str,
                    mode: str) -> pd.DataFrame:
    """Attach the ISO 4217 currency label to *df* for *country* / *table*.

    Parameters
    ----------
    df : pandas.DataFrame
        A finalized table frame.  Returned unchanged if empty.
    country, table : str
        Used to resolve the currency and to decide whether *table* is monetary
        (a no-op for non-monetary tables).
    mode : {'index', 'column'}
        ``'index'`` appends a ``currency`` index level (placed last);
        ``'column'`` adds a ``currency`` column.

    Notes
    -----
    The currency is resolved **per row** from the ``t`` (wave) index level, so
    within-country redenominations (Ghana, Tajikistan) are labeled correctly;
    when there is no ``t`` level the country default is used for every row.

    pandas-3.0 / CoW safe: returns a new frame, never mutates *df* in place,
    uses the nullable ``string`` dtype (``pd.NA`` for unknowns), and preserves
    ``df.attrs`` (notably ``id_converted``) across the ``set_index`` mutation.
    Idempotent: a frame already carrying a ``currency`` level/column is returned
    unchanged.
    """
    if mode not in _VALID_MODES:
        raise ValueError(
            f"currency mode must be one of {sorted(_VALID_MODES)} or None; "
            f"got {mode!r}"
        )
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df
    if not _monetary_columns(table, country):
        return df

    names = list(df.index.names)
    if CURRENCY_LEVEL in names or CURRENCY_LEVEL in df.columns:
        return df  # already labeled; don't double-apply

    saved_attrs = dict(df.attrs)

    # Resolve per-row currency from the wave (`t`) level.
    if "t" in names:
        tvals = df.index.get_level_values("t")
        uniq = {t: currency_for(country, t) for t in pd.unique(tvals)}
        codes = [uniq[t] for t in tvals]
    else:
        codes = [currency_for(country, None)] * len(df)
    currency_series = pd.Series(pd.array(codes, dtype="string"), index=df.index)

    out = df.copy()
    out[CURRENCY_LEVEL] = currency_series
    if mode == "index":
        out = out.set_index(CURRENCY_LEVEL, append=True)
    out.attrs = saved_attrs
    return out
