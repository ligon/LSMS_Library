"""Cross-country Feature class for assembling harmonized DataFrames."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from importlib.resources import files

from .yaml_utils import load_yaml
from .currency import CURRENCY_LEVEL, is_monetary_table
from .paths import countries_root


def _load_global_columns() -> dict[str, dict[str, Any]]:
    """Load the Columns section from the global data_info.yml."""
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("Columns", {})


def _canonical_index_levels(table_name: str) -> list[str]:
    """Return the canonical index level names for *table_name*.

    Reads the global ``Index Info: index_info`` section of data_info.yml,
    whose values are tuple strings like ``(t, v, i)``.  Returns ``[]`` when
    the table is not listed (no canonical reshaping is then attempted).
    """
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    spec = data.get("Index Info", {}).get("index_info", {}).get(table_name)
    if not isinstance(spec, str):
        return []
    cleaned = spec.strip()
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = cleaned[1:-1]
    return [tok.strip() for tok in cleaned.split(",") if tok.strip()]


# Tables whose measure columns are ADDITIVE across a dropped recall/visit level.
# When collapsing the duplicate index left after dropping that level, these must
# be SUMMED (not reduced via first(), which undercounts the cross-country total).
# Motivating case (GH #501): GhanaLSS food_acquired carries a per-visit level
# (~12 repeated visits over a month); CONTENTS.org states the visits are summed.
# Keeping first() there silently kept only ~48% of total Quantity.
_ADDITIVE_MEASURE_COLUMNS = {
    "food_acquired": ("Quantity", "Expenditure"),
}


def _collapse_duplicate_index(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Collapse duplicate index tuples left after dropping an extra index level.

    For additive-measure tables (GH #501) sum the additive columns and re-derive
    any unit-``Price`` column from the summed totals (price is per-unit, NOT
    additive).  Otherwise keep the first row per group (the historical default).
    """
    additive = _ADDITIVE_MEASURE_COLUMNS.get(table_name)
    grouped = df.groupby(level=list(df.index.names), observed=True)
    present = [c for c in (additive or ()) if c in df.columns]
    if not present:
        return grouped.first()
    agg = {c: ("sum" if c in present else "first") for c in df.columns}
    out = grouped.agg(agg)
    if "Price" in out.columns and {"Expenditure", "Quantity"} <= set(out.columns):
        out["Price"] = out["Expenditure"] / out["Quantity"].where(out["Quantity"] != 0)
    return out


def _harmonize_country_frame(
    df: pd.DataFrame, canonical_levels: list[str], country: str, table_name: str
) -> pd.DataFrame:
    """Coerce a single country's frame toward the canonical shape before concat.

    Defensive net for cross-country assembly (GH #325): a stray extra index
    level or an all-NaN leaked column on ONE country otherwise makes
    ``pd.concat`` fall back to an unnamed object index of stringified tuples
    for the WHOLE feature.  This drops all-NaN columns and removes index
    levels that are not part of the canonical index (only when every
    canonical level is present, so legitimately-reduced frames are left
    alone).  It never fabricates missing levels.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df

    # Drop columns that are entirely missing (e.g. a `date`/`v` column left
    # populated only on other countries).  Concat re-introduces them as NaN
    # where another country supplies values, so no information is lost.
    all_nan = [c for c in df.columns if df[c].isna().all()]
    if all_nan:
        warnings.warn(
            f"{table_name}: dropping all-NaN column(s) {all_nan} from {country} "
            "before cross-country concat"
        )
        df = df.drop(columns=all_nan)

    # Remove undeclared extra index levels so every country shares the same
    # MultiIndex names.  Only act when all canonical levels are present and
    # there is at least one extra (keeps single-country reductions intact).
    if canonical_levels and isinstance(df.index, pd.MultiIndex):
        names = list(df.index.names)
        have_all_canonical = all(lvl in names for lvl in canonical_levels)
        if have_all_canonical:
            # Put the canonical levels in canonical ORDER (then any extras).  Do
            # this even when there is no extra level to drop, so that the
            # positional set_names in __call__ aligns levels by MEANING, not
            # position: a country whose per-country index is correctly *named*
            # but *ordered* e.g. [i, t, v] would otherwise have its t/v/i values
            # scrambled under the canonical [t, v, i] labels (GH #498).
            ordered = [lvl for lvl in canonical_levels if lvl in names] + \
                      [n for n in names if n not in canonical_levels]
            if ordered != names:
                try:
                    df = df.reorder_levels(ordered)
                    names = ordered
                except (ValueError, TypeError):
                    pass
            # Remove undeclared extra index levels so every country shares the
            # same MultiIndex names (keeps single-country reductions intact).
            extra = [n for n in names if n not in canonical_levels]
            if extra and len(names) > len(extra):
                warnings.warn(
                    f"{table_name}: dropping extra index level(s) {extra} from "
                    f"{country} before cross-country concat"
                )
                df = df.droplevel(extra)
                if not df.index.is_unique:
                    df = _collapse_duplicate_index(df, table_name)

    return df


# Derived tables and the source table they require in data_scheme.yml
_DERIVED_SOURCE = {
    'household_characteristics': 'household_roster',
    'food_expenditures': 'food_acquired',
    'food_prices': 'food_acquired',
    'food_quantities': 'food_acquired',
}


def _discover_countries_for_table(table_name: str) -> list[str]:
    """Find all countries whose data_scheme.yml declares the given table.

    For derived tables (e.g. household_characteristics), discovers
    countries that have the source table (e.g. household_roster).
    """
    # If this is a derived table, look for its source instead
    lookup_name = _DERIVED_SOURCE.get(table_name, table_name)

    countries_dir = countries_root()
    result = []
    for entry in sorted(Path(countries_dir).iterdir()):
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        scheme_path = entry / "_" / "data_scheme.yml"
        if not scheme_path.exists():
            continue
        with open(scheme_path, "r", encoding="utf-8") as f:
            data = load_yaml(f)
        if not isinstance(data, dict):
            continue
        scheme = data.get("Data Scheme", {})
        if isinstance(scheme, dict) and (table_name in scheme or lookup_name in scheme):
            result.append(entry.name)
    return result


class Feature:
    """Assemble a single harmonized DataFrame for a table across countries.

    Parameters
    ----------
    table_name : str
        The table to load (e.g. ``'household_roster'``, ``'cluster_features'``).
    trust_cache : bool, optional
        If *True*, read existing cached parquets without validation (fast).
        Can be overridden per-call. Default ``False``.

    Examples
    --------
    >>> import lsms_library as ll
    >>> roster = ll.Feature('household_roster')
    >>> roster.countries          # which countries have this table
    >>> df = roster(['Mali', 'Uganda'])  # load specific countries
    >>> df = roster()                    # load all available countries
    >>> df = roster(trust_cache=True)    # fast read from cache
    """

    def __init__(self, table_name: str, trust_cache: bool = False) -> None:
        self.table_name = table_name
        self.trust_cache = trust_cache
        self._countries: list[str] | None = None

    def __repr__(self) -> str:
        return f"Feature({self.table_name!r})"

    @property
    def countries(self) -> list[str]:
        """Countries that declare this table in their data_scheme.yml."""
        if self._countries is None:
            self._countries = _discover_countries_for_table(self.table_name)
        return self._countries

    @property
    def columns(self) -> list[str]:
        """Required columns from the global data_info.yml for this table."""
        all_columns = _load_global_columns()
        table_cols = all_columns.get(self.table_name, {})
        return [
            col for col, meta in table_cols.items()
            if isinstance(meta, dict) and meta.get("required", False)
        ]

    def __call__(self, countries: list[str] | None = None, trust_cache: bool | None = None,
                 currency: str | None = 'index', numeraire: str | None = None) -> pd.DataFrame:
        """Load and concatenate data across countries.

        Parameters
        ----------
        countries : list of str, optional
            Countries to include. Defaults to all available countries.
        trust_cache : bool, optional
            If *True*, read existing cached parquets without validation.
            Defaults to the instance-level setting from ``__init__``.
        currency : {'index', 'column', None}, optional
            Attach the ISO 4217 currency code to monetary tables.  Defaults to
            ``'index'`` here (cross-country stacking is exactly where mixed
            currencies are silently incommensurable) -- unlike single-country
            ``Country(...)`` calls, which default to ``None``.  A no-op for
            non-monetary tables (there is nothing to label).  See
            :func:`lsms_library.currency.attach_currency`.
        numeraire : str, optional
            Convert monetary columns to a comparable basis -- a target column of
            ``conversion_factors.org`` (e.g. ``'PPP-2017'``).  Supersedes
            ``currency`` (the converted frame is labelled with the target).  A
            no-op for non-monetary tables.  See
            :func:`lsms_library.conversion.convert`.

        Returns
        -------
        pd.DataFrame
            DataFrame with a ``country`` index level prepended.
        """
        from . import Country

        if currency is not None and currency not in {'index', 'column'}:
            raise ValueError(
                f"currency must be 'index', 'column', or None; got {currency!r}"
            )
        monetary = is_monetary_table(self.table_name)
        if numeraire is not None and monetary:
            from .conversion import conversion_targets
            if numeraire not in conversion_targets():
                raise ValueError(
                    f"Unknown numeraire {numeraire!r}; available: {conversion_targets()}"
                )
        effective_trust_cache = trust_cache if trust_cache is not None else self.trust_cache
        targets = countries if countries is not None else self.countries
        frames: list[pd.DataFrame] = []
        canonical_levels = _canonical_index_levels(self.table_name)

        # numeraire supersedes currency; both are no-ops for non-monetary tables.
        # Either way the output carries a `currency` index level (relabelled to
        # the basis token for numeraire), so widen the canonical index to keep it.
        use_numeraire = numeraire if monetary else None
        pass_currency = None if use_numeraire else (currency if monetary else None)
        if (use_numeraire is not None or pass_currency == 'index') and canonical_levels:
            canonical_levels = canonical_levels + [CURRENCY_LEVEL]

        for name in targets:
            try:
                c = Country(name, trust_cache=effective_trust_cache)
                method = getattr(c, self.table_name)
                # Pass numeraire/currency only for monetary tables (the generated
                # method accepts them); preserve the bare call otherwise.
                if use_numeraire is not None:
                    df = method(numeraire=use_numeraire)
                elif pass_currency is not None:
                    df = method(currency=pass_currency)
                else:
                    df = method()
                if not isinstance(df, pd.DataFrame) or df.empty:
                    warnings.warn(
                        f"No data for {self.table_name} in {name}"
                    )
                    continue
                # Coerce toward the canonical shape so one country's stray
                # column / extra index level can't collapse the whole
                # concatenated index to object tuples (GH #325).
                df = _harmonize_country_frame(
                    df, canonical_levels, name, self.table_name
                )
                # Prepend country as an index level
                df = pd.concat({name: df}, names=["country"])
                frames.append(df)
            except Exception as e:  # broad catch intentional: surface per-country failures as warnings
                # Cross-country aggregation must not crash on one country's
                # implementation error; the warning carries the specific type.
                warnings.warn(
                    f"Failed to load {self.table_name} for {name}: {type(e).__name__}: {e}"
                )

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames)

        # GH #326: pd.concat can leave the (structurally-consistent) index
        # levels UNNAMED, forcing callers to index positionally instead of
        # `groupby('country')`.  When the level count matches the canonical
        # shape (country + declared levels), restore the names — the per-country
        # frames were already coerced to canonical order by
        # _harmonize_country_frame above.  The nlevels-mismatch (genuinely
        # heterogeneous) case is left to the warning below.
        expected_names = ["country"] + canonical_levels
        if (result.index.nlevels == len(expected_names)
                and list(result.index.names) != expected_names):
            result.index = result.index.set_names(expected_names)

        # Surface (rather than silently return) the pathological case where
        # heterogeneous per-country indices made pandas fall back to an
        # unnamed object index of stringified tuples (GH #325).
        if len(frames) > 1 and list(result.index.names) == [None]:
            shapes = {
                f.index.get_level_values(0)[0] if len(f) else "?":
                    list(f.index.names)
                for f in frames
            }
            warnings.warn(
                f"{self.table_name}: cross-country index collapsed to an "
                f"unnamed object index; per-country index names differ: {shapes}"
            )

        return result
