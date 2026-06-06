"""Cross-country Feature class for assembling harmonized DataFrames."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from importlib.resources import files

from .yaml_utils import load_yaml


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
        extra = [n for n in names if n not in canonical_levels]
        have_all_canonical = all(lvl in names for lvl in canonical_levels)
        if extra and have_all_canonical and len(names) > len(extra):
            ordered = [lvl for lvl in canonical_levels if lvl in names] + \
                      [n for n in names if n not in canonical_levels]
            try:
                df = df.reorder_levels(ordered)
            except (ValueError, TypeError):
                pass
            warnings.warn(
                f"{table_name}: dropping extra index level(s) {extra} from "
                f"{country} before cross-country concat"
            )
            df = df.droplevel(extra)
            if not df.index.is_unique:
                df = df.groupby(level=list(df.index.names), observed=True).first()

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

    countries_dir = files("lsms_library") / "countries"
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

    def __call__(self, countries: list[str] | None = None, trust_cache: bool | None = None) -> pd.DataFrame:
        """Load and concatenate data across countries.

        Parameters
        ----------
        countries : list of str, optional
            Countries to include. Defaults to all available countries.
        trust_cache : bool, optional
            If *True*, read existing cached parquets without validation.
            Defaults to the instance-level setting from ``__init__``.

        Returns
        -------
        pd.DataFrame
            DataFrame with a ``country`` index level prepended.
        """
        from . import Country

        effective_trust_cache = trust_cache if trust_cache is not None else self.trust_cache
        targets = countries if countries is not None else self.countries
        frames: list[pd.DataFrame] = []
        canonical_levels = _canonical_index_levels(self.table_name)

        for name in targets:
            try:
                c = Country(name, trust_cache=effective_trust_cache)
                method = getattr(c, self.table_name)
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
