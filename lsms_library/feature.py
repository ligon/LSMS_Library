"""Cross-country Feature class for assembling harmonized DataFrames."""

from __future__ import annotations

import inspect
import warnings
from pathlib import Path
from typing import Any


_UNSET = object()


def _method_parameters(method: Any) -> set[str]:
    """Parameter names a generated Country method accepts (for kwarg forwarding)."""
    try:
        return set(inspect.signature(method).parameters)
    except (TypeError, ValueError):
        return set()

import pandas as pd
import yaml
from importlib.resources import files

from .yaml_utils import load_yaml
from .currency import CURRENCY_LEVEL, is_monetary_table
from .paths import countries_root
from .errors import LabelUnavailableError


def _load_global_columns() -> dict[str, dict[str, Any]]:
    """Load the Columns section from the global data_info.yml."""
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("Columns", {})


def _all_known_features() -> set[str]:
    """Every table any country declares in its data_scheme.yml, plus the
    runtime-derived tables -- i.e. the set of valid ``Feature(...)`` names.
    Used to reject typos with a helpful suggestion rather than silently
    returning an empty frame.
    """
    names: set[str] = set(_DERIVED_SOURCE)
    try:
        for f in Path(countries_root()).glob("*/_/data_scheme.yml"):
            try:
                ds = (load_yaml(f) or {}).get("Data Scheme") or {}
                names.update(ds.keys() if isinstance(ds, dict) else ds)
            except Exception:
                continue
    except Exception:
        pass
    return names


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


def _fabricates_missing_levels(table_name: str) -> bool:
    """Whether *table_name* opts into NaN-fabrication of missing canonical
    levels (``Index Info: fabricate_missing_levels`` in data_info.yml).

    When True, ``_harmonize_country_frame`` adds any missing canonical index
    levels to a reduced-index country as a ``pd.NA`` level (so every country
    shares the full canonical shape and is KEPT), instead of leaving it reduced
    to be modal-excluded.  Use only where the reduced shape is legitimate
    variation -- e.g. interview_date's single-visit countries vs per-visit
    EHCVM ones (#506).
    """
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    lst = data.get("Index Info", {}).get("fabricate_missing_levels", []) or []
    return table_name in lst


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
    df: pd.DataFrame, canonical_levels: list[str], country: str, table_name: str,
    fabricate_missing: bool = False,
) -> pd.DataFrame:
    """Coerce a single country's frame toward the canonical shape before concat.

    Defensive net for cross-country assembly (GH #325): a stray extra index
    level or an all-NaN leaked column on ONE country otherwise makes
    ``pd.concat`` fall back to an unnamed object index of stringified tuples
    for the WHOLE feature.  This drops all-NaN columns and removes index
    levels that are not part of the canonical index (only when every
    canonical level is present, so legitimately-reduced frames are left
    alone).

    By default it never fabricates missing levels.  When ``fabricate_missing``
    (a per-feature opt-in, #506), any canonical level absent from this country's
    index is added as a ``pd.NA`` level so reduced-index countries share the
    full canonical shape and are KEPT (rather than modal-excluded in __call__).
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
        # Opt-in (#506): fabricate any missing canonical level as a pd.NA index
        # level so a legitimately-reduced country keeps the full canonical shape
        # (and is KEPT, not modal-excluded).  E.g. interview_date single-visit
        # countries gain visit=NaN to stack with per-visit EHCVM countries.
        missing = [lvl for lvl in canonical_levels if lvl not in names]
        if fabricate_missing and missing:
            flat = df.reset_index()
            for lvl in missing:
                flat[lvl] = pd.NA
            df = flat.set_index(names + missing)
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

    def __getattribute__(self, name: str) -> Any:
        # Proxy the per-feature docstring from a representative Country method so
        # `Feature('food_expenditures').__doc__` mirrors
        # `Country(...).food_expenditures.__doc__` (GH #508).  Built lazily on
        # first access and cached; the class docstring is left intact.
        if name == "__doc__":
            cached = object.__getattribute__(self, "__dict__").get("_proxied_doc", _UNSET)
            if cached is not _UNSET:
                return cached
            doc = object.__getattribute__(self, "_build_proxied_doc")()
            object.__getattribute__(self, "__dict__")["_proxied_doc"] = doc
            return doc
        return object.__getattribute__(self, name)

    def _build_proxied_doc(self) -> str | None:
        """The underlying ``Country(...).<table>`` docstring, prefixed with the
        cross-country contract.  Falls back to the class docstring on any error."""
        try:
            from . import Country
            ctries = self.countries
            if not ctries:
                return type(self).__doc__
            base = getattr(Country(ctries[0]), self.table_name).__doc__ or ""
            return (
                f"Cross-country Feature for {self.table_name!r} -- mirrors "
                f"``Country(...).{self.table_name}`` with *waves* -> *countries* "
                f"(prepends a ``country`` index level; cross-country defaults "
                f"differ, e.g. ``currency='index'``).\n\n{base}"
            )
        except Exception:
            return type(self).__doc__

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
                 currency: str | None = 'index', numeraire: str | None = None,
                 **kwargs: Any) -> pd.DataFrame:
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
        **kwargs
            Any other per-feature option the underlying
            ``Country(...).<table>`` method accepts (e.g. ``market``,
            ``labels``, ``units``, ``age_cuts``) is forwarded to each country
            (GH #508 -- ``Feature`` mirrors the ``Country`` method interface,
            swapping *waves* -> *countries*).  ``market`` adds an ``m`` index
            level across countries.  A kwarg a country's method does not accept
            is ignored with a warning.

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
        # Reject an unknown / mistyped table name with a helpful error instead of
        # silently returning an empty DataFrame (e.g. Feature('food_expenditure')).
        if not self.countries:
            import difflib
            sugg = difflib.get_close_matches(
                self.table_name, sorted(_all_known_features()), n=1
            )
            hint = f" Did you mean {sugg[0]!r}?" if sugg else ""
            raise ValueError(
                f"Unknown feature {self.table_name!r}: no country declares it and "
                f"it is not a runtime-derived table.{hint}"
            )
        targets = countries if countries is not None else self.countries
        frames: list[pd.DataFrame] = []
        canonical_levels = _canonical_index_levels(self.table_name)
        fabricate_missing = _fabricates_missing_levels(self.table_name)  # #506

        # numeraire supersedes currency; both are no-ops for non-monetary tables.
        # Either way the output carries a `currency` index level (relabelled to
        # the basis token for numeraire), so widen the canonical index to keep it.
        use_numeraire = numeraire if monetary else None
        pass_currency = None if use_numeraire else (currency if monetary else None)
        if (use_numeraire is not None or pass_currency == 'index') and canonical_levels:
            canonical_levels = canonical_levels + [CURRENCY_LEVEL]
        # `market` (forwarded below) is applied by Country._add_market_index,
        # which DROPS the `v` level and inserts an `m` level.  Mirror that in the
        # canonical level list -- drop `v`, add `m` -- so (a) _harmonize_country_frame
        # can still reorder each frame to canonical order (with `v` gone, requiring
        # every *original* canonical level to be present would skip the reorder),
        # and (b) `expected_names` matches the real per-country nlevels, letting the
        # GH#326 set_names restoration fire instead of leaving a trailing unnamed
        # level (issue #511: food_prices(market='Region') -> [..., 's', None]).
        if kwargs.get("market") is not None and canonical_levels:
            canonical_levels = [lvl for lvl in canonical_levels if lvl != "v"]
            if "m" not in canonical_levels:
                canonical_levels = canonical_levels + ["m"]

        # Countries dropped because they cannot honour a labels=X request (no
        # curated column).  Accumulated and reported ONCE after the loop, with a
        # df.attrs marker -- distinct from genuine per-country build failures.
        labels_unavailable: list[str] = []

        for name in targets:
            try:
                c = Country(name, trust_cache=effective_trust_cache)
                method = getattr(c, self.table_name)
                # Build the per-country call: currency/numeraire with the monetary
                # gating above, plus any extra per-feature kwargs (market, labels,
                # units, age_cuts, ...) this country's generated method actually
                # accepts -- mirroring the Country(...).<table> interface (GH #508).
                call_kwargs: dict[str, Any] = {}
                if use_numeraire is not None:
                    call_kwargs["numeraire"] = use_numeraire
                elif pass_currency is not None:
                    call_kwargs["currency"] = pass_currency
                accepted = _method_parameters(method)
                for key, val in kwargs.items():
                    if key in accepted:
                        call_kwargs[key] = val
                    elif val is not None:
                        warnings.warn(
                            f"{self.table_name}: {name}'s method does not accept "
                            f"{key!r}; ignored for this country"
                        )
                df = method(**call_kwargs)
                if not isinstance(df, pd.DataFrame) or df.empty:
                    warnings.warn(
                        f"No data for {self.table_name} in {name}"
                    )
                    continue
                # Coerce toward the canonical shape so one country's stray
                # column / extra index level can't collapse the whole
                # concatenated index to object tuples (GH #325).
                df = _harmonize_country_frame(
                    df, canonical_levels, name, self.table_name, fabricate_missing
                )
                # Prepend country as an index level
                df = pd.concat({name: df}, names=["country"])
                frames.append(df)
            except LabelUnavailableError:
                # Country curates no such label column -> degrade, don't conflate
                # with a build failure.  Reported once after the loop (Contract B).
                labels_unavailable.append(name)
                continue
            except Exception as e:  # broad catch intentional: surface per-country failures as warnings
                # Cross-country aggregation must not crash on one country's
                # implementation error; the warning carries the specific type.
                warnings.warn(
                    f"Failed to load {self.table_name} for {name}: {type(e).__name__}: {e}"
                )

        def _mark_labels_unavailable(result: pd.DataFrame, n_kept: int) -> pd.DataFrame:
            """Contract B: one aggregated warning + a df.attrs marker for the
            countries dropped because they curate no requested label column."""
            if not labels_unavailable:
                return result
            warnings.warn(
                f"{self.table_name}: labels={kwargs.get('labels')!r} unavailable "
                f"for {len(labels_unavailable)} country(ies) {labels_unavailable} "
                f"-- they curate no such food-label column and were dropped from "
                f"the assembly (kept {n_kept}). Add the column or pass a country "
                f"subset to silence this."
            )
            result.attrs['labels_unavailable'] = list(labels_unavailable)
            return result

        if not frames:
            return _mark_labels_unavailable(pd.DataFrame(), 0)

        # pandas cannot stack frames whose index DEPTH/NAMES differ into a named
        # MultiIndex -- it falls back to an unnamed object index, collapsing the
        # WHOLE feature (issue #512: EthiopiaRHS's documented (t,i) reduced assets
        # stacked with item-level (t,i,j); also surfaces under labels='Aggregate'
        # when most countries KeyError-drop and a j-less survivor remains).  Keep
        # the modal index shape and exclude the divergent frame(s) with a loud,
        # named warning -- the excluded country stays available via
        # Country(name).<table>().
        if len(frames) > 1:
            from collections import Counter
            shape_of = lambda f: tuple(f.index.names)
            counts = Counter(shape_of(f) for f in frames)
            if len(counts) > 1:
                modal = counts.most_common(1)[0][0]
                kept = [f for f in frames if shape_of(f) == modal]
                dropped = [f for f in frames if shape_of(f) != modal]
                dropped_names = [f.index.get_level_values("country")[0]
                                 for f in dropped if len(f)]
                warnings.warn(
                    f"{self.table_name}: excluded {len(dropped)} country frame(s) "
                    f"{dropped_names} with a divergent index shape from the "
                    f"cross-country assembly (kept modal shape {list(modal)}); "
                    f"stacking heterogeneous index depths would collapse the whole "
                    f"result to an unnamed index. Access the excluded data via "
                    f"Country(name).{self.table_name}()."
                )
                frames = kept

        result = pd.concat(frames)
        n_kept = result.index.get_level_values("country").nunique()

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
        # heterogeneous per-country indices left an unnamed level -- either a full
        # fallback to an unnamed object index (GH #325, names == [None]) or a
        # PARTIAL collapse (e.g. ['country', None, None]) that previously slipped
        # through silently (issue #512, labels='Aggregate' variant).
        if len(frames) > 1 and None in list(result.index.names):
            shapes = {
                f.index.get_level_values(0)[0] if len(f) else "?":
                    list(f.index.names)
                for f in frames
            }
            warnings.warn(
                f"{self.table_name}: cross-country index collapsed to an "
                f"unnamed object index; per-country index names differ: {shapes}"
            )

        return _mark_labels_unavailable(result, n_kept)
