"""Top-level catalog helpers: enumerate available countries and features.

Public entry points (re-exported from the package root):

- :func:`countries` — names accepted by :class:`lsms_library.Country`.
- :func:`features`  — table names accepted by :class:`lsms_library.Feature`.

Both are config-based (they reflect what's *declared*, i.e. what the
constructors accept), not build-based: a listed country/feature may still
fail at data-access time if its microdata isn't available.  Each is
optionally filterable by the other axis.
"""
from __future__ import annotations

from functools import lru_cache

from .paths import countries_root
from .yaml_utils import load_yaml


@lru_cache(maxsize=1)
def _country_dirs() -> tuple[str, ...]:
    """Country directory names that carry a ``_/data_scheme.yml``.

    These are exactly the strings accepted by :class:`lsms_library.Country`.
    Uses ``pathlib`` (not shell globbing) so multi-word names like
    ``"South Africa"`` survive intact.
    """
    out: list[str] = []
    for p in sorted(countries_root().iterdir()):
        if not p.is_dir() or p.name.startswith("."):
            continue
        if (p / "_" / "data_scheme.yml").exists():
            out.append(p.name)
    return tuple(out)


def _declared_tables(country: str) -> set[str]:
    """The ``Data Scheme`` table names declared in *country*'s data_scheme.yml."""
    f = countries_root() / country / "_" / "data_scheme.yml"
    if not f.exists():
        return set()
    with open(f, encoding="utf-8") as fh:
        data = load_yaml(fh)
    if not isinstance(data, dict):
        return set()
    scheme = data.get("Data Scheme", {})
    return set(scheme) if isinstance(scheme, dict) else set()


@lru_cache(maxsize=1)
def _all_features() -> tuple[str, ...]:
    """Every table name accepted by :class:`lsms_library.Feature`.

    Union of all countries' declared tables, plus auto-derived tables
    (e.g. ``household_characteristics``), minus the dict-valued properties
    (``panel_ids`` / ``updated_ids``) that are not ``Feature``-able.
    """
    # Lazy imports avoid an import cycle (country/feature import the package).
    from .country import JSON_CACHE_METHODS
    from .feature import _DERIVED_SOURCE

    tables: set[str] = set()
    for c in _country_dirs():
        tables |= _declared_tables(c)
    tables |= set(_DERIVED_SOURCE)        # derived features (household_characteristics, food_*)
    tables -= set(JSON_CACHE_METHODS)     # panel_ids / updated_ids: not Feature-able
    return tuple(sorted(tables))


def countries(feature: str | None = None) -> list[str]:
    """List country names accepted by :class:`lsms_library.Country`.

    Parameters
    ----------
    feature : str, optional
        Restrict to countries that provide this feature — including
        features auto-derived from a declared source table (e.g.
        ``feature='food_quantities'`` finds countries with ``food_acquired``).

    Returns
    -------
    list[str]
        Sorted country names.

    Raises
    ------
    ValueError
        If *feature* is not a known feature (see :func:`features`).

    Examples
    --------
    >>> import lsms_library as ll
    >>> ll.countries()                       # doctest: +SKIP
    ['Albania', 'Benin', ..., 'Uganda']
    >>> ll.countries(feature='food_acquired')  # doctest: +SKIP
    ['Benin', 'Burkina_Faso', ..., 'Uganda']
    """
    if feature is None:
        return list(_country_dirs())
    valid = _all_features()
    if feature not in valid:
        raise ValueError(
            f"Unknown feature {feature!r}; choose from {list(valid)}"
        )
    from .feature import _discover_countries_for_table
    return sorted(_discover_countries_for_table(feature))


def features(country: str | None = None) -> list[str]:
    """List feature/table names accepted by :class:`lsms_library.Feature`.

    Parameters
    ----------
    country : str, optional
        Restrict to features available for this country — its declared
        tables plus any auto-derived from them.

    Returns
    -------
    list[str]
        Sorted feature names.

    Raises
    ------
    ValueError
        If *country* is not a known country (see :func:`countries`).

    Examples
    --------
    >>> import lsms_library as ll
    >>> ll.features()                  # doctest: +SKIP
    ['assets', 'cluster_features', ..., 'subjective_well_being']
    >>> ll.features(country='Uganda')  # doctest: +SKIP
    ['assets', 'cluster_features', ..., 'shocks']
    """
    if country is None:
        return list(_all_features())
    valid = _country_dirs()
    if country not in valid:
        raise ValueError(
            f"Unknown country {country!r}; choose from {list(valid)}"
        )
    from .country import Country, JSON_CACHE_METHODS
    scheme = Country(country, preload_panel_ids=False).data_scheme
    return sorted(set(scheme) - set(JSON_CACHE_METHODS))
