"""Runtime-derived food tables are discovered through their source (GH #218)."""

from lsms_library import Country, Feature, countries
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


def test_runtime_derived_tables_are_not_raw_registrations():
    """A null registration is still a raw declaration, not derivation metadata."""
    derived = set(Country._FOOD_DERIVED) | set(Country._ROSTER_DERIVED)
    offenders = []
    for name in countries():
        path = countries_root() / name / "_" / "data_scheme.yml"
        if not path.exists():
            continue
        scheme = load_yaml(path).get("Data Scheme", {})
        offenders.extend((name, table) for table in derived if table in scheme)
    assert not offenders, f"Runtime-derived tables registered as raw: {offenders}"


def test_malawi_food_derivatives_remain_public_without_registrations():
    country = Country("Malawi", preload_panel_ids=False)
    derived = list(Country._FOOD_DERIVED)
    assert "food_acquired" in country.features
    assert country.features == country.data_scheme
    assert [table for table in country.features if table in derived] == derived
    for table in derived:
        assert callable(getattr(country, table))
        assert country._materialization_entry(table) == {}
        assert "Malawi" in Feature(table).countries
