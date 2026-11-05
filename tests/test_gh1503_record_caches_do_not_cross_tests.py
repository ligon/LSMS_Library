"""GH #1503: a record loader must not carry one test's config tree into the next.

``derivation_records`` and its siblings are ``lru_cache``d by country alone and
read a yml under ``countries_root()``.  A test that repoints
``LSMS_COUNTRIES_ROOT`` used to leave its fake tree's registry cached, so a later
test failed or passed with xdist's scheduling.  The two tests below run in file
order on one worker: the first fills the caches and the second finds them empty.
"""
from __future__ import annotations

from lsms_library.derivations import derivation_records, framework_records
from lsms_library.population import population_records
from lsms_library.recall import recall_records
from lsms_library.sampling import sampling_records

LOADERS = (derivation_records, population_records, recall_records, sampling_records)


def test_a_loader_fills_its_cache():
    for load in LOADERS:
        load("Niger")
        assert load.cache_info().currsize == 1, load.__name__
    framework_records()
    assert framework_records.cache_info().currsize == 1


def test_the_next_test_starts_with_it_empty():
    for load in (*LOADERS, framework_records):
        assert load.cache_info().currsize == 0, load.__name__


def test_a_repointed_tree_is_not_served_after_the_repoint(tmp_path, monkeypatch):
    """The case that bit: a fake tree's empty registry, then the real one."""
    real = derivation_records("Niger")
    assert real, "Niger ships a derivations.yml"
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(tmp_path))
    from lsms_library.paths import countries_root

    countries_root.cache_clear()
    derivation_records.cache_clear()
    assert derivation_records("Niger") == {}


def test_the_real_registry_is_back():
    assert "Niger::community_prices::one-item-per-price" in derivation_records("Niger")
