"""
Tests for Wave.__getattr__ recursion guard fix.

This ensures that:
1. External access to wave.data_scheme works (fixes regression from ba8337d6)
2. Legacy loader fallback path works (country.py:987)
3. Infinite recursion is prevented
4. Dynamic method creation from data_scheme works
5. Missing attributes raise AttributeError correctly

Related issues:
- Original recursion bug: ba8337d6
- Regression fix: commit 0cdde86e
"""
from dataclasses import dataclass, field
import os

import pytest

import lsms_library as ll
from lsms_library.country import Wave


@dataclass
class _DummyCountry:
    """Minimal country stub for unit tests."""

    name: str = "Dummy"
    _scheme: list[str] = field(default_factory=lambda: ["foo"])

    @property
    def data_scheme(self):
        return list(self._scheme)

    @property
    def formatting_functions(self):
        return {}

    resources = {}
    wave_folder_map = {}


def test_wave_getattr_returns_data_scheme_via_descriptor(monkeypatch):
    """Calling __getattr__('data_scheme') should return the property value, not raise."""
    dummy_country = _DummyCountry()
    wave = Wave("2020-21", "2020-21", dummy_country)

    # Provide a lightweight scheme to avoid filesystem access
    monkeypatch.setattr(Wave, "data_scheme", property(lambda self: ["foo", "bar"]))
    result = Wave.__getattr__(wave, "data_scheme")

    assert result == ["foo", "bar"]


def test_wave_getattr_dynamic_method_uses_scheme_without_recursion(monkeypatch):
    """Dynamic methods should be built even when schemes are provided via properties."""
    dummy_country = _DummyCountry(_scheme=["foo"])
    wave = Wave("2020-21", "2020-21", dummy_country)

    monkeypatch.setattr(Wave, "data_scheme", property(lambda self: ["foo"]))
    monkeypatch.setattr(Wave, "resources", property(lambda self: {}))
    monkeypatch.setattr(Wave, "grab_data", lambda self, name: f"built:{name}")

    method = getattr(wave, "foo")
    assert callable(method)
    assert method() == "built:foo"


def test_wave_data_scheme_external_access():
    """Test that external access to wave.data_scheme works.

    This was broken by the overly aggressive recursion guard in ba8337d6.
    The guard blocked ALL access to data_scheme, not just recursive access.
    """
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)
    wave_name = country.waves[0]
    wave = country[wave_name]

    # External access should work
    data_scheme = wave.data_scheme

    assert isinstance(data_scheme, list)
    assert len(data_scheme) > 0


def test_wave_data_scheme_multiple_waves():
    """Test data_scheme access across multiple waves."""
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)

    # Should work for all waves
    for wave_name in country.waves[:3]:  # Test first 3
        wave = country[wave_name]
        data_scheme = wave.data_scheme
        assert isinstance(data_scheme, list)


def test_wave_properties_accessible():
    """Test that Wave properties are accessible without triggering recursion."""
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)
    wave = country[country.waves[0]]

    # These should all work without recursion
    _ = wave.file_path
    _ = wave.resources
    _ = wave.data_scheme
    _ = wave.wave_folder


def test_wave_dynamic_method_creation():
    """Test that dynamic methods are created from data_scheme.

    When accessing a method name that's in data_scheme, __getattr__
    should dynamically create a method to handle it.
    """
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)
    wave = country[country.waves[0]]

    # Get a method name from data_scheme
    scheme = wave.data_scheme
    if not scheme:
        pytest.skip("No data_scheme available")

    method_name = scheme[0]

    # This should trigger __getattr__ and create a dynamic method
    method = getattr(wave, method_name)
    assert callable(method)


def test_wave_missing_attribute_raises():
    """Test that accessing missing attributes raises AttributeError.

    The recursion guard should not interfere with proper error handling.
    """
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)
    wave = country[country.waves[0]]

    with pytest.raises(AttributeError, match="has no attribute"):
        _ = wave.nonexistent_method


def test_wave_recursion_flag_cleanup():
    """Test that the recursion guard flag is properly cleaned up.

    The _in_getattr flag should never leak outside of __getattr__.
    """
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)
    wave = country[country.waves[0]]

    # Access data_scheme multiple times
    for _ in range(5):
        _ = wave.data_scheme

    # Flag should not be present in __dict__
    assert '_in_getattr' not in wave.__dict__


@pytest.mark.skipif(
    os.getenv("LSMS_SKIP_AUTH", "").lower() in {"1", "true", "yes"},
    reason="Requires DVC data access (LSMS_SKIP_AUTH is set)",
)
def test_fallback_path_uses_wave_data_scheme():
    """Test that the fallback path at country.py:987 works.

    The load_from_waves function accesses wave_obj.data_scheme, which
    was broken by the aggressive recursion guard.
    """
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)

    # This triggers load_from_waves which accesses wave.data_scheme at line 987
    result = country.food_expenditures()

    assert len(result) > 0
    assert 'i' in result.index.names  # household ID
    assert 'j' in result.index.names  # food item


def test_wave_nested_access_patterns():
    """Test various nested access patterns don't cause issues."""
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)
    wave = country[country.waves[0]]

    # Access data_scheme after other operations
    _ = wave.file_path
    scheme1 = wave.data_scheme

    _ = wave.resources
    scheme2 = wave.data_scheme

    # Should be consistent
    assert scheme1 == scheme2


def test_no_recursion_with_rapid_access():
    """Test that rapid repeated access doesn't cause recursion.

    This verifies the recursion guard works correctly.
    """
    country = ll.Country('Uganda', preload_panel_ids=False, verbose=False)
    wave = country[country.waves[0]]

    # Rapid repeated access should not cause RecursionError
    for _ in range(20):
        _ = wave.data_scheme
