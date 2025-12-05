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
import pytest
import lsms_library as ll


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
