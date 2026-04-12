"""Tests for the deprecated Country.locality() method.

These tests verify the deprecation machinery introduced in the
2026-04-11 release:

- DeprecationWarning is emitted when locality() is called
- hasattr() / dir() still find the method
- legacy_locality() shim returns the correct DataFrame shape
- test_all_data_schemes skips deprecated tables
"""
import warnings
import pytest


def test_locality_emits_deprecation_warning():
    """Country('Uganda').locality() must emit DeprecationWarning."""
    import lsms_library as ll
    c = ll.Country('Uganda')
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        try:
            c.locality()
        except Exception:
            pass  # The shim may still raise if Uganda data is unavailable;
                  # we only care that the warning fired
        assert any(
            issubclass(warning.category, DeprecationWarning)
            and 'sample()' in str(warning.message)
            for warning in w
        ), f"Expected DeprecationWarning mentioning sample(), got: {[str(x.message) for x in w]}"


def test_locality_hasattr_still_true():
    """hasattr(country, 'locality') must return True despite the deprecation."""
    import lsms_library as ll
    c = ll.Country('Uganda')
    assert hasattr(c, 'locality'), "locality should still be discoverable via hasattr"


def test_locality_in_dir():
    """dir(country) must include 'locality' for IPython discoverability."""
    import lsms_library as ll
    c = ll.Country('Uganda')
    assert 'locality' in dir(c), "locality should appear in dir() listing"


def test_legacy_locality_shape():
    """legacy_locality() must return a DataFrame indexed by (i, t, m) with column v."""
    import lsms_library as ll
    from lsms_library.transformations import legacy_locality
    c = ll.Country('Uganda')
    try:
        loc = legacy_locality(c)
    except Exception as e:
        pytest.skip(f"Uganda data unavailable: {e}")
    assert list(loc.index.names) == ['i', 't', 'm'], \
        f"Expected (i, t, m), got {list(loc.index.names)}"
    assert list(loc.columns) == ['v'], \
        f"Expected ['v'], got {list(loc.columns)}"
    assert len(loc) > 0, "legacy_locality() returned empty DataFrame"


def test_deprecated_skipped_by_test_all_data_schemes():
    """test_all_data_schemes must not try to build deprecated tables."""
    import lsms_library as ll
    c = ll.Country('Uganda')
    # test_all_data_schemes builds all data_scheme entries; locality should be
    # skipped (not attempted) because it is in _DEPRECATED.
    # We verify this by checking _DEPRECATED directly — the key guarantee is
    # that 'locality' is registered in _DEPRECATED so the skip logic fires.
    assert 'locality' in type(c)._DEPRECATED, \
        "locality must be in Country._DEPRECATED"
    # Also verify it is NOT in data_scheme (we removed it from data_scheme.yml)
    assert 'locality' not in c.data_scheme, \
        "locality should have been removed from data_scheme.yml"
