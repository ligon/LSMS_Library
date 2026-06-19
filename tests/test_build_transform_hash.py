"""Red-tests for the build-path transform cache fingerprint (cache step 2).

Guards the two genuinely hard sub-problems behind
``lsms_library/_build_registry.py`` (see
``SkunkWorks/build_transform_cache_hash.org``):

- *resolution* -- the closure walk must not silently drop a referenced
  ``lsms_library`` object via the function-level-import (cycle-dodge) idiom;
- *serialization* -- no referenced constant may leak a ``0x`` identity address
  into the fingerprint (which would make it non-deterministic).

Plus drift (the tagged-entry-point set is pinned), determinism, per-table
scoping, the no-wrapper tag invariant, and that the fingerprint is actually
folded into both hash methods.
"""
import re

import pytest

import lsms_library  # noqa: F401  (ensure package import side effects)
import lsms_library.country  # noqa: F401
import lsms_library.build_transforms  # noqa: F401
from lsms_library import _build_registry as R


EXPECTED_ENTRY_POINTS = {
    "lsms_library.build_transforms.food_acquired_to_canonical",
    "lsms_library.build_transforms._finalize_canonical_food_acquired",
    "lsms_library.build_transforms.fill_v_with_coord_bin",
    "lsms_library.build_transforms.apply_derived",
    "lsms_library.country._normalize_dataframe_index",
    "lsms_library.country.Wave.grab_data",
}


def _all_parts():
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    return parts


def test_entry_point_set_is_pinned():
    """Drift guard: a new/removed build-path tag must be a conscious change."""
    assert R.registered_entry_points() == EXPECTED_ENTRY_POINTS


def test_no_unresolved_build_imports():
    """Resolution red-test: every referenced function-level import resolves.

    This is the GH #514 class -- ``getattr(own_module, name)`` misses the
    ``from .feature import _ADDITIVE_MEASURE_COLUMNS`` cycle-dodge import.  If a
    future edit adds such an import and the AST walk can't resolve it, the
    closure would silently drop a build dependency -> stale cache.  Fail loudly.
    """
    unresolved = []
    for qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        fn = R._unwrap(fn)
        co = set(fn.__code__.co_names)
        for name, obj in R._import_map(fn).items():
            if name in co and obj is None:
                unresolved.append(f"{qn} -> {name}")
    assert not unresolved, f"unresolved build imports (closure would drop them): {unresolved}"


def test_additive_measure_columns_is_in_the_closure():
    """The motivating symbol: reached via a function-level import, its VALUE
    (not just its name) must be folded in, so editing it invalidates."""
    joined = "\x1f".join(_all_parts())
    assert "_ADDITIVE_MEASURE_COLUMNS" in joined
    assert "country._ADDITIVE_MEASURE_COLUMNS=" in joined  # value serialized, not just referenced


def test_no_identity_address_leak():
    """Serialization red-test: a default repr of any referenced object would
    embed ``0x...`` and make the fingerprint non-deterministic."""
    leaks = [p[:120] for p in _all_parts() if re.search(r"0x[0-9a-fA-F]+", p)]
    assert not leaks, f"identity-repr leak in fingerprint parts: {leaks}"


def test_fingerprint_is_deterministic():
    R.build_transforms_fingerprint.cache_clear()
    a = R.build_transforms_fingerprint("food_acquired")
    R.build_transforms_fingerprint.cache_clear()
    b = R.build_transforms_fingerprint("food_acquired")
    assert a == b


def test_per_table_scoping():
    """food_acquired folds in its food-specific tags; household_roster does
    not -- so their fingerprints differ."""
    fa = R.build_transforms_fingerprint("food_acquired")
    hr = R.build_transforms_fingerprint("household_roster")
    assert fa != hr


def test_tags_do_not_wrap():
    """@build_transform must return the function unchanged (no wrapper)."""
    assert lsms_library.build_transforms.apply_derived.__name__ == "apply_derived"
    assert lsms_library.build_transforms.food_acquired_to_canonical.__qualname__ == \
        "food_acquired_to_canonical"
    assert lsms_library.country._normalize_dataframe_index.__code__ is not None


def test_serialiser_rejects_unknown_types():
    """_ser must raise on an un-serialisable type rather than leak an identity
    repr (the else-reject branch)."""
    class Weird:
        pass
    with pytest.raises(TypeError):
        R._ser(Weird(), set(), [])


@pytest.mark.parametrize("country_name", ["Uganda"])
def test_fingerprint_is_wired_into_table_cache_hash(country_name, monkeypatch):
    """The country-level hash must fold in the build-transform fingerprint:
    perturbing the fingerprint (simulating a build-code edit) must change
    _table_cache_hash without any data/config change."""
    c = lsms_library.country.Country(country_name)
    waves = c.waves
    base = c._table_cache_hash("food_acquired", waves)
    if base is None:
        pytest.skip("table not introspectable for this country")
    monkeypatch.setattr(R, "build_transforms_fingerprint", lambda table=None: "PERTURBED")
    # country.py imported the name, so patch there too
    monkeypatch.setattr(lsms_library.country, "build_transforms_fingerprint",
                        lambda table=None: "PERTURBED")
    perturbed = c._table_cache_hash("food_acquired", waves)
    assert perturbed != base, "build-transform fingerprint is NOT folded into _table_cache_hash"
