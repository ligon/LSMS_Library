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
import os
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
    "lsms_library.local_tools.df_data_grabber",
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
        co = R._all_co_names(fn.__code__)   # nested scopes too (the #522-class fix)
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


@pytest.mark.parametrize("country_name", ["Uganda"])
def test_fingerprint_is_wired_into_input_hash(country_name, monkeypatch):
    """Wave-level fold (red-team #5): perturbing the fingerprint must change
    Wave._input_hash, else a refactor could drop the wave-level fold and serve
    stale L2-wave parquets with every other test still green."""
    c = lsms_library.country.Country(country_name)
    w = c[c.waves[0]]
    base = w._input_hash("food_acquired")
    if base is None:
        pytest.skip("wave input hash not introspectable")
    monkeypatch.setattr(lsms_library.country, "build_transforms_fingerprint",
                        lambda table=None: "PERTURBED")
    assert w._input_hash("food_acquired") != base, \
        "build-transform fingerprint is NOT folded into Wave._input_hash"


def test_nested_scope_references_are_walked():
    """Red-team #1/#2 regression: a callable referenced ONLY from a nested
    helper (comprehension/lambda/inner def) must appear in _all_co_names.
    Without co_consts recursion, df_data_grabber-class deps vanish -> stale cache."""
    def outer():
        def inner():
            return _SENTINEL_NESTED_NAME()  # referenced only here
        return [inner() for _ in range(_SENTINEL_NESTED_NAME and 1)]
    names = R._all_co_names(outer.__code__)
    assert "_SENTINEL_NESTED_NAME" in names, "nested-scope name not captured by _all_co_names"


def test_df_data_grabber_is_versioned():
    """Red-team #6 positive-coverage: the core extraction transform's SOURCE
    must be a folded closure part (so editing it invalidates)."""
    seen, parts = set(), []
    for _qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    joined = "\x1f".join(parts)
    assert "local_tools.df_data_grabber=" in joined, \
        "df_data_grabber not folded into the closure -> extraction edits would not invalidate"


def test_no_absolute_path_or_dunder_leak():
    """Red-team #3: __file__ resolves to the package's ABSOLUTE path; folding it
    as a constant makes the hash machine/install-dependent.  Assert the package's
    absolute path never appears in any part, and no dunder name=value constant
    leaks.  (AST *source* dumps legitimately contain the literal string
    'lsms_library' -- that is source, not a runtime path, so we match the real
    absolute install path instead.)"""
    seen, parts = set(), []
    for _qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    pkg_abs = os.path.dirname(lsms_library.__file__)        # /.../lsms_library (runtime, machine-specific)
    bad = [p[:120] for p in parts if pkg_abs in p or re.search(r"\.__\w+__=", p)]
    assert not bad, f"absolute-path / dunder constant leak: {bad}"


def test_fingerprint_stable_across_pythonhashseed():
    """Red-team #7: set/dict ordering must not depend on PYTHONHASHSEED.  Compute
    the fingerprint in two subprocesses with different seeds and compare."""
    import os
    import subprocess
    import sys
    code = ("import lsms_library.country, lsms_library.build_transforms, lsms_library.local_tools;"
            "from lsms_library import _build_registry as R;"
            "print(R.build_transforms_fingerprint('food_acquired'))")
    outs = []
    for seed in ("0", "1"):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
        assert r.returncode == 0, r.stderr
        outs.append(r.stdout.strip())
    assert outs[0] == outs[1], f"fingerprint depends on PYTHONHASHSEED: {outs}"
