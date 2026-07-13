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
    # GH #323 (conscious additions -- both are build-path, i.e. their output is
    # baked into the L2 parquet, so they must version the cache):
    #   add_visit_level   -- stamps the constant `visit` recall level on the
    #                        single-recall waves of a country whose OTHER wave
    #                        repeats the food-consumption recall (Burkina 2014's
    #                        four quarterly EMC passages).
    #   reduce_to_agreed  -- agree-or-NA collapse for a table whose declared
    #                        grain is coarser than its source (cluster_features
    #                        off a household-level cover page); replaces a silent
    #                        groupby().first() that picked an arbitrary winner.
    "lsms_library.build_transforms.add_visit_level",
    "lsms_library.build_transforms.reduce_to_agreed",
    "lsms_library.country._normalize_dataframe_index",
    "lsms_library.country.Wave.grab_data",
    "lsms_library.country.Country._aggregate_wave_data",
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
    """Serialization red-test: a default repr of any referenced object embeds an
    identity address ('<X object at 0x..>' / '<function .. at 0x..>') and makes
    the fingerprint non-deterministic.  Match the identity-repr signature ' at
    0x' specifically -- a stray '0x' in a source string literal is deterministic
    source, not a runtime leak (and docstrings are stripped in _norm anyway)."""
    leaks = [p[:120] for p in _all_parts() if re.search(r"at 0x[0-9a-fA-F]+", p)]
    assert not leaks, f"identity-repr leak in fingerprint parts: {leaks}"


def test_orchestrator_versioned_but_read_path_is_not():
    """Round-6 fix: the build orchestrator _aggregate_wave_data bakes cross-wave
    alignment+concat (nested safe_concat_dataframe_dict) into the parquet, NOT
    re-applied on read, so it must be versioned.  But _finalize_result and its
    read-path subtree (kinship/spellings), re-applied on every read, must NOT be
    in the closure (else read-path edits over-invalidate the warm cache)."""
    seen, parts = set(), []
    for _qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    joined = "\x1f".join(parts)
    assert any("_aggregate_wave_data=" in p for p in parts), "orchestrator not versioned"
    assert "safe_concat_dataframe_dict" in joined, "nested cross-wave concat not folded"
    leaked = [q for q in seen if q.endswith("._finalize_result") or "_expand_kinship" in q
              or "enforce_canonical_spellings" in q or q.endswith("._join_v_from_sample")]
    assert not leaked, f"read-path transforms leaked into the build closure: {leaked}"


def test_self_method_build_calls_are_versioned():
    """Round-5 fix (GH #522): a build METHOD called on self from a tagged
    transform -- Wave.column_mapping / formatting_functions, which drive
    YAML-route extraction -- is resolved against the module's classes, not just
    module globals.  Their source must be folded; the hash MECHANISM methods
    (_input_hash, _table_cache_hash) must NOT (self-reference)."""
    seen, parts = set(), []
    for _qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    assert any("Wave.column_mapping=" in p for p in parts), "column_mapping not versioned"
    assert any("formatting_functions=" in p for p in parts), "formatting_functions not versioned"
    leaked_mech = [q for q in seen if q.endswith("._input_hash") or q.endswith("._table_cache_hash")
                   or "build_transforms_fingerprint" in q or "framework_imports_fingerprint" in q]
    assert not leaked_mech, f"hash mechanism leaked into the closure (self-reference): {leaked_mech}"


def test_fingerprint_is_deterministic():
    R.clear_caches()
    a = R.build_transforms_fingerprint("food_acquired")
    R.clear_caches()
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
    """Red-team #1/#2 regression -- NON-VACUOUS: a name referenced ONLY inside a
    nested def must be ABSENT from the flat top-level co_names yet PRESENT in the
    recursive _all_co_names.  (round-2 caught the earlier version putting the
    sentinel in a comprehension iterable, which leaks into the enclosing
    co_names and made the test pass even with the co_consts recursion removed.)"""
    def outer():
        def inner():
            return _SENTINEL_NESTED_NAME()   # referenced ONLY in this nested def
        return inner
    flat = set(outer.__code__.co_names)
    assert "_SENTINEL_NESTED_NAME" not in flat, "test vacuous: name leaked into flat co_names"
    assert "_SENTINEL_NESTED_NAME" in R._all_co_names(outer.__code__), \
        "nested-scope name not captured -> co_consts recursion is broken (GH #522 reopens)"


def test_grab_data_body_is_versioned():
    """Round-2 fix: Wave.grab_data's inline content logic (check_adding_t, the
    >1e99 sentinel filter, the dfs merge) and map_index (the j->i swap) must be
    folded into the closure, else editing them serves a stale L2-wave parquet."""
    seen, parts = set(), []
    for _qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    joined = "\x1f".join(parts)
    assert "country.Wave.grab_data=" in joined, "grab_data body not versioned"
    assert any("map_index" in p for p in parts), "map_index (j->i swap) not versioned"


def test_cache_mechanism_is_excluded_but_to_parquet_is_not():
    """Round-2 fix: re-tagging grab_data must NOT drag the pure hash/freshness
    mechanism into the content fingerprint (self-referential over-invalidation)
    -- BUT to_parquet mutates content on write (astype/reset_index/replace), so
    it MUST stay versioned (excluding it would be a new under-invalidation)."""
    seen = set()
    for _qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        R._closure_parts(fn, seen)
    for prim in ("lsms_library.local_tools.cache_freshness",
                 "lsms_library.local_tools.source_fingerprint"):
        assert prim not in seen, f"pure hash primitive {prim} leaked into the content closure"
    assert "lsms_library.local_tools.to_parquet" in seen, \
        "to_parquet mutates content on write and MUST be versioned"


@pytest.mark.parametrize("country,table,helper", [
    # script route, framework helper imported by the country module:
    ("Malawi", "food_acquired", "conversion_table_matching_global"),
    # script route, framework helper reached via a spec_from_file_location LOCAL
    # helper (Nigeria _age_helpers.py -> age_handler) -- round-4 finding:
    ("Nigeria", "household_roster", "age_handler"),
    # YAML route, framework helper reached via a df_edit hook in the country
    # module (Malawi interview_date -> melt_visit_intervals) -- round-4 finding:
    ("Malawi", "interview_date", "melt_visit_intervals"),
])
def test_dynamically_dispatched_framework_helpers_are_versioned(monkeypatch, country, table, helper):
    """GH #522, dynamic-dispatch routes (rounds 3-4): a framework content helper
    reached out-of-process / via a local helper module / via a df_edit hook must
    still invalidate its table's L2 hash when edited.  We fold the lsms_library
    closures of every build module in the wave + country _/ dirs, so perturbing
    the helper's source changes _table_cache_hash."""
    import inspect
    import lsms_library.local_tools as lt
    c = lsms_library.country.Country(country)
    base = c._table_cache_hash(table, c.waves)
    if base is None:
        pytest.skip(f"{country}.{table} not introspectable")
    target = inspect.unwrap(getattr(lt, helper))
    real = inspect.getsource
    monkeypatch.setattr(inspect, "getsource",
                        lambda o: real(o) + "\n_redteam_probe = 1\n" if o is target else real(o))
    R.clear_caches()  # _source/_norm are memoised -> must clear to see the getsource edit
    after = c._table_cache_hash(table, c.waves)
    assert base != after, \
        f"editing framework helper {helper} did NOT invalidate {country}.{table} (#522 reopened)"


@pytest.mark.parametrize("country_name", ["Nigeria", "Uganda"])
def test_local_helper_module_body_is_versioned(country_name, monkeypatch):
    """Round-7 fix (GH #522): a LOCAL helper module loaded by spec_from_file_location
    (e.g. _age_helpers.py: _clean_year / apply_age_handler bake DOB->Age into
    household_roster) must have its OWN body file-hashed.  framework_imports_fingerprint
    folds only its lsms_library-import closure (age_handler), not its own logic; the
    all-_/*.py file-hash closes that.  Perturb the helper's content hash -> table hash changes."""
    c = lsms_library.country.Country(country_name)
    helper = c.file_path / "_" / "_age_helpers.py"
    if not helper.exists():
        pytest.skip(f"no _age_helpers.py for {country_name}")
    base = c._table_cache_hash("household_roster", c.waves)
    if base is None:
        pytest.skip("household_roster not introspectable")
    real = lsms_library.country.cached_file_hash
    monkeypatch.setattr(lsms_library.country, "cached_file_hash",
                        lambda p, _r=real, _h=str(helper): ((_r(p) or "") + "X") if str(p) == _h else _r(p))
    assert c._table_cache_hash("household_roster", c.waves) != base, \
        f"{country_name} _age_helpers.py body not file-hashed -> editing it serves stale cache"


def test_local_data_file_build_input_is_versioned(monkeypatch):
    """Round-8 fix (GH #522): a .csv/.json data table in a _/ dir (Malawi
    ihs3_conversions.csv -> cfactor -> Quantity_kg, baked and not re-applied on
    read) must be content-hashed like the .py helper bodies."""
    c = lsms_library.country.Country("Malawi")
    csv = c.file_path / "2010-11" / "_" / "ihs3_conversions.csv"
    if not csv.exists():
        pytest.skip("no ihs3_conversions.csv")
    base = c._table_cache_hash("food_acquired", c.waves)
    if base is None:
        pytest.skip("food_acquired not introspectable")
    real = lsms_library.country.cached_file_hash
    monkeypatch.setattr(lsms_library.country, "cached_file_hash",
                        lambda p, _r=real, _h=str(csv): ((_r(p) or "") + "X") if str(p) == _h else _r(p))
    assert c._table_cache_hash("food_acquired", c.waves) != base, \
        "ihs3_conversions.csv not content-hashed -> editing it serves stale Quantity_kg"


def test_excluded_callables_are_write_or_hash_only():
    """Guard the _EXCLUDED_CALLABLES list: every excluded callable must be free
    of content-mutating ops, so the exclusion can never become a stale-cache."""
    import inspect
    from lsms_library import local_tools as lt
    mutators = ("astype(", "reset_index(", "set_index(", "rename(", "fillna(",
                "replace(", "dropna(", "groupby(", "merge(", "reindex(")
    for qn in R._EXCLUDED_CALLABLES:
        if ".local_tools." not in qn:
            continue  # country hash methods are excluded for self-reference, not content
        fn = getattr(lt, qn.rsplit(".", 1)[1], None)
        assert fn is not None, f"excluded callable {qn} not found"
        src = inspect.getsource(inspect.unwrap(fn))
        bad = [m for m in mutators if m in src]
        assert not bad, f"excluded {qn} contains content-mutating ops {bad} -> unsafe to exclude"


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
