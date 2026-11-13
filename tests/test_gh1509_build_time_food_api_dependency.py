"""A country script that reads the Country food API at build time imports the
transform behind it (GH #1509; ruling @ligon 2026-11-12, "Make the dependency
visible").

``framework_imports_fingerprint`` folds into a country's L2-country and L2-wave
hashes the closure of every ``from lsms_library.X import name`` its ``_/*.py``
files make.  A call through the Country API -- ``ll.Country('Uganda')
.food_quantities()`` -- is invisible to it: ``Country`` is a class, and the
closure walk folds functions.  So an edit to the kilogram ladder
(``food_kg_factors``) moved Uganda's served ``nutrition`` and no Uganda hash,
and a warm ``var/nutrition.parquet`` kept serving the old numbers (red team on
#1509, F3).  The ruling: such a script imports the transform explicitly, so a
kg-factor edit re-warms that country.

What these tests pin, data-free:

- every country or wave ``_/*.py`` that calls ``.food_quantities(``,
  ``.food_prices(`` or ``.food_expenditures(`` imports the matching
  ``*_from_acquired`` from ``lsms_library.transformations`` (a static sweep, so
  a new script that reads the API without the import goes red here);
- a no-op edit to ``food_kg_factors`` moves the framework fingerprint of
  exactly the countries that read it -- GhanaLSS (whose ``nutrition.py`` calls
  the transform directly), Uganda and Ethiopia -- and no
  ``build_transforms_fingerprint``.

A ``getattr(country, name)()`` dispatch is not visible to the sweep; none
reads a food table today (Uganda ``income.py`` reads ``earnings`` and
``enterprise_income``).
"""
from __future__ import annotations

import ast
import inspect

import pytest

import lsms_library.country  # noqa: F401  (package import side effects)
from lsms_library import _build_registry as R
from lsms_library import transformations as T
from lsms_library.paths import countries_root

#: The Country API's derived food tables and the transform behind each
#: (``Country._FOOD_DERIVED``).
DERIVED = dict(lsms_library.country.Country._FOOD_DERIVED)

#: The countries whose framework fingerprint folds the kilogram ladder: the
#: three whose ``nutrition.py`` reads it (GhanaLSS through a direct call of
#: ``food_quantities_from_acquired``, Uganda and Ethiopia through the API).
READS_THE_KG_LADDER = ['Ethiopia', 'GhanaLSS', 'Uganda']


@pytest.fixture(autouse=True)
def _clear_fingerprints():
    R.clear_caches()
    yield
    R.clear_caches()


def _country_scripts():
    root = countries_root()
    return sorted(root.glob('*/_/*.py')) + sorted(root.glob('*/*/_/*.py'))


def _api_calls_and_imports(text):
    tree = ast.parse(text)
    called, imported = set(), set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in DERIVED):
            called.add(node.func.attr)
        elif (isinstance(node, ast.ImportFrom)
              and node.module == 'lsms_library.transformations'):
            imported |= {a.name for a in node.names}
    return called, imported


def test_the_derived_table_map_names_real_transforms():
    assert DERIVED == {
        'food_expenditures': 'food_expenditures_from_acquired',
        'food_prices': 'food_prices_from_acquired',
        'food_quantities': 'food_quantities_from_acquired',
    }
    for fn in DERIVED.values():
        assert callable(getattr(T, fn))


def test_every_build_time_food_api_call_imports_its_transform():
    root = countries_root()
    callers, missing, unparsed = {}, {}, []
    for path in _country_scripts():
        text = path.read_text(encoding='utf-8', errors='replace')
        try:
            called, imported = _api_calls_and_imports(text)
        except SyntaxError:
            # An unparsable script cannot hide a call: fail if it names one.
            if any(f'.{name}(' in text for name in DERIVED):
                unparsed.append(str(path.relative_to(root)))
            continue
        if not called:
            continue
        rel = str(path.relative_to(root))
        callers[rel] = sorted(called)
        lack = sorted({DERIVED[c] for c in called} - imported)
        if lack:
            missing[rel] = lack
    assert not unparsed, unparsed
    assert not missing, (
        f'{missing}: these scripts read the Country food API at build time '
        f'without importing the transform behind it, so a kg-factor edit '
        f'moves their output and no cache hash (GH #1509, "Make the '
        f'dependency visible").  Add `from lsms_library.transformations '
        f'import <name>`.')
    # Positive control: the sweep finds the two callers the ruling was made
    # against.
    assert {'Ethiopia/_/nutrition.py', 'Uganda/_/nutrition.py'} <= set(callers)


def test_the_import_folds_food_kg_factors():
    seen = set()
    R._closure_parts(T.food_quantities_from_acquired, seen)
    assert 'lsms_library.transformations.food_kg_factors' in seen


def _fingerprints(names):
    root = countries_root()
    return {n: R.framework_imports_fingerprint(
        tuple(sorted(str(p) for p in (root / n / '_').glob('*.py'))))
        for n in names}


def test_a_kg_factor_edit_moves_exactly_the_countries_that_read_it(monkeypatch):
    """The no-op probe: one new statement in ``food_kg_factors``.

    A comment or docstring would move nothing (``_build_registry._norm`` strips
    both), so the probe is a statement.  It moves the framework fingerprint of
    GhanaLSS, Uganda and Ethiopia and of no other country, and no
    ``build_transforms_fingerprint``: the derived food tables are still read
    path (``test_build_transform_hash.py::
    test_no_transformations_symbol_reaches_any_fingerprint``).
    """
    names = sorted(p.parent.parent.name
                   for p in countries_root().glob('*/_/data_scheme.yml'))
    before = _fingerprints(names)
    btf_before = R.build_transforms_fingerprint(None)
    target = inspect.unwrap(T.food_kg_factors)
    real = inspect.getsource
    monkeypatch.setattr(
        inspect, 'getsource',
        lambda obj: (real(obj) + '\n_gh1509_probe = 1\n'
                     if inspect.unwrap(obj) is target else real(obj)))
    R.clear_caches()
    after = _fingerprints(names)
    moved = sorted(n for n in names if before[n] != after[n])
    assert moved == READS_THE_KG_LADDER
    assert R.build_transforms_fingerprint(None) == btf_before
