"""A terminal diagnostic should not make a successful cache stale."""
import ast
import inspect
from pathlib import Path

import pytest

from lsms_library import _build_registry as registry
from lsms_library import country


ERROR_NAME = '_rebuild_failure_error'
ERROR_QN = f'{country.__name__}.{ERROR_NAME}'
SELECTION = '_union_waves'
TABLES = ('housing', 'income', 'sample')


def test_build_failure_constructor_is_only_raised():
    tree = ast.parse(Path(inspect.getsourcefile(country)).read_text())
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    uses = [node for node in ast.walk(tree)
            if isinstance(node, ast.Name) and node.id == ERROR_NAME]
    assert uses
    for use in uses:
        call = parents[use]
        assert isinstance(call, ast.Call) and call.func is use
        scope = parents[call]
        assert isinstance(scope, ast.Raise) and scope.exc is call


def test_terminal_diagnostic_keeps_bare_runtime_error_and_the_requested_path(tmp_path, monkeypatch):
    monkeypatch.setattr(country, 'data_root', lambda name: tmp_path / name)
    result = country._rebuild_failure_error('Nigeria', 'housing')
    assert type(result) is RuntimeError
    assert type(result) in country._GENERIC_BUILD_FAILURES
    assert str(tmp_path / 'Nigeria/var/housing.parquet') in str(result)
    assert 'Could not materialize Nigeria/housing' in str(result)


@pytest.mark.parametrize('table', TABLES)
def test_message_mutation_is_not_a_build_input_but_selection_is(table, monkeypatch):
    calculate = registry.build_transforms_fingerprint
    norm = registry._norm
    helper = country._rebuild_failure_error
    selector = country.Country._union_waves
    mutation = helper

    def changed(fn):
        original = norm(fn)
        return original + ' invented source mutation' if fn is mutation else original

    try:
        calculate.cache_clear()
        before = calculate(table)
        monkeypatch.setattr(registry, '_norm', changed)
        calculate.cache_clear()
        assert calculate(table) == before
        mutation = selector
        calculate.cache_clear()
        assert calculate(table) != before
    finally:
        calculate.cache_clear()


def test_exclusion_does_not_require_changing_the_runtime_helper(monkeypatch):
    assert ERROR_QN in registry._EXCLUDED_CALLABLES
    caller = country.Country._aggregate_wave_data

    def contributes():
        return any(part.startswith(ERROR_QN + '=')
                   for part in registry._closure_parts(caller, set()))

    assert not contributes()
    monkeypatch.setattr(registry, '_EXCLUDED_CALLABLES', registry._EXCLUDED_CALLABLES - {ERROR_QN})
    assert contributes()
