"""Pure GH1454 dependency/mapping controls; invented rows/maps only, no sources."""
import inspect

import pandas as pd
import pytest

from lsms_library import _build_registry as R, country as C, ehcvm


@pytest.fixture(autouse=True)
def clear_fingerprints():
    R.clear_caches()
    yield
    R.clear_caches()


def script(tmp_path, name, imported):
    path = tmp_path / name
    path.write_text(f'from {imported[0]} import {imported[1]}\n')
    return (str(path),)


def mutate_source(monkeypatch, target, before, after):
    original = inspect.getsource
    assert before in original(target)
    monkeypatch.setattr(inspect, 'getsource',
        lambda obj: original(obj).replace(before, after)
        if inspect.unwrap(obj) is target else original(obj))
    R.clear_caches()


def test_real_ehcvm_entry_closure_reaches_mapper_and_own_helpers():
    seen = set()
    R._closure_parts(ehcvm.food_acquired_ehcvm, seen)
    assert 'lsms_library.country._apply_build_categorical_mappings' in seen
    assert 'lsms_library.country.Country._apply_categorical_mappings' in seen
    assert 'lsms_library.country._augment_numeric_code_keys' in seen
    assert 'lsms_library.country._categorical_key_column' in seen
    assert 'lsms_library.country.Country._finalize_result' not in seen


def test_semantic_mapper_edit_versions_ehcvm_import_not_unrelated_builds(monkeypatch, tmp_path):
    ehcvm_path = script(tmp_path, 'sized.py', ('lsms_library.ehcvm', 'food_acquired_ehcvm'))
    other_path = script(tmp_path, 'other.py', ('lsms_library.build_transforms', 'add_visit_level'))
    before = (R.framework_imports_fingerprint(ehcvm_path), R.framework_imports_fingerprint(other_path),
              R.build_transforms_fingerprint('food_acquired'), R.build_transforms_fingerprint('sample'))
    mutate_source(monkeypatch, C.Country._apply_categorical_mappings,
                  'attrs = dict(getattr(df, "attrs", {}) or {})', 'attrs = {}')
    after = (R.framework_imports_fingerprint(ehcvm_path), R.framework_imports_fingerprint(other_path),
             R.build_transforms_fingerprint('food_acquired'), R.build_transforms_fingerprint('sample'))
    assert before[0] != after[0]
    assert before[1:] == after[1:]


def test_post_read_finalizer_edit_is_not_folded_into_ehcvm_import(monkeypatch, tmp_path):
    paths = script(tmp_path, 'sized.py', ('lsms_library.ehcvm', 'food_acquired_ehcvm'))
    before = R.framework_imports_fingerprint(paths)
    real = inspect.getsource
    monkeypatch.setattr(inspect, 'getsource', lambda obj: real(obj) + '\n_semantic_probe = 1\n'
        if inspect.unwrap(obj) is C.Country._finalize_result else real(obj))
    R.clear_caches()
    assert R.framework_imports_fingerprint(paths) == before


def test_delegation_preserves_existing_mapper_payload_and_call(monkeypatch):
    calls = []
    maps = {'u': pd.DataFrame({'Code': ['tin', 'kg'], 'Preferred Label': ['Tin', 'Kg']})}
    class InventedCountry:
        name = 'Invented'
        categorical_mapping = maps
        _apply_categorical_mappings = C.Country._apply_categorical_mappings
        def __init__(self, name):
            assert name == 'Invented'
            calls.append(name)
    monkeypatch.setattr(C, 'Country', InventedCountry)
    frame = pd.DataFrame({'Quantity': pd.array([2., None, 3.], dtype='Float64')},
        index=pd.MultiIndex.from_tuples([('tin', 3), ('kg', 1), ('other', 2)], names=['u', '_row']))
    frame.attrs = {'id_converted': True, 'detail': {'status': 'invented'}}
    old_input = frame.copy(deep=True)
    expected = InventedCountry('Invented')._apply_categorical_mappings(frame.copy(deep=True))
    actual = C._apply_build_categorical_mappings('Invented', frame.copy(deep=True))
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert actual.attrs == expected.attrs == old_input.attrs
    pd.testing.assert_frame_equal(frame, old_input, check_exact=True)
    assert calls == ['Invented', 'Invented']


def test_served_labels_retains_probe_and_empty_fastpath(monkeypatch):
    calls = []
    def mapper(country, frame):
        assert country == 'Invented'
        assert frame.shape == (3, 0) and frame.index.names == ['u', '_row']
        assert frame.index.get_level_values('_row').tolist() == [0, 1, 2]
        calls.append(frame.copy(deep=True))
        return frame.rename(index={'tin': 'Tin'}, level='u')
    monkeypatch.setattr(C, '_apply_build_categorical_mappings', mapper)
    values = ['tin', 'kg', 'unlisted']
    out = ehcvm.served_unit_labels('Invented', iter(values))
    assert out.dtype == object and out.tolist() == ['Tin', 'kg', 'unlisted']
    assert values == ['tin', 'kg', 'unlisted']
    empty = ehcvm.served_unit_labels('Invented', [])
    assert empty.dtype == object and empty.shape == (0,) and len(calls) == 1


def test_sized_build_payload_and_bare_rows_are_preserved(monkeypatch):
    maps = {'u': pd.DataFrame({'Code': ['tin', 'kg'], 'Preferred Label': ['Tin', 'Kg']})}
    class InventedCountry:
        name = 'Invented'
        categorical_mapping = maps
        _apply_categorical_mappings = C.Country._apply_categorical_mappings
        def __init__(self, name):
            assert name == 'Invented'
    monkeypatch.setattr(C, 'Country', InventedCountry)
    frame = pd.DataFrame({'Quantity': pd.array([2., 3., None, 4.], dtype='Float64')},
        index=pd.MultiIndex.from_tuples([('tin', 3), ('kg', 1), ('unlisted', 2), (None, 4)], names=['u', '_row']))
    frame.attrs = {'id_converted': True}
    original = frame.copy(deep=True)
    out = ehcvm.carry_unit_size(frame, ['Petit', 'Grand', None, 'Petit'], 'Invented')
    expected = original.rename(index={'tin': 'Tin (Petit)'}, level='u')
    pd.testing.assert_frame_equal(out, expected, check_exact=True)
    pd.testing.assert_frame_equal(frame, original, check_exact=True)
    assert out.attrs == original.attrs
