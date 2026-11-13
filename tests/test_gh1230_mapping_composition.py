"""Declared mapping sequences must consume the preceding transformation."""
from types import SimpleNamespace
import pandas as pd
import pytest
from lsms_library import local_tools as lt
from lsms_library.country import Wave

@pytest.fixture
def source(monkeypatch):
    frame = pd.DataFrame({'raw': ['a', 'b', 'a'], 'other': ['x', 'y', 'z']})
    monkeypatch.setattr(lt, 'get_dataframe', lambda *a, **kw: frame.copy(deep=True))
    return frame

def upper(row):
    return row.iloc[0].upper()

def test_declared_table_mapping_follows_registered_formatter(source):
    wave = SimpleNamespace(country=SimpleNamespace(resources={'Data Scheme': {}}),
        formatting_functions={'v': upper},
        categorical_mapping={'codes': pd.DataFrame({'key': ['A'], 'label': ['mapped-a']})})
    plan = Wave.column_mapping(wave, 'sample', {'file': 'invented',
        'idxvars': {'v': ['raw', {'mappings': ['codes', 'key', 'label']}]}, 'myvars': {}})
    result = lt.df_data_grabber('invented', plan['invented']['idxvars'])
    assert result.index.tolist() == ['mapped-a', 'B', 'mapped-a']

@pytest.mark.parametrize('stages,expected', [
    ((upper,), ['A', 'B', 'A']),
    ((upper, {'A': 'mapped-a'}), ['mapped-a', 'B', 'mapped-a']),
    (({'a': 'x'}, {'x': 'mapped-x'}), ['mapped-x', 'b', 'mapped-x']),
    ((upper, {'A': 'x'}, str.lower), ['x', 'b', 'x']),
])
def test_sequence_preserves_order_and_unmatched_values(source, stages, expected):
    # str.lower is a descriptor, so use the documented plain-function shape.
    if stages[-1] is str.lower:
        stages = stages[:-1] + (lambda value: value.lower(),)
    result = lt.df_data_grabber('invented', {'v': (['raw'], stages)})
    assert result.index.tolist() == expected

def test_multicolumn_first_stage_is_applied_once(source):
    seen = []
    def combine(row):
        seen.append(tuple(row))
        return row['raw'] + row['other']
    result = lt.df_data_grabber('invented', {'v': (['raw', 'other'],
        (combine, {'ax': 'mapped'}))})
    assert seen == [('a', 'x'), ('b', 'y'), ('a', 'z')]
    assert result.index.tolist() == ['mapped', 'by', 'az']
