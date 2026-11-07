"""Recovered #1275 prerequisite; no food or weight policy lives here.

The opt-in protects an earlier-issued split ID while leaving the unguarded
path and every pre-guard wave identical (ledger malawi-food-followups §4).
"""
import json
import random
from types import SimpleNamespace

import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.local_tools import (
    panel_ids, panel_ids_guarding, update_id, update_id_reserving,
)


def _frame():
    # A split descendant disappears; the surviving line splits again later.
    rows = [('w1', 'a', 'A'), ('w1', 'b', 'A'), ('w2', 'p', 'a'),
            ('w3', 'q', 'p'), ('w3', 'r', 'p')]
    return pd.DataFrame({'previous_i': [r[2] for r in rows]},
                        index=pd.MultiIndex.from_tuples(
                            [r[:2] for r in rows], names=['t', 'i']))


def _country(resources, payload):
    calls = []

    def aggregate(waves, table):
        calls.append(table)
        return payload[table]

    country = SimpleNamespace(resources=resources, data_scheme={'panel_ids': {}},
                              _aggregate_wave_data=aggregate)
    country._panel_id_guard_from = lambda: Country._panel_id_guard_from(country)
    return country, calls


def test_empty_reservations_preserve_legacy_values_and_split_counters():
    rng = random.Random(1275)
    for _ in range(200):
        targets = [f'T{rng.randrange(6)}' + ('_1' if rng.random() < .2 else '')
                   for _ in range(rng.randrange(1, 12))]
        links = {f'k{j}': target for j, target in enumerate(targets)}
        splits = {f'T{j}': rng.randrange(3) for j in range(6) if rng.random() < .5}
        assert update_id(dict(links), dict(splits)) == update_id_reserving(
            dict(links), dict(splits), set())


def test_reserved_former_lineage_is_not_reissued():
    links = {'x': 'A', 'y': 'A'}
    assert update_id(dict(links), {'A': 0})[0]['y'] == 'A_1'
    assert update_id_reserving(dict(links), {'A': 0}, {'A_1'})[0]['y'] == 'A_2'


def test_guard_retains_earlier_maps_and_raw_links():
    frame = _frame()
    raw, plain = panel_ids(frame)
    guarded_raw, guarded = panel_ids_guarding(frame, guard_from='w3')
    assert raw == guarded_raw
    assert guarded['w1'] == plain['w1']
    assert guarded['w2'] == plain['w2']
    assert plain['w3']['r'] == 'A_1'
    assert guarded['w3']['r'] == 'A_2'
    assert not {'A_2'} & (set(guarded['w1'].values()) | set(guarded['w2'].values()))


def test_future_guard_is_identical_to_legacy_panel_builder():
    assert panel_ids_guarding(_frame(), guard_from='z') == panel_ids(_frame())


@pytest.mark.parametrize('resources', [None, {}, {'Panel IDs': None},
                                     {'Panel IDs': {}}, {'Panel IDs': {'other': 1}}])
def test_country_without_opt_in_keeps_legacy_mapping(resources):
    frame = _frame()
    country, calls = _country(resources, {'panel_ids': frame})
    Country._compute_panel_ids(country)
    expected_raw, expected_updated = panel_ids(frame)
    assert country._panel_ids_cache == expected_raw
    assert country._updated_ids_cache == expected_updated
    assert calls == ['panel_ids']
    assert country._panel_ids_attempted


def test_country_opt_in_uses_the_same_guard_as_the_committed_map_builder():
    frame = _frame()
    country, _ = _country({'Panel IDs': {'guard_minted_from': 'w3'}},
                          {'panel_ids': frame})
    Country._compute_panel_ids(country)
    expected_raw, expected_updated = panel_ids_guarding(frame, guard_from='w3')
    assert country._panel_ids_cache == expected_raw
    assert country._updated_ids_cache == expected_updated


def test_committed_map_path_does_not_recompute_even_when_opted_in():
    raw, updated = panel_ids_guarding(_frame(), guard_from='w3')
    # JSON loading yields a plain dict; the computational helper returns a
    # RecursiveDict wrapper, which is not the committed-file route.
    raw = dict(raw)
    country, calls = _country({'Panel IDs': {'guard_minted_from': 'w3'}},
                              {'panel_ids': raw, 'updated_ids': updated})
    Country._compute_panel_ids(country)
    assert country._panel_ids_cache is raw
    assert country._updated_ids_cache is updated
    assert calls == ['panel_ids', 'updated_ids']


@pytest.mark.parametrize('error', [
    json.JSONDecodeError('broken', '{', 0),
    UnicodeDecodeError('utf-8', b'\xff', 0, 1, 'invalid byte'),
    RuntimeError('unexpected source failure'),
])
def test_guard_keeps_current_failure_cleanup_and_successful_retry(error):
    """The opt-in lives inside #1590's unchanged retry/raise boundary."""
    frame = _frame()
    country, _ = _country({'Panel IDs': {'guard_minted_from': 'w3'}},
                          {'panel_ids': frame})
    country.name = 'Malawi'
    country._panel_ids_attempted = False
    country._panel_ids_cache = None
    country._updated_ids_cache = None
    calls = []

    def aggregate(waves, table):
        calls.append(table)
        if len(calls) == 1:
            raise error
        return frame

    country._aggregate_wave_data = aggregate
    country._compute_panel_ids = lambda: Country._compute_panel_ids(country)
    with pytest.raises(type(error)) as caught:
        Country.panel_ids.fget(country)
    assert caught.value is error
    assert country._panel_ids_attempted is False
    assert country._panel_ids_cache is None
    assert country._updated_ids_cache is None

    updated = Country.updated_ids.fget(country)
    expected_raw, expected_updated = panel_ids_guarding(frame, guard_from='w3')
    assert updated == expected_updated
    assert country._panel_ids_cache == expected_raw
    assert country._panel_ids_attempted is True
    assert calls == ['panel_ids', 'panel_ids']
    assert updated['w3']['r'] == 'A_2'
