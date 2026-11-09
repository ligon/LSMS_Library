"""Configured wiring absence is distinct from unavailable upstream data.

Reuse the ordinary GH1590 private Country/Wave fixture; no country payloads.
The existing GH1039/1281/1512 companions cover warm hits and real failures.
"""
import logging
from types import SimpleNamespace
import warnings

import pandas as pd
import pytest

from lsms_library import country as C
from test_gh1590_wave_failure_manifest import (
    survey, NAME, GOOD, BAD, EMPTY, ABSENT, WAVES,
)


ORIGINAL_RESOLVE = C.Country._resolve_materialize_stages


class OriginalRoute(RuntimeError):
    """A test stop at the unchanged post-diagnostic build boundary."""


def _unwire(survey):
    for wave in WAVES:
        (survey.country_path / wave / '_/data_info.yml').write_text('{}\n')


def _stop_original_work(monkeypatch, error):
    def stop(*args, **kwargs):
        raise error
    monkeypatch.setattr(C.Country, '_evict_hashless_wave_caches', stop)


@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_full_unwired_request_has_one_precise_terminal_error(
        survey, monkeypatch, caplog, backend):
    _unwire(survey)
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    _stop_original_work(monkeypatch, AssertionError('impossible work began'))
    with caplog.at_level(logging.WARNING), warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        with pytest.raises(RuntimeError) as caught:
            survey.load()
    assert type(caught.value) is RuntimeError
    assert str(caught.value) == (
        f'Could not materialize {NAME}/facts: no configured wave source or country '
        f'builder for requested waves {WAVES}. This describes current table '
        'wiring, not upstream data availability.')
    assert seen == []
    assert not any('still missing after running fallbacks' in r.message
                   for r in caplog.records)
    assert survey.reads == [] and survey.getters == []
    assert not survey.cache.exists()


@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_unwired_strict_subset_keeps_declared_empty_and_no_work(
        survey, monkeypatch, backend):
    _unwire(survey)
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    _stop_original_work(monkeypatch, AssertionError('empty subset did work'))
    country = survey.country()
    entry = country._materialization_entry('facts')
    expected = country._finalize_result(C._empty_declared_frame(entry), entry, 'facts')
    got = survey.load([BAD], country=country)
    pd.testing.assert_frame_equal(got, expected)
    assert not survey.cache.exists()
    assert survey.reads == [] and survey.getters == []


@pytest.mark.parametrize('stage_kind', ['partial', 'complete', 'country'])
def test_any_singleton_stage_prevents_false_unwired_notice(
        survey, monkeypatch, stage_kind):
    _unwire(survey)
    # Exercise the real existing resolver, including its partial-map behavior.
    stage = SimpleNamespace(stage_ref='invented:materialize@facts')
    if stage_kind == 'partial':
        stages = {(NAME, BAD, 'facts'): stage}
    elif stage_kind == 'complete':
        stages = {(NAME, w, 'facts'): stage for w in WAVES}
    else:
        stages = {(NAME, None, 'facts'): stage}
    monkeypatch.setattr(C, '_load_materialize_stage_map', lambda root: stages)
    monkeypatch.setattr(C.Country, '_resolve_materialize_stages', ORIGINAL_RESOLVE)
    country = survey.country()
    if stage_kind == 'partial':
        assert country._resolve_materialize_stages('facts', WAVES) == []
        assert country._resolve_materialize_stages('facts', [BAD]) == [stage]
    # Keep actual stage execution outside this narrow boundary test. Existing
    # GH1590 stage controls verify production success/failure/coverage routes.
    survey.no_repo()
    error = OriginalRoute('existing fallback path retained')
    _stop_original_work(monkeypatch, error)
    with pytest.raises(OriginalRoute) as caught:
        survey.load(country=country)
    assert caught.value is error
    assert not survey.cache.exists()


def test_stage_inspection_error_is_not_wiring_absence(survey, monkeypatch):
    _unwire(survey)
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'make')
    def bad_stage(*args, **kwargs):
        raise ValueError('malformed configured stage map')
    monkeypatch.setattr(C.Country, '_resolve_materialize_stages', bad_stage)
    error = OriginalRoute('inspection uncertainty retained original path')
    _stop_original_work(monkeypatch, error)
    with pytest.raises(OriginalRoute) as caught:
        survey.load()
    assert caught.value is error
    assert not survey.cache.exists()


@pytest.mark.parametrize('inspection', ['present', 'uncertain'])
def test_country_builder_or_inspection_uncertainty_keeps_original_route(
        survey, monkeypatch, inspection):
    _unwire(survey)
    error = OriginalRoute('country build route retained')
    _stop_original_work(monkeypatch, error)
    if inspection == 'present':
        monkeypatch.setattr(C.Country, '_has_country_builder', lambda *a: True)
    else:
        unknown = ValueError('cannot inspect country rule')
        def unknown_builder(*args, **kwargs):
            raise unknown
        monkeypatch.setattr(C.Country, '_has_country_builder', unknown_builder)
    with pytest.raises(OriginalRoute) as caught:
        survey.load()
    assert caught.value is error
    assert not survey.cache.exists()


def test_empty_yaml_declaration_is_still_a_configured_producer(survey, monkeypatch):
    _unwire(survey)
    (survey.country_path / GOOD / '_/data_info.yml').write_text('facts: {}\n')
    error = OriginalRoute('declared source route retained')
    _stop_original_work(monkeypatch, error)
    with pytest.raises(OriginalRoute) as caught:
        survey.load()
    assert caught.value is error


def test_private_wave_input_preparation_bypasses_public_diagnostic(survey, monkeypatch):
    _unwire(survey)
    def no_proof(*args, **kwargs):
        raise AssertionError('public diagnostic entered private preparation')
    monkeypatch.setattr(C.Country, '_legacy_can_build', no_proof)
    country = survey.country()
    assert country._aggregate_wave_data(
        None, 'facts', _make_wave_inputs_only=set()) is None
    assert survey.reads == [] and survey.getters == []
    assert not survey.cache.exists()


def test_empty_wave_selection_retains_original_route(survey, monkeypatch):
    _unwire(survey)
    error = OriginalRoute('empty selection retains original path')
    _stop_original_work(monkeypatch, error)
    with pytest.raises(OriginalRoute) as caught:
        survey.load([])
    assert caught.value is error
    assert not survey.cache.exists()
