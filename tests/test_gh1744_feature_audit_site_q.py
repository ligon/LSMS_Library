"""Source-free tests for advisory Site Q findings in the Feature scan."""
import json
import pickle
from types import SimpleNamespace
import warnings

import pandas as pd
import pytest

from bench.feature_audit import cluster, scan
from lsms_library.quantity_audit import QuantityImplausibleWarning, SurveyKgPriceWarning


class _ExtendedPriceWarning(SurveyKgPriceWarning):
    pass


@pytest.mark.parametrize('phase', ['1a-sanity', '1b-assembly', '2-kwargs'])
def test_audit_classifies_and_scopes_site_q_in_each_phase(monkeypatch, phase):
    frame = pd.DataFrame({'Quantity': [1.0]})
    # Conflict with two existing substring rules, and look like a row ID.
    # Site Q capture must use actual type instead of interpreting any of it.
    def build(*args, **kwargs):
        warnings.warn('duplicate i=synthetic-private-key', QuantityImplausibleWarning)
        warnings.warn('does not accept plot=synthetic-private-plot', _ExtendedPriceWarning)
        warnings.warn('No data for another task', RuntimeWarning)
        return frame

    monkeypatch.setattr(scan.ll, 'Country', lambda name: name)
    monkeypatch.setattr(scan, 'load_feature', build)
    monkeypatch.setattr(scan.ll, 'Feature', lambda feature: build)
    monkeypatch.setattr(scan, 'is_this_feature_sane', lambda *a, **kw: SimpleNamespace(checks=[]))
    monkeypatch.setattr(scan, '_null_read_findings', lambda *a: [])
    monkeypatch.setattr(scan, '_scale_findings', lambda *a: [])
    monkeypatch.setattr(scan, '_assembly_invariants', lambda *a, **kw: [])
    feature, country = 'food_prices', 'Invented'
    kwargs = {'labels': 'Aggregate'} if phase == '2-kwargs' else {}
    if phase == '1a-sanity':
        findings, name, rows = scan._scan_one_country(feature, country)
        assert name == country and rows == 1
    elif phase == '1b-assembly':
        findings, assembled = scan.phase1b_assembly(feature, [country], {country: 1})
        assert assembled is frame
    else:
        findings = scan._scan_one_kwarg(feature, [country], kwargs, {}, {}, None, {country: 1})
    ordinary, price, legacy = findings
    assert [f.check for f in findings] == [
        'quantity_implausible', 'survey_kg_price', 'runtime_warning']
    for finding in (ordinary, price):
        assert (finding.status, finding.severity, finding.expected) == ('warn', 'C', False)
        assert finding.country == (country if phase == '1a-sanity' else None)
        assert finding.phase == phase and finding.feature == feature and finding.kwargs == kwargs
        assert finding.metrics == {}
    assert 'synthetic-private' not in json.dumps([f.to_record() for f in findings])
    assert (legacy.status, legacy.severity, legacy.expected) == ('warn', 'A', True)
    assert legacy.country is None and legacy.detail == 'No data for another task'
    assert legacy.kwargs == kwargs and legacy.phase == phase


def test_warning_name_in_text_or_unrelated_class_does_not_classify_it():
    same_name = type('SurveyKgPriceWarning', (UserWarning,), {})
    messages = ['SurveyKgPriceWarning text only', 'QuantityImplausibleWarning text only']

    def build():
        warnings.warn(messages[0], same_name)
        warnings.warn(messages[1], RuntimeWarning)

    _, captured, error, _ = scan._build(build)
    assert error is None
    findings = scan._warnings_to_findings('1a-sanity', 'food_prices', captured, {})
    assert [f.detail for f in findings] == messages
    assert [(f.check, f.country, f.expected, f.severity) for f in findings] == [
        ('runtime_warning', None, False, 'C')] * 2


def test_site_q_captured_before_failing_build_remains_advisory(monkeypatch):
    def build(*args):
        warnings.warn('screen could not run (synthetic-private-key)', QuantityImplausibleWarning)
        raise ValueError('separate synthetic build error')

    monkeypatch.setattr(scan.ll, 'Country', lambda name: name)
    monkeypatch.setattr(scan, 'load_feature', build)
    findings, _, rows = scan._scan_one_country('food_prices', 'Invented')
    site_q, build_error = findings
    assert rows is None
    assert (site_q.status, site_q.severity, site_q.check) == ('warn', 'C', 'quantity_implausible')
    assert 'synthetic-private-key' not in site_q.detail
    assert (build_error.status, build_error.severity, build_error.detail) == (
        'error', 'A', 'ValueError: separate synthetic build error')


def test_site_q_json_pickle_and_clusters_never_include_row_text():
    def build():
        warnings.warn('i=private-id-one plot=private-plot-one', QuantityImplausibleWarning)
        warnings.warn('i=private-id-two plot=private-plot-two', QuantityImplausibleWarning)
        warnings.warn('i=private-id-one plot=private-plot-one', SurveyKgPriceWarning)
        warnings.warn('keep this unrelated warning text', RuntimeWarning)

    _, captured, _, _ = scan._build(build)
    found = scan._warnings_to_findings('1a-sanity', 'crop_production', captured, {}, country='Invented')
    restored = pickle.loads(pickle.dumps(found))
    assert restored == found
    records = [json.loads(json.dumps(f.to_record())) for f in restored]
    a, b, price, generic = records
    assert a == b  # All stable fields, including the dedup key.
    assert a['fingerprint'] == 'crop_production|Invented||quantity_implausible'
    assert price['fingerprint'] == 'crop_production|Invented||survey_kg_price'
    assert generic['fingerprint'] == 'crop_production|ALL||runtime_warning'
    assert a.keys() == price.keys() == generic.keys()
    assert 'private-' not in json.dumps(records)

    clustered = {g['check']: g for g in cluster.cluster(records)}
    assert set(clustered) == {'quantity_implausible', 'survey_kg_price', 'runtime_warning'}
    assert clustered['quantity_implausible']['count'] == 2
    for r in (a, price):
        g = clustered[r['check']]
        assert g['countries'] == ['Invented'] and g['features'] == ['crop_production']
        assert g['example_fingerprint'] == r['fingerprint']
        assert g['example_detail'] == r['detail']
    assert 'private-' not in json.dumps(clustered)
    assert clustered['runtime_warning']['example_detail'] == generic['detail']
