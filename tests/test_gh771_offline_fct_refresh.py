"""Invented FDC transport/food operands only; never refresh a real snapshot."""
from __future__ import annotations
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('country', ['Ethiopia', 'Uganda'])
def test_actual_public_curation_metadata_needs_no_key_or_api(country, monkeypatch):
    import fooddatacentral as fdc
    script = _load('_test_fct_public_metadata', ROOT / 'scripts/refresh_fct.py')
    monkeypatch.setattr(fdc, 'nutrients', lambda *a, **k: pytest.fail('metadata must not call FDC'))
    monkeypatch.delenv('FDC_API_KEY', raising=False)
    addition, parser, harmonizer, labels, ids, sources = script._country_inputs(country, ROOT / 'lsms_library/countries')
    assert len(labels) == len(ids) > 0 and len(set(labels)) == len(labels)
    assert all(isinstance(i, int) and i > 0 for i in ids)
    assert callable(parser) and callable(harmonizer) and all(path.is_file() for path in sources)


def test_metadata_rejects_import_from_another_checkout(monkeypatch):
    from lsms_library import local_tools
    script = _load('_test_fct_import_identity', ROOT / 'scripts/refresh_fct.py')
    monkeypatch.setattr(local_tools, '__file__', '/invented/other-checkout/local_tools.py')
    with pytest.raises(RuntimeError, match='another checkout'):
        script._country_inputs('Ethiopia', ROOT / 'lsms_library/countries')


@pytest.fixture
def refresh_case(tmp_path, monkeypatch):
    script = _load('_test_fct_refresh', ROOT / 'scripts/refresh_fct.py')
    cfg = _load('_test_refresh_config', ROOT / 'lsms_library/config.py')
    config = tmp_path / 'config'
    config.mkdir()
    monkeypatch.setenv('LSMS_CONFIG_DIR', str(config))
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(ROOT / 'lsms_library/countries'))
    monkeypatch.setenv('FDC_API_KEY', 'invented-token-not-a-real-key')
    real_module = script._module
    monkeypatch.setattr(script, '_module', lambda name, path: cfg if name == '_refresh_lsms_config' else real_module(name, path))
    helper = real_module('_test_actual_ethiopia_harmonizer', ROOT / 'lsms_library/countries/Ethiopia/_/fct_tools.py')
    addition = pd.DataFrame({'Preferred Label': ['Invented A', 'Invented B'], 'FDC ID': [101, 202]})
    labels = pd.DataFrame({'FDC Label': ['Energy', 'Protein'], 'Preferred Label': ['Energy', 'Protein']})
    metadata = tmp_path / 'curation.txt'
    metadata.write_text('invented source metadata\n')
    monkeypatch.setattr(script, '_country_inputs', lambda *a: (addition, helper.nutrient_df,
        lambda raw: helper.harmonize_nutrient(raw, labels), ['Invented A', 'Invented B'], [101, 202], [metadata]))
    import fooddatacentral.fooddatacentral as provider
    calls = []

    def get(url, *, params, timeout=None, allow_redirects=True):
        calls.append(dict(url=url, timeout=timeout, allow_redirects=allow_redirects))
        number = int(url.rsplit('/', 1)[1])
        body = json.dumps({'fdcId': number, 'foodNutrients': [
            {'type': 'FoodNutrient', 'nutrient': {'name': 'Energy', 'unitName': 'kcal'}, 'amount': None},
            {'type': 'FoodNutrient', 'nutrient': {'name': 'Energy (Atwater General Factors)', 'unitName': 'kcal'}, 'amount': 20.5 if number == 101 else 30.25},
            {'type': 'FoodNutrient', 'nutrient': {'name': 'Protein', 'unitName': 'g'}, 'amount': 1.25 if number == 101 else 2.125},
        ]}).encode()
        return SimpleNamespace(status_code=200, content=body, json=lambda: json.loads(body))

    monkeypatch.setattr(provider.requests, 'get', get)
    return SimpleNamespace(script=script, cfg=cfg, provider=provider, calls=calls, get=get,
                           output=tmp_path / 'snapshot.csv', evidence=tmp_path / 'evidence',
                           metadata=metadata, config=config)


def test_explicit_refresh_reuses_actual_parser_energy_fallback_and_perkg_harmonizer(refresh_case):
    c = refresh_case
    receipt = c.script.refresh('Ethiopia', c.output, c.evidence)
    actual = pd.read_csv(c.output, index_col=0, float_precision='round_trip')
    expected = pd.DataFrame({'Energy': [205., 302.5], 'Protein': [12.5, 21.25]},
                            index=pd.Index(['Invented A', 'Invented B']))
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    np.testing.assert_array_equal(actual.to_numpy(), expected.to_numpy(), strict=True)
    assert [x['url'].rsplit('/', 1)[1] for x in c.calls] == ['101', '202']
    assert all(x['timeout'] == (10, 30) and x['allow_redirects'] is False for x in c.calls)
    assert receipt['status'] == 'complete' and receipt['source_closure'] is True
    assert len(list(c.evidence.glob('response-*.json'))) == 2
    assert c.provider.requests.get is c.get
    assert 'invented-token-not-a-real-key' not in (c.evidence / 'RECEIPT.json').read_text()


def test_explicit_refresh_bypasses_but_restores_existing_parser_cache(refresh_case):
    import fooddatacentral as fdc
    c = refresh_case
    original = fdc.nutrients
    original('invented-token-not-a-real-key', 101)
    before = original.cache_info()
    c.calls.clear()
    c.script.refresh('Ethiopia', c.output, c.evidence)
    assert len(c.calls) == 2
    assert fdc.nutrients is original
    assert original.cache_info() == before


def test_shared_fdc_id_uses_one_owned_response_for_both_labels(refresh_case, monkeypatch):
    c = refresh_case
    addition, nutrient, harmonize, labels, ids, sources = c.script._country_inputs('Ethiopia', ROOT)
    addition = addition.copy()
    addition['FDC ID'] = [101, 101]
    monkeypatch.setattr(c.script, '_country_inputs', lambda *a: (addition, nutrient, harmonize, labels, [101, 101], sources))
    receipt = c.script.refresh('Ethiopia', c.output, c.evidence)
    actual = pd.read_csv(c.output, index_col=0, float_precision='round_trip')
    assert actual.index.tolist() == ['Invented A', 'Invented B']
    np.testing.assert_array_equal(actual.iloc[0].to_numpy(), actual.iloc[1].to_numpy(), strict=True)
    assert len(c.calls) == len(receipt['requests']) == 1
    assert receipt['owned_response_memo_hits'] == [101]


@pytest.mark.parametrize('identity', [999, '101', True, None])
def test_wrong_or_missing_response_identity_is_retained_but_never_published(refresh_case, monkeypatch, identity):
    c = refresh_case
    def get(url, **kwargs):
        response = c.get(url, **kwargs)
        data = response.json()
        if identity is None:
            data.pop('fdcId')
        else:
            data['fdcId'] = identity
        body = json.dumps(data).encode()
        return SimpleNamespace(status_code=200, content=body, json=lambda: json.loads(body))
    monkeypatch.setattr(c.provider.requests, 'get', get)
    with pytest.raises(ValueError, match='identity'):
        c.script.refresh('Ethiopia', c.output, c.evidence)
    assert not c.output.exists() and len(c.calls) == 1
    assert len(list(c.evidence.glob('response-*.json'))) == 1
    assert json.loads((c.evidence / 'RECEIPT.json').read_text())['requests'][0]['response_identity_matches'] is False


def test_one_ulp_csv_change_fails_even_if_frame_assertion_false_passes(refresh_case, monkeypatch):
    c = refresh_case
    original = pd.read_csv
    def changed(*args, **kwargs):
        out = original(*args, **kwargs)
        out.iloc[0, 0] = np.nextafter(out.iloc[0, 0], np.inf)
        return out
    monkeypatch.setattr(pd, 'read_csv', changed)
    # Model the documented GH1206 false pass; the independent array/value
    # checks must still prevent publication of a changed scientific input.
    monkeypatch.setattr(pd.testing, 'assert_frame_equal', lambda *a, **k: None)
    with pytest.raises(AssertionError):
        c.script.refresh('Ethiopia', c.output, c.evidence)
    assert not c.output.exists()


def test_key_config_and_environment_precedence(refresh_case, monkeypatch):
    c = refresh_case
    (c.config / 'config.yml').write_text('fdc_api_key: invented-config-token\n')
    c.cfg._load_config.cache_clear()
    assert c.cfg.get('fdc_api_key') == 'invented-token-not-a-real-key'
    monkeypatch.delenv('FDC_API_KEY')
    assert c.cfg.get('fdc_api_key') == 'invented-config-token'


def test_missing_key_refuses_before_transport(refresh_case, monkeypatch):
    c = refresh_case
    monkeypatch.delenv('FDC_API_KEY')
    with pytest.raises(ValueError, match='fdc_api_key'):
        c.script.refresh('Ethiopia', c.output, c.evidence)
    assert c.calls == [] and not c.output.exists() and not c.evidence.exists()


@pytest.mark.parametrize('replace', [False, True])
def test_existing_snapshot_requires_explicit_replace(refresh_case, replace):
    c = refresh_case
    c.output.write_bytes(b'old snapshot\n')
    if not replace:
        with pytest.raises(FileExistsError):
            c.script.refresh('Ethiopia', c.output, c.evidence)
        assert c.output.read_bytes() == b'old snapshot\n' and c.calls == []
    else:
        assert c.script.refresh('Ethiopia', c.output, c.evidence, replace=True)['status'] == 'complete'
        assert c.output.read_bytes() != b'old snapshot\n'


@pytest.mark.parametrize('existing', [False, True])
def test_partial_response_never_publishes_final_snapshot(refresh_case, monkeypatch, existing):
    c = refresh_case
    if existing:
        c.output.write_bytes(b'old snapshot\n')
    def get(url, **kwargs):
        if url.endswith('/202'):
            c.calls.append({'url': url})
            return SimpleNamespace(status_code=404)
        return c.get(url, **kwargs)
    monkeypatch.setattr(c.provider.requests, 'get', get)
    with pytest.raises(ValueError, match='incomplete FDC'):
        c.script.refresh('Ethiopia', c.output, c.evidence, replace=existing)
    assert (c.output.read_bytes() == b'old snapshot\n') if existing else not c.output.exists()
    receipt = json.loads((c.evidence / 'RECEIPT.json').read_text())
    assert receipt['status'] == 'failed' and len(receipt['requests']) == 2
    assert len(list(c.evidence.glob('response-*.json'))) == 1
    assert list(c.output.parent.glob('.fct-refresh-*.csv')) == []


def test_secret_safe_failure_and_no_automatic_retry(refresh_case, monkeypatch, capsys):
    import fooddatacentral as fdc
    c = refresh_case
    parser = fdc.nutrients
    cache = parser.cache_info()
    token = 'invented-token-not-a-real-key'
    def get(*a, **k):
        c.calls.append('attempt')
        raise RuntimeError('invented transport URL would include api_key=' + token)
    monkeypatch.setattr(c.provider.requests, 'get', get)
    assert c.script.main(['--country', 'Ethiopia', '--output', str(c.output), '--evidence-dir', str(c.evidence)]) == 1
    streams = capsys.readouterr()
    assert token not in streams.out + streams.err
    assert 'RuntimeError' in streams.err and c.calls == ['attempt']
    assert token not in (c.evidence / 'RECEIPT.json').read_text()
    assert not c.output.exists()
    assert fdc.nutrients is parser and parser.cache_info() == cache


def test_request_start_is_durable_before_transport_and_failure_is_not_erased(refresh_case, monkeypatch):
    c = refresh_case
    def get(*args, **kwargs):
        journal = json.loads((c.evidence / 'RECEIPT.json').read_text())
        assert journal['requests'] == [{'fdc_id': 101, 'ordinal': 1, 'state': 'started'}]
        raise RuntimeError('invented transport interruption')
    monkeypatch.setattr(c.provider.requests, 'get', get)
    with pytest.raises(RuntimeError):
        c.script.refresh('Ethiopia', c.output, c.evidence)
    journal = json.loads((c.evidence / 'RECEIPT.json').read_text())
    assert journal['requests'][0]['state'] == 'failed'
    assert journal['requests'][0]['error_type'] == 'RuntimeError'
    assert len(journal['requests']) == 1 and not c.output.exists()


def test_source_change_prevents_snapshot_commit(refresh_case, monkeypatch):
    c = refresh_case
    def get(url, **kwargs):
        c.metadata.write_text('changed invented source\n')
        return c.get(url, **kwargs)
    monkeypatch.setattr(c.provider.requests, 'get', get)
    with pytest.raises(ValueError, match='source changed'):
        c.script.refresh('Ethiopia', c.output, c.evidence)
    assert not c.output.exists()
    assert json.loads((c.evidence / 'RECEIPT.json').read_text())['source_closure'] is False


@pytest.mark.parametrize('url', ['http://api.nal.usda.gov/fdc/v1/food/101',
                                'https://example.invalid/fdc/v1/food/101',
                                'https://api.nal.usda.gov/fdc/v1/food/999'])
def test_unrequested_endpoint_never_reaches_transport(refresh_case, url):
    c = refresh_case
    c.evidence.mkdir()
    receipt = {'requests': []}
    with pytest.raises(ValueError):
        with c.script._bounded_requests([101], 'invented-key', c.evidence, receipt):
            c.provider.requests.get(url, params=(('api_key', 'invented-key'),))
    assert c.calls == [] and c.provider.requests.get is c.get


@pytest.mark.parametrize('country', ['Ethiopia', 'Uganda'])
@pytest.mark.parametrize('failure', [FileNotFoundError, pd.errors.ParserError])
def test_runtime_missing_or_corrupt_snapshot_never_calls_food_or_fdc(country, failure, monkeypatch):
    import lsms_library as ll
    from lsms_library import local_tools as lt
    import fooddatacentral as fdc
    calls = []
    def forbidden(*args, **kwargs):
        calls.append('forbidden')
        raise AssertionError('no food/provider call allowed')
    monkeypatch.setattr(ll, 'Country', forbidden)
    monkeypatch.setattr(lt, 'to_parquet', forbidden)
    monkeypatch.setattr(fdc, 'nutrients', forbidden)
    foods = pd.DataFrame({'Preferred Label': ['Invented'], 'FTC Code': [1], 'FDC ID': [101]})
    monkeypatch.setattr(lt, 'df_from_orgfile', lambda *a, **k: foods.copy())
    monkeypatch.setitem(sys.modules, 'fct_tools', SimpleNamespace(fct_filter=lambda *a: pd.DataFrame()))
    def read(path, *args, **kwargs):
        if path == 'fct_uganda.csv':
            return pd.DataFrame({'i': ['Invented'], 'Energy': [1.]})
        assert path == 'fct_usda.csv'
        raise failure('invented unavailable snapshot')
    monkeypatch.setattr(pd, 'read_csv', read)
    monkeypatch.setattr(sys, 'argv', ['nutrition.py'])
    monkeypatch.setattr(sys, 'path', list(sys.path))
    path = ROOT / 'lsms_library/countries' / country / '_' / 'nutrition.py'
    with pytest.raises(failure) as caught:
        exec(compile(path.read_text(), str(path), 'exec'), {'__name__': '__main__', '__file__': str(path)})
    if failure is FileNotFoundError:
        assert 'scripts/refresh_fct.py --country ' + country in str(caught.value)
    assert calls == []


def test_ethiopia_runtime_uses_snapshot_without_rescaling_or_fdc(monkeypatch):
    import lsms_library as ll
    from lsms_library import local_tools as lt
    import fooddatacentral as fdc
    q = pd.DataFrame({'Quantity': [2., 3., 100.]}, index=pd.MultiIndex.from_tuples([
        ('h', 'wave', 'Origin food', 'kg'), ('h', 'wave', 'USDA food', 'Kg'),
        ('h', 'wave', 'USDA food', 'piece')], names=['i', 't', 'j', 'u']))
    monkeypatch.setattr(ll, 'Country', lambda c: SimpleNamespace(food_quantities=lambda: q.copy()))
    monkeypatch.setattr(fdc, 'nutrients', lambda *a: pytest.fail('runtime must stay offline'))
    foods = pd.DataFrame({'Preferred Label': ['Origin food', 'USDA food'], 'FTC Code': [1, pd.NA], 'FDC ID': [pd.NA, 101]})
    monkeypatch.setattr(lt, 'df_from_orgfile', lambda *a, **k: foods.copy())
    origin = pd.DataFrame({'Energy': [10.], 'Protein': [2.]}, index=['Origin food'])
    snapshot = pd.DataFrame({'Energy': [30.], 'Protein': [3.]}, index=['USDA food'])
    monkeypatch.setitem(sys.modules, 'fct_tools', SimpleNamespace(fct_filter=lambda *a: origin.copy()))
    monkeypatch.setattr(pd, 'read_csv', lambda *a, **k: snapshot.copy())
    writes = {}
    monkeypatch.setattr(lt, 'to_parquet', lambda df, path: writes.__setitem__(path, df.copy()))
    monkeypatch.setattr(sys, 'path', list(sys.path))
    path = ROOT / 'lsms_library/countries/Ethiopia/_/nutrition.py'
    exec(compile(path.read_text(), str(path), 'exec'), {'__name__': '__main__', '__file__': str(path)})
    expected = pd.DataFrame([[110., 13.]], index=pd.MultiIndex.from_tuples([('h', 'wave')], names=['i', 't']), columns=['Energy', 'Protein'])
    pd.testing.assert_frame_equal(writes['../var/nutrition.parquet'], expected, check_exact=True)
    np.testing.assert_array_equal(writes['../var/nutrition.parquet'].to_numpy(), expected.to_numpy(), strict=True)


def test_actual_refresh_snapshot_preserves_existing_nutrition_units_and_matrix(refresh_case, monkeypatch):
    import lsms_library as ll
    from lsms_library import local_tools as lt
    c = refresh_case
    c.script.refresh('Ethiopia', c.output, c.evidence)
    q = pd.DataFrame({'Quantity': [1., 2., 3., 1000.]}, index=pd.MultiIndex.from_tuples([
        ('h', 'wave', 'Origin food', 'kg'), ('h', 'wave', 'Invented A', 'Kg'),
        ('h', 'wave', 'Invented B', 'kg'), ('h', 'wave', 'Invented A', 'piece')],
        names=['i', 't', 'j', 'u']))
    monkeypatch.setattr(ll, 'Country', lambda country: SimpleNamespace(food_quantities=lambda: q.copy()))
    foods = pd.DataFrame({'Preferred Label': ['Origin food', 'Invented A', 'Invented B'],
                          'FTC Code': [1, pd.NA, pd.NA], 'FDC ID': [pd.NA, 101, 202]})
    monkeypatch.setattr(lt, 'df_from_orgfile', lambda *a, **k: foods.copy())
    origin = pd.DataFrame({'Energy': [10.], 'Protein': [2.]}, index=pd.Index(['Origin food'], name='i'))
    origin.columns.name = 'n'  # Existing fct_filter metadata convention.
    monkeypatch.setitem(sys.modules, 'fct_tools', SimpleNamespace(fct_filter=lambda *a: origin.copy()))
    reader = pd.read_csv
    monkeypatch.setattr(pd, 'read_csv', lambda path, *a, **k: reader(c.output, *a, **k))
    writes = {}
    monkeypatch.setattr(lt, 'to_parquet', lambda df, path: writes.__setitem__(path, df.copy()))
    monkeypatch.setattr(sys, 'path', list(sys.path))
    path = ROOT / 'lsms_library/countries/Ethiopia/_/nutrition.py'
    exec(compile(path.read_text(), str(path), 'exec'), {'__name__': '__main__', '__file__': str(path)})
    # Original helper's per100g→perkg x10 is applied once by refresh. The
    # unchanged consumer must neither rescale nor include the piece row.
    expected = pd.DataFrame([[1327.5, 90.75]], index=pd.MultiIndex.from_tuples([('h', 'wave')], names=['i', 't']), columns=['Energy', 'Protein'])
    pd.testing.assert_frame_equal(writes['../var/nutrition.parquet'], expected, check_exact=True)
    np.testing.assert_array_equal(writes['../var/nutrition.parquet'].to_numpy(), expected.to_numpy(), strict=True)
    assert len(c.calls) == 2  # The ordinary build performs no extra request.
