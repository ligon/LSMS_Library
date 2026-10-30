"""Ruled native price varieties; invented inputs and frozen production oracle."""
import ast
import copy
import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import warnings

import numpy as np
import pandas as pd
import pytest

from tests.test_gh1196_parquet_index_policy import _exact, _typed


@pytest.fixture(scope='module')
def nigeria():
    path = Path(__file__).parents[1] / 'lsms_library/countries/Nigeria/_/nigeria.py'
    spec = importlib.util.spec_from_file_location('nigeria_637_variety', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def original(nigeria):
    # Verbatim helper at public26803b81, not a rewritten expected algorithm.
    path = Path(__file__).with_name('fixtures') / 'nigeria_community_prices_pre_637_variety.txt'
    source = path.read_text()
    assert hashlib.sha256(source.encode()).hexdigest() == \
        '5e8c2023435f05f2917d4de0a0a04b44f5d71462d90492363d059e90c3f6d9c8'
    assert len(ast.parse(source).body) == 1
    namespace = dict(vars(nigeria))
    exec(compile(source, str(path), 'exec'), namespace)
    return namespace['community_prices_for_wave']


def names(codes, prices, index=None, ea=None):
    n = len(codes)
    raw = pd.DataFrame({'state': [1] * n, 'lga': [101] * n,
                        'ea': [1] * n if ea is None else ea,
                        'item_cd': codes, 'price': prices}, index=index)
    return dict(df=raw, dec=raw, ea='ea', item='item_cd', price='price')


def call(function, wave, frames, mode='names'):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return function(wave, frames, mode, {11: 'Millet', 12: 'Sorghum'})


@pytest.mark.parametrize('wave', ['2011Q1', '2013Q1'])
@pytest.mark.parametrize('index', [None, [8, 2, 9, 1], [4, 4, 1, 1]])
def test_full_size_and_lumped_item_identities_survive_equal_prices(nigeria, wave, index):
    out = call(nigeria.community_prices_for_wave, wave,
               [names([1511, 1512, 3, 4], [50., 50., 70., 70.], index=index)])
    flat = out.reset_index()
    got = dict(zip(flat.variety, zip(flat.j, flat.u, flat.Price)))
    assert got == {'1511': ('Milk powder', 'tin', 50.),
                   '1512': ('Milk powder', 'tin', 50.),
                   '3': ('Maize', 'Kg', 70.), '4': ('Maize', 'Kg', 70.)}
    assert len(out) == 4 and out.index.is_unique
    assert set(flat.v) == {'1/101/1'} and flat.i.equals(flat.v)
    assert flat.obs.tolist() == [1] * 4
    assert not any(v.endswith('.0') for v in flat.variety)


@pytest.mark.parametrize('prices', [[50., 50.], [50., 90.]])
def test_repeated_complete_native_key_refuses_even_equal_prices(nigeria, prices):
    with pytest.raises(AssertionError, match='broken key'):
        call(nigeria.community_prices_for_wave, '2011Q1', [names([1511, 1511], prices)])


@pytest.mark.parametrize('code', [1511.5, 3.25])
def test_retained_fractional_identity_refuses(nigeria, code):
    with pytest.raises(ValueError, match='finite integers for native variety'):
        call(nigeria.community_prices_for_wave, '2011Q1', [names([code], [50.])])


@pytest.mark.parametrize('junk', ['zero_price', 'missing_price', 'missing_ea', 'unmapped'])
def test_excluded_junk_does_not_become_a_new_identity_or_duplicate_failure(nigeria, original, junk):
    code, price, ea = {'zero_price': (1511.5, 0., 1),
                       'missing_price': (1511.5, np.nan, 1),
                       'missing_ea': (1511.5, 30., None),
                       'unmapped': (99999.5, 30., 1)}[junk]
    fr = names([1511, code, code], [50., price, price], ea=[1, ea, ea])
    old = call(original, '2011Q1', [fr])
    new = call(nigeria.community_prices_for_wave, '2011Q1', [fr])
    _exact(new.droplevel('variety'), old)
    assert new.index.get_level_values('variety').tolist() == ['1511']


def codes_frame(unit='Heap Small', size=None, free_text=None, index=None, obs=1):
    raw = pd.DataFrame({'state': [1, 1], 'lga': [101, 101], 'ea': [1, 2],
                        'item_cd': [11, 12], 'price': [500., 300.],
                        'unit': [1, 1], 'size': [np.nan, np.nan],
                        'text': [free_text, free_text]}, index=index)
    dec = raw.copy()
    dec['unit'] = unit
    dec['size'] = size
    fr = dict(df=raw, dec=dec, ea='ea', item='item_cd', price='price',
              unit='unit', obs=obs)
    if size is not None:
        fr['size'] = 'size'
    if free_text is not None or unit in ['GRAMS (G)', 'KILOGRAMS (KG)']:
        fr['size_text'] = 'text'
    return fr


def with_constant_variety(old):
    result = old.copy(deep=True)
    result['variety'] = 'unknown_variety'
    return result.set_index('variety', append=True).reorder_levels(
        ['t', 'v', 'i', 'j', 'u', 'variety', 'obs'])


@pytest.mark.parametrize('wave,unit,size,text', [
    ('2016Q1', 'Heap Small', None, None),
    ('2016Q1', 'GRAMS (G)', None, '50cl'),
    ('2016Q1', 'KILOGRAMS (KG)', None, None),
    ('2019Q1', '200. LOAF', '99. ONE SIZE ONLY', None),
    ('2019Q1', 'GRAMS (G)', '34. 250 GRAMS', None),
    ('2019Q1', 'KILOGRAMS (KG)', '99. ONE SIZE ONLY', None),
    ('2024Q1', 'GRAMS (G)', '34. 250 GRAMS', None),
    ('2024Q1', '2. GRAMS (G)', None, None),
])
@pytest.mark.parametrize('index', [[8, 2], [4, 4]])
def test_codes_modes_exact_frozen_payload_axes_masks_and_empty_dtype(
        nigeria, original, wave, unit, size, text, index):
    fr = codes_frame(unit, size, text, index)
    old = call(original, wave, [fr], 'codes')
    new = call(nigeria.community_prices_for_wave, wave, [fr], 'codes')
    _exact(new, with_constant_variety(old))
    _exact(new.droplevel('variety'), old)


def test_existing_nonfinite_source_refusal_is_preserved(nigeria, original):
    # The preexisting name resolver int() refusal must not become a new key.
    for function in [original, nigeria.community_prices_for_wave]:
        with pytest.raises(OverflowError):
            call(function, '2011Q1', [names([np.inf], [0.])])


@pytest.mark.parametrize('mode', ['names', 'codes'])
def test_empty_output_has_declared_typed_axis(nigeria, original, mode):
    fr = names([1511], [0.]) if mode == 'names' else codes_frame('KILOGRAMS (KG)')
    if mode == 'codes':
        fr['df']['price'] = 0.
    old = call(original, '2011Q1', [fr], mode)
    new = call(nigeria.community_prices_for_wave, '2011Q1', [fr], mode)
    expected = with_constant_variety(old)
    if mode == 'names':
        # Native source code string has NA-aware dtype even when no row survives.
        pos = expected.index.names.index('variety')
        levels = list(expected.index.levels)
        levels[pos] = pd.Index(pd.array([], dtype='string'), name='variety')
        expected.index = pd.MultiIndex(levels=levels, codes=expected.index.codes,
                                       names=expected.index.names)
    _exact(new, expected)


def feature(frame, monkeypatch):
    import lsms_library as ll
    # Feature.__call__ imports Country from the package at call time; this
    # invented frame provider tests the REAL assembly and attrs contract only.
    monkeypatch.setattr(ll, 'Country', lambda *a, **kw: SimpleNamespace(
        community_prices=lambda: frame.copy(deep=True)))
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return ll.Feature('community_prices')(['Nigeria'], currency=None)


def test_real_feature_payload_and_only_declared_provenance_change(nigeria, original, monkeypatch):
    frames = [codes_frame(obs=1), codes_frame(obs=2)]
    # Same-price two-location pairs are observations; location2-only also survives.
    frames[1]['df'].loc[2] = [1, 101, 3, 11, 750., 1, np.nan, None]
    frames[1]['dec'] = frames[1]['df'].copy()
    frames[1]['dec']['unit'] = 'Heap Small'
    old = call(original, '2016Q1', frames, 'codes').droplevel('i')
    new = call(nigeria.community_prices_for_wave, '2016Q1', frames, 'codes').droplevel('i')
    old.attrs['recall'] = {'scope': 'synthetic', 'days': 7}
    new.attrs = copy.deepcopy(old.attrs)
    before = feature(old, monkeypatch)
    actual = feature(new, monkeypatch)
    expected = before.copy(deep=True)
    expected.attrs = copy.deepcopy(before.attrs)
    _typed(before.attrs['canonical_alignment'], {'fabricated': {
        'Nigeria': {'variety': 'unknown_variety', 'visit': 1}}})
    expected.attrs['canonical_alignment']['fabricated']['Nigeria'].pop('variety')
    _exact(actual, expected)
    assert len(actual) == 5
    assert actual.index.names == ['country', 't', 'v', 'j', 'u', 'variety', 'visit', 'obs']
    assert set(actual.index.get_level_values('variety')) == {'unknown_variety'}
    assert set(actual.index.get_level_values('visit')) == {1}
    assert actual.attrs['recall'] == before.attrs['recall']


@pytest.mark.parametrize('mutation', [np.nextafter(1., 2.), 1e-300])
def test_strict_payload_adapter_refuses_real_nullable_float_mutation(mutation):
    a = pd.DataFrame({'Price': pd.array([1., None], dtype='Float64')})
    b = a.copy(deep=True)
    b.iloc[0, 0] = mutation
    with pytest.raises(AssertionError):
        _exact(a, b)


def test_nigeria_registry_declares_uniform_native_variety_axis():
    from lsms_library.paths import countries_root
    from lsms_library.yaml_utils import load_yaml
    spec = load_yaml(countries_root() / 'Nigeria/_/data_scheme.yml')
    index = spec['Data Scheme']['community_prices']['index']
    assert [x.strip() for x in index.strip('()').split(',')] == \
        ['t', 'v', 'j', 'u', 'variety', 'obs']
