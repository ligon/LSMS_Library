"""Actual Country descriptors on invented metadata; no constructors or sources.

The parent control shadows only the new descriptor and seeds the parent's
empty public attribute. waves/__getitem__ are unchanged parent methods (the
dispatch byteproof also checks them against b8346cf1). Expectations below name
invented logical labels and physical folders independently of those methods.
"""
import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from lsms_library import country as C
from lsms_library.transformations import pp_ph_area_t


class MetadataPath:
    """Strict in-memory path provider: never touches a filesystem."""

    def __init__(self, provider, parts=()):
        self.provider, self.parts = provider, parts

    def __truediv__(self, part):
        return MetadataPath(self.provider, self.parts + (part,))

    @property
    def name(self):
        return self.parts[-1]

    def exists(self):
        self.provider.events.append(('exists', self.parts))
        if self.parts == ('_', 'control.py'):
            return self.provider.module_exists
        if (len(self.parts) == 3 and self.parts[0] in self.provider.folders
                and self.parts[1] == 'Documentation'
                and self.parts[2] in ('SOURCE.org', 'SOURCE')):
            return self.parts[2] in self.provider.folders[self.parts[0]]
        raise AssertionError(f'unexpected metadata path: {self.parts}')

    def iterdir(self):
        assert self.parts == ()
        self.provider.events.append(('iterdir',))
        return [self / folder for folder in self.provider.folders]

    def is_dir(self):
        assert len(self.parts) == 1 and self.name in self.provider.folders
        return self.provider.folders[self.name] is not None


class InventedCountry(C.Country):
    @property
    def file_path(self):
        return self.__dict__['provider'].root

    @property
    def resources(self):
        provider = self.__dict__['provider']
        assert not provider.module_exists, 'module existence must suppress YAML'
        provider.events.append(('resources',))
        if provider.resource_error is not None:
            raise provider.resource_error
        return provider.config

    def __getattr__(self, name):
        raise AssertionError(f'hidden Country getter: {name}')


class ParentCountry(InventedCountry):
    # Parent waves/getitem are inherited unchanged; old __init__ seeded {}.
    wave_folder_map = None


class InertWave:
    def __init__(self, year, wave_folder, country):
        self.year, self.wave_folder, self.country = year, wave_folder, country

    def __getattr__(self, name):
        raise AssertionError(f'hidden Wave getter: {name}')


@pytest.fixture
def bind(monkeypatch):
    providers = []

    def spec_from_file_location(name, path):
        assert name == 'control' and isinstance(path, MetadataPath)
        provider = path.provider
        assert provider in providers and provider.module_exists
        assert path.parts == ('_', 'control.py')
        provider.events.append(('module-spec',))

        def exec_module(module):
            provider.events.append(('module-exec-invented',))
            if provider.module_error is not None:
                raise provider.module_error
            assert set(provider.attrs) <= {'waves', 'Waves', 'wave_folder_map'}
            module.__dict__.update(provider.attrs)

        spec = SimpleNamespace(loader=SimpleNamespace(exec_module=exec_module))
        provider.spec = spec
        return spec

    def module_from_spec(spec):
        assert any(getattr(p, 'spec', None) is spec for p in providers)
        return SimpleNamespace()

    monkeypatch.setattr(C.importlib.util, 'spec_from_file_location', spec_from_file_location)
    monkeypatch.setattr(C.importlib.util, 'module_from_spec', module_from_spec)
    monkeypatch.setattr(C, 'Wave', InertWave)

    def install(*, attrs=None, config=None, folders=None, parent=False,
                module_error=None, resource_error=None):
        provider = SimpleNamespace(
            module_exists=attrs is not None, attrs=attrs,
            config={} if config is None else config,
            folders={} if folders is None else folders, events=[],
            module_error=module_error, resource_error=resource_error,
        )
        provider.root = MetadataPath(provider)
        providers.append(provider)
        cls = ParentCountry if parent else InventedCountry
        country = cls.__new__(cls)
        country.__dict__.update(name='Control', provider=provider)
        if parent:
            country.__dict__['wave_folder_map'] = {}
        assert cls.waves is C.Country.waves
        assert cls.__getitem__ is C.Country.__getitem__
        return country, provider

    return install


@pytest.mark.parametrize('authority', ['waves', 'Waves', 'both', 'yaml-list', 'json-dict'])
def test_direct_read_before_waves_uses_existing_authority(bind, authority):
    mapping = {'later': 'bundle', 'earlier': 'bundle', 'solo': 'separate'}
    expected = dict(mapping)
    if authority.startswith(('yaml', 'json')):
        declared = ['solo', 'later', 'earlier']
        if authority == 'json-dict':
            declared = dict.fromkeys(declared, None)
        config = json.loads(json.dumps({'Waves': declared, 'wave_folder_map': mapping}))
        country, provider = bind(config=config)
        original_map = config['wave_folder_map']
    else:
        attrs = {'wave_folder_map': mapping}
        if authority in ('waves', 'both'):
            attrs['waves'] = ['solo', 'later', 'earlier']
        if authority in ('Waves', 'both'):
            attrs['Waves'] = {'later': {}, 'earlier': {}}
        country, provider = bind(attrs=attrs, config={'Waves': ['ignored']})
        original_map = mapping
    assert 'wave_folder_map' not in country.__dict__
    actual = country.wave_folder_map
    assert actual == expected and actual is original_map
    events = list(provider.events)
    assert country.wave_folder_map is actual
    assert provider.events == events
    assert country.waves == (['earlier', 'later'] if authority == 'Waves'
                             else ['earlier', 'later', 'solo'])
    assert country.wave_folder_map is actual


@pytest.mark.parametrize('authority', ['module', 'yaml'])
def test_parent_negative_direct_read_then_compatibility_after_waves(bind, authority):
    expected = {'late': 'physical', 'early': 'physical'}
    kwargs = ({'attrs': {'waves': ['late', 'early'], 'wave_folder_map': expected}}
              if authority == 'module' else
              {'config': {'Waves': ['late', 'early'], 'wave_folder_map': expected}})
    old, old_provider = bind(parent=True, **kwargs)
    new, new_provider = bind(**kwargs)
    assert old.wave_folder_map == {} and old.wave_folder_map != expected
    assert old_provider.events == []
    assert new.wave_folder_map == expected
    assert old.waves == new.waves == ['early', 'late']
    assert old.wave_folder_map is new.wave_folder_map is expected
    assert ('iterdir',) not in old_provider.events + new_provider.events


@pytest.mark.parametrize('attrs,config,expected_map', [
    ({'waves': ['z', 'a']}, {'Waves': ['yaml'], 'wave_folder_map': {'yaml': 'bad'}}, {}),
    ({}, {'Waves': ['yaml'], 'wave_folder_map': {'yaml': 'bad'}}, {}),
    ({'wave_folder_map': {'a': 'shared'}}, {'Waves': ['yaml']}, {'a': 'shared'}),
    (None, {'wave_folder_map': {'a': 'ignored-without-Waves'}}, {}),
    (None, {'Waves': [], 'wave_folder_map': {'a': 'ignored'}}, {}),
])
def test_module_existence_and_directory_fallback_match_parent(bind, attrs, config, expected_map):
    # SOURCE.org and legacy SOURCE are supported; plain/misnamed/file entries aren't.
    folders = {'z': {'SOURCE'}, 'plain': set(), 'a': {'SOURCE.org'}, 'file': None}
    old, _ = bind(attrs=attrs, config=config, folders=folders, parent=True)
    new, provider = bind(attrs=attrs, config=config, folders=folders)
    assert new.wave_folder_map == expected_map
    assert old.waves == new.waves == ['a', 'z']
    assert old.wave_folder_map == new.wave_folder_map == expected_map
    assert new.wave_folder_map is new.wave_folder_map
    if attrs is not None:
        assert ('resources',) not in provider.events


@pytest.mark.parametrize('map_value', [None, {}, [], ''])
def test_truthy_yaml_waves_without_a_truthy_map_keeps_empty_fallback(bind, map_value):
    config = {'Waves': ['z', 'a'], 'wave_folder_map': map_value}
    old, _ = bind(config=config, parent=True)
    new, _ = bind(config=config)
    first = new.wave_folder_map
    assert first == {} and new.wave_folder_map is first
    assert old.waves == new.waves == ['a', 'z']
    assert old.wave_folder_map == new.wave_folder_map == {}
    # The original YAML resolver assigns a new empty dict on each waves read.
    assert new.wave_folder_map is not first


def test_absent_yaml_map_and_waves_first_are_supported(bind):
    country, _ = bind(config={'Waves': ['b', 'a']})
    assert country.waves == ['a', 'b']
    mapping = country.wave_folder_map
    assert mapping == {} and country.wave_folder_map is mapping


@pytest.mark.parametrize('value', [{}, {'override': 'owned'}, None, []])
def test_explicit_assignment_is_an_ordinary_public_attribute_without_lazy_work(bind, value):
    country, provider = bind(attrs={'waves': ['a'], 'wave_folder_map': {'a': 'module'}})
    country.wave_folder_map = value
    assert country.wave_folder_map is value
    assert country.__dict__['wave_folder_map'] is value
    assert provider.events == []
    assert country.waves == ['a']
    assert country.wave_folder_map is provider.attrs['wave_folder_map']


@pytest.mark.parametrize('authority', ['module-map', 'module-no-map', 'yaml-map', 'directory'])
def test_mutation_aliasing_override_and_refresh_follow_parent(bind, authority):
    def make(parent):
        if authority.startswith('module'):
            attrs = {'waves': ['a', 'b']}
            if authority == 'module-map':
                attrs['wave_folder_map'] = {'a': 'one', 'b': 'two'}
            return bind(attrs=attrs, parent=parent)
        if authority == 'yaml-map':
            return bind(config={'Waves': ['b', 'a'], 'wave_folder_map': {'a': 'one'}}, parent=parent)
        return bind(folders={'b': {'SOURCE'}, 'a': {'SOURCE.org'}}, parent=parent)

    old, _ = make(True)
    new, _ = make(False)
    assert old.waves == new.waves == ['a', 'b']
    for country in (old, new):
        original = country.wave_folder_map
        country.wave_folder_map['extra'] = 'mutation'
        assert original['extra'] == 'mutation'
        country.wave_folder_map = {'replacement': 'explicit'}
        assert original['extra'] == 'mutation'
        assert country.waves == ['a', 'b']
    assert old.wave_folder_map == new.wave_folder_map
    assert ('replacement' in new.wave_folder_map) == (authority in ('module-no-map', 'directory'))


def test_later_waves_reload_module_map_and_waves_are_not_cached(bind):
    first, second = {'a': 'first'}, {'b': 'second'}
    country, provider = bind(attrs={'waves': ['a'], 'wave_folder_map': first})
    assert country.wave_folder_map is first
    provider.attrs.update(waves=['c', 'b'], wave_folder_map=second)
    assert country.wave_folder_map is first  # ordinary instance value until waves refresh
    assert country.waves == ['b', 'c']
    assert country.wave_folder_map is second
    assert provider.events.count(('module-exec-invented',)) == 2


def test_public_deletion_releases_mapping_and_next_read_resolves_again(bind):
    country, provider = bind(attrs={'waves': ['a'], 'wave_folder_map': {'a': 'first'}})
    first = country.wave_folder_map
    del country.wave_folder_map
    provider.attrs['wave_folder_map'] = {'a': 'second'}
    assert country.wave_folder_map == {'a': 'second'}
    assert country.wave_folder_map is not first


@pytest.mark.parametrize('declared_map', [None, [], {'unlisted': 'retained'}])
def test_module_map_value_is_not_coerced_or_filtered(bind, declared_map):
    country, _ = bind(attrs={'waves': ['a'], 'wave_folder_map': declared_map})
    assert country.wave_folder_map is declared_map
    assert country.waves == ['a']
    assert country.wave_folder_map is declared_map


@pytest.mark.parametrize('authority', ['module', 'yaml', 'directory'])
def test_getitem_transports_logical_and_physical_identity_with_inert_wave(bind, authority):
    kwargs = ({'attrs': {'waves': ['late', 'early', 'solo'],
                         'wave_folder_map': {'late': 'bundle', 'early': 'bundle'}}}
              if authority == 'module' else
              {'config': {'Waves': ['late', 'early', 'solo'],
                          'wave_folder_map': {'late': 'bundle', 'early': 'bundle'}}}
              if authority == 'yaml' else
              {'folders': {'late': {'SOURCE'}, 'early': {'SOURCE.org'}, 'solo': {'SOURCE'}}})
    for parent in (True, False):
        country, _ = bind(parent=parent, **kwargs)
        for label in ('late', 'early', 'solo'):
            wave = country[label]
            assert type(wave) is InertWave and wave.country is country
            assert wave.year == label
            assert wave.wave_folder == ('bundle' if authority != 'directory' and label != 'solo' else label)
        with pytest.raises(KeyError, match='missing is not a valid wave for Control'):
            country['missing']


@pytest.mark.parametrize('authority', ['module', 'yaml'])
def test_metadata_errors_propagate_by_identity_without_caching_empty_success(bind, authority):
    error = RuntimeError('invented metadata unavailable')
    kwargs = ({'attrs': {}, 'module_error': error} if authority == 'module' else
              {'resource_error': error})
    old, _ = bind(parent=True, **kwargs)
    new, _ = bind(**kwargs)
    with pytest.raises(RuntimeError) as parent_error:
        _ = old.waves
    with pytest.raises(RuntimeError) as candidate_error:
        _ = new.wave_folder_map
    assert parent_error.value is candidate_error.value is error
    assert 'wave_folder_map' not in new.__dict__


def test_direct_map_is_sufficient_for_existing_pair_consumer(bind):
    country, _ = bind(config={'Waves': ['2001Q1', '2000Q3', '2002'],
                              'wave_folder_map': {'2001Q1': 'pair', '2000Q3': 'pair', '2002': 'solo'}})
    assert pp_ph_area_t(country.wave_folder_map) == {'2000Q3': '2001Q1'}


def test_tanzania_literal_mapping_without_executing_its_module(bind):
    source = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Tanzania/_/tanzania.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    literal = next(ast.literal_eval(node.value) for node in tree.body
                   if isinstance(node, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == 'wave_folder_map' for t in node.targets))
    expected = {'2008-09': '2008-15', '2010-11': '2008-15', '2012-13': '2008-15',
                '2014-15': '2008-15', '2019-20': '2019-20', '2020-21': '2020-21'}
    assert literal == expected
    country, _ = bind(attrs={'waves': list(reversed(expected)), 'wave_folder_map': literal})
    assert country.wave_folder_map == expected
    assert country.waves == sorted(expected)
    with pytest.raises(ValueError, match='holds 4 rounds'):
        pp_ph_area_t(country.wave_folder_map)
