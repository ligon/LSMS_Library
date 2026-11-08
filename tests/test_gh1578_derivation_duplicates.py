"""Registry-only duplicate refusal; no producer/callable resolution or data."""
from io import StringIO
import textwrap

import pytest
import yaml

from importlib import import_module
D = import_module("lsms_library.derivations")
from lsms_library.yaml_utils import SchemeLoader, load_yaml

KEY = 'Invented::table::one'
ENTRY = """rule: invented rule
function: invented.module:derive
inputs: invented.module:inputs
raw_variables: [native_a]
assumptions:
  - text: invented assumption
    basis: modelling-choice
columns: [original]
"""


def write(tmp_path, body=ENTRY, key=KEY):
    path = tmp_path / 'derivations.yml'
    path.write_text(key + ':\n' + textwrap.indent(body, '  '), encoding='utf-8')
    return path


@pytest.fixture(autouse=True)
def clear_registry_caches():
    D.derivation_records.cache_clear(); D.framework_records.cache_clear()
    yield
    D.derivation_records.cache_clear(); D.framework_records.cache_clear()


def test_duplicate_top_record_names_both_locations(tmp_path):
    path = write(tmp_path); path.write_text(path.read_text()*2)
    with pytest.raises(yaml.constructor.ConstructorError, match='duplicate key') as caught:
        D._load(path, 'Invented')
    error = caught.value
    assert KEY in str(error) and str(path) in str(error)
    assert error.context_mark.line == 0 and error.problem_mark.line == len((KEY+':\n'+textwrap.indent(ENTRY,'  ')).splitlines())


@pytest.mark.parametrize('first,second', [('rule', 'rule'), ('rule', '"rule"'),
                                         ('rule', '"ru\\x6ce"')])
def test_quoted_and_escaped_fields_are_the_same_key(tmp_path, first, second):
    body = ENTRY.replace('rule: invented rule', f'{first}: first\n{second}: second')
    with pytest.raises(yaml.constructor.ConstructorError, match="duplicate key 'rule'"):
        D._load(write(tmp_path, body), 'Invented')


@pytest.mark.parametrize('first,second', [('true','1'),('01','1'),('1.0','0x1'),
                                         ('null','~'),('.nan','.NaN'),('yes','true'),('=','"="')])
def test_nested_resolved_equivalent_keys_are_duplicates(tmp_path, first, second):
    body = ENTRY + f'rows: {{{first}: first, {second}: second}}\n'
    with pytest.raises(yaml.constructor.ConstructorError, match='duplicate key'):
        D._load(write(tmp_path, body), 'Invented')


def test_nested_assumption_member_is_checked(tmp_path):
    body = ENTRY.replace('basis: modelling-choice','basis: modelling-choice\n    basis: questionnaire')
    with pytest.raises(yaml.constructor.ConstructorError, match="duplicate key 'basis'"):
        D._load(write(tmp_path, body), 'Invented')


def test_duplicate_aliased_key_points_to_repeated_use(tmp_path):
    body = ENTRY.replace('rule: invented rule','? &field rule\n: first\n? *field\n: second')
    path = write(tmp_path, body)
    with pytest.raises(yaml.constructor.ConstructorError, match="duplicate key 'rule'") as caught:
        D._load(path, 'Invented')
    lines = path.read_text().splitlines()
    assert caught.value.problem_mark.line == next(i for i,line in enumerate(lines) if '*field' in line)
    assert caught.value.context_mark.line == next(i for i,line in enumerate(lines) if '&field' in line)


def test_duplicate_merge_directives_are_explicit_members(tmp_path):
    body = ENTRY + 'rows: {<<: {a: one}, <<: {b: two}}\n'
    with pytest.raises(yaml.constructor.ConstructorError, match="duplicate key '<<'"):
        D._load(write(tmp_path, body), 'Invented')


@pytest.mark.parametrize('rows', ["{1: numeric, '1': quoted}",
                                "{<<: [{a: first, s: inherited}, {a: second, b: another}], s: explicit}",
                                "{<<: {a: inherited}, '<<': literal}"])
def test_nonduplicate_keys_and_intentional_merge_precedence_match_scheme_loader(tmp_path, rows):
    path = write(tmp_path, ENTRY+'rows: '+rows+'\n')
    expected = D.DerivationRecord.from_config(KEY, load_yaml(path)[KEY], 'Invented')
    actual = D._load(path, 'Invented')[KEY]
    assert actual == expected and list(actual) == list(expected)
    assert list(actual.rows.items()) == list(expected.rows.items())


def test_whole_record_alias_and_nested_inherited_override_stay_supported(tmp_path):
    path = tmp_path/'derivations.yml'
    path.write_text(KEY+': &base\n'+textwrap.indent(ENTRY,'  ')+
                    'Invented::table::middle: &middle\n  <<: *base\n  columns: [middle]\n'+
                    'Invented::table::last:\n  <<: *middle\n  columns: [last]\n'+
                    'Invented::table::alias: *middle\n')
    old = {k:D.DerivationRecord.from_config(k,v,'Invented') for k,v in load_yaml(path).items()}
    actual = D._load(path,'Invented')
    assert actual == old and list(actual) == list(old)
    assert actual['Invented::table::last'].columns == ['last']
    assert actual['Invented::table::alias'].columns == ['middle']


def test_duplicate_inside_a_merged_anchor_still_refuses(tmp_path):
    path = write(tmp_path, ENTRY+'rows: {<<: &bad {a: first, a: second}}\n')
    with pytest.raises(yaml.constructor.ConstructorError, match="duplicate key 'a'"):
        D._load(path,'Invented')


def test_recursive_value_alias_does_not_loop_or_change_existing_graph(tmp_path):
    path = write(tmp_path,ENTRY+'validated_against: &loop {self: *loop}\n')
    rec = D._load(path,'Invented')[KEY]
    assert rec.validated_against['self'] is rec.validated_against


def test_existing_scheme_tag_remains_supported(tmp_path):
    path = write(tmp_path,ENTRY+'rows: !make {target: invented.parquet}\n')
    assert D._load(path,'Invented')[KEY].rows == load_yaml(path)[KEY]['rows']


@pytest.mark.parametrize('bad', ['[broken', '*undefined', '!!unknown value',
                                '{? [a,b]: value}', 'a: &same 1\nb: &same 2',
                                '---\na: 1\n---\nb: 2'])
def test_nondduplicate_malformed_yaml_keeps_native_refusal_type(tmp_path, bad):
    path = tmp_path/'derivations.yml'; path.write_text(bad)
    with pytest.raises(yaml.YAMLError) as baseline: load_yaml(path)
    with pytest.raises(type(baseline.value)): D._load(path,'Invented')


@pytest.mark.parametrize('text', ['', '{}', 'null', 'false', '[]'])
def test_existing_falsey_empty_documents_stay_empty(tmp_path,text):
    path = tmp_path/'derivations.yml'; path.write_text(text)
    assert D._load(path,'Invented') == {}


def test_missing_file_and_nonmapping_errors_stay_explicit(tmp_path):
    path = tmp_path/'derivations.yml'; assert D._load(path,'Invented') == {}
    path.write_text('[not, empty]')
    with pytest.raises(ValueError,match='top level must be a mapping'):D._load(path,'Invented')
    path.write_bytes(b'\xff')
    with pytest.raises(UnicodeDecodeError):D._load(path,'Invented')


def test_country_cache_clear_and_exception_retry_preserve_contract(tmp_path,monkeypatch):
    path = write(tmp_path); monkeypatch.setattr(D,'derivations_yml_path',lambda _:path)
    first = D.derivation_records('Invented')
    path.write_text(path.read_text()*2)
    assert D.derivation_records('Invented') is first
    D.derivation_records.cache_clear()
    with pytest.raises(yaml.constructor.ConstructorError):D.derivation_records('Invented')
    write(tmp_path)
    assert D.derivation_records('Invented') == first


def test_missing_file_cached_until_explicit_clear(tmp_path,monkeypatch):
    path = tmp_path/'derivations.yml';monkeypatch.setattr(D,'derivations_yml_path',lambda _:path)
    assert D.derivation_records('Invented') == {}
    write(tmp_path);assert D.derivation_records('Invented') == {}
    D.derivation_records.cache_clear();assert KEY in D.derivation_records('Invented')


def test_framework_and_combined_entrypoints_refuse_duplicates(tmp_path,monkeypatch):
    country = tmp_path/'country';country.mkdir(); cp=write(country)
    framework = tmp_path/'framework';framework.mkdir();fp=write(framework,key='::table::framework')
    monkeypatch.setattr(D,'derivations_yml_path',lambda _:cp)
    monkeypatch.setattr(D,'framework_yml_path',lambda:fp)
    assert set(D.records_for('Invented')) == {KEY,'::table::framework'}
    fp.write_text(fp.read_text()*2);D.framework_records.cache_clear()
    with pytest.raises(yaml.constructor.ConstructorError):D.framework_records()
    with pytest.raises(yaml.constructor.ConstructorError):D.records_for('Invented')


def test_generic_loader_and_safe_loader_are_unchanged(tmp_path):
    path=tmp_path/'generic.yml';path.write_text('a: first\na: second\n')
    assert load_yaml(path) == {'a':'second'}
    assert SchemeLoader.construct_document is yaml.SafeLoader.construct_document
    assert yaml.safe_load('a: first\na: second') == {'a':'second'}


@pytest.mark.parametrize('value', ['!!binary SGVsbG8=', '!!pairs [{x: 1}, {x: 2}]',
                                  '!!omap [{x: 1}, {y: 2}]', '!!set {x: null, y: null}',
                                  '2001-01-01'])
def test_nonduplicate_native_yaml_constructors_match_existing_loader(tmp_path, value):
    path = write(tmp_path, ENTRY+'validated_against: '+value+'\n')
    expected = D.DerivationRecord.from_config(KEY,load_yaml(path)[KEY],'Invented')
    actual = D._load(path,'Invented')[KEY]
    assert actual == expected
    assert type(actual.validated_against) is type(expected.validated_against)


def test_merge_tag_preserves_existing_tag_based_handling(tmp_path):
    path = write(tmp_path, ENTRY+'rows:\n  ? !!merge [ignored]\n  : {s: produced}\n')
    assert D._load(path,'Invented')[KEY].rows == load_yaml(path)[KEY]['rows'] == {'s':'produced'}


def test_public_framework_table_function_keeps_its_entrypoints(tmp_path,monkeypatch):
    import lsms_library as ll
    path = write(tmp_path,key='::table::framework')
    monkeypatch.setattr(D,'framework_yml_path',lambda:path)
    monkeypatch.setattr(import_module('lsms_library.catalog'),'countries',lambda:[])
    assert ll.derivations().index.tolist() == ['::table::framework']
    path.write_text(path.read_text()*2);D.framework_records.cache_clear()
    with pytest.raises(yaml.constructor.ConstructorError):ll.derivations()
