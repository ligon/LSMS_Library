"""Remove a phantom source marker without changing Niger's real input closure.

Metadata only, following the existing livestock scanner/collector fixture.
No Country construction, native source read, producer or processing execution.
The c8 whole-source pin makes the inverse independent of candidate processing.
See ledger 1288-niger-plot-input-phantom-reference §§2–5.
"""
import ast
import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from lsms_library.country import Country
from lsms_library.local_tools import scan_script_data_refs, source_fingerprint


COUNTRY = (Path(__file__).resolve().parents[1] / 'lsms_library' /
           'countries' / 'Niger')
SCRIPT = COUNTRY / '2014-15' / '_' / 'plot_inputs.py'
EXPECTED_PATH = '../Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS02CP1.dta'
LEAF = 'ECVMA2_AS02CP1.dta'
SIDECAR = (COUNTRY / '2014-15' / 'Data' /
           'NER_2014_ECVMA-II_v02_M_STATA8' / (LEAF + '.dvc'))
PRIOR_SHA256 = '5dda581782197be20f27ed132ebfd452f04cdc9e11bdbdcb6e1a2307bbb1d21d'
SIDECAR_SHA256 = '01c06d0fc063fc39b66591c6d1a7fc08f6ae2fa15a98684d1380b240a0c90fdb'


def source_pair():
    candidate = SCRIPT.read_text(encoding='utf-8')
    literal = "get_dataframe('" + EXPECTED_PATH + "',"
    assert candidate.count(literal) == 2
    prior = candidate.replace(literal, "get_dataframe(base + 'ECVMA2_AS02CP1.dta',")
    assert hashlib.sha256(prior.encode('utf-8')).hexdigest() == PRIOR_SHA256
    return prior, candidate


def getters(tree):
    return [node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == 'get_dataframe']


def test_only_two_getter_operands_change_whole_source_and_ast():
    prior, candidate = source_pair()
    old_tree, new_tree = ast.parse(prior), ast.parse(candidate)
    old_calls, new_calls = getters(old_tree), getters(new_tree)
    assert len(old_calls) == len(new_calls) == 2
    for old, new in zip(old_calls, new_calls):
        assert isinstance(old.args[0], ast.BinOp)
        assert isinstance(new.args[0], ast.Constant)
        assert new.args[0].value == EXPECTED_PATH
        new.args[0] = copy.deepcopy(old.args[0])
    assert ast.dump(old_tree) == ast.dump(new_tree)
    # The byte inverse above also pins imports, base, comments and provenance.


@pytest.mark.parametrize('version', ['prior', 'candidate'])
def test_getter_paths_kwargs_and_order_match_independent_literal(version):
    text = source_pair()[version == 'candidate']
    assignments = [node for node in ast.parse(text).body
                   if isinstance(node, ast.Assign)
                   and isinstance(node.targets[0], ast.Name)
                   and (node.targets[0].id == 'base'
                        or (isinstance(node.value, ast.Call)
                            and isinstance(node.value.func, ast.Name)
                            and node.value.func.id == 'get_dataframe'))]
    assert [node.targets[0].id for node in assignments] == ['base', 'src', 'srcn']
    calls = []

    def record(*args, **kwargs):
        calls.append((args, kwargs))
        return object()

    # Execute just the three operand assignments, never imports or processing.
    exec(compile(ast.Module(body=assignments, type_ignores=[]),
                 '<Niger getter operands only>', 'exec'), {'get_dataframe': record})
    assert calls == [((EXPECTED_PATH,), {'convert_categoricals': True}),
                     ((EXPECTED_PATH,), {'convert_categoricals': False})]


@pytest.mark.parametrize('version', ['prior', 'candidate'])
def test_actual_scanner_loses_only_bare_leaf(tmp_path, version):
    script = tmp_path / 'plot_inputs.py'
    script.write_text(source_pair()[version == 'candidate'], encoding='utf-8')
    refs = list(scan_script_data_refs(script))
    if version == 'prior':
        assert sorted(refs) == sorted([LEAF, LEAF, EXPECTED_PATH, 'plot_inputs.parquet'])
    else:
        assert sorted(refs) == sorted([EXPECTED_PATH] * 3 + ['plot_inputs.parquet'])
        assert LEAF not in refs


@pytest.mark.parametrize('version', ['prior', 'candidate'])
def test_actual_preparation_collector_removes_phantom_manifest_only(tmp_path, version):
    country = tmp_path / 'Niger'
    wave_path = country / '2014-15'
    script = wave_path / '_' / 'plot_inputs.py'
    script.parent.mkdir(parents=True)
    script.write_text(source_pair()[version == 'candidate'], encoding='utf-8')
    real = (script.parent / EXPECTED_PATH).resolve()
    sidecar = real.with_name(real.name + '.dvc')
    sidecar.parent.mkdir(parents=True)
    sidecar.write_bytes(SIDECAR.read_bytes())
    assert hashlib.sha256(sidecar.read_bytes()).hexdigest() == SIDECAR_SHA256
    assert not real.exists()  # Only metadata is copied, never native bytes.

    class MetadataOnly:
        name = 'Niger'
        file_path = country
        waves = ['2014-15']
        resources = {'Data Scheme': {'plot_inputs': {}}}

        def __getitem__(self, year):
            assert year == '2014-15'
            return SimpleNamespace(file_path=wave_path, resources={})

        def _make_other_references(self, method):
            assert method == 'plot_inputs'
            return {}

        def _make_wave_inputs(self, method):
            assert method == 'plot_inputs'
            return {Path('Niger/2014-15/_/plot_inputs.parquet'): (['2014-15'], False)}

    # Exact existing collector; selected one-wave fixture is not a transaction.
    manifest, sources = Country._make_declared_inputs(
        MetadataOnly(), 'plot_inputs', _with_sources=True)
    phantom = script.parent / LEAF
    if version == 'prior':
        assert sources == {real, phantom}
        assert manifest == {'tables': {}, 'sources': {str(phantom): 'missing:' + LEAF}}
    else:
        assert sources == {real}
        assert phantom not in sources
        assert manifest == {'tables': {}, 'sources': {}}
    assert source_fingerprint(real) == 'dvc:' + LEAF + ':' + SIDECAR_SHA256
    assert sidecar.read_bytes() == SIDECAR.read_bytes()


def test_original_real_sidecar_identity_and_provenance_are_retained():
    assert hashlib.sha256(SIDECAR.read_bytes()).hexdigest() == SIDECAR_SHA256
    assert source_fingerprint(SIDECAR.with_suffix('')) == 'dvc:' + LEAF + ':' + SIDECAR_SHA256
    for source in source_pair():
        calls = [node for node in ast.walk(ast.parse(source))
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                 and node.func.id == '_ecvma_input_observations']
        assert len(calls) == 1
        assert ast.literal_eval(calls[0].args[2]) == EXPECTED_PATH
