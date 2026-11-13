"""GH #1500 source-only regression DRAFTS — UNRUN at delivery.

Collecting or running this file needs a separate task-runtime grant. All numbers
and row identities below are invented; none encodes the issue's historical
counts. No Country/Feature/table API or raw reader is used by these drafts.

Two deliberately bounded tiers:
* Return-boundary tests execute the actual live food_acquired function, extracted
  by AST, with its upstream helper replaced by an invented canonical frame.
  This isolates label repair and dtype/row/attrs conservation, not NSU parity.
* Composition tests keep the real shared size/reshape/value/NSU-stamping code,
  stub _nsu_factor (invented factors), and replace served_unit_labels with a
  tiny declared fake label map. They do NOT validate the real Org tables, NSU
  reads/joins, native-source counts, typed API metadata or cache/Feature behavior.

The AST extraction avoids executing the other mapping-module hooks or importing
the module by path. These drafts themselves have not been imported or executed.
"""
from __future__ import annotations

import ast
from collections import namedtuple
from copy import deepcopy
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library import ehcvm, local_tools
from lsms_library.transformations import U_UNKNOWN


ROOT = Path(__file__).resolve().parents[1]
COUNTRY = ROOT / 'lsms_library' / 'countries' / 'Senegal'
MAPPING = COUNTRY / '2018-19' / '_' / 'mapping.py'
PURCHASE = 'Senegal::food_acquired::7day-purchase-at-last-purchase-unit-value'
NSU = 'Senegal::food_acquired::nsu-national-unit-weight'
BoundaryRecord = namedtuple('BoundaryRecord', 'label axes')


def _live_hook(upstream):
    tree = ast.parse(MAPPING.read_text(encoding='utf-8'))
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name == 'food_acquired']
    assert len(functions) == 1
    namespace = dict(pd=pd, np=np, U_UNKNOWN=U_UNKNOWN,
                     FOOD_ACQUIRED_DERIVATION=PURCHASE,
                     _food_acquired_with_unit_weights=upstream)
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(MAPPING), 'exec'),
         namespace)
    return namespace['food_acquired']


@pytest.fixture(autouse=True)
def forbid_source_reads(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('GH1500 draft crossed its source-free stub boundary')

    monkeypatch.setattr(local_tools, 'get_dataframe', forbidden)
    monkeypatch.setattr(local_tools, 'df_data_grabber', forbidden)
    monkeypatch.setattr(ehcvm, '_nsu_read', forbidden)


def _output(units, source='purchased'):
    """Invented post-helper frame with nullable/categorical unrelated axes."""
    n = len(units)
    households = pd.array(range(n), dtype='Int64')
    clusters = ['invented-cluster'] * n
    if n:
        households[-1] = pd.NA
        clusters[0] = None
    idx = pd.MultiIndex.from_arrays([
        pd.Index(pd.array(['2018-19'] * n, dtype='string')),
        pd.CategoricalIndex(clusters,
                            categories=['unused-cluster', 'invented-cluster'],
                            ordered=True),
        pd.Index(households),
        pd.CategoricalIndex(['invented-item'] * n,
                            categories=['invented-item', 'unused-item']),
        units,
        pd.CategoricalIndex([source] * n,
                            categories=['purchased', 'produced', 'inkind', 'other']),
    ], names=['t', 'v', 'i', 'j', 'u', 's'])
    def repeated(values):
        return [values[k % len(values)] for k in range(n)]
    out = pd.DataFrame({
        'Quantity': pd.array(repeated([7.0, 0.0, -3.0, None]), dtype='Float64'),
        'Expenditure': pd.array(repeated([None, 0.0, -8.0, 16.0]), dtype='Float64'),
        'Quantity_kg': pd.array(repeated([1.75, None, -0.75, 0.0]), dtype='Float64'),
        'Derivation': pd.array(repeated([PURCHASE + '+' + NSU, None, NSU, PURCHASE]),
                               dtype='string'),
    }, index=idx)
    # A typed synthetic carrier, explicitly NOT real Country/Feature metadata.
    out.attrs = {'id_converted': True,
                 'typed_boundary': BoundaryRecord('invented', ('t', 'u')),
                 'nested': {'source': ['synthetic-return-boundary']}}
    return out


def _assert_only_u(got, before, expected_units):
    pd.testing.assert_frame_equal(got.droplevel('u'), before.droplevel('u'),
                                  check_exact=True)
    assert got.index.names == before.index.names
    assert got.index.get_level_values('u').tolist() == expected_units
    for position, name in enumerate(before.index.names):
        if name != 'u':
            pd.testing.assert_index_equal(got.index.levels[position],
                                          before.index.levels[position], exact=True)
            np.testing.assert_array_equal(got.index.codes[position],
                                          before.index.codes[position])
    assert got.attrs == before.attrs
    assert type(got.attrs.get('typed_boundary')) is type(before.attrs.get('typed_boundary'))


def _boundary(upstream_output):
    incoming = object()
    calls = []
    def upstream(df, key, *, wave):
        assert df is incoming
        assert key == PURCHASE and wave == '2018-19'
        calls.append(wave)
        return upstream_output
    return _live_hook(upstream), incoming, calls


@pytest.mark.parametrize('missing', [None, float('nan'), pd.NA])
@pytest.mark.parametrize('source', ['purchased', 'produced', 'inkind'])
def test_missing_label_only_preserves_payload_order_axes_and_typed_boundary(missing, source):
    original = _output(pd.Index([missing, 'Kg', 'Tas (Petit)', 'Unknown'], dtype=object), source)
    before = deepcopy(original)
    hook, incoming, calls = _boundary(original)
    got = hook(incoming)
    assert U_UNKNOWN == 'Unknown'
    _assert_only_u(got, before, ['Unknown', 'Kg', 'Tas (Petit)', 'Unknown'])
    assert calls == ['2018-19']


def test_present_codes_tokens_and_mojibake_labels_are_never_reclassified():
    units = ['nan', 'None', '', ' ', 0, 659, '659', 'Manquant', 'unitÃ©',
             'Unité (Moyen)', 'BoÃ®te', 'Kg', 'Value', 'Unknown']
    original = _output(pd.Index(units, dtype=object))
    before = deepcopy(original)
    hook, incoming, _ = _boundary(original)
    got = hook(incoming)
    _assert_only_u(got, before, units)
    pd.testing.assert_frame_equal(got, before, check_exact=True)


@pytest.mark.parametrize('unknown_in_categories', [False, True])
def test_categorical_unit_adds_or_reuses_an_unused_unknown_category(unknown_in_categories):
    categories = ['Kg', 'Tas (Petit)', 'unused-unit']
    if unknown_in_categories:
        categories.append('Unknown')
    units = pd.CategoricalIndex([None, 'Kg', 'Tas (Petit)', None],
                                categories=categories, ordered=True)
    original = _output(units)
    before = deepcopy(original)
    hook, incoming, _ = _boundary(original)
    got = hook(incoming)
    _assert_only_u(got, before, ['Unknown', 'Kg', 'Tas (Petit)', 'Unknown'])
    level = got.index.levels[got.index.names.index('u')]
    assert isinstance(level, pd.CategoricalIndex) and level.ordered
    assert level.categories.tolist() == ['Kg', 'Tas (Petit)', 'unused-unit', 'Unknown']


def test_empty_output_preserves_every_dtype_level_and_attribute():
    original = _output(pd.Index([], dtype=object))
    before = deepcopy(original)
    hook, incoming, _ = _boundary(original)
    pd.testing.assert_frame_equal(hook(incoming), before, check_exact=True)
    assert original.attrs == before.attrs


def _same_key_output(units):
    out = _output(pd.Index(units, dtype=object))
    out.index = pd.MultiIndex.from_tuples([
        ('2018-19', 'invented-cluster', 'invented-household', 'invented-item', u, 'purchased')
        for u in units], names=out.index.names)
    return out


def test_preexisting_duplicate_full_keys_are_preserved_without_reduction():
    original = _same_key_output([None, None, None, 'Kg'])
    before = deepcopy(original)
    hook, incoming, _ = _boundary(original)
    got = hook(incoming)
    _assert_only_u(got, before, ['Unknown', 'Unknown', 'Unknown', 'Kg'])
    assert got.index.duplicated(keep=False).tolist() == [True, True, True, False]


@pytest.mark.parametrize('units', [[None, 'Unknown'],
                                   [None, None, 'Unknown', 'Unknown']])
def test_new_full_key_collision_preserves_all_rows_even_if_old_keys_repeat(units):
    original = _same_key_output(units)
    before = deepcopy(original)
    incoming = _output(pd.Index(['Kg', None], dtype=object))
    input_before = deepcopy(incoming)
    def upstream(df, key, *, wave):
        assert df is incoming
        assert key == PURCHASE and wave == '2018-19'
        return deepcopy(original)
    got = _live_hook(upstream)(incoming)
    _assert_only_u(got, before, ['Unknown'] * len(units))
    assert got.shape == before.shape
    assert got.index.duplicated(keep=False).tolist() == [True] * len(units)
    pd.testing.assert_frame_equal(original, before, check_exact=True)
    assert original.attrs == before.attrs
    pd.testing.assert_frame_equal(incoming, input_before, check_exact=True)
    assert incoming.attrs == input_before.attrs


def _wide(unit='Tas', size='Petit', **changes):
    idx = pd.MultiIndex.from_arrays([
        ['2018-19'], ['invented-cluster'], [1], ['invented-household'],
        ['invented-item'], pd.Index([unit], dtype=object)],
        names=['t', 'v', 'visit', 'i', 'j', 'u'])
    values = dict(Quantity=12.0, Produced=3.0, GiftQuantity=2.0,
                  ConsumptionUnitCode=unit, ConsumptionUnitSize=size,
                  LastPurchaseQuantity=2.0, LastPurchaseUnitCode=unit,
                  LastPurchaseUnitSize=size, LastPurchaseValue=80.0)
    values.update(changes)
    return pd.DataFrame({k: [v] for k, v in values.items()}, index=idx)


def _composition(monkeypatch, wide, factor=np.nan):
    """Actual shared pipeline; only label lookup and source-dependent factor stubbed."""
    before = deepcopy(wide)
    calls = []
    def factors(df, country, wave, decode=None):
        assert country == 'Senegal' and wave == '2018-19' and decode is None
        pd.testing.assert_frame_equal(df, before, check_exact=True)
        calls.append((country, wave))
        return np.full(len(df), factor)
    fake_labels = {'unitÃ©': 'Unité', 'BoÃ®te': 'Boîte'}
    def labels(country, values):
        assert country == 'Senegal'
        return np.asarray([fake_labels.get(v, v) for v in values], dtype=object)
    monkeypatch.setattr(ehcvm, '_nsu_factor', factors)
    monkeypatch.setattr(ehcvm, 'served_unit_labels', labels)
    hook = _live_hook(ehcvm.food_acquired_with_unit_weights)
    got = hook(wide)
    base = ehcvm.food_acquired_with_unit_weights(before.copy(), PURCHASE, wave='2018-19')
    pd.testing.assert_frame_equal(wide, before, check_exact=True)
    assert wide.attrs == before.attrs
    assert calls == [('Senegal', '2018-19'), ('Senegal', '2018-19')]
    return got, base


@pytest.mark.parametrize('missing', [None, float('nan'), pd.NA])
@pytest.mark.parametrize('size', ['Moyen', None, 'Taille unique'])
def test_missing_base_with_size_or_last_purchase_stays_exact_unknown(monkeypatch, missing, size):
    wide = _wide(missing, size, LastPurchaseUnitCode='Kg', LastPurchaseUnitSize='Grand')
    got, base = _composition(monkeypatch, wide)
    _assert_only_u(got, base, ['Unknown', 'Unknown', 'Unknown'])
    assert got.index.get_level_values('s').tolist() == ['purchased', 'produced', 'inkind']
    assert got['Quantity'].tolist() == [7.0, 3.0, 2.0]
    assert got['Expenditure'].isna().tolist() == [True, True, True]
    assert got['Quantity_kg'].isna().tolist() == [True, True, True]
    assert got['Derivation'].iloc[0] == PURCHASE
    assert got['Derivation'].iloc[1:].isna().all()


@pytest.mark.parametrize('unit,size,want', [
    ('Tas', 'Petit', 'Tas (Petit)'), ('Tas', None, 'Tas'),
    ('unitÃ©', 'Moyen', 'Unité (Moyen)'),
    ('BoÃ®te', 'Petit', 'Boîte (Petit)'),
    ('unitÃ©', 'Taille unique', 'unitÃ©'),  # bare mapping remains a later API step
    ('Kg', 'Moyen', 'Kg'), ('659', 'Taille unique', '659'),
    ('nan', 'Taille unique', 'nan'), ('None', 'Taille unique', 'None'),
    ('', 'Taille unique', ''), (0, 'Taille unique', 0),
])
def test_existing_size_and_label_behavior_is_preserved_under_fake_label_map(monkeypatch, unit, size, want):
    got, base = _composition(monkeypatch, _wide(unit, size))
    _assert_only_u(got, base, [want, want, want])
    pd.testing.assert_frame_equal(got, base, check_exact=True)
    assert got['Quantity'].tolist() == [7.0, 3.0, 2.0]


def test_survey_kilograms_and_derivation_stamp_keep_literal_invented_values(monkeypatch):
    got, base = _composition(monkeypatch, _wide(), factor=0.25)
    pd.testing.assert_frame_equal(got, base, check_exact=True)
    assert got['Quantity'].tolist() == [7.0, 3.0, 2.0]
    assert got['Expenditure'].iloc[0] == 280.0
    assert got['Expenditure'].iloc[1:].isna().all()
    assert got['Quantity_kg'].tolist() == [1.75, 0.75, 0.5]
    assert got['Derivation'].tolist() == [PURCHASE + '+' + NSU, NSU, NSU]


@pytest.mark.parametrize('changes', [
    {'LastPurchaseUnitCode': 'Kg'}, {'LastPurchaseUnitSize': 'Grand'},
    {'LastPurchaseQuantity': 0.0}, {'LastPurchaseQuantity': -2.0},
    {'LastPurchaseQuantity': np.nan},
])
def test_original_scalar_answers_still_control_value_eligibility(monkeypatch, changes):
    got, base = _composition(monkeypatch, _wide(**changes))
    pd.testing.assert_frame_equal(got, base, check_exact=True)
    assert got['Quantity'].tolist() == [7.0, 3.0, 2.0]
    assert got['Expenditure'].isna().all()


@pytest.mark.parametrize('quantity', [0.0, -2.0, np.nan])
def test_upstream_nonpositive_or_missing_eligibility_is_not_changed(monkeypatch, quantity):
    got, base = _composition(monkeypatch, _wide(None, 'Moyen', Quantity=quantity,
                                              Produced=0.0, GiftQuantity=0.0))
    pd.testing.assert_frame_equal(got, base, check_exact=True)
    assert len(got) == 0


def test_live_yaml_and_2021_binding_remain_source_exact():
    protected = {
        '2018-19/_/data_info.yml': '6699bceaf41a0d213ab5666502f96471714809d089a2e429a6564a431a3b6de7',
        # Re-pinned 2026-11-12: the salt-per-capita rule (ruling @ligon 2026-11-12)
        # adds its hook to the 2021-22 wave.  Its selector reads s07b alone (red team
        # F1), so 2021-22's data_info.yml is development's again, byte for byte.
        '2021-22/_/data_info.yml': '2522506789ff4584a08c37454eb774cb99189ade93a9e7fdd38351913f913ebc',
        '2021-22/_/mapping.py': '29f39839d100bfec4164e14d420af0367a2a8f9c95939618ca97dd5a8c9a3a60',
        # Re-pinned 2026-11-13 (GH #1118 / #1119, stream manquant): the plot_inputs
        # comment in data_scheme.yml now names the Unknown fill, and
        # harmonize_seed_crop maps maize / cowpea seed to crop_production's 'Maïs'
        # / 'Niébé'.  Neither touches the food_acquired u path this test guards.
        # The scheme digest moved again (comments only) when the plot_inputs
        # comment stopped calling the 2018-19 gift answers a purchase, and when
        # 2018-19 began reading the purchase (ruling "Extend to all four now").
        '_/data_scheme.yml': '45e9a5fe58ea0d3d4ce91b3873546ef2bf7fc6233b29495fc09ab3f5ef14718c',
        '_/categorical_mapping.org': '24fe7f2d0b79a64578d4dffdc1861ac1b9f1434afca766b2cac26dae53f5c8c7',
    }
    for rel, digest in protected.items():
        assert hashlib.sha256((COUNTRY / rel).read_bytes()).hexdigest() == digest, rel
    text = (COUNTRY / '2018-19' / '_' / 'data_info.yml').read_text(encoding='utf-8')
    block = text.split('\nfood_acquired:\n', 1)[1].split('\nassets:', 1)[0]
    assert '    file: s07b_me_sen2018.dta\n' in block
    assert '        u: s07bq03b\n' in block
    assert '        ConsumptionUnitCode: s07bq03b\n' in block
    assert '        ConsumptionUnitSize: s07bq03c\n' in block
    assert '        LastPurchaseUnitCode: s07bq07b\n' in block
    assert '        LastPurchaseUnitSize: s07bq07c\n' in block


def test_all_other_mapping_symbols_and_imports_keep_the_base_ast():
    """Protect all non-food hooks, including scalar formatters and community prices."""
    tree = ast.parse(MAPPING.read_text(encoding='utf-8'))
    assert sum(isinstance(n, ast.ImportFrom)
               and n.module == 'lsms_library.transformations'
               and [(a.name, a.asname) for a in n.names] == [('U_UNKNOWN', None)]
               for n in tree.body) == 1
    body = [n for n in tree.body
            if not (isinstance(n, ast.FunctionDef) and n.name == 'food_acquired')
            and not (isinstance(n, ast.ImportFrom)
                     and n.module == 'lsms_library.transformations'
                     and [(a.name, a.asname) for a in n.names] == [('U_UNKNOWN', None)])]
    normal = ast.dump(ast.Module(body=body, type_ignores=[]), include_attributes=False)
    # Base-source AST identity, not a data fixture or generated output baseline.
    # Re-pinned 2026-11-12 for GH #1519: the shared hook is imported as
    # `_shared_food_acquired_with_unit_weights` and wrapped by a module-level
    # `_food_acquired_with_unit_weights` plus the near-zero helpers; `food_acquired`
    # itself is unchanged.
    assert hashlib.sha256(normal.encode()).hexdigest() == 'da0f54bd5fec7d825c56b02d437cd0cdea1422f79de4f424b3968aba580c8834'
