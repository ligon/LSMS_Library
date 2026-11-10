"""Frozen invented-frame controls for the own2021 s16b candidate; no LSMS IO.

Run this file only, with --noconftest, after the dispatcher admits verification.
No source records, Country/Feature, producer, parquet or provider is needed.
Expected frames are literal, not obtained by calling the candidate twice.
"""
import ast
import builtins
from copy import deepcopy
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
COUNTRY = ROOT / 'lsms_library' / 'countries' / 'Burkina_Faso'
MODULE = COUNTRY / '_' / 's16b_2021.py'
RAW = ['grappe', 'menage', 'vague', 's16bq01', 's16bq02', 's16bq03a',
       's16bq03b', 's16bq05', 's16bq07a', 's16bq07b', 's16bq08',
       's16bq09a', 's16bq09b', 's16bq09c']
MENU = ["Engrais organiques - déchets d'animaux",
        'Engrais organiques - Ordures ménagères',
        'Engrais inorganiques - Urée', 'Engrais inorganiques - Phosphates',
        'Engrais inorganiques - NPK/Formule unique',
        'DAP et autres engrais inorganiques',
        'Produits phytosanitaires - Pesticides',
        'Produits phytosanitaires - Herbicides',
        'Produits phytosanitaires -Fongicides', 'Autres produits phytosanitaires',
        'Semences de petit mil', 'Semences de sorgho', 'Semences de maïs',
        'Semences de riz', "Semences d'autres céréales", 'Semences de coton',
        'Semences de césame', 'Semences de haricots/niébé',
        "Semences d'arichide", 'Plants/boutures de tubercules',
        "Plants d'hévéa, de teck", 'Autres semences']
UNITS = ['Gramme', 'Kilogramme', 'Tonne', 'Litre', 'Charrette', 'Sachet',
         'Tiya', 'Sac (50 kg)', 'Sac (25 kg)', 'Sac (100 kg)', 'Boîte', 'Tas',
         'Charrette asine', 'Charrette bovine', 'Seau', 'Panier', 'Camion benne',
         'Yorouba', 'Demi-yorouba', 'Quart-yorouba', 'Tricycle', 'Brouette']


def _table(path, name, key, value):
    """Independent small Org reader for literal fixtures; never a provider."""
    lines = path.read_text().splitlines()
    start = next(i for i, line in enumerate(lines)
                 if line.lower() == '#+name: ' + name.lower())
    rows = []
    for line in lines[start + 1:]:
        if not line.startswith('|'):
            break
        if set(line) <= set('|+- '):
            continue
        rows.append([cell.strip() for cell in line.strip('|').split('|')])
    columns = rows.pop(0)
    return {row[columns.index(key)]: row[columns.index(value)] for row in rows}


def _extract(path, name, namespace):
    """Execute one actual production helper definition, excluding all imports/IO."""
    tree = ast.parse(path.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    # These two existing helpers have no non-pandas/provider dependencies.
    for call in (n for n in ast.walk(node) if isinstance(n, ast.Call)):
        if isinstance(call.func, ast.Name):
            assert call.func.id in {'TypeError', 'ValueError'}  # normally no bare calls
        elif isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name):
            assert call.func.value.id in {'pd', 'tools', 's', 'head', 'tail', 'id'}
    code = ast.Module(body=[ast.ImportFrom(module='__future__',
                                         names=[ast.alias(name='annotations')], level=0), node],
                      type_ignores=[])
    exec(compile(ast.fix_missing_locations(code), str(path), 'exec'), namespace)
    return namespace[name]


@pytest.fixture(autouse=True)
def no_provider_imports(monkeypatch):
    original = builtins.__import__

    def checked(name, *args, **kwargs):
        assert name.split('.')[0] not in {
            'lsms_library', 'ligonlibrary', 'dvc', 's3fs', 'boto3', 'botocore',
            'pyreadstat', 'requests', 'urllib3',
        }, 'provider/package import attempted: ' + name
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', checked)


@pytest.fixture
def adapter():
    spec = importlib.util.spec_from_file_location('bfa_1483_candidate', MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def bindings():
    # Actual current formatters, not a test reimplementation of household IDs.
    format_id = _extract(ROOT / 'lsms_library' / 'local_tools.py', 'format_id', {'pd': pd})
    country_i = _extract(COUNTRY / '_' / 'burkina_faso.py', 'i',
                         {'tools': SimpleNamespace(format_id=format_id)})
    path = COUNTRY / '_' / 's16b_2021_cards.org'
    cards = {field: _table(path, 'bfa2021_' + field, 'Code', 'Label')
             for field in ['s16bq01', 's16bq02', 's16bq05', 's16bq08',
                           's16bq03b', 's16bq07b', 's16bq09b']}
    path = COUNTRY / '_' / 'categorical_mapping.org'
    maps = {name: _table(path, name, 'Original Label', 'Preferred Label')
            for name in ['harmonize_input', 'harmonize_seed_crop', 'u']}
    return dict(cards=cards, input_map=maps['harmonize_input'],
                seed_crop_map=maps['harmonize_seed_crop'], unit_map=maps['u'],
                id_fn=country_i), format_id


def _source():
    # The last row is explicitly NOT used. Distinct indices exercise positional
    # alignment; no native household ID or quantity is taken from source data.
    return pd.DataFrame({
        'grappe': [2] * 5, 'menage': [126, 126, 126, 126, 127],
        'vague': [1, 1, 1, 1, 2], 's16bq01': [3, 19, 21, 99, 3],
        's16bq02': [1, 1, 1, 1, 2],
        's16bq03a': [1.25, 0., -3., None, 90.],
        's16bq03b': [2, 9, None, 99, 1],
        's16bq05': [1, 2, None, 1, 1],
        's16bq07a': [2.5, None, 4., 0., 7.],
        's16bq07b': [4, 22, None, 2, 5],
        's16bq08': [2, 1, None, 2, 1],
        's16bq09a': [111., 222., None, -9., 999.],
        's16bq09b': [5, 8, None, 99, 3],
        's16bq09c': [11100, 22200, 33300, -900, 99900],
    }, index=[40, 10, 90, 20, 70])


def _adapt(adapter, bindings, source):
    return adapter.adapt_s16b_2021(source, **bindings[0])


def test_raw_answers_keep_no_blank_unknown_and_typed_attrs_before_public_gate(adapter, bindings):
    source = _source()
    source['s16bq02'] = [1., 2., None, 9., 2.]
    source.attrs = {'raw': ('unclassified', False, 7)}
    expected = source.copy(deep=True)
    got = adapter.raw_s16b_2021(source)
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    assert got['s16bq02'].isna().tolist() == [False, False, True, False, False]
    assert got.index.tolist() == [40, 10, 90, 20, 70]
    assert got.attrs == {'raw': ('unclassified', False, 7)}
    assert type(got.attrs['raw']) is tuple
    # Only the literal positive use answer denotes an application; No,
    # missing and unlisted answers remain distinct in the raw projection.
    public = adapter.adapt_s16b_2021(source, **bindings[0])
    assert public['s16bq02'].tolist() == [1.]
    assert public['Quantity'].tolist() == [1.25]
    assert public.attrs['s16b_2021_candidate']['excluded_explicit_nonuse'] == 2


def test_complete_literal_frame_keys_types_masks_and_raw_answers(adapter, bindings):
    source = _source()
    source.attrs = {'invented': {'integer': 7, 'tuple': ('kept', None), 'flag': False}}
    before = source.copy(deep=True)
    got = _adapt(adapter, bindings, source)
    expected = pd.DataFrame({
        't': pd.array(['2021-22'] * 4, dtype='string'),
        'i': pd.array(['2126'] * 4, dtype='string'),
        'input': pd.array(['Urea', 'Seed', "Plants d'hévéa, de teck",
                           'Unknown [Burkina_Faso 2021-22 s16bq01=99]'], dtype='string'),
        'crop': pd.array(['(not crop-specific)', 'Arachide', '(not crop-specific)',
                          '(not crop-specific)'], dtype='string'),
        'u': pd.array(['Kilogramme', 'Sac (25 kg)', 'Manquant',
                       'Unknown [Burkina_Faso 2021-22 s16bq03b=99]'], dtype='string'),
        'Quantity': pd.array([1.25, 0., -3., None], dtype='Float64'),
        'Purchased': pd.array([False, True, None, False], dtype='boolean'),
        'Quantity_purchased': pd.array([111., 222., None, -9.], dtype='Float64'),
        'Unit_purchased': pd.array(['Charrette', 'Sac (50 kg)', None,
                                    'Unknown [Burkina_Faso 2021-22 s16bq09b=99]'], dtype='string'),
        'Received_as_gift': pd.array([True, False, None, True], dtype='boolean'),
        'Quantity_received_as_gift': pd.array([2.5, None, 4., 0.], dtype='Float64'),
        'Unit_received_as_gift': pd.array(['Litre', 'Brouette', None, 'Kilogramme'], dtype='string'),
        'grappe': [2] * 4, 'menage': [126] * 4, 'vague': [1] * 4,
        's16bq01': [3, 19, 21, 99], 's16bq02': [1] * 4,
        's16bq03a': [1.25, 0., -3., None], 's16bq03b': [2., 9., None, 99.],
        's16bq05': [1., 2., None, 1.], 's16bq07a': [2.5, None, 4., 0.],
        's16bq07b': [4., 22., None, 2.], 's16bq08': [2., 1., None, 2.],
        's16bq09a': [111., 222., None, -9.], 's16bq09b': [5., 8., None, 99.],
        's16bq09c': [11100, 22200, 33300, -900],
    }).set_index(['t', 'i', 'input', 'crop', 'u'])
    expected.attrs = {
        'invented': {'integer': 7, 'tuple': ('kept', None), 'flag': False},
        's16b_2021_candidate': {
            'excluded_explicit_nonuse': 1,
            'purchase_quantity': 's16bq09a; own q09b Unité / q09c Montant',
            'component': 'panel source only; no Cross_Section coverage claim',
        },
    }
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    pd.testing.assert_frame_equal(source, before, check_exact=True)
    assert got.attrs == expected.attrs
    assert type(got.attrs['invented']['tuple']) is tuple
    assert type(got.attrs['invented']['flag']) is bool
    assert type(got.attrs['s16b_2021_candidate']['excluded_explicit_nonuse']) is int
    assert got['Quantity'].isna().tolist() == [False, False, False, True]
    assert got['Quantity_purchased'].isna().tolist() == [False, False, True, False]
    assert got['Quantity_purchased'].tolist()[:2] == [111., 222.]
    assert got['s16bq09c'].tolist() == [11100, 22200, 33300, -900]
    assert got.index.is_unique and 'plot_id' not in got.index.names
    assert 'id_converted' not in got.attrs  # exactly-once walk belongs downstream


def test_cards_equal_independent_full_menus_and_own_attached_flags(bindings):
    cards = bindings[0]['cards']
    assert cards['s16bq01'] == {str(i): label for i, label in enumerate(MENU, 1)}
    for field in ['s16bq03b', 's16bq07b', 's16bq09b']:
        assert cards[field] == {str(i): label for i, label in enumerate(UNITS, 1)}
    for field in ['s16bq02', 's16bq05', 's16bq08']:
        assert cards[field] == {'1': 'Oui', '2': 'Non'}


def test_all_22_slots_keep_own_identities_and_tail_not_2018(adapter, bindings):
    source = pd.concat([_source().iloc[[0]]] * 22, ignore_index=True)
    source['s16bq01'] = list(range(1, 23))
    got = _adapt(adapter, bindings, source)
    assert got['s16bq01'].tolist() == list(range(1, 23))
    assert got.index.get_level_values('input').tolist() == [
        'Organic manure', 'Organic compost', 'Urea', 'Phosphate', 'NPK',
        'DAP et autres engrais inorganiques',
        'Pesticide', 'Herbicide', 'Fungicide', 'Other phytosanitary'] + ['Seed'] * 10 + [
        "Plants d'hévéa, de teck", 'Seed']
    assert got.index.get_level_values('crop').tolist() == ['(not crop-specific)'] * 10 + [
        'Mil', 'Sorgho', 'Maïs', 'Riz Paddy', 'Autre céréale', 'Coton', 'Sésame',
        'Niébé', 'Arachide', 'Autre tubercule', '(not crop-specific)', 'Autre crop']
    assert len(got) == 22 and got.index.is_unique


def test_code6_retains_full_catchall_despite_legacy_dap_map(adapter, bindings):
    source = _source().iloc[[0]].copy()
    source['s16bq01'] = 6
    assert bindings[0]['input_map']['DAP et autres engrais inorganiques'] == 'DAP'
    got = _adapt(adapter, bindings, source)
    assert got.index.get_level_values('input').tolist() == ['DAP et autres engrais inorganiques']
    assert got['s16bq01'].tolist() == [6]
    assert got['Quantity'].tolist() == [1.25]
    assert not any('nitrogen' in name.lower() or 'grade' in name.lower() for name in got.columns)
    # Actual current normalizer tries exact names, never a DAP substring.
    path = ROOT / 'lsms_library' / 'transformations.py'
    tree = ast.parse(path.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                and n.name == '_normalise_input_label')
    namespace = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
    assert namespace['_normalise_input_label']('DAP et autres engrais inorganiques') == [
        'dap et autres engrais inorganiques', 'dap et autres engrais inorganiques fertilizer']
    assert not {'dap', 'dap fertilizer'}.intersection(
        namespace['_normalise_input_label']('DAP et autres engrais inorganiques'))


def test_each_unit_uses_own_card_and_exact_curated_label(adapter, bindings):
    args = deepcopy(bindings[0])
    args['cards']['s16bq03b']['2'] = 'Invented used unit'
    args['cards']['s16bq07b']['4'] = 'Invented gift unit'
    args['cards']['s16bq09b']['5'] = 'Invented purchased unit'
    args['unit_map'] = {'Invented gift unit': 'Curated gift unit'}
    got = adapter.adapt_s16b_2021(_source().iloc[[0]], **args)
    assert got.index.get_level_values('u').tolist() == ['Invented used unit']
    assert got['Unit_received_as_gift'].tolist() == ['Curated gift unit']
    assert got['Unit_purchased'].tolist() == ['Invented purchased unit']


@pytest.mark.parametrize('field', ['s16bq03b', 's16bq07b', 's16bq09b'])
def test_all_22_units_retain_literal_sizes_without_conversion(adapter, bindings, field):
    source = pd.concat([_source().iloc[[0]]] * 22, ignore_index=True)
    source['menage'] = list(range(1, 23))
    source[field] = list(range(1, 23))
    got = _adapt(adapter, bindings, source)
    actual = (got.index.get_level_values('u').tolist() if field == 's16bq03b'
              else got['Unit_received_as_gift' if field == 's16bq07b' else 'Unit_purchased'].tolist())
    assert actual == UNITS
    assert got['Quantity'].tolist() == [1.25] * 22


@pytest.mark.parametrize('field,value,message', [
    ('s16bq05', 9, 'unknown flag'), ('s16bq08', 9, 'unknown flag'),
    ('grappe', None, 'missing native'), ('menage', None, 'missing native'),
    ('s16bq01', None, 'missing native'), ('s16bq01', 1.5, 'integral numeric'),
    ('s16bq03a', 'bad amount', 'numeric source amounts'),
    ('s16bq07a', 'bad amount', 'numeric source amounts'),
    ('s16bq09a', 'bad amount', 'numeric source amounts'),
    ('s16bq09b', 3.5, 'integral numeric'),
])
def test_bad_sources_refuse_without_silent_drop_or_recode(adapter, bindings, field, value, message):
    source = _source().iloc[[0]].copy()
    source[field] = value
    with pytest.raises(adapter.S16b2021Error, match=message):
        adapter.adapt_s16b_2021(source, **bindings[0])


def test_unknown_unit_and_flags_never_fill_one_another(adapter, bindings):
    source = _source().iloc[[0]].copy()
    source.loc[:, ['s16bq05', 's16bq08', 's16bq07a', 's16bq07b', 's16bq09b']] = None
    got = _adapt(adapter, bindings, source)
    assert got['Quantity'].iloc[0] == 1.25
    assert got[['Purchased', 'Received_as_gift', 'Quantity_received_as_gift',
                'Unit_received_as_gift', 'Unit_purchased']].isna().all(axis=None)
    assert got['s16bq09a'].iloc[0] == got['Quantity_purchased'].iloc[0] == 111.
    assert got['s16bq09c'].iloc[0] == 11100  # never the purchased quantity


def test_native_duplicates_and_canonical_equal_payload_collisions_refuse(adapter, bindings):
    duplicate = pd.concat([_source().iloc[[0]]] * 2, ignore_index=True)
    with pytest.raises(adapter.S16b2021Error, match='duplicate native'):
        adapter.adapt_s16b_2021(duplicate, **bindings[0])
    source = duplicate.copy()
    source['s16bq01'] = [15, 22]
    args = deepcopy(bindings[0])
    args['seed_crop_map'].update({"Semences d'autres céréales": 'Autre crop'})
    with pytest.raises(adapter.S16b2021Error, match='identities collide'):
        adapter.adapt_s16b_2021(source, **args)


@pytest.mark.parametrize('field', RAW)
def test_missing_declared_column_refuses(adapter, bindings, field):
    with pytest.raises(adapter.S16b2021Error, match='missing source columns'):
        adapter.adapt_s16b_2021(_source().drop(columns=field), **bindings[0])


def test_walked_source_and_missing_or_changed_cards_refuse(adapter, bindings):
    source = _source()
    source.attrs['id_converted'] = True
    with pytest.raises(adapter.S16b2021Error, match='already walked'):
        adapter.adapt_s16b_2021(source, **bindings[0])
    args = deepcopy(bindings[0])
    args['cards']['s16bq09b'] = {}
    with pytest.raises(adapter.S16b2021Error, match='own card unavailable'):
        adapter.adapt_s16b_2021(_source(), **args)
    args['cards']['s16bq09b'] = {'1': 'Gramme'}
    args['cards']['s16bq02'] = {'1': 'Non', '2': 'Oui'}
    with pytest.raises(adapter.S16b2021Error, match='unexpected own flag card'):
        adapter.adapt_s16b_2021(_source(), **args)


def test_only_explicit_nonuse_is_excluded_with_typed_empty_result(adapter, bindings):
    source = _source().iloc[[4]]
    got = _adapt(adapter, bindings, source)
    assert len(got) == 0
    assert got.index.names == ['t', 'i', 'input', 'crop', 'u']
    assert got['Quantity'].dtype == pd.Float64Dtype()
    assert got['Purchased'].dtype == pd.BooleanDtype()
    assert got['Unit_purchased'].dtype == pd.StringDtype()
    assert got.attrs['s16b_2021_candidate']['excluded_explicit_nonuse'] == 1


def test_driver_reads_only_own_numeric_source_and_writes_after_validation(adapter, bindings):
    calls, writes = [], []

    def read(path, **kwargs):
        calls.append(('read', path, kwargs))
        assert path == '../Data/s16b_me_bfa2021.dta'
        assert kwargs == {'convert_categoricals': False}
        return _source()

    def get_map(*, tablename, idxvars, **kwargs):
        calls.append(('map', tablename))
        if tablename.startswith('bfa2021_'):
            assert kwargs == {'fn': 's16b_2021_cards.org', 'dirs': ['../../_/'], 'Label': 'Label'}
            assert idxvars == 'Code'
            return bindings[0]['cards'][tablename[len('bfa2021_'):]]
        assert idxvars == 'Original Label' and kwargs == {'Preferred Label': 'Preferred Label'}
        return bindings[0][{'harmonize_input': 'input_map',
                            'harmonize_seed_crop': 'seed_crop_map', 'u': 'unit_map'}[tablename]]

    def write(frame, path):
        assert path == 'plot_inputs.parquet'
        assert frame.index.is_unique
        calls.append(('write', path))
        writes.append(frame.copy())

    # Execute the actual serving wrapper's final call with only injected leaves.
    # No package imports, sys.path changes or source-provider code executes.
    wrapper = COUNTRY / '2021-22' / '_' / 'plot_inputs.py'
    call = ast.parse(wrapper.read_text()).body[-1]
    assert isinstance(call, ast.Expr) and isinstance(call.value, ast.Call)
    assert isinstance(call.value.func, ast.Name) and call.value.func.id == 'build_s16b_candidate'
    source_arg = next(item.value for item in call.value.keywords if item.arg == 'source_path')
    assert isinstance(source_arg, ast.Constant)
    assert source_arg.value == '../Data/s16b_me_bfa2021.dta'
    namespace = dict(build_s16b_candidate=adapter.build_s16b_candidate,
                     get_dataframe=read, to_parquet=write, get_categorical_mapping=get_map,
                     i=bindings[0]['id_fn'], format_id=bindings[1])
    exec(compile(ast.Module(body=[call], type_ignores=[]), str(wrapper), 'exec'), namespace)
    got = writes[0]
    assert len(writes) == 1
    assert got.index.get_level_values('i').tolist() == ['2126'] * 4
    assert got['s16bq01'].tolist() == [3, 19, 21, 99]
    assert got['Quantity_purchased'].isna().tolist() == [False, False, True, False]
    assert got['Quantity_purchased'].iloc[0] == 111.
    assert got['s16bq09c'].iloc[0] == 11100
    assert [x[0] for x in calls] == ['map'] * 10 + ['read', 'write']
    writes.clear()
    with pytest.raises(adapter.S16b2021Error, match='duplicate native'):
        adapter.build_s16b_candidate(
            read=lambda *a, **k: pd.concat([_source().iloc[[0]]] * 2),
            write=write, get_map=get_map, id_fn=bindings[0]['id_fn'], format_id=bindings[1])
    assert writes == []
    with pytest.raises(adapter.S16b2021Error, match='unexpected own2021 source path'):
        adapter.build_s16b_candidate(
            read=lambda *a, **k: pytest.fail('unexpected source read'),
            write=write, get_map=get_map, id_fn=bindings[0]['id_fn'],
            format_id=bindings[1], source_path='../Data/s16b_me_bfa2018.dta')
    assert writes == []


def test_provider_free_adapter_and_exact_source_only_serving_wrapper():
    tree = ast.parse(MODULE.read_text())
    imports = {node.names[0].name for node in tree.body if isinstance(node, ast.Import)}
    assert imports == {'pandas'}
    assert not any(isinstance(node, ast.ImportFrom) for node in ast.walk(tree))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    assert not any(isinstance(node.func, ast.Attribute) and node.func.attr in {
        'read_stata', 'read_parquet', 'to_parquet', 'open', 'connect', 'urlopen', 'system',
    } for node in calls)
    assert (COUNTRY / '2021-22' / '_' / 'plot_inputs.py').exists()
    assert not (COUNTRY / '2021-22' / '_' / 's16b_2021.py').exists()
    assert 'plot_inputs:' not in (COUNTRY / '2021-22' / '_' / 'data_info.yml').read_text()


@pytest.mark.parametrize('answer', [2, None, 9])
def test_nonapplication_payload_cannot_abort_positive_applied_membership(adapter, bindings, answer):
    source = pd.concat([_source().iloc[[0]], _source().iloc[[4]]], ignore_index=True)
    source.loc[1, 's16bq02'] = answer
    # Nonapplication payload does not enter application identity/amount/flag
    # validation. It remains verbatim in the independent raw projection.
    source.loc[1, ['grappe', 'menage', 's16bq01']] = None
    source.loc[1, 's16bq05'] = 9
    source['s16bq03a'] = source['s16bq03a'].astype(object)
    source.loc[1, 's16bq03a'] = 'unclassified nonapplication answer'
    before = source.copy(deep=True)
    raw = adapter.raw_s16b_2021(source)
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    public = _adapt(adapter, bindings, source)
    expected = _adapt(adapter, bindings, before.iloc[[0]].copy())
    # Source-independent expected payload is already pinned by the complete
    # literal oracle above; this check composes exact exclusion/invariance.
    expected.attrs['s16b_2021_candidate']['excluded_explicit_nonuse'] = int(answer == 2)
    pd.testing.assert_frame_equal(public, expected, check_exact=True)
    assert public.attrs == expected.attrs
    assert public['s16bq02'].tolist() == [1.]
    pd.testing.assert_frame_equal(source, before, check_exact=True)
