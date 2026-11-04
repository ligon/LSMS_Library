"""Nigeria W4 food units: code plus the recorded size, decoded per round (GH #1033).

W3 prints the size in the unit code (``20. CONGO SMALL``, ``21. CONGO LARGE``).
W4 prints ``20. CONGO`` and asks the size in its own column.  The ``u`` table's
W4 column carried W3's ``small ...`` labels, so every row on these codes was
served ``small`` whatever its size said, and SMALL is the recorded size on 42.3%
(2018Q3) and 36.7% (2019Q1) of them.  The rounds' own labels also differ: 40 is
``DERICA`` at PP and ``DERICA/TIN`` at PH, 120 is ``PACKET/SACHET`` and
``SACHET``.

Ruling (@ligon 2026-11-04): combine the code with the size column, onto W3's
small / medium / large, and decode the two rounds separately.  Each round reads
its own columns; PH's 40 and 120 land on W3's names (fold ruling, 2026-11-04).

Three tiers: the table and the rule (no data), the builder on synthetic rows (no
data), and a guardrail on the built table whose oracle reads the round's ``.dta``
value labels directly, not the CSV codes the build reads.
"""
import ast
import importlib.util
import shutil
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

NIGERIA = countries_root() / 'Nigeria'
W4 = ('2018Q3', '2019Q1')
SIZED = {20: 'congo', 30: 'mudu', 40: 'derica', 50: 'tiya', 60: 'kobiowu',
         70: 'bowl', 80: 'piece', 90: 'heap', 100: 'bunch', 110: 'stalk',
         120: 'packet/sachet'}
SIZE_WORD = {0: 'small', 1: 'medium', 2: 'large', 3: 'very large'}


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def nigeria():
    return _load('_nigeria_1033', NIGERIA / '_' / 'nigeria.py')


@pytest.fixture(scope='module')
def u_table():
    return df_from_orgfile(str(NIGERIA / '_' / 'categorical_mapping.org'), name='u')


# --- the table -------------------------------------------------------------

@pytest.mark.parametrize('t', W4)
def test_no_w4_label_asserts_a_size(nigeria, t):
    labels = nigeria.food_unit_labels(t)
    for code, base in SIZED.items():
        assert labels[code] == base


@pytest.mark.parametrize('t', W4)
def test_w3s_size_variants_are_not_w4_codes(nigeria, t):
    """W3's 21, 41, ... encode a size in the code.  W4 never records them."""
    labels = nigeria.food_unit_labels(t)
    assert not {21, 31, 41, 42, 43, 51, 52, 61, 62, 71, 72, 81, 82, 91, 92,
                101, 102, 111, 112, 121, 122} & set(labels)


def test_w4_rounds_read_their_own_columns(nigeria):
    pp, ph = (nigeria.food_unit_labels(t) for t in W4)
    # 14, 250, 300 and 340 are on PP's value labels and not PH's.
    for code in (14, 250, 300, 340):
        assert code in pp and code not in ph


def test_ph_names_land_on_w3s_unit(nigeria):
    """PH's DERICA/TIN and SACHET are W3's derica and packet/sachet (fold ruling).
    A PH spelling of its own left inferred rows in cells no other wave shares."""
    pp, ph = (nigeria.food_unit_labels(t) for t in W4)
    assert ph[40] == pp[40] == 'derica'
    assert ph[120] == pp[120] == 'packet/sachet'


def test_w3_keeps_its_sized_codes(nigeria):
    for t in ('2015Q3', '2016Q1'):
        labels = nigeria.food_unit_labels(t)
        assert (labels[20], labels[21], labels[41]) == (
            'small congo', 'large congo', 'medium derica')


@pytest.mark.parametrize('t', W4)
def test_composed_labels_are_w3s_spelling(nigeria, u_table, t):
    """Every (code, size) pair W4 records lands on a label of W3's column."""
    observed = {20: (0, 2), 30: (0, 2), 40: (0, 1, 2, 3), 50: (0, 1, 2),
                60: (0, 1, 2), 70: (0, 1, 2), 80: (0, 1, 2), 90: (0, 1, 2),
                100: (0, 1, 2), 110: (0, 1, 2), 120: (0, 1, 2)}
    labels = nigeria.food_unit_labels(t)
    w3 = set(u_table['2015-16'])
    for code, sizes in observed.items():
        for size in sizes:
            composed = nigeria.sized_food_unit(
                pd.Series([float(code)]), pd.Series([float(size)]), labels).iloc[0]
            assert composed == f'{SIZE_WORD[size]} {labels[code]}'
            assert composed in w3, composed


# --- the rule --------------------------------------------------------------

@pytest.mark.parametrize('code,size,expected', [
    (90., 1., 'medium heap'),
    (90., 0., 'small heap'),
    (20., 2., 'large congo'),
    (40., 3., 'very large derica'),
    (120., 0., 'small packet/sachet'),
    (80., np.nan, 'piece'),          # the unit is recorded, its size is not
    (120., 30., 'packet/sachet'),    # 30. 20 GRAMS is not an ordinal size
    (120., 55., 'packet/sachet'),    # 55. 400 GRAMS (100 CUBES)
    (90., 99., 'heap'),              # 99. ONE SIZE ONLY
    (200., 0., 'Loaf'),              # W3 has no sized loaf
    (130., 11., 'Sack/Bag'),         # 11. 50 KG
    (240., 24., 'Bottle/Can'),       # 24. 75 CL
    (1., np.nan, 'Kg'),
])
def test_size_rule(nigeria, code, size, expected):
    labels = nigeria.food_unit_labels('2019Q1')
    out = nigeria.sized_food_unit(pd.Series([code]), pd.Series([size]), labels)
    assert out.iloc[0] == expected


def test_a_code_the_round_does_not_list_stays_na(nigeria):
    labels = nigeria.food_unit_labels('2019Q1')
    out = nigeria.sized_food_unit(pd.Series([65., np.nan]), pd.Series([0., 0.]), labels)
    assert out.isna().all()


# --- the builder -----------------------------------------------------------

@pytest.fixture
def build(nigeria, monkeypatch, tmp_path):
    from lsms_library import local_tools
    shutil.copyfile(NIGERIA / '_' / 'categorical_mapping.org',
                    tmp_path / 'categorical_mapping.org')
    monkeypatch.chdir(tmp_path)

    def _build(raw, t, sources):
        monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
        return nigeria.food_acquired_for_wave(
            'synthetic.csv', t, sources, expenditure='spend')
    return _build


W4_SOURCES = {
    'purchased': {'quantity': 'q9', 'unit': 'u9', 'kg_factor': 'cvn9', 'size': 's9'},
    'produced': {'quantity': 'q6', 'unit': 'u2', 'kg_factor': 'cvn2', 'size': 's2'},
}


def test_each_source_reads_the_size_beside_its_own_unit(build):
    raw = pd.DataFrame({
        'hhid': [1, 2], 'item_cd': [10, 10],
        'q9': [2., 3.], 'u9': [90., 90.], 's9': [0., 2.], 'cvn9': [.5, 1.5],
        'q6': [4., 5.], 'u2': [90., 80.], 's2': [1., np.nan], 'cvn2': [.7, np.nan],
        'spend': [100., 200.],
    })
    out = build(raw, '2019Q1', W4_SOURCES).reset_index().set_index(['i', 's'])
    assert out.loc[('1', 'purchased'), 'u'] == 'small heap'
    assert out.loc[('2', 'purchased'), 'u'] == 'large heap'
    assert out.loc[('1', 'produced'), 'u'] == 'medium heap'
    assert out.loc[('2', 'produced'), 'u'] == 'piece'
    # Only the label moves.
    assert out.loc[('2', 'purchased'), 'Quantity'] == 3.
    assert out.loc[('2', 'purchased'), 'Expenditure'] == 200.
    assert out.loc[('2', 'purchased'), 'Quantity_kg'] == 3. * 1.5


def test_a_blank_unit_is_still_unknown_whatever_its_size(build):
    raw = pd.DataFrame({
        'hhid': [1], 'item_cd': [10],
        'q9': [2.], 'u9': [np.nan], 's9': [0.], 'cvn9': [np.nan],
        'q6': [np.nan], 'u2': [np.nan], 's2': [np.nan], 'cvn2': [np.nan],
        'spend': [100.],
    })
    out = build(raw, '2018Q3', W4_SOURCES)
    assert out.index.get_level_values('u').tolist() == ['Unknown']


def test_an_unlisted_code_still_keeps_its_raw_code(build):
    raw = pd.DataFrame({
        'hhid': [1], 'item_cd': [10],
        'q9': [2.], 'u9': [65.], 's9': [0.], 'cvn9': [np.nan],
        'q6': [np.nan], 'u2': [np.nan], 's2': [np.nan], 'cvn2': [np.nan],
        'spend': [100.],
    })
    out = build(raw, '2018Q3', W4_SOURCES)
    assert out.index.get_level_values('u').tolist() == ['65']


def test_a_source_without_a_size_decodes_as_before(build):
    """W1-W3 declare no size column: the code alone decides."""
    raw = pd.DataFrame({'hhid': [1], 'item_cd': [10], 'q3': [2.], 'u3': [91.],
                        'spend': [100.]})
    out = build(raw, '2015Q3',
                {'purchased': {'quantity': 'q3', 'unit': 'u3'}})
    assert out.index.get_level_values('u').tolist() == ['medium heap']


@pytest.mark.parametrize('folder', ['2010-11', '2012-13', '2015-16'])
def test_no_earlier_wave_names_a_size_column(folder):
    """The rule is W4's: W3 and before carry the size in the code, or none."""
    tree = ast.parse((NIGERIA / folder / '_' / 'food_acquired.py').read_text())
    keys = [k.value for n in ast.walk(tree) if isinstance(n, ast.Dict)
            for k in n.keys if isinstance(k, ast.Constant)]
    assert 'quantity' in keys and 'size' not in keys


def test_the_881_inputs_carry_both_size_columns():
    """A user re-deriving from q9 or q2 needs the size beside the unit."""
    src = (NIGERIA / '2018-19' / '_' / 'mapping.py').read_text()
    raw = next(n.value for n in ast.walk(ast.parse(src))
               if isinstance(n, ast.Assign)
               and getattr(n.targets[0], 'id', None) == '_W4_RAW')
    assert {'9c', '2c'} <= set(ast.literal_eval(raw))
    registry = load_yaml(NIGERIA / '_' / 'derivations.yml')
    variables = registry['Nigeria::food_acquired::w4-last-purchase-30d']['raw_variables']
    assert {'s7bq9c', 's7bq2c', 's10bq9c', 's10bq2c'} <= set(variables)


# --- the built table: oracle reads the .dta labels directly -----------------

def _oracle_counts(t):
    """``{(s, u): rows}`` for W4's sized units, from the round's value labels."""
    from lsms_library.local_tools import get_dataframe
    fn, p = {'2018Q3': ('sect7b_plantingw4.dta', 's7bq'),
             '2019Q1': ('sect10b_harvestw4.dta', 's10bq')}[t]
    raw = get_dataframe(NIGERIA / '2018-19' / 'Data' / fn)
    spend = pd.to_numeric(raw[f'{p}10'], errors='coerce')
    words = {'0. SMALL': 'small', '1. MEDIUM': 'medium', '2. LARGE': 'large',
             '3. VERY LARGE': 'very large'}
    bases = {'20. CONGO': 'congo', '30. MUDU': 'mudu', '40. DERICA': 'derica',
             '40. DERICA/TIN': 'derica', '50. TIYA': 'tiya',
             '60. KOBIOWU': 'kobiowu', '70. BOWL': 'bowl', '80. PIECE': 'piece',
             '90. HEAP': 'heap', '100. BUNCH/BUNDLE': 'bunch',
             '110. STALK': 'stalk', '120. PACKET/SACHET': 'packet/sachet',
             '120. SACHET': 'packet/sachet'}
    counts = {}
    for s, q, unit, size in (('purchased', '9a', '9b', '9c'),
                             ('produced', '6a', '2b', '2c'),
                             ('inkind', '7a', '2b', '2c')):
        keep = pd.to_numeric(raw[p + q], errors='coerce').gt(0)
        if s == 'purchased':
            keep |= spend.gt(0)
        base = raw.loc[keep, p + unit].astype('string').map(bases)
        word = raw.loc[keep, p + size].astype('string').map(words)
        sized = base.notna()
        label = word[sized].fillna('').str.cat(base[sized], sep=' ').str.strip()
        for u, n in label.value_counts().items():
            counts[(s, u)] = int(n)
    return counts


@pytest.fixture(scope='module')
def food_acquired():
    ll = pytest.importorskip('lsms_library')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df = ll.Country('Nigeria').food_acquired()
    return df


@pytest.mark.requires_s3
@pytest.mark.parametrize('t', W4)
def test_served_units_match_the_rounds_value_labels(food_acquired, t):
    oracle = _oracle_counts(t)
    assert sum(oracle.values()) == {'2018Q3': 48305, '2019Q1': 50181}[t]
    frame = food_acquired.xs(t, level='t')
    served = (frame.groupby(['s', 'u'], observed=True).size())
    for key, n in oracle.items():
        assert served.get(key, 0) == n, key


@pytest.mark.requires_s3
@pytest.mark.parametrize('t', W4)
def test_small_is_no_longer_every_row(food_acquired, t):
    u = food_acquired.xs(t, level='t').index.get_level_values('u').astype(str)
    for size in ('small', 'medium', 'large'):
        assert u.str.startswith(size + ' ').sum() > 5000, size


@pytest.mark.requires_s3
def test_rounds_keep_distinct_t(food_acquired):
    assert set(W4) <= set(food_acquired.index.get_level_values('t'))
    assert food_acquired.index.is_unique
