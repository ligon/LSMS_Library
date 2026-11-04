"""Malawi plot_inputs: Module H purchase binding and free seed (GH #1190).

The IHS4/IHS5 Cross_Sectional Module H keeps the original names: the
purchased quantity is ``ag_h07a``/``ag_h07b`` and its VALUE is ``ag_h10``;
``ag_h09`` is the transport payment for that purchase, ``ag_h38a``/``ag_h38b``
the seed received FOR FREE and ``ag_h40`` the transport payment for it
(labels read from the held 2016-17 and 2019-20 headers, 2026-11-03).  The
drivers used to gate the purchase on ``ag_h09`` and add ``ag_h38a`` to it.

These tests need no microdata: a static check on the four drivers and
synthetic frames through the real helpers.  The data-gated oracle is
``tests/test_gh637_malawi_tail.py::TestRawOracle::test_seed_totals_from_raw_modules``.
"""
import ast
import importlib.util
import warnings

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

MALAWI = countries_root() / 'Malawi'


@pytest.fixture(scope='module')
def malawi():
    path = MALAWI / '_' / 'malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_gh1190', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _half_calls(wave):
    """(seed_qty, seed_unit, seed_value) literal lists of each _half() call."""
    tree = ast.parse((MALAWI / wave / '_' / 'plot_inputs.py').read_text())
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == '_half':
            kw = {k.arg: ast.literal_eval(k.value) for k in node.keywords
                  if k.arg in ('seed_qty', 'seed_unit', 'seed_value')}
            out.append(kw)
    return out


@pytest.mark.parametrize('wave', ['2016-17', '2019-20'])
def test_cross_sectional_purchase_is_bound_to_its_value(wave):
    calls = _half_calls(wave)
    assert len(calls) == 2
    xs = [c for c in calls if 'ag_h07a' in c['seed_qty']]
    assert xs == [{'seed_qty': ['ag_h07a'], 'seed_unit': ['ag_h07b'],
                   'seed_value': ['ag_h10']}]
    for c in calls:
        # transport payments are never a value gate; free seed is never a purchase
        assert not {'ag_h09', 'ag_h40'} & set(c['seed_value'])
        assert 'ag_h38a' not in c['seed_qty']


def _purchase_block_calls(wave):
    tree = ast.parse((MALAWI / wave / '_' / 'plot_inputs.py').read_text())
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == '_seed_purchase_block':
            out.append({k.arg: ast.literal_eval(k.value) for k in node.keywords
                        if k.arg in ('qty_cols', 'unit_cols', 'value_cols')})
    return out


def test_2010_11_reads_both_blocks_of_its_one_module_h_file():
    # IHS3 keeps the cross-sectional (ag_h05 = 1: ag_h07a/b, value ag_h10)
    # and panel (ag_h05 = 2: ag_h16/h19, ag_h26/h29) blocks in ONE file
    calls = _purchase_block_calls('2010-11')
    assert calls == [{'qty_cols': ['ag_h07a', 'ag_h16a', 'ag_h26a'],
                      'unit_cols': ['ag_h07b', 'ag_h16b', 'ag_h26b'],
                      'value_cols': ['ag_h10', 'ag_h19', 'ag_h29']}]


@pytest.mark.parametrize('wave', ['2010-11', '2013-14', '2016-17', '2019-20'])
def test_every_driver_serves_free_seed(wave):
    text = (MALAWI / wave / '_' / 'plot_inputs.py').read_text()
    assert '_seed_gift_block(' in text
    assert 'assemble_plot_inputs(WAVE, ' in text and 'gift' in text.split(
        'assemble_plot_inputs(WAVE, ')[-1].split('\n')[0]


def _seed(malawi, rows, half='Cross_Sectional'):
    g = pd.DataFrame(rows)
    return malawi._seed_block(
        g, hhid='hhid', plotkey='plotkey', cropcode='crop_code', qty='ag_g04a',
        unit='ag_g04b', source_ref='Malawi/x/ag_mod_g.dta@dvc:ag_mod_g.dta:0',
        source_half=half, row_positions=list(range(len(g))))


def _h(rows):
    return pd.DataFrame(rows)


def test_gift_is_its_own_measurement_with_its_own_unit(malawi):
    seed = _seed(malawi, [
        {'hhid': '1', 'plotkey': 'p1', 'crop_code': 1, 'ag_g04a': 3.0, 'ag_g04b': 2},
        {'hhid': '2', 'plotkey': 'p1', 'crop_code': 1, 'ag_g04a': 1.0, 'ag_g04b': 2},
    ])
    h = _h({'hhid': ['1', '2'], 'crop_code': [1, 1],
            'ag_h07a': [5.0, pd.NA], 'ag_h07b': [2, pd.NA],
            'ag_h09': [0.0, pd.NA], 'ag_h10': [900.0, pd.NA],
            'ag_h37': [1, 2],
            'ag_h38a': [2.0, 7.0], 'ag_h38b': [8, pd.NA]})
    purchase = malawi._seed_purchase_block(
        h, hhid='hhid', cropcode='crop_code', qty_cols=['ag_h07a'],
        unit_cols=['ag_h07b'], value_cols=['ag_h10'], source_half='Cross_Sectional')
    gift = malawi._seed_gift_block(h, hhid='hhid', cropcode='crop_code',
                                   source_half='Cross_Sectional')
    out = malawi.assemble_plot_inputs('2019-20', [seed], [purchase], [gift])
    by_i = out.reset_index().set_index('i')
    # a purchase with zero transport cost is still a purchase (value-backed)
    assert by_i.loc['1', 'Quantity_purchased'] == 5.0
    assert bool(by_i.loc['1', 'Purchased']) is True
    assert by_i.loc['1', 'Quantity_received_as_gift'] == 2.0
    assert by_i.loc['1', 'Unit_received_as_gift'] == '50 kg Bag'
    # free seed alone: no purchase, its own amount, a missing unit is Unknown
    assert pd.isna(by_i.loc['2', 'Quantity_purchased'])
    assert pd.isna(by_i.loc['2', 'Purchased'])
    assert by_i.loc['2', 'Quantity_received_as_gift'] == 7.0
    assert by_i.loc['2', 'Unit_received_as_gift'] == 'Unknown'
    assert out['Quantity_received_as_gift'].dtype == pd.Float64Dtype()
    # the gate is carried as answered: a quantity under "No" stays visible
    assert bool(by_i.loc['1', 'Received_as_gift']) is True
    assert bool(by_i.loc['2', 'Received_as_gift']) is False
    assert out['Received_as_gift'].dtype == pd.BooleanDtype()


def test_received_as_gift_is_na_where_a_crop_s_rows_disagree(malawi):
    h = _h({'hhid': ['1', '1', '2'], 'crop_code': [1, 1, 1],
            'ag_h37': [1, 2, pd.NA], 'ag_h38a': [2.0, 3.0, 4.0],
            'ag_h38b': [2, 2, 2]})
    gift = malawi._seed_gift_block(h, hhid='hhid', cropcode='crop_code',
                                   source_half='single')
    by_i = gift.set_index('i')
    assert by_i.loc['1', 'Quantity_received_as_gift'] == 5.0
    assert pd.isna(by_i.loc['1', 'Received_as_gift'])
    assert pd.isna(by_i.loc['2', 'Received_as_gift'])


def test_gift_is_not_summed_across_two_units(malawi):
    h = _h({'hhid': ['1', '1', '2', '2'], 'crop_code': [1, 1, 1, 1],
            'ag_h38a': [2.0, 3.0, 2.0, 3.0], 'ag_h38b': [2, 8, 2, 2]})
    gift = malawi._seed_gift_block(h, hhid='hhid', cropcode='crop_code',
                                   source_half='single')
    assert gift.attrs['mixed_unit_groups'] == 1
    assert gift['i'].tolist() == ['2']
    assert gift['Quantity_received_as_gift'].tolist() == [5.0]
    assert gift['Unit_received_as_gift'].tolist() == ['Kilogramme']


def test_gift_needs_a_single_known_observation(malawi):
    seed = _seed(malawi, [
        {'hhid': '1', 'plotkey': 'p1', 'crop_code': 1, 'ag_g04a': 3.0, 'ag_g04b': 2},
        {'hhid': '1', 'plotkey': 'p2', 'crop_code': 1, 'ag_g04a': 1.0, 'ag_g04b': 2},
        {'hhid': '2', 'plotkey': None, 'crop_code': 1, 'ag_g04a': 1.0, 'ag_g04b': 2},
    ])
    h = _h({'hhid': ['1', '2'], 'crop_code': [1, 1],
            'ag_h38a': [2.0, 4.0], 'ag_h38b': [2, 2]})
    gift = malawi._seed_gift_block(h, hhid='hhid', cropcode='crop_code',
                                   source_half='Cross_Sectional')
    out = malawi.assemble_plot_inputs('2016-17', [seed], None, [gift])
    # two plots of one crop, or an unknown plot: not attached anywhere
    assert out['Quantity_received_as_gift'].isna().all()
    assert len(out) == 3


def test_gift_without_source_observations_is_refused(malawi):
    fert = pd.DataFrame({'i': ['1'], 'plot_id': ['p1'], '_input_code': [1],
                         'crop': pd.array([pd.NA], dtype='string'),
                         'Quantity': pd.array([1.0], dtype='Float64'),
                         'u': ['Kilogramme'],
                         'Improved': pd.array([pd.NA], dtype='boolean')})
    gift = pd.DataFrame({'i': ['1'], '_crop_code': ['1'], '_source_half': ['single'],
                         'Quantity_received_as_gift': [1.0],
                         'Unit_received_as_gift': ['Kilogram']})
    with pytest.raises(ValueError, match='source observations'):
        malawi.assemble_plot_inputs('2010-11', [fert], None, [gift])


# --- the purchase's own unit (GH #1190, ruling 2026-11-04) -------------------

def _panel_h(rows):
    """rows: (hhid, code, q1, u1, v1, q2, u2, v2), the two Panel purchase slots."""
    return pd.DataFrame(rows, columns=['hhid', 'crop_code', 'ag_h16a', 'ag_h16b', 'ag_h19',
                                       'ag_h26a', 'ag_h26b', 'ag_h29'])


def _purchase(malawi, rows, half='Panel'):
    return malawi._seed_purchase_block(
        _panel_h(rows), hhid='hhid', cropcode='crop_code',
        qty_cols=['ag_h16a', 'ag_h26a'], unit_cols=['ag_h16b', 'ag_h26b'],
        value_cols=['ag_h19', 'ag_h29'], source_half=half)


NA = pd.NA


def test_the_purchase_unit_is_served_and_is_not_the_planted_unit(malawi):
    seed = _seed(malawi, [{'hhid': '1', 'plotkey': 'p1', 'crop_code': 1,
                           'ag_g04a': 3.0, 'ag_g04b': 1}], half='Panel')
    purchase = _purchase(malawi, [('1', 1, 5.0, 8, 900.0, NA, NA, NA)])
    out = malawi.assemble_plot_inputs('2016-17', [seed], [purchase]).reset_index()
    assert out['u'].tolist() == ['Gram']
    assert out['Unit_purchased'].tolist() == ['50 kg Bag']
    assert out['Quantity_purchased'].tolist() == [5.0]
    assert out['Unit_purchased'].dtype == pd.StringDtype()


def test_components_in_one_unit_add(malawi):
    got = _purchase(malawi, [('1', 1, 5.0, 2, 900.0, 2.0, 2, 100.0)]).iloc[0]
    assert got['Quantity_purchased'] == 7.0
    assert got['Unit_purchased'] == 'Kilogramme'
    assert got['Purchased'] is True or bool(got['Purchased']) is True


def test_lines_of_one_crop_in_one_unit_add(malawi):
    got = _purchase(malawi, [('1', 1, 5.0, 2, 900.0, NA, NA, NA),
                             ('1', 1, 2.0, 2, 100.0, NA, NA, NA)])
    assert got['Quantity_purchased'].tolist() == [7.0]
    assert got.attrs['mixed_unit_groups'] == 0


@pytest.mark.parametrize('rows', [
    # two slots of one line
    [('1', 1, 5.0, 2, 900.0, 1.0, 6, 100.0)],
    # two lines of one crop (2010-11 and 2013-14 repeat a crop in Module H)
    [('1', 1, 5.0, 2, 900.0, NA, NA, NA), ('1', 1, 1.0, 6, 100.0, NA, NA, NA)],
    # a recorded amount with no unit cannot be said to be in the other's unit
    [('1', 1, 5.0, 2, 900.0, 1.0, NA, 100.0)],
])
def test_unlike_units_are_refused_never_summed(malawi, rows):
    got = _purchase(malawi, rows)
    assert len(got) == 1 and got.attrs['mixed_unit_groups'] == 1
    assert pd.isna(got['Quantity_purchased'].iloc[0])
    assert pd.isna(got['Unit_purchased'].iloc[0])
    assert bool(got['Purchased'].iloc[0]) is True


def test_what_a_refused_group_serves(malawi):
    """Its planted answer as recorded, Purchased True, no amount and no unit.
    Neither component is chosen; a build log says so."""
    seed = _seed(malawi, [
        {'hhid': '1', 'plotkey': 'p1', 'crop_code': 1, 'ag_g04a': 3.0, 'ag_g04b': 2},
        {'hhid': '2', 'plotkey': 'p1', 'crop_code': 1, 'ag_g04a': 4.0, 'ag_g04b': 2}],
        half='Panel')
    purchase = _purchase(malawi, [('1', 1, 5.0, 2, 900.0, 1.0, 6, 100.0),
                                  ('2', 1, 5.0, 2, 900.0, 1.0, 2, 100.0)])
    with pytest.warns(UserWarning, match=r'1 household-crop seed purchase'):
        out = malawi.assemble_plot_inputs('2016-17', [seed], [purchase])
    by_i = out.reset_index().set_index('i')
    assert by_i.loc['1', 'Quantity'] == 3.0 and by_i.loc['1', 'u'] == 'Kilogram'
    assert bool(by_i.loc['1', 'Purchased']) is True
    assert pd.isna(by_i.loc['1', 'Quantity_purchased'])
    assert pd.isna(by_i.loc['1', 'Unit_purchased'])
    # its neighbour, in one unit, is untouched
    assert by_i.loc['2', 'Quantity_purchased'] == 6.0
    assert by_i.loc['2', 'Unit_purchased'] == 'Kilogramme'
    assert out.attrs['seed_purchase_mixed_units'] == 1


@pytest.mark.parametrize('zero_unit', [NA, 6])
def test_a_recorded_zero_cannot_disagree(malawi, zero_unit):
    """It adds nothing: 2019-20 Panel holds 2 Kilogram + 0 with no unit."""
    got = _purchase(malawi, [('1', 1, 2.0, 2, 900.0, 0.0, zero_unit, 100.0)]).iloc[0]
    assert got['Quantity_purchased'] == 2.0
    assert got['Unit_purchased'] == 'Kilogramme'


def test_a_lone_zero_keeps_the_unit_it_was_recorded_in(malawi):
    named = _purchase(malawi, [('1', 1, 0.0, 6, 900.0, NA, NA, NA)]).iloc[0]
    blank = _purchase(malawi, [('1', 1, 0.0, NA, 900.0, NA, NA, NA)]).iloc[0]
    assert named['Quantity_purchased'] == 0.0 and named['Unit_purchased'] == '5 kg Bag'
    assert blank['Quantity_purchased'] == 0.0 and blank['Unit_purchased'] == 'Unknown'


def test_a_component_failing_the_value_gate_has_no_vote(malawi):
    """No value, no purchase: its quantity and its unit are both ignored."""
    got = _purchase(malawi, [('1', 1, 5.0, 2, 900.0, 1.0, 6, NA)]).iloc[0]
    assert got['Quantity_purchased'] == 5.0
    assert got['Unit_purchased'] == 'Kilogramme'
    none = _purchase(malawi, [('1', 1, 5.0, 2, 0.0, NA, NA, NA)])
    assert none.empty


def test_an_unlisted_unit_code_is_source_qualified(malawi):
    got = _purchase(malawi, [('1', 1, 5.0, 77, 900.0, NA, NA, NA)]).iloc[0]
    assert got['Unit_purchased'] == 'Seed unit code 77'


def _no_source_plot_inputs(malawi, purchase_rows):
    """Seed rows with no source observation: the branch that reduces purchases a
    second time, across the codes one crop label covers.  No wave script takes it."""
    g = pd.DataFrame({'hhid': ['1', '1'], 'plotkey': ['p1', 'p1'], 'crop_code': [1, 3],
                      'ag_g04a': [3.0, 4.0], 'ag_g04b': [2, 2]})
    seed = malawi._seed_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code',
                              qty='ag_g04a', unit='ag_g04b', t='2010-11')
    purchase = malawi._seed_purchase_block(
        _panel_h(purchase_rows), hhid='hhid', cropcode='crop_code',
        qty_cols=['ag_h16a', 'ag_h26a'], unit_cols=['ag_h16b', 'ag_h26b'],
        value_cols=['ag_h19', 'ag_h29'])
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return malawi.assemble_plot_inputs('2010-11', [seed], [purchase]).reset_index()


@pytest.mark.parametrize('rows', [
    # refused below, then reduced again beside a code in one unit
    [('1', 1, 5.0, 2, 900.0, 1.0, 6, 100.0), ('1', 3, 2.0, 2, 100.0, NA, NA, NA)],
    # refused below, alone in its crop
    [('1', 1, 5.0, 2, 900.0, 1.0, 6, 100.0)],
    # each code in one unit, but not the same one
    [('1', 1, 5.0, 2, 900.0, NA, NA, NA), ('1', 3, 1.0, 6, 100.0, NA, NA, NA)],
])
def test_a_refusal_survives_the_reduction_across_codes(malawi, rows):
    """A group refused below keeps its NA unit, so the second pass must read NA as
    "no unit to vote" rather than fail on a non-boolean condition."""
    out = _no_source_plot_inputs(malawi, rows)
    assert len(out) == 1
    assert bool(out['Purchased'].iloc[0]) is True
    assert pd.isna(out['Quantity_purchased'].iloc[0])
    assert pd.isna(out['Unit_purchased'].iloc[0])


def test_codes_in_one_unit_still_add_across_the_reduction(malawi):
    out = _no_source_plot_inputs(malawi, [('1', 1, 5.0, 2, 900.0, NA, NA, NA),
                                          ('1', 3, 2.0, 2, 100.0, NA, NA, NA)])
    assert out['Quantity_purchased'].tolist() == [7.0]
    assert out['Unit_purchased'].tolist() == ['Kilogramme']


def test_the_column_is_declared_optional():
    scheme = yaml.safe_load((MALAWI / '_' / 'data_scheme.yml').read_text())['Data Scheme']
    assert scheme['plot_inputs']['Unit_purchased'] == {'type': 'str', 'optional': True}


@pytest.mark.requires_s3
@pytest.mark.parametrize('wave, halves', [
    ('2010-11', [('2010-11/Data/Full_Sample/Agriculture/ag_mod_h.dta', 'case_id', '', 'ag_h0b',
                  [('ag_h07a', 'ag_h07b', 'ag_h10'), ('ag_h16a', 'ag_h16b', 'ag_h19'),
                   ('ag_h26a', 'ag_h26b', 'ag_h29')])]),
    ('2013-14', [('2013-14/Data/AG_MOD_H_13.dta', 'y2_hhid', '', 'ag_h0c',
                  [('ag_h16a', 'ag_h16b', 'ag_h19'), ('ag_h26a', 'ag_h26b', 'ag_h29')])]),
    ('2016-17', [('2016-17/Data/Cross_Sectional/ag_mod_h.dta', 'case_id', 'cs-17-', 'crop_code',
                  [('ag_h07a', 'ag_h07b', 'ag_h10')]),
                 ('2016-17/Data/Panel/ag_mod_h_16.dta', 'y3_hhid', '', 'crop_code',
                  [('ag_h16a', 'ag_h16b', 'ag_h19'), ('ag_h26a', 'ag_h26b', 'ag_h29')])]),
    ('2019-20', [('2019-20/Data/Cross_Sectional/ag_mod_h.dta', 'case_id', 'cs-19-', 'crop_code',
                  [('ag_h07a', 'ag_h07b', 'ag_h10')]),
                 ('2019-20/Data/Panel/ag_mod_h_19.dta', 'y4_hhid', '', 'crop_code',
                  [('ag_h16a', 'ag_h16b', 'ag_h19'), ('ag_h26a', 'ag_h26b', 'ag_h29')])]),
])
def test_the_blocks_agree_with_an_oracle_on_module_h(malawi, wave, halves):
    """Oracle: reads Module H directly, with no block and no groupby in the block's
    shape.  The unit of every household-crop purchase, and the refused set."""
    from lsms_library.local_tools import format_id, get_dataframe
    for rel, hh, prefix, crop, slots in halves:
        h = get_dataframe(str(MALAWI / rel), convert_categoricals=False)
        if wave == '2010-11':
            h = h[~h.duplicated()]
        hhid = prefix + h[hh].apply(format_id).astype('string')
        native = malawi._seed_crop_identity(h[crop])[1]
        expected = {}
        for q_col, u_col, v_col in slots:
            q, u, v = (pd.to_numeric(h[c], errors='coerce') for c in (q_col, u_col, v_col))
            bought = q.notna() & v.notna() & v.ne(0) & native.notna()
            for ident, code, amount, unit in zip(hhid[bought], native[bought], q[bought], u[bought]):
                expected.setdefault((ident, code), []).append(
                    (float(amount), None if pd.isna(unit) else int(unit)))
        h = h.assign(hhid=hhid.values)
        got = malawi._seed_purchase_block(
            h, hhid='hhid', cropcode=crop, qty_cols=[s[0] for s in slots],
            unit_cols=[s[1] for s in slots], value_cols=[s[2] for s in slots],
            source_half='x').set_index(['i', '_crop_code'])
        assert set(got.index) == set(expected)
        refused = 0
        for key, components in expected.items():
            row = got.loc[key]
            nonzero = {u for q, u in components if q != 0}
            if len(nonzero) > 1:
                refused += 1
                assert pd.isna(row['Quantity_purchased']) and pd.isna(row['Unit_purchased'])
            else:
                assert row['Quantity_purchased'] == pytest.approx(sum(q for q, _ in components))
                code = next(iter(nonzero), None)
                if code is None:
                    code = next((u for _, u in components if u is not None), None)
                label = ('Unknown' if code is None else
                         malawi._MWI_PURCH_UNIT_LABELS.get(code, f'Seed unit code {code}'))
                assert row['Unit_purchased'] == label
        assert got.attrs['mixed_unit_groups'] == refused


def test_the_purchase_and_gift_units_are_on_the_served_u_axis(malawi):
    """The API maps ``u`` from 'Kilogram' to 'Kilogramme' and never maps a column
    with another name, so the two unit columns are written canonical at build
    time; otherwise one row spells its planted and its bought kilogram two ways."""
    assert malawi._MWI_SEED_UNIT_LABELS[2] == 'Kilogram'    # `u`, before the API
    assert malawi._MWI_PURCH_UNIT_LABELS[2] == 'Kilogramme'
    table = malawi._malawi_code_map('harmonize_crop_unit')
    assert table[1] == 'Kilogramme'


@pytest.mark.requires_s3
def test_every_served_purchase_row_says_its_unit_or_why_not():
    from lsms_library.country import Country
    pi = Country('Malawi').plot_inputs()
    bought = pi[pi['Purchased'].fillna(False).astype(bool)]
    assert len(bought) > 0
    # unit and amount are present together: a blank pair is the refusal, nothing else
    assert (bought['Quantity_purchased'].isna() == bought['Unit_purchased'].isna()).all()
    assert pi.loc[pi['Quantity_purchased'].notna(), 'Purchased'].fillna(False).all()
    # one vocabulary with the served u: a planted and a bought kilogram match
    u = set(pi.index.get_level_values('u').astype(str))
    for column in ('Unit_purchased', 'Unit_received_as_gift'):
        assert set(pi[column].dropna().astype(str)) <= u, column
