"""Tanzania #1119: actual helper functions, invented sources, no data builds.

Vocabulary oracle: issue1119 comment6368100317 / data_info.yml:250–265.
Payload oracle: exact frozen pre-migration production function, never a
second implementation. Native colmaps and categorical tables are read as code.

GH #637 ruling (@ligon 2026-11-03): seed rows keep each native AG_SEC_4A
report via ``input_obs`` = ``reported:4A:<cropid>``.  Where the frozen
function did not reduce, its rows are compared after dropping that level.
Where it summed coded seed records, or OR'd their flags, the expectation is
REVERSED here for that stated reason: the records stay separate, and a
repeated native record raises.  Unresolved records (no crop code, and every
plot-level input) keep the frozen reduction and are compared as before.
"""
import ast
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.test_gh905_tanzania_vectorised_sums import SOURCE, load_helper
from tests.tanzania_crop_marker_oracle import (
    assert_strict, original_function, relabel_expected,
)


STRUCTURAL = '(not crop-specific)'
UNKNOWN = 'unknown_crop'
UNRESOLVED = 'unresolved_observation'


def legacy(after):
    """The served frame without the round-4 ``input_obs`` level."""
    return after.droplevel('input_obs')


def obs(after):
    return after.index.get_level_values('input_obs').tolist()
COLS3 = ('ag3a_41', 'ag3a_42', 'ag3a_43', 'ag3a_44', 'ag3a_47', 'ag3a_48',
         'ag3a_49', 'ag3a_51', 'ag3a_54', 'ag3a_55', 'ag3a_56', 'ag3a_58',
         'ag3a_60', 'ag3a_62_1', 'ag3a_62_2', 'ag3a_63', 'ag3a_65a',
         'ag3a_65b_1', 'ag3a_65b_2', 'ag3a_65c')


@pytest.fixture(params=['2019-20', '2020-21'])
def helpers(request, monkeypatch):
    wave = request.param
    script = SOURCE.parents[1] / wave / '_' / 'plot_inputs.py'
    # Read ONLY the literal assignment, never execute the native driver.
    assignment = next(n for n in ast.parse(script.read_text()).body
                      if isinstance(n, ast.Assign) and any(
                          isinstance(t, ast.Name) and t.id == 'colmap' for t in n.targets))
    cm = dict((kw.arg, ast.literal_eval(kw.value)) for kw in assignment.value.keywords)
    assert cm == ({'hhid': 'sdd_hhid', 'plot': 'plotnum', 'crop': 'cropid'}
                  if wave == '2019-20' else
                  {'hhid': 'y5_hhid', 'plot': 'plot_id', 'crop': 'cropid'})
    old, new = load_helper(), load_helper()
    exec(compile(original_function(), str(SOURCE), 'exec'), old.__dict__)
    monkeypatch.chdir(script.parent)
    return old, new, wave, cm


def sources(cm, seeds=(), inputs=()):
    base = {cm['hhid']: '100', cm['plot']: 1, cm['crop']: None,
            'ag4a_10_1': 1., 'ag4a_10_2': 1, 'ag4a_10c_1': pd.NA,
            'ag4a_10c_2': pd.NA, 'ag4a_12': pd.NA, 'ag4a_08': pd.NA}
    sec4 = pd.DataFrame([dict(base, **r) for r in seeds], columns=list(base))
    base3 = {cm['hhid']: '200', cm['plot']: 1, **{c: pd.NA for c in COLS3}}
    sec3 = pd.DataFrame([dict(base3, **r) for r in inputs], columns=list(base3))
    sec3.attrs = sec4.attrs = {'source': ['invented', {'flag': True, 'count': 1}]}
    return sec3, sec4


def run(helpers, seeds=(), inputs=()):
    old, new, wave, cm = helpers
    sec3, sec4 = sources(cm, seeds, inputs)
    frozen3, frozen4 = sec3.copy(deep=True), sec4.copy(deep=True)
    before = old.plot_inputs_for_wave(wave, sec3, sec4, cm)
    after = new.plot_inputs_for_wave(wave, sec3, sec4, cm)
    assert_strict(sec3, frozen3)
    assert_strict(sec4, frozen4)
    return before, after


def known(helpers):
    return next((k, v) for k, v in helpers[1]._crop_labels().items() if pd.notna(v))


def lump(helpers):
    grouped = defaultdict(list)
    for code, label in helpers[1]._crop_labels().items():
        if pd.notna(label):
            grouped[label].append(code)
    return next((codes[:2], label) for label, codes in grouped.items() if len(codes) > 1)


def test_source_branches_numeric_literal_reserved_and_plot_axes(helpers):
    _, new, _, cm = helpers
    code, label = known(helpers)
    assert 98765 not in new._crop_labels() and 0 not in new._crop_labels()
    crops = [code, 98765, 98765., ' 98765 ', 'Literal crop', 'n/a',
             STRUCTURAL, UNKNOWN, 'crop_98765', None, ' \t ', 0]
    expected = [label, '98765', '98765', '98765', 'Literal crop', 'n/a',
                STRUCTURAL, UNKNOWN, 'crop_98765', UNKNOWN, UNKNOWN, '0']
    seeds = [{cm['crop']: crop, cm['plot']: j + 1} for j, crop in enumerate(crops)]
    # Literal reserved text remains source text, even in the crop-specific
    # seed branch. Plot sentinels are just recorded plot IDs in this fixture.
    inputs = [{cm['plot']: 'Farm-level', 'ag3a_41': 1, 'ag3a_42': 0.,
               'ag3a_60': 1, 'ag3a_62_2': 2},
              {cm['plot']: 'unknown_plot', 'ag3a_65a': 1, 'ag3a_65b_2': 4}]
    before, after = run(helpers, seeds, inputs)
    assert after.index.get_level_values('crop').tolist() == expected + [STRUCTURAL] * 3
    assert after.index.get_level_values('plot_id').tolist()[-3:] == [
        'Farm-level', 'Farm-level', 'unknown_plot']
    assert_strict(legacy(after), relabel_expected(before, expected + [STRUCTURAL] * 3))
    # Only an integer crop code names a native record; the rest are unresolved.
    assert obs(after) == ([f'reported:4A:{code}'] + ['reported:4A:98765'] * 3
                          + [UNRESOLVED] * 7 + ['reported:4A:0'] + [UNRESOLVED] * 3)


def test_legacy_eligibility_precedes_markers_and_passthrough(helpers):
    cm = helpers[3]
    code, label = known(helpers)
    seeds = [
        {cm['crop']: None, 'ag4a_10_1': pd.NA},
        {cm['crop']: 98765, 'ag4a_10_1': pd.NA},
        {cm['crop']: 'Literal crop', 'ag4a_10_1': pd.NA},
        {cm['crop']: STRUCTURAL, 'ag4a_10_1': pd.NA},
        {cm['crop']: UNKNOWN, 'ag4a_10_1': pd.NA},
        {cm['crop']: None, 'ag4a_10_1': 0.},
        {cm['crop']: code, 'ag4a_10_1': pd.NA},
        {cm['crop']: None, 'ag4a_10_1': pd.NA, 'ag4a_08': 1,
         'ag4a_10c_1': 3., 'ag4a_10c_2': 1, 'ag4a_12': 100},
    ]
    for j, row in enumerate(seeds):
        row[cm['plot']] = j + 1
    before, after = run(helpers, seeds, [{'ag3a_41': 1}])
    assert after.index.get_level_values('plot_id').tolist() == ['6', '7', '1']
    assert_strict(legacy(after), relabel_expected(before, [UNKNOWN, label, STRUCTURAL]))
    assert obs(after) == [UNRESOLVED, f'reported:4A:{code}', UNRESOLVED]
    assert after.Quantity.iloc[0] == 0.
    assert pd.isna(after.Quantity.iloc[1])


def test_mapped_lumped_codes_keep_their_own_rows_and_flags(helpers):
    """REVERSED (GH #637 round 4).  The frozen function summed the lumped codes
    ``a`` and ``b`` under one label and OR'd their flags; each native record
    is now its own row with its own payload."""
    cm = helpers[3]
    (a, b), label = lump(helpers)
    seeds = [
        {cm['crop']: a, 'ag4a_10_1': 1e16},
        {cm['crop']: b, 'ag4a_10_1': 1., 'ag4a_10c_1': 1.,
         'ag4a_10c_2': 1, 'ag4a_08': 1},
        {cm['crop']: 98765, 'ag4a_10_1': 7.},
    ]
    before, after = run(helpers, seeds)
    assert before.index.get_level_values('crop').tolist() == [label, 'crop_98765']
    assert before.Quantity.tolist() == [1e16 + 1., 7.]
    assert after.index.get_level_values('crop').tolist() == [label, label, '98765']
    assert obs(after) == [f'reported:4A:{a}', f'reported:4A:{b}', 'reported:4A:98765']
    assert after.Quantity.tolist() == [1e16, 1., 7.]
    assert pd.isna(after.Quantity_purchased.iloc[0])
    assert after.Quantity_purchased.iloc[1] == 1.
    assert after.Unit_purchased.iloc[1] == 'Kg'
    assert after.Purchased.tolist() == [False, True, False]
    assert pd.isna(after.Improved.iloc[0]) and bool(after.Improved.iloc[1])


def test_a_repeated_native_seed_record_refuses(helpers):
    """The frozen function summed two records of one code; one (household,
    plot, cropid) is now one record, so a repeat raises instead."""
    old, new, wave, cm = helpers
    (a, _), _ = lump(helpers)
    sec3, sec4 = sources(cm, [{cm['crop']: a, 'ag4a_10_1': 1.},
                              {cm['crop']: a, 'ag4a_10_1': 2.}])
    assert old.plot_inputs_for_wave(wave, sec3, sec4, cm).Quantity.tolist() == [3.]
    with pytest.raises(ValueError, match='repeat a native'):
        new.plot_inputs_for_wave(wave, sec3, sec4, cm)


def test_absent_and_blank_crop_keep_old_duplicate_sum(helpers):
    cm = helpers[3]
    before, after = run(helpers, [{cm['crop']: None, 'ag4a_10_1': 2.},
                                  {cm['crop']: ' ', 'ag4a_10_1': 3.}])
    assert after.Quantity.tolist() == [5.]
    assert obs(after) == [UNRESOLVED]
    assert_strict(legacy(after), relabel_expected(before, [UNKNOWN]))


@pytest.mark.parametrize('crops', [('Crop A', 'Crop B'), (None, STRUCTURAL),
                                  ('98765', 'Literal crop')])
def test_distinct_desired_identities_in_legacy_group_refuse(helpers, crops):
    old, new, wave, cm = helpers
    sec3, sec4 = sources(cm, [{cm['crop']: crop} for crop in crops])
    # numeric 98765 used to be crop_98765, unlike the literal: no old pooling.
    if crops[0] == '98765':
        assert len(old.plot_inputs_for_wave(wave, sec3, sec4, cm)) == 2
        out = new.plot_inputs_for_wave(wave, sec3, sec4, cm)
        assert out.index.get_level_values('crop').tolist() == ['98765', 'Literal crop']
    else:
        assert len(old.plot_inputs_for_wave(wave, sec3, sec4, cm)) == 1
        with pytest.raises(ValueError, match='legacy crop aggregation conflates distinct'):
            new.plot_inputs_for_wave(wave, sec3, sec4, cm)


def test_structural_and_seed_provenance_conflict_is_not_blanket_fill(helpers, monkeypatch):
    old, new, wave, cm = helpers
    for module in (old, new):
        monkeypatch.setattr(module, '_plot_harmonized_codes', lambda *a, **k: {1: 'Seed'})
        monkeypatch.setattr(module, '_crop_labels', lambda: {})
    sec3, sec4 = sources(cm, [{cm['hhid']: '200'}],
                        [{'ag3a_47': 1, 'ag3a_48': 1, 'ag3a_49': 3.}])
    assert len(old.plot_inputs_for_wave(wave, sec3, sec4, cm)) == 1
    with pytest.raises(ValueError, match='legacy crop aggregation conflates distinct'):
        new.plot_inputs_for_wave(wave, sec3, sec4, cm)


@pytest.mark.parametrize('reduced', [False, True])
def test_coded_and_literal_records_of_one_label_stay_distinct(helpers, reduced):
    """REVERSED (GH #637 round 4).  A coded record and a literal one that
    relabel to the same crop used to collide and refuse; ``input_obs`` now
    tells them apart.  Repeating the coded record raises as a repeat."""
    old, new, wave, cm = helpers
    code, label = known(helpers)
    seeds = [{cm['crop']: code}, {cm['crop']: label}]
    if reduced:
        seeds.append({cm['crop']: code})
    sec3, sec4 = sources(cm, seeds)
    assert len(old.plot_inputs_for_wave(wave, sec3, sec4, cm)) == 2
    if reduced:
        with pytest.raises(ValueError, match='repeat a native'):
            new.plot_inputs_for_wave(wave, sec3, sec4, cm)
        return
    out = new.plot_inputs_for_wave(wave, sec3, sec4, cm)
    assert out.index.get_level_values('crop').tolist() == [label, label]
    assert obs(out) == [f'reported:4A:{code}', UNRESOLVED]


def test_two_units_unknown_residual_and_both_fertilizer_slots(helpers):
    _, new, _, cm = helpers
    (a, b), label = lump(helpers)
    urea = next(k for k, v in new._plot_harmonized_codes('harmonize_input').items()
                if v == 'Urea')
    seeds = [{cm['crop']: a, 'ag4a_10_1': 2., 'ag4a_10_2': 5},
             {cm['crop']: b, 'ag4a_10_1': 50., 'ag4a_10_2': 5},
             {cm['crop']: a, cm['plot']: 2, 'ag4a_10_1': 3., 'ag4a_10_2': 1},
             {cm['crop']: b, cm['plot']: 2, 'ag4a_10_1': 5., 'ag4a_10_2': 2}]
    before, after = run(helpers, seeds, [{
        'ag3a_47': 1, 'ag3a_48': urea, 'ag3a_49': 50., 'ag3a_51': 100.,
        'ag3a_54': 1, 'ag3a_55': urea, 'ag3a_56': 100., 'ag3a_58': 200.}])
    # Frozen: the two OTHER (SPECIFY) quantities were one Unknown row (52).
    assert before.Quantity.tolist() == [52., 5., 3., 150.]
    # REVERSED (GH #637 round 4): two records, two rows; the signed
    # fertilizer-slot sum (unresolved) is unchanged.
    flat = after.reset_index()
    got = {(p, o): (u, q) for p, o, u, q in
           zip(flat['plot_id'], flat['input_obs'], flat['u'], flat['Quantity'])}
    assert got == {('1', f'reported:4A:{a}'): ('Unknown', 2.),
                   ('1', f'reported:4A:{b}'): ('Unknown', 50.),
                   ('2', f'reported:4A:{a}'): ('kg', 3.),
                   ('2', f'reported:4A:{b}'): ('20 liter bucket', 5.),
                   ('1', UNRESOLVED): ('kg', 150.)}
    assert set(flat.loc[flat['input'] == 'Seed', 'crop']) == {label}
    assert after.Quantity_purchased.isna().all()  # min_count=1


def test_purchased_unit_disagreement_refusal_survives(helpers):
    """For unresolved records, which still reduce, both functions refuse.
    REVERSED for coded records (GH #637 round 4): two lumped codes no longer
    share a key, so each keeps its own purchase unit instead of refusing."""
    old, new, wave, cm = helpers
    (a, b), _ = lump(helpers)
    sec3, sec4 = sources(cm, [
        {cm['crop']: None, 'ag4a_10c_1': 2., 'ag4a_10c_2': 1},
        {cm['crop']: None, 'ag4a_10c_1': 3., 'ag4a_10c_2': 3}])
    for module in (old, new):
        with pytest.raises(ValueError, match='Quantity_purchased cannot be summed'):
            module.plot_inputs_for_wave(wave, sec3, sec4, cm)
    sec3, sec4 = sources(cm, [
        {cm['crop']: a, 'ag4a_10c_1': 2., 'ag4a_10c_2': 1},
        {cm['crop']: b, 'ag4a_10c_1': 3., 'ag4a_10c_2': 3}])
    with pytest.raises(ValueError, match='Quantity_purchased cannot be summed'):
        old.plot_inputs_for_wave(wave, sec3, sec4, cm)
    out = new.plot_inputs_for_wave(wave, sec3, sec4, cm)
    assert out.Unit_purchased.tolist() == ['Kg', 'Small Cup']
    assert out.Quantity_purchased.tolist() == [2., 3.]


@pytest.mark.parametrize('crop', [98765.25, Decimal('98765.25'), np.inf,
                                  1e30, complex(1, 2)])
def test_unsupported_numeric_types_retain_original_refusal(helpers, crop):
    old, new, wave, cm = helpers
    sec3, sec4 = sources(cm, [{cm['crop']: crop}])
    failures = []
    for module in (old, new):
        with pytest.raises((TypeError, ValueError, OverflowError)) as failure:
            module.plot_inputs_for_wave(wave, sec3, sec4, cm)
        failures.append((type(failure.value), str(failure.value)))
    assert failures[0] == failures[1]


def test_native_nondefault_source_index_keeps_each_native_record(helpers):
    """REVERSED (GH #637 round 4).  The legacy fallback reindexes the source
    code on the constructed frame's index, so with a nondefault source index
    both records lost their code and pooled (the #1119 helper refused that
    ambiguity).  ``input_obs`` is aligned by position, like the passthrough
    labels, so each native record keeps its own row and crop."""
    old, new, wave, cm = helpers
    sec3, sec4 = sources(cm, [{cm['crop']: 98765}, {cm['crop']: 98766}])
    sec4.index = [10, 20]
    assert len(old.plot_inputs_for_wave(wave, sec3, sec4, cm)) == 1
    out = new.plot_inputs_for_wave(wave, sec3, sec4, cm)
    assert out.index.get_level_values('crop').tolist() == ['98765', '98766']
    assert obs(out) == ['reported:4A:98765', 'reported:4A:98766']


@pytest.mark.parametrize('reduced', [False, True])
def test_attrs_survive_actual_reducer_and_relabel(helpers, monkeypatch, reduced):
    cm = helpers[3]
    code, label = known(helpers)
    concat = pd.concat

    def with_attrs(*args, **kwargs):
        frame = concat(*args, **kwargs)
        frame.attrs = {'nested': [{'flag': True, 'number': 1, 'pair': (False, 2)}]}
        return frame

    monkeypatch.setattr(pd, 'concat', with_attrs)
    # GH #637 round 4: a repeated coded record raises, so the reducer is
    # exercised by two records without a crop code (unresolved).
    seeds = [{cm['crop']: None}] * 2 if reduced else [{cm['crop']: code}]
    before, after = run(helpers, seeds)
    # The exact frozen production function (and pandas' existing dict-agg
    # path) drops attrs when it reduces. Preserve that behavior; marker work
    # must not silently repair it. The unique path retains the typed value.
    if reduced:
        assert before.attrs == {}
    else:
        assert before.attrs['nested'][0]['flag'] is True
    assert_strict(legacy(after), relabel_expected(before, [UNKNOWN if reduced else label]))


@pytest.mark.parametrize('value', [np.nextafter(1., 2.), 1. + 1e-12, -1e-300])
def test_strong_numeric_oracle_rejects_nullable_false_pass(value):
    a = pd.DataFrame({'x': pd.array([1. if value > 0 else 0., None], dtype='Float64')})
    b = a.copy()
    b.loc[0, 'x'] = value
    with pytest.raises(AssertionError):
        assert_strict(a, b)


def test_oracle_rejects_typed_attrs_and_unused_axis_domain_changes():
    a = pd.DataFrame({'x': pd.array([1., None], dtype='Float64')})
    b = a.copy()
    a.attrs = {'nested': [{'flag': True}]}
    b.attrs = {'nested': [{'flag': 1}]}
    with pytest.raises(AssertionError):
        assert_strict(a, b)
    a.attrs = b.attrs = {}
    a.index = pd.MultiIndex(levels=[['observed', 'unused']], codes=[[0, 0]], names=['i'])
    b.index = pd.MultiIndex(levels=[['observed']], codes=[[0, 0]], names=['i'])
    with pytest.raises(AssertionError):
        assert_strict(a, b)
