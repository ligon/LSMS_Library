"""Nigeria crop provenance: invented inputs, unchanged eligibility and payloads."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.test_gh1196_parquet_index_policy import _axis, _exact, _typed
from tests.test_gh1118_nigeria_retained_units import _finalize_invented


@pytest.fixture
def nigeria():
    path = Path(__file__).parents[1] / 'lsms_library/countries/Nigeria/_/nigeria.py'
    spec = importlib.util.spec_from_file_location('nigeria_1119', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def part(crop='Maize', label=None, **payload):
    frame = pd.DataFrame({'i': ['synthetic'], 'plot_id': ['p'],
                          'input': ['Seed'], 'crop': [crop], **payload})
    if label is not None:
        frame['_crop_label'] = label
    return frame


def test_long_fertilizer_provenance_keeps_channel_policy(nigeria):
    raw = pd.DataFrame({'hh': [1, 2], 'plot': [1, 1],
                        'type0': [1, 1], 'q0': [2., 0.],
                        'type1': [1, 1], 'q1': [3., 0.]})
    rows = nigeria.fert_rows_long_typed(
        raw, raw, 'hh', 'plot',
        [{'type': 'type0', 'qty': 'q0', 'unit': None, 'purchased': False},
         {'type': 'type1', 'qty': 'q1', 'unit': None, 'purchased': True}],
        {1: 'NPK'})
    assert rows.crop.tolist() == ['n/a', 'n/a']
    assert rows._crop_label.tolist() == [nigeria.NO_CROP] * 2
    out = nigeria.assemble_plot_inputs('2010Q3', [rows])
    assert out.index.get_level_values('crop').tolist() == [nigeria.NO_CROP] * 2
    assert out.Quantity.tolist()[0] == 5.
    assert pd.isna(out.Quantity.iloc[1])  # existing all-zero long-type rule
    assert out.Quantity_purchased.tolist() == [3., 0.]
    assert out.Purchased.tolist() == [True, False]
    assert out.u.tolist() == ['Kg', 'Kg']


def test_wide_fertilizer_keeps_recorded_zero_not_missing(nigeria):
    raw = pd.DataFrame({'hh': [1, 2, 3, None], 'plot': [1] * 4,
                        'q': [0., None, 3., 4.]})
    rows = nigeria.fert_rows_wide_typed(
        raw, raw, 'hh', 'plot', [{'input': 'NPK', 'qty': 'q', 'unit': None}])
    out = nigeria.assemble_plot_inputs('2018Q3', [rows])
    assert out.Quantity.tolist() == [0., 3.]
    assert out.index.get_level_values('i').tolist() == ['1', '3']
    assert out.index.get_level_values('crop').tolist() == [nigeria.NO_CROP] * 2


def test_chemical_positive_quantity_gate_and_days_are_unchanged(nigeria):
    raw = pd.DataFrame({'hh': [1, 2, 3, 4, None], 'plot': [1] * 5,
                        'own': [0., -2., None, 1., 2.],
                        'rent': [0., 0., None, 2., 1.]})
    rows = nigeria.chem_rows(raw, raw, 'hh', 'plot',
                            [{'input': 'Animal Traction',
                              'qty_cols': ['own', 'rent'], 'u_label': 'days'}])
    out = nigeria.assemble_plot_inputs('2010Q3', [rows])
    assert len(out) == 1 and out.Quantity.tolist() == [3.]
    assert out.u.tolist() == ['days']
    assert out.index.get_level_values('crop').tolist() == [nigeria.NO_CROP]


def seeds(nigeria, codes, mapping, quantities=None, improved=None):
    raw = pd.DataFrame({'hh': ['synthetic'] * len(codes),
                        'plot': list(range(1, len(codes) + 1)), 'code': codes,
                        'q': quantities if quantities is not None else [None] * len(codes)})
    return nigeria.seed_rows_for_wave(
        raw, raw, 'hh', 'plot', 'code',
        [{'qty': 'q', 'unit': None, 'purchased': False}], mapping,
        improved=improved)


def test_seed_resolved_crop_gate_remains_before_markers(nigeria):
    rows = seeds(nigeria, [1, None, 999, 0], {1: 'Maize'}, quantities=[None, 5., 6., 7.])
    assert rows.plot_id.tolist() == ['1']
    assert rows._crop_label.tolist() == ['Maize']
    out = nigeria.assemble_plot_inputs('2018Q3', [rows])
    # The existing nullable where-chain yields False even with no purchased
    # amount; preserve that current constructor behavior, not an assumed NA.
    assert len(out) == 1 and out.Quantity.isna().all()
    assert out.Purchased.tolist() == [False]
    assert out.u.tolist() == ['Unknown']
    assert out.index.get_level_values('crop').tolist() == ['Maize']
    assert len(_finalize_invented(out)) == 1


def test_no_quantity_seed_with_observed_flag_is_not_deleted(nigeria):
    rows = seeds(nigeria, [1], {1: 'Tree crop'},
                 improved=pd.Series([False], dtype='boolean'))
    out = nigeria.assemble_plot_inputs('2018Q3', [rows])
    served = _finalize_invented(out)
    assert len(served) == 1 and served.Improved.tolist() == [False]
    assert served.u.tolist() == ['Unknown'] and served.Quantity.isna().all()
    assert served.index.get_level_values('crop').tolist() == ['Tree crop']


def test_numeric_zero_uses_its_actual_lookup_not_uganda_rule(nigeria):
    kept = seeds(nigeria, [0], {0: 'Recorded zero label'})
    assert kept.crop.tolist() == ['Recorded zero label']
    assert seeds(nigeria, [0], {}).empty


def test_fractional_seed_numeric_refusal_is_not_silently_replaced(nigeria):
    with pytest.raises(TypeError):
        seeds(nigeria, [1.25], {1: 'Maize'})


@pytest.mark.parametrize('literal', ['n/a', '(not crop-specific)', 'unknown_crop', 'Literal crop'])
def test_recorded_mapped_literal_is_not_reclassified(nigeria, literal):
    rows = seeds(nigeria, [1], {1: literal}, quantities=[2.])
    out = nigeria.assemble_plot_inputs('2010Q3', [rows])
    assert out.index.get_level_values('crop').tolist() == [literal]
    assert out.Quantity.tolist() == [2.]


@pytest.mark.parametrize('literal', ['Maize', '(not crop-specific)', 'unknown_crop', ''])
def test_explicit_nonnull_direct_labels_remain_compatible(nigeria, literal):
    out = nigeria.assemble_plot_inputs('2018Q3', [part(crop=literal, Quantity=[2.])])
    assert out.index.get_level_values('crop').tolist() == [literal]


@pytest.mark.parametrize('legacy', [None, pd.NA, float('nan'), 'n/a'])
def test_untagged_missing_or_legacy_crop_requires_provenance(nigeria, legacy):
    with pytest.raises(ValueError, match='2018Q3.*explicit source provenance'):
        nigeria.assemble_plot_inputs('2018Q3', [part(crop=legacy, Quantity=[2.])])


def test_absent_crop_column_also_requires_explicit_provenance(nigeria):
    frame = part(Quantity=[2.]).drop(columns='crop')
    with pytest.raises(ValueError, match='explicit source provenance'):
        nigeria.assemble_plot_inputs('2018Q3', [frame])


def test_untagged_hollow_null_also_needs_provenance(nigeria):
    # Deliberate direct-call compatibility boundary, before finalizer dropna.
    with pytest.raises(ValueError, match='explicit source provenance'):
        nigeria.assemble_plot_inputs('2018Q3', [part(crop=None)])


@pytest.mark.parametrize('label', ['unknown_crop', '(not crop-specific)'])
def test_explicit_provenance_relabels_without_rescuing_hollow_payload(nigeria, label):
    out = nigeria.assemble_plot_inputs('2018Q3', [part(crop=None, label=label)])
    assert out.index.get_level_values('crop').tolist() == [label]
    assert list(out.columns) == ['Quantity', 'u', 'Purchased', 'Quantity_purchased', 'Improved']
    assert out.isna().all(axis=None)
    assert _finalize_invented(out).empty


@pytest.mark.parametrize('hollow_first', [True, False])
def test_same_identity_keep_first_precedes_hollow_drop(nigeria, hollow_first):
    hollow = part(crop='n/a', label=nigeria.NO_CROP)
    observed = part(crop='n/a', label=nigeria.NO_CROP, Purchased=[False])
    parts = [hollow, observed] if hollow_first else [observed, hollow]
    out = nigeria.assemble_plot_inputs('2018Q3', parts)
    assert len(out) == 1
    served = _finalize_invented(out)
    assert len(served) == (0 if hollow_first else 1)
    if not hollow_first:
        assert served.Purchased.tolist() == [False]
        assert served.index.get_level_values('crop').tolist() == [nigeria.NO_CROP]


@pytest.mark.parametrize('null_key', [None, 'i', 'plot_id', 'input'])
def test_conflicting_desired_identities_refuse_before_legacy_keep_first(nigeria, null_key):
    frame = pd.concat([part(crop='n/a', label='Crop A', Quantity=[2.]),
                       part(crop='n/a', label=nigeria.NO_CROP, Quantity=[3.])], ignore_index=True)
    if null_key is not None:
        frame[null_key] = pd.NA
    with pytest.raises(ValueError, match='legacy crop dedup conflates.*1119/#1120'):
        nigeria.assemble_plot_inputs('2018Q3', [frame])


def test_relabel_does_not_move_legacy_sort_or_duplicate_winner(nigeria):
    frame = pd.DataFrame({'i': ['synthetic'] * 4, 'plot_id': ['p'] * 4,
                          'input': ['Seed'] * 4, 'crop': ['z', 'n/a', 'a', 'n/a'],
                          '_crop_label': ['a', nigeria.NO_CROP, 'z', nigeria.NO_CROP],
                          'Quantity': pd.array([7., 1., 9., 2.], dtype='Float64')})
    out = nigeria.assemble_plot_inputs('2018Q3', [frame])
    assert out.Quantity.tolist() == [9., 1., 7.]
    assert out.index.get_level_values('crop').tolist() == ['z', nigeria.NO_CROP, 'a']
    assert not any(c.startswith('_crop') for c in out)


def test_different_legacy_keys_cannot_collide_after_relabel(nigeria):
    frame = pd.concat([
        part(crop='n/a', label=nigeria.NO_CROP, Quantity=[2.]),
        part(crop=nigeria.NO_CROP, label=nigeria.NO_CROP, Quantity=[3.]),
    ], ignore_index=True)
    with pytest.raises(ValueError, match='crop relabel creates duplicate.*1119/#1120'):
        nigeria.assemble_plot_inputs('2018Q3', [frame])


def test_other_full_index_domains_and_typed_attrs_are_unchanged(nigeria):
    frame = pd.DataFrame({'i': pd.Categorical(['synthetic'], categories=['unused', 'synthetic']),
                          'plot_id': pd.Categorical(['p'], categories=['unused', 'p']),
                          'input': ['NPK'], 'crop': ['n/a'],
                          '_crop_label': ['n/a'],
                          'Quantity': pd.array([np.nextafter(1., 2.)], dtype='Float64')})
    frame.attrs = {'nested': [True, 0, 0.0]}
    legacy = nigeria.assemble_plot_inputs('2018Q3', [frame])
    frame['_crop_label'] = nigeria.NO_CROP
    actual = nigeria.assemble_plot_inputs('2018Q3', [frame])
    for pos, name in enumerate(legacy.index.names):
        if name != 'crop':
            _axis(legacy.index.levels[pos], actual.index.levels[pos])
            np.testing.assert_array_equal(legacy.index.codes[pos], actual.index.codes[pos], strict=True)
    _typed(legacy.attrs, actual.attrs)
    _exact(legacy.reset_index(drop=True), actual.reset_index(drop=True))


def test_real_finalizer_and_canonical_feature_alignment_preserve_structural_crop(nigeria):
    from lsms_library.feature import (_canonical_index_levels, _harmonize_country_frame,
                                     _level_aliases, _missing_level_sentinels,
                                     _fabricates_missing_levels)
    out = nigeria.assemble_plot_inputs('2018Q3', [part(crop='n/a', label=nigeria.NO_CROP,
                                                      Quantity=[0.], Purchased=[False])])
    served = _finalize_invented(out)
    report = {}
    aligned = _harmonize_country_frame(
        served, _canonical_index_levels('plot_inputs'), 'Nigeria', 'plot_inputs',
        _fabricates_missing_levels('plot_inputs'), aliases=_level_aliases('plot_inputs'),
        sentinels=_missing_level_sentinels('plot_inputs'), alignment_report=report)
    assert len(aligned) == 1 and aligned.Quantity.tolist() == [0.]
    assert aligned.index.get_level_values('j').tolist() == [nigeria.NO_CROP]
    assert aligned.index.get_level_values('u').tolist() == ['Unknown']


@pytest.mark.parametrize('value', [np.nextafter(1., 2.), np.nextafter(0., 1.)])
def test_exact_payload_oracle_rejects_nullable_float_mutation(value):
    base = pd.DataFrame({'x': pd.array([1. if value > 1 else 0., None], dtype='Float64')})
    mutant = base.copy()
    mutant.iloc[0, 0] = value
    with pytest.raises(AssertionError):
        _exact(base, mutant)
