"""GH #1721 relabels a W3 sale, not its grain, quantities or factor."""
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import local_tools
from lsms_library.transformations import _sold_kg_factors
from tests.test_gh843_nigeria_household_crop_sales import LABELS, _harvest_frame, ng


W3 = '2015-16'
W4 = '2018-19'
W5 = '2023-24'
UNIT_LABELS = {
    3: '3. LITRE (L)',
    4: '4. CENTILITRE (CL)',
    190: '190. JERRY CAN/RUBBER 10 LITRES',
    191: '191. JERRY CAN/RUBBER 20 LITRES',
    192: '192. JERRY CAN/RUBBER 25 LITRES',
    193: '193. JERRY CAN/RUBBER 50 LITRES',
}
ROWS = [
    ('h190', 190, 1., 100.),
    ('h191', 191, 2., 200.),
    ('h191', 191, 3., 300.),
    ('h192', 192, 4., 400.),
    ('h193', 193, 5., 500.),
    ('h3', 3, 6., 600.),
    ('h4', 4, 7., 700.),
    ('mixed', 191, 8., 800.),
    ('mixed', 192, 9., 900.),
]


def plant(ng, folder, monkeypatch, *, labels=UNIT_LABELS):
    spec = ng._HH_SALE_SOURCE[folder]
    raw = pd.DataFrame(ROWS, columns=['hhid', spec['unit'], spec['qty'], spec['value']])
    raw[spec['unit']] = raw[spec['unit']].astype(float)
    raw['cropcode'] = 1010
    raw[spec['sold']] = 1
    if folder == W3:
        raw['crop_number'] = range(1, len(raw) + 1)
    for field, val in [('size', 1), ('condition', 2), ('conv', 2.5)]:
        if field in spec:
            raw[spec[field]] = val
    decoded = raw.copy()
    decoded['cropcode'] = '1010. MAIZE'
    decoded[spec['unit']] = raw[spec['unit']].map(labels)

    def get_dataframe(path, *, convert_categoricals):
        assert Path(path).as_posix().endswith(f"{folder}/{spec['fn']}")
        return (decoded if convert_categoricals else raw).copy()

    monkeypatch.setattr(local_tools, 'get_dataframe', get_dataframe)
    return raw


def test_w3_decoder_changes_only_approved_labels_and_not_sold_factors(ng, monkeypatch):
    raw = plant(ng, W3, monkeypatch)
    new = ng.hh_crop_sale_records(W3, LABELS)
    with monkeypatch.context() as old:
        old.setattr(ng, 'W3_HARVEST_UNIT_OVERRIDE', {})
        base = ng.hh_crop_sale_records(W3, LABELS)
    mask = raw['sa3iiq5b'].isin([190, 191, 192])
    assert new.loc[mask, 'Unit_sold'].tolist() == ['Jerry Can'] * int(mask.sum())
    assert base.loc[mask, 'Unit_sold'].tolist() == ['l'] * int(mask.sum())
    assert new.loc[~mask, 'Unit_sold'].tolist() == ['l', 'l', 'cl']  # 193, 3, 4
    pd.testing.assert_frame_equal(new.drop(columns='Unit_sold'),
                                  base.drop(columns='Unit_sold'))
    assert new['KgFactor_sold'].isna().all()
    assert ng._canon_unit(UNIT_LABELS[191]) == 'l'

    # A nonmissing Jerry Can is not a plain litre and must not borrow Kg.
    old_factor, old_counts = _sold_kg_factors(base.assign(u='Kg'))
    new_factor, new_counts = _sold_kg_factors(new.assign(u='Kg'))
    assert old_factor[mask].tolist() == [1.] * int(mask.sum())
    assert new_factor[mask].isna().all()
    pd.testing.assert_series_equal(new_factor[~mask], old_factor[~mask])
    assert new_counts['unresolved'] == old_counts['unresolved'] + int(mask.sum())
    assert new_counts['harvest_fallback_attempted'] == old_counts['harvest_fallback_attempted'] == 0


@pytest.mark.parametrize('folder', [W4, W5])
def test_other_waves_keep_readers_scoped(ng, monkeypatch, folder):
    # The plain can matches W4's card. The other labels are deliberate
    # negative controls: even identical W3-like spellings cannot widen W3.
    labels = {**UNIT_LABELS, 190: '190. JERRY CAN/RUBBER'}
    plant(ng, folder, monkeypatch, labels=labels)
    seen = ng.hh_crop_sale_records(folder, LABELS)
    assert seen['Unit_sold'].tolist() == [ng._canon_unit(labels[code]) for _, code, *_ in ROWS]
    assert seen['KgFactor_sold'].tolist() == [2.5] * len(ROWS)


def test_attachment_and_derivation_inputs_only_change_served_unit_label(ng, monkeypatch):
    raw = plant(ng, W3, monkeypatch)
    served = pd.DataFrame({
        'i': ['h190', 'h191', 'mixed', 'mixed'],
        'plot_id': ['p1', 'p2', 'm1', 'm2'],
        'crop': ['Maize'] * 4,
        'production_obs': ['line-1', 'line-1', 'line-1', 'line-1'],
        'Quantity': [1., 1., 1., 1.],
    })
    monkeypatch.setattr(ng, '_crop_labels_from_anywhere', lambda: LABELS)
    monkeypatch.setattr(ng, '_served_plot_crop_keys', lambda folder, labels: served)
    readers = (
        ng.inputs_hh_crop_sale_single_row,
        ng.inputs_hh_crop_sale_records_summed,
        ng.inputs_hh_crop_sale_two_units,
    )
    harvest = _harvest_frame([(h, p, 1010, 1.) for h, p in zip(served['i'], served['plot_id'])])
    candidate_inputs = [reader('2016Q1') for reader in readers]
    records = ng.hh_crop_sale_records(W3, LABELS)
    attached = ng.derive_hh_crop_sale_attachment(records, served)
    built = ng.crop_production_for_wave('2016Q1', [harvest], LABELS, hh_sales=records)
    with monkeypatch.context() as old:
        old.setattr(ng, 'W3_HARVEST_UNIT_OVERRIDE', {})
        baseline_inputs = [reader('2016Q1') for reader in readers]
        baseline_records = ng.hh_crop_sale_records(W3, LABELS)
        base = ng.derive_hh_crop_sale_attachment(baseline_records, served)
        built_base = ng.crop_production_for_wave('2016Q1', [harvest], LABELS,
                                                hh_sales=baseline_records)

    pd.testing.assert_frame_equal(built.drop(columns='Unit_sold'),
                                  built_base.drop(columns='Unit_sold'))
    changed = ~built['Unit_sold'].eq(built_base['Unit_sold']).fillna(
        built['Unit_sold'].isna() & built_base['Unit_sold'].isna())
    assert built.loc[changed, 'Unit_sold'].tolist() == ['Jerry Can'] * 3
    assert set(built.loc[changed].index.get_level_values('i')) == {'h190', 'h191', 'h192'}
    pd.testing.assert_frame_equal(attached.drop(columns='Unit_sold'),
                                  base.drop(columns='Unit_sold'))
    assert attached.loc[('h191', 'Maize'), 'Unit_sold'] == 'Jerry Can'
    assert attached.loc[('h191', 'Maize'), 'Quantity_sold'] == 5.
    assert pd.isna(attached.loc[('mixed', 'Maize'), 'Quantity_sold'])
    assert pd.isna(attached.loc[('mixed', 'Maize'), 'Unit_sold'])
    assert attached.loc[('mixed', 'Maize'), 'Value_sold'] == 1700.
    assert attached.loc[('mixed', 'Maize'), 'plot_id'] == ng.FARM_LEVEL
    for new, old in zip(candidate_inputs, baseline_inputs):
        assert not new.empty
        assert 'Unit_sold' not in new.columns and 'sa3iiq5b' in new.columns
        pd.testing.assert_frame_equal(new, old)
        for column in new.columns:
            expected = raw.loc[new.index.get_level_values('source_row'), column].to_numpy()
            assert new[column].tolist() == expected.tolist()
