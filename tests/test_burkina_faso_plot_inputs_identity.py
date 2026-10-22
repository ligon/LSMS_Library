"""GH #1123: keep every native input slot before Country can collapse it.

The source's unlabelled slot 21 is not a new crop classification.  Its existing
Seed input binding remains legacy curation; the qualified crop token preserves
identity.  Equal payloads on two native slots still require two rows.
"""
from functools import lru_cache
from pathlib import Path
import runpy
import sys

import numpy as np
import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root


UNKNOWN_21 = 'Unknown [Burkina_Faso 2018-19 s16bq01=21]'
CROP_BY_CODE = {
    11: 'Mil', 12: 'Sorgho', 13: 'Maïs', 14: 'Riz Paddy',
    15: 'Autre céréale', 16: 'Coton', 17: 'Sésame',
    18: 'Niébé', 19: 'Autre tubercule',
    20: 'Autre crop', 21: UNKNOWN_21,
}
INPUT_BY_CODE = {
    1: 'Organic manure', 2: 'Organic compost', 3: 'Urea', 4: 'Phosphate',
    5: 'NPK', 6: 'DAP', 7: 'Pesticide', 8: 'Herbicide',
    9: 'Fungicide', 10: 'Other phytosanitary',
}
SOURCE_LABELS = {
    3: 'Engrais inorganiques - Urée',
    15: "Semences d'autres céréales", 19: 'Plants/boutures de tubercules',
    20: 'Autres semences', 21: 21,
}


def _root():
    return countries_root() / 'Burkina_Faso'


@lru_cache(maxsize=1)
def _maps():
    country = Country('Burkina_Faso', preload_panel_ids=False)
    return {name: table.set_index('Original Label')['Preferred Label'].to_dict()
            for name, table in country.categorical_mapping.items()
            if name in {'harmonize_input', 'harmonize_seed_crop', 'u'}}


def _run_script(monkeypatch, codes, *, seed_override=None, purchased=False,
                missing_unit=False):
    """Run the actual script with source-shaped observations and a write spy."""
    numeric = pd.DataFrame({
        'grappe': [1] * len(codes), 'menage': [1] * len(codes),
        's16bq01': codes, 's16bq02': [1] * len(codes),
        's16bq03a': [1.25] * len(codes),
        's16bq03b': [np.nan if missing_unit else 1] * len(codes),
        's16bq05': [1 if purchased else 2] * len(codes),
        's16bq07a': [0.75 if purchased else np.nan] * len(codes),
    })
    labelled = numeric.copy()
    labelled['s16bq01'] = labelled['s16bq01'].map(SOURCE_LABELS)
    labelled['s16bq03b'] = np.nan if missing_unit else 'Kilogramme'
    labelled['s16bq05'] = 'Oui' if purchased else 'Non'
    maps = {name: dict(values) for name, values in _maps().items()}
    if seed_override:
        maps['harmonize_seed_crop'].update(seed_override)
    written = []

    def source(filename, convert_categoricals=True, **kwargs):
        assert Path(filename).name == 's16b_me_bfa2018.dta'
        return (labelled if convert_categoricals else numeric).copy()

    monkeypatch.setattr(local_tools, 'get_dataframe', source)
    monkeypatch.setattr(local_tools, 'get_categorical_mapping',
                        lambda *, tablename, **kwargs: maps[tablename])
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, filename: written.append(frame.copy()))
    monkeypatch.chdir(_root() / '2018-19' / '_')
    monkeypatch.syspath_prepend(str(_root() / '_'))
    monkeypatch.delitem(sys.modules, 'burkina_faso', raising=False)
    script = _root() / '2018-19' / '_' / 'plot_inputs.py'
    try:
        runpy.run_path(str(script), run_name='__bf1123_regression__')
    except AssertionError:
        assert not written, 'uniqueness must be checked before the parquet write'
        raise
    assert len(written) == 1
    return written[0]


def test_existing_map_separates_all_four_present_native_slots():
    mapping = _maps()['harmonize_seed_crop']
    assert [mapping[k] for k in ("Semences d'autres céréales",
                                'Plants/boutures de tubercules',
                                'Autres semences', '21')] == [
        'Autre céréale', 'Autre tubercule', 'Autre crop', UNKNOWN_21]
    # Aliases not involved in this repair remain intact.
    assert mapping['20'] == 'Autre crop'
    assert mapping['Semences de césame'] == mapping['Semences de sésame']


@pytest.mark.parametrize('purchased', [False, True])
def test_equal_payloads_on_distinct_native_slots_all_survive(monkeypatch, purchased):
    result = _run_script(monkeypatch, [15, 19, 20, 21], purchased=purchased)
    assert len(result) == 4
    assert set(result.index.get_level_values('crop')) == {
        'Autre céréale', 'Autre tubercule', 'Autre crop', UNKNOWN_21}
    assert set(result.index.get_level_values('input')) == {'Seed'}
    assert result['Quantity'].tolist() == [1.25] * 4
    assert result['Purchased'].tolist() == [purchased] * 4
    if purchased:
        assert result['Quantity_purchased'].tolist() == [0.75] * 4
    else:
        assert result['Quantity_purchased'].isna().all()


@pytest.mark.parametrize('override', [
    # The incomplete Benin/Togo copy: named categories split, but 21 pooled.
    {'21': 'Autre crop'},
    {"Semences d'autres céréales": 'Autre crop',
     'Plants/boutures de tubercules': 'Autre crop', '21': 'Autre crop'},
])
def test_lossy_mapping_mutants_fail_before_writing(monkeypatch, override):
    with pytest.raises(AssertionError, match='Burkina_Faso plot_inputs 2018-19:'):
        _run_script(monkeypatch, [15, 19, 20, 21], seed_override=override)


def test_future_even_identical_native_key_repeat_fails_before_writing(monkeypatch):
    with pytest.raises(AssertionError, match=r'1 duplicate tuple\(s\)') as error:
        _run_script(monkeypatch, [20, 20])
    # The production error names the aggregate scope, never a source tuple.
    assert '(1,' not in str(error.value)


def test_nonseed_missing_unit_and_native_fractional_payload_are_unchanged(monkeypatch):
    result = _run_script(monkeypatch, [3], missing_unit=True)
    assert result.index.names == ['t', 'i', 'input', 'crop', 'u']
    assert result.index.get_level_values('input').tolist() == ['Urea']
    assert result.index.get_level_values('crop').tolist() == ['(not crop-specific)']
    assert result.index.get_level_values('u').tolist() == ['Manquant']
    assert result['Quantity'].tolist() == [1.25]


@pytest.fixture(scope='module')
def actual_country():
    return Country('Burkina_Faso')


@pytest.mark.requires_s3
def test_every_real_native_line_and_full_payload_survives(actual_country):
    source = _root() / '2018-19' / 'Data' / 's16b_me_bfa2018.dta'
    raw = local_tools.get_dataframe(source, convert_categoricals=False)
    labelled = local_tools.get_dataframe(source, convert_categoricals=True)
    raw = raw.loc[raw['s16bq02'] == 1]
    labelled = labelled.loc[raw.index]
    native_key = ['grappe', 'menage', 's16bq01']
    assert not raw[native_key].isna().any(axis=None)
    assert not raw.duplicated(native_key).any()
    assert len(raw) == 22877
    actual = actual_country.plot_inputs(waves=['2018-19']).reset_index()
    assert len(actual) == len(raw)
    reverse_crops = {label: code for code, label in CROP_BY_CODE.items()}
    reverse_inputs = {label: code for code, label in INPUT_BY_CODE.items()}
    actual['_native_code'] = pd.array(
        [reverse_crops[crop] if kind == 'Seed' else reverse_inputs[kind]
         for kind, crop in zip(actual['input'], actual['crop'])],
        dtype=raw['s16bq01'].dtype)
    hh = (raw['grappe'].map(local_tools.format_id) + '0'
          + raw['menage'].map(lambda x: local_tools.format_id(x, zeropadding=2)))
    expected = pd.DataFrame({
        'i': hh.to_numpy(), '_native_code': raw['s16bq01'].to_numpy(),
        'u': labelled['s16bq03b'].astype('string').replace(
            {'Kilogramme': 'Kg'}).to_numpy(),
        'Quantity': pd.array(raw['s16bq03a'], dtype='Float64'),
        'Purchased': pd.array(raw['s16bq05'].map({1: True, 2: False}), dtype='boolean'),
        'Quantity_purchased': pd.array(raw['s16bq07a'], dtype='Float64'),
    }).set_index(['i', '_native_code']).sort_index()
    got = actual.set_index(['i', '_native_code'])[expected.columns].sort_index()
    assert got.index.is_unique
    assert set(got.index) == set(expected.index)
    # Align index encoding; core canonically uses pandas str, the raw source
    # uses nullable strings. Measurement dtypes/NA patterns remain exact.
    pd.testing.assert_frame_equal(got.reindex(expected.index), expected,
                                  check_exact=True, check_dtype=True)


@pytest.mark.requires_s3
def test_source21_is_unlabelled_and_qualified_only_in_its_current_wave(actual_country):
    cats = local_tools.get_dataframe(
        _root() / '2018-19' / 'Data' / 's16b_me_bfa2018.dta', categories_only=True)
    assert 21 not in cats['s16bq01'] and cats['s16bq01'][20] == 'Autres semences'
    result = actual_country.plot_inputs()
    selected = result.index.get_level_values('crop') == UNKNOWN_21
    assert int(np.count_nonzero(selected)) == 602
    assert set(result.index.get_level_values('t')[selected]) == {'2018-19'}
    assert set(result.index.get_level_values('input')[selected]) == {'Seed'}
    for wave in ['2014', '2021-22']:
        assert actual_country.plot_inputs(waves=[wave]).empty
