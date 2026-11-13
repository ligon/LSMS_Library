"""Cached-source checks for the W2 education population documented in CONTENTS.

No acquisition: skip unless the declared PH Stata blob is already in local L1.
These checks complement PR 1318's synthetic timing tests; they do not assert
that the PH series covers the full education population or that PP has no data.
"""
import pandas as pd
import pytest

from lsms_library import Country
from lsms_library import local_tools as L
from lsms_library.paths import countries_root


@pytest.fixture(scope="module")
def ph_source():
    path = (countries_root() / 'Nigeria/2012-13/Data/Post Harvest Wave 2'
            / 'Household/sect2a_harvestw2.dta')
    if L._dvc_cache_path(path) is None:
        pytest.skip('W2 PH education source is not cached locally; no acquisition')
    return L.get_dataframe(path)


def test_w2_ph_attainment_includes_current_students(ph_source):
    """The source contradicts the old left-school-only explanation."""
    raw = ph_source
    observed = raw.s2aq9.notna()
    routed = (raw.s2aq1.eq('YES') & raw.s2aq2.eq('YES')
              & raw.s2aq6.eq('YES'))
    pd.testing.assert_series_equal(observed, routed, check_names=False)
    assert observed.sum() == 313
    assert raw.loc[observed, 's2aq13'].value_counts().to_dict() == {
        'YES': 175, 'NO': 138,
    }
    assert not raw.duplicated(['hhid', 'indiv']).any()


def test_w2_ph_wave_preserves_source_keys_nulls_and_mapped_answers(
        ph_source, monkeypatch):
    """Exercise the real reader and YAML extractor, without an L2 shortcut."""
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    wave = Country('Nigeria')['2013Q1']
    frame = wave.individual_education()
    assert len(frame) == len(ph_source)
    assert frame.index.get_level_values('i').tolist() == [
        L.format_id(value) for value in ph_source.hhid]
    assert frame.index.get_level_values('pid').tolist() == [
        L.format_id(value) for value in ph_source.indiv]
    assert set(frame.index.get_level_values('t')) == {'2013Q1'}
    actual = frame['Educational Attainment'].reset_index(drop=True)
    assert actual.isna().tolist() == ph_source.s2aq9.isna().tolist()
    mapping = wave.categorical_mapping['harmonize_education'].set_index(
        'Original Label')['Preferred Label'].to_dict()
    expected = ph_source.s2aq9.map(mapping)
    assert expected.notna().sum() == ph_source.s2aq9.notna().sum()
    pd.testing.assert_series_equal(actual, expected, check_names=False,
                                   check_dtype=False)
