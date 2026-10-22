"""GH1188: input purchases and gifts carry their own source quantities/units."""
from pathlib import Path
import runpy
import sys

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root


FIELDS = {
    'Unit_purchased': 'str', 'Received_as_gift': 'bool',
    'Quantity_received_as_gift': 'float', 'Unit_received_as_gift': 'str',
}


def _run(monkeypatch, *, missing=False, unit_map=None):
    root = countries_root() / 'Burkina_Faso'
    numeric = pd.DataFrame({
        'grappe': [1, 1, 1, 1], 'menage': [1, 2, 3, 4],
        's16bq01': [3] * 4, 's16bq02': [1, 1, 1, 2],
        's16bq03a': [1.25, 2.5, 3.75, 9.0], 's16bq03b': [5] * 4,
        's16bq05': [1, 2, np.nan, 1],
        's16bq07a': [2.5, np.nan, np.nan, 7.0], 's16bq07b': [4, np.nan, np.nan, 4],
        's16bq08': [2, 1, np.nan, 2],
        's16bq09a': [np.nan, 4.5, np.nan, np.nan], 's16bq09b': [np.nan, 5, np.nan, np.nan],
    })
    if missing:
        numeric.loc[:, ['s16bq05', 's16bq07a', 's16bq07b',
                        's16bq08', 's16bq09a', 's16bq09b']] = np.nan
    labelled = numeric.copy()
    labelled['s16bq01'] = 'Engrais inorganiques - Urée'
    labelled['s16bq03b'] = 'Charrett\u00e9e'
    for name in ('s16bq05', 's16bq08'):
        labelled[name] = numeric[name].map({1: 'Oui', 2: 'Non'})
    # Same numeric code5 has a different label in the usage dictionary.
    labelled['s16bq07b'] = numeric['s16bq07b'].map({4: 'Litre'})
    labelled['s16bq09b'] = numeric['s16bq09b'].map({5: 'Charrette'})
    country = Country('Burkina_Faso', preload_panel_ids=False)
    maps = {name: table.set_index('Original Label')['Preferred Label'].to_dict()
            for name, table in country.categorical_mapping.items()
            if name in {'harmonize_input', 'harmonize_seed_crop', 'u'}}
    if unit_map is not None:
        maps['u'] = unit_map
    written = []

    def source(filename, convert_categoricals=True, **kwargs):
        assert Path(filename).name == 's16b_me_bfa2018.dta'
        return (labelled if convert_categoricals else numeric).copy()

    monkeypatch.setattr(local_tools, 'get_dataframe', source)
    monkeypatch.setattr(local_tools, 'get_categorical_mapping',
                        lambda *, tablename, **kwargs: maps[tablename])
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, filename: written.append(frame.copy()))
    monkeypatch.chdir(root / '2018-19' / '_')
    monkeypatch.syspath_prepend(str(root / '_'))
    monkeypatch.delitem(sys.modules, 'burkina_faso', raising=False)
    runpy.run_path(str(root / '2018-19' / '_' / 'plot_inputs.py'),
                   run_name='__bf1188_regression__')
    assert len(written) == 1
    return written[0]


def test_purchase_and_gift_use_independent_flags_quantities_and_units(monkeypatch):
    result = _run(monkeypatch, unit_map={})
    expected = pd.DataFrame({
        'Quantity': pd.array([1.25, 2.5, 3.75], dtype='Float64'),
        'Purchased': pd.array([False, True, pd.NA], dtype='boolean'),
        'Quantity_purchased': pd.array([pd.NA, 4.5, pd.NA], dtype='Float64'),
        'Unit_purchased': pd.array([pd.NA, 'Charrette', pd.NA], dtype='string'),
        'Received_as_gift': pd.array([True, False, pd.NA], dtype='boolean'),
        'Quantity_received_as_gift': pd.array([2.5, pd.NA, pd.NA], dtype='Float64'),
        'Unit_received_as_gift': pd.array(['Litre', pd.NA, pd.NA], dtype='string'),
    })
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected,
                                  check_exact=True, check_dtype=True)
    assert result.index.names == ['t', 'i', 'input', 'crop', 'u']
    assert result.index.get_level_values('i').tolist() == ['1001', '1002', '1003']
    assert result.index.get_level_values('u').tolist() == ['Charrett\u00e9e'] * 3
    assert result.index.get_level_values('crop').tolist() == ['(not crop-specific)'] * 3


def test_missing_acquisition_fields_stay_missing_even_with_known_used_unit(monkeypatch):
    result = _run(monkeypatch, missing=True, unit_map={})
    assert result['Quantity'].tolist() == [1.25, 2.5, 3.75]
    assert result.drop(columns='Quantity').isna().all(axis=None)
    assert result['Purchased'].dtype == pd.BooleanDtype()
    assert result['Received_as_gift'].dtype == pd.BooleanDtype()
    for name in ('Quantity', 'Quantity_purchased', 'Quantity_received_as_gift'):
        assert result[name].dtype == pd.Float64Dtype()
    for name in ('Unit_purchased', 'Unit_received_as_gift'):
        assert result[name].dtype == pd.StringDtype()


def test_unit_formatter_uses_exact_own_labels_without_code5_equivalence(monkeypatch):
    result = _run(monkeypatch, unit_map={'Charrett\u00e9e': 'Curated used cart',
                                       'Litre': 'Curated litre'})
    assert result.index.get_level_values('u').tolist() == ['Curated used cart'] * 3
    assert result['Unit_purchased'].iloc[1] == 'Charrette'
    assert result['Unit_received_as_gift'].iloc[0] == 'Curated litre'
    assert result['Unit_purchased'].isna().tolist() == [True, False, True]
    assert result['Unit_received_as_gift'].isna().tolist() == [False, True, True]


@pytest.mark.parametrize('name,type_name', FIELDS.items())
def test_only_the_new_country_local_fields_are_optional(name, type_name):
    scheme = yaml.safe_load((countries_root() / 'Burkina_Faso' / '_' /
                             'data_scheme.yml').read_text())['Data Scheme']['plot_inputs']
    assert scheme[name] == {'type': type_name, 'optional': True}
    assert scheme['Quantity'] == 'float'
    assert scheme['Purchased'] == 'bool'
    assert scheme['Quantity_purchased'] == 'float'
