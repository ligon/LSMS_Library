"""GH1483: the real wave callback must resolve its writer outside source."""
from copy import deepcopy
from pathlib import Path
import runpy
import sys

import pandas as pd

from lsms_library import local_tools
from lsms_library.paths import countries_root, data_root


def test_actual_wrapper_helper_writer_resolves_private_wave_and_keeps_payload(monkeypatch, tmp_path):
    root = Path(__file__).resolve().parents[1]
    assert countries_root().resolve() == root / 'lsms_library/countries'
    country = countries_root() / 'Burkina_Faso'
    wave = country / '2021-22/_'
    private = tmp_path / 'data'
    monkeypatch.setenv('LSMS_DATA_DIR', str(private))
    assert data_root().resolve() == private.resolve()
    source = pd.DataFrame({
        'grappe': [2], 'menage': [126], 'vague': [1], 's16bq01': [3],
        's16bq02': [1], 's16bq03a': [1.25], 's16bq03b': [2],
        's16bq05': [1], 's16bq07a': [2.5], 's16bq07b': [4],
        's16bq08': [2], 's16bq09a': [111.], 's16bq09b': [5],
        's16bq09c': [11100],
    }, index=[40])
    source.attrs = {'invented': ('retained', False)}
    before = source.copy(deep=True)
    before_attrs = deepcopy(source.attrs)
    original_reader = local_tools.get_dataframe
    reads, writes = [], []

    def invented_leaf(filename, *args, **kwargs):
        if Path(filename).suffix == '.dta':
            assert filename == '../Data/s16b_me_bfa2021.dta'
            assert kwargs == {'convert_categoricals': False}
            reads.append(filename)
            return source.copy(deep=True)
        return original_reader(filename, *args, **kwargs)

    def parquet_leaf(frame, filename, *, engine, index):
        # This spy is BELOW the unchanged to_parquet resolver; wrapping that
        # writer itself would change the inspected caller and hide the defect.
        path = Path(filename).resolve()
        expected = private / 'Burkina_Faso/2021-22/_/plot_inputs.parquet'
        assert path.parent == expected.parent
        assert path.name.startswith(expected.name + '.tmp.')
        assert not path.is_relative_to(root)
        assert engine == 'pyarrow' and index is True
        writes.append(frame.copy(deep=True))
        path.write_bytes(b'inert private parquet-leaf witness')

    monkeypatch.setattr(local_tools, 'get_dataframe', invented_leaf)
    monkeypatch.setattr(pd.DataFrame, 'to_parquet', parquet_leaf)
    monkeypatch.chdir(wave)
    # Wave imports are actual country/helper source, scoped to this test.
    monkeypatch.syspath_prepend(str(country / '_'))
    for name in ['burkina_faso', 's16b_2021']:
        monkeypatch.delitem(sys.modules, name, raising=False)
    try:
        runpy.run_path(str(wave / 'plot_inputs.py'), run_name='__main__')
    finally:
        for name in ['burkina_faso', 's16b_2021']:
            sys.modules.pop(name, None)
    expected = pd.DataFrame({
        't': pd.array(['2021-22'], dtype='string'),
        'i': pd.array(['2126'], dtype='string'),
        'input': pd.array(['Urea'], dtype='string'),
        'crop': pd.array(['(not crop-specific)'], dtype='string'),
        'u': pd.array(['Kilogramme'], dtype='string'),
        'Quantity': pd.array([1.25], dtype='Float64'),
        'Purchased': pd.array([False], dtype='boolean'),
        'Quantity_purchased': pd.array([111.], dtype='Float64'),
        'Unit_purchased': pd.array(['Charrette'], dtype='string'),
        'Received_as_gift': pd.array([True], dtype='boolean'),
        'Quantity_received_as_gift': pd.array([2.5], dtype='Float64'),
        'Unit_received_as_gift': pd.array(['Litre'], dtype='string'),
        **{name: source[name].tolist() for name in source.columns},
    }).set_index(['t', 'i', 'input', 'crop', 'u'])
    expected.attrs = {
        'invented': ('retained', False),
        's16b_2021_candidate': {
            'excluded_explicit_nonuse': 0,
            'purchase_quantity': 's16bq09a; own q09b Unité / q09c Montant',
            'component': 'panel source only; no Cross_Section coverage claim',
        },
    }
    assert reads == ['../Data/s16b_me_bfa2021.dta'] and len(writes) == 1
    pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)
    assert writes[0].attrs == expected.attrs
    assert type(writes[0].attrs['invented']) is tuple
    assert type(writes[0].attrs['invented'][1]) is bool
    pd.testing.assert_frame_equal(source, before, check_exact=True)
    assert source.attrs == before_attrs
    output = private / 'Burkina_Faso/2021-22/_/plot_inputs.parquet'
    assert output.read_bytes() == b'inert private parquet-leaf witness'
    assert not (wave / 'plot_inputs.parquet').exists()
