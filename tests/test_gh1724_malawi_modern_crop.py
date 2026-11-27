"""Source-free tests of Malawi 2024-25's own G and I scope."""
from pathlib import Path
import runpy

import pandas as pd
import pytest

from lsms_library import Country, local_tools


MALAWI = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Malawi'
DRIVER = MALAWI / '2024-25/_/crop_production.py'
XS = '../Data/Cross_Sectional/'
PN = '../Data/Panel/'
SOURCE_PATHS = {
    XS + 'hh_mod_a_filt.dta', PN + 'hh_mod_a_filt_24.dta',
    XS + 'ag_mod_g.dta', PN + 'ag_mod_g_24.dta',
    XS + 'ag_mod_i.dta', PN + 'ag_mod_i_24.dta',
}


def _g(id_field, records):
    return pd.DataFrame([{
        id_field: hh, 'gardenid': 'g1', 'plotid': plot, 'crop_code': code,
        'ag_g13a': qty, 'ag_g13b': unit, 'ag_g13c': 2,
        'ag_g13b_oth': '90 KG BAG', 'ag_g08a': 12345, 'ag_g07': 2,
        'ag_g01': 1, 'ag_g05a': 10, 'ag_g05b': 2024, 'ag_g12b': 2,
    } for hh, plot, code, qty, unit in records])


def _i(id_field, records):
    return pd.DataFrame([{
        id_field: hh, 'crop_code': code, 'ag_i02a': qty, 'ag_i03': value,
        'ag_i01': 1, 'ag_i02b': unit, 'ag_i02c': 1,
        'ag_i02b_oth': 'BASKET (DENGU)',
    } for hh, code, qty, value, unit in records])


@pytest.fixture
def modern_crop(monkeypatch):
    files = {
        XS + 'hh_mod_a_filt.dta': pd.DataFrame({'case_id': ['xs', 'shared']}),
        PN + 'hh_mod_a_filt_24.dta': pd.DataFrame({
            'y5_hhid': ['retained', 'refresh'], 'y4_hhid': ['previous', ''],
            # A retained case_id can look like an IHS6 case. y4_hhid distinguishes it.
            'case_id': ['xs', 'shared'],
        }),
        XS + 'ag_mod_g.dta': _g('case_id', [
            ('xs', 'a', 48, 0, 1), ('xs', 'a', 49, None, 2),
            ('shared', 'a', 50, 3, 13),
        ]),
        PN + 'ag_mod_g_24.dta': _g('y5_hhid', [
            ('refresh', 'a', 50, 999, 1), ('refresh', 'b', 50, 1, 1),
            ('retained', 'a', 50, 8, 1),
        ]),
        XS + 'ag_mod_i.dta': _i('case_id', [
            ('xs', 49, 2, 88, 2), ('xs', 48, 0, None, 1),
            ('shared', 50, None, 5, 13),
        ]),
        PN + 'ag_mod_i_24.dta': _i('y5_hhid', [
            ('refresh', 50, 33, 999, 1), ('retained', 50, 2, 20, 1),
        ]),
    }
    calls = []
    def read(path, **kw):
        calls.append(path)
        assert kw.get('convert_categoricals') is False
        return files[path].copy()

    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    monkeypatch.syspath_prepend(str(MALAWI / '_'))
    # The cover bridge already has separate exact-source verification. Pin its
    # contract here so this test never reads or writes actual survey data.
    import malawi
    assert Path(malawi.__file__).resolve() == (MALAWI / '_/malawi.py').resolve()
    monkeypatch.setattr(malawi, 'ihps24_refreshed', lambda: {'refresh': 'shared'})
    runner = runpy.run_path(str(DRIVER))
    return files, calls, runner['build_crop_production']


def test_rainy_harvest_and_unallocated_native_sales(modern_crop):
    _, calls, build = modern_crop
    rows = build().reset_index()
    assert set(calls) == SOURCE_PATHS and len(calls) == len(SOURCE_PATHS)
    assert rows['t'].unique().tolist() == ['2024-25']
    assert set(rows['i']) == {'cs-24-xs', 'cs-24-shared', 'retained'}
    g = rows[rows['production_obs'].str.startswith('reported:G:')].set_index(
        ['i', 'production_obs'])
    i = rows[rows['production_obs'].str.startswith('reported:I:')].set_index(
        ['i', 'production_obs'])
    assert len(g) == 4 and len(i) == 3
    assert g.loc[('cs-24-xs', 'reported:G:48'), 'crop'] == 'Other (Specify) 1'
    assert g.loc[('cs-24-xs', 'reported:G:49'), 'crop'] == 'Other (Specify) 2'
    assert g.loc[('cs-24-xs', 'reported:G:48'), 'Quantity'] == 0
    assert pd.isna(g.loc[('cs-24-xs', 'reported:G:49'), 'Quantity'])
    # G07 says not complete and G08 says 12345. Only the IHS6 G13 is served.
    assert g.loc[('cs-24-shared', 'reported:G:50'), 'Quantity'] == 3
    assert g.loc[('cs-24-shared', 'reported:G:50'), 'u'] == 'Other (Specify)'
    assert g.loc[('cs-24-shared', 'reported:G:50'), 'condition'] == 'unshelled'
    assert g.loc[('cs-24-shared', 'reported:G:50'), 'crop'] == 'Cowpea (Khobwe)'
    assert g[['Quantity_sold', 'Value_sold', 'Unit_sold', 'Condition_sold']].isna().all().all()
    assert set(g['plot_id']) == {'g1_a'}
    assert set(i['plot_id']) == {'Farm-level'}
    assert set(i['u']) == {'Unknown'}
    assert set(i['condition']) == {'unknown_condition'}
    assert set(i['Condition_sold']) == {'shelled'}
    assert i['Quantity'].isna().all()
    assert i.loc[('cs-24-xs', 'reported:I:49'), 'Quantity_sold'] == 2
    assert i.loc[('cs-24-xs', 'reported:I:49'), 'Unit_sold'] == '50 kg Bag'
    assert i.loc[('cs-24-xs', 'reported:I:49'), 'crop'] == 'Other (Specify) 2'
    assert pd.isna(i.loc[('cs-24-shared', 'reported:I:50'), 'Quantity_sold'])
    assert i.loc[('cs-24-shared', 'reported:I:50'), 'Unit_sold'] == 'Other (Specify)'
    assert i.loc[('cs-24-shared', 'reported:I:50'), 'Value_sold'] == 5
    assert i.loc[('retained', 'reported:I:50'), 'Value_sold'] == 20
    # Both harvest and sale name cowpea from their native code, not from text.
    native_cowpea = rows['production_obs'].isin({'reported:G:50', 'reported:I:50'})
    assert rows['crop'].eq('Cowpea (Khobwe)').tolist() == native_cowpea.tolist()
    assert rows['Derivation'].isna().all()
    assert not rows['perennial'].any()
    assert 'season' not in rows
    assert not rows.duplicated(['t', 'i', 'plot_id', 'crop', 'u', 'condition', 'production_obs']).any()


@pytest.mark.parametrize('case', ('cover', 'orphan', 'duplicate-harvest', 'duplicate-sale', 'bad-route'))
def test_source_contract_fails_instead_of_silently_reallocating(modern_crop, case):
    files, _, build = modern_crop
    if case == 'cover':
        files[PN + 'hh_mod_a_filt_24.dta'].loc[1, 'case_id'] = 'xs'
        expected = 'invalid refreshment cover bridge'
    elif case == 'orphan':
        files[PN + 'ag_mod_i_24.dta'].loc[1, 'y5_hhid'] = 'orphan'
        expected = 'outside its own cover'
    elif case == 'duplicate-harvest':
        path = XS + 'ag_mod_g.dta'
        files[path] = pd.concat([files[path], files[path].iloc[:1]], ignore_index=True)
        expected = 'repeated native key'
    elif case == 'duplicate-sale':
        path = XS + 'ag_mod_i.dta'
        files[path] = pd.concat([files[path], files[path].iloc[:1]], ignore_index=True)
        expected = 'repeated native key'
    else:
        files[XS + 'ag_mod_i.dta'].loc[0, 'ag_i01'] = 2
        expected = 'positive I record without Sale Yes'
    with pytest.raises(ValueError, match=expected):
        build()


def test_native_source_paths_bind_wave_fingerprints_and_make_inputs():
    assert set(local_tools.scan_script_data_refs(DRIVER)) == SOURCE_PATHS | {'crop_production.parquet'}
    for logical in SOURCE_PATHS:
        assert (DRIVER.parent / (logical + '.dvc')).is_file()
    country = Country('Malawi', preload_panel_ids=False)
    assert 'crop_production' in country['2024-25'].data_scheme
    _, make_paths = country._make_declared_inputs('crop_production', _with_sources=True)
    assert {str((DRIVER.parent / path).resolve()) for path in SOURCE_PATHS} <= {
        str(path.resolve()) for path in make_paths
    }
    make = (MALAWI / '_/Makefile').read_text()
    crop_waves, = [line for line in make.splitlines() if line.startswith('crop_waves :=')]
    assert crop_waves.split()[2:] == ['2010-11', '2013-14', '2016-17', '2019-20', '2024-25']
    sidecars = make.split('crop_2024_sidecars :=', 1)[1].split('\n$(')[0]
    assert {word for word in sidecars.split() if word.endswith('.dvc')} == {
        path.replace('../Data/', '../2024-25/Data/') + '.dvc' for path in SOURCE_PATHS
    }
