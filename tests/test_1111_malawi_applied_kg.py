"""Actual Malawi drivers bind enumerator totals to kg before grouping.

Questionnaire evidence for all four waves is in ledger #1111.
The raw native quantity/unit pair and timing deliberately disagree with the
selected total here, so decoding a nearby field cannot pass by coincidence.
"""
import contextlib
import importlib.util
import io
import runpy
import sys

import pandas as pd
import pyreadstat
import pytest

from lsms_library import local_tools
from lsms_library.paths import countries_root


@pytest.fixture
def drive(monkeypatch):
    root = countries_root() / 'Malawi'
    spec = importlib.util.spec_from_file_location(
        'malawi_1111_test', root / '_' / 'malawi.py')
    malawi = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(malawi)
    monkeypatch.setitem(sys.modules, 'malawi', malawi)

    def run(wave, timings=(1, 12), amounts=(7.0, 11.0)):
        # IHS4/IHS5 layout (f gate, g type, j total) vs IHS3/IHPS2 (f type,
        # i total, j timing).
        modern = wave in MODERN
        captures = {'blocks': [], 'pieces': [], 'writes': [], 'dvcfs': []}
        block = malawi._fertilizer_block
        assemble = malawi.assemble_plot_inputs

        def capture_block(*args, **kwargs):
            result = block(*args, **kwargs)
            captures['blocks'].append((kwargs, result))
            return result

        def capture_assembly(t, pieces, purchases):
            captures['pieces'] = [p.copy(deep=True) for p in pieces]
            return assemble(t, pieces, purchases)

        monkeypatch.setattr(malawi, '_fertilizer_block', capture_block)
        monkeypatch.setattr(malawi, 'assemble_plot_inputs', capture_assembly)

        def source(path, **kwargs):
            panel = '/Panel/' in path
            identity = '700' if panel else '7'
            # 2013-14 source names are upper case (AG_MOD_D_13.dta).
            name = path.lower()
            ids = {'case_id': [identity], 'y2_hhid': [identity],
                   'y3_hhid': [identity], 'y4_hhid': [identity]}
            if 'ag_mod_d' in name:
                d = pd.DataFrame({
                    **{k: v * 2 for k, v in ids.items()},
                    'ag_d00': ['R1', 'R2'], 'gardenid': ['RG01'] * 2,
                    'plotid': ['R1', 'R2'], 'ag_d36': [2, 1],
                    'ag_d37a': [pd.NA, 123.0], 'ag_d37b': [pd.NA, 10],
                    'ag_d38': [1, 2], 'ag_d39a': [4, 4],
                    'ag_d39b': [999.0, 999.0], 'ag_d39c': [1, 7],
                    'ag_d39d': [amounts[0], 0.0],
                    'ag_d39e': [timings[0], pd.NA],
                    'ag_d39f': [1 if modern else 4] * 2,
                    'ag_d39g': [4 if modern else 999.0] * 2,
                    'ag_d39h': [999.0 if modern else 7] * 2,
                    'ag_d39i': [7 if modern else amounts[1],
                                7 if modern else pd.NA],
                    'ag_d39j': [amounts[1] if modern else timings[1],
                                pd.NA],
                    'ag_d40': [1, 2], 'ag_d41a': [7, pd.NA],
                    'ag_d41b': [2.0, pd.NA], 'ag_d41c': [8, pd.NA],
                    'ag_d41d': [pd.NA, pd.NA],
                    'ag_d41e': [pd.NA, pd.NA],
                    'ag_d41f': [pd.NA, pd.NA],
                    'ag_d41g': [pd.NA, pd.NA],
                })
                return d
            if 'ag_mod_g' in name:
                # 2010-11 keys the plot on ag_g0b; 2013-14 keys it on
                # ag_g00 and carries the crop code in ag_g0b.
                plot_crop = ({'ag_g00': ['R1'], 'ag_g0b': [1]}
                             if wave == '2013-14' else {'ag_g0b': ['R1']})
                return pd.DataFrame({
                    **ids, **plot_crop, 'ag_g0d': [1], 'crop_code': [1],
                    'gardenid': ['RG01'], 'plotid': ['R1'],
                    'ag_g04a': [3.0], 'ag_g04b': [1], 'ag_g0f': [2],
                })
            assert 'ag_mod_h' in name
            return pd.DataFrame({
                **ids, 'ag_h0b': [1], 'ag_h0c': [1], 'crop_code': [1],
                'ag_h16a': [5.0], 'ag_h16b': [4], 'ag_h19': [10.0],
                'ag_h26a': [pd.NA], 'ag_h26b': [pd.NA],
                'ag_h29': [pd.NA], 'ag_h07a': [5.0], 'ag_h07b': [4],
                'ag_h09': [10.0], 'ag_h38a': [pd.NA],
                'ag_h38b': [pd.NA], 'ag_h40': [pd.NA],
            })

        monkeypatch.setattr(local_tools, 'get_dataframe', source)

        # 2016-17 reads its Cross_Sectional Module D through a restricted
        # DVC reader (a latin-1 bad byte breaks a full read); serve the
        # same synthetic frame there, honouring its column restriction.
        class _DVCFS:
            @staticmethod
            def open(path, *args, **kwargs):
                captures['dvcfs'].append(path)
                return io.BytesIO(b'')

        def read_dta(path, usecols=None, **kwargs):
            frame = source(captures['dvcfs'][-1])
            return (frame if usecols is None else frame[usecols]), None

        monkeypatch.setattr(local_tools, '_ensure_dvc_pulled',
                            lambda *args, **kwargs: None)
        monkeypatch.setattr(local_tools, '_dvc_working_directory',
                            lambda *args, **kwargs: contextlib.nullcontext())
        monkeypatch.setattr(local_tools, 'DVCFS', _DVCFS)
        monkeypatch.setattr(pyreadstat, 'read_dta', read_dta)
        monkeypatch.setattr(local_tools, 'to_parquet',
                            lambda df, path: captures['writes'].append(df))
        runpy.run_path(str(root / wave / '_' / 'plot_inputs.py'))
        assert len(captures['writes']) == 1
        captures['result'] = captures['writes'][0]
        return captures

    return run


WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']
MODERN = {'2016-17', '2019-20'}
# Waves concatenating a Cross_Sectional and a Panel half.
TWO_HALVES = {'2016-17', '2019-20'}
HHIDS = {'2010-11': {'7'}, '2013-14': {'7'}, '2016-17': {'cs-17-7', '700'},
         '2019-20': {'7', '700'}}


@pytest.mark.parametrize('wave', WAVES)
@pytest.mark.parametrize('timings', [(1, 12), (8, 2), (pd.NA, pd.NA)])
def test_actual_driver_uses_total_kg_not_timing_or_native_pair(drive, wave,
                                                            timings):
    captured = drive(wave, timings=timings)
    assert len(captured['blocks']) == (2 if wave in TWO_HALVES else 1)
    if wave == '2016-17':
        assert captured['dvcfs'] == [
            'Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta']
    for kwargs, block in captured['blocks']:
        assert kwargs['unit1'] is None and kwargs['unit2'] is None
        assert block['u'].dtype == pd.StringDtype()
        assert block['u'].eq('Kilogramme').all()
        assert block['Quantity'].dtype == pd.Float64Dtype()
        assert block['Quantity'].tolist()[:3] == [7.0, 0.0, 11.0]
        assert pd.isna(block['Quantity'].iloc[3])


@pytest.mark.parametrize('wave', WAVES)
def test_same_input_applications_sum_before_the_unit_becomes_a_key(drive, wave):
    result = drive(wave)['result']
    fert = result[result.index.get_level_values('input') == 'Urea Fertilizer']
    assert fert.index.is_unique
    assert len(fert) == (4 if wave in TWO_HALVES else 2)
    assert set(fert.index.get_level_values('u')) == {'Kilogramme'}
    assert sorted(fert.Quantity.tolist()) == ([0.0, 0.0, 18.0, 18.0]
                                             if wave in TWO_HALVES
                                             else [0.0, 18.0])
    # Relabelling after assembly would leave two identical kg keys and
    # permit a later first-selector to lose one application's contribution.
    for column in ['Quantity_purchased', 'Purchased', 'Improved']:
        assert fert[column].isna().all()


@pytest.mark.parametrize('wave', WAVES)
def test_nonfertilizer_channels_and_native_namespaces_stay_independent(drive,
                                                                     wave):
    result = drive(wave)['result']
    ids = set(result.index.get_level_values('i'))
    assert ids == HHIDS[wave]
    seed = result[result.index.get_level_values('input') == 'Seed']
    assert (seed.index.get_level_values('u') == 'Gram').all()
    assert seed.Quantity.eq(3.0).all()
    assert seed.Quantity_purchased.eq(5.0).all()
    assert seed.Purchased.eq(True).all()
    pesticide = result[result.index.get_level_values('input') == 'Insecticide']
    assert (pesticide.index.get_level_values('u') == 'Liter').all()
    assert pesticide.Quantity.eq(2.0).all()


@pytest.mark.parametrize('wave', WAVES)
def test_missing_and_zero_totals_keep_existing_eligibility_and_sum(drive, wave):
    captured = drive(wave, amounts=(pd.NA, 0.0))
    for _, block in captured['blocks']:
        assert pd.isna(block.Quantity.iloc[0])
        assert block.Quantity.iloc[1] == block.Quantity.iloc[2] == 0.0
        assert pd.isna(block.Quantity.iloc[3])
        assert block.u.eq('Kilogramme').all()
    fert = captured['result']
    fert = fert[fert.index.get_level_values('input') == 'Urea Fertilizer']
    assert fert.Quantity.eq(0.0).all()
