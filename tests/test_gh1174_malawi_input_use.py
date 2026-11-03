"""Malawi plot_inputs: Module D use answers and missing amounts (GH #1174).

Module D asks use before amount: ag_d36 (organic), ag_d38 ("Did you use any
inorganic fertilizer on this [PLOT]?") and ag_d40 (agrochemicals).  A type
recorded under a Yes with no amount is a reported use with a missing amount.
The reducer used to sum such a group to 0 (``sum`` with ``min_count=0``), so it
was served as a measured zero; summing with ``min_count=1`` alone would have
let the framework's all-payload-NA filter delete it.  Now the use answer rides
in ``Applied`` and the amount stays missing.  No microdata needed.
"""
import importlib.util

import pandas as pd
import pytest

from lsms_library.paths import countries_root


@pytest.fixture(scope='module')
def malawi():
    path = countries_root() / 'Malawi' / '_' / 'malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_gh1174', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fert(malawi, gate, q1, q2=None, type2=None):
    n = len(gate)
    src = pd.DataFrame({'hhid': [str(k) for k in range(n)], 'plotkey': ['p1'] * n,
                        'ag_d38': pd.array(gate, dtype='Int64'),
                        'type1': [4] * n, 'q1': pd.array(q1, dtype='Float64'),
                        'type2': type2 or [pd.NA] * n,
                        'q2': pd.array(q2 or [pd.NA] * n, dtype='Float64')})
    return malawi._fertilizer_block(
        src, hhid='hhid', plotkey='plotkey', type1='type1', qty1='q1', unit1=None,
        type2='type2', qty2='q2', unit2=None, t='2019-20', gate='ag_d38')


def test_fertilizer_carries_its_use_answer_without_filtering(malawi):
    block = _fert(malawi, gate=[1, 2, pd.NA], q1=[5.0, 5.0, 5.0])
    assert len(block) == 3                      # the gate never drops a type row
    assert block['Applied'].tolist() == [True, False, pd.NA]
    assert block['Applied'].dtype == pd.BooleanDtype()


def test_reported_use_with_no_amount_is_missing_not_zero(malawi):
    block = _fert(malawi, gate=[1, 1, 1], q1=[pd.NA, 0.0, pd.NA],
                  type2=[4, 4, 4], q2=[pd.NA, pd.NA, 3.0])
    block['u'] = 'Kilogramme'
    out = malawi.assemble_plot_inputs('2019-20', [block])
    by_i = out.reset_index().set_index('i')
    assert pd.isna(by_i.loc['0', 'Quantity'])       # two blanks: missing
    assert by_i.loc['1', 'Quantity'] == 0.0         # a recorded zero stays 0
    assert by_i.loc['2', 'Quantity'] == 3.0
    assert by_i['Applied'].eq(True).all()
    assert out['Quantity'].dtype == pd.Float64Dtype()


def test_pesticide_rows_are_kept_only_under_yes_and_say_so(malawi):
    src = pd.DataFrame({'hhid': ['1', '2'], 'plotkey': ['p1', 'p1'],
                        'ag_d40': [1, 2], 'a': [7, 7], 'b': [pd.NA, 2.0], 'c': [8, 8]})
    block = malawi._pesticide_block(src, hhid='hhid', plotkey='plotkey',
                                    gate='ag_d40', slots=[('a', 'b', 'c')], t='x')
    assert block['i'].tolist() == ['1']
    assert block['Applied'].tolist() == [True]
    out = malawi.assemble_plot_inputs('2010-11', [block])
    assert pd.isna(out['Quantity'].iloc[0])


def test_reported_use_with_no_amount_survives_country_finalization(malawi):
    from lsms_library.country import Country

    block = _fert(malawi, gate=[1, pd.NA], q1=[pd.NA, pd.NA])
    block['u'] = 'Kilogramme'
    assembled = malawi.assemble_plot_inputs('2010-11', [block])
    country = Country('Malawi')
    country._updated_ids_cache = {}
    country._panel_ids_attempted = True
    country._categorical_mapping_cache = {}
    country._sample_v_cache = pd.DataFrame(
        {'v': ['c1', 'c1']}, index=pd.MultiIndex.from_tuples(
            [('0', '2010-11'), ('1', '2010-11')], names=['i', 't']))

    served = country._finalize_result(
        assembled, country._materialization_entry('plot_inputs'), 'plot_inputs')

    # the answered Yes survives with its missing amount; the row with no
    # answer and no amount is all-NA payload and is not served
    assert served.index.get_level_values('i').tolist() == ['0']
    assert served['Quantity'].isna().all()
    assert served['Applied'].tolist() == [True]


@pytest.mark.parametrize('wave', ['2010-11', '2013-14', '2016-17', '2019-20'])
def test_every_driver_passes_the_inorganic_use_gate(wave):
    text = (countries_root() / 'Malawi' / wave / '_' / 'plot_inputs.py').read_text()
    assert "gate='ag_d38'" in text.split('_fertilizer_block(')[1].split(')')[0] + ')'
