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

MALAWI = countries_root() / 'Malawi'


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


# --- the seed half (GH #1174, ruling 2026-11-04): excluded, counted, not recovered ---

def _g_lines(malawi, **cols):
    base = {'hhid': ['1', '2'], 'plotkey': ['p1', 'p1'], 'crop_code': [1, 1],
            'ag_g04a': [3.0, pd.NA], 'ag_g04b': [2, pd.NA]}
    base.update(cols)
    frame = pd.DataFrame(base)
    return malawi._seed_block(
        frame, hhid='hhid', plotkey='plotkey', cropcode='crop_code', qty='ag_g04a',
        unit='ag_g04b', source_ref='Malawi/x/ag_mod_g.dta@dvc:ag_mod_g.dta:0',
        source_half='single', row_positions=list(range(len(frame))))


PAYLOAD = ['Quantity', 'Quantity_purchased', 'Unit_purchased', 'Purchased', 'Improved',
           'Applied', 'Quantity_received_as_gift', 'Unit_received_as_gift', 'Received_as_gift']


def test_a_seed_line_with_no_answer_stays_blank_at_wave_grain(malawi):
    """It reaches the wave frame with no payload at all, and no 0 is minted: the
    instrument does not say whether seed was not used or the amount is unknown,
    so _finalize_result's all-NA rule, not this builder, decides it is not served."""
    out = malawi.assemble_plot_inputs('2019-20', [_g_lines(malawi)])
    by_i = out.reset_index().set_index('i')
    assert by_i.loc['1', 'Quantity'] == 3.0
    blank = by_i.loc['2']
    assert blank[[c for c in PAYLOAD if c in by_i.columns]].isna().all()


def test_an_attached_answer_keeps_the_line_in_the_table(malawi):
    """A line with no planted amount and a free-seed answer has a payload."""
    h = pd.DataFrame({'hhid': ['2'], 'crop_code': [1], 'ag_h37': [1],
                      'ag_h38a': [2.0], 'ag_h38b': [2]})
    gift = malawi._seed_gift_block(h, hhid='hhid', cropcode='crop_code', source_half='single')
    out = malawi.assemble_plot_inputs('2019-20', [_g_lines(malawi)], None, [gift])
    by_i = out.reset_index().set_index('i')
    assert pd.isna(by_i.loc['2', 'Quantity'])
    assert by_i.loc['2', 'Quantity_received_as_gift'] == 2.0


#: Each wave's Module G files, the column the wave script reads Improved from.
_G_SOURCES = {
    '2010-11': [('2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta', None)],
    '2013-14': [('2013-14/Data/AG_MOD_G_13.dta', None)],
    '2016-17': [('2016-17/Data/Cross_Sectional/ag_mod_g.dta', 'ag_g0f'),
                ('2016-17/Data/Panel/ag_mod_g_16.dta', 'ag_g0f')],
    '2019-20': [('2019-20/Data/Cross_Sectional/ag_mod_g.dta', 'ag_g0f'),
                ('2019-20/Data/Panel/ag_mod_g_19.dta', 'ag_g0f')],
}


@pytest.mark.requires_s3
@pytest.mark.parametrize('wave', sorted(_G_SOURCES))
def test_every_excluded_seed_line_has_no_seed_amount(wave):
    """Oracle on Module G: a line missing from the served table answers neither the
    planted amount nor Improved, and a served line is never a minted 0.  (Its unit
    may be recorded, 2 lines in 2010-11: ``u`` is an index level, so a unit alone is
    not a payload.)  ``input_obs`` carries the line's position in its file (GH #1298)."""
    import numpy as np
    from lsms_library.country import Country
    from lsms_library.local_tools import get_dataframe
    pi = Country('Malawi').plot_inputs().reset_index()
    all_seed = pi[pi['t'].astype(str).eq(wave) & pi['input'].astype(str).eq('Seed')]
    # The H acquisition carrier adds separate Seed rows with a field-qualified
    # H coordinate.  This Module G deletion oracle must account for G lines only.
    acquired = all_seed['Carrier_role'].eq('reported_H_component').fillna(False)
    assert acquired.any()
    assert all_seed.loc[~acquired, 'Carrier_role'].isna().all()
    assert all_seed.loc[acquired, 'input_obs'].astype(str).str.contains('/field:', regex=False).all()
    seed = all_seed.loc[~acquired]
    obs = seed['input_obs'].astype(str).str.extract(r'^row:Malawi/(?P<src>[^@]+)@.*:(?P<pos>\d+)$')
    assert obs.notna().all(axis=None)
    excluded = 0
    for rel, improved in _G_SOURCES[wave]:
        g = get_dataframe(str(MALAWI / rel), convert_categoricals=False).reset_index(drop=True)
        g['_pos'] = np.arange(len(g))
        if wave == '2010-11':   # the wave script drops exact repeats first
            g = g[~g.drop(columns='_pos').duplicated()]
        served = set(obs.loc[obs['src'].eq(rel), 'pos'].astype(int))
        gone = g[~g['_pos'].isin(served)]
        excluded += len(gone)
        assert pd.to_numeric(gone['ag_g04a'], errors='coerce').isna().all(), rel
        if improved:
            assert pd.to_numeric(gone[improved], errors='coerce').isna().all(), rel
        # a served line with a planted amount reports what the source reports
        kept = g[g['_pos'].isin(served)]
        # float64 before summing: the file stores float32, whose own sum rounds.
        planted = pd.to_numeric(kept['ag_g04a'], errors='coerce').astype('float64')
        rows = seed[obs['src'].eq(rel)]
        assert float(pd.to_numeric(rows['Quantity'], errors='coerce').sum()) == pytest.approx(
            float(planted.sum()), abs=1e-6)
        assert int(rows['Quantity'].notna().sum()) == int(planted.notna().sum())
    assert excluded > 0


# --- the registered deletion ---------------------------------------------------

DELETION = 'Malawi::plot_inputs::seed-line-no-payload'


def test_the_deletion_is_registered_and_resolves():
    from lsms_library.derivations import derivation_records
    record = derivation_records('Malawi')[DELETION]
    assert record['rows'] == 0
    assert record['table'] == 'plot_inputs'
    assert list(record['waves']) == sorted(_G_SOURCES)
    assert callable(record.resolve('function'))
    assert callable(record.resolve('inputs'))


def test_the_deletion_covers_every_wave_plot_inputs_is_built_for(malawi):
    """A wave that builds plot_inputs and is missing here deletes its lines unregistered,
    and derivation_inputs refuses the wave: nothing else would notice."""
    from lsms_library.derivations import derivation_records
    built = sorted(p.parent.parent.name for p in MALAWI.glob('*/_/plot_inputs.py'))
    assert sorted(derivation_records('Malawi')[DELETION]['waves']) == built
    assert sorted(malawi._SEED_LINE_SOURCES) == built


def test_the_predicate_is_a_seed_row_with_no_payload(malawi):
    out = malawi.assemble_plot_inputs('2019-20', [_g_lines(malawi)])
    picked = malawi.derive_seed_line_no_payload(out)
    assert sorted(out[picked].index.get_level_values('i')) == ['2']


@pytest.mark.parametrize('wave', sorted(_G_SOURCES))
def test_the_source_table_matches_the_wave_script(malawi, wave):
    text = (MALAWI / wave / '_' / 'plot_inputs.py').read_text()
    for half in malawi._SEED_LINE_SOURCES[wave]:
        for key in ('g', 'h'):
            assert half[key].rsplit('/', 1)[-1] in text, (wave, half[key])
        for key in ('half', 'hhid', 'prefix', 'gcrop', 'hcrop'):
            if half[key]:
                assert half[key] in text, (wave, key, half[key])
        for name in half['q'] + half['u'] + half['v']:
            assert name in text, (wave, name)


_LINE = r'^row:Malawi/(?P<src>[^@]+)@.*:(?P<pos>\d+)$'


def _lines(input_obs):
    """``{(file, position)}`` of each ``input_obs``."""
    found = pd.Series(input_obs, dtype='string').str.extract(_LINE)
    return set(zip(found['src'].dropna(), found['pos'].dropna().astype(int)))


@pytest.mark.requires_s3
@pytest.mark.parametrize('wave', sorted(_G_SOURCES))
def test_the_registered_inputs_are_the_excluded_lines(wave):
    """Exactly the excluded lines: none served, and none left out.  The right-hand
    side is an oracle on Module G, every line of the wave's files (after the 2010-11
    repeat policy) that no served row holds; nothing from the callable enters it."""
    import numpy as np
    from lsms_library.country import Country
    from lsms_library.local_tools import get_dataframe
    country = Country('Malawi')
    pi = country.plot_inputs()
    assert pi.attrs['derivations']['Malawi'][DELETION]['rows'] == 0
    gone = country.derivation_inputs(DELETION, wave=wave)
    assert list(gone.index.names) == ['t', 'i', 'input_obs']
    assert gone.index.is_unique
    obs = gone.index.get_level_values('input_obs').astype(str)
    served = pi.xs(wave, level='t').index.get_level_values('input_obs').astype(str)
    assert len(obs) and set(obs).isdisjoint(served)
    assert pd.to_numeric(gone['ag_g04a'], errors='coerce').isna().all()

    held = _lines(served)
    expected = set()
    for rel, _ in _G_SOURCES[wave]:
        g = get_dataframe(str(MALAWI / rel), convert_categoricals=False)
        keep = ~g.duplicated().to_numpy() if wave == '2010-11' else np.ones(len(g), dtype=bool)
        expected |= {(rel, int(p)) for p in np.arange(len(g))[keep]} - held
    assert _lines(obs) == expected
