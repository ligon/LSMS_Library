"""Malawi: code 48 lines whose crop text says cowpea get a cowpea label (GH #1228).

Seasonal Module G has no cowpea code, so respondents who grew it were coded 48
OTHER (SPECIFY) and served ``crop = 'Other'``.  2010-11 carries the name on the
row (``ag_g0d_os``; ruling 2026-11-04, the smallest step).  2016-17 / 2019-20
carry it in Module D question 20's "Other Crop" text; the ruling of 2026-11-12
("Join D, single-text only") names a line from D only where its plot has exactly
one text, and a code-48 sale follows the household's lines.  2013-14 (no text
held) and the local names stay 'Other'.

Static synthetic tier, plus a source-gated tier whose oracles read Modules G and
D directly.
"""
import ast
import importlib.util

import pandas as pd
import pytest

from lsms_library.paths import countries_root

MALAWI = countries_root() / 'Malawi'
LABEL = 'Cowpea (Khobwe)'
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']


@pytest.fixture(scope='module')
def malawi():
    path = MALAWI / '_' / 'malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_gh1228', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _g(codes, texts):
    n = len(codes)
    return pd.DataFrame({'hhid': [f'h{k}' for k in range(n)], 'plotkey': ['R1'] * n,
                         'code': codes, 'text': texts, 'q': [10.0] * n, 'unit': [1] * n,
                         'cond': [1] * n, 'seed': [2.0] * n, 'seed_u': [2] * n})


def _harvest(malawi, g, **kw):
    return malawi._harvest_block(g, hhid='hhid', plotkey='plotkey', cropcode='code',
                                 qty='q', unit='unit', condition='cond', t='2010-11', **kw)


def test_a_line_whose_own_text_says_cowpea_gets_the_label(malawi):
    out = _harvest(malawi, _g([48], ['COWPEAS']), crop_text='text')
    assert out['crop'].tolist() == [LABEL]
    assert out.attrs['crop_text_recovered'] == 1


def test_what_was_recorded_stays(malawi):
    """The code, the variety and the observation key still say 48."""
    plain = _harvest(malawi, _g([48], ['COWPEAS']))
    named = _harvest(malawi, _g([48], ['COWPEAS']), crop_text='text')
    assert plain['crop'].tolist() == ['Other']
    for column in ('_crop_code', 'production_obs', 'crop_variety', 'Quantity', 'u'):
        assert named[column].tolist() == plain[column].tolist(), column
    assert named['production_obs'].tolist() == ['reported:G:48']


@pytest.mark.parametrize('code, text, expected', [
    (48, 'NSEULA', 'Other'),      # no source says what this is
    (48, 'MSEWULA', 'Other'),
    (48, 'NKHOBWE', 'Other'),     # the food label attests khobwe, not this spelling
    (48, '', 'Other'),
    (48, None, 'Other'),
    (36, 'COWPEAS', 'Pigeon Pea (Nandolo)'),   # a coded crop is not a residue
])
def test_nothing_else_moves(malawi, code, text, expected):
    out = _harvest(malawi, _g([code], [text]), crop_text='text')
    assert out['crop'].tolist() == [expected]
    assert out.attrs['crop_text_recovered'] == 0


def test_the_text_is_normalised_before_it_is_matched(malawi):
    out = _harvest(malawi, _g([48], ['  cowpeas ']), crop_text='text')
    assert out['crop'].tolist() == [LABEL]


def test_a_sale_coded_48_stays_attached_to_the_line_it_attached_to(malawi):
    """The attach keys on the code, so naming the crop detaches nothing (the
    historical proposal, a synthetic code, would have)."""
    i_mod = pd.DataFrame({'hhid': ['h0'], 'code': [48], 'sold': [1], 'qs': [5.0],
                          'vs': [100.0], 'us': [1], 'cs': [1]})
    sale = [malawi._sale_block(i_mod, hhid='hhid', cropcode='code', sold_flag='sold',
                               qty_sold='qs', value_sold='vs', unit_sold='us',
                               condition='cs')]
    plain = malawi.assemble_crop_production('2010-11', [_harvest(malawi, _g([48], ['COWPEAS']))], sale)
    named = malawi.assemble_crop_production(
        '2010-11', [_harvest(malawi, _g([48], ['COWPEAS']), crop_text='text')], sale)
    assert named['Quantity_sold'].tolist() == plain['Quantity_sold'].tolist() == [5.0]
    assert named['Value_sold'].tolist() == plain['Value_sold'].tolist() == [100.0]
    assert named.index.get_level_values('crop').tolist() == [LABEL]
    assert named.attrs['crop_text_recovered'] == 1


def test_a_cowpea_line_and_an_nseula_line_are_not_summed_as_one_crop(malawi):
    g = _g([48, 48], ['COWPEAS', 'NSEULA'])
    g['hhid'] = 'h0'
    out = malawi.assemble_crop_production('2010-11', [_harvest(malawi, g, crop_text='text')], [])
    assert sorted(out.index.get_level_values('crop')) == [LABEL, 'Other']


def test_the_seed_row_of_the_same_line_carries_the_same_label(malawi):
    out = malawi._seed_block(_g([48, 48], ['COWPEAS', 'NSEULA']), hhid='hhid',
                             plotkey='plotkey', cropcode='code', qty='seed', unit='seed_u',
                             t='2010-11', crop_text='text')
    assert out['crop'].tolist() == [LABEL, 'Other']


def test_the_label_is_the_food_tables_and_reserves_no_code(malawi):
    from lsms_library.local_tools import all_dfs_from_orgfile
    tables = all_dfs_from_orgfile(MALAWI / '_' / 'categorical_mapping.org')
    food = tables['harmonize_food']
    assert set(malawi._CROP_TEXT_RECODE.values()) <= set(food['Preferred Label'])
    assert LABEL not in set(tables['harmonize_crop']['Preferred Label'])


def _block_calls(wave, script, block):
    tree = ast.parse((MALAWI / wave / '_' / f'{script}.py').read_text())
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)
            and getattr(n.func, 'id', None) == block]


#: What each wave hands the G blocks as ``crop_text``: its own G column, the
#: plot's Module D text (a Name), or nothing (2013-14 holds no text).
CROP_TEXT_SOURCE = {'2010-11': "'ag_g0d_os'", '2013-14': None,
                    '2016-17': 'D20_TEXT_COL', '2019-20': 'D20_TEXT_COL'}


@pytest.mark.parametrize('wave', WAVES)
@pytest.mark.parametrize('script, block', [('crop_production', '_harvest_block'),
                                           ('plot_inputs', '_seed_block')])
def test_each_wave_reads_the_ruled_crop_text(wave, script, block):
    """The rulings' scope, as a rule: 2010-11 its own text, 2016-17 / 2019-20 Module D's."""
    calls = _block_calls(wave, script, block)
    assert calls
    passes = [ast.unparse(k.value) for call in calls for k in call.keywords if k.arg == 'crop_text']
    expected = CROP_TEXT_SOURCE[wave]
    assert passes == ([expected] if expected else [])
    src = (MALAWI / wave / '_' / f'{script}.py').read_text()
    assert ('attach_module_d_text(g, d' in src) == (expected == 'D20_TEXT_COL')


# --- the Module D join (GH #1228, ruling 2026-11-12) ------------------------

def _gd(texts, slot_codes, g_codes=(48,), d_rows=1):
    """One plot; ``texts`` the D "Other Crop" texts, ``slot_codes`` D20's codes."""
    g = pd.DataFrame({'hhid': ['h0'] * len(g_codes), 'gardenid': ['RG01'] * len(g_codes),
                      'plotid': ['R01'] * len(g_codes), 'crop_code': list(g_codes),
                      'q': [10.0] * len(g_codes), 'unit': [1] * len(g_codes),
                      'cond': [3] * len(g_codes), 'plotkey': ['RG01_R01'] * len(g_codes)})
    slots = list(slot_codes) + [None] * (5 - len(slot_codes))
    row = {'hhid': 'h0', 'gardenid': 'RG01', 'plotid': 'R01',
           **{f'ag_d20{x}': s for x, s in zip('abcde', slots)},
           'ag_d20_oth': texts[0] if texts else '',
           'ag_d20b_oth': texts[1] if len(texts) > 1 else ''}
    return g, pd.DataFrame([row] * d_rows)


@pytest.mark.parametrize('texts, slots, g_codes, d_rows, reason', [
    (['COW PEAS'], [1, 48], (48,), 1, 'single'),
    (['COWPEAS'], [48], (48,), 1, 'single'),
    # 2019-20 XS: "Other Crop 2" (ag_d20b_oth) is code 49's text (D20: 48 OTHER SPECIFY 1,
    # 49 OTHER SPECIFY 2), so a plot naming both Other crops has two texts and keeps 'Other'.
    (['COW PEAS', 'NKHAKA'], [48, 49], (48,), 1, 'multi_text'),
    # ag_d20b_oth never names code 48's crop (red team F2, 2026-11-13).
    (['', 'COW PEAS'], [48], (48,), 1, 'text_not_for_code_48'),
    ([], [48], (48,), 1, 'no_text'),
    (['COWPEAS'], [36], (48,), 1, 'text_no_other_slot'),
    (['COWPEAS'], [48, 48], (48,), 1, 'text_multi_other_slot'),
    (['COWPEAS'], [48], (48, 48), 1, 'g_multi_other_on_plot'),
    (['COWPEAS'], [48], (48,), 2, 'd_not_unique'),
])
def test_the_d_text_is_taken_only_where_it_is_unambiguous(malawi, texts, slots, g_codes, d_rows, reason):
    g, d = _gd(texts, slots, g_codes, d_rows)
    text, why = malawi.module_d_other_crop_text(g, d, cropcode='crop_code')
    assert set(why) == {reason}
    assert text.notna().all() == (reason == 'single')
    assert list(text.index) == list(g.index)


def test_a_g_line_without_a_d_row_or_key_keeps_no_text(malawi):
    g, d = _gd(['COWPEAS'], [48])
    g2 = pd.concat([g, g.assign(hhid='h1'), g.assign(plotid=None)], ignore_index=True)
    text, why = malawi.module_d_other_crop_text(g2, d, cropcode='crop_code')
    assert list(why) == ['single', 'no_d_row', 'key_incomplete']
    assert text.notna().tolist() == [True, False, False]


@pytest.mark.parametrize('texts, expected', [
    (['COW PEAS'], LABEL), (['  cowpeas '], LABEL),
    (['NKHOBWE'], 'Other'), (['NSEULA'], 'Other'), (['MSEWULA'], 'Other'),
    (['COWPEAS(NSEULA)'], 'Other'),       # a local name in the text: not on the exact list
    (['MUCUNA'], 'Other'), (['COW PEAS', 'CUCUMBER'], 'Other'),
])
def test_the_harvest_line_takes_the_d_text_label(malawi, texts, expected):
    g, d = _gd(texts, [48])
    malawi.attach_module_d_text(g, d, cropcode='crop_code')
    out = malawi._harvest_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code', qty='q',
                                unit='unit', condition='cond', t='2019-20',
                                crop_text=malawi.D20_TEXT_COL, crop_text_reason=malawi.D20_REASON_COL)
    assert out['crop'].tolist() == [expected]
    assert out['production_obs'].tolist() == ['reported:G:48']
    assert out.attrs['crop_text_recovered'] == int(expected == LABEL)
    left = out.attrs['crop_text_left_other']
    assert sum(left.values()) == int(expected != LABEL)
    if len(texts) == 2:
        assert left == {'multi_text': 1}


KEY = 'Malawi::crop_production::cowpea-from-crop-text'
SEED_KEY = 'Malawi::plot_inputs::cowpea-from-crop-text'


@pytest.mark.parametrize('existing, add, expected', [
    (None, True, KEY), ('', True, KEY), (None, False, None),
    ('Malawi::crop_production::sale-basis-no-reference', True,
     'Malawi::crop_production::cowpea-from-crop-text+Malawi::crop_production::sale-basis-no-reference'),
    (KEY, True, KEY),                                      # never doubled
    ('Malawi::crop_production::sale-basis-unresolved', False,
     'Malawi::crop_production::sale-basis-unresolved'),
])
def test_the_key_joins_an_existing_one_in_sorted_order(malawi, existing, add, expected):
    out = malawi.union_derivation_key(pd.Series([existing], dtype='string'), [add], KEY)
    if expected is None:
        assert pd.isna(out[0])
    else:
        assert out[0] == expected


def test_the_relabelled_rows_carry_the_key_beside_a_ladder_key(malawi):
    """A single-plot cowpea line whose sale lands on the unknown wildcard carries both keys."""
    g, d = _gd(['COW PEAS'], [48])
    g['unit'], g['cond'] = 9, float('nan')              # an unanswered S/U: the wildcard
    malawi.attach_module_d_text(g, d, cropcode='crop_code')
    h = malawi._harvest_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code', qty='q',
                              unit='unit', condition='cond', t='2019-20',
                              crop_text=malawi.D20_TEXT_COL, crop_text_reason=malawi.D20_REASON_COL)
    out = malawi.assemble_crop_production('2019-20', [h], _sale(malawi, [48]))
    assert out['Derivation'].tolist() == ['+'.join(sorted([KEY, malawi.SALE_BASIS_WILDCARD]))]
    plain = malawi.assemble_crop_production('2019-20', [_harvest(malawi, _g([48], ['MUCUNA']))], [])
    assert plain['Derivation'].isna().all()


def test_the_registry_names_the_rule_and_its_inputs(malawi):
    from lsms_library.derivations import derivation_records
    recs = derivation_records('Malawi')
    for key in (KEY, SEED_KEY):
        r = recs[key]
        assert tuple(r.waves) == malawi.CROP_TEXT_WAVES
        assert r.resolve('function').__name__ == 'derive_cowpea_from_crop_text'
        assert r.resolve('inputs').__name__ == 'inputs_cowpea_from_crop_text'
    assert set(malawi.CROP_TEXT_KEYS.values()) == {KEY, SEED_KEY}


def test_the_seed_line_takes_the_same_d_text_label(malawi):
    g, d = _gd(['COW PEAS'], [48])
    g['seed'], g['seed_u'] = 2.0, 2
    malawi.attach_module_d_text(g, d, cropcode='crop_code')
    out = malawi._seed_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code', qty='seed',
                             unit='seed_u', t='2019-20', crop_text=malawi.D20_TEXT_COL)
    assert out['crop'].tolist() == [LABEL]


def _sale(malawi, codes):
    i_mod = pd.DataFrame({'hhid': ['h0'] * len(codes), 'code': codes, 'sold': [1] * len(codes),
                          'qs': [5.0] * len(codes), 'vs': [100.0] * len(codes),
                          'us': [9] * len(codes), 'cs': [3] * len(codes)})
    return [malawi._sale_block(i_mod, hhid='hhid', cropcode='code', sold_flag='sold', qty_sold='qs',
                               value_sold='vs', unit_sold='us', condition='cs')]


def _named_harvest(malawi, plots):
    """One household; ``plots`` maps a plot id to its single D text."""
    gs, ds = [], []
    for plot, text in plots.items():
        g, d = _gd([text], [48])
        g['plotid'], d['plotid'], g['plotkey'] = plot, plot, f'RG01_{plot}'
        g['unit'] = 9
        gs.append(g)
        ds.append(d)
    g, d = pd.concat(gs, ignore_index=True), pd.concat(ds, ignore_index=True)
    malawi.attach_module_d_text(g, d, cropcode='crop_code')
    return malawi._harvest_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code', qty='q',
                                 unit='unit', condition='cond', t='2019-20',
                                 crop_text=malawi.D20_TEXT_COL, crop_text_reason=malawi.D20_REASON_COL)


def test_a_code_48_sale_moves_with_its_single_plot_line(malawi):
    """The attach keys on the code, so the sale lands on the relabelled line."""
    out = malawi.assemble_crop_production('2019-20', [_named_harvest(malawi, {'R01': 'COW PEAS'})],
                                          _sale(malawi, [48]))
    assert out.index.get_level_values('crop').tolist() == [LABEL]
    assert out['Value_sold'].tolist() == [100.0]


@pytest.mark.parametrize('plots, expected', [
    ({'R01': 'COW PEAS', 'R02': 'COW PEAS'}, LABEL),   # every code-48 line is cowpea
    ({'R01': 'COW PEAS', 'R02': 'MUCUNA'}, 'Other'),   # the household's lines disagree
])
def test_a_farm_level_code_48_sale_follows_the_households_lines(malawi, plots, expected):
    out = malawi.assemble_crop_production('2019-20', [_named_harvest(malawi, plots)], _sale(malawi, [48]))
    farm = out[out.index.get_level_values('plot_id') == malawi.FARM_LEVEL]
    assert farm.index.get_level_values('crop').tolist() == [expected]
    assert farm['Value_sold'].tolist() == [100.0]


@pytest.mark.parametrize('cached', [True, False])
def test_the_restricted_d_reader_uses_held_bytes_and_cleans_up(malawi, tmp_path, monkeypatch, cached):
    """2016-17 XS Module D: the held blob, else DVCFS; only the columns the file carries."""
    import contextlib
    import io
    from pathlib import Path
    from types import SimpleNamespace
    import pyreadstat
    from lsms_library import local_tools
    payload = b'invented source bytes, including \xff'
    blob = tmp_path / 'blob'
    blob.write_bytes(payload)
    countries = tmp_path / 'countries'
    target = countries / 'Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta'
    target.parent.mkdir(parents=True)
    calls, temps = [], []
    monkeypatch.setattr(local_tools, '_ensure_dvc_pulled', lambda p: calls.append(('ensure', p)))
    monkeypatch.setattr(local_tools, '_dvc_cache_path', lambda p: blob if cached else None)
    monkeypatch.setattr(local_tools, '_COUNTRIES_DIR', countries)
    monkeypatch.setattr(local_tools, '_dvc_working_directory', lambda _: contextlib.nullcontext())

    def dvc_open(rel):
        assert not cached, 'a held blob must not construct the DVC index'
        calls.append(('dvc', rel))
        return io.BytesIO(payload)

    monkeypatch.setattr(local_tools, 'DVCFS', SimpleNamespace(open=dvc_open))
    frame = pd.DataFrame({'case_id': [1]})

    def read_dta(path, **kwargs):
        temps.append(Path(path))
        assert Path(path).read_bytes() == payload
        if kwargs.get('metadataonly'):
            return None, SimpleNamespace(column_names=['case_id', 'gardenid', 'ag_d20_oth'])
        assert kwargs == {'usecols': ['case_id', 'ag_d20_oth'], 'apply_value_formats': False}
        return frame, None

    monkeypatch.setattr(pyreadstat, 'read_dta', read_dta)
    out = malawi.read_module_d_columns(target, ['case_id', 'ag_d20_oth', 'ag_d20b_oth'])
    assert out is frame
    assert [c[0] for c in calls] == ['ensure'] + ([] if cached else ['dvc'])
    if not cached:
        assert calls[-1][1] == 'Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta'
    assert temps and not any(t.exists() for t in temps)


def test_raw_sale_lines_take_the_label_their_household_agrees_on(malawi):
    harv = pd.DataFrame({'i': ['h0', 'h1', 'h1', 'h2'], '_crop_code': [48, 48, 48, 12],
                         'crop': [LABEL, LABEL, 'Other', 'Groundnut']})
    lines = pd.DataFrame({'i': ['h0', 'h1', 'h2', 'h3', 'h0'], '_crop_code': [48, 48, 12, 48, 12],
                          'crop': ['Other', 'Other', 'Groundnut', 'Other', 'Groundnut']})
    out = malawi.household_crop_labels(lines, harv)
    assert out.tolist() == [LABEL, 'Other', 'Groundnut', 'Other', 'Groundnut']


# --- source-gated: oracles reading Modules G and D directly -----------------

def _g_lines():
    from lsms_library.local_tools import format_id, get_dataframe
    g = get_dataframe(str(MALAWI / '2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta'),
                      convert_categoricals=False)
    g = g[~g.duplicated(keep='first')]
    code = pd.to_numeric(g['ag_g0d'], errors='coerce')
    text = g['ag_g0d_os'].astype('string').str.strip().str.upper()
    lines = g[code.eq(48).fillna(False) & text.eq('COWPEAS').fillna(False)].copy()
    lines['i'] = lines['case_id'].apply(format_id).astype(str)
    return lines


#: The later halves, as the wave scripts read them: (G, D, household id, prefix).
LATER = {'2016-17': [('Cross_Sectional/ag_mod_g.dta', 'Cross_Sectional/ag_mod_d.dta', 'case_id', 'cs-17-'),
                     ('Panel/ag_mod_g_16.dta', 'Panel/ag_mod_d_16.dta', 'y3_hhid', '')],
         '2019-20': [('Cross_Sectional/ag_mod_g.dta', 'Cross_Sectional/ag_mod_d.dta', 'case_id', 'cs-19-'),
                     ('Panel/ag_mod_g_19.dta', 'Panel/ag_mod_d_19.dta', 'y4_hhid', '')]}
#: Measured 2026-11-13 (CONTENTS.org): 2016-17 XS / Panel, 2019-20 XS / Panel.
LATER_LINES = {'2016-17': 238 + 64, '2019-20': 584 + 113}


def _later_lines(wave, malawi):
    """An independent oracle: the code-48 G lines whose plot has ONE D row, ONE text
    (exactly COWPEAS / COW PEAS), ONE D code-48 slot and ONE G code-48 line."""
    import json
    from lsms_library.local_tools import format_id, get_dataframe
    # The served `i` is the wave's raw id walked once through its own map (GH #1245).
    walk = json.loads((MALAWI / '_' / 'updated_ids.json').read_text()).get(wave, {})
    out = []
    for gfile, dfile, hh, prefix in LATER[wave]:
        g = get_dataframe(str(MALAWI / wave / 'Data' / gfile), convert_categoricals=False)
        dpath = MALAWI / wave / 'Data' / dfile
        if wave == '2016-17' and dfile.startswith('Cross'):
            d = malawi.read_module_d_columns(dpath, [hh, 'gardenid', 'plotid', 'ag_d20a', 'ag_d20b',
                                                     'ag_d20c', 'ag_d20d', 'ag_d20e', 'ag_d20_oth'])
        else:
            d = get_dataframe(str(dpath), convert_categoricals=False)
        for df in (g, d):
            df['_k'] = (prefix + df[hh].apply(format_id) + '|' + df['gardenid'].apply(format_id)
                        + '|' + df['plotid'].apply(format_id))
        texts = [c for c in ('ag_d20_oth', 'ag_d20b_oth') if c in d.columns]
        t = d[texts].apply(lambda s: s.astype('string').str.strip().replace('', pd.NA))
        n48 = sum(pd.to_numeric(d[c], errors='coerce').eq(48).astype(int) for c in
                  ('ag_d20a', 'ag_d20b', 'ag_d20c', 'ag_d20d', 'ag_d20e'))
        ok = (t.notna().sum(axis=1) == 1) & (n48 == 1)
        word = t.bfill(axis=1).iloc[:, 0].str.upper()
        once = ~d['_k'].duplicated(keep=False)
        cow = set(d.loc[ok & once & word.isin(['COWPEAS', 'COW PEAS']), '_k'])
        g48 = g[pd.to_numeric(g['crop_code'], errors='coerce').eq(48).fillna(False)]
        g48 = g48[~g48['_k'].duplicated(keep=False)]
        lines = g48[g48['_k'].isin(cow)].copy()
        lines['i'] = lines['_k'].str.split('|').str[0].map(lambda x: walk.get(x, x))
        lines['plot_id'] = lines['gardenid'].apply(format_id) + '_' + lines['plotid'].apply(format_id)
        out.append(lines)
    return pd.concat(out, ignore_index=True)


@pytest.mark.requires_s3
def test_the_served_rows_are_the_source_lines(malawi):
    from lsms_library.country import Country
    lines = _g_lines()
    assert len(lines) == 157
    cp = Country('Malawi').crop_production()
    named = cp[cp.index.get_level_values('crop') == LABEL]
    # GH #1457's Farm-level sale rows take the household's label too; they are no harvest line.
    named = named[named.index.get_level_values('plot_id') != 'Farm-level']
    assert set(named.index.get_level_values('t')) == {'2010-11', '2016-17', '2019-20'}
    assert set(named.index.get_level_values('production_obs')) == {'reported:G:48'}
    early = named.xs('2010-11', level='t')
    assert len(early) == len(lines)
    assert sorted(set(early.index.get_level_values('i').astype(str))) == sorted(set(lines['i']))
    assert float(pd.to_numeric(early['Quantity'], errors='coerce').sum()) == pytest.approx(
        float(pd.to_numeric(lines['ag_g13a'], errors='coerce').sum()))
    for wave, n in LATER_LINES.items():
        oracle = _later_lines(wave, malawi)
        assert len(oracle) == n, wave
        served = named.xs(wave, level='t')
        keys = set(zip(served.index.get_level_values('i').astype(str),
                       served.index.get_level_values('plot_id').astype(str)))
        assert keys == set(zip(oracle['i'], oracle['plot_id'])), wave
        assert float(pd.to_numeric(served['Quantity'], errors='coerce').sum()) == pytest.approx(
            float(pd.to_numeric(oracle['ag_g13a'], errors='coerce').sum())), wave
    assert 'Cowpea (Khobwe)' not in set(cp.xs('2013-14', level='t').index.get_level_values('crop'))


@pytest.mark.requires_s3
def test_seed_rows_carry_the_same_label(malawi):
    from lsms_library.country import Country
    lines = _g_lines()
    pi = Country('Malawi').plot_inputs().reset_index()
    named = pi[pi['crop'].astype(str).eq(LABEL)]
    assert set(named['t'].astype(str)) == {'2010-11', '2016-17', '2019-20'}
    assert set(named.loc[named['plot_id'].astype(str).ne('Farm-level'), 'input'].astype(str)) == {'Seed'}
    assert set(named.loc[named['t'].astype(str).eq('2010-11'), 'i'].astype(str)) <= set(lines['i'])
    for wave in LATER_LINES:
        oracle = _later_lines(wave, malawi)
        seeds = named[named['t'].astype(str).eq(wave)]
        assert set(zip(seeds['i'].astype(str), seeds['plot_id'].astype(str))) \
            <= set(zip(oracle['i'], oracle['plot_id'])), wave


@pytest.mark.requires_s3
def test_the_label_joins_food_acquired_in_every_wave():
    from lsms_library.country import Country
    fa = Country('Malawi').food_acquired().reset_index()
    waves = set(fa.loc[fa['j'].astype(str).eq(LABEL), 't'].astype(str))
    assert waves == set(fa['t'].astype(str))
    # Six food waves since 2024-25 was wired (GH #1275, PR #1604).
    assert len(waves) == 6


@pytest.mark.requires_s3
@pytest.mark.parametrize('table, key', [('crop_production', KEY), ('plot_inputs', SEED_KEY)])
def test_every_relabelled_row_carries_the_key_and_joins_its_inputs(table, key):
    """Ruling 2026-11-13, "Register all waves": the key on exactly the rows the label
    names, beside any other key, and the D (or own G) text behind each one."""
    from lsms_library.country import Country
    malawi = Country('Malawi')
    served = getattr(malawi, table)().reset_index()
    keys = served['Derivation'].astype('string').str.split('+')
    keyed = keys.map(lambda ks: isinstance(ks, list) and key in ks).to_numpy(dtype=bool)
    cowpea = served['crop'].astype(str).eq(LABEL).to_numpy()
    if table == 'plot_inputs':      # Module H's acquisition rows keep their code's label
        cowpea &= served['input'].astype(str).eq('Seed').to_numpy()
    assert (keyed == cowpea).all()
    assert set(served.loc[keyed, 't'].astype(str)) == {'2010-11', '2016-17', '2019-20'}
    for wave, rows in served[keyed].groupby(served.loc[keyed, 't'].astype(str)):
        inputs = malawi.derivation_inputs(key, wave=wave)
        assert list(inputs.index.names) == ['t', 'i', 'crop']
        have = set(zip(inputs.index.get_level_values('i').astype(str),
                       inputs.index.get_level_values('crop').astype(str)))
        assert set(zip(rows['i'].astype(str), rows['crop'].astype(str))) <= have, wave
        if wave != '2010-11':
            assert inputs['ag_d20_oth'].astype('string').str.strip().str.upper() \
                .isin(['COWPEAS', 'COW PEAS']).all(), wave
