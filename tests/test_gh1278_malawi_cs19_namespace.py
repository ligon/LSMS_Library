"""Malawi 2019-20: the cross-sectional half lives in the ``cs-19-`` namespace (GH #1278).

IHS5 is an independent cross-section, but its 2019-20 ``data_info.yml`` spelled
the formatter ``- function: cs_i``, a key ``Wave.column_mapping`` does not read,
so its ``i`` was the bare ``case_id``.  Two of those strings equal IHS3 2010-11
``case_id``s, which ``panel_attrition`` and any set intersection then counted as
panel links with no document behind them.  The fix is atomic because every table
must agree: seven dead keys, four YAML tables with a bare ``i: case_id``, ten
household scripts, and the two source tables in ``malawi.py`` behind
``derivation_inputs``.

The first half needs no microdata.  The second reads the two cover files directly
(an oracle that never enters a wave script) and is gated ``requires_s3``.
"""
import ast
import importlib.util
import json
import re

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

MALAWI = countries_root() / 'Malawi'
WAVE_DIR = MALAWI / '2019-20' / '_'
PREFIX = 'cs-19-'

#: Household-grain script tables (community_prices is cluster-grain).
SCRIPTS = ['anthropometry', 'crop_production', 'food_acquired', 'food_coping',
           'livestock', 'months_food_inadequate', 'people_last7days',
           'plot_features', 'plot_inputs', 'plot_labor']

requires_s3 = pytest.mark.requires_s3


@pytest.fixture(scope='module')
def info():
    return yaml.safe_load((WAVE_DIR / 'data_info.yml').read_text())


def _id_entries(block):
    """Every declared ``i`` of a table (one per sub-frame of a ``dfs:`` table)."""
    if 'dfs' in block:
        return [block[d]['idxvars']['i'] for d in block['dfs']
                if 'i' in block[d].get('idxvars', {})]
    return [block['idxvars']['i']] if 'i' in block.get('idxvars', {}) else []


def test_every_case_id_goes_through_cs_i(info):
    """The rule, over the whole file: ``case_id`` never reaches ``i`` bare."""
    seen = 0
    for table, block in info.items():
        if not isinstance(block, dict):
            continue
        for entry in _id_entries(block):
            if entry == 'case_id':
                raise AssertionError(f'{table}: bare i: case_id')
            if isinstance(entry, list) and entry[0] == 'case_id':
                assert entry[1:] == [{'mapping': 'cs_i'}], (table, entry)
                seen += 1
    # 7 formerly dead declarations (cluster_features has two) + 4 formerly bare.
    assert seen == 11


def test_the_panel_block_is_untouched(info):
    """The crosswalk reads the Panel cover only; ``cs_i`` must not reach it."""
    assert info['panel_ids'] == {'file': 'Panel/hh_mod_a_filt_19.dta',
                                 'idxvars': {'i': 'y4_hhid'},
                                 'myvars': {'previous_i': 'y3_hhid'}}


def test_no_data_info_in_the_corpus_uses_the_dead_key():
    """``function:`` is not a key ``Wave.column_mapping`` reads.  Written under
    a column it is ignored with no error, which is how this issue stayed silent.
    (``function:`` in a ``derivations.yml`` is a different, legitimate key.)"""
    offenders = []
    for path in countries_root().glob('*/*/_/data_info.yml'):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if re.match(r'\s*-?\s*function\s*:', line):
                offenders.append(f'{path}:{n}')
    assert not offenders, offenders


def test_cs_i_is_the_prefix():
    spec = importlib.util.spec_from_file_location(
        'malawi_2019_mapping_1278', WAVE_DIR / 'mapping.py')
    mapping = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mapping)
    assert mapping.cs_i(pd.Series(['123456789012'])) == PREFIX + '123456789012'


@pytest.mark.parametrize('script', SCRIPTS)
def test_each_household_script_prefixes_its_cross_sectional_half(script):
    """Statically: the script spells the prefix, in code and not only in prose."""
    tree = ast.parse((WAVE_DIR / f'{script}.py').read_text())
    literals = [n.value for n in ast.walk(tree)
                if isinstance(n, ast.Constant) and n.value == PREFIX]
    assert literals, f'{script}.py never applies {PREFIX!r} to the XS half'


def test_the_source_tables_behind_derivation_inputs_agree():
    """``derivation_inputs`` rebuilds ``i`` from ``malawi.py``'s own source table,
    so a stale prefix there unjoins the inputs from the served rows."""
    text = (MALAWI / '_' / 'malawi.py').read_text()
    block = text[text.index("'2019-20': [dict(half='xs'"):]
    assert re.search(r"hhid='case_id', prefix='cs-19-'", block[:400])


@pytest.mark.parametrize('name', ['panel_ids.json', 'updated_ids.json'])
def test_the_crosswalk_never_names_a_cross_sectional_household(name):
    """Regenerating it moved no byte: only the Panel half links back."""
    assert PREFIX not in (MALAWI / '_' / name).read_text()
    assert json.loads((MALAWI / '_' / name).read_text())


# --- data-gated: an oracle reading the cover files directly -----------------

def _cover(rel):
    from lsms_library.local_tools import format_id, get_dataframe
    df = get_dataframe(str(MALAWI / rel), convert_categoricals=False)
    return set(df['case_id'].apply(format_id).astype(str))


@pytest.fixture(scope='module')
def covers():
    return {'ihs5': _cover('2019-20/Data/Cross_Sectional/hh_mod_a_filt.dta'),
            'ihs3': _cover('2010-11/Data/Full_Sample/Household/hh_mod_a_filt.dta')}


@pytest.fixture(scope='module')
def spine():
    from lsms_library.country import Country
    fl = Country('Malawi').sample().reset_index()
    return {str(t): set(g['i'].astype(str)) for t, g in fl.groupby(fl['t'].astype(str))}


@requires_s3
def test_the_coincidences_exist_in_the_source(covers):
    """Why this matters at all.  If a re-release removes them this says so."""
    assert len(covers['ihs5']) == 11434
    assert len(covers['ihs5'] & covers['ihs3']) == 2


@requires_s3
def test_no_served_2019_id_is_a_bare_ihs5_id(covers, spine):
    assert not (spine['2019-20'] & covers['ihs5'])
    assert {PREFIX + x for x in covers['ihs5']} <= spine['2019-20']


@requires_s3
def test_the_false_links_are_gone(covers, spine):
    """A household shared by 2010-11 and 2019-20 is a crosswalk link, never a
    cross-sectional string that happens to match.  On the bare ids this set
    held exactly the two coincidences beyond the crosswalk."""
    uid = json.loads((MALAWI / '_' / 'updated_ids.json').read_text())['2019-20']
    assert spine['2010-11'] & spine['2019-20'] <= set(uid.values())
    assert not (spine['2019-20'] & (covers['ihs5'] & covers['ihs3']))


@requires_s3
def test_every_panel_household_keeps_its_link(spine):
    """Each 2019-20 panel household is served under the id its crosswalk names."""
    uid = json.loads((MALAWI / '_' / 'updated_ids.json').read_text())['2019-20']
    assert set(uid.values()) <= spine['2019-20']
    assert not any(i.startswith(PREFIX) for i in uid.values())


@requires_s3
@pytest.mark.parametrize('table', [
    'household_roster', 'individual_education', 'housing', 'interview_date',
    'subjective_well_being', 'food_security', 'shocks', 'assets', 'food_acquired',
    'crop_production', 'plot_inputs', 'plot_labor', 'plot_features', 'livestock',
    'anthropometry', 'people_last7days', 'food_coping', 'months_food_inadequate'])
def test_every_table_joins_the_spine_in_2019(table, spine):
    """No table was left behind: per wave, the ids are a subset of ``sample``'s.
    The symptom of a missed site is silence (an empty join reads as "this wave
    has no such data"), which is why this is checked table by table."""
    from lsms_library.country import Country
    fl = getattr(Country('Malawi'), table)().reset_index()
    ids = set(fl.loc[fl['t'].astype(str).eq('2019-20'), 'i'].astype(str))
    assert ids, f'{table}: no 2019-20 rows'
    assert ids <= spine['2019-20'], (
        f'{table}: {len(ids - spine["2019-20"])} of {len(ids)} ids are not in sample()')
