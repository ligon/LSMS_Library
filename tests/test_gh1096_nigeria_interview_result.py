"""Nigeria ``interview_date`` and ``sample``: the cover's interview result is served.

GH #1096 / #1444, rulings @ligon 2026-11-04.  Every wave's cover records the visit
whether or not anyone was interviewed, so ``Int_t`` on 419 W1-W4 rows dates an
attempt (216 W1-W3, 203 W4).  Those rows stay, and ``InterviewResult`` says what
the visit produced, so a user can filter; ``sample`` carries it too, in all ten
rounds.  Interview status does NOT gate who a round's frame admits: the #1120 D1
rule (cover, supplement cover, own-round roster) still decides.

Two tiers: the label rule and the wiring (no data), then a guardrail on the built
table whose oracle reads each round's cover directly, with its value labels.
"""
import ast
import importlib.util
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

NIGERIA = countries_root() / 'Nigeria'
#: The cover field each round records its result in.  Not ``saq22b`` (status of
#: DATA ENTRY) and not W3-PH's ``saq22`` (a yes/no on observations).
RESULT_COLUMN = {
    '2010Q3': 'saq22a', '2011Q1': 'sq1', '2012Q3': 'saq22a', '2013Q1': 'saq1',
    '2015Q3': 'saq22a', '2016Q1': 'saq22a',
    '2018Q3': 'interview_result', '2019Q1': 'interview_result',
}
#: `sample` also serves W5, which has no interview_date script.
SAMPLE_RESULT_COLUMN = {**RESULT_COLUMN, '2023Q3': 'interview_result',
                        '2024Q1': 'interview_result'}
FOLDER = {'2010Q3': '2010-11', '2011Q1': '2010-11', '2012Q3': '2012-13',
          '2013Q1': '2012-13', '2015Q3': '2015-16', '2016Q1': '2015-16',
          '2018Q3': '2018-19', '2019Q1': '2018-19',
          '2023Q3': '2023-24', '2024Q1': '2023-24'}
COMPLETED = {'Completed', 'Partially completed',
             'Partially completed (refused)', 'Partially completed (unavailable)'}


@pytest.fixture(scope='module')
def nigeria():
    spec = importlib.util.spec_from_file_location('_nigeria_1096', NIGERIA / '_' / 'nigeria.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- the label rule --------------------------------------------------------

@pytest.mark.parametrize('recorded,served', [
    ('completed', 'Completed'),                      # W1
    ('COMPLETED', 'Completed'),                      # W2
    ('1. COMPLETED', 'Completed'),                   # W3's value label
    ('1. COMPLETE', 'Completed'),                    # W4
    ('partially completed', 'Partially completed'),
    ('2. PARTIALLY COMPLETE (REFUSED)', 'Partially completed (refused)'),
    ('3. PARTIALLY COMPLETE (UNAVAILABLE)', 'Partially completed (unavailable)'),
    ('NOT AT HOME', 'Not at home'),
    ('4. REFUSED', 'Refused'),
    ('7. REFUSED', 'Refused'),
    ('HOUSEHOLD NOT LOCATED', 'Household not located'),
    ('moved away', 'Moved away'),
    ('5. HOUSEHOLD HAS MOVED AWAY', 'Moved away'),
    ('other (specify)', 'Other'),
    ('4. UNABLE TO IDENTIFY HOUSEHOLD', 'Unable to identify household'),
    ('6. LONG TERM UNAVAILABLE', 'Long-term unavailable'),
    ('6. UNAVAILABLE FOR INTERVIEW', 'Unavailable for interview'),   # W5
    ('8. ALL HH MEMBERS DECEASED', 'All household members deceased'),   # W5
])
def test_recorded_labels_are_served_without_folding_categories(nigeria, recorded, served):
    assert nigeria.interview_result_label(recorded) == served


def test_w4_categories_stay_distinct(nigeria):
    """W4's refusal and absence partials are different answers; so are W2's
    'not located' and W4's 'unable to identify'."""
    labels = {nigeria.interview_result_label(x) for x in (
        '2. PARTIALLY COMPLETE (REFUSED)', '3. PARTIALLY COMPLETE (UNAVAILABLE)',
        'PARTIALLY COMPLETED', 'HOUSEHOLD NOT LOCATED',
        '4. UNABLE TO IDENTIFY HOUSEHOLD')}
    assert len(labels) == 5


@pytest.mark.parametrize('value', ['SOMETHING NEW', '9. SOMETHING NEW', ''])
def test_an_unrecorded_label_is_refused_not_guessed(nigeria, value):
    with pytest.raises(ValueError, match='interview result'):
        nigeria.interview_result_label(value)


def test_a_blank_result_stays_missing(nigeria):
    assert pd.isna(nigeria.interview_result_label(np.nan))
    assert pd.isna(nigeria.interview_result_label(pd.NA))


@pytest.mark.parametrize('code,served', [
    (1, 'Completed'), (2, 'Partially completed'), (3, 'Not at home'),
    (4, 'Refused'), (5, 'Household not located'), (6, 'Moved away'), (7, 'Other'),
    (1.0, 'Completed'),
])
def test_w3_reads_codes(nigeria, code, served):
    """W3's interview_date reads codes (GH #1037), W1-W3 share these codes, and
    W4's differ: its 4 is 'unable to identify', its 7 'refused'."""
    assert nigeria.response_status_label(code) == served


def test_w3_code_outside_the_card_is_refused(nigeria):
    with pytest.raises(ValueError, match='interview result'):
        nigeria.response_status_label(9)


# --- the wiring ------------------------------------------------------------

def _grabber_calls(folder):
    tree = ast.parse((NIGERIA / folder / '_' / 'interview_date.py').read_text())
    # myvars is built by assignment; collect each dict(...) naming InterviewResult.
    return [node.value for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == 'myvars' for t in node.targets)]


@pytest.mark.parametrize('folder,columns', [
    ('2010-11', {'saq22a', 'sq1'}), ('2012-13', {'saq22a', 'saq1'}),
    ('2015-16', {'saq22a'}), ('2018-19', {'interview_result'}),
])
def test_each_round_reads_its_own_result_field(folder, columns):
    calls = _grabber_calls(folder)
    assert len(calls) == 2
    named = set()
    for call in calls:
        kw = {k.arg: k.value for k in call.keywords}
        assert 'InterviewResult' in kw, ast.unparse(call)
        named.add(kw['InterviewResult'].elts[0].value)
    assert named == columns


@pytest.mark.parametrize('folder', ['2010-11', '2012-13', '2015-16', '2018-19'])
def test_scripts_keep_undated_rows_out(folder):
    """A second column would otherwise carry a row with no date past the
    framework's dropna(how='all'), which is what used to drop it."""
    src = (NIGERIA / folder / '_' / 'interview_date.py').read_text()
    assert "dropna(subset=['int_t'])" in src


def test_the_column_is_declared():
    scheme = load_yaml(NIGERIA / '_' / 'data_scheme.yml')['Data Scheme']['interview_date']
    assert scheme['InterviewResult'] == 'str'
    assert load_yaml(NIGERIA / '_' / 'data_scheme.yml')['Data Scheme']['sample'][
        'InterviewResult'] == 'str'
    columns = load_yaml(countries_root().parent / 'data_info.yml')['Columns']
    for table in ('interview_date', 'sample'):
        assert columns[table]['InterviewResult']['type'] == 'str'
        assert columns[table]['InterviewResult']['optional'] is True


@pytest.mark.parametrize('folder', sorted(set(FOLDER.values())))
def test_each_round_names_its_result_field_beside_its_cover(nigeria, folder):
    spec = nigeria.ROUND_COVERS[folder]
    for visit, t in (('pp', nigeria.PP_QUARTER[folder]), ('ph', nigeria.PH_QUARTER[folder])):
        assert spec[visit + '_result'] == SAMPLE_RESULT_COLUMN[t]


def test_sample_keeps_the_result_and_cluster_features_does_not(nigeria, monkeypatch):
    """One read, two tables: a household column must not reach the (t, v)
    collapse, and must not filter the frame."""
    frame = pd.DataFrame(
        {'v': ['1/1/1', '1/1/1'], 'weight': [1., 2.], 'panel_weight': [1., 2.],
         'strata': 'z', 'Rural': 'Rural', 'Region': 'z', 'District': 's',
         'InterviewResult': ['Completed', 'Refused']},
        index=pd.Index(['a', 'b'], name='i'))
    monkeypatch.setattr(nigeria, 'round_covers',
                        lambda folder: {'2018Q3': frame, '2019Q1': frame})
    out = nigeria.sample_for_wave('2018-19')
    assert out.xs('2018Q3', level='t')['InterviewResult'].tolist() == ['Completed', 'Refused']
    assert len(out) == 4
    from lsms_library import local_tools
    monkeypatch.setattr(
        local_tools, 'df_data_grabber',
        lambda fn, idxvars, **myvars: pd.DataFrame(
            {n: [1., 2.] for n in myvars}, index=pd.Index(['a', 'b'], name='i')))
    clusters = nigeria.cluster_features_for_wave('2018-19')
    assert len(clusters) == 4
    assert 'InterviewResult' not in clusters.columns


def test_cover_frame_asks_only_a_rounds_household_cover(nigeria, monkeypatch):
    """W2's agriculture supplement has the same file name as its household cover."""
    from lsms_library import local_tools
    asked = {}

    def grabber(fn, idxvars, convert_categoricals=True, **myvars):
        asked[fn] = set(myvars)
        names = list(myvars) or ['x']
        return pd.DataFrame({n: ['1/1/1'] for n in names},
                            index=pd.Index(['a'], name='i'))
    monkeypatch.setattr(local_tools, 'df_data_grabber', grabber)
    spec = nigeria.ROUND_COVERS['2012-13']
    for key, weights in (('pp', True), ('ph', True), ('pp_supplement', False)):
        nigeria._cover_frame(nigeria._data_path('2012-13', spec[key]), spec, weights=weights)
    keys = {fn.split('/Data/', 1)[1]: names for fn, names in asked.items()}
    assert 'InterviewResult' in keys[spec['pp']]
    assert 'InterviewResult' in keys[spec['ph']]
    assert 'InterviewResult' not in keys[spec['pp_supplement']]


def test_interview_status_does_not_gate_the_frame():
    """#1444: the round's frame is decided by the D1 rule, not by a status.

    Re-pinned when `sample` gained the column: the source check is now the
    functions that decide membership; `sample_for_wave` only keeps a column, which
    the synthetic test above and the built-table test below hold to its rows."""
    src = (NIGERIA / '_' / 'nigeria.py').read_text()
    tree = ast.parse(src)
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
               and n.name in ('round_covers', '_roster_locator')]:
        text = ast.get_source_segment(src, fn)
        code = '\n'.join(line for line in text.splitlines()
                         if not line.lstrip().startswith('#'))
        body = ast.get_docstring(fn) or ''
        for needle in ('interview_result', 'InterviewResult', 'saq22a', 'HHTrack', 'hhstatus'):
            assert needle not in code.replace(body, ''), (fn.name, needle)


# --- the built table: the oracle reads the covers directly -------------------

def _cover(nigeria, t):
    folder = FOLDER[t]
    visit = 'pp' if nigeria.PP_QUARTER[folder] == t else 'ph'
    path = NIGERIA / folder / 'Data' / nigeria.ROUND_COVERS[folder][visit]
    cover = get_dataframe(path)
    cover.index = cover['hhid'].map(format_id)
    return cover, get_dataframe(path, convert_categoricals=False)


def _dated(raw, t):
    raw = raw.set_index(raw['hhid'].map(format_id))
    if FOLDER[t] == '2018-19':
        return pd.to_datetime(raw['InterviewStart'], errors='coerce')
    triple = raw[['saq13y', 'saq13m', 'saq13d']].set_axis(['year', 'month', 'day'], axis=1)
    return pd.to_datetime(triple, errors='coerce')


@pytest.fixture(scope='module')
def interview_date():
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return ll.Country('Nigeria').interview_date()


@pytest.mark.requires_s3
@pytest.mark.parametrize('t', sorted(RESULT_COLUMN))
def test_served_result_is_the_covers_own(nigeria, interview_date, t):
    cover, raw = _cover(nigeria, t)
    frame = interview_date.xs(t, level='t').droplevel(
        [n for n in interview_date.index.names if n not in ('t', 'i')])
    dated = _dated(raw, t).dropna()
    # The rows are exactly the cover's dated households.
    assert set(frame.index) == set(dated.index)
    expected = cover.loc[frame.index, RESULT_COLUMN[t]].astype('string').map(
        nigeria.interview_result_label)
    assert (frame['InterviewResult'].astype('string').values == expected.values).all()


@pytest.mark.requires_s3
def test_dated_non_interviews_are_kept_and_labelled(interview_date):
    served = interview_date['InterviewResult'].astype('string')
    assert served.notna().all()
    assert interview_date['Int_t'].notna().all()
    t = interview_date.index.get_level_values('t')
    not_interviewed = ~served.isin(COMPLETED).to_numpy(dtype=bool)
    for wave in (('2010Q3', '2011Q1', '2012Q3', '2013Q1', '2015Q3', '2016Q1'),
                 ('2018Q3', '2019Q1')):
        assert not_interviewed[t.isin(wave)].sum() > 100, wave


@pytest.mark.requires_s3
def test_rounds_keep_distinct_t(interview_date):
    assert set(RESULT_COLUMN) == set(interview_date.index.get_level_values('t'))
    assert interview_date.index.is_unique


@pytest.mark.requires_s3
@pytest.mark.parametrize('t', sorted(SAMPLE_RESULT_COLUMN))
def test_sample_result_is_the_covers_own(nigeria, t):
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        sample = ll.Country('Nigeria').sample()
    frame = sample.xs(t, level='t')
    frame = frame.droplevel([n for n in frame.index.names if n != 'i'])
    cover, _ = _cover(nigeria, t)
    served = frame['InterviewResult'].astype('string')
    on_cover = frame.index.isin(cover.index)
    # Every cover household is served, and status removes none of them.
    assert set(cover.index) <= set(frame.index)
    expected = cover.loc[frame.index[on_cover], SAMPLE_RESULT_COLUMN[t]].astype('string').map(
        nigeria.interview_result_label)
    assert (served[on_cover].values == expected.values).all()
    # A household only a supplement cover or a roster lists has no result.
    assert served[~on_cover].isna().all()


@pytest.mark.requires_s3
def test_roster_only_households_stay_served_with_no_result():
    """#1444's two households: a roster, no cover in that round, HHTrack says not
    interviewed.  Status gates nothing, so they stay in ``sample`` and the roster;
    with no cover they have no date and no result, which is what is recorded."""
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        country = ll.Country('Nigeria')
        sample = country.sample()
        roster = country.household_roster()
        dates = country.interview_date()
    for frame, present in ((sample, True), (roster, True), (dates, False)):
        ids = frame.xs('2016Q1', level='t').index.get_level_values('i').astype(str)
        assert ({'320201', '30082'} <= set(ids)) is present
    rows = sample.xs('2016Q1', level='t')
    rows = rows[rows.index.get_level_values('i').astype(str).isin(['320201', '30082'])]
    assert rows['InterviewResult'].isna().all()
