"""Nigeria interview_date: W4 stores a datetime, and every wave keys on ``i``.

GH #1037.  ``2018-19/_/interview_date.py`` used ``pd.to_datetime(x).date``
without the call, so every W4 row stored the TEXT of a bound method
(``<bound method Timestamp.date of Timestamp('2018-08-20 ...')>``).  Those
strings parse to NaT at read time (PR #1025's ``DatetimeCoercionWarning``
counted 10,288 of them) and ``dropna(how='all')`` deleted both W4 rounds:
2018Q3 and 2019Q1 served 0 rows.  The script now stores the source
``InterviewStart`` timestamp as ``datetime64``: 5,224 + 5,012 = 10,236 rows
come back, and the 52 blank at source are dropped like the other waves'
blank dates.

All four wave scripts also wrote the household id under ``j`` (the canonical
item level) and were rescued only by ``local_tools.map_index``'s legacy
j->i swap; they now write ``i``, as ``data_scheme.yml`` declares ``(t, i)``.

W3 (2015-16) served NO rows either: ``saq13m`` carries Stata value labels, so
a label-decoding read handed ``pd.to_datetime`` the text ``'9. SEPTEMBER'``
and every W3 row was NaT.  The script now reads the month CODE
(``convert_categoricals=False``): 2015Q3 0 -> 4,672 and 2016Q1 0 -> 4,590.

Two tiers: static checks on the wave scripts (no microdata), and a
cache-gated guardrail on the BUILT table.
"""
import ast
import io
import tokenize
import warnings

import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

COUNTRY = 'Nigeria'
NIGERIA = countries_root() / COUNTRY
SCRIPTS = sorted(NIGERIA.glob('*/_/interview_date.py'))
W4_ROUNDS = ('2018Q3', '2019Q1')
W3_ROUNDS = ('2015Q3', '2016Q1')



def _bound_date_attributes(src):
    """Positions of ``<expr>.date`` NOT followed by a call -- the bound method
    itself.  Read from the token stream, so docstrings and comments (which
    quote the old spelling) are not code."""
    toks = [t for t in tokenize.generate_tokens(io.StringIO(src).readline)
            if t.type not in (tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE,
                              tokenize.INDENT, tokenize.DEDENT)]
    hits = []
    for k in range(1, len(toks)):
        if (toks[k].type == tokenize.NAME and toks[k].string == 'date'
                and toks[k - 1].string == '.'):
            nxt = toks[k + 1].string if k + 1 < len(toks) else ''
            if nxt != '(':
                hits.append(toks[k].start)
    return hits


def test_every_wave_has_a_script():
    """The checks below are vacuous if the glob finds nothing."""
    waves = {p.parent.parent.name for p in SCRIPTS}
    assert {'2010-11', '2012-13', '2015-16', '2018-19'} <= waves, waves


@pytest.mark.parametrize('script', SCRIPTS, ids=lambda p: p.parent.parent.name)
def test_no_script_stores_a_bound_date_method(script):
    assert not _bound_date_attributes(script.read_text())


def test_the_detector_sees_the_old_spelling():
    """The pre-#1037 line, verbatim: the check above must catch it."""
    old = "myvars = dict(int_t=('InterviewStart', lambda x: pd.to_datetime(x).date))\n"
    assert _bound_date_attributes(old)
    assert not _bound_date_attributes(old.replace('.date)', '.date())'))


def _idxvars_dicts(src):
    """The keyword names of every ``idxvars = dict(...)`` in a script, read
    from its syntax tree -- whatever the spacing, quoting or line breaks."""
    found = []
    for node in ast.walk(ast.parse(src)):
        if (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == 'idxvars'
                        for t in node.targets)
                and isinstance(node.value, ast.Call)
                and getattr(node.value.func, 'id', None) == 'dict'):
            found.append({kw.arg: kw.value for kw in node.value.keywords})
    return found


@pytest.mark.parametrize('script', SCRIPTS, ids=lambda p: p.parent.parent.name)
def test_household_id_is_the_declared_i_level(script):
    declared = load_yaml(NIGERIA / '_' / 'data_scheme.yml')['Data Scheme']['interview_date']['index']
    assert 'i' in declared and 'j' not in declared, declared
    dicts = _idxvars_dicts(script.read_text())
    assert len(dicts) == 2, f'expected one idxvars per round, found {len(dicts)}'
    for kws in dicts:
        assert set(kws) == {'i', 't'}, sorted(kws)
        assert isinstance(kws['i'], ast.Constant) and kws['i'].value == 'hhid'


def test_the_idxvars_reader_sees_the_old_level():
    old = "idxvars = dict(j='hhid',\n               t=('hhid', lambda x: '2018Q3'))\n"
    assert set(_idxvars_dicts(old)[0]) == {'j', 't'}


def test_w3_reads_the_month_code_not_its_label():
    """W3's ``saq13m`` carries value labels; a decoding read yields the text
    ``'9. SEPTEMBER'``, which ``pd.to_datetime`` cannot assemble."""
    src = (NIGERIA / '2015-16' / '_' / 'interview_date.py').read_text()
    calls = [n for n in ast.walk(ast.parse(src))
             if isinstance(n, ast.Call)
             and getattr(n.func, 'id', None) == 'df_data_grabber']
    assert len(calls) == 2, len(calls)
    for call in calls:
        kw = {k.arg: k.value for k in call.keywords}
        assert 'convert_categoricals' in kw, ast.unparse(call)
        assert isinstance(kw['convert_categoricals'], ast.Constant)
        assert kw['convert_categoricals'].value is False


def test_a_label_month_is_what_failed():
    """The premise, pinned: the label NaTs, the code does not."""
    def f(month):
        return pd.to_datetime(pd.DataFrame(
            {'year': [2015], 'month': [month], 'day': [4]}), errors='coerce')
    assert f('9. SEPTEMBER').isna().all()
    assert f(9.0).notna().all()


# --- built-table guardrail -------------------------------------------------

@pytest.fixture(scope='module')
def built():
    ll = pytest.importorskip('lsms_library')
    from lsms_library.country import DatetimeCoercionWarning
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = ll.Country(COUNTRY).interview_date()
        except Exception as exc:                  # noqa: BLE001 - data absent
            pytest.skip(f'Nigeria interview_date unavailable: {exc}')
    if df is None or df.empty:
        pytest.skip('Nigeria interview_date empty (no microdata)')
    coercion = [w.message for w in caught
                if isinstance(w.message, DatetimeCoercionWarning)]
    return df, coercion


@pytest.mark.requires_s3
def test_w4_rounds_are_served(built):
    df, _ = built
    per_t = df.groupby(level='t').size()
    # 5,224 / 5,012 on the 2026-10-18 build; floors, not pins.
    for t in W4_ROUNDS:
        assert per_t.get(t, 0) > 5000, per_t.to_dict()


@pytest.mark.requires_s3
def test_int_t_is_a_datetime_and_nothing_failed_to_parse(built):
    df, coercion = built
    assert pd.api.types.is_datetime64_any_dtype(df['Int_t']), df['Int_t'].dtype
    assert not [w for w in coercion if w.column == 'Int_t'], [
        (w.count, w.examples) for w in coercion]
    w4 = df[df.index.get_level_values('t').isin(W4_ROUNDS)]['Int_t']
    assert w4.notna().all()
    years = w4.dt.year
    assert years.between(2018, 2019).all(), years.value_counts().to_dict()


@pytest.mark.requires_s3
def test_household_level_is_i(built):
    df, _ = built
    assert 'i' in df.index.names and 'j' not in df.index.names, df.index.names


@pytest.mark.requires_s3
def test_w3_rounds_are_served(built):
    df, _ = built
    per_t = df.groupby(level='t').size()
    # 4,672 / 4,590 on the 2026-10-19 build.  Floor and ceiling, not pins:
    # the cover pages hold 4,696 / 4,592 households, and the panel was
    # drawn as 5,000.
    for t in W3_ROUNDS:
        assert 4000 < per_t.get(t, 0) <= 5000, per_t.to_dict()
    w3 = df[df.index.get_level_values('t').isin(W3_ROUNDS)]['Int_t']
    assert w3.notna().all()
    assert w3.dt.year.between(2015, 2016).all(), w3.dt.year.value_counts().to_dict()
    # a day/month/year triple: no time of day (W4 is the wave that has one)
    assert (w3 == w3.dt.normalize()).all()
    ph = df.xs('2016Q1', level='t')['Int_t']
    assert (ph.dt.year == 2016).all()


@pytest.mark.requires_s3
def test_every_wave_parquet_is_written_on_i(built):
    """Structural, on what the wave scripts WROTE: the served frame alone
    cannot tell, because ``map_index``'s legacy j->i swap rescued ``j``.

    Requests ``built`` for its side effect, not its value: on a root built
    before this branch the country table is stale, so the fixture's read
    evicts the hashless wave parquets and re-runs the wave scripts.  Without
    it the test read whatever wave parquets were on disk -- run alone on a
    pre-branch root it failed on the OLD scripts' ``['j', 't']``."""
    from lsms_library.paths import data_root
    parquets = sorted(data_root(COUNTRY).glob('*/_/interview_date.parquet'))
    waves = {p.parent.parent.name for p in parquets}
    if not {'2010-11', '2012-13', '2015-16', '2018-19'} <= waves:
        pytest.skip(f'wave parquets not built: {sorted(waves)}')
    for p in parquets:
        names = list(pd.read_parquet(p).index.names)
        assert names == ['i', 't'], (p.parent.parent.name, names)
