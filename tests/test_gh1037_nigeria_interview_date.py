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

Two tiers: static checks on the wave scripts (no microdata), and a
cache-gated guardrail on the BUILT table.
"""
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


@pytest.mark.parametrize('script', SCRIPTS, ids=lambda p: p.parent.parent.name)
def test_household_id_is_the_declared_i_level(script):
    declared = load_yaml(NIGERIA / '_' / 'data_scheme.yml')['Data Scheme']['interview_date']['index']
    assert 'i' in declared and 'j' not in declared, declared
    src = script.read_text()
    assert "j='hhid'" not in src
    assert "i='hhid'" in src


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
