"""`nonfood_expenditures` is ONE table across its declarers (GH #817).

It used to be two.  Togo (PR #773) shipped the canonical long
`(t, i, j) x {Expenditure, RecallWindow}`; Uganda and Nigeria shipped a WIDE
item-by-household matrix -- 41 and 96 item columns -- with `m` baked into the
index, produced by a pivot in their country-level `_/nonfood_expenditures.py`.
Nothing could catch it: `lsms_library/data_info.yml` named the feature in the
Feature Vocabulary but declared no `Columns:` block and no `index_info` entry,
and the two `!make` declarations carried no `index:` either, so
`_assert_built_required_columns`, the `dfs:` guard and the coverage grader had
nothing to grade against.  Every cell graded `sane` while the three frames
could not be stacked.

The tests split into two tiers on purpose:

* the CONFIG tier needs no microdata and runs everywhere.  It is the tier that
  would have failed in 2026-09, and it is what keeps a future country from
  re-declaring a bare `!make`;
* the DATA tier skips when a country cannot be built.  It pins the two things
  a re-pivot would break -- the long shape, and the sparsity convention (an
  unreported item has NO ROW, never a fabricated 0).

The per-wave money totals are pinned too, following the precedent of
`tests/test_togo_nonfood_expenditures.py`'s exact row count.  Reshaping a table
must not move money: the pivot summed the same numbers, and every one of the
seventeen (country, wave) totals below was measured identical before and after
the change.
"""
import re

import pytest
import yaml

from lsms_library.paths import countries_root

_DATA_INFO = countries_root().parent / 'data_info.yml'

with open(_DATA_INFO, encoding='utf-8') as _f:
    _CANONICAL = yaml.safe_load(_f)

TABLE = 'nonfood_expenditures'

#: Countries whose `data_scheme.yml` declares the table, discovered rather than
#: listed -- a fourth declarer must satisfy the same contract on arrival.
def _declarers():
    class _Loader(yaml.SafeLoader):
        pass
    _Loader.add_constructor('!make', lambda l, n: {'__make__': True})

    out = {}
    for yml in sorted(countries_root().glob('*/_/data_scheme.yml')):
        with open(yml, encoding='utf-8') as f:
            data = yaml.load(f, Loader=_Loader) or {}
        spec = (data.get('Data Scheme') or {}).get(TABLE)
        if spec is not None:
            out[yml.parent.parent.name] = spec
    return out


DECLARERS = _declarers()

#: (country, wave) -> total Expenditure, measured 2026-09-08 both BEFORE and
#: AFTER the #817 reshape.  Identical in every cell: the fix moved no money.
EXPECTED_TOTALS = {
    ('Uganda', '2005-06'): 258235592.0,
    ('Uganda', '2009-10'): 367348751.0,
    ('Uganda', '2010-11'): 322706249.5,
    ('Uganda', '2011-12'): 439852688.7,
    ('Uganda', '2013-14'): 537600540.0,
    ('Uganda', '2015-16'): 579731192.0,
    ('Uganda', '2018-19'): 740118640.0,
    ('Uganda', '2019-20'): 757048418.0,
    ('Nigeria', '2010Q3'): 124489779.1,
    ('Nigeria', '2011Q1'): 264207232.0,
    ('Nigeria', '2012Q3'): 308827687.0,
    ('Nigeria', '2013Q1'): 144483313.0,
    ('Nigeria', '2015Q3'): 134160553.0,
    ('Nigeria', '2016Q1'): 133478140.0,
    ('Nigeria', '2018Q3'): 208182611.0,
    ('Nigeria', '2019Q1'): 248757023.0,
    ('Togo', '2018'): 716006884.0,
}


# ---------------------------------------------------------------------------
# Config tier -- no microdata required
# ---------------------------------------------------------------------------

def test_at_least_three_countries_declare_it():
    """Guards the discovery above: an empty sweep must not pass vacuously."""
    assert set(DECLARERS) >= {'Uganda', 'Nigeria', 'Togo'}, sorted(DECLARERS)


def test_canonical_schema_exists():
    """`data_info.yml` must carry the block whose absence was the bug."""
    cols = (_CANONICAL.get('Columns') or {}).get(TABLE)
    assert cols, (
        f'{TABLE} has no Columns block in {_DATA_INFO}.  Without it nothing '
        f'checks what the declarers deliver -- which is how two incompatible '
        f'shapes coexisted (GH #817).')
    assert cols['Expenditure'].get('required') is True
    assert cols['Expenditure'].get('monetary') is True
    assert cols['Expenditure'].get('type') == 'float'
    # RecallWindow is optional: a survey with one window has nothing to
    # disambiguate.  It must NOT become required -- that would break Uganda
    # and Nigeria, whose modules do not record it.
    assert cols['RecallWindow'].get('optional') is True
    assert not cols['RecallWindow'].get('required')


def test_registered_in_index_info():
    """Registration is what checks the cross-country assembly (#817 step 3)."""
    idx = (_CANONICAL.get('Index Info') or {}).get('index_info') or {}
    assert idx.get(TABLE) == '(t, v, i, j)', idx.get(TABLE)


def test_v_is_joined_not_declared():
    """`v` is in the canonical index, so `_join_v_from_sample` must fire.

    A `skip_extra` entry would disarm it.  The table is household-grain and
    must never emit `v` itself (CLAUDE.md, "`sample()` and Cluster Identity").
    """
    skip = ((_CANONICAL.get('Join v from sample') or {}).get('skip_extra')
            or [])
    assert TABLE not in skip


@pytest.mark.parametrize('country', sorted(DECLARERS))
def test_declarer_declares_the_long_index(country):
    """No bare `!make`: the index and the column must be written down."""
    spec = DECLARERS[country]
    assert not spec.get('__make__'), (
        f'{country} declares {TABLE} as a bare `!make`, so it declares no '
        f'index and no columns and nothing can grade it.  Declare '
        f'`index: (t, i, j)` and `Expenditure: float` with '
        f'`materialize: make`.')
    got = re.sub(r'\s+', '', str(spec.get('index', '')))
    assert got == '(t,i,j)', (
        f'{country}: {TABLE} declares index {spec.get("index")!r}; the '
        f'canonical grain is (t, i, j).  `v` is joined from sample() at API '
        f'time and `m` is added on demand by market=, so neither belongs '
        f'here.')
    assert 'Expenditure' in spec, (
        f'{country}: {TABLE} must declare the required column Expenditure.')


@pytest.mark.parametrize('country', sorted(DECLARERS))
def test_no_market_level_declared(country):
    """`m` must not be baked into the table (CLAUDE.md, other_features)."""
    spec = DECLARERS[country]
    idx = re.sub(r'\s+', '', str(spec.get('index', '')))
    assert 'm' not in idx.strip('()').split(','), spec.get('index')


# ---------------------------------------------------------------------------
# Data tier -- skips when a country cannot be built
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def built():
    from lsms_library.country import Country
    out = {}
    for country in sorted(DECLARERS):
        try:
            out[country] = Country(country).nonfood_expenditures()
        except Exception as exc:                               # pragma: no cover
            out[country] = exc
    return out


def _frame(built, country):
    got = built.get(country)
    if got is None or isinstance(got, Exception):               # pragma: no cover
        pytest.skip(f'{country}/{TABLE} could not be built: {got!r}')
    return got


@pytest.mark.parametrize('country', sorted(DECLARERS))
def test_delivered_shape_is_long(built, country):
    df = _frame(built, country)
    assert 'j' in (df.index.names or []), (
        f'{country}/{TABLE} has no `j` level: {df.index.names}.  A wide '
        f'item-by-household matrix cannot take labels=, cannot be currency-'
        f'converted per item, and cannot be stacked with the other '
        f'declarers (GH #817).')
    assert 'm' not in (df.index.names or []), (
        f'{country}/{TABLE} bakes `m` into the index; it is added on demand '
        f'by `market=`.')
    assert 'Expenditure' in df.columns, sorted(df.columns)
    extra = set(df.columns) - {'Expenditure', 'RecallWindow'}
    assert not extra, (
        f'{country}/{TABLE} carries {len(extra)} column(s) beyond the '
        f'canonical two: {sorted(extra)[:8]}.  Item names as COLUMNS is the '
        f'wide shape coming back.')


@pytest.mark.parametrize('country', sorted(DECLARERS))
def test_no_fabricated_zeros(built, country):
    """Sparsity: an unreported item has no row, not an Expenditure of 0.

    The wide shape's `fillna(0)` made "not asked / not reported" and "reported
    zero" the same number -- against the convention
    `transformations.food_expenditures_from_acquired` documents for food.  A
    zero is the signature of that fabrication, so a zero is what is banned.

    NaN is deliberately NOT banned, and the distinction is the whole point: a
    NaN row is "the household reported buying this and the AMOUNT is missing",
    which is a fact the survey recorded.  Togo carries 121 such rows -- every
    row of s09b..s09f passes the gate `q02 == 1`, and
    `tests/test_togo_nonfood_expenditures.py` pins that all 108,445 of them
    reach the API.  Collapsing those to 0 would be the same mistake in the
    other direction.
    """
    df = _frame(built, country)
    e = df['Expenditure']
    assert e.notna().any(), (
        f'{country}/{TABLE}: Expenditure is 100% NaN.')
    assert not (e == 0).any(), (
        f'{country}/{TABLE}: {int((e == 0).sum())} rows have an Expenditure '
        f'of exactly 0.  Those are the zeros the pivot used to fabricate for '
        f'every (household, item) pair the survey never recorded.')
    assert not (e < 0).any(), (
        f'{country}/{TABLE}: {int((e < 0).sum())} rows have a negative '
        f'Expenditure.')


@pytest.mark.parametrize('country', sorted(DECLARERS))
def test_reshaping_moved_no_money(built, country):
    """Per-wave totals, measured identical before and after the #817 reshape."""
    df = _frame(built, country)
    expected = {w: v for (c, w), v in EXPECTED_TOTALS.items() if c == country}
    if not expected:                                            # pragma: no cover
        pytest.skip(f'no pinned totals for {country}')
    got = df.groupby(level='t')['Expenditure'].sum()
    assert set(got.index) == set(expected), (
        f'{country}/{TABLE} waves changed: {sorted(got.index)} vs '
        f'{sorted(expected)}')
    for wave, want in expected.items():
        assert abs(float(got[wave]) - want) < 0.5, (
            f'{country}/{TABLE} {wave}: total {float(got[wave]):.2f}, '
            f'expected {want:.2f}.  A reshape must not move money.')


def test_feature_assembles_across_declarers():
    """The point of the whole exercise: one table, three countries, one column."""
    import lsms_library as ll
    try:
        f = ll.Feature(TABLE)(sorted(DECLARERS))
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'Feature({TABLE!r}) could not be built: {exc!r}')
    assert 'country' in (f.index.names or []), f.index.names
    for level in ('t', 'i', 'j'):
        assert level in f.index.names, (level, f.index.names)
    assert 'Expenditure' in f.columns
    got = set(f.index.get_level_values('country'))
    assert got == set(DECLARERS), sorted(got)
    # Every country contributes real numbers -- the pre-#817 union-of-columns
    # assembly gave Expenditure entirely NaN for the two wide countries.
    per_country = f.groupby('country')['Expenditure'].count()
    assert (per_country > 0).all(), per_country.to_dict()
