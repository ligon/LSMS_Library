"""GH #1234: ``local_tools.map_index`` must not mint ``u = 'unit'``.

``map_index`` runs on every wave and country frame as it is built or read
(``Wave.grab_data``, the L2-country read gate, the assume-fresh serve, the
country make target).  It renamed a missing unit on the ``u`` index level to
the literal ``'unit'`` -- a spelling that reads as a real unit, is not one of
the sentinels the kg ladder refuses (``transformations._U_SENTINELS``), and
collides with genuine ``'unit'`` labels (Panama decodes ``unidad`` to
``'unit'``).  The GH #1041 ruling: a missing unit is ``U_UNKNOWN``
(``'Unknown'``), never a minted spelling.

The rename's KEYS are unchanged by the fix and pinned here: ``pd.NA`` (what a
``StringDtype(na_value=pd.NA)`` level yields -- the policy restored on read
since bd80d86c2) and the stringified ``'nan'`` / ``'<NA>'``.  A level whose
missing value is a NaN-policy ``NaN`` was never matched and still is not; that
residual is GH #1118's per-country work.
"""
import pandas as pd
import pytest

from lsms_library.local_tools import map_index
from lsms_library.transformations import U_UNKNOWN, _U_SENTINELS


def _frame(u_values, dtype, names=('t', 'i', 'u')):
    n = len(u_values)
    df = pd.DataFrame({
        names[0]: ['2016-17'] * n,
        names[1]: [f'h{k}' for k in range(n)],
        'u': pd.Series(u_values, dtype=dtype),
        'Quantity': [float(k) for k in range(n)],
    })
    return df.set_index(list(names))


def _u(df):
    return list(df.index.get_level_values('u'))


def test_sentinel_is_the_canonical_one():
    assert U_UNKNOWN == 'Unknown'
    assert U_UNKNOWN.lower() in _U_SENTINELS


def test_na_on_a_string_na_policy_level_becomes_unknown():
    """The live case: a ``string`` (pd.NA) ``u`` level, as the read path now
    restores it.  Development served ``'unit'`` here."""
    df = _frame(['Kilogram', None, 'Gram'], pd.StringDtype(na_value=pd.NA))
    out = map_index(df)
    assert _u(out) == ['Kilogram', U_UNKNOWN, 'Gram']
    assert 'unit' not in _u(out)


@pytest.mark.parametrize('spelling', ['nan', '<NA>'])
@pytest.mark.parametrize('dtype', [object, 'str', pd.StringDtype(na_value=pd.NA)])
def test_stringified_missing_becomes_unknown(spelling, dtype):
    """``astype(str)`` of a missing unit yields ``'nan'`` / ``'<NA>'``."""
    out = map_index(_frame(['Kilogram', spelling], dtype))
    assert _u(out) == ['Kilogram', U_UNKNOWN]


@pytest.mark.parametrize('dtype', [object, 'str', pd.StringDtype(na_value=pd.NA)])
def test_genuine_labels_are_untouched(dtype):
    """A source label that genuinely says ``unit`` (Panama ``unidad``), its
    case variants, an existing ``Unknown`` and real units survive unchanged."""
    labels = ['unit', 'Unit', 'units', 'Unknown', 'Kilogram', 'Value']
    out = map_index(_frame(labels, dtype))
    assert _u(out) == labels
    assert out['Quantity'].tolist() == [float(k) for k in range(len(labels))]


def test_values_and_shape_are_unchanged():
    df = _frame(['Kilogram', None, 'nan'], pd.StringDtype(na_value=pd.NA))
    out = map_index(df)
    assert out.shape == df.shape
    assert list(out.index.names) == ['t', 'i', 'u']
    assert out['Quantity'].tolist() == df['Quantity'].tolist()


def test_single_level_u_index():
    df = pd.DataFrame({'u': pd.Series(['Kilogram', None], dtype='string'),
                       'x': [1, 2]}).set_index('u')
    assert list(map_index(df).index) == ['Kilogram', U_UNKNOWN]


def test_w_is_still_renamed_to_t():
    out = map_index(_frame(['Kilogram', None], 'string', names=('w', 'i', 'u')))
    assert list(out.index.names) == ['t', 'i', 'u']
    assert _u(out) == ['Kilogram', U_UNKNOWN]


def test_legacy_j_i_swap_is_unchanged():
    """``j`` before ``i`` is the legacy household-under-``j`` shape: swapped."""
    df = _frame(['Kilogram', None], 'string', names=('t', 'j', 'u'))
    df = df.reset_index().assign(i=['maize', 'rice']).set_index(['t', 'j', 'i', 'u'])
    out = map_index(df)
    assert list(out.index.names) == ['t', 'i', 'j', 'u']
    assert list(out.index.get_level_values('i')) == ['h0', 'h1']
    assert _u(out) == ['Kilogram', U_UNKNOWN]


def test_cluster_item_frame_keeps_j():
    """``(t, v, j, u)`` with no ``i`` is a cluster-level item table
    (community_prices): no swap."""
    out = map_index(_frame(['Kilogram', None], 'string', names=('t', 'v', 'u'))
                    .reset_index().assign(j=['maize', 'rice'])
                    .set_index(['t', 'v', 'j', 'u']))
    assert list(out.index.names) == ['t', 'v', 'j', 'u']
    assert _u(out) == ['Kilogram', U_UNKNOWN]
