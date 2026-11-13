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


# --- round 2: a u PROMOTED from a column is served Unknown cold AND warm ----
#
# A wave script that writes ``u`` as a COLUMN (Malawi crop_production, all
# four waves) reaches ``Wave.grab_data``'s ``map_index`` with an index that has
# no ``u`` level, so nothing is renamed there.  ``load_from_waves``' per-wave
# ``_normalize_dataframe_index`` then promotes the column into the index, the
# L2-country parquet stores the pd.NA, and only the warm read gate's
# ``map_index`` renamed it: the cold build served pd.NA (8,902 rows) and the
# warm read 'Unknown'.  ``Country._finalize_result`` now applies map_index's
# own rename to the ``u`` level, so the served value no longer depends on
# cache state.

from types import SimpleNamespace

from lsms_library.country import Country as _CountryCls
from lsms_library.country import _normalize_dataframe_index
from lsms_library.local_tools import get_dataframe, to_parquet

_SCHEME = {'index': '(t, i, plot_id, crop, u)', 'Quantity': 'float'}
_TABLE = 'crop_production'


def _fake_country():
    """Bind the real ``_finalize_result`` onto a stand-in, stubbing only what
    this test does not exercise (index augmentation, id_walk, the v-join);
    the scaffold of tests/test_finalize_order_797.py."""
    fake = SimpleNamespace(name='Fixtureland', categorical_mapping={})
    fake._apply_categorical_mappings = (
        _CountryCls._apply_categorical_mappings.__get__(fake))
    fake._augment_index_from_related_tables = lambda df, scheme_entry, wave: df
    fake.data_scheme = []
    fake._updated_ids_cache = None
    fake._unit_kg_factors = lambda: {}
    fake._finalize_result = _CountryCls._finalize_result.__get__(fake)
    return fake


def _wave_script_frame():
    """What the wave script writes: ``u`` a string (pd.NA) COLUMN."""
    idx = pd.MultiIndex.from_tuples(
        [('2016-17', f'h{k}', '1', crop) for k, crop in
         enumerate(['Maize', 'Maize', 'Beans', 'Groundnut'])],
        names=['t', 'i', 'plot_id', 'crop'])
    df = pd.DataFrame({
        'u': pd.Series(['Kilogram', None, '50 Kg Bag', None],
                       dtype=pd.StringDtype(na_value=pd.NA), index=idx),
        'Quantity': [10.0, 3.0, 2.0, 7.0]}, index=idx)
    df.attrs['id_converted'] = True
    return df


def _built():
    # The build path: grab_data's map_index sees no u level ...
    df = map_index(_wave_script_frame())
    assert 'u' not in df.index.names
    # ... and load_from_waves' per-wave normalisation promotes the column.
    df = _normalize_dataframe_index(df, _SCHEME, '2016-17', _TABLE,
                                    country='Fixtureland')
    assert 'u' in df.index.names
    assert df.index.get_level_values('u').isna().sum() == 2
    return df


def test_promoted_u_is_unknown_cold_and_warm(tmp_path):
    fake = _fake_country()
    cold = fake._finalize_result(_built(), _SCHEME, _TABLE)

    path = tmp_path / f'{_TABLE}.parquet'
    to_parquet(_built(), path, absolute_path=True)
    warm = fake._finalize_result(map_index(get_dataframe(path)), _SCHEME, _TABLE)

    for leg, served in (('cold', cold), ('warm', warm)):
        u = pd.Series(served.index.get_level_values('u'))
        assert int(u.isna().sum()) == 0, f'{leg}: a missing u is served as NA'
        assert int(u.eq(U_UNKNOWN).sum()) == 2, leg
        assert set(u) == {'Kilogram', '50 Kg Bag', U_UNKNOWN}, leg
        assert not served.index.duplicated().any()
        assert served['Quantity'].sum() == 22.0
    pd.testing.assert_frame_equal(cold.sort_index(), warm.sort_index())


def test_finalize_rename_keeps_attrs_and_leaves_genuine_labels():
    fake = _fake_country()
    df = _built()
    df = df.rename(index={'Kilogram': 'unit'}, level='u')   # a genuine 'unit'
    out = fake._finalize_result(df, _SCHEME, _TABLE)
    u = pd.Series(out.index.get_level_values('u'))
    assert int(u.isna().sum()) == 0
    assert int(u.eq('unit').sum()) == 1 and int(u.eq(U_UNKNOWN).sum()) == 2
    assert out.attrs.get('id_converted') is True


def test_finalize_leaves_a_nan_policy_nan_alone():
    """map_index never matched a NaN-policy NaN; the finalize site asks
    map_index, so neither does it (GH #1118's decision, not this one)."""
    fake = _fake_country()
    df = _built()
    df.index = df.index.set_levels(
        df.index.levels[df.index.names.index('u')].astype('str'), level='u')
    assert df.index.get_level_values('u').isna().sum() == 2
    out = fake._finalize_result(df, _SCHEME, _TABLE)
    assert out.index.get_level_values('u').isna().sum() == 2
