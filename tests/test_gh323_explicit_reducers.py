"""GH #323 -- the EXPLICIT grain helpers a country's mapping.py calls by name.

The #323 policy (design note ``slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org``,
decision D1) is that **core does not aggregate**: the access path (``country.py``,
``feature.py``, ``local_tools.py``) never reduces grain.  A reducer a country
invokes EXPLICITLY, at build time, from its own ``_/mapping.py`` is a different
animal -- that is the country aggregating, visibly -- and is what these helpers
are.  See ``SkunkWorks/grain_aggregation_policy.org``.

The tests that matter here are the LOUD ones.  A reducer that quietly picks a
winner among disagreeing rows would just relocate the #323 bug into a new
function, so every path that would destroy an observed value must raise (or,
when the caller has explicitly accepted the loss, blank the cell and warn).
"""
import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (   # imported the way a country script does
    reduce_to_agreed,
    collapse_to_cluster_grain,
    add_visit_level,
    GrainConflict,
    GrainConflictWarning,
)


def _cluster_frame(regions=('Dakar', 'Dakar', 'Dakar', 'Thies', 'Thies', 'Thies')):
    """Two clusters x three households, as read off a household cover page."""
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '01')] * 3 + [('2018-19', '02')] * 3, names=['t', 'v'])
    return pd.DataFrame({'Region': list(regions),
                         'Rural': [False] * 3 + [True] * 3}, index=idx)


# --------------------------------------------------------------------------
# reduce_to_agreed -- the lossless path
# --------------------------------------------------------------------------

def test_agreeing_rows_collapse_losslessly():
    out = reduce_to_agreed(_cluster_frame())
    assert len(out) == 2
    assert out.loc[('2018-19', '01'), 'Region'] == 'Dakar'
    assert out.loc[('2018-19', '02'), 'Region'] == 'Thies'
    assert out.loc[('2018-19', '02'), 'Rural'] is True or out.loc[('2018-19', '02'), 'Rural']
    assert list(out.index.names) == ['t', 'v']


def test_unique_index_is_returned_untouched():
    """Nothing to collapse -> same rows, same order, same dtypes."""
    df = _cluster_frame().iloc[[0, 3]]              # one household per cluster
    out = reduce_to_agreed(df)
    pd.testing.assert_frame_equal(out, df)


def test_nan_is_absence_not_contradiction():
    """One row reports Dakar, another reports nothing: no OBSERVED value is
    discarded by keeping Dakar, so the completion is lossless and silent."""
    df = _cluster_frame(regions=('Dakar', np.nan, 'Dakar', 'Thies', 'Thies', 'Thies'))
    out = reduce_to_agreed(df)                       # must not raise
    assert out.loc[('2018-19', '01'), 'Region'] == 'Dakar'


def test_nan_index_key_rows_are_not_deleted():
    """groupby(dropna=False): a row whose declared index level is NaN is not
    annihilated by the reduce itself (#323 Site 3 is core's business, not ours)."""
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '01'), ('2018-19', '01'),
         ('2018-19', np.nan), ('2018-19', np.nan)], names=['t', 'v'])
    df = pd.DataFrame({'Region': ['Dakar', 'Dakar', 'Fatick', 'Fatick']}, index=idx)
    out = reduce_to_agreed(df)
    assert len(out) == 2, out
    assert 'Fatick' in set(out['Region'])


def test_empty_frame_passes_through():
    """grab_data hands the df_edit hook an empty, unnamed-index frame when no
    source file loaded; that must not become a hard failure."""
    empty = pd.DataFrame()
    pd.testing.assert_frame_equal(reduce_to_agreed(empty), empty)


# --------------------------------------------------------------------------
# reduce_to_agreed -- the must-be-loud path
# --------------------------------------------------------------------------

def test_disagreement_raises_by_default():
    df = _cluster_frame(regions=('Dakar', 'Diourbel', 'Dakar', 'Thies', 'Thies', 'Thies'))
    with pytest.raises(GrainConflict) as exc:
        reduce_to_agreed(df)
    msg = str(exc.value)
    assert 'Region' in msg                       # names the offending column
    assert "('2018-19', '01')" in msg            # names the offending group
    assert 'GH #323' in msg
    assert 'Rural' not in msg                    # the clean column is not accused


def test_disagreement_is_never_resolved_by_picking_a_winner():
    """The whole point: no mode of this helper returns 'Dakar' (or 'Diourbel')
    for a cluster whose households disagree."""
    df = _cluster_frame(regions=('Dakar', 'Diourbel', 'Dakar', 'Thies', 'Thies', 'Thies'))
    with pytest.raises(GrainConflict):
        reduce_to_agreed(df)                                   # default
    with pytest.warns(GrainConflictWarning):
        out = reduce_to_agreed(df, on_conflict='na')           # opt-in
    assert pd.isna(out.loc[('2018-19', '01'), 'Region'])


def test_on_conflict_na_blanks_only_the_conflicted_cells_and_warns():
    df = _cluster_frame(regions=('Dakar', 'Diourbel', 'Dakar', 'Thies', 'Thies', 'Thies'))
    with pytest.warns(GrainConflictWarning, match='Region'):
        out = reduce_to_agreed(df, on_conflict='na')
    assert len(out) == 2
    assert pd.isna(out.loc[('2018-19', '01'), 'Region'])       # the conflict -> NA
    assert out.loc[('2018-19', '02'), 'Region'] == 'Thies'     # the clean cluster survives
    assert not out.loc[('2018-19', '01'), 'Rural']             # the clean COLUMN survives


def test_na_is_conflict_flag_treats_a_missing_report_as_disagreement():
    df = _cluster_frame(regions=('Dakar', np.nan, 'Dakar', 'Thies', 'Thies', 'Thies'))
    with pytest.raises(GrainConflict):
        reduce_to_agreed(df, na_is_conflict=True)


def test_grain_strict_env_escalates_na_to_raise(monkeypatch):
    monkeypatch.setenv('LSMS_GRAIN_STRICT', '1')
    df = _cluster_frame(regions=('Dakar', 'Diourbel', 'Dakar', 'Thies', 'Thies', 'Thies'))
    with pytest.raises(GrainConflict, match='LSMS_GRAIN_STRICT'):
        reduce_to_agreed(df, on_conflict='na')


def test_grain_conflict_is_a_valueerror():
    """A country script catching ValueError still catches it."""
    assert issubclass(GrainConflict, ValueError)


def test_unnamed_index_on_a_nonempty_frame_raises():
    """A caller who forgot to set the declared index gets an error, not a
    silent no-op that would leave the collapse to core."""
    df = pd.DataFrame({'Region': ['Dakar', 'Dakar']})
    with pytest.raises(ValueError, match='named'):
        reduce_to_agreed(df)


def test_unknown_on_conflict_raises():
    with pytest.raises(ValueError, match='on_conflict'):
        reduce_to_agreed(_cluster_frame(), on_conflict='first')


# --------------------------------------------------------------------------
# collapse_to_cluster_grain -- the named cluster case
# --------------------------------------------------------------------------

def test_collapse_to_cluster_grain_dedups_identical_household_rows():
    out = collapse_to_cluster_grain(_cluster_frame())
    assert len(out) == 2
    assert out.loc[('2018-19', '01'), 'Region'] == 'Dakar'


def test_collapse_to_cluster_grain_raises_when_an_attribute_conflicts():
    """'Invariant by construction of the sampling design' is prose.  This is
    the enforcement: a cluster code unique only within a district merges two
    real clusters, and the collapse must not silently keep one Region."""
    df = _cluster_frame(regions=('Dakar', 'Diourbel', 'Dakar', 'Thies', 'Thies', 'Thies'))
    with pytest.raises(GrainConflict):
        collapse_to_cluster_grain(df)


def test_collapse_to_cluster_grain_works_as_a_bare_df_edit_hook():
    """Country scripts alias-import it (`... as cluster_features`), so it must
    be callable with the single positional frame grab_data passes."""
    hook = collapse_to_cluster_grain
    assert len(hook(_cluster_frame())) == 2


# --------------------------------------------------------------------------
# add_visit_level -- widens the index, never reduces it
# --------------------------------------------------------------------------

def _food_frame():
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', 'h1', 'Rice', 'kg', 'purchased'),
         ('2018-19', 'h1', 'Maize', 'kg', 'produced')],
        names=['t', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Quantity': [2.0, 3.0], 'Expenditure': [500.0, np.nan]}, index=idx)


def test_add_visit_level_appends_the_recall_occasion():
    out = add_visit_level(_food_frame(), visit=1)
    assert list(out.index.names) == ['t', 'i', 'j', 'u', 's', 'visit']
    assert set(out.index.get_level_values('visit')) == {1}
    assert len(out) == 2                       # widens the index; loses no rows
    assert out['Quantity'].tolist() == [2.0, 3.0]


def test_add_visit_level_takes_a_nondefault_passage():
    out = add_visit_level(_food_frame(), visit=3)
    assert set(out.index.get_level_values('visit')) == {3}


def test_add_visit_level_refuses_to_stamp_over_an_existing_visit():
    already = add_visit_level(_food_frame(), visit=2)
    with pytest.raises(ValueError, match='already present'):
        add_visit_level(already, visit=1)
    with pytest.raises(ValueError, match='already present'):
        add_visit_level(_food_frame().assign(visit=4))


# --------------------------------------------------------------------------
# The policy itself: core must not be able to reach these behind the caller
# --------------------------------------------------------------------------

_COUNTRY_FACING_REDUCERS = (
    'reduce_to_agreed', 'collapse_to_cluster_grain', 'add_visit_level',
)
_CORE_MODULES = ('country.py', 'feature.py', 'local_tools.py')


def _core_sources():
    from pathlib import Path
    import lsms_library
    root = Path(lsms_library.__file__).parent
    return [(mod, (root / mod).read_text()) for mod in _CORE_MODULES]


def test_core_never_reduces_grain_without_auditing_it():
    """D1, stated as the property it actually is.

    **The previous form of this test had its polarity inverted.**  It banned
    ``reduce_to_agreed`` / ``collapse_to_cluster_grain`` / ``add_visit_level``
    from core.  But those three helpers never CHOOSE: they raise on
    disagreement, or blank the cell to ``<NA>`` and warn.  Core calling one of
    them cannot violate D1.  Meanwhile the construct that DOES choose -- a bare
    ``groupby(...).first()``, which silently keeps one of two disagreeing
    observed values -- was not guarded at all.  The ban forbade the safe tools
    and permitted the dangerous one.

    So guard the dangerous one.  A core function may reduce grain with a raw
    pandas reducer only if it also AUDITS that reduction, in the same function,
    while the pre-collapse frame still exists (one line later the evidence is
    gone -- that is the whole lesson of GH #323).

    Note what this deliberately PERMITS: core calling a fail-loud helper.  If a
    site is rewritten to use ``reduce_to_agreed``, the raw reducer disappears
    and no audit is required, because nothing can be silently destroyed.  That
    is the intended gradient -- use the safe helper and you are free; use the
    raw primitive and you must show your work.

    Still not provable here: whether the audit is CORRECT, or whether the
    reduction is semantically right.  Behaviour is pinned in
    ``tests/test_gh323_grain_contract.py``.
    """
    import ast

    def _is_grain_reducer(node):
        """A pandas call that drops rows by selecting within groups."""
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return False
        attr = node.func.attr
        if attr in ('agg', 'aggregate') and node.args:
            a = node.args[0]
            return isinstance(a, ast.Constant) and a.value in ('first', 'last')
        if attr not in ('first', 'last', 'nth'):
            return False
        # only when the receiver is a groupby/resample chain
        chain, cur = [], node.func.value
        while isinstance(cur, (ast.Call, ast.Attribute, ast.Subscript)):
            if isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
                chain.append(cur.func.attr); cur = cur.func.value
            elif isinstance(cur, ast.Attribute):
                chain.append(cur.attr); cur = cur.value
            elif isinstance(cur, ast.Subscript):
                cur = cur.value
        return any(c in ('groupby', 'resample') for c in chain)

    offenders = []
    for mod, src in _core_sources():
        tree = ast.parse(src)
        for fn in [n for n in ast.walk(tree)
                   if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
            reducers = [n.lineno for n in ast.walk(fn) if _is_grain_reducer(n)]
            if not reducers:
                continue
            audits = [n for n in ast.walk(fn)
                      if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                      and 'audit' in n.func.id]
            if not audits:
                offenders.append(f'{mod}::{fn.name} reduces at line(s) {reducers} '
                                 f'with no audit call in the same function')

    assert not offenders, (
        'unaudited grain reduction in the access path: ' + '; '.join(offenders) +
        ' -- core may reduce only if it audits the reduction first, or uses a '
        'fail-loud reducer instead (GH #323 D1)'
    )


def test_core_does_not_read_an_aggregation_key():
    """The actual Design-A wiring: core dispatching a reducer named by config.

    Twelve branches of the #323 sweep proposed teaching core to read an
    ``aggregation:`` key from ``data_scheme.yml``.  D1 rejected it.  Core may
    mention the string (it is in the ``_skip`` set of scheme keys, so the key is
    ignored rather than silently treated as a column) -- but it must never
    RETRIEVE it, because retrieving it is the first line of Design A.
    """
    import ast

    offenders = []
    for mod, src in _core_sources():
        for node in ast.walk(ast.parse(src)):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == 'get' and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == 'aggregation'):
                offenders.append(f'{mod}:{node.lineno} .get("aggregation")')
            if (isinstance(node, ast.Subscript)
                    and isinstance(node.slice, ast.Constant)
                    and node.slice.value == 'aggregation'):
                offenders.append(f'{mod}:{node.lineno} ["aggregation"]')

    assert not offenders, (
        f'core retrieves an `aggregation:` config key: {offenders} -- that is '
        'core aggregating on a config\'s say-so (GH #323 D1, Design A rejected)'
    )


def test_the_reducers_are_not_yaml_dispatchable():
    """The other half of D1: no ``aggregation:`` key may drive a reducer.

    ``reduce_to_agreed`` / ``collapse_to_cluster_grain`` must NOT be registered
    as ``@build_transform``s, or a country's YAML could reach them through the
    ``derived:`` dispatch and core would be aggregating on a config's say-so --
    Design A through the back door.
    """
    from lsms_library import _build_registry

    registered = set(getattr(_build_registry, '_REGISTRY', {}) or {})
    if not registered:  # registry keyed differently; fall back to the tag
        from lsms_library import build_transforms as bt
        registered = {
            name for name in dir(bt)
            if getattr(getattr(bt, name, None), '_build_transform', False)
        }

    leaked = {'reduce_to_agreed', 'collapse_to_cluster_grain'} & registered
    assert not leaked, (
        f"{sorted(leaked)} is registered as a build transform, so a country's "
        "`derived:` YAML can dispatch it -- that is core aggregating on config's "
        "say-so (GH #323 D1)"
    )
