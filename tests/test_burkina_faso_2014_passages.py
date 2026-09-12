"""Burkina Faso 2014 EMC: four quarterly passages, four independent 7-day recalls.

GH #851 / ``.coder/charter-recall-windows.md`` §2.7.

THE BUG, in two mechanisms that had to be fixed together.  ``2014/_/data_info.yml``
reads FOUR passage files -- ``emc2014_p{1..4}_conso7jours_16032015.dta`` -- under
one ``(t, v, i, j, u, s)`` index with no recall-occasion level.  The EMC is a
CONTINUOUS survey: the same households are revisited each quarter and the food
module is re-asked, so those are four DIFFERENT measurements of the same
(household, item), not four transactions inside one 7-day window.

1. ``food_acquired`` is in ``feature._ADDITIVE_MEASURE_COLUMNS``, so without a
   ``visit`` level the passages collide on one tuple and are SUMMED into a
   single figure presented as a 7-day number.
2. Only passage 1 fields the quantity grid (``qachat``/``uachat``/``qautocons``);
   passages 2-4 record money only.  So ``u`` arrived NaN for 100% of their rows,
   and ``groupby(dropna=True)`` in ``_normalize_dataframe_index`` DELETED them
   before the sum could even happen -- 460,438 of 557,822 rows (82.5%), taking
   CFA 261,824,419 of the wave's CFA 342,110,515 reported food expenditure
   (76.5%) with them.  That is the worst instance in the corpus of the
   NaN-index-key class (CLAUDE.md "Grain Collapse" §3b).

Mechanism 2 is why mechanism 1 was invisible: the passages were never actually
summed, they were deleted.  Fixing only (1) would have been cosmetic -- the
passage-2/3/4 rows never reach the index -- so the ``Unknown`` ``u`` sentinel
(GH #842, the Niger pattern) lands with it.

INSTRUMENT NOTE.  Do NOT assert only that the delivered index is unique: the
collapse makes it unique BY CONSTRUCTION, so such an assertion passes with the
bug fully present.  These tests assert on the PRE-collapse wave frame, on the
per-passage totals being recoverable, and on conservation between the two tiers.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


# Measured on a cold build, 2026-09-09 (empty LSMS_DATA_DIR, dvc-cache only).
N_WAVE_ROWS = 557_822                 # canonical rows extracted from the 4 files
N_DELIVERED_ROWS = 557_692            # after the lossless additive de-dup
PASSAGE_ROWS = {1: 152_589, 2: 139_834, 3: 136_948, 4: 128_451}
PASSAGE_EXPENDITURE = {1: 93_383_167.0, 2: 82_473_394.0,
                       3: 93_328_730.0, 4: 72_925_224.0}
WAVE_EXPENDITURE = 342_110_515.0
WAVE_QUANTITY = 4_208_737.6


# ---------------------------------------------------------------------------
# 1. Configuration -- no microdata needed, so these run everywhere.
# ---------------------------------------------------------------------------

def _wave_config():
    return load_yaml(countries_root() / 'Burkina_Faso' / '2014' / '_'
                     / 'data_info.yml')


def test_visit_is_declared_in_the_country_index():
    """``visit`` must be a DECLARED index level, or ``_normalize_dataframe_index``
    drops it as an "unexpected" level and the collapse re-sums the passages."""
    scheme = load_yaml(countries_root() / 'Burkina_Faso' / '_'
                       / 'data_scheme.yml')
    index = scheme['Data Scheme']['food_acquired']['index']
    levels = [tok.strip() for tok in index.strip('() ').split(',')]
    assert levels == ['t', 'v', 'i', 'j', 'u', 's', 'visit'], index


def test_every_passage_file_stamps_its_own_visit_number():
    """The passage number lives in the FILENAME and in no source column, so it
    has to be stamped per file.  Pin the wiring: file ``p{n}`` -> ``visit_p{n}``.

    This is the test that fails if someone adds a fifth passage file, or
    re-points an override at the wrong function -- both of which silently
    re-merge two recall occasions."""
    fa = _wave_config()['food_acquired']

    # Passage 1 is the un-overridden default declared in `myvars`.
    assert fa['myvars']['visit'][-1]['mappings'] == 'visit_p1'

    stamped = {}
    for entry in fa['file']:
        if isinstance(entry, str):
            stamped[entry] = fa['myvars']['visit'][-1]['mappings']
        else:
            name, overrides = next(iter(entry.items()))
            stamped[name] = overrides['visit'][-1]['mappings']

    assert len(stamped) == 4, stamped
    for name, fn in stamped.items():
        # 'emc2014_p3_conso7jours_16032015.dta' -> '3'
        passage = name.split('_p', 1)[1][0]
        assert fn == f'visit_p{passage}', (name, fn)


def test_the_stamp_functions_return_their_passage_number():
    """``visit_p{n}`` ignores the row it is handed and returns the constant."""
    import importlib.util

    path = countries_root() / 'Burkina_Faso' / '2014' / '_' / 'mapping.py'
    spec = importlib.util.spec_from_file_location('bf2014_mapping', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    for n in (1, 2, 3, 4):
        fn = getattr(mod, f'visit_p{n}')
        assert fn(pd.Series({'zd': '0001'})) == n
        assert isinstance(fn(pd.Series({'zd': 'anything'})), int)


def test_the_hook_refuses_a_frame_with_no_passage_marker():
    """Prose is not enforcement.  If a config edit ever stops stamping ``visit``,
    the hook must RAISE rather than quietly serve a 4x-summed 7-day figure."""
    import importlib.util

    path = countries_root() / 'Burkina_Faso' / '2014' / '_' / 'mapping.py'
    spec = importlib.util.spec_from_file_location('bf2014_mapping_norefuse',
                                                  path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    df = pd.DataFrame(
        {'Quantity': [1.0], 'Expenditure': [10.0], 'Produced': [0.0]},
        index=pd.MultiIndex.from_tuples(
            [('2014', 'v1', 'h1', 'Maize', 'Kg')],
            names=['t', 'v', 'i', 'j', 'u']),
    )
    with pytest.raises(KeyError, match='visit'):
        mod.food_acquired(df)

    # ... and a NaN `visit` must raise too, not be eaten by groupby's dropna --
    # which is the same silent-deletion mechanism that was costing this wave
    # 82.5% of its rows on `u`.
    df2 = pd.DataFrame(
        {'Quantity': [1.0, 2.0], 'Expenditure': [10.0, 20.0],
         'Produced': [0.0, 0.0], 'visit': [1, None]},
        index=pd.MultiIndex.from_tuples(
            [('2014', 'v1', 'h1', 'Maize', 'Kg'),
             ('2014', 'v1', 'h2', 'Maize', 'Kg')],
            names=['t', 'v', 'i', 'j', 'u']),
    )
    with pytest.raises(ValueError, match='carry no `visit`'):
        mod.food_acquired(df2)


# ---------------------------------------------------------------------------
# 2. Delivered data.
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def burkina():
    try:
        return Country('Burkina_Faso')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Burkina_Faso unavailable: {exc}')


@pytest.fixture(scope='module')
def wave_frame(burkina):
    """The PRE-collapse wave frame -- the only tier that shows what was lost."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return burkina['2014'].grab_data('food_acquired')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Burkina_Faso 2014 food_acquired could not be built: '
                    f'{type(exc).__name__}: {exc}')


@pytest.fixture(scope='module')
def delivered(burkina):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return burkina.food_acquired()
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Burkina_Faso food_acquired could not be built: '
                    f'{type(exc).__name__}: {exc}')


def test_wave_frame_carries_visit_one_through_four(wave_frame):
    assert 'visit' in wave_frame.index.names
    visit = wave_frame.index.get_level_values('visit')
    assert sorted(visit.unique().tolist()) == [1, 2, 3, 4]
    assert pd.api.types.is_integer_dtype(visit.dtype), visit.dtype
    assert wave_frame.groupby(level='visit').size().to_dict() == PASSAGE_ROWS


def test_no_row_carries_a_null_unit(wave_frame):
    """The ``Unknown`` sentinel (GH #842).  A NaN on the declared ``u`` level is a
    DEFERRED SILENT DELETION -- the row is served, then vanishes in whichever
    ``groupby`` runs first.  Passages 2-4 field no quantity grid at all, so
    without the sentinel every one of their rows is deleted."""
    u = wave_frame.index.get_level_values('u')
    assert pd.isna(u).sum() == 0
    assert (u.astype(str) == 'Unknown').sum() > 400_000


def test_delivered_table_keeps_the_passage_level(delivered):
    assert 'visit' in delivered.index.names
    wave = delivered.xs('2014', level='t', drop_level=False)
    visit = wave.index.get_level_values('visit')
    assert sorted(visit.unique().tolist()) == [1, 2, 3, 4]
    assert pd.api.types.is_integer_dtype(visit.dtype), visit.dtype


def test_per_passage_expenditure_is_recoverable(delivered):
    """The point of the level.  Each passage's own 7-day total must be readable
    off the delivered table -- not merged into an unmarked 28-day figure."""
    wave = delivered.xs('2014', level='t')
    per_passage = wave.groupby(level='visit')['Expenditure'].sum()
    for passage, expected in PASSAGE_EXPENDITURE.items():
        assert per_passage[passage] == pytest.approx(expected, rel=1e-9)
    # ... and the sum over passages is the wave's reported total.
    assert per_passage.sum() == pytest.approx(WAVE_EXPENDITURE, rel=1e-9)


def test_nothing_is_lost_and_nothing_is_manufactured(wave_frame, delivered):
    """Conservation between the extracted frame and the served table.

    The 130-row difference is the framework's ADDITIVE de-dup of 129 genuine
    within-passage duplicate groups (the same household reporting the same item
    twice inside one passage) -- lossless by construction, which is exactly what
    the equality of the two totals proves."""
    wave = delivered.xs('2014', level='t', drop_level=False)
    assert len(wave_frame) == N_WAVE_ROWS
    assert len(wave) == N_DELIVERED_ROWS
    for col, total in (('Expenditure', WAVE_EXPENDITURE),
                       ('Quantity', WAVE_QUANTITY)):
        assert wave_frame[col].sum() == pytest.approx(total, rel=1e-9)
        assert wave[col].sum() == pytest.approx(total, rel=1e-9)


def test_the_ehcvm_waves_ask_once_and_carry_visit_one(delivered):
    """``add_visit_level``'s own use case: the single-recall waves get the
    constant 1 so every wave shares one index shape.  Their NUMBERS must be
    untouched -- adding a constant level reduces nothing."""
    for t in ('2018-19', '2021-22'):
        wave = delivered.xs(t, level='t')
        assert wave.index.get_level_values('visit').unique().tolist() == [1]
    # Re-pinned 2026-09-12 (GH #876).  These two totals used to be the raw
    # `s07bq08` sums -- 65,315,307.5 and 39,905,158.0, the value of ONE purchase
    # per row, up to 30 days old.  The served Expenditure is now the row's own
    # 7-day purchased quantity -- (s07bq03a - s07bq04 - s07bq05).clip(0), the
    # survey's own accounting identity -- times that purchase's unit value, NA
    # where the units differ.  What this test is about -- that `add_visit_level`
    # reduces nothing -- is unchanged; only the level the numbers sit at moved.
    assert delivered.xs('2018-19', level='t')['Expenditure'].sum() == \
        pytest.approx(52_466_280.38, rel=1e-6)
    assert delivered.xs('2021-22', level='t')['Expenditure'].sum() == \
        pytest.approx(27_888_221.36, rel=1e-6)


def test_the_grain_collapse_no_longer_destroys_or_deletes(burkina):
    """The GrainCollapseWarning this cell used to raise on every cold build --
    "460,438 row(s) ... DELETED OUTRIGHT" -- must be gone, and gone because the
    index is right, not because it was silenced."""
    from lsms_library.country import GrainCollapseWarning

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        burkina.food_acquired()
    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)
             and 'food_acquired' in str(w.message)]
    assert not grain, [str(w.message)[:200] for w in grain]


def test_feature_still_sums_the_passages_for_cross_country_work(burkina):
    """The cross-country contract is UNCHANGED: ``Feature`` drops the
    non-canonical ``visit`` level and SUMS the additive columns, exactly as it
    does for GhanaLSS's repeated visits (GH #501).  The detail is kept where it
    can be seen -- at the Country level -- and collapsed where the canonical
    index has no room for it."""
    from lsms_library.feature import Feature

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        f = Feature('food_acquired')(['Burkina_Faso'])
    assert 'visit' not in f.index.names
    wave = f.xs('2014', level='t')
    assert wave['Expenditure'].sum() == pytest.approx(WAVE_EXPENDITURE, rel=1e-9)
