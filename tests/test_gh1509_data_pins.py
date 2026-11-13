"""Data-gated pins for the GH #1509 numbers the red team found unpinned.

The red team on #1509 (``framework-1509-redteam/VERDICT.md``, "Blind spot")
noted that no test pinned GhanaSPS, which moves by +3.93% kg under purchases-
only evidence, nor any ``nutrition`` total, although GhanaLSS ``nutrition``
moves by double digits and ``tests/test_ghanalss_nutrition.py`` passes either
way.  These pin the served numbers, measured cold in private roots at
``30523f946`` (base ``e5fdd2fa8``, pre-reorder head ``0966d62ef``):

| | base | head | this branch |
|---|---|---|---|
| GhanaSPS kg | 1,384,290.06 | 1,438,671.82 | 1,438,671.82 |
| GhanaLSS kg | 11,007,391.86 | 9,918,751.31 | 10,209,229.94 |
| GhanaLSS ``item_unit_nonpurchase`` rows | -- | 208,090 | 287,863 |
| GhanaLSS ``nutrition`` Energy | 18,256,924,922 | 14,848,083,268 | 15,503,685,127 |
| Uganda ``nutrition`` Energy | 2,087,321,650 | 2,065,011,048 | 2,065,011,048 |

The GhanaLSS rows and kilograms between head and this branch are the
maintainer's "Item self-valued first" (@ligon 2026-11-12): 79,773 produced rows
of no-purchase cells move from the u-pooled purchase ``unit`` rung to their
own item's self-valued factor, +290,479 kg.  A movement, not a correction: no
external unit weight was scored.  A pin moved here is a measured movement and
needs its reason recorded beside it.

Tolerance: ``rel=1e-6`` on totals, which absorbs the float-summation and BLAS
kernel noise measured on Uganda ``nutrition`` (eight cells, two ULPs) and
nothing a rung change could cause.
"""
import pytest

pytestmark = [pytest.mark.requires_s3, pytest.mark.slow]

#: ``food_quantities(units='kgs')`` total kilograms (``u == 'kg'`` rows).
GHANASPS_KG = 1_438_671.8197880546
GHANALSS_KG = 10_209_229.942355849
#: ``attrs['kg_factor_sources']`` on GhanaLSS ``food_quantities``.
GHANALSS_SOURCES = {'item_unit_nonpurchase': 287_863, 'unit': 814_840,
                    'unit_nonpurchase': 28_939, 'item_unit': 532_603,
                    'item_unit_tight': 5_980}
GHANASPS_SOURCES = {'item_unit_nonpurchase': 0, 'unit': 281_085,
                    'unit_nonpurchase': 0, 'item_unit': 92_855,
                    'item_unit_tight': 2_855}
#: ``nutrition()`` column totals.
GHANALSS_ENERGY = 15_503_685_126.745514
UGANDA_ENERGY = 2_065_011_048.4727514


def _country(name):
    try:
        import lsms_library as ll
        c = ll.Country(name)
        fa = c.food_acquired()
    except Exception as e:                    # pragma: no cover - no microdata
        pytest.skip(f'{name} food_acquired not buildable here: {e}')
    if fa is None or len(fa) == 0:            # pragma: no cover
        pytest.skip(f'{name} food_acquired is empty here')
    return c


def _kg(q):
    u = q.index.get_level_values('u').astype(str)
    return float(q.loc[u == 'kg', 'Quantity'].sum())


@pytest.mark.parametrize('name, kg, sources', [
    ('GhanaSPS', GHANASPS_KG, GHANASPS_SOURCES),
    ('GhanaLSS', GHANALSS_KG, GHANALSS_SOURCES),
])
def test_kilograms_and_rungs(name, kg, sources):
    q = _country(name).food_quantities(units='kgs')
    assert _kg(q) == pytest.approx(kg, rel=1e-6)
    got = q.attrs['kg_factor_sources']
    assert {k: got[k] for k in sources} == sources


@pytest.mark.parametrize('name, energy', [
    ('GhanaLSS', GHANALSS_ENERGY),
    ('Uganda', UGANDA_ENERGY),
])
def test_nutrition_energy_total(name, energy):
    n = _country(name).nutrition()
    assert float(n['Energy'].sum()) == pytest.approx(energy, rel=1e-6)
