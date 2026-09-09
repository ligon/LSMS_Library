"""Regression tests for the 2009-10 ``99999`` harvest-quantity sentinel.

GH #861.  Uganda 2009-10's post-harvest module (AGSEC5A ``a5aq6a`` / AGSEC5B
``a5bq6a``) uses ``99999`` as a missing-value sentinel for "no information
reported on this harvest row" -- never stripped before this fix, inflating
2009-10's ``crop_production.Quantity`` sum by ~300x relative to every other
wave (``lsms_library/countries/Uganda/_/CONTENTS.org``, the now-closed TODO
at what was ~:2105-2109).  EPAR's own pipeline for this survey round applies
the identical unconditional rule to the coalesced A/B column
(``EPAR_UW_Uganda_UNPS_W1.do:558``:
``replace quantity_harv=. if quantity_harv==99999``).

The fix is declared per-condition as ``qty_sentinel`` in
``uganda.CROP_COLMAPS['2009-10']`` and applied in
``uganda.crop_production_for_wave`` as an EXACT-VALUE strip (never a range:
99998 / 100000 are real quantities and must survive) that turns the sentinel
into NaN WITHOUT dropping the row -- ``_qty_reported`` tracks whether the
source column carried *anything* before the strip so the pre-existing
"no measure reported" filter does not newly delete a sentinel-only row.

Two test families:

* ``test_qty_sentinel_*`` -- fixture-level, synthetic AGSEC5A rows, no data
  build.  Always run.
* ``test_2009_10_*`` (data-gated) -- read the REAL 2009-10 raw modules via
  ``get_dataframe`` (DVC -> S3) and call ``crop_production_for_wave``
  directly, bypassing the Country() cache entirely (no L2 read/write), so
  this is unaffected by cache staleness.  Skipped without S3 credentials,
  mirroring the house pattern in ``tests/test_uganda_crop_condition.py``.

No test here reads the shared ``~/.local/share/lsms_library`` cache through
``Country()`` / ``Feature()``, so none of them needs the
xfail-until-rewarm treatment: that cache is warmed by CI / prior sessions
and is PRE-fix until Uganda is rebuilt post-merge (see the PR description).
A future test that asserts "0 sentinels" via ``ll.Country('Uganda')
.crop_production()`` against the *warm shared cache* would need exactly that
treatment; this file deliberately avoids the warm cache to not need it.
"""
from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

SENTINEL = 99999


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    (Mirrors the identical helper in ``tests/test_uganda_crop_condition.py``
    et al.; duplication is the house pattern until a shared ``conftest.py``
    lands.)
    """
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (
        Path(__file__).parent.parent
        / "lsms_library" / "countries" / ".dvc" / "s3_creds"
    )
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


@pytest.fixture(scope="module")
def uganda_module():
    uganda_dir = str(countries_root() / "Uganda" / "_")
    sys.path.insert(0, uganda_dir)
    try:
        import uganda
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"cannot import uganda module: {exc!r}")
    return uganda


@pytest.fixture()
def in_2009_10_wave_dir():
    """`crop_production_for_wave` resolves `categorical_mapping.org` (crop /
    unit / condition label tables) via CWD-relative candidate dirs
    (`local_tools.get_categorical_mapping`'s `dirs=['./','../../_/',
    '../../../_/']`), exactly as when the real wave script runs with CWD
    set to its own `_/` directory. Reproduce that CWD so the direct-call
    tests below don't need to know the resolution mechanism."""
    wave_dir = countries_root() / "Uganda" / "2009-10" / "_"
    prev = os.getcwd()
    os.chdir(wave_dir)
    try:
        yield
    finally:
        os.chdir(prev)


# ---------------------------------------------------------------------------
# config-level: the sentinel is declared where the issue says it should be,
# and ONLY there
# ---------------------------------------------------------------------------

def test_2009_10_colmap_declares_qty_sentinel(uganda_module):
    colmap = uganda_module.CROP_COLMAPS["2009-10"]
    for season in ("A", "B"):
        for cond in colmap[season]["conditions"]:
            assert cond.get("qty_sentinel") == SENTINEL, (
                f"2009-10 season {season} does not declare qty_sentinel={SENTINEL}"
            )


def test_qty_sentinel_is_2009_10_only(uganda_module):
    """No other wave should silently inherit this sentinel.

    Measured (GH #861): 2010-11 through 2019-20 Quantity sums are in the
    same order of magnitude as each other with no 99999-sentinel signature;
    stripping 99999 there would be an undeclared, unmeasured change. If a
    future wave genuinely needs it, this test should be updated alongside
    a CONTENTS.org entry documenting the measurement -- it must never be
    silently inherited.
    """
    for wave, colmap in uganda_module.CROP_COLMAPS.items():
        if wave == "2009-10":
            continue
        for season in ("A", "B"):
            cm = colmap.get(season)
            if not cm:
                continue
            for cond in cm.get("conditions", []):
                assert cond.get("qty_sentinel") is None, (
                    f"{wave}/{season} unexpectedly declares qty_sentinel "
                    f"(GH #861 is a 2009-10-only fix)"
                )


# ---------------------------------------------------------------------------
# fixture-level: exact-value strip, never a range; row survives with NaN
# ---------------------------------------------------------------------------

def _synthetic_frame(uganda_module, qty_values):
    """A minimal AGSEC5A-shaped frame with N rows, one crop/unit/condition
    code repeated, varying only the harvest-quantity column ``a5aq6a``.
    """
    crop_map = uganda_module._crop_label_map()
    unit_map = uganda_module._harvest_unit_map()
    condition_map = uganda_module._harvest_condition_map()
    crop_code = next(iter(crop_map))
    unit_code = next(iter(unit_map))
    condition_code = next(iter(condition_map))
    n = len(qty_values)
    return pd.DataFrame({
        "HHID": [f"100000{i:04d}" for i in range(n)],
        "a5aq1": [1] * n,           # parcel
        "a5aq3": [1] * n,           # plot
        "a5aq5": [crop_code] * n,   # crop code
        "a5aq6a": qty_values,       # quantity
        "a5aq6c": [unit_code] * n,  # unit code
        "a5aq6b": [condition_code] * n,  # condition code
        "a5aq7a": [np.nan] * n,     # quantity sold -- deliberately empty,
        "a5aq8":  [np.nan] * n,     # value sold    -- so these rows have
                                     # NO OTHER reported measure, the exact
                                     # case that would previously have been
                                     # dropped once Quantity turned NaN.
    })


def test_qty_sentinel_exact_value_only(uganda_module, in_2009_10_wave_dir):
    """99999 -> NaN; 99998 and 100000 (near-sentinel real quantities) survive
    UNCHANGED. Exact-value match, never a range (GH #861)."""
    df5a = _synthetic_frame(uganda_module, [99999, 99998, 100000, 5.0])
    colmap = {
        "A": {
            "hhid": "HHID", "parcel": "a5aq1", "plot": "a5aq3", "crop": "a5aq5",
            "conditions": [{
                "qty": "a5aq6a", "unit": "a5aq6c", "condition": "a5aq6b",
                "qty_sold": "a5aq7a", "value_sold": "a5aq8", "month": None,
                "qty_sentinel": SENTINEL,
            }],
        },
        "B": None,
        "intercrop": None,
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = uganda_module.crop_production_for_wave("2009-10", df5a, None, None, colmap)

    qty_by_hhid = out.reset_index().set_index("i")["Quantity"]
    assert pd.isna(qty_by_hhid.loc["1000000000"]), "99999 must become NaN"
    assert qty_by_hhid.loc["1000000001"] == 99998, "99998 is a real quantity, must survive"
    assert qty_by_hhid.loc["1000000002"] == 100000, "100000 is a real quantity, must survive"
    assert qty_by_hhid.loc["1000000003"] == 5.0, "ordinary quantities untouched"


def test_qty_sentinel_row_is_not_dropped(uganda_module, in_2009_10_wave_dir):
    """A sentinel-only row (no Quantity_sold / Value_sold either) must STAY,
    with Quantity NaN -- it must not be deleted by the pre-existing 'no
    measure reported' filter now that Quantity itself turns NaN. All 3,097
    of 2009-10's real sentinel rows are exactly this shape (measured)."""
    df5a = _synthetic_frame(uganda_module, [99999])
    colmap = {
        "A": {
            "hhid": "HHID", "parcel": "a5aq1", "plot": "a5aq3", "crop": "a5aq5",
            "conditions": [{
                "qty": "a5aq6a", "unit": "a5aq6c", "condition": "a5aq6b",
                "qty_sold": "a5aq7a", "value_sold": "a5aq8", "month": None,
                "qty_sentinel": SENTINEL,
            }],
        },
        "B": None,
        "intercrop": None,
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = uganda_module.crop_production_for_wave("2009-10", df5a, None, None, colmap)

    assert len(out) == 1, (
        "the sentinel-only row was dropped instead of surviving with NaN "
        "Quantity -- GH #861 explicitly forbids dropping rows"
    )
    assert pd.isna(out["Quantity"].iloc[0])


def test_qty_sentinel_absent_is_a_no_op(uganda_module, in_2009_10_wave_dir):
    """A condition dict with no `qty_sentinel` key (every other wave) must
    behave exactly as before -- no accidental universal strip."""
    df5a = _synthetic_frame(uganda_module, [99999, 5.0])
    colmap = {
        "A": {
            "hhid": "HHID", "parcel": "a5aq1", "plot": "a5aq3", "crop": "a5aq5",
            "conditions": [{
                "qty": "a5aq6a", "unit": "a5aq6c", "condition": "a5aq6b",
                "qty_sold": "a5aq7a", "value_sold": "a5aq8", "month": None,
                # no qty_sentinel
            }],
        },
        "B": None,
        "intercrop": None,
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = uganda_module.crop_production_for_wave("2009-10", df5a, None, None, colmap)
    qty_by_hhid = out.reset_index().set_index("i")["Quantity"]
    assert qty_by_hhid.loc["1000000000"] == 99999, (
        "without qty_sentinel declared, 99999 must pass through untouched"
    )


# ---------------------------------------------------------------------------
# data-gated: the real 2009-10 wave, read directly (bypasses Country() cache)
# ---------------------------------------------------------------------------

@pytest.mark.slow
@pytest.mark.skipif(
    not _aws_creds_available(),
    reason="reads the raw UNPS Stata modules (DVC -> S3); no credentials in "
           "the `unit-tests` CI job (LSMS_SKIP_AUTH=1). Runs in `data-tests`.",
)
def test_2009_10_no_99999_quantity_sentinel_remains(uganda_module, in_2009_10_wave_dir):
    """Built directly from the raw 2009-10 modules (not the Country() cache),
    so this is immune to the shared cache being pre-fix / stale."""
    from lsms_library.local_tools import get_dataframe

    def _try(path):
        try:
            return get_dataframe(path, convert_categoricals=False)
        except Exception:
            return None

    root = countries_root() / "Uganda" / "2009-10" / "Data"
    df5a = _try(str(root / "AGSEC5A.dta"))
    df5b = _try(str(root / "AGSEC5B.dta"))
    df4a = _try(str(root / "AGSEC4A.dta"))
    if df5a is None or df5b is None:
        pytest.skip("could not read 2009-10 AGSEC5A/5B")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = uganda_module.crop_production_for_wave(
            "2009-10", df5a, df5b, df4a, uganda_module.CROP_COLMAPS["2009-10"])

    assert (df["Quantity"] == SENTINEL).sum() == 0, (
        "a 99999 sentinel survived the strip"
    )
    # Measured 2026-09-09, cold, isolated LSMS_DATA_DIR (GH #861): rows
    # unchanged at 25,485 (season A 13,244 / season B 12,241); pre-fix
    # Quantity sum was 310,664,565.84 (~300x every other wave); post-fix
    # 967,662.84, now the same order of magnitude as 2010-11 (708,149.50)
    # and 2011-12 (1,128,560.35).
    assert len(df) == 25485, f"row count changed: {len(df)} (expected 25485 -- rows must not be dropped)"
    total = df["Quantity"].sum()
    assert 0 < total < 5_000_000, (
        f"Quantity sum {total} is not in the corrected order of magnitude "
        f"(expected ~9.7e5, well under the pre-fix 3.1e8)"
    )
