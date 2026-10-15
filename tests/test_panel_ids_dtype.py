"""``panel_ids()`` must REPLACE its id columns, not set them in place.

Regression for the ``pandas >= 3.0`` breakage fixed in
``fix(panel_ids): replace columns instead of setting in place``.

``62ac64c9e`` ("Fix (some) assignments that raise warnings in
panel_ids.py") rewrote ``df[col] = df[col].apply(format_id)`` as
``df.loc[:, col] = df[col].apply(format_id)`` to silence a
``SettingWithCopyWarning``.  Those are not the same operation:

- ``df[col] = ...``       *replaces* the column, so its dtype may change;
- ``df.loc[:, col] = ...`` sets *in place* and PRESERVES the dtype.

Household ids arrive from Stata as ``float64`` (with ``NaN`` for "no
previous wave"), and :func:`format_id` returns ``str``.  Writing strings
into a float64 column is therefore a lossy setitem: pandas 2.x upcast
with a warning, pandas 3.0 -- which this project targets
(``pyproject.toml``: ``pandas >=3.0``) -- raises

    TypeError: Invalid value '<ArrowStringArray> [...]' for dtype 'float64'

That aborted ``panel_ids.py`` for every country whose ids are numeric,
and because ``panel_ids.json`` is a prerequisite of every other target
in those countries' ``_/Makefile``, it took down *every* table with an
opaque ``CalledProcessError`` from the wave ``make`` -- e.g.
``Country('Uganda').food_expenditures()``.

The ``SettingWithCopyWarning`` that motivated ``62ac64c9e`` came from
``get_dataframe(path)[columns]`` being a *slice*; ``.copy()`` is the
correct remedy, and ``test_no_warnings`` below pins it so the next
warning-silencer does not reach for ``.loc`` again.

Hermetic: the only I/O boundary (``get_dataframe``) is stubbed, so this
needs no microdata, no DVC and no S3.
"""

from __future__ import annotations

import inspect
import warnings

import pandas as pd
import pytest

import lsms_library.local_tools as lt
from lsms_library.local_tools import panel_ids


# Two waves whose id columns arrive float64 with NaN -- the shape Stata
# hands us, and the shape that made `.loc[:, col] =` raise.
WAVES = {
    "2005-06": ("hh05.dta", "HHID", "HHID_old"),
    "2009-10": ("hh09.dta", "HHID", "HHID_old"),
}

SOURCES = {
    "../2005-06/Data/hh05.dta": pd.DataFrame(
        {
            "HHID": [1013000201.0, 1021000108.0],
            # first wave: nothing to point back to
            "HHID_old": [float("nan"), float("nan")],
        }
    ),
    "../2009-10/Data/hh09.dta": pd.DataFrame(
        {
            "HHID": [2013000201.0, 2021000108.0, 2099000999.0],
            # third household is new in this wave -> NaN
            "HHID_old": [1013000201.0, 1021000108.0, float("nan")],
        }
    ),
}


@pytest.fixture
def stub_sources(monkeypatch):
    """Stub the one I/O boundary; everything else is the shipped code."""
    monkeypatch.setattr(
        lt, "get_dataframe", lambda fn, *a, **k: SOURCES[fn].copy()
    )


def test_float_id_columns_do_not_raise(stub_sources):
    """The regression itself: float64 id columns must survive format_id.

    Before the fix this raised ``TypeError: Invalid value ... for dtype
    'float64'`` under pandas 3.0.
    """
    D, updated_ids = panel_ids(WAVES)

    assert set(updated_ids) == {"2005-06", "2009-10"}
    assert D is not None


def test_ids_are_strings_not_floats(stub_sources):
    """``format_id`` output must reach the map as ``str``.

    A column that stayed ``float64`` would round-trip the ids back to
    ``'1013000201.0'`` (or to floats), which silently fails to join
    against every other table's string ``i``.
    """
    _, updated_ids = panel_ids(WAVES)

    for wave, mapping in updated_ids.items():
        for current, previous in mapping.items():
            assert isinstance(current, str), (wave, current, type(current))
            assert isinstance(previous, str), (wave, previous, type(previous))
            assert not current.endswith(".0"), current
            assert not previous.endswith(".0"), previous


def test_previous_wave_mapping_is_correct(stub_sources):
    """The fix must preserve behaviour, not merely stop the crash."""
    _, updated_ids = panel_ids(WAVES)

    # Wave 2 households resolve back to their wave-1 ids...
    assert updated_ids["2009-10"]["2013000201"] == "1013000201"
    assert updated_ids["2009-10"]["2021000108"] == "1021000108"
    # ...and a household with no previous id is simply absent (dropna).
    assert "2099000999" not in updated_ids["2009-10"]


def test_no_warnings(stub_sources):
    """Pins the ``.copy()``.

    ``62ac64c9e`` reached for ``.loc`` to silence a
    ``SettingWithCopyWarning`` raised because ``get_dataframe(...)[cols]``
    is a slice.  ``.copy()`` removes the warning at its source, so the
    function is clean *without* the in-place assignment.  If this test
    starts failing, fix the copy -- do not reintroduce ``.loc``.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        panel_ids(WAVES)


def test_panel_ids_does_not_set_columns_in_place():
    """Static guard against re-landing ``62ac64c9e``.

    The runtime tests above only fail on a pandas that actually raises.
    This one states the rule directly, so the regression is caught by
    reading the source rather than by a future pandas release.
    """
    src = inspect.getsource(panel_ids)
    offenders = [
        line.strip()
        for line in src.splitlines()
        if ".loc[:," in line or ".loc[:, " in line
    ]
    assert not offenders, (
        "panel_ids() assigns whole columns with `.loc[:, col] =`, which sets "
        "in place and preserves dtype; use `df[col] = ...` to replace the "
        "column. See this module's docstring.\n  " + "\n  ".join(offenders)
    )
