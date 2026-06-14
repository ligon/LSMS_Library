"""Id index levels must look like canonical ``format_id`` output.

Motivated by the Uganda 2013-14 AGSEC regression: the AGSEC (agricultural)
modules encode ``HHID`` as a raw ``int32`` (e.g. ``1100401``) whereas every
other table uses the ``H00110-04-01`` form.  ``format_id`` always returns a
*string*, so a typed check on the household / cluster / person id levels
(``i`` / ``v`` / ``pid``) catches the ``int32`` -- and the ``'12345.0'``
float-stringified -- class of bug.

It does NOT catch a well-typed but wrongly-*encoded* id string (``'1100401'``
where ``'H00110-04-01'`` was expected): that cross-table mismatch is the job
of the ``_join_v_from_sample`` 100%-NaN-``v`` guard in ``country.py``.  See
``tests/test_join_v_silent_skip_warn.py``.
"""

import numpy as np
import pandas as pd
import pytest

from lsms_library.diagnostics import _check_ids_format_id_canonical
from lsms_library.paths import data_root


def _frame(i_values):
    """A minimal ``(t, i)``-indexed frame carrying *i_values* as the ``i`` level."""
    t = ["2013-14"] * len(i_values)
    idx = pd.MultiIndex.from_arrays([t, i_values], names=["t", "i"])
    return pd.DataFrame({"x": range(len(i_values))}, index=idx)


# --- unit tests: the check behaves as specified ---------------------------

def test_clean_string_ids_pass():
    chk = _check_ids_format_id_canonical(_frame(["1013000201", "1013000202"]))
    assert chk.status == "pass", chk.message


def test_hyphen_form_ids_pass():
    # The GSEC 'H#####-##-##' form is valid format_id output (no numeric strip).
    chk = _check_ids_format_id_canonical(_frame(["H00110-04-01", "H02110-04-01"]))
    assert chk.status == "pass", chk.message


def test_int32_ids_flagged():
    # The exact Uganda 2013-14 AGSEC class: raw int32 HHIDs.
    chk = _check_ids_format_id_canonical(
        _frame(np.array([1100401, 21100401], dtype="int32"))
    )
    assert chk.status != "pass"
    assert "i" in chk.message


def test_float_ids_flagged():
    chk = _check_ids_format_id_canonical(_frame(np.array([1100401.0, 2110.0])))
    assert chk.status != "pass"


def test_float_stringified_ids_flagged():
    chk = _check_ids_format_id_canonical(_frame(["12345.0", "678.0"]))
    assert chk.status != "pass"


def test_v_level_also_checked():
    idx = pd.MultiIndex.from_arrays(
        [["2013-14", "2013-14"], ["i1", "i2"], np.array([10, 20], dtype="int64")],
        names=["t", "i", "v"],
    )
    df = pd.DataFrame({"x": [1, 2]}, index=idx)
    chk = _check_ids_format_id_canonical(df)
    assert chk.status != "pass"
    assert "v" in chk.message


def test_label_levels_not_flagged():
    # j/u are NOT format_id levels; label values must not be flagged even
    # though they are not format_id output.
    idx = pd.MultiIndex.from_arrays(
        [["2013-14", "2013-14"], ["i1", "i2"], ["Maize", "Beans"]],
        names=["t", "i", "j"],
    )
    df = pd.DataFrame({"x": [1, 2]}, index=idx)
    assert _check_ids_format_id_canonical(df).status == "pass"


# --- integration: the freshly-fixed Uganda AGSEC features stay canonical ---

_UGANDA_AGSEC_FEATURES = [
    "plot_inputs",
    "plot_features",
    "plot_labor",
    "crop_production",
    "livestock",
]


@pytest.mark.parametrize("feature", _UGANDA_AGSEC_FEATURES)
def test_uganda_agsec_cached_ids_canonical(feature):
    """Cached Uganda AGSEC parquets carry canonical id levels (no builds)."""
    path = data_root("Uganda") / "var" / f"{feature}.parquet"
    if not path.exists():
        pytest.skip(f"{path} not cached")
    df = pd.read_parquet(path, engine="pyarrow")
    chk = _check_ids_format_id_canonical(df)
    assert chk.status == "pass", f"Uganda {feature}: {chk.message}"
