"""Ethiopia ``plot_inputs.j``: one "not crop-specific" marker (GH #1119).

Ruling (@ligon, 2026-10-23): the marker is ``(not crop-specific)`` everywhere
-- the EHCVM / Mali spelling and constant name (``CROP_NA``).  Ethiopia spelled
it ``Whole Plot``.  It is a ``j`` value, distinct from the STRUCTURAL
``plot_id`` sentinel ``Farm-level``.

Ethiopia's other plot tables carry no such marker: ``plot_labor`` and
``plot_features`` have no ``j`` level and ``crop_production.j`` is a crop.

Tiers: the synthetic test needs no data; ``requires_s3`` builds the table.
"""
from __future__ import annotations

import importlib.util
import sys
import warnings

import pandas as pd
import pytest

from lsms_library.paths import countries_root

ROOT = countries_root() / "Ethiopia"
MARKER = "(not crop-specific)"


@pytest.fixture(scope="module")
def eth():
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_1119_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


def test_the_marker_is_the_canonical_spelling(eth):
    assert eth.CROP_NA == MARKER
    assert not hasattr(eth, "WHOLE_PLOT_SENTINEL")


def test_plot_level_inputs_carry_the_marker(eth):
    """A §3 fertilizer row and an organic row: no crop, so the marker."""
    raw = pd.DataFrame(dict(hh=["H", "H"], holder=[1, 1], parcel=[1, 1],
                            field=[1, 2], urea_used=[1, 2], urea_kg=[5.0, None],
                            manure=[2, 1]))
    ids = dict(hhid="hh", holder_id="holder", parcel_id="parcel",
               field_id="field")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = eth.plot_inputs_for_wave(
            "2011-12", raw, None, None,
            dict(ids, urea_used="urea_used", urea_kg="urea_kg",
                 manure_used="manure"))
    j = set(out.index.get_level_values("j"))
    n = int(len(out))
    assert n == 2, n
    assert j == {MARKER}, sorted(j)


def test_no_ethiopia_source_spells_the_old_marker_as_a_value():
    """The literal survives only in comments that record the rename."""
    for path in [ROOT / "_" / "ethiopia.py", ROOT / "_" / "plot_inputs.py",
                 *sorted(ROOT.glob("*/_/plot_inputs.py"))]:
        code = [ln for ln in path.read_text().splitlines()
                if "Whole Plot" in ln and not ln.lstrip().startswith("#")]
        n = len(code)
        assert n == 0, f"{path.name}: {n} non-comment line(s) spell 'Whole Plot'"


@pytest.mark.requires_s3
def test_served_plot_inputs_carry_only_the_canonical_marker():
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pi = Country("Ethiopia").plot_inputs()
    j = pi.index.get_level_values("j").astype(str)
    n_old = int((j == "Whole Plot").sum())
    n_new = int((j == MARKER).sum())
    assert n_old == 0, n_old
    assert n_new > 60000, n_new
    non_crop = pi.index.get_level_values("input").isin(
        ["Urea", "DAP", "NPS", "Manure", "Compost", "Other Organic"])
    n_bad = int((non_crop & (j != MARKER)).sum())
    assert n_bad == 0, n_bad
