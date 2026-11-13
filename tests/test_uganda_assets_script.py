"""The Uganda Makefile compiler must preserve the wave-level assets grain.

Execute the shipped script, replacing only IO.  An API-only check misses this
route because the hashless country output is rebuilt from waves (GH #788).
"""
import importlib.util
import json
import runpy
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


UGANDA = Path(__file__).resolve().parents[1] / "lsms_library/countries/Uganda/_"


def _frame(rows):
    return pd.DataFrame(
        {"Value": [value for _, _, value in rows]},
        index=pd.MultiIndex.from_tuples(
            [(household, item) for household, item, _ in rows], names=["i", "j"]
        ),
    )


def _run_script(frames, updated_ids, tmp_path, monkeypatch):
    import lsms_library.local_tools as lt
    from lsms_library import Country
    from lsms_library.paths import countries_root

    # Ledger1310-uganda-assets-ci-fixture §§2,4: use the actual registry API
    # on owned two-wave metadata; module.Waves is not compiler authority.
    root = tmp_path / "countries"
    general = root / "Uganda" / "_"
    general.mkdir(parents=True)
    (general / "data_scheme.yml").write_text(
        "Waves: " + json.dumps(list(frames)) + "\nData Scheme: {}\n"
    )
    for wave in frames:
        native = root / "Uganda" / wave / "_"
        native.mkdir(parents=True)
        (native / "assets.py").write_text("# invented declaration; never executed\n")

    written = {}
    try:
        with monkeypatch.context() as fixture:
            fixture.setenv("LSMS_COUNTRIES_ROOT", str(root))
            countries_root.cache_clear()
            country = Country("Uganda")
            assert country.file_path.resolve() == (root / "Uganda").resolve()
            assert country.waves == sorted(frames)
            for wave in frames:
                assert country[wave].wave_folder == wave
                assert "assets" in country[wave].data_scheme

            # Keep the real walker and compile bound source without pyc lookup.
            helper = UGANDA / "uganda.py"
            spec = importlib.util.spec_from_file_location("uganda", helper)
            module = importlib.util.module_from_spec(spec)
            fixture.setitem(sys.modules, "uganda", module)
            exec(compile(helper.read_bytes(), str(helper), "exec"), module.__dict__)
            assert Path(module.__file__).resolve() == helper.resolve()
            sources = {f"../{wave}/_/assets.parquet": frame for wave, frame in frames.items()}
            fixture.setattr(lt, "get_dataframe", lambda path: sources[path].copy())
            fixture.setattr(lt, "to_parquet", lambda frame, path: written.__setitem__(path, frame))
            fixture.chdir(general)
            (general / "updated_ids.json").write_text(json.dumps(updated_ids))
            runpy.run_path(str(UGANDA / "assets.py"), run_name="__main__")
    finally:
        # The context restores the environment before the cached path is cleared.
        countries_root.cache_clear()
    assert list(written) == ["../var/assets.parquet"]
    return written["../var/assets.parquet"]


@pytest.mark.parametrize(
    "first,second",
    [
        ([("a", "chair", 10.0), ("b", "radio", 20.0)],
         [("c", "bed", 30.0), ("d", "table", 40.0)]),
        ([("a", "chair", 10.0)],
         [("b", "bed", 30.0), ("b", "radio", 40.0)]),
        ([("a", "chair", 10.0), ("a", "table", 20.0)],
         [("a", "chair", 30.0), ("a", "table", 40.0)]),
        ([("a", "chair", np.nan), ("a", "table", np.nan)],
         [("b", "bed", 30.0), ("b", "radio", 40.0)]),
    ],
    ids=["disjoint-waves", "singleton-wave", "multiple-items", "all-null-wave"],
)
def test_assets_script_preserves_exact_wave_observations(first, second, tmp_path, monkeypatch):
    frames = {"2005-06": _frame(first), "2009-10": _frame(second)}
    actual = _run_script(frames, {}, tmp_path, monkeypatch)
    expected = pd.DataFrame(
        {"Value": [value for rows in (first, second) for _, _, value in rows]},
        index=pd.MultiIndex.from_tuples(
            [(wave, household, item)
             for wave, rows in zip(frames, (first, second))
             for household, item, _ in rows],
            names=["t", "i", "j"],
        ),
    )
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.index.is_unique
    assert actual.attrs["id_converted"] is True


def test_assets_script_applies_wave_specific_household_crosswalk_once(tmp_path, monkeypatch):
    frames = {
        "2005-06": _frame([("a", "chair", 10.0), ("a", "table", 20.0)]),
        "2009-10": _frame([("a", "radio", 30.0), ("b", "bed", 40.0)]),
    }
    updated_ids = {
        "2005-06": {"a": "panel-a", "panel-a": "must-not-apply-twice"},
        "2009-10": {"a": "panel-b"},
    }
    actual = _run_script(frames, updated_ids, tmp_path, monkeypatch)
    expected = pd.DataFrame(
        {"Value": [10.0, 20.0, 30.0, 40.0]},
        index=pd.MultiIndex.from_tuples(
            [("2005-06", "panel-a", "chair"), ("2005-06", "panel-a", "table"),
             ("2009-10", "panel-b", "radio"), ("2009-10", "b", "bed")],
            names=["t", "i", "j"],
        ),
    )
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.attrs["id_converted"] is True
