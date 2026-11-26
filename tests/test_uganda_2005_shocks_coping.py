"""The Uganda baseline's GSEC16 driver must keep both previously omitted labels."""
from pathlib import Path
import runpy
import sys

import pandas as pd

from lsms_library import local_tools
from lsms_library.coping import MULTI_SEP, normalise_coping


def test_uganda_2005_coping_keeps_recorded_answers_in_their_ranks(monkeypatch):
    driver = (
        Path(__file__).resolve().parents[1]
        / "lsms_library/countries/Uganda/2005-06/_/shocks.py"
    )
    child_raw = "withdraw children from school and sent them for wage employment"
    work_raw = "work as self employed"
    child = "Withdrew children from school and sent them for wage employment"
    work = "Worked in self-employment"
    grid = pd.DataFrame({
        "HHID": list(range(1, 6)),
        "h16q2": ["drought"] * 5,
        "h16q5": [2004] * 4 + [None],
        "h16q6": [1] * 5,
        "h16q7a": [child_raw, work_raw, None, "formal borrowing", child_raw],
        "h16q7b": [work_raw, child_raw, work_raw, "informal borrowing", work_raw],
        "h16q7c": [child_raw, None, work_raw, "improve technology", child_raw],
    })
    cover = pd.DataFrame({"HHID": grid.HHID})
    inputs = {"GSEC16.dta": grid, "GSEC1.dta": cover}
    written = {}

    monkeypatch.setattr(local_tools, "get_dataframe", lambda path: inputs[Path(path).name].copy())
    monkeypatch.setattr(local_tools, "to_parquet", lambda frame, path: written.update({path: frame}))
    monkeypatch.setattr(sys, "path", sys.path.copy())
    runpy.run_path(str(driver))

    x = written["shocks.parquet"]
    assert list(x.index.names) == ["i", "t", "Shock"]
    assert list(x.index.get_level_values("i")) == [1, 2, 3, 4]
    assert "HowCopedSet" not in x
    y = normalise_coping(x, "Uganda")
    credit = "Obtained credit"
    crops = "Changed cropping practices (crop choices or technology)"
    ranks = ["HowCoped0", "HowCoped1", "HowCoped2"]
    assert y[ranks].astype("string").fillna("<missing>").values.tolist() == [
        [child, work, child],
        [work, child, "<missing>"],
        ["<missing>", work, work],
        [credit, credit, crops],
    ]
    assert y["HowCopedSet"].tolist() == [
        MULTI_SEP.join([child, work]),
        MULTI_SEP.join([child, work]),
        work,
        MULTI_SEP.join([crops, credit]),
    ]
