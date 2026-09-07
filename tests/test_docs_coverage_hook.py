"""The docs coverage renderer (``docs_hooks/coverage_matrix.py``) is stdlib-only
and cannot import the package, so its copy of the tier ladder is pinned here
to the canonical one.  It drifted once (2026-09-07): the hook knew 8 tiers,
the ladder 13, and every ``undeclared`` cell rendered blank on the site.
"""
from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SNAPSHOT = REPO / ".coder" / "coverage" / "latest.csv"


def _hook():
    spec = importlib.util.spec_from_file_location(
        "docs_hooks.coverage_matrix", REPO / "docs_hooks" / "coverage_matrix.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("docs_hooks.coverage_matrix", mod)
    spec.loader.exec_module(mod)
    return mod


ROWS = [
    # country, feature, wave, tier, coverage, n_rows, detail
    dict(country="A", feature="household_roster", wave="2005-06", tier="sane", coverage="declared", n_rows="10", detail="all checks pass"),
    dict(country="A", feature="household_roster", wave="2010Q3", tier="builds", coverage="declared", n_rows="9", detail="fail: x"),
    dict(country="A", feature="household_roster", wave="2011Q1", tier="broken", coverage="declared", n_rows="", detail="boom"),
    dict(country="A", feature="food_acquired", wave="2005-06", tier="absent", coverage="absent", n_rows="", detail=""),
    dict(country="A", feature="shocks", wave="", tier="undeclared", coverage="absent", n_rows="", detail="never declared"),
    dict(country="A", feature="panel_ids", wave="", tier="n/a", coverage="declared", n_rows="", detail=""),
    dict(country="B", feature="household_roster", wave="1994", tier="blessed", coverage="declared", n_rows="5", detail=""),
    dict(country="B", feature="food_acquired", wave="1994", tier="not-asked", coverage="absent", n_rows="", detail="adjudicated"),
    dict(country="C", feature="household_roster", wave="1996", tier="blocked", coverage="absent", n_rows="", detail="no channel"),
]


def test_ladder_matches_the_package():
    from lsms_library.coverage_matrix import ROLLUP_PRIORITY, TIER_ORDER
    hook = _hook()
    assert hook.TIER_ORDER == TIER_ORDER
    assert hook.ROLLUP_PRIORITY == ROLLUP_PRIORITY


def test_every_tier_has_presentation_in_both_themes():
    hook = _hook()
    for t in hook.TIER_ORDER:
        assert t in hook.TIER_COLOR["light"], t
        assert t in hook.TIER_COLOR["dark"], t
        assert t in hook.TIER_GLYPH, t
        assert t in hook.TIER_HELP, t
        assert t in hook.FAMILY, t
    assert set(hook.LEGEND_ORDER) == set(hook.TIER_ORDER)


def test_matrix_fragment_draws_one_segment_per_wave_and_no_cell_text():
    hook = _hook()
    html = hook.render_fragment(ROWS)
    # A's roster: three waves -> three segments, chronological
    seg = html.index('data-c="A" data-f="household_roster"')
    cell = html[seg:html.index("</td>", seg)]
    assert cell.count("<i ") == 3
    assert cell.index('data-w="2005-06"') < cell.index('data-w="2010Q3"') < cell.index('data-w="2011Q1"')
    assert 't-sane' in cell and 't-builds' in cell and 't-broken' in cell
    # colour, not text: the strip carries no visible characters (the glyph
    # is a data- attribute the optional overlay reads, never text content)
    import re
    visible = re.sub(r"<[^>]+>", "", cell.split(">", 1)[1])
    assert visible.strip() == ""
    assert 'data-g="✓"' in cell
    # every tier present in the data is in the legend with its count
    assert '<b>undeclared</b>' in html and '<b>blocked</b>' in html
    # table view omits the un-graded background but keeps graded cells
    tbl = html[html.index("Table view"):]
    assert "undeclared" not in tbl.split("</summary>")[1]
    assert "2011Q1" in tbl and "boom" in tbl


def test_timeline_bars_encode_graded_count_and_share():
    hook = _hook()
    tl = hook.timeline_rows(ROWS)
    a = {d["wave"]: d for d in tl["A"]}
    assert a["2005-06"]["graded"] == 1 and a["2005-06"]["sane"] == 1
    assert a["2005-06"]["y0"] == 2005 and a["2005-06"]["y1"] == 2007
    assert a["2010Q3"]["kind"] == "graded" and a["2011Q1"]["kind"] == "graded"
    assert tl["C"][0]["kind"] == "blocked"
    svg = hook.render_timeline(ROWS)
    assert svg.count("<rect") == 5           # A x3, B x1 (its two features share one wave), C x1
    assert 'class="w blocked"' in svg
    assert 'class="w sh3"' in svg            # B 1994: 1/1 sane -> top step
    assert "<title>" in svg                  # hover text on every bar


@pytest.mark.parametrize("label,span", [
    ("1994", (1994, 1995)), ("2005-06", (2005, 2007)), ("2008-15", (2008, 2016)),
    ("1999-00", (1999, 2001)), ("2010Q3", (2010, 2011)), ("1994a", (1994, 1995)),
    ("1995-97", (1995, 1998)), ("", None), ("junk", None),
])
def test_wave_span(label, span):
    assert _hook().wave_span(label) == span


@pytest.mark.skipif(not SNAPSHOT.exists(), reason="no committed snapshot")
def test_committed_snapshot_renders_completely():
    hook = _hook()
    with SNAPSHOT.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    unknown = {r["tier"] for r in rows} - set(hook.TIER_ORDER)
    assert not unknown, f"snapshot tiers missing from the ladder: {unknown}"
    unparsed = {r["wave"] for r in rows if r["wave"] and hook.wave_span(r["wave"]) is None}
    assert not unparsed, f"wave labels the timeline cannot place: {unparsed}"
    html = hook.render_fragment(rows)
    n_cells = len({(r["country"], r["feature"]) for r in rows})
    grid = html[:html.index("Table view")]
    assert grid.count(' data-c="') == n_cells          # populated cells
    n_countries = len({r["country"] for r in rows}); n_features = len({r["feature"] for r in rows})
    assert grid.count('class="cov-cell') == n_countries * n_features   # full cross product
    assert hook.render_timeline(rows).count("<rect") == len({(r["country"], r["wave"]) for r in rows if r["wave"]})


def test_on_page_markdown_replaces_both_markers(tmp_path):
    """The mkdocs entry point: both markers on the coverage page are replaced
    from the snapshot; other pages are untouched."""
    hook = _hook()
    cov = tmp_path / ".coder" / "coverage"; cov.mkdir(parents=True)
    with (cov / "latest.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["country", "feature", "wave", "tier", "coverage", "n_rows", "detail"])
        w.writeheader(); w.writerows(ROWS)
    (tmp_path / "mkdocs.yml").write_text("site_name: x\n")
    config = {"config_file_path": str(tmp_path / "mkdocs.yml")}

    class _File:  # what mkdocs hands the hook
        src_uri = hook.PAGE
    class _Page:
        file = _File()
    md = f"# Coverage\n\n{hook.TIMELINE_MARKER}\n\nmore\n\n{hook.MARKER}\n"
    out = hook.on_page_markdown(md, page=_Page(), config=config)
    assert hook.MARKER not in out and hook.TIMELINE_MARKER not in out
    assert '<svg' in out and 'class="cov-grid"' in out
    other = type("P", (), {"file": type("F", (), {"src_uri": "index.md"})()})()
    assert hook.on_page_markdown(md, page=other, config=config) == md
