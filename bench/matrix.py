#!/usr/bin/env python
"""Country × Feature × Wave readiness matrix — CLI + HTML renderer.

The data model lives in :mod:`lsms_library.coverage` (importable / testable;
reused by ``ll.coverage()``). This script is the thin command-line wrapper that
builds the matrix, writes the git-tracked snapshot, and renders a self-contained
HTML readout (pure string templating — no jinja2 / Styler, no new deps).

See `.coder/charter-coverage-matrix.md` for the tier ladder + rationale.

CLI::

    python bench/matrix.py                       # full cube + HTML + snapshot
    python bench/matrix.py --countries Uganda    # subset
    python bench/matrix.py --features food_prices household_roster
    python bench/matrix.py --no-readiness        # coverage layer only (auth-free)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from html import escape as _esc
from pathlib import Path

import pandas as pd

from lsms_library.coverage_matrix import (
    ROLLUP_PRIORITY,
    TIER_ORDER,
    build_matrix,
    default_snapshot_path,
    save_snapshot,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "bench" / "results"

# Presentation only (the model is colour-agnostic).
TIER_COLOR = {
    "n/a":      "#e9ecef",
    # `absent` is the LIVE QUEUE (an un-adjudicated gap), so it must not read as
    # quietly settled.  The adjudicated tiers below are the settled ones.
    "absent":   "#f1f3f5",
    # Adjudicated-and-closed: dimmer than `absent`, because there is no work here.
    "not-asked":             "#dee2e6",
    # Adjudicated as an ACQUISITION problem (asked, but not in the shipped
    # extract) -- a distinct colour, because it routes to a different queue
    # entirely and should never be mistaken for either config work or a close.
    "asked-not-distributed": "#c5b3e6",
    "declared": "#cfe2ff",
    "dropped":  "#f5c2c7",
    "broken":   "#dc3545",
    "builds":   "#ffe08a",
    "sane":     "#a3e635",
    "blessed":  "#2f9e44",
}
TIER_GLYPH = {
    "n/a": "·", "absent": "–", "declared": "?", "dropped": "✗!",
    "broken": "✗", "builds": "⚠", "sane": "✓", "blessed": "★",
    "not-asked": "∅", "asked-not-distributed": "⤓",
}


# ---------------------------------------------------------------------------
# Roll-up grid
# ---------------------------------------------------------------------------
def _rollup_tier(tiers) -> str:
    present = set(map(str, tiers))
    for t in ROLLUP_PRIORITY:
        if t in present:
            return t
    return "n/a"


def _grid(df: pd.DataFrame):
    """Return (text_grid, tier_grid) DataFrames indexed country × feature."""
    text, tiers = {}, {}
    for (c, f), g in df.groupby(["country", "feature"], observed=True):
        counts = g["tier"].astype(str).value_counts()
        roll = _rollup_tier(g["tier"].tolist())
        summary = " ".join(
            f"{TIER_GLYPH.get(t, t)}{int(counts[t])}"
            for t in ROLLUP_PRIORITY if t in counts and int(counts[t]) > 0
        )
        text.setdefault(c, {})[f] = summary
        tiers.setdefault(c, {})[f] = roll
    text_df = pd.DataFrame(text).T.sort_index()
    text_df = text_df.reindex(columns=sorted(text_df.columns)).fillna("")
    tier_df = pd.DataFrame(tiers).T.reindex(index=text_df.index, columns=text_df.columns)
    return text_df, tier_df


# ---------------------------------------------------------------------------
# HTML (hand-rolled; no jinja2 / Styler dependency)
# ---------------------------------------------------------------------------
def _legend_html() -> str:
    items = "".join(
        f'<span style="display:inline-block;padding:2px 8px;margin:2px;'
        f'border-radius:3px;background:{TIER_COLOR[t]};">{TIER_GLYPH[t]} {_esc(t)}</span>'
        for t in TIER_ORDER
    )
    return f'<div class="legend">{items}</div>'


def _grid_table_html(text_df: pd.DataFrame, tier_df: pd.DataFrame) -> str:
    cols = list(text_df.columns)
    head = ('<tr><th class="rowhead">country \\ feature</th>'
            + "".join(f"<th>{_esc(str(c))}</th>" for c in cols) + "</tr>")
    body = []
    for country in text_df.index:
        cells = [f'<th class="rowhead">{_esc(str(country))}</th>']
        for c in cols:
            txt = text_df.at[country, c]
            tier = tier_df.at[country, c]
            if not txt or pd.isna(tier):
                cells.append("<td></td>")
                continue
            color = TIER_COLOR.get(str(tier), "#fff")
            cells.append(f'<td style="background:{color}" title="{_esc(str(tier))}">'
                         f"{_esc(str(txt))}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return (f'<table class="grid"><thead>{head}</thead>'
            f"<tbody>{''.join(body)}</tbody></table>")


def _detail_table_html(df: pd.DataFrame):
    """Per-cell detail — declared cells only (absent / n/a omitted)."""
    keep = df[~df["tier"].astype(str).isin(["absent", "n/a"])].copy()
    keep["tier"] = keep["tier"].astype(str)
    # Sort worst-first by the ladder, not alphabetically, so defects head the table.
    _rank = {t: i for i, t in enumerate(ROLLUP_PRIORITY)}
    keep["_r"] = keep["tier"].map(lambda t: _rank.get(t, len(ROLLUP_PRIORITY)))
    keep = keep.sort_values(["_r", "country", "feature", "wave"]).drop(columns="_r")
    headers = ["country", "feature", "wave", "tier", "n_rows", "detail"]
    head = "<tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr>"
    body = []
    for _, r in keep.iterrows():
        tier = str(r["tier"])
        color = TIER_COLOR.get(tier, "#fff")
        nrows = "" if pd.isna(r["n_rows"]) or r["n_rows"] == "" else int(float(r["n_rows"]))
        body.append(
            "<tr>"
            f'<td>{_esc(str(r["country"]))}</td>'
            f'<td>{_esc(str(r["feature"]))}</td>'
            f'<td>{_esc(str(r["wave"]))}</td>'
            f'<td style="background:{color}">{_esc(tier)}</td>'
            f'<td style="text-align:right">{nrows}</td>'
            f'<td>{_esc(str(r["detail"]))}</td>'
            "</tr>"
        )
    return (f'<table class="detail"><thead>{head}</thead>'
            f"<tbody>{''.join(body)}</tbody></table>"), len(keep)


def render_html(df: pd.DataFrame, path: Path, *, readiness=True) -> Path:
    """Render a self-contained HTML readout — pure string templating, no deps."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text_df, tier_df = _grid(df)
    grid_html = _grid_table_html(text_df, tier_df)
    detail_html, n_detail = _detail_table_html(df)

    n_cells = len(df)
    counts = df["tier"].astype(str).value_counts().reindex(TIER_ORDER, fill_value=0)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    summary_row = " · ".join(f"{t}: {int(counts[t])}" for t in TIER_ORDER)
    mode = "coverage + readiness" if readiness else "coverage only (no builds)"

    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>LSMS coverage matrix</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 1.5rem; color:#212529; }}
 h1 {{ font-size: 1.3rem; }} h2 {{ font-size: 1.05rem; margin-top: 1.6rem; }}
 .meta {{ color:#666; font-size: 12px; }}
 .legend span {{ font-size: 12px; }}
 table {{ border-collapse: collapse; font-size: 11px; }}
 td, th {{ border: 1px solid #dee2e6; padding: 2px 6px; }}
 table.grid td {{ text-align: center; white-space: nowrap; }}
 table.grid th {{ position: sticky; top: 0; background: #fff; }}
 th.rowhead {{ text-align: left; background: #f8f9fa; position: sticky; left: 0; }}
 .grid-wrap {{ overflow-x: auto; max-width: 100%; }}
</style></head><body>
<h1>LSMS Library — country × feature × wave readiness</h1>
<p class="meta">Generated {stamp} · mode: {mode} · {n_cells} cells.
 Tier counts — {summary_row}.<br>
 Source: <code>bench/matrix.py</code> · snapshot <code>.coder/coverage/latest.csv</code>.
 Roll-up cells show worst-first glyph counts over a country's waves; see the
 detail table for per-wave tiers.</p>
{_legend_html()}
<h2>Roll-up grid</h2>
<div class="grid-wrap">{grid_html}</div>
<h2>Per-cell detail ({n_detail} declared cells; absent/n-a omitted)</h2>
<div class="grid-wrap">{detail_html}</div>
</body></html>
"""
    path.write_text(html, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--countries", nargs="*", default=None,
                    help="Restrict to these countries (default: all).")
    ap.add_argument("--features", nargs="*", default=None,
                    help="Restrict to these features (default: all).")
    ap.add_argument("--no-readiness", action="store_true",
                    help="Coverage layer only — no builds, no data access.")
    ap.add_argument("--snapshot", type=Path, default=None,
                    help="Status CSV path (default: .coder/coverage/latest.csv).")
    ap.add_argument("--html", type=Path, default=None,
                    help="HTML output path (default: bench/results/<date>/matrix.html).")
    ap.add_argument("--no-html", action="store_true",
                    help="Skip the HTML render (for sharded builds; see matrix_aggregate.py).")
    args = ap.parse_args(argv)

    readiness = not args.no_readiness
    print(f"[matrix] building ({'coverage only' if not readiness else 'full'}) ...",
          file=sys.stderr)
    df = build_matrix(args.countries, args.features, readiness=readiness,
                      log=lambda m: print(m, file=sys.stderr))

    snap_path = args.snapshot or default_snapshot_path()
    snap = save_snapshot(df, snap_path)
    print(f"[matrix] snapshot -> {snap} ({len(df)} cells)", file=sys.stderr)

    if args.no_html:
        print("[matrix] html     -> skipped (--no-html)", file=sys.stderr)
    else:
        html_path = args.html or (RESULTS_DIR
                                  / datetime.now(timezone.utc).strftime("%Y-%m-%d")
                                  / "matrix.html")
        render_html(df, html_path, readiness=readiness)
        print(f"[matrix] html     -> {html_path}", file=sys.stderr)

    counts = df["tier"].astype(str).value_counts().reindex(TIER_ORDER, fill_value=0)
    print("[matrix] tiers: " + ", ".join(f"{t}={int(counts[t])}" for t in TIER_ORDER),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
