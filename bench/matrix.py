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
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
# Presentation (colours, glyphs, families) and the grid/timeline renderers are
# shared with the docs hook so the site and this readout cannot drift again
# (they had: the hook knew 8 tiers, the ladder 13 -- GH #797, 2026-09-07).
from docs_hooks.coverage_matrix import (  # noqa: E402
    TIER_COLOR, TIER_GLYPH, render_fragment, render_timeline,
)

# ---------------------------------------------------------------------------
# Roll-up grid
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# HTML (hand-rolled; no jinja2 / Styler dependency)
# ---------------------------------------------------------------------------
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
    rows = df.astype(object).where(df.notna(), "").astype(str).to_dict("records")
    for r in rows:                      # the renderer reads the CSV's column names
        r.setdefault("detail", "")
    fragment = render_fragment(rows)
    timeline = render_timeline(rows)
    detail_html, n_detail = _detail_table_html(df)

    n_cells = len(df)
    counts = df["tier"].astype(str).value_counts().reindex(TIER_ORDER, fill_value=0)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    summary_row = " · ".join(f"{t}: {int(counts[t])}" for t in TIER_ORDER if int(counts[t]))
    mode = "coverage + readiness" if readiness else "coverage only (no builds)"

    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>LSMS coverage matrix</title>
<style>
 body {{ font-family: system-ui, -apple-system, "Segoe UI", sans-serif; margin: 1.5rem; }}
 h1 {{ font-size: 1.3rem; }} h2 {{ font-size: 1.05rem; margin-top: 1.6rem; }}
 .meta {{ opacity:.75; font-size: 12px; }}
 table.detail {{ border-collapse: collapse; font-size: 11px; }}
 table.detail td, table.detail th {{ border: 1px solid #dee2e6; padding: 2px 6px; }}
 .grid-wrap {{ overflow-x: auto; max-width: 100%; }}
</style></head><body>
<h1>LSMS Library — country × feature × wave readiness</h1>
<p class="meta">Generated {stamp} · mode: {mode} · {n_cells} cells.
 Tier counts — {summary_row}.<br>
 Source: <code>bench/matrix.py</code> · snapshot <code>.coder/coverage/latest.csv</code>.
 The same renderer draws the docs site (<code>docs_hooks/coverage_matrix.py</code>).</p>
<h2>Survey timeline</h2>
{timeline}
<h2>Feature matrix</h2>
{fragment}
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

    # A cheap-layer run is a VIEW, not a snapshot update.  `--no-readiness`
    # grades every cell `declared` (no builds), and `save_snapshot` upserts by
    # (country, feature, wave) -- so writing it to the committed snapshot
    # silently DOWNGRADES every readiness tier it touches.  Measured on this
    # corpus: 1,217 `sane` + 138 `builds` + 39 `dropped` + 8 `broken` all
    # became `declared` in one run.  This is pre-existing (the overwrite is the
    # upsert, not the #724 deletion -- verified with deletion disabled), and it
    # is what `make matrix-coverage` does by default.
    #
    # So a no-readiness run refuses the DEFAULT snapshot and requires an
    # explicit `--snapshot` path.  It still prints and still renders HTML.
    if not readiness and not args.snapshot:
        print("[matrix] snapshot -> SKIPPED: --no-readiness grades every cell "
              "`declared`, which would overwrite the readiness tiers in the "
              "committed snapshot.  Pass an explicit --snapshot PATH to write "
              "a cheap-layer view somewhere else.", file=sys.stderr)
        snap_path = None
    else:
        snap_path = args.snapshot or default_snapshot_path()
    if snap_path is not None:
        # A run with no --features filter is AUTHORITATIVE about which cells
        # each country it graded has, so stale rows for those countries are
        # dropped rather than left to contradict the fresh ones (GH #724).
        # With a feature filter the run knows nothing about the features it
        # skipped, so it stays purely additive.
        authoritative = None if args.features else sorted(
            {str(c) for c in df["country"].unique()})
        snap = save_snapshot(df, snap_path,
                             authoritative_countries=authoritative)
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
