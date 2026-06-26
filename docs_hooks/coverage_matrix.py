"""mkdocs hook: inject the country × feature × wave coverage grid into the docs.

Renders the committed snapshot ``.coder/coverage/latest.csv`` into the
``guide/coverage.md`` page at build time, replacing the ``<!-- COVERAGE_MATRIX -->``
marker. Intentionally **stdlib-only** (csv + html): the docs CI installs only
mkdocs-material + mkdocstrings (no pandas, no lsms_library), so this must not
import the package. The expensive cube build is fully decoupled — it just
commits a fresh CSV, and the next docs build re-renders.

Presentation constants are kept in sync with ``bench/matrix.py`` (the standalone
HTML renderer); they are duplicated here only because the package is not
importable in the docs environment.
"""
from __future__ import annotations

import csv
from html import escape
from pathlib import Path

# --- kept in sync with bench/matrix.py + lsms_library.coverage_matrix ---
TIER_ORDER = ["n/a", "absent", "declared", "dropped", "broken", "builds", "sane", "blessed"]
ROLLUP_PRIORITY = ["broken", "dropped", "builds", "declared", "sane", "blessed", "absent", "n/a"]
TIER_COLOR = {
    "n/a": "#e9ecef", "absent": "#f1f3f5", "declared": "#cfe2ff", "dropped": "#f5c2c7",
    "broken": "#dc3545", "builds": "#ffe08a", "sane": "#a3e635", "blessed": "#2f9e44",
}
TIER_GLYPH = {
    "n/a": "·", "absent": "–", "declared": "?", "dropped": "✗!",
    "broken": "✗", "builds": "⚠", "sane": "✓", "blessed": "★",
}

MARKER = "<!-- COVERAGE_MATRIX -->"
PAGE = "guide/coverage.md"


def _snapshot_path(config) -> Path:
    root = Path(config["config_file_path"]).parent
    return root / ".coder" / "coverage" / "latest.csv"


def _read_rows(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _rollup(tiers: set[str]) -> str:
    for t in ROLLUP_PRIORITY:
        if t in tiers:
            return t
    return "n/a"


def _legend() -> str:
    spans = "".join(
        f'<span style="display:inline-block;padding:2px 8px;margin:2px;border-radius:3px;'
        f'background:{TIER_COLOR[t]};font-size:12px;">{TIER_GLYPH[t]} {escape(t)}</span>'
        for t in TIER_ORDER
    )
    return f'<div>{spans}</div>'


def _render(rows: list[dict]) -> str:
    countries = sorted({r["country"] for r in rows})
    features = sorted({r["feature"] for r in rows})
    # (country, feature) -> list of tiers across waves
    cell: dict[tuple[str, str], list[str]] = {}
    counts: dict[str, int] = {t: 0 for t in TIER_ORDER}
    for r in rows:
        cell.setdefault((r["country"], r["feature"]), []).append(r["tier"])
        counts[r["tier"]] = counts.get(r["tier"], 0) + 1

    # header
    head = ('<tr><th style="text-align:left;position:sticky;left:0;background:#f8f9fa;">'
            'country \\ feature</th>'
            + "".join(f"<th>{escape(f)}</th>" for f in features) + "</tr>")
    body = []
    for c in countries:
        tds = [f'<th style="text-align:left;position:sticky;left:0;background:#f8f9fa;">'
               f"{escape(c)}</th>"]
        for f in features:
            tiers = cell.get((c, f))
            if not tiers:
                tds.append("<td></td>")
                continue
            from collections import Counter
            cc = Counter(tiers)
            roll = _rollup(set(tiers))
            summary = " ".join(f"{TIER_GLYPH.get(t, t)}{cc[t]}"
                               for t in ROLLUP_PRIORITY if cc.get(t))
            tds.append(f'<td style="background:{TIER_COLOR.get(roll, "#fff")};'
                       f'text-align:center;white-space:nowrap;" title="{escape(roll)}">'
                       f"{escape(summary)}</td>")
        body.append("<tr>" + "".join(tds) + "</tr>")

    summary_line = " · ".join(f"{t}: {counts.get(t, 0)}" for t in TIER_ORDER if counts.get(t))
    n_cells = len(rows)
    table = (
        '<style>.cov-grid{border-collapse:collapse;font-size:11px;}'
        '.cov-grid td,.cov-grid th{border:1px solid #dee2e6;padding:2px 6px;}</style>'
        '<div style="overflow-x:auto;max-width:100%;">'
        '<table class="cov-grid">'
        f"<thead>{head}</thead><tbody>{''.join(body)}</tbody></table></div>"
    )
    return (
        f"{_legend()}\n\n"
        f"*{len(countries)} countries × {len(features)} features · {n_cells} cells · "
        f"tiers — {summary_line}.*\n\n"
        f"{table}\n"
    )


def _fragment(config) -> str:
    path = _snapshot_path(config)
    if not path.exists():
        return ('!!! warning "Coverage snapshot not generated yet"\n'
                "    No `.coder/coverage/latest.csv` found. Generate it with "
                "`make matrix` (full readiness) or `make matrix-coverage` "
                "(config-only), then rebuild the docs.\n")
    try:
        rows = _read_rows(path)
    except Exception as exc:  # never break --strict on a malformed snapshot
        return f'!!! danger "Could not read coverage snapshot"\n    `{escape(str(exc))}`\n'
    if not rows:
        return '!!! note "Coverage snapshot is empty."\n'
    return _render(rows)


def on_page_markdown(markdown: str, *, page, config, files=None, **kwargs):  # mkdocs hook
    if page.file.src_uri != PAGE or MARKER not in markdown:
        return markdown
    return markdown.replace(MARKER, _fragment(config))


# --- standalone smoke test: python docs_hooks/coverage_matrix.py <csv> ---
if __name__ == "__main__":
    import sys

    csv_path = Path(sys.argv[1] if len(sys.argv) > 1 else ".coder/coverage/latest.csv")
    rows = _read_rows(csv_path)
    print(_render(rows))
