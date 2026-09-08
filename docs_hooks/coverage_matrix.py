"""mkdocs hook + shared renderer: the country x feature x wave coverage grid.

Renders the committed snapshot ``.coder/coverage/latest.csv`` into the
``guide/coverage.md`` page at build time, replacing the ``<!-- COVERAGE_MATRIX -->``
marker.  Intentionally **stdlib-only** (csv + html + json): the docs CI installs
only mkdocs-material + mkdocstrings (no pandas, no lsms_library), so this must
not import the package.  The expensive cube build is fully decoupled -- it just
commits a fresh CSV, and the next docs build re-renders.

This module is ALSO the single source of the tier ladder's presentation
(colours, glyphs, families) and of the grid renderer for ``bench/matrix.py``,
which imports it from the repo root.  Before 2026-09-07 the two carried
duplicated constants that had drifted: the hook knew 8 tiers while the ladder
had 13, so every ``undeclared`` cell (478 of them) rendered blank on the site.
``tests/test_docs_coverage_hook.py`` pins ``TIER_ORDER`` / ``ROLLUP_PRIORITY``
here to ``lsms_library.coverage_matrix``.

Cell encoding (2026-09-07).  A (country, feature) cell is a STRIP of one
segment per wave, in chronological order, each coloured by that wave's tier --
colour carries the state, text does not (the tier names sit in the legend, in
each cell's hover text, and in the table view).  Colours are one hue per
semantic FAMILY with lightness steps inside a family, validated in both
themes with the dataviz palette validator (five family hues clear the
lightness band, chroma floor and 3:1 dark-surface contrast; the classic
status collision amber/green and red/green sits in the CVD warn band, which is
why the glyph overlay, the hover text and the table view exist):

    defect        red      broken (deep) / dropped (light)
    warning       amber    builds
    work-to-do    magenta  unconfigured (deep) / undeclared (light)
    acquisition   blue     blocked (deep) / asked-not-distributed (light)
    config-only   grey-blue declared (never in a readiness snapshot)
    safe          green    blessed (deep) / sane (light)
    gap           neutral  absent (live queue) / not-asked (settled) / n/a
"""
from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import date
from html import escape
from pathlib import Path

# --- The ladder.  MUST equal lsms_library.coverage_matrix.{TIER_ORDER,ROLLUP_PRIORITY};
# pinned by tests/test_docs_coverage_hook.py (the package is not importable here).
TIER_ORDER = [
    "n/a", "not-asked", "asked-not-distributed", "blocked", "unconfigured",
    "undeclared", "absent", "declared", "dropped", "broken", "builds", "sane",
    "blessed",
]
ROLLUP_PRIORITY = [
    "broken", "dropped", "builds", "unconfigured", "undeclared", "absent",
    "declared", "sane", "blessed", "blocked", "asked-not-distributed",
    "not-asked", "n/a",
]

# Presentation.  Family -> the hue; tier -> the step.  Both themes are
# SELECTED steps of the same hues, not a flip.
FAMILY = {
    "broken": "defect", "dropped": "defect",
    "builds": "warning",
    "unconfigured": "todo", "undeclared": "todo",
    "blocked": "acquire", "asked-not-distributed": "acquire",
    "declared": "config",
    "sane": "safe", "blessed": "safe",
    "absent": "gap", "not-asked": "gap", "n/a": "gap",
}
TIER_COLOR = {
    "light": {
        "broken": "#c9342f", "dropped": "#e88b9b",
        "builds": "#d99a00",
        "unconfigured": "#7a1f66", "undeclared": "#cc63ad",
        "blocked": "#1a4a8e", "asked-not-distributed": "#5b93dd",
        "declared": "#9fb3c8",
        "blessed": "#137a2e", "sane": "#4fae52",
        "absent": "#d9d5cd", "not-asked": "#b8c4c2", "n/a": "#ecebe6",
    },
    "dark": {
        "broken": "#d9524c", "dropped": "#e89aa8",
        "builds": "#b98c1f",
        "unconfigured": "#8a3a84", "undeclared": "#d068b8",
        "blocked": "#2c5fae", "asked-not-distributed": "#4f8fe0",
        "declared": "#3d4a5a",
        "blessed": "#237a3e", "sane": "#57b45a",
        "absent": "#3a3936", "not-asked": "#2f3a3a", "n/a": "#242422",
    },
}
TIER_GLYPH = {
    "n/a": "·", "absent": "–", "declared": "?", "dropped": "✗!",
    "broken": "✗", "builds": "⚠", "sane": "✓", "blessed": "★",
    "not-asked": "∅", "asked-not-distributed": "⤓", "unconfigured": "⌀",
    "blocked": "⛔", "undeclared": "⌀",
}
TIER_HELP = {
    "blessed": "a human read the numbers and believes them",
    "sane": "builds and passes every automated check",
    "builds": "builds, but a sanity check fails",
    "broken": "the build raised or returned nothing",
    "dropped": "built, but this wave's slice is empty",
    "declared": "config exists; not built (coverage layer only)",
    "absent": "not declared for this wave; un-adjudicated (the live queue)",
    "undeclared": "this country never declared the feature (work not started)",
    "unconfigured": "microdata present, no config at all",
    "blocked": "wanted, but the data channel is broken",
    "asked-not-distributed": "the instrument asked; the shipped extract lacks it",
    "not-asked": "adjudicated: the survey never asked",
    "n/a": "country-level feature; no per-wave grade",
}
# Legend order: safe first (the answer to "what can I use"), then what needs work.
LEGEND_ORDER = ["blessed", "sane", "builds", "broken", "dropped", "undeclared",
                "unconfigured", "asked-not-distributed", "blocked", "declared",
                "absent", "not-asked", "n/a"]

# Column groups: the library's data domains, in the order an analyst meets them.
FEATURE_GROUPS = [
    ("Sample & roster", re.compile(r"^(sample|cluster_features|community_cluster_xwalk|household_roster|household_characteristics|hhsize|panel_ids|updated_ids|interview_date|individual_|education|migration|anthropometry|health)")),
    ("Food", re.compile(r"^(food_|nutrition|fct|consumption|community_prices|nonfood|months_food)")),
    ("Agriculture", re.compile(r"^(plot_|crop_|livestock|harvest|inputs|land)")),
    ("Assets, housing & shocks", re.compile(r"^(assets|housing|shocks|income|enterprise|transfers|credit|durable)")),
    ("People & work", re.compile(r"^(employment|earnings|people_|labor|life_satisfaction|subjective_)")),
]

MARKER = "<!-- COVERAGE_MATRIX -->"
PAGE = "guide/coverage.md"


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
def _snapshot_path(config) -> Path:
    root = Path(config["config_file_path"]).parent
    return root / ".coder" / "coverage" / "latest.csv"


def _read_rows(path) -> list[dict]:
    path = Path(path)
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _rollup(tiers) -> str:
    present = set(tiers)
    for t in ROLLUP_PRIORITY:
        if t in present:
            return t
    return "n/a"


def wave_key(wave: str):
    """Chronological sort key for wave labels: leading year, then the label
    (``2010Q3`` < ``2011Q1``; ``2008-15`` sorts on 2008; '' last)."""
    m = re.match(r"(\d{4})", wave or "")
    return (int(m.group(1)) if m else 9999, wave or "")


def feature_group(feature: str) -> str:
    for name, rx in FEATURE_GROUPS:
        if rx.match(feature):
            return name
    return "Other"


def ordered_features(features) -> list[tuple[str, list[str]]]:
    """[(group, [features...])] in FEATURE_GROUPS order, alphabetical inside."""
    groups: dict[str, list[str]] = {}
    for f in features:
        groups.setdefault(feature_group(f), []).append(f)
    names = [g for g, _ in FEATURE_GROUPS] + ["Other"]
    return [(g, sorted(groups[g])) for g in names if g in groups]


# ---------------------------------------------------------------------------
# CSS (tokens for both themes; the docs site stamps data-md-color-scheme)
# ---------------------------------------------------------------------------
def _token_css() -> str:
    """The theme tokens every fragment reads (tier colours + chrome), for the
    three viewer states: un-stamped (media query), data-theme (the artifact
    toggle) and data-md-color-scheme (mkdocs-material).  Emitted by BOTH
    fragments so either marker renders alone."""
    light = "".join(f"--t-{k}:{v};" for k, v in TIER_COLOR["light"].items())
    dark = "".join(f"--t-{k}:{v};" for k, v in TIER_COLOR["dark"].items())
    return f"""<style>
.cov{{--cov-bg:#faf9f6;--cov-ink:#1c1b18;--cov-ink2:#5b5952;--cov-line:#e4e1d9;--cov-head:#f2f0ea;{light}
  font-variant-numeric:tabular-nums;color:var(--cov-ink);}}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]) .cov{{--cov-bg:#151513;--cov-ink:#ece9e1;--cov-ink2:#a9a69c;--cov-line:#2b2a27;--cov-head:#1d1c1a;{dark}}}}}
:root[data-theme="dark"] .cov,[data-md-color-scheme="slate"] .cov{{--cov-bg:#151513;--cov-ink:#ece9e1;--cov-ink2:#a9a69c;--cov-line:#2b2a27;--cov-head:#1d1c1a;{dark}}}
</style>"""


def _css() -> str:
    return _token_css() + f"""<style>
.cov-legend{{display:flex;flex-wrap:wrap;gap:6px 14px;margin:0 0 10px;font-size:12px;color:var(--cov-ink2);}}
.cov-legend span{{display:inline-flex;align-items:center;gap:6px;}}
.cov-legend i{{display:inline-block;width:14px;height:14px;border-radius:2px;}}
.cov-legend b{{font-weight:600;color:var(--cov-ink);}}
.cov-wrap{{overflow-x:auto;max-width:100%;background:var(--cov-bg);}}
table.cov-grid{{border-collapse:separate;border-spacing:0;font-size:11px;background:var(--cov-bg);color:var(--cov-ink);}}
.cov-grid th,.cov-grid td{{padding:0;border-bottom:1px solid var(--cov-line);}}
.cov-grid thead th{{position:sticky;top:0;z-index:2;background:var(--cov-head);font-weight:500;color:var(--cov-ink2);}}
.cov-grid thead tr.grp th{{text-align:left;padding:6px 6px 2px;font-size:10px;letter-spacing:.08em;text-transform:uppercase;border-left:1px solid var(--cov-line);}}
.cov-grid thead tr.feat th{{height:118px;vertical-align:bottom;padding:0 0 6px;}}
.cov-grid thead tr.feat th span{{display:block;writing-mode:vertical-rl;transform:rotate(180deg);white-space:nowrap;margin:0 auto;padding-left:4px;color:var(--cov-ink);}}
.cov-grid th.rowhead{{position:sticky;left:0;z-index:1;background:var(--cov-head);text-align:left;padding:0 10px 0 6px;white-space:nowrap;font-weight:500;}}
.cov-grid thead th.rowhead{{z-index:3;}}
.cov-grid td.gs{{border-left:1px solid var(--cov-line);}}
.cov-cell{{padding:3px 3px;}}
.cov-strip{{display:flex;gap:2px;height:16px;min-width:26px;}}
.cov-strip i{{flex:1 1 0;min-width:3px;border-radius:2px;display:block;position:relative;}}
.cov-cell:hover .cov-strip i{{filter:brightness(1.12);}}
.cov.glyphs .cov-strip i::after{{content:attr(data-g);position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:9px;line-height:1;color:#fff;mix-blend-mode:difference;}}
.cov-summary{{display:flex;height:10px;gap:2px;margin:10px 0 4px;border-radius:2px;overflow:hidden;}}
.cov-summary i{{display:block;}}
.cov-table{{font-size:12px;border-collapse:collapse;margin-top:8px;}}
.cov-table th,.cov-table td{{border-bottom:1px solid var(--cov-line);padding:3px 8px;text-align:left;vertical-align:top;}}
.cov-table td.t i{{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:6px;vertical-align:-1px;}}
{"".join(f".t-{k}{{background:var(--t-{k});}}" for k in TIER_ORDER)}
</style>"""


# ---------------------------------------------------------------------------
# Fragments
# ---------------------------------------------------------------------------
def _legend(counts: Counter) -> str:
    spans = []
    for t in LEGEND_ORDER:
        n = counts.get(t, 0)
        if not n:
            continue
        spans.append(f'<span title="{escape(TIER_HELP[t])}"><i class="t-{t}"></i>'
                     f'<b>{escape(t)}</b> {TIER_GLYPH[t]} {n:,}</span>')
    return f'<div class="cov-legend">{"".join(spans)}</div>'


def _summary_strip(counts: Counter) -> str:
    total = sum(counts.values()) or 1
    segs = "".join(
        f'<i class="t-{t}" style="flex:{counts[t]}" title="{escape(t)}: {counts[t]:,} cells ({100*counts[t]/total:.0f}%)"></i>'
        for t in LEGEND_ORDER if counts.get(t))
    return f'<div class="cov-summary">{segs}</div>'


def _grid(rows: list[dict]) -> str:
    countries = sorted({r["country"] for r in rows})
    features = {r["feature"] for r in rows}
    groups = ordered_features(features)
    cell: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        cell.setdefault((r["country"], r["feature"]), []).append(r)

    grp_row = ['<th class="rowhead"></th>']
    feat_row = ['<th class="rowhead">country</th>']
    for g, fs in groups:
        grp_row.append(f'<th colspan="{len(fs)}">{escape(g)}</th>')
        for i, f in enumerate(fs):
            feat_row.append(f'<th{" class=gs" if i == 0 else ""}><span>{escape(f)}</span></th>')
    head = f'<tr class="grp">{"".join(grp_row)}</tr><tr class="feat">{"".join(feat_row)}</tr>'

    body = []
    for c in countries:
        tds = [f'<th class="rowhead">{escape(c)}</th>']
        for g, fs in groups:
            for i, f in enumerate(fs):
                cls = "cov-cell" + (" gs" if i == 0 else "")
                waves = cell.get((c, f))
                if not waves:
                    tds.append(f'<td class="{cls}"></td>')
                    continue
                waves = sorted(waves, key=lambda r: wave_key(r["wave"]))
                roll = _rollup(r["tier"] for r in waves)
                tip = "; ".join(f"{r['wave'] or 'country-level'}: {r['tier']}" for r in waves)
                segs = "".join(
                    f'<i class="t-{r["tier"]}" data-g="{TIER_GLYPH.get(r["tier"], "")}" data-w="{escape(r["wave"])}"></i>'
                    for r in waves)
                tds.append(f'<td class="{cls}" data-c="{escape(c)}" data-f="{escape(f)}" data-roll="{roll}" '
                           f'title="{escape(c)} / {escape(f)} -- {escape(tip)}">'
                           f'<span class="cov-strip">{segs}</span></td>')
        body.append("<tr>" + "".join(tds) + "</tr>")
    return (f'<div class="cov-wrap"><table class="cov-grid"><thead>{head}</thead>'
            f"<tbody>{''.join(body)}</tbody></table></div>")


def _table_view(rows: list[dict]) -> str:
    """Accessibility / search surface: every graded cell as text (undeclared,
    absent and n/a omitted -- they are the un-graded background)."""
    keep = [r for r in rows if r["tier"] not in ("absent", "n/a", "undeclared")]
    keep.sort(key=lambda r: (r["country"], r["feature"], wave_key(r["wave"])))
    trs = "".join(
        f'<tr><td>{escape(r["country"])}</td><td>{escape(r["feature"])}</td><td>{escape(r["wave"])}</td>'
        f'<td class="t"><i class="t-{r["tier"]}"></i>{escape(r["tier"])}</td>'
        f'<td style="text-align:right">{escape(r.get("n_rows", "") or "")}</td><td>{escape(r.get("detail", "") or "")}</td></tr>'
        for r in keep)
    return (f'<details><summary>Table view ({len(keep):,} graded cells; the same data as text)</summary>'
            f'<table class="cov-table"><thead><tr><th>country</th><th>feature</th><th>wave</th>'
            f'<th>tier</th><th>rows</th><th>detail</th></tr></thead><tbody>{trs}</tbody></table></details>')


def render_fragment(rows: list[dict], *, table_view: bool = True) -> str:
    """The complete grid fragment (CSS + legend + summary + grid [+ table])."""
    counts = Counter(r["tier"] for r in rows)
    countries = {r["country"] for r in rows}
    features = {r["feature"] for r in rows}
    caption = (f"{len(countries)} countries x {len(features)} features x waves = {len(rows):,} cells. "
               f"Each cell is one segment per wave, oldest to newest; colour is the tier. "
               f"Hover a cell for the wave-by-wave reading.")
    parts = [_css(), '<div class="cov">', _legend(counts), _summary_strip(counts),
             f'<p style="font-size:12px;color:var(--cov-ink2);margin:0 0 8px">{escape(caption)}</p>',
             _grid(rows)]
    if table_view:
        parts.append(_table_view(rows))
    parts.append("</div>")
    return "\n".join(parts)


def cell_json(rows: list[dict]) -> str:
    """Per-cell wave detail for an interactive wrapper (the artifact)."""
    cells: dict[str, list] = {}
    for r in rows:
        cells.setdefault(f"{r['country']}␟{r['feature']}", []).append(
            [r["wave"], r["tier"], r.get("n_rows", ""), r.get("detail", "")])
    for v in cells.values():
        v.sort(key=lambda x: wave_key(x[0]))
    return json.dumps(cells, ensure_ascii=True)


# ---------------------------------------------------------------------------
# mkdocs plumbing
# ---------------------------------------------------------------------------
def _render(rows: list[dict]) -> str:  # kept for the standalone smoke test / bench
    return render_fragment(rows)


def _status_banner(config) -> str:
    """Freshness + expiry banner from refresh_status.json (auto-refresh chain).

    Escalates note -> warning -> danger as expiry nears. Empty if no status file
    (e.g. before the auto-refresh has ever run).
    """
    p = _snapshot_path(config).parent / "refresh_status.json"
    if not p.exists():
        return ""
    try:
        st = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return ""
    last = st.get("last_refreshed", "?")
    expires = st.get("expires") or ""
    fresh = f"Last auto-refreshed **{escape(str(last))}**"
    if not expires:
        return f'!!! note "Coverage status"\n    {fresh}.\n\n'
    try:
        days = (date.fromisoformat(expires) - date.today()).days
    except Exception:
        return f'!!! note "Coverage status"\n    {fresh}.\n\n'
    renew = (f"Renew with `sbatch --export=ALL,COV_EXPIRES=<future> "
             f"bin/coverage_refresh.sbatch` or it stops.")
    if days < 0:
        kind, msg = "danger", f"Auto-refresh **expired {escape(expires)}** -- snapshot is frozen. {renew}"
    elif days <= 7:
        kind, msg = "danger", f"Auto-refresh **expires {escape(expires)}** (~{days}d). {renew}"
    elif days <= 30:
        kind, msg = "warning", f"Auto-refresh expires {escape(expires)} (~{days}d). {renew}"
    else:
        kind, msg = "note", f"Auto-refresh active; renewal due by {escape(expires)} (~{days}d)."
    return f'!!! {kind} "Coverage status"\n    {fresh}. {msg}\n\n'


def _fragment(config) -> str:
    banner = _status_banner(config)
    path = _snapshot_path(config)
    if not path.exists():
        return banner + ('!!! warning "Coverage snapshot not generated yet"\n'
                "    No `.coder/coverage/latest.csv` found. Generate it with "
                "`make matrix` (full readiness) or `make matrix-coverage` "
                "(config-only), then rebuild the docs.\n")
    try:
        rows = _read_rows(path)
    except Exception as exc:  # never break --strict on a malformed snapshot
        return banner + f'!!! danger "Could not read coverage snapshot"\n    `{escape(str(exc))}`\n'
    if not rows:
        return banner + '!!! note "Coverage snapshot is empty."\n'
    return banner + render_fragment(rows)


# --- standalone smoke test: python docs_hooks/coverage_matrix.py <csv> ---
if __name__ == "__main__":
    import sys

    csv_path = Path(sys.argv[1] if len(sys.argv) > 1 else ".coder/coverage/latest.csv")
    print(render_fragment(_read_rows(csv_path)))


# ---------------------------------------------------------------------------
# Survey timeline: years x countries, one bar per wave (2026-09-07)
# ---------------------------------------------------------------------------
# A wave bar's THICKNESS is the number of features graded for that wave (the
# more the library covers, the taller the bar), and its COLOUR is the share of
# those that are sane/blessed, on a single-hue ordinal ramp validated in both
# themes (4 steps; light end clears 2:1 against the surface; monotone L).
# Two channels, two facts: one scale each.  A wave with nothing graded (or a
# country whose data channel is blocked / unconfigured) is an outlined bar in
# the family colour that names WHY, so an empty year never reads as "no survey".
GRADED_TIERS = {"sane", "blessed", "builds", "broken", "dropped", "declared"}
SHARE_BINS = [(0.5, 0), (0.8, 1), (0.999, 2), (10.0, 3)]     # share < bound -> step
SHARE_RAMP = {
    "light": ["#7dbd76", "#48a64b", "#2a8a37", "#10702a"],
    "dark":  ["#2f6b3a", "#3a8c42", "#4aa54e", "#86d786"],
}
SHARE_LABEL = ["under half sane", "half to 80% sane", "80-99% sane", "all sane"]
TIMELINE_MARKER = "<!-- COVERAGE_TIMELINE -->"


def wave_span(wave: str) -> tuple[int, int] | None:
    """(start_year, end_year_exclusive) for a wave label, or None if unparseable.
    ``1994`` -> (1994, 1995); ``2005-06`` -> (2005, 2007); ``2008-15`` -> (2008, 2016);
    ``2010Q3`` -> (2010, 2011); ``1994a`` -> (1994, 1995); ``1995-97`` -> (1995, 1998)."""
    m = re.match(r"^(\d{4})(?:-(\d{2,4}))?", wave or "")
    if not m:
        return None
    y0 = int(m.group(1))
    if m.group(2):
        tail = m.group(2)
        y1 = int(tail) if len(tail) == 4 else (y0 // 100) * 100 + int(tail)
        if y1 < y0:            # '1999-00' style century wrap
            y1 += 100
        return (y0, y1 + 1)
    return (y0, y0 + 1)


def timeline_rows(rows: list[dict]) -> dict[str, list[dict]]:
    """{country: [{wave, y0, y1, graded, sane, tiers Counter, kind}, ...]} in wave order."""
    per: dict[tuple[str, str], Counter] = {}
    for r in rows:
        if not r["wave"]:
            continue
        per.setdefault((r["country"], r["wave"]), Counter())[r["tier"]] += 1
    out: dict[str, list[dict]] = {}
    for (c, w), tiers in per.items():
        span = wave_span(w)
        if span is None:
            continue
        graded = sum(n for t, n in tiers.items() if t in GRADED_TIERS)
        sane = tiers.get("sane", 0) + tiers.get("blessed", 0)
        if graded:
            kind = "graded"
        elif tiers.get("blocked"):
            kind = "blocked"
        elif tiers.get("unconfigured"):
            kind = "unconfigured"
        else:
            kind = "empty"
        out.setdefault(c, []).append(dict(wave=w, y0=span[0], y1=span[1], graded=graded,
                                          sane=sane, tiers=tiers, kind=kind))
    for v in out.values():
        v.sort(key=lambda d: (d["y0"], d["wave"]))
    return out


def _share_step(sane: int, graded: int) -> int:
    share = sane / graded if graded else 0.0
    for bound, step in SHARE_BINS:
        if share < bound:
            return step
    return 3


def render_timeline(rows: list[dict], *, order: str = "alpha") -> str:
    """Inline SVG timeline.  ``order``: 'alpha' | 'first' (earliest wave first)."""
    data = timeline_rows(rows)
    if not data:
        return '<p class="cov-note">No wave-level cells in the snapshot.</p>'
    countries = sorted(data)
    if order == "first":
        countries.sort(key=lambda c: (data[c][0]["y0"], c))
    years = [d["y0"] for v in data.values() for d in v] + [d["y1"] for v in data.values() for d in v]
    Y0, Y1 = min(years) - 1, max(years) + 1
    max_graded = max((d["graded"] for v in data.values() for d in v), default=1) or 1

    LEFT, TOP, ROW, PX = 150, 30, 22, 17        # px per year
    W = LEFT + (Y1 - Y0) * PX + 12
    H = TOP + ROW * len(countries) + 8
    x = lambda y: LEFT + (y - Y0) * PX

    ramp_light = "".join(f".sh{i}{{fill:{c};}}" for i, c in enumerate(SHARE_RAMP["light"]))
    ramp_dark = "".join(f".sh{i}{{fill:{c};}}" for i, c in enumerate(SHARE_RAMP["dark"]))
    css = f"""<style>
.cov-tl{{background:var(--cov-bg);color:var(--cov-ink);overflow-x:auto;max-width:100%;}}
.cov-tl svg{{display:block;font:11px system-ui,-apple-system,"Segoe UI",sans-serif;}}
.cov-tl text{{fill:var(--cov-ink2);}} .cov-tl text.c{{fill:var(--cov-ink);}}
.cov-tl line.g{{stroke:var(--cov-line);stroke-width:1;}} .cov-tl line.g5{{stroke:var(--cov-ink2);stroke-opacity:.35;}}
.cov-tl rect.w:hover{{filter:brightness(1.15);}}
.cov-tl rect.empty{{fill:none;stroke:var(--t-absent);stroke-width:1.5;}}
.cov-tl rect.blocked{{fill:none;stroke:var(--t-blocked);stroke-width:1.5;stroke-dasharray:3 2;}}
.cov-tl rect.unconfigured{{fill:none;stroke:var(--t-unconfigured);stroke-width:1.5;stroke-dasharray:3 2;}}
.cov-tl {ramp_light}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]) .cov-tl {ramp_dark}}}
:root[data-theme="dark"] .cov-tl,[data-md-color-scheme="slate"] .cov-tl {ramp_dark}
.cov-tl-legend{{display:flex;flex-wrap:wrap;gap:8px 18px;align-items:center;font-size:12px;color:var(--cov-ink2);margin:6px 0 8px;}}
.cov-tl-legend .ramp i{{display:inline-block;width:16px;height:12px;margin-right:2px;vertical-align:-2px;}}
.cov-tl-legend .thick i{{display:inline-block;width:14px;background:var(--cov-ink2);margin-right:2px;vertical-align:bottom;border-radius:1px;}}
</style>"""
    parts = [f'<svg viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
             f'aria-label="Survey waves by year and country">']
    # year grid + labels
    for y in range(Y0, Y1 + 1):
        major = y % 5 == 0
        parts.append(f'<line class="g{" g5" if major else ""}" x1="{x(y)}" y1="{TOP - 6}" x2="{x(y)}" y2="{H - 4}"/>')
        if major:
            parts.append(f'<text x="{x(y)}" y="{TOP - 10}" text-anchor="middle">{y}</text>')
    for i, c in enumerate(countries):
        cy = TOP + ROW * i + ROW / 2
        parts.append(f'<line class="g" x1="{LEFT - 4}" y1="{TOP + ROW * (i + 1)}" x2="{W - 8}" y2="{TOP + ROW * (i + 1)}"/>')
        parts.append(f'<text class="c" x="{LEFT - 8}" y="{cy + 4}" text-anchor="end">{escape(c)}</text>')
        for d in data[c]:
            x0, x1 = x(d["y0"]) + 1, x(d["y1"]) - 1
            if d["kind"] == "graded":
                h = 4 + 14 * d["graded"] / max_graded
                step = _share_step(d["sane"], d["graded"])
                cls = f"w sh{step}"
            else:
                h, cls = 8, f"w {d['kind']}"
            counts = ", ".join(f"{t} {n}" for t, n in sorted(d["tiers"].items(), key=lambda kv: -kv[1]))
            tip = (f"{c} {d['wave']}: {d['graded']} features graded, {d['sane']} sane"
                   f"{' / blessed' if d['tiers'].get('blessed') else ''}"
                   f"{'' if d['kind'] == 'graded' else ' -- ' + d['kind']} ({counts})")
            parts.append(f'<rect class="{cls}" x="{x0}" y="{cy - h / 2:.1f}" width="{max(x1 - x0, 3)}" height="{h:.1f}" rx="2" '
                         f'data-c="{escape(c)}" data-w="{escape(d["wave"])}"><title>{escape(tip)}</title></rect>')
    parts.append("</svg>")
    legend = ('<div class="cov-tl-legend">'
              '<span class="thick">thickness = features graded &nbsp;'
              '<i style="height:5px"></i><i style="height:9px"></i><i style="height:14px"></i><i style="height:18px"></i></span>'
              '<span class="ramp">colour = share sane &nbsp;'
              + "".join(f'<i class="sh{i}" style="background:{SHARE_RAMP["light"][i]}"></i>' for i in range(4))
              + f' <small>{" / ".join(SHARE_LABEL)}</small></span>'
              '<span><i style="display:inline-block;width:14px;height:8px;border:1.5px solid var(--t-absent);vertical-align:-1px"></i> survey, nothing graded</span>'
              '<span><i style="display:inline-block;width:14px;height:8px;border:1.5px dashed var(--t-blocked);vertical-align:-1px"></i> data channel blocked / unconfigured</span>'
              '</div>')
    return _token_css() + css + '<div class="cov">' + legend + f'<div class="cov-tl">{"".join(parts)}</div></div>'


def _fragment_timeline(config) -> str:
    path = _snapshot_path(config)
    if not path.exists():
        return ""
    try:
        rows = _read_rows(path)
    except Exception:
        return ""
    return render_timeline(rows) if rows else ""


def on_page_markdown(markdown: str, *, page, config, files=None, **kwargs):  # mkdocs hook
    if page.file.src_uri != PAGE:
        return markdown
    if MARKER in markdown:
        markdown = markdown.replace(MARKER, _fragment(config))
    if TIMELINE_MARKER in markdown:
        markdown = markdown.replace(TIMELINE_MARKER, _fragment_timeline(config))
    return markdown
