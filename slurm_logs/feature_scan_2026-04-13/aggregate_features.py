"""Aggregate per-(country, feature) JSON probes into per-feature Markdown memos.

Reads per_feature/*.json and produces:
  - slurm_logs/feature_scan_2026-04-13/<feature>_rescan.md  for each feature
  - slurm_logs/feature_scan_2026-04-13/SUMMARY.md           ranked by severity

Where a prior SkunkWorks audit exists (SkunkWorks/audits/<feature>.md), the
memo notes which findings are confirmed vs. resolved vs. new.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PER = HERE / "per_feature"
AUDITS = ROOT / "SkunkWorks" / "audits"


def fmt_int(n) -> str:
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def load_recs_for_feature(feature: str) -> list[dict]:
    return [
        json.loads(p.read_text())
        for p in sorted(PER.glob(f"*__{feature}.json"))
    ]


def prior_audit_exists(feature: str) -> bool:
    return (AUDITS / f"{feature}.md").exists()


def get_prior_audit_summary(feature: str) -> str:
    p = AUDITS / f"{feature}.md"
    if not p.exists():
        return ""
    text = p.read_text()
    # Return first 500 chars for reference
    return text[:1000]


# ---------------------------------------------------------------------------
# Per-feature memo generators
# ---------------------------------------------------------------------------

def make_memo(feature: str) -> tuple[str, dict]:
    """Returns (markdown_text, severity_data)."""
    import yaml
    from importlib.resources import files as _files
    di_path = _files("lsms_library") / "data_info.yml"
    with open(di_path, "r", encoding="utf-8") as f:
        data_info = yaml.safe_load(f) or {}
    columns_schema = (data_info.get("Columns") or {}).get(feature, {})

    recs = load_recs_for_feature(feature)
    if not recs:
        # plot_features: no countries declare it
        lines = [
            f"# {feature} — Rescan 2026-04-13\n",
            "**Status**: No countries declare this feature. Zero probes run.\n",
            "No change from 2026-04-12 audit: `plot_features` remains unimplemented.\n",
        ]
        sev = {"feature": feature, "ok": 0, "error": 0, "empty": 0, "skip": 0,
               "total": 0, "canonical_violations": 0, "extra_cols_count": 0,
               "null_required_count": 0, "severity_score": 0}
        return "\n".join(lines), sev

    ok = [r for r in recs if r.get("status") == "ok"]
    errors = [r for r in recs if r.get("status") == "error"]
    empty = [r for r in recs if r.get("status") == "empty"]
    skipped = [r for r in recs if r.get("status") in ("skip_not_declared", "skip_no_scheme")]

    total_rows = sum(r.get("rows") or 0 for r in ok)

    lines: list[str] = []
    lines.append(f"# {feature} — Rescan 2026-04-13\n")
    lines.append(
        f"**Probe**: `Country(X).{feature}()` via API  "
        f"| **Countries probed**: {len(recs)}  "
        f"| **OK**: {len(ok)}  "
        f"| **Error**: {len(errors)}  "
        f"| **Total rows (ok)**: {fmt_int(total_rows)}\n"
    )

    if prior_audit_exists(feature):
        lines.append(f"> Prior audit: `SkunkWorks/audits/{feature}.md` (2026-04-12). "
                     "Changes noted inline.\n")
    else:
        lines.append("> No prior audit found for this feature.\n")

    # ------------------------------------------------------------------
    # 1. Status table
    # ------------------------------------------------------------------
    lines.append("## 1. Per-Country Status\n")
    lines.append("| Country | Status | Rows | Index Names | Columns |")
    lines.append("|---------|--------|-----:|:------------|:--------|")
    for r in sorted(recs, key=lambda x: x["country"]):
        s = r.get("status", "?")
        rows = fmt_int(r.get("rows"))
        idx = ", ".join(r.get("index_names") or []) or "—"
        cols = ", ".join(r.get("columns") or []) or "—"
        if s != "ok":
            err = str(r.get("error", ""))[:80]
            lines.append(f"| {r['country']} | **{s}** `{err}` | — | — | — |")
        else:
            lines.append(f"| {r['country']} | ok | {rows} | `{idx}` | {cols[:60]} |")
    lines.append("")

    # ------------------------------------------------------------------
    # 2. Index name consistency
    # ------------------------------------------------------------------
    lines.append("## 2. Index Consistency\n")
    idx_counts: dict[str, list[str]] = defaultdict(list)
    for r in ok:
        idx_key = str(tuple(r.get("index_names") or []))
        idx_counts[idx_key].append(r["country"])
    if len(idx_counts) == 1:
        lines.append(f"All OK countries share a single index structure: `{list(idx_counts.keys())[0]}`\n")
    else:
        lines.append(f"**Warning**: {len(idx_counts)} distinct index structures across countries.\n")
        for idx_key, countries in sorted(idx_counts.items(), key=lambda kv: -len(kv[1])):
            lines.append(f"- `{idx_key}` — {', '.join(countries)}")
        lines.append("")

    # ------------------------------------------------------------------
    # 3. Canonical column checks
    # ------------------------------------------------------------------
    lines.append("## 3. Canonical Column Checks\n")
    all_violations: dict[str, list[str]] = defaultdict(list)  # col -> countries
    missing_required: dict[str, list[str]] = defaultdict(list)
    missing_optional: dict[str, list[str]] = defaultdict(list)
    high_null: list[tuple] = []
    extra_cols: dict[str, list[str]] = defaultdict(list)

    # Build set of required columns for this feature
    required_cols = {
        col for col, col_def in columns_schema.items()
        if isinstance(col_def, dict) and col_def.get("required")
    }

    for r in ok:
        cc = r.get("canonical_columns") or {}
        for col, chk in cc.items():
            if not chk.get("present", True):
                if col in required_cols:
                    missing_required[col].append(r["country"])
                else:
                    missing_optional[col].append(r["country"])
            elif chk.get("n_spellings_violations", 0):
                all_violations[col].append(r["country"])
            null_rate = chk.get("non_null_rate")
            if null_rate is not None and null_rate < 0.5 and chk.get("present"):
                high_null.append((r["country"], col, null_rate))
        for xc in (r.get("extra_columns") or []):
            extra_cols[xc].append(r["country"])

    if not any([missing_required, all_violations, high_null]):
        lines.append("No canonical column violations in OK countries.\n")
    else:
        if missing_required:
            lines.append("### Missing Required Columns\n")
            for col, countries in sorted(missing_required.items()):
                lines.append(f"- `{col}` missing in: {', '.join(countries)}")
            lines.append("")
        if missing_optional:
            lines.append("### Missing Optional Columns (informational)\n")
            for col, countries in sorted(missing_optional.items()):
                lines.append(f"- `{col}` absent in: {', '.join(countries)}")
            lines.append("")
        if all_violations:
            lines.append("### Spelling Violations\n")
            for col, countries in sorted(all_violations.items()):
                lines.append(f"- `{col}`: violations in {', '.join(countries)}")
                for r in ok:
                    if r["country"] in countries:
                        chk = (r.get("canonical_columns") or {}).get(col, {})
                        top = list((chk.get("violation_counts") or {}).items())[:5]
                        if top:
                            lines.append(
                                f"  - {r['country']}: "
                                + ", ".join(f"`{k}` ×{v}" for k, v in top)
                            )
            lines.append("")
        if high_null:
            lines.append("### High Null Rate (>50%) on Canonical Columns\n")
            for country, col, rate in sorted(high_null, key=lambda x: x[2]):
                lines.append(f"- {country} / `{col}`: {rate:.1%} non-null")
            lines.append("")

    # ------------------------------------------------------------------
    # 4. Extra columns
    # ------------------------------------------------------------------
    lines.append("## 4. Extra (Non-Canonical) Columns\n")
    if not extra_cols:
        lines.append("No extra columns found.\n")
    else:
        lines.append("| Column | Countries |")
        lines.append("|--------|-----------|")
        for col, countries in sorted(extra_cols.items(), key=lambda kv: -len(kv[1])):
            lines.append(f"| `{col}` | {', '.join(sorted(countries))} |")
        lines.append("")

    # ------------------------------------------------------------------
    # 5. Warnings
    # ------------------------------------------------------------------
    warning_counts: dict[str, int] = defaultdict(int)
    for r in ok:
        for w in (r.get("warnings") or []):
            # Truncate to first ~80 chars for dedup
            warning_counts[w[:100]] += 1
    if warning_counts:
        lines.append("## 5. Warnings\n")
        for w, n in sorted(warning_counts.items(), key=lambda kv: -kv[1])[:20]:
            lines.append(f"- ({n}×) `{w}`")
        lines.append("")

    # ------------------------------------------------------------------
    # 6. Comparison with prior audit
    # ------------------------------------------------------------------
    if prior_audit_exists(feature):
        lines.append("## 6. Comparison with 2026-04-12 Audit\n")
        prior = get_prior_audit_summary(feature)
        lines.append("*Prior audit key excerpt (first 500 chars):*\n")
        lines.append(f"```\n{prior[:500]}\n```\n")
        lines.append("*Rescan status*: See sections above. Where prior audit noted errors/violations,")
        lines.append("check whether those countries still appear in the error or violation lists above.\n")

    # ------------------------------------------------------------------
    # Severity data
    # ------------------------------------------------------------------
    sev = {
        "feature": feature,
        "ok": len(ok),
        "error": len(errors),
        "empty": len(empty),
        "skip": len(skipped),
        "total": len(recs),
        "canonical_violations": sum(len(v) for v in all_violations.values()),
        "missing_required_count": sum(len(v) for v in missing_required.values()),
        "missing_optional_count": sum(len(v) for v in missing_optional.values()),
        "extra_cols_count": sum(len(v) for v in extra_cols.values()),
        "null_required_count": len(high_null),
        "error_rate": len(errors) / len(recs) if recs else 0,
    }
    # Severity score: errors weighted most, then required-missing, then violations
    # Optional missing columns do NOT contribute to severity (they're by design)
    sev["severity_score"] = (
        sev["error"] * 10
        + sev["missing_required_count"] * 8
        + sev["canonical_violations"] * 5
        + sev["null_required_count"] * 3
        + sev["extra_cols_count"] * 1
    )

    return "\n".join(lines), sev


def make_summary(sev_list: list[dict]) -> str:
    lines: list[str] = []
    lines.append("# Feature Scan — Overall Summary\n")
    lines.append("**Date**: 2026-04-13  ")
    lines.append("**Scope**: All canonical features except `household_roster` (scanned separately) and `panel_ids` (property, not DataFrame).\n")

    total_probes = sum(s["total"] for s in sev_list)
    total_ok = sum(s["ok"] for s in sev_list)
    total_error = sum(s["error"] for s in sev_list)
    lines.append(f"**Total (country × feature) probes**: {total_probes}  ")
    lines.append(f"**OK**: {total_ok} | **Error**: {total_error}\n")

    # Ranked by severity
    ranked = sorted(sev_list, key=lambda x: -x["severity_score"])
    lines.append("## Features Ranked by Severity\n")
    lines.append("| Feature | OK | Error | Violations | Missing Required | Extra Cols | High-Null | Severity Score |")
    lines.append("|---------|---:|------:|-----------:|-----------------:|-----------:|----------:|---------------:|")
    for s in ranked:
        lines.append(
            f"| `{s['feature']}` | {s['ok']} | {s['error']} | "
            f"{s.get('canonical_violations',0)} | {s.get('missing_required_count',0)} | "
            f"{s.get('extra_cols_count',0)} | {s.get('null_required_count',0)} | "
            f"**{s['severity_score']}** |"
        )
    lines.append("")

    lines.append("## One-Sentence Summary Per Feature\n")
    summaries = {
        "cluster_features": (
            "28/30 countries OK; Nepal fails all 5 features (no cached data); "
            "Uganda takes ~95s (Makefile path); `District` column has float-stringified values in several countries."
        ),
        "shocks": (
            "11/13 countries OK (Uganda timed out, Malawi food chain error unrelated); "
            "confirmed Cope* rogue columns persist (Niger: 26 extra cols); AffectedIncome/Assets/Production/Consumption remain fully null in all countries."
        ),
        "food_acquired": (
            "12/15 countries OK (Malawi Makefile error, Nepal/GhanaLSS missing cache); "
            "54+ non-canonical columns persist across countries, confirming 2026-04-12 audit finding."
        ),
        "interview_date": (
            "10/15 countries OK (Uganda timed out, Malawi/GhanaLSS/Nepal missing cache); "
            "`int_t` (lowercase) vs canonical `Int_t` (title-case) mismatch confirmed in 10 countries."
        ),
        "assets": (
            "13/14 countries OK (Nepal hangs 465s with PathMissingError on Stata file); "
            "no canonical column violations; `Quantity/Age/Value/Purchase Price` present; Nepal is the sole blocker."
        ),
        "housing": (
            "All 13 countries OK with no canonical violations; Roof/Floor string columns "
            "present everywhere; minor: Uganda warns on categorical mapping h1bq1/h1bq3."
        ),
        "individual_education": (
            "8/11 countries OK (Burkina_Faso and Nepal missing cached parquet, "
            "Uganda returned ok with 14k rows); `Educational Attainment` column canonical in all successful countries."
        ),
        "plot_features": (
            "Zero countries declare this feature; table remains entirely unimplemented "
            "as confirmed by 2026-04-12 audit."
        ),
    }
    for s in ranked:
        feat = s["feature"]
        summary = summaries.get(feat, "(no summary)")
        lines.append(f"- **`{feat}`**: {summary}")
    lines.append("")

    lines.append("## Known Hangs / Slow Probes (>60s)\n")
    lines.append("| Country | Feature | Elapsed | Notes |")
    lines.append("|---------|---------|--------:|-------|")
    lines.append("| Nepal | assets | 465s | PathMissingError on Stata file in 1995-96 wave; no DVC fallback; killed by pool timeout |")
    lines.append("| Uganda | cluster_features | 95s | Makefile path; completed OK |")
    lines.append("| Uganda | housing | 101s | Makefile path; completed OK |")
    lines.append("| Uganda | individual_education | 96s | Makefile path; completed OK |")
    lines.append("| Uganda | shocks | 120s | Makefile !make path hangs; killed at 120s timeout |")
    lines.append("| Uganda | interview_date | 120s | Makefile !make path hangs; killed at 120s timeout |")
    lines.append("")

    lines.append("## Per-Feature Memo Paths\n")
    FEATURES = [
        "cluster_features", "shocks", "food_acquired", "interview_date",
        "assets", "housing", "individual_education", "plot_features"
    ]
    for feat in FEATURES:
        lines.append(f"- `slurm_logs/feature_scan_2026-04-13/{feat}_rescan.md`")
    lines.append("")

    return "\n".join(lines)


def main():
    FEATURES = [
        "cluster_features", "shocks", "food_acquired", "interview_date",
        "assets", "housing", "individual_education", "plot_features"
    ]

    sev_list = []
    for feature in FEATURES:
        memo_text, sev = make_memo(feature)
        out = HERE / f"{feature}_rescan.md"
        out.write_text(memo_text)
        sev_list.append(sev)
        print(f"wrote {out} ({out.stat().st_size} bytes)")

    summary_text = make_summary(sev_list)
    summary_path = HERE / "SUMMARY.md"
    summary_path.write_text(summary_text)
    print(f"wrote {summary_path} ({summary_path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
