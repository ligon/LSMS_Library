#!/usr/bin/env python3
"""Read-only Uganda baseline verification on an EXPLICIT private data root.

PYTHONPATH=. python scripts/check_uganda_invariance.py \
    --data-root /private/lsms-data --complete --report /private/report.json

The root is the parent of Uganda/. This command never builds, fetches,
clears, repairs, stamps or regenerates anything (ledger1480 §§2–5).
"""
import argparse
import json
from pathlib import Path

from tests import test_uganda_invariance as invariance
from tests.uganda_invariance_reporting import summarize, summary_lines


def check(data_root, baseline, waves, tables):
    """Reuse the actual test's reader and immutable comparison oracles."""
    root = Path(data_root).resolve() / "Uganda"
    results = {key: invariance.inspect_parquet(key, entry, root / key, waves, tables)
               for key, entry in sorted(baseline.items())}
    # Only this explicitly supplied root is scanned, and only for names.
    extra = ({path.relative_to(root).as_posix() for path in root.rglob("*.parquet")}
             - set(baseline)) if root.is_dir() else set()
    report = summarize(baseline, results, unknown_files=extra)
    report["data_root"] = str(Path(data_root).resolve())
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, type=Path,
                        help="Explicit private L2 root containing Uganda/; no configured-root fallback")
    parser.add_argument("--complete", action="store_true",
                        help="Fail unless every required entry/field was compared and matched")
    parser.add_argument("--report", type=Path, help="Write the complete entry/field accounting as JSON")
    args = parser.parse_args(argv)
    if args.report:
        target = args.report.resolve()
        cache = args.data_root.resolve() / "Uganda"
        if target.is_relative_to(cache.resolve()) or target == invariance.BASELINE_PATH.resolve() or target.suffix == ".parquet":
            parser.error("report must be outside the Uganda cache and baseline manifest")
    from lsms_library import Country

    country = Country("Uganda", preload_panel_ids=False)
    report = check(args.data_root, invariance._load_baseline(),
                   [str(w) for w in country.waves], set(country.resources["Data Scheme"]))
    for line in summary_lines(report):
        print(line)
    for entry in report["entries"]:
        if entry["status"] != "matched":
            print(f"  {entry['key']}: {entry['status']}: {entry['reason'] or 'not compared'}")
    if report["unknown_files"]:
        print("Outside baseline scope: " + ", ".join(report["unknown_files"]))
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    failed = any(report["entry_counts"].get(kind, 0)
                 for kind in ("mismatch", "error", "unknown_key", "baseline_error"))
    return int(failed or (args.complete and not report["invariance_passed"]))


if __name__ == "__main__":
    raise SystemExit(main())
