"""Coverage accounting only; no cache discovery, data readers or builders.

The manifest supplies the scope, and ordinary pytest reports supply evidence
(ledger1480 §§2–5). The explicit CLI uses the same result schema.
"""
from collections import Counter
import json


FIELDS = ("shape", "columns", "index_names", "dtypes", "content_hash")
PROPERTY = "uganda_invariance"
TEST_ID = "test_uganda_invariance.py::test_parquet_matches_baseline["


def entry_result(key, entry, status="not_run", reason=None):
    """Uncompared pinned fields stay visibly uncompared, never passed."""
    fields = {field: "unpinned" if field in entry and entry[field] is None else "not_compared"
              for field in FIELDS}
    return {"key": key, "status": status, "reason": reason, "fields": fields}


def summarize(baseline, results, *, unknown_files=None):
    """Full manifest coverage and successful invariance are separate claims."""
    entries = [results.get(key, entry_result(key, entry))
               for key, entry in sorted(baseline.items())]
    statuses = Counter(entry["status"] for entry in entries)
    field_counts = Counter(value for entry in entries for value in entry["fields"].values())
    checked = field_counts["passed"] + field_counts["failed"]
    required = sum(value.get(field) is not None for value in baseline.values() for field in FIELDS)
    compared = sum(any(value in {"passed", "failed"} for value in entry["fields"].values())
                   and "not_compared" not in entry["fields"].values() for entry in entries)
    complete = bool(entries) and compared == len(entries) and checked == required
    return {
        "coverage_complete": complete,
        "invariance_passed": complete and statuses["matched"] == len(entries) and not (set(results) - set(baseline)),
        "required_entries": len(entries), "compared_entries": compared,
        "required_fields": required, "checked_fields": checked,
        "entry_counts": dict(sorted(statuses.items())),
        "field_counts": {name: field_counts[name] for name in
                         ("passed", "failed", "not_compared", "unpinned")},
        "baseline_tables": sorted({key.rsplit("/", 1)[-1].removesuffix(".parquet") for key in baseline}),
        "unknown_files": None if unknown_files is None else sorted(unknown_files),
        "unknown_result_keys": sorted(set(results) - set(baseline)),
        "entries": entries,
    }


def summary_lines(summary):
    coverage = "COMPLETE" if summary["coverage_complete"] else "INCOMPLETE"
    verdict = "PASSED" if summary["invariance_passed"] else "NOT PASSED"
    counts = ", ".join(f"{key}={value}" for key, value in summary["entry_counts"].items())
    unknown = "not inventoried" if summary["unknown_files"] is None else len(summary["unknown_files"])
    return [
        f"Uganda invariance coverage {coverage}: {summary['compared_entries']}/{summary['required_entries']} entries, "
        f"{summary['checked_fields']}/{summary['required_fields']} required field comparisons; verification {verdict}.",
        f"Entry outcomes: {counts or 'none'}. Unpinned fields={summary['field_counts']['unpinned']}; "
        f"unknown files={unknown}; unknown reported keys={len(summary['unknown_result_keys'])}.",
    ]


def pytest_coverage(terminalreporter, baseline, *, collected=(), deselected=()):
    """Reuse xdist-serialized report properties; never inspect a data root."""
    results = {}
    phase_errors = set()
    seen = False
    for nodeid in set(collected) | set(deselected):
        if TEST_ID not in nodeid:
            continue
        seen = True
        key = nodeid.split(TEST_ID, 1)[1].removesuffix("]")
        if key not in baseline:
            results[key] = {"key": key, "status": "unknown_key", "fields": {},
                            "reason": "pytest collected a key outside the baseline inventory"}
        else:
            status = "deselected" if nodeid in deselected else "not_run"
            results[key] = entry_result(key, baseline[key], status, "not selected by pytest" if status == "deselected" else None)
    for category, reports in terminalreporter.stats.items():
        for report in reports:
            nodeid = getattr(report, "nodeid", "")
            if TEST_ID not in nodeid:
                continue
            seen = True
            key = nodeid.split(TEST_ID, 1)[1].removesuffix("]")
            if key not in baseline:
                results[key] = {"key": key, "status": "unknown_key", "fields": {},
                                "reason": "pytest reported a key outside the baseline inventory"}
                continue
            if category == "deselected":
                results[key] = entry_result(key, baseline[key], "deselected", "not selected by pytest")
                continue
            for name, value in getattr(report, "user_properties", ()):
                if name == PROPERTY:
                    results[key] = json.loads(value)
            if getattr(report, "failed", False) and getattr(report, "when", "call") != "call":
                phase_errors.add(key)
            if key in results and results[key]["status"] in {"matched", "mismatch", "error", "partial", "missing", "baseline_error", "unknown_key"}:
                continue
            if getattr(report, "skipped", False):
                reason = str(getattr(report, "longrepr", "skipped before comparison"))
                status = "credential_skipped" if "needs S3 credentials" in reason else "skipped"
                results[key] = entry_result(key, baseline[key], status, reason)
            elif getattr(report, "failed", False):
                results[key] = entry_result(key, baseline[key], "error", "pytest failed before recording comparisons")
    for key in phase_errors:
        result = results.setdefault(key, entry_result(key, baseline[key]))
        result.update(status="error", reason="pytest setup/teardown failed")
    return summarize(baseline, results) if seen else None
