#!/usr/bin/env python
"""Run ll.diagnostics on the new outputs from each pending PR.

Targets:
  - PR #230: household_characteristics for Guyana, Azerbaijan, Serbia and Montenegro
  - PR #242: food_expenditures for Ethiopia, Nigeria
  - PR #243: food_acquired + food_expenditures for GhanaLSS
              + check_panel_consistency('GhanaLSS') for the GLSS1↔GLSS2 panel
"""
import sys
import warnings
import time

warnings.simplefilter("ignore")

import lsms_library as ll
from lsms_library.diagnostics import (
    is_this_feature_sane,
    validate_feature,
    check_panel_consistency,
)


TARGETS = [
    ("PR #230", "Guyana", "household_characteristics"),
    ("PR #230", "Azerbaijan", "household_characteristics"),
    ("PR #230", "Serbia and Montenegro", "household_characteristics"),
    ("PR #242", "Ethiopia", "food_expenditures"),
    ("PR #242", "Nigeria", "food_expenditures"),
    ("PR #243", "GhanaLSS", "food_acquired"),
    ("PR #243", "GhanaLSS", "food_expenditures"),
]


def _run_validate(pr, country, feature):
    print(f"\n{'=' * 70}")
    print(f"{pr}: validate_feature({country!r}, {feature!r})")
    print("=" * 70, flush=True)
    t0 = time.time()
    try:
        report = validate_feature(country, feature)
    except Exception as e:
        print(f"  RAISED: {type(e).__name__}: {e}")
        return None
    secs = time.time() - t0
    print(f"  ran in {secs:.1f}s, report.ok={report.ok}")
    report.summarize()
    return report


def _run_panel(country):
    print(f"\n{'=' * 70}")
    print(f"PR #243: check_panel_consistency(Country({country!r}))")
    print("=" * 70, flush=True)
    t0 = time.time()
    try:
        report = check_panel_consistency(ll.Country(country))
    except Exception as e:
        print(f"  RAISED: {type(e).__name__}: {e}")
        return None
    secs = time.time() - t0
    print(f"  ran in {secs:.1f}s, report.ok={report.ok}")
    report.summarize()
    return report


def main():
    print(f"=== diagnostics on combined PR branch {time.strftime('%H:%M:%S')} ===")
    print(f"lsms_library: {ll.__file__}")
    print()

    results = []
    for pr, country, feature in TARGETS:
        rep = _run_validate(pr, country, feature)
        results.append((pr, country, feature, rep))

    panel_rep = _run_panel("GhanaLSS")

    print(f"\n\n{'#' * 70}")
    print("FINAL TALLY")
    print("#" * 70)
    for pr, country, feature, rep in results:
        if rep is None:
            print(f"  {pr}: {country:<22} {feature:<28} EXCEPTION")
            continue
        n_total = len(rep.checks)
        n_fail = sum(1 for c in rep.checks if c.status == "FAIL")
        n_warn = sum(1 for c in rep.checks if c.status == "WARN")
        n_pass = n_total - n_fail - n_warn
        verdict = "OK" if rep.ok else "FAIL"
        print(f"  {pr}: {country:<22} {feature:<28} {verdict}  ({n_pass}P/{n_warn}W/{n_fail}F of {n_total})")

    if panel_rep is not None:
        n_total = len(panel_rep.checks)
        n_fail = sum(1 for c in panel_rep.checks if c.status == "FAIL")
        n_warn = sum(1 for c in panel_rep.checks if c.status == "WARN")
        n_pass = n_total - n_fail - n_warn
        verdict = "OK" if panel_rep.ok else "FAIL"
        print(f"  PR #243: GhanaLSS               panel_consistency           {verdict}  ({n_pass}P/{n_warn}W/{n_fail}F of {n_total})")


if __name__ == "__main__":
    main()
