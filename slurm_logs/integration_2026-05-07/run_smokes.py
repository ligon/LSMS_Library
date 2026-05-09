#!/usr/bin/env python
"""Integration smokes for PRs #228 (Timor-Leste interview_date) and #230 (NaN v rejoin).

Run from main checkout (which has the .pth pin) on branch
``pr-228-230-integration-test``.  Emits clear BEGIN / PASS / FAIL markers
suitable for line-buffered Monitor filtering.
"""
import sys
import time
import traceback

import lsms_library as ll


TESTS = [
    # (label, country, table, sanity_lambda, sanity_descr)
    (
        "PR #228 Timor-Leste interview_date",
        "Timor-Leste",
        "interview_date",
        lambda df: len(df) > 0,
        "non-empty (t,i)-indexed datetime DataFrame",
    ),
    (
        "PR #230 Guyana household_characteristics",
        "Guyana",
        "household_characteristics",
        lambda df: len(df) > 0,
        "non-empty (was returning (0, 15) before fix)",
    ),
    (
        "PR #230 Azerbaijan household_characteristics",
        "Azerbaijan",
        "household_characteristics",
        lambda df: len(df) > 0,
        "non-empty",
    ),
    (
        "PR #230 Serbia and Montenegro household_characteristics",
        "Serbia and Montenegro",
        "household_characteristics",
        lambda df: len(df) > 0,
        "non-empty",
    ),
]


def main():
    print(f"=== integration smokes started {time.strftime('%Y-%m-%d %H:%M:%S')} ===", flush=True)
    print(f"lsms_library: {ll.__file__}", flush=True)
    print(f"version: {ll.__version__}", flush=True)
    print(f"python: {sys.executable}", flush=True)
    print()

    results = []
    for label, country, table, sanity, descr in TESTS:
        print(f"\n{'='*70}")
        print(f"BEGIN: {label}")
        print(f"  expectation: {descr}")
        print(f"{'='*70}", flush=True)
        t0 = time.time()
        try:
            c = ll.Country(country)
            df = getattr(c, table)()
            secs = time.time() - t0
            shape = df.shape
            if not sanity(df):
                status = "FAIL"
                detail = f"sanity check failed; shape={shape}"
            else:
                status = "PASS"
                # Sample of the data: first 3 dtypes, first 2 rows
                dtypes_sample = dict(list(df.dtypes.items())[:3])
                detail = f"shape={shape}, dtypes_sample={dtypes_sample}"
                # Print head for human eyeballing
                with __import__("pandas").option_context("display.max_columns", 10, "display.width", 200):
                    print(f"\n  head(2):\n{df.head(2)}", flush=True)
        except Exception as e:
            secs = time.time() - t0
            status = "FAIL"
            detail = f"{type(e).__name__}: {e}"
            traceback.print_exc()

        results.append((label, status, detail, secs))
        print(f"\n[{status}] {label} ({secs:.1f}s) :: {detail}", flush=True)

    print("\n\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    for label, status, detail, secs in results:
        print(f"  [{status}] {label} ({secs:.1f}s)")
        print(f"          {detail}")

    failed = [r for r in results if r[1] == "FAIL"]
    n = len(results)
    if failed:
        print(f"\nFINAL: FAILED {len(failed)}/{n} tests")
        sys.exit(1)
    else:
        print(f"\nFINAL: PASSED {n}/{n} tests")
        sys.exit(0)


if __name__ == "__main__":
    main()
