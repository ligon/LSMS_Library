"""Runnable doctests for pure-Python helpers in ``local_tools``.

This module does NOT enable ``pytest --doctest-modules``. That would
sweep the whole library and trip on illustrative ``>>>`` blocks in
``Country``, ``Feature``, and ``diagnostics.validate_feature`` that
use ellipses and/or require live DVC/WB data access. Instead, we
pick out the functions whose docstring examples are genuinely
runnable (no network, no dataframes, no categorical_mapping.org)
and run ``doctest`` on them one at a time.

If you add a new pure-Python helper with a doctest, add it to
:data:`DOCTEST_TARGETS` below.
"""

from __future__ import annotations

import doctest

import pytest

from lsms_library import local_tools


# Each entry: the function under test + the globals dict that doctest
# should see. Globals should contain the function's own name so the
# ``>>>`` lines can reference it without an import prefix.
DOCTEST_TARGETS = [
    (
        local_tools.format_id,
        {"format_id": local_tools.format_id},
    ),
    (
        local_tools.category_union,
        {"category_union": local_tools.category_union},
    ),
]


@pytest.mark.parametrize(
    "func, globs",
    DOCTEST_TARGETS,
    ids=[t[0].__name__ for t in DOCTEST_TARGETS],
)
def test_doctest_examples(func, globs):
    """Every ``>>>`` example in the target function's docstring must pass.

    Uses :func:`doctest.DocTestFinder` + :class:`doctest.DocTestRunner`
    rather than :func:`doctest.run_docstring_examples` so we can
    inspect the failure count and fail the pytest cleanly (the
    higher-level helper only prints to stdout).
    """
    finder = doctest.DocTestFinder()
    runner = doctest.DocTestRunner(
        optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
    )
    tests = finder.find(func, globs=globs)
    assert tests, f"no doctests found on {func.__name__}"
    for test in tests:
        runner.run(test)
    results = runner.summarize(verbose=False)
    assert results.failed == 0, (
        f"{func.__name__}: {results.failed} of {results.attempted} "
        f"doctest examples failed"
    )
