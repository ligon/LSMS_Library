"""Guard: the `tests` package must resolve to THIS repo, not site-packages.

## Why this exists

`fooddatacentral` -- a direct dependency (`pyproject.toml`) -- installs a
**top-level `tests/` package** into site-packages, and it is broken on import:
its `__init__.py` is the single line `from .tests import *`, and there is no
`tests/tests.py`.

So any test module doing the natural thing::

    from tests.conftest import requires_s3

resolves `tests` to *that* package and dies with::

    ModuleNotFoundError: No module named 'tests.tests'

at **collection**, which aborts the entire pytest session.  One CI run lost all
2,005 collected tests that way.  It has already cost two PRs (#632, #651) a
full diagnosis cycle each, and the error names neither the test nor the change
under review.

## Why it is invisible where people test

A developer checkout passes because `.venv/.../lsms_library.pth` puts the repo
root on `sys.path` **ahead of** site-packages.  CI's `poetry install` has no
such entry.  Green locally, red in CI.

## What to do if this test fails

Do **not** "fix" it by adding the import back.  Either:

  * use the import-free marker form, which `tests/conftest.py` recommends::

        pytestmark = pytest.mark.requires_s3

  * or load `tests/conftest.py` **by path** (`importlib.util.spec_from_file_location`),
    which is identical in every import mode.

See GH #680 for the upstream report and the durable options.
"""
import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_tests_package_resolves_inside_this_repo():
    """`import tests` must not reach a dependency's top-level `tests` package."""
    spec = importlib.util.find_spec("tests")
    assert spec is not None, "no `tests` package on sys.path at all"
    origin = Path(spec.origin).resolve()
    assert REPO_ROOT in origin.parents, (
        f"`tests` resolves to {origin}, OUTSIDE this repo ({REPO_ROOT}).\n"
        "A dependency is shadowing our tests package -- see GH #680.  Any\n"
        "`from tests.conftest import ...` will abort collection for the WHOLE\n"
        "session.  Use `pytestmark = pytest.mark.requires_s3` or load\n"
        "tests/conftest.py by path instead."
    )


def test_no_test_module_imports_conftest_by_package_path():
    """No test module may use the import spellings that break in CI.

    Cheaper and more direct than the resolution check above: it fails on the
    *line that would break*, naming the offending file, instead of on a global
    property of sys.path.

    Parsed with `ast`, NOT by text matching -- the first cut of this test
    grepped for the string and promptly failed on the example inside its own
    docstring.  An import is a syntactic construct; match it syntactically.
    """
    import ast

    BAD_MODULES = {"conftest", "tests.conftest"}
    offenders = []
    for path in sorted((REPO_ROOT / "tests").glob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:                      # not our problem here
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module in BAD_MODULES:
                offenders.append(f"{path.name}:{node.lineno}: from {node.module} import ...")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in BAD_MODULES:
                        offenders.append(f"{path.name}:{node.lineno}: import {alias.name}")
    assert not offenders, (
        "these modules import conftest by package path, which aborts pytest\n"
        "collection in CI (GH #680):\n  " + "\n  ".join(offenders) +
        "\n\nUse `pytestmark = pytest.mark.requires_s3`, or load\n"
        "tests/conftest.py by path via importlib."
    )
