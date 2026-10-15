"""The venv must not carry an editable install of this project (GH #957).

``lsms_library.pth`` named ONE checkout -- the shared mirror -- so every
``python <script>.py`` and every console script imported that checkout no matter
which tree the caller was working in.  Removing it is only half the fix: the
other half is that nothing may quietly put it back.

``poetry install`` installs the root project as an editable package BY DEFAULT.
So a single ``poetry install`` anywhere in the repo's own tooling silently
reverts the change, and the reversion is invisible -- everything keeps working,
just against the wrong checkout.  That is exactly what happened: ``Makefile``'s
setup stamp took ``poetry.lock`` as a prerequisite, the lock became tracked, and
the next ``make test`` would have reinstalled the root.  It was caught by review,
not by a test.  This is the test.

The second test is the other half of the same defect.  With no editable install,
NEITHER invocation style used in this repo puts the repo on ``sys.path``:

    poetry run pytest ...            console script -> sys.path[0] = <venv>/bin
    poetry run python bench/x.py     script run     -> sys.path[0] = bench/

so a Makefile that stops installing the root must also export ``PYTHONPATH``, or
every recipe raises ``ModuleNotFoundError``.  The two have to move together, and
pinning one without the other would licence a half-fix.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# Executable tooling only.  Prose that merely mentions the command (docs, skills,
# handoffs) is not a trigger and must not be a false positive.
_SCANNED = [
    REPO / "Makefile",
    REPO / "lsms_library" / "Makefile",
    *sorted((REPO / ".github" / "workflows").glob("*.yml")),
    *sorted((REPO / "bin").glob("*.sh")),
    *sorted((REPO / "bin").glob("*.sbatch")),
]

_INSTALL = re.compile(r"\bpoetry\s+install\b")
_COMMENT = re.compile(r"^\s*#")
# `@echo "... poetry install ..."` in a help target PRINTS the command, it does
# not run it.  Excluding echo lines rather than the whole Makefile keeps a real
# `echo x && poetry install` visible.
_ECHOED = re.compile(r"^\s*@?echo\b")


def _install_lines():
    """(path, lineno, line) for every non-comment `poetry install` invocation."""
    for path in _SCANNED:
        if not path.exists():
            continue
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if _COMMENT.match(line) or _ECHOED.match(line):
                continue
            if not _INSTALL.search(line):
                continue
            yield path.relative_to(REPO), n, line.strip()


def test_every_poetry_install_passes_no_root():
    offenders = [
        f"{p}:{n}: {line}"
        for p, n, line in _install_lines()
        if "--no-root" not in line
    ]
    assert not offenders, (
        "`poetry install` installs this project as an editable root by default, "
        "which recreates the lsms_library.pth that GH #957 removed and silently "
        "re-imposes one checkout on every consumer.  Add --no-root:\n  "
        + "\n  ".join(offenders)
    )


def test_it_found_something_to_check():
    """Guard against the scan silently matching nothing.

    A regex that stops matching would make the test above vacuously green --
    the same failure mode as the cache guards this repo keeps finding.
    """
    found = list(_install_lines())
    assert found, f"no `poetry install` invocation found under {[str(p) for p in _SCANNED]}"


@pytest.mark.parametrize("makefile", ["Makefile", "lsms_library/Makefile"])
def test_makefiles_export_pythonpath(makefile):
    text = (REPO / makefile).read_text()
    assert re.search(r"^export PYTHONPATH\b", text, re.M), (
        f"{makefile} must `export PYTHONPATH`: with no editable install, neither "
        "`poetry run pytest` (console script) nor `poetry run python <script>.py` "
        "puts the repo on sys.path, so every recipe raises ModuleNotFoundError."
    )
    assert "CURDIR" not in re.search(r"^export PYTHONPATH.*$", text, re.M).group(0), (
        f"{makefile} derives PYTHONPATH from $(CURDIR), which is the WORKING "
        "directory, not this file's -- `make -f <repo>/Makefile` from elsewhere "
        "would name the wrong tree.  Use $(MAKEFILE_LIST)."
    )
