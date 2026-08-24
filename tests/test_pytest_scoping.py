"""Collection must be scoped to `tests/` (GH #729 fallout).

pytest's default collection scans the rootdir, so any file matching
`test_*.py` / `*_test.py` ANYWHERE in the repo is imported at collection
time.  A module that does I/O at import then fails *during collection*,
which aborts the entire session -- zero tests run, and the error names
neither a test nor the change under review.

That happened: `slurm_logs/ghanasps/plot_identity_permutation_test.py`
shipped with a findings doc, matched `*_test.py`, loaded data at module
level, and killed CI with `NoCredentialsError`.

`SkunkWorks/` holds four `test_*.py` files collected today that are benign
only because their module-level code does not raise.  This pins the setting
so the protection is configuration rather than naming luck.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _pytest_config() -> dict:
    with open(REPO_ROOT / "pyproject.toml", "rb") as f:
        return tomllib.load(f).get("tool", {}).get("pytest", {}).get("ini_options", {})


def test_testpaths_is_pinned_to_tests():
    assert _pytest_config().get("testpaths") == ["tests"], (
        "pyproject.toml must pin `testpaths = [\"tests\"]`.  Without it pytest "
        "imports every test-named file in the repo at collection, and one that "
        "does I/O at import aborts the whole session (GH #729)."
    )


def test_stray_collectible_scripts_outside_tests_are_not_a_landmine():
    """Report them; do NOT fail on their existence.

    We do not control what lands in `SkunkWorks/` or `slurm_logs/`, and an
    analysis script is entitled to be named however its author likes now that
    `testpaths` means it is never imported.  This test exists to keep the
    inventory visible, so that if `testpaths` is ever removed the blast radius
    is written down rather than rediscovered.
    """
    stray = [
        p.relative_to(REPO_ROOT)
        for pat in ("test_*.py", "*_test.py")
        for p in REPO_ROOT.rglob(pat)
        if "tests" not in p.relative_to(REPO_ROOT).parts
        and ".venv" not in p.parts
        and not any(part.startswith(".") for part in p.relative_to(REPO_ROOT).parts)
    ]
    print(f"collectible-named scripts outside tests/ ({len(stray)}): "
          + ", ".join(sorted(str(s) for s in stray)))
    assert True
