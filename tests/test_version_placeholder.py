"""A killed `poetry build` must not leave this project unable to version itself.

The version comes from the git tag via poetry-dynamic-versioning.  To build, the
plugin REWRITES `pyproject.toml` into a statically-versioned form and restores it
afterwards.  A build killed partway (OOM, timeout, Ctrl-C) never restores, and
the rewritten file looks entirely plausible in a diff.

It makes THREE changes, and all three must be reverted together:

    dynamic = ["version"]   ->  dynamic = []
    version = "0.0.0"       ->  version = "<computed>"     (moved into [project])
    enable = true           ->  enable = false             ([tool.poetry-dynamic-versioning])

This is not hypothetical, and the partial fix is the instructive part.  A build
killed on a 4-core Slurm slice left all three; only the `version` string was
reverted, and *this test in its first form checked only that string*, so it
passed on a still-broken file.  The tag was cut, and `publish.yml` failed with
"Building 0.0.0 from v0.10.1" -- because `enable = false` meant the plugin never
ran.

Two guards, two directions, both needed:

* `publish.yml` fails when the version resolves to `0.0.0` -- the plugin was
  inactive or tags were not fetched.  It catches a DISABLED plugin, which is
  what actually happened, but only in CI, after a tag has been cut and a release
  published.
* This test catches the rewritten file in the working tree, before the commit.
  It also catches the case `publish.yml` structurally cannot: a stale but
  PLAUSIBLE static version (`0.9.1.post208.dev0+...`), which passes the `0.0.0`
  check and would publish a mis-versioned artifact to PyPI, where a version can
  never be reused.
"""
import re
from pathlib import Path

import pytest

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"

_HINT = (
    "\n\nThis is what poetry-dynamic-versioning leaves behind when a build is "
    "killed before it can restore the file.\n"
    "Restore all three together:\n"
    '    dynamic = ["version"]        under [project]\n'
    '    version = "0.0.0"            under [tool.poetry]\n'
    "    enable = true                under [tool.poetry-dynamic-versioning]\n"
    "Reverting only some of them still leaves the project unable to version "
    "itself, and the failure surfaces in CI after the tag is already cut."
)


@pytest.fixture(scope="module")
def text():
    if not PYPROJECT.exists():
        pytest.skip("pyproject.toml absent (installed package, not a checkout)")
    return PYPROJECT.read_text()


def test_version_is_declared_dynamic(text):
    m = re.search(r'^dynamic\s*=\s*(\[[^\]]*\])', text, re.M)
    assert m, "no top-level `dynamic =` line in [project]" + _HINT
    assert "version" in m.group(1), (
        f"[project] dynamic = {m.group(1)}, which does not include \"version\", "
        "so the version is NOT derived from the git tag." + _HINT
    )


def _section(text, header):
    """Body of a top-level TOML table up to the next ``[...]`` header."""
    parts = text.split(f"\n{header}\n", 1)
    if len(parts) < 2:
        return ""
    body = parts[1]
    nxt = re.search(r"^\[", body, re.M)
    return body[: nxt.start()] if nxt else body


def test_placeholder_lives_under_tool_poetry_not_project(text):
    """poetry-dynamic-versioning (PEP 621 mode) substitutes ``[tool.poetry]
    version`` and requires ``[project]`` to carry NO static version -- only
    ``dynamic = ["version"]``.  v0.11.0's first publish run resolved 0.0.0
    because a restoration put the placeholder under ``[project]``; the line
    existed, the section was wrong, and the older checks here passed."""
    assert re.search(r'^version\s*=\s*"0\.0\.0"', _section(text, "[tool.poetry]"), re.M), (
        "no `version = \"0.0.0\"` under [tool.poetry]" + _HINT)
    assert not re.search(r"^version\s*=", _section(text, "[project]"), re.M), (
        "[project] carries a static `version =`; with dynamic = [\"version\"] the "
        "plugin then resolves 0.0.0 (publish run 34193399219, 2026-09-08)" + _HINT)


def test_static_version_is_the_placeholder(text):
    m = re.search(r'^version\s*=\s*"([^"]*)"', text, re.M)
    assert m, "no top-level `version =` line found" + _HINT
    assert m.group(1) == "0.0.0", (
        f'version = "{m.group(1)}", not the "0.0.0" placeholder.  Publishing '
        "this would put a mis-versioned artifact on PyPI, and publish.yml's "
        "guard only catches the 0.0.0 case, not this one." + _HINT
    )


def test_dynamic_versioning_is_enabled(text):
    block = text.split("[tool.poetry-dynamic-versioning]", 1)
    assert len(block) == 2, "no [tool.poetry-dynamic-versioning] section" + _HINT
    m = re.search(r'^enable\s*=\s*(\w+)', block[1], re.M)
    assert m, "no `enable =` in [tool.poetry-dynamic-versioning]" + _HINT
    assert m.group(1) == "true", (
        f"poetry-dynamic-versioning is enable = {m.group(1)}.  The build will "
        'resolve the static "0.0.0" instead of the git tag -- exactly the '
        "failure publish.yml reports as \"Dynamic version resolved to 0.0.0\"."
        + _HINT
    )
