"""`pyproject.toml`'s static version must stay the `0.0.0` placeholder.

This project derives its version from the git tag via poetry-dynamic-versioning.
The plugin SUBSTITUTES the computed version into `pyproject.toml` during a build
and restores the placeholder afterwards -- so a build that is killed partway
(OOM, timeout, Ctrl-C) leaves the substituted value behind.  Commit that, and
the repository now carries a hardcoded, wrong, and silently stale version.

It happened: a `poetry build` killed on a 4-core Slurm slice left
`version = "0.9.1.post208.dev0+fcec3642"`, which was committed and tagged as
v0.10.1 before being caught (GH #741's PR).

`publish.yml` guards the OTHER direction -- it fails if the version resolves to
`0.0.0`, i.e. the plugin was inactive or tags were not fetched.  It cannot catch
this one: a stale-but-plausible version passes that check and would publish a
mis-versioned artifact to PyPI, where versions cannot be reused.

So the two guards are complements, not duplicates, and this is the missing half.
"""
import re
from pathlib import Path

import pytest

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


@pytest.mark.skipif(not PYPROJECT.exists(),
                    reason="pyproject.toml absent (installed package, not a checkout)")
def test_static_version_is_the_placeholder():
    text = PYPROJECT.read_text()
    m = re.search(r'^version\s*=\s*"([^"]*)"', text, re.M)
    assert m, "no top-level `version =` line found in pyproject.toml"
    assert m.group(1) == "0.0.0", (
        f'pyproject.toml has version = "{m.group(1)}", not the "0.0.0" '
        "placeholder.\n"
        "poetry-dynamic-versioning substitutes the real version during a build "
        "and restores the placeholder after; a KILLED build skips the restore.\n"
        "Fix: set it back to 0.0.0 before committing.  Shipping this value "
        "would publish a mis-versioned package, and publish.yml's guard only "
        "catches the 0.0.0 case, not this one."
    )
