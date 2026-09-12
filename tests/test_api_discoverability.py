"""The docs must document the API the code actually exposes.

Regression net for a failure that is invisible by construction: the data
methods (``food_prices``, ``household_roster``, ...) are synthesised in
``Country.__getattr__``, so mkdocstrings cannot introspect them and their
keyword arguments silently never reach the docs site.  Measured when this
test was written: **6 of the 9 kwargs** -- ``age_cuts``, ``units``,
``volume_as_mass``, ``currency``, ``numeraire``, ``basis`` -- appeared
nowhere in ``docs/``.  Four of them were independently reinvented in design
discussion because nobody could find them.

Nothing here hardcodes the kwarg list: it is read from ``country.py`` by AST,
so adding a kwarg without documenting it fails, and renaming one cannot leave
this test quietly asserting the old name.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from api_surface_audit import check_kwargs, data_method_kwargs  # noqa: E402


def test_dispatcher_kwargs_are_discoverable():
    """Every data-method kwarg appears somewhere in the reference docs."""
    missing = check_kwargs(verbose=False)
    assert not missing, (
        f"Undocumented data-method kwarg(s): {', '.join(missing)}.\n"
        "These are invisible to mkdocstrings (the methods are generated at\n"
        "attribute-access time), so an undocumented kwarg is effectively\n"
        "undiscoverable.  Run `make docs-gen` to regenerate\n"
        "docs/guide/data-methods.md from the live docstrings."
    )


def test_kwarg_list_is_read_from_source_not_hardcoded():
    """The audit finds a plausible dispatcher, not an empty or stale list."""
    kwargs = data_method_kwargs()
    assert len(kwargs) >= 5, f"suspiciously few kwargs found: {kwargs}"
    for expected in ("waves", "labels"):
        assert expected in kwargs, f"{expected!r} missing -- dispatcher moved?"


def test_generated_data_methods_page_is_current():
    """The committed page matches what the generator produces now."""
    page = REPO / "docs" / "guide" / "data-methods.md"
    assert page.exists(), "docs/guide/data-methods.md missing; run `make docs-gen`"
    before = page.read_text()
    proc = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "gen_data_method_docs.py")],
        cwd=REPO, capture_output=True, text=True,
        env={**__import__("os").environ, "PYTHONPATH": str(REPO)},
    )
    if proc.returncode != 0:
        pytest.skip(f"generator could not run here: {proc.stderr[-300:]}")
    after = page.read_text()
    if before != after:
        page.write_text(before)  # leave the tree as we found it
        pytest.fail(
            "docs/guide/data-methods.md is stale relative to the docstrings in "
            "country.py.  Run `make docs-gen` and commit the result."
        )


def test_every_skill_is_named_in_agents_md():
    """A skill nobody can find is a skill nobody reads.

    `.claude/skills/add-feature/food-acquired/aggregate-labels/` documents the
    `labels='Aggregate'` contract and appeared NOWHERE in AGENTS.md; an
    investigation into exactly that contract (GH #787) was carried out without
    it and had to be corrected afterwards.  At the time only 6 of 15 skills
    were named by path.  The catalog is hand-maintained, so it is checked.
    """
    from api_surface_audit import check_skills, skills  # noqa: E402

    found = skills()
    assert len(found) >= 10, f"skill discovery looks broken: {found}"
    missing = check_skills(verbose=False)
    assert not missing, (
        "skill(s) absent from AGENTS.md: " + ", ".join(missing) +
        "\nAdd them to the 'Task-Specific Skills' list."
    )


def test_skill_descriptions_are_available_for_indexing():
    """Each skill states when to read it; that text is what makes it findable."""
    from api_surface_audit import skills  # noqa: E402

    undocumented = [rel for rel, desc in skills() if not desc.strip()]
    assert not undocumented, (
        "skill(s) with no `description` in frontmatter: "
        + ", ".join(undocumented)
    )


def test_no_hashed_build_input_is_untracked():
    """A cache-hash input git does not track makes checkouts disagree.

    GH #790: a generated `Uganda/_/fct_usda.csv` was gitignored by the blanket
    `*.csv` rule yet hashed via `_BUILD_INPUT_SUFFIXES`, so two clean checkouts
    of the same commit differed on all 28 of Uganda's cache hashes -- silently,
    because a gitignored file never shows in `git status`.  Found only while
    debugging something unrelated.
    """
    from api_surface_audit import check_untracked_inputs  # noqa: E402

    bad = check_untracked_inputs(verbose=False)
    assert not bad, (
        "hashed build input(s) not tracked by git:\n  " + "\n  ".join(bad) +
        "\nTrack the file, or stop generating it into a country's `_/`."
    )
