#!/usr/bin/env python
"""Audit the *discoverable* API surface: undocumented kwargs, uncalled machinery.

Why this exists
---------------
The library keeps getting features reinvented that already exist -- a
``unit_values()`` proposed beside the existing ``units=``, a currency
blocklist proposed beside the existing ``currency=``, a median price ladder
designed beside the existing :func:`transformations.median_price_valuation`.
Each time, the thing already existed and the only retrieval path was somebody
remembering it.

Two blind spots cause it, and this script measures both:

1. **The data methods are generated, so mkdocstrings cannot see them.**
   ``Country.food_prices`` and friends are synthesised in ``__getattr__`` from
   ``data_scheme``, so no docstring exists for a doc generator to compile.
   Their *shared kwarg surface* is therefore invisible on the docs site --
   measured at first run: 6 of 9 kwargs appeared nowhere in ``docs/``.

2. **Machinery with no call site is invisible to grep.**  The usual way to
   discover a helper is to find something calling it.  A function nothing
   calls yet -- built ahead of its consumer, or orphaned by a refactor -- is
   exactly the reusable code most likely to be rebuilt from scratch, and it
   is precisely what a use-site search cannot find.

Both checks read the code, never a hand-maintained list, so neither can rot
into agreeing with a stale copy of itself.
"""
from __future__ import annotations

import argparse
import ast
import re
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs"

#: Modules whose public functions should have a discoverable call site.
MACHINERY_MODULES = ("lsms_library/transformations.py", "lsms_library/local_tools.py")

#: Skill trees that are generated or vendored, not hand-authored here.
SKILL_EXCLUDE = ("gitnexus", "generated")

#: Directories that are copies of the tree, not the tree.
EXCLUDE_PARTS = {".claude", ".venv", "worktrees", "__pycache__", "build", "dist"}


def _tree_files() -> list[Path]:
    """Tracked ``.py`` files, via ``git ls-files``.

    Deliberately not ``rglob``: on a networked filesystem a full-tree walk is
    slow, and ``git ls-files`` excludes worktrees, the venv and build output
    without needing a hand-maintained ignore list.
    """
    import subprocess
    out = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                         capture_output=True, text=True, check=True).stdout
    return [REPO / line for line in out.splitlines() if line]


_WORD = re.compile(r"\b[A-Za-z_]\w*\b")
_DEF = re.compile(r"^\s*(?:async\s+)?def\s+(\w+)")


def _identifier_census() -> tuple[Counter, Counter]:
    """One pass over the tree: ``(all identifier uses, uses that are a def)``.

    Counting every identifier once is what makes this affordable -- the naive
    form re-scans the tree per symbol, which is quadratic and took over eight
    minutes here before timing out.
    """
    uses: Counter = Counter()
    defs: Counter = Counter()
    for p in _tree_files():
        try:
            text = p.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for line in text.splitlines():
            m = _DEF.match(line)
            if m:
                defs[m.group(1)] += 1
            uses.update(_WORD.findall(line))
    return uses, defs


def data_method_kwargs() -> list[str]:
    """Kwarg names of the generated data-method dispatcher, read from source.

    Locates the nested ``def method(...)`` inside ``Country.__getattr__`` --
    the function every generated data method (``food_prices``, ``sample``,
    ``household_roster``, ...) actually is.
    """
    tree = ast.parse((REPO / "lsms_library" / "country.py").read_text())
    for cls in ast.walk(tree):
        if not (isinstance(cls, ast.ClassDef) and cls.name == "Country"):
            continue
        for ga in ast.walk(cls):
            if not (isinstance(ga, ast.FunctionDef) and ga.name == "__getattr__"):
                continue
            best: list[str] = []
            for fn in ast.walk(ga):
                if isinstance(fn, ast.FunctionDef) and fn.name == "method":
                    names = [a.arg for a in fn.args.args if a.arg != "self"]
                    names += [a.arg for a in fn.args.kwonlyargs]
                    if len(names) > len(best):
                        best = names
            if best:
                return best
    raise SystemExit("FATAL: could not locate the data-method dispatcher in country.py")


def skills() -> list[tuple[str, str]]:
    """``(relative skill path, description)`` for every hand-authored skill.

    The description comes from the SKILL.md frontmatter, which is where a
    skill states when it should be read -- the very thing a reader needs in
    order to know it exists.
    """
    out = []
    root = REPO / ".claude" / "skills"
    for f in sorted(root.rglob("SKILL.md")):
        rel = f.relative_to(root).parent.as_posix()
        if any(part in SKILL_EXCLUDE for part in f.parts):
            continue
        desc = ""
        try:
            text = f.read_text()
        except OSError:
            text = ""
        if text.startswith("---"):
            fm = text.split("---", 2)[1] if text.count("---") >= 2 else ""
            for line in fm.splitlines():
                if line.startswith("description:"):
                    desc = line.split(":", 1)[1].strip()
                    break
        out.append((rel, desc))
    return out


def check_skills(verbose: bool = True) -> list[str]:
    """Skills that AGENTS.md never mentions -- i.e. undiscoverable by reading it.

    A skill nobody can find is a skill nobody reads.  This is not
    hypothetical: `add-feature/food-acquired/aggregate-labels` documents the
    `labels='Aggregate'` contract and appeared nowhere in AGENTS.md, so an
    investigation into exactly that contract was carried out without it and
    had to be corrected afterwards.
    """
    agents = (REPO / "AGENTS.md")
    text = agents.read_text() if agents.exists() else ""
    found = skills()
    missing = [rel for rel, _ in found if rel not in text]
    if verbose:
        print(f"\nskills on disk (excluding {'/'.join(SKILL_EXCLUDE)}): {len(found)}")
        for rel, desc in found:
            mark = "MISSING " if rel in missing else "listed  "
            head = (desc[:78] + "...") if len(desc) > 78 else desc
            print(f"  {mark} {rel}")
            if head:
                print(f"           {head}")
    return missing


def _docs_text() -> str:
    """All prose docs, excluding release notes (historical, not reference)."""
    out = []
    for p in DOCS.rglob("*.md"):
        if "releases" in p.parts or "migration" in p.parts:
            continue
        out.append(p.read_text())
    return "\n".join(out)


def check_kwargs(verbose: bool = True) -> list[str]:
    """Return kwargs that appear nowhere in the reference docs."""
    kwargs = data_method_kwargs()
    text = _docs_text()
    missing = [k for k in kwargs if f"{k}=" not in text]
    if verbose:
        print(f"data-method kwargs (from country.py): {len(kwargs)}")
        for k in kwargs:
            print(f"  {'MISSING ' if k in missing else 'ok      '} {k}=")
    return missing


def public_functions(relpath: str) -> dict[str, int]:
    """``{name: lineno}`` for public module-level functions."""
    tree = ast.parse((REPO / relpath).read_text())
    return {
        n.name: n.lineno
        for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and not n.name.startswith("_")
    }


def check_unused(verbose: bool = True) -> list[tuple[str, str, int]]:
    """Public machinery whose only appearance in the tree is its own ``def``."""
    uses, defs = _identifier_census()
    found = []
    for rel in MACHINERY_MODULES:
        for name, lineno in sorted(public_functions(rel).items()):
            # every ``def`` line also contributes one identifier use
            references = uses[name] - defs[name]
            if references <= 0:
                found.append((rel, name, lineno))
    if verbose:
        if found:
            print(f"\npublic machinery with NO call site ({len(found)}):")
            for rel, name, lineno in found:
                print(f"  {rel}:{lineno}  {name}()")
            print("\n  Not necessarily dead -- may be built ahead of its consumer,")
            print("  or public API for downstream users.  It IS the code most")
            print("  likely to be reinvented, because no use-site search finds it.")
        else:
            print("\npublic machinery with no call site: none")
    return found


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--kwargs", action="store_true", help="only the kwarg check")
    ap.add_argument("--unused", action="store_true", help="only the call-site report")
    ap.add_argument("--skills", action="store_true", help="only the skill index")
    ap.add_argument("--strict", action="store_true",
                    help="exit 1 if any kwarg is undocumented")
    args = ap.parse_args()
    both = not (args.kwargs or args.unused or args.skills)

    missing: list[str] = []
    if both or args.kwargs:
        missing = check_kwargs()
    if both or args.unused:
        check_unused()
    missing_skills: list[str] = []
    if both or args.skills:
        missing_skills = check_skills()

    rc = 0
    if missing:
        print(f"\n{len(missing)} undocumented kwarg(s): {', '.join(missing)}")
        print("Document them in docs/guide/data-methods.md.")
        rc = 1
    if missing_skills:
        print(f"\n{len(missing_skills)} skill(s) not mentioned in AGENTS.md: "
              f"{', '.join(missing_skills)}")
        print("Add them to the skills list; a skill nobody can find is a skill "
              "nobody reads.")
        rc = 1
    return rc if args.strict else 0


if __name__ == "__main__":
    sys.exit(main())
