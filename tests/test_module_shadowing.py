"""A public name that shadows a submodule name -- GH #883.

``lsms_library/__init__.py`` binds ``recall`` and ``derivations`` to the public
TABLE FUNCTIONS, while ``lsms_library.recall`` and ``lsms_library.derivations``
are also submodules.  Python's ``from <package> import <name>`` prefers an
already-bound package attribute over the submodule, so after the rebind::

    from lsms_library import recall     # the FUNCTION
    recall.recall_records              # AttributeError

``sys.modules["lsms_library.recall"]`` is still the module; only the attribute
is rebound.

**The issue recorded this as latent ("only ``__init__.py`` itself does the
module-form import today ... the trap is latent for the next module").  It is
not latent -- it is live for every caller OUTSIDE the package**, which is how
it was hit a second time, from a one-off script that wrote
``from lsms_library import recall`` and got an ``AttributeError`` on
``recall_records``.  An in-package scan alone would not have caught that, so
these tests pin the escape hatches as well as the ban.

Why not just rename something?  Both renames were considered and declined in
GH #883: the public ``ll.recall()`` / ``ll.derivations()`` names are the
documented API, and renaming the submodules would churn every internal import.
The cheap option -- document it, and make a THIRD shadow a deliberate act
rather than an accident -- is what this file enforces.

The shadowed set is DISCOVERED, never hardcoded: a new shadow fails
``test_shadowed_names_are_exactly_the_known_two`` instead of silently joining
the trap.
"""
from __future__ import annotations

import ast
import importlib
import pkgutil
import sys
from pathlib import Path

import pytest

import lsms_library as ll

#: The names GH #883 signed off on, with their reason. Anything else that
#: shadows a submodule is an accident until someone adds it here on purpose.
KNOWN_SHADOWS = {"recall", "derivations"}

PACKAGE_ROOT = Path(ll.__file__).resolve().parent


def _submodule(name: str):
    """The real module object for ``lsms_library.<name>``, or None."""
    mod = sys.modules.get(f"lsms_library.{name}")
    if mod is not None:
        return mod
    try:
        return importlib.import_module(f"lsms_library.{name}")
    except Exception:
        return None


def discover_shadowed() -> set[str]:
    """Every submodule name the package rebinds to something else.

    Runtime, not AST: the question is what ``from lsms_library import x``
    actually yields, and only the live attribute answers that.
    """
    out = set()
    for info in pkgutil.iter_modules([str(PACKAGE_ROOT)]):
        name = info.name
        if not hasattr(ll, name):
            continue
        mod = _submodule(name)
        if mod is not None and getattr(ll, name) is not mod:
            out.add(name)
    return out


def test_shadowed_names_are_exactly_the_known_two():
    """A third shadow must be a decision, not a surprise."""
    found = discover_shadowed()
    assert found == KNOWN_SHADOWS, (
        f"shadowed submodule names changed: {sorted(found)} != "
        f"{sorted(KNOWN_SHADOWS)}.\n"
        "If you deliberately bound a public name over a submodule, add it to "
        "KNOWN_SHADOWS and document it at the rebind site in "
        "lsms_library/__init__.py. If you did not, you have created the GH "
        "#883 trap for a new name -- rename the public symbol instead."
    )


def test_the_shadow_is_real_and_this_is_what_it_looks_like():
    """Pin the surprising behaviour itself, so it is documented, not folklore."""
    for name in sorted(KNOWN_SHADOWS):
        attr = getattr(ll, name)
        assert callable(attr), f"ll.{name} should be the public table callable"
        mod = _submodule(name)
        assert mod is not None, f"lsms_library.{name} should still be a module"
        assert attr is not mod, (
            f"ll.{name} is the module again -- if the shadow was removed on "
            "purpose, drop it from KNOWN_SHADOWS and simplify the comment in "
            "__init__.py"
        )
        # `sys.modules` is untouched by the rebind; this is why the escape
        # hatches below work at all.
        assert sys.modules[f"lsms_library.{name}"] is mod


@pytest.mark.parametrize("name", sorted(KNOWN_SHADOWS))
def test_escape_hatches_reach_the_module(name):
    """The two forms the comment in __init__.py tells people to use."""
    by_importlib = importlib.import_module(f"lsms_library.{name}")
    assert by_importlib is _submodule(name)
    # `from lsms_library.<name> import <symbol>` resolves through sys.modules,
    # so it is unaffected by the attribute rebind.
    symbol = {"recall": "recall_records", "derivations": "derivation_records"}[name]
    imported = getattr(
        importlib.import_module(f"lsms_library.{name}"), symbol, None)
    assert imported is not None, (
        f"lsms_library.{name}.{symbol} should be importable by name")


def _in_package_modules():
    for path in PACKAGE_ROOT.rglob("*.py"):
        # `countries/` holds per-country config scripts, not package modules;
        # they run data reads at import, so they are read with ast, never
        # imported -- same rule as tests/test_derivations.py.
        if "countries" in path.relative_to(PACKAGE_ROOT).parts:
            continue
        yield path


def test_no_in_package_module_imports_a_shadowed_name_as_a_module():
    """``from . import recall`` inside the package yields the FUNCTION.

    Read with ``ast`` rather than by importing, so this cannot be defeated by
    import side effects.  ``__init__.py`` itself is exempt: its
    ``from . import recall as _recall_mod`` runs BEFORE the rebind and is the
    aliased form that keeps the module reachable.
    """
    offenders = []
    for path in _in_package_modules():
        if path.name == "__init__.py" and path.parent == PACKAGE_ROOT:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            # `from . import recall` -> module='' (relative), level=1
            if isinstance(node, ast.ImportFrom) and not node.module:
                for alias in node.names:
                    if alias.name in KNOWN_SHADOWS:
                        offenders.append(
                            f"{path.relative_to(PACKAGE_ROOT)}:{node.lineno} "
                            f"from . import {alias.name}")
    assert not offenders, (
        "these modules import a shadowed name in the module form, which "
        "yields the public FUNCTION rather than the module (GH #883):\n  "
        + "\n  ".join(offenders)
        + "\nImport the symbols by name instead: "
          "`from .recall import recall_records`."
    )
