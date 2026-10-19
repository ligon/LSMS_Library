"""No country build code may read a file the cache hash deliberately skips (GH #836).

``country._ORG_HASH_SKIP`` (today ``{"CONTENTS.org"}``) names the ``*.org``
files that ``Wave._input_hash`` / ``Country._table_cache_hash`` leave OUT of
the content hash, because they are documentation.  That exclusion is only
sound while nothing *builds* from them: a table read out of ``CONTENTS.org``
changes the served data without moving any hash, so the edit is silently
ignored by every warm cache.  ``Tanzania/_/food_acquired.py`` did exactly
that (``df_from_orgfile('CONTENTS.org','food_labels')``, line 34 on
development before GH #836) and this test would have failed there.

The scan is AST-based, not textual: dozens of country modules *mention*
``CONTENTS.org`` in comments and docstrings, which is what the file is for.
It flags any string literal outside a docstring whose basename is in
``_ORG_HASH_SKIP`` -- so ``df_from_orgfile('CONTENTS.org', ...)``,
``open('../_/CONTENTS.org')`` and ``Path(d) / 'CONTENTS.org'`` alike.

Scope: ``lsms_library/countries/**/*.py`` only.  ``Country.notes_path``
(``country.py``) returns the path as a documentation accessor and is not
build code.  Known gap: a path assembled dynamically (``'CONTENTS' + '.org'``)
is not caught.
"""
from __future__ import annotations

import ast
from pathlib import Path, PurePosixPath

from lsms_library.country import _ORG_HASH_SKIP

COUNTRIES = Path(__file__).resolve().parent.parent / "lsms_library" / "countries"


def _docstring_nodes(tree: ast.AST) -> set[int]:
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                ids.add(id(body[0].value))
    return ids


def skipped_org_literals(source: str) -> list[int]:
    """Line numbers of non-docstring string literals naming a hash-skipped file."""
    tree = ast.parse(source)
    docs = _docstring_nodes(tree)
    hits = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in docs
                and PurePosixPath(node.value.strip().replace("\\", "/")).name in _ORG_HASH_SKIP):
            hits.append(node.lineno)
    return sorted(hits)


def test_detector_fires_on_the_retired_tanzania_read():
    # The exact line GH #836 removed (Tanzania/_/food_acquired.py:34).
    src = "from lsms_library.local_tools import df_from_orgfile\n" \
          "fl = df_from_orgfile('CONTENTS.org','food_labels')\n"
    assert skipped_org_literals(src) == [2]
    assert skipped_org_literals("open('../_/CONTENTS.org')") == [1]
    assert skipped_org_literals("from pathlib import Path\nPath('x') / 'CONTENTS.org'\n") == [2]


def test_detector_ignores_prose():
    src = '"""See _/CONTENTS.org for the design."""\n' \
          "# read CONTENTS.org first\n" \
          "def f():\n    'see CONTENTS.org'\n    return 1\n"
    assert skipped_org_literals(src) == []


def test_no_country_code_reads_a_hash_skipped_org_file():
    assert _ORG_HASH_SKIP, "expected at least CONTENTS.org to be hash-skipped"
    hits, unparsed = [], []
    for py in sorted(COUNTRIES.rglob("*.py")):
        try:
            lines = skipped_org_literals(py.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError) as exc:
            unparsed.append(f"{py.relative_to(COUNTRIES)}: {exc.__class__.__name__}")
            continue
        hits += [f"{py.relative_to(COUNTRIES)}:{n}" for n in lines]
    assert not hits and not unparsed, (
        f"Country code references a file excluded from the cache hash "
        f"({sorted(_ORG_HASH_SKIP)}); a table read from it would never "
        f"invalidate a cache (GH #836).  Move the table to a hashed .org "
        f"(e.g. categorical_mapping.org).\n  hits: {hits}\n  unparsed: {unparsed}"
    )
