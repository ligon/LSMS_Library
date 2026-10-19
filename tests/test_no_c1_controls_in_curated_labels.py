"""No C1 control code point (U+0080-U+009F) may reach a SERVED label from
curated org config (GH #926).

A C1 control in text is never intentional: it is the signature of cp1252
bytes decoded as latin-1 (U+0092 for a right single quote, U+0096 for an en
dash, U+0094 for a right double quote).  GhanaLSS `nonfood_items` carried 28
such cells in `Preferred Label` / `Label_2016-17`, and the 2016-17
`harmonize_price_item` three more, so `nonfood_expenditures.j` served 25,585
rows and `community_prices.j` / `.Description` 927 rows with the control
embedded.  The reader-side fix in GH #805 (cp1252-first for pre-118 `.dta`)
cannot clear a code point that is COMMITTED in config -- and would not have
prevented these either, because `g7sec9a.dta` is format 118 and its string
data mixes raw cp1252 bytes with clean ASCII for the same words (see
`GhanaLSS/_/CONTENTS.org`).

Scope, and why it is narrow
---------------------------
Two assertions, deliberately not a whole-corpus ban on C1 in `.org` files:

1. Every `Preferred Label` column of every `#+name:` table under
   `countries/*/_/`, `countries/*/*/_/` and `lsms_library/categorical_mapping/`
   is C1-free.  `Preferred Label` is the column that becomes a served `j`
   (or other label axis); nothing else in an org table is served verbatim.

2. The two GhanaLSS files are C1-free ANYWHERE.  They were the only files in
   which a C1 reached a served label, and this pins the whole repair rather
   than the served column alone.

3. (GH #805, widening 1 as this docstring asked.)  No cell of ANY table in
   those files -- key columns included -- carries a cp1252-class C1 code
   point.  The reader re-decodes cp1252 strings of pre-118 `.dta` files
   since #805, so a key spelled with that damage now matches nothing; the
   C1-keyed rows it re-keyed were Mali `harmonize_food` `Code` (2 cells),
   Niger `harmonize_food` `Original Label` (2), Burkina_Faso `harmonize_food`
   `Original Label` (3) and Uganda's `u` wave-label column (4, inert -- `u`
   is keyed on `Code`).  A C1 code point is admitted only INSIDE a UTF-8
   double encoding (UTF-8 bytes read as latin-1, e.g. ``b\xc5\x93uf``),
   which #805 deliberately leaves alone and which is still load-bearing as
   a key (Burkina_Faso's 2018-19 beef row; `Togo/2018/_/notes.org`
   documents the same spelling).  That is a rule about the CLASS of the
   string, not an allowlist of cells.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import lsms_library
from lsms_library.paths import countries_root

C1 = re.compile("[\u0080-\u009f]")
GHANALSS_FILES = (
    "GhanaLSS/_/categorical_mapping.org",
    "GhanaLSS/2016-17/_/categorical_mapping.org",
)


def _read(f: Path) -> str:
    """UTF-8, falling back to latin-1: `GhanaSPS/_/units.org` and
    `Senegal/_/units.org` are not UTF-8 (ISO-8859 bytes, e.g. 0xA0), a
    separate defect this test reports on but does not adjudicate -- latin-1
    maps every byte, so the scan still runs and no 0x80-0x9F byte hides."""
    raw = f.read_bytes()
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def _curated_org_files() -> list[Path]:
    root = countries_root()
    files = sorted(root.glob("*/_/*.org")) + sorted(root.glob("*/*/_/*.org"))
    files += sorted((Path(lsms_library.__file__).parent / "categorical_mapping").glob("*.org"))
    return [f for f in files if f.name != "CONTENTS.org"]


def _preferred_label_cells(text: str):
    """Yield ``(lineno, cell)`` for every `Preferred Label` cell of every
    named table in an org file."""
    col = None
    for lineno, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if s.startswith("#+name:"):
            col = None            # a new table; header not yet seen
            continue
        if not s.startswith("|"):
            col = None if col is None else col
            if s and not s.startswith("#"):
                col = None        # left the table
            continue
        if s.startswith("|-"):
            continue
        cells = [c.strip() for c in s.strip("|").split("|")]
        if col is None:
            for k, c in enumerate(cells):
                if c.lower() == "preferred label":
                    col = k
                    break
            continue              # header row (or a table with no such column)
        if col < len(cells):
            yield lineno, cells[col]


def test_no_c1_control_in_any_preferred_label_column():
    offenders = []
    for f in _curated_org_files():
        text = _read(f)
        if not C1.search(text):
            continue
        for lineno, cell in _preferred_label_cells(text):
            if C1.search(cell):
                offenders.append(f"{f.relative_to(countries_root().parent)}:{lineno}: {cell!r}")
    assert not offenders, (
        "C1 control code points in a served Preferred Label "
        "(cp1252 bytes decoded as latin-1, GH #926):\n  " + "\n  ".join(offenders))


@pytest.mark.parametrize("rel", GHANALSS_FILES)
def test_ghanalss_org_config_is_c1_free_everywhere(rel):
    text = (countries_root() / rel).read_text(encoding="utf-8")
    hits = [(i, l) for i, l in enumerate(text.splitlines(), 1) if C1.search(l)]
    assert not hits, f"{rel}: {len(hits)} line(s) carry a C1 control: {hits[:5]}"


def test_scanner_sees_the_known_tables():
    """The scanner must actually find Preferred Label columns, or assertion 1
    is vacuous."""
    text = (countries_root() / GHANALSS_FILES[0]).read_text(encoding="utf-8")
    cells = [c for _, c in _preferred_label_cells(text)]
    assert "Charcoal - maxibag" in cells
    assert "Women's dress shoes" in cells
    text = (countries_root() / GHANALSS_FILES[1]).read_text(encoding="utf-8")
    cells = [c for _, c in _preferred_label_cells(text)]
    assert "Women's polyester skirt" in cells          # harmonize_price_item code 309


def _is_double_encoding(s: str) -> bool:
    """True when *s* is UTF-8 bytes read as latin-1 -- the one class whose C1
    code points are a UTF-8 continuation byte rather than cp1252 damage."""
    try:
        return s.encode("latin-1").decode("utf-8") != s
    except (UnicodeEncodeError, UnicodeDecodeError):
        return False


def test_no_cp1252_damage_in_any_table_cell():
    """GH #805: key columns too.  Every C1-bearing cell of every org table row
    must be a UTF-8 double encoding; a cp1252-class C1 (what the latin-1 read
    of a cp1252 file used to emit) is now unreachable as a key."""
    offenders = []
    for f in _curated_org_files():
        text = _read(f)
        if not C1.search(text):
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if not line.strip().startswith("|") or not C1.search(line):
                continue
            for cell in line.strip().strip("|").split("|"):
                cell = cell.strip()
                if C1.search(cell) and not _is_double_encoding(cell):
                    offenders.append(f"{f.relative_to(countries_root().parent)}:{lineno}: {cell!r}")
    assert not offenders, (
        "cp1252-class C1 code points in a curated org table (re-key to the "
        "decoded spelling the reader now emits, GH #805):\n  " + "\n  ".join(offenders))


def test_double_encoding_exemption_is_not_vacuous():
    """The exemption must still be needed (or the widening above is the whole
    rule and this exemption should go): Burkina_Faso keys its 2018-19 beef on
    the UTF-8 double encoding, and that row is C1-bearing by construction."""
    text = (countries_root() / "Burkina_Faso/_/categorical_mapping.org").read_text(encoding="utf-8")
    cells = [c.strip() for line in text.splitlines() if line.strip().startswith("|")
             for c in line.strip().strip("|").split("|")]
    double = [c for c in cells if C1.search(c) and _is_double_encoding(c)]
    assert double == ["Viande de b\u00c5\u0093uf"], double
