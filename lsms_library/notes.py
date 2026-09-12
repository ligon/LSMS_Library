"""Read a country's ``CONTENTS.org`` -- the per-country idiosyncrasy record.

Why this exists
---------------
``countries/{C}/_/CONTENTS.org`` is where this repository records what is odd
about a survey: identifier conventions, design quirks, known defects, and
decisions already taken with their reasons.  It is the first thing
``CLAUDE.md`` tells a contributor to read.  Until now it was reachable only by
navigating the filesystem, so anyone working through the API could not see it
-- the knowledge existed and the retrieval path did not.

Parsing rules, and why they are conservative
--------------------------------------------
**TODO keywords are taken from the file when it declares them.**  Org itself
only treats ``TODO`` and ``DONE`` as keywords unless a ``#+TODO:`` line (or the
reader's global config) says otherwise, so a file that declares its own states
is the only one that can be read unambiguously by anyone.  When a file does not
declare them, :data:`DEFAULT_STATES` applies.

**A headline's first word is a keyword only if it is in that set.**  Measured
across the corpus, the first all-caps word of a headline is a genuine state in
124 cases (``DONE`` 82, ``TODO`` 39, ``WAITING`` 3) and is *not* one in about
thirty others -- ``GH``, ``WARNING``, ``TLSS``, ``ENV``, ``GPS``, ``BID``,
``WHERE``, ``SOURCE``, ``PSU``.  A permissive "leading all-caps word" rule
would invent states for every one of those, so it is an allowlist.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = ["Headline", "DEFAULT_STATES", "todo_keywords", "parse", "extract"]

#: States assumed when a file declares no ``#+TODO:`` line.  Deliberately the
#: three with documented meaning in this repo, not every capitalised word.
DEFAULT_STATES = frozenset({"TODO", "DONE", "WAITING"})

_HEADLINE = re.compile(r"^(\*+)\s+(.*?)\s*$")
_TODO_LINE = re.compile(r"^#\+TODO:\s*(.+)$", re.IGNORECASE)


@dataclass(frozen=True)
class Headline:
    """One org headline: its depth, TODO state (if any), text, and line index."""

    level: int
    keyword: str | None
    text: str
    line: int

    def __str__(self) -> str:
        star = "*" * self.level
        kw = f"{self.keyword} " if self.keyword else ""
        return f"{star} {kw}{self.text}"


def todo_keywords(text: str) -> frozenset[str]:
    """States this document declares, else :data:`DEFAULT_STATES`.

    A ``#+TODO:`` line lists active states, optionally ``|``-separated from the
    done states; both sides count as keywords.
    """
    for line in text.splitlines():
        m = _TODO_LINE.match(line)
        if m:
            words = m.group(1).replace("|", " ").split()
            found = {w.strip() for w in words if w.strip() and w.strip() != "|"}
            if found:
                return frozenset(found)
    return DEFAULT_STATES


def parse(text: str) -> list[Headline]:
    """Every headline in ``text``, in document order."""
    states = todo_keywords(text)
    out: list[Headline] = []
    for i, line in enumerate(text.splitlines()):
        m = _HEADLINE.match(line)
        if not m:
            continue
        stars, rest = m.group(1), m.group(2)
        keyword = None
        parts = rest.split(None, 1)
        if parts and parts[0] in states:
            keyword = parts[0]
            rest = parts[1] if len(parts) > 1 else ""
        out.append(Headline(len(stars), keyword, rest, i))
    return out


def _subtree_end(lines: list[str], heads: list[Headline], idx: int) -> int:
    """Line index at which headline ``heads[idx]``'s subtree ends (exclusive)."""
    level = heads[idx].level
    for later in heads[idx + 1:]:
        if later.level <= level:
            return later.line
    return len(lines)


def extract(text: str, topic: str | None = None,
            state: str | None = None) -> str:
    """Return the sections matching ``topic`` and/or ``state``, with subtrees.

    ``topic`` matches the headline **text** case-insensitively as a substring;
    body text is not searched.  ``state`` matches the TODO keyword exactly.
    With neither, the whole document is returned unchanged.

    A match returns the headline and everything nested beneath it, because the
    useful content is nested -- a country's ``Weights`` and ``Strata`` sit
    under its ``Sampling Design``.  When a parent and a descendant both match,
    only the parent is returned: it already contains the descendant, and
    emitting both would duplicate it.
    """
    if topic is None and state is None:
        return text

    lines = text.splitlines()
    heads = parse(text)
    needle = topic.lower() if topic else None

    spans: list[tuple[int, int]] = []
    covered_to = -1
    for i, h in enumerate(heads):
        if needle is not None and needle not in h.text.lower():
            continue
        if state is not None and h.keyword != state:
            continue
        if h.line < covered_to:      # already inside a returned subtree
            continue
        end = _subtree_end(lines, heads, i)
        spans.append((h.line, end))
        covered_to = end

    if not spans:
        return ""
    return "\n".join("\n".join(lines[a:b]).rstrip() for a, b in spans) + "\n"
