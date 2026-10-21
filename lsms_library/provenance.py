"""Per-wave provenance: which source dataset a wave directory actually holds.

Every wave directory ``countries/{Country}/{wave}/`` records where its raw
data came from in ``Documentation/SOURCE.org``.  Historically that file held
nothing but a bare URL, which was enough for a human but not for code:

* Roughly half the recorded URLs are **DOIs** (``https://doi.org/10.48529/…``)
  rather than ``/catalog/{id}`` links, so no catalog id could be extracted.
* Nothing tied a wave directory to a specific World Bank catalog **id**, so
  :func:`lsms_library.data_access.discover_waves` had to guess which catalog
  entries we already held by reconstructing a wave *label* from the entry's
  year range (``year_start=2018, year_end=2019 -> "2018-19"``) and string-
  matching it against directory names.  Labels collide: ``Nigeria/2018-19``
  matched **both** the Living Standards Survey (id 3827) and the GHS-Panel
  Wave 4 (id 3557).  We hold W4; the label heuristic reported that we held
  the LSS.

This module makes provenance explicit and machine-readable.  It **extends**
``SOURCE.org`` (the file :func:`lsms_library.data_access.add_wave` already
writes) rather than introducing a parallel mechanism, by appending org-mode
``#+KEY: value`` keyword lines beneath the existing free-text URL body.  The
bare URL stays at the top of the file. Acquisition reuses this parser's
primary URL rule through ``data_access._read_source_url``; metadata and
notes cannot supply a missing primary URL.

Example::

    SOURCE

    https://microdata.worldbank.org/index.php/catalog/3557

    #+CATALOG_ID: 3557
    #+CATALOG_IDNO: NGA_2018_GHSP-W4_v03_M
    #+CATALOG_TITLE: General Household Survey, Panel 2018-2019, Wave 4
    #+CATALOG_YEARS: 2018-2019
    #+CATALOG_REPOSITORY: lsms
    #+CATALOG_URL: https://microdata.worldbank.org/index.php/catalog/3557
    #+PROVENANCE_SOURCE: worldbank
    #+PROVENANCE_METHOD: source-url
    #+PROVENANCE_RECORDED: 2026-07-12

Three ``PROVENANCE_SOURCE`` values are possible, and the distinction between
the last two matters:

``worldbank``
    The wave came from the WB Microdata Library and ``CATALOG_ID`` is the
    numeric catalog id.
``external``
    The wave came from somewhere that is definitively *not* the WB catalog
    (Ghana Statistical Service, Harvard Dataverse, …).  ``CATALOG_ID`` is
    ``none`` — we know there is no WB id to record.
``unknown``
    We do **not** know where this wave came from.  ``CATALOG_ID`` is
    ``unknown``.  This is recorded explicitly rather than left blank so that
    a gap in our knowledge is visible and greppable instead of silently
    defaulting to a confident-looking answer.

A wave directory is **not** in 1:1 correspondence with a catalog entry (GH
#600), in either direction, so two of the keys above are **lists**:

``#+CATALOG_ID: 2936, 2939``
    The directory holds the files of *several* catalog entries.
    ``Malawi/2016-17/`` is one: ``Data/Cross_Sectional/`` is the Fourth
    Integrated Household Survey (2936) and ``Data/Panel/`` is the 2016 wave of
    the Integrated Household Panel Survey (2939) — 97 of its 98 files appear in
    2939's World Bank datafile list and **none** in 2936's.  Recording only the
    first made ``discover_waves()`` report a study we hold as missing.

``#+CATALOG_COVERS: 76, 1050, 2252, 2862``
    The entry we hold **subsumes** the content of other entries whose files we
    do *not* hold.  ``Tanzania/2008-15/`` holds the Uniform Panel Dataset
    (3814) and nothing else — but the UPD's ``round`` column carries rounds
    1-4, i.e. the content of the four individual NPS rounds, which is why
    ``Country('Tanzania').waves`` exposes four waves out of one directory.
    Those four entries are *covered*, not *held*: listing them under
    ``CATALOG_ID`` would claim we hold five entries when we hold one — the same
    species of false claim, pointed the other way.

Relations *between catalog entries* (a study re-catalogued in a second WB
repository under a second id; a derived re-release built out of studies we
hold) are facts about the **catalog**, not about any one of our directories, so
they live in :mod:`lsms_library.catalog_relations` — see
``lsms_library/catalog_relations.yml``.

A catalog entry also has **releases** (``v01_M``, ``v02_M``, …), and the
record names two of them, under different keys (GH #1114 / #1128):

``#+CATALOG_IDNO:`` / ``#+CATALOG_DOI:``
    The release whose files the directory **holds**.  Moved only by an
    acquisition (``data_access.add_wave`` / ``populate_and_push``), never by a
    re-stamp from the catalog.
``#+CATALOG_LATEST_IDNO:`` / ``#+CATALOG_LATEST_DOI:`` / ``#+CATALOG_LATEST_SEEN:``
    The catalog's **newer** release, when it has one, and the date a re-stamp
    first saw it.  Absent when the latest is the release held.  So
    ``rg CATALOG_LATEST_IDNO lsms_library/countries`` is the queue of waves
    whose newer release has not been acquired.

Everything under :data:`NOTES_HEADING` belongs to a human and is carried
verbatim, ``#+KEY:`` lines included (GH #1115): ``GhanaSPS/2017-18`` records a
second, Dataverse vintage of ten files as ``#+SECONDARY_SOURCE*`` lines in the
middle of its notes.  Keywords are parsed only from the record above that
heading.
"""

from __future__ import annotations

import datetime as _dt
import logging
import re
from dataclasses import dataclass, field, replace
from pathlib import Path

logger = logging.getLogger(__name__)

# DOI prefix of the World Bank Microdata Library.  A SOURCE.org whose URL is
# a doi.org link with this prefix is a WB study whose numeric catalog id we
# have to look up (the DOI does not contain it).
WB_DOI_PREFIX = "10.48529"

WB_HOSTS = ("microdata.worldbank.org",)

# PROVENANCE_SOURCE values.
SOURCE_WORLDBANK = "worldbank"
SOURCE_EXTERNAL = "external"
SOURCE_UNKNOWN = "unknown"

# Sentinel CATALOG_ID values.  ``none`` = definitively not a WB catalog entry;
# ``unknown`` = we have not determined it.  Kept distinct on purpose.
_ID_NONE = "none"
_ID_UNKNOWN = "unknown"

# PROVENANCE_VALIDATION values.  A catalog id can be right for two quite
# different reasons, and the difference is worth recording rather than
# flattening: an id corroborated by the wave's own data files is a much
# stronger claim than one matched only on catalog metadata.  Nigeria/2018-19
# is precisely why -- its recorded id matched the year range perfectly and was
# still the wrong survey; only the filenames settled it.
VALIDATION_CONTENT = "content-validated"   # the wave's data files corroborate the id
VALIDATION_CATALOG_ONLY = "catalog-only"   # catalog metadata only; no local data to check

# ``[ \t]*``, not ``\s*``: under MULTILINE ``\s*`` spans a newline, so an
# empty-valued key swallowed the next line into its value and BOTH lines
# vanished from the render (GH #1115, red team r1).  A keyword's value ends at
# its own line, always.
_KEYWORD_RE = re.compile(r"^[ \t]*#\+([A-Z_]+):[ \t]*(.*?)[ \t]*$", re.MULTILINE)
_URL_RE = re.compile(r"https?://[^\s\]\)]+")
_CATALOG_ID_RE = re.compile(r"/catalog/(\d+)")

# Separator inside a list-valued keyword: ``#+CATALOG_ID: 2936, 2939``.  Commas
# or whitespace, so both ``2936,2939`` and ``2936 2939`` parse.
_LIST_SEP_RE = re.compile(r"[,\s]+")

# Human-written prose in a legacy SOURCE.org (e.g. the EthiopiaRHS round map)
# is preserved verbatim beneath this heading rather than being overwritten by
# the structured record.  Parsing it back out again makes the writer
# idempotent: re-running the backfill does not nest or duplicate the block.
NOTES_HEADING = "* Notes (preserved from the original SOURCE.org)"

# The body render_source_org writes when a record has no URL.  It is
# boilerplate, not prose: it used to be re-ingested as prose, so the next
# render carried it into the notes block (GH #1115; GhanaSPS/2017-18).
URL_PLACEHOLDER = "(source URL not recorded)"

# A line that carries no information beyond the URL itself: the literal
# "SOURCE" banner, a bare URL / org-mode [[link]], or the placeholder above.
_BOILERPLATE_RE = re.compile(
    r"^\s*(SOURCE|/?\[?\[?https?://[^\]\s]*\]?\]?|"
    + re.escape(URL_PLACEHOLDER) + r")\s*$", re.IGNORECASE)

# The catalog's newer release, recorded beside the held one (GH #1128).  A
# re-stamp writes and removes these; they are observations of the catalog, not
# facts about the directory, so removing them is not a deletion.
LATEST_KEYS = ("CATALOG_LATEST_IDNO", "CATALOG_LATEST_DOI",
               "CATALOG_LATEST_SEEN")

# Every keyword WaveProvenance has a field for.  Any other #+KEY: above the
# notes heading is carried in ``extra`` and rendered back after these.
_KNOWN_KEYS = frozenset({
    "CATALOG_ID", "CATALOG_COVERS", "CATALOG_IDNO", "CATALOG_TITLE",
    "CATALOG_YEARS", "CATALOG_REPOSITORY", "CATALOG_DOI", "CATALOG_URL",
    *LATEST_KEYS,
    "PROVENANCE_SOURCE", "PROVENANCE_METHOD", "PROVENANCE_VALIDATION",
    "PROVENANCE_RECORDED", "PROVENANCE_NOTE", "PROVENANCE_SUPERSEDED_URL"})

# Repeated scalar keywords still have a last-value accessor, but that must not
# make a direct parse/render erase the other recorded values (#1115; ledger
# section 4).  Keep their occurrences separately; a writer changing such a
# scalar must first resolve the ambiguity rather than silently drop facts.
_SCALAR_FIELDS = {
    "CATALOG_IDNO": "idno", "CATALOG_TITLE": "title",
    "CATALOG_YEARS": "years", "CATALOG_REPOSITORY": "repository",
    "CATALOG_DOI": "doi", "CATALOG_URL": "url",
    "CATALOG_LATEST_IDNO": "latest_idno", "CATALOG_LATEST_DOI": "latest_doi",
    "CATALOG_LATEST_SEEN": "latest_seen", "PROVENANCE_SOURCE": "source",
    "PROVENANCE_METHOD": "method", "PROVENANCE_VALIDATION": "validation",
    "PROVENANCE_RECORDED": "recorded", "PROVENANCE_NOTE": "note",
    "PROVENANCE_SUPERSEDED_URL": "superseded_url",
}


def _regions(text: str) -> tuple[list[str], list[str]]:
    """Split SOURCE.org into ``(record, notes)``: the lines above
    :data:`NOTES_HEADING` and the lines below it (``[]`` when it is absent)."""
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        if ln.strip() == NOTES_HEADING:
            return lines[:i], lines[i + 1:]
    return lines, []


def _body(record: list[str]) -> list[str]:
    """The free-text body: the record's lines above its first ``#+`` line."""
    out: list[str] = []
    for ln in record:
        if ln.lstrip().startswith("#+"):
            break
        out.append(ln)
    return out


def _primary_url(record: list[str]) -> str | None:
    """Explicit primary metadata, or the URL in the pre-metadata body."""
    values = [value for key, value in _KEYWORD_RE.findall("\n".join(record))
              if key == "CATALOG_URL" and value]
    if values:
        return values[-1]
    match = _URL_RE.search("\n".join(_body(record)))
    return match.group(0).rstrip("/") if match else None


def _tidy(lines: list[str]) -> list[str]:
    """Collapse runs of blank lines to one and trim blank lines at the ends,
    keeping the paragraph breaks a human wrote."""
    out: list[str] = []
    for ln in lines:
        if ln.strip():
            out.append(ln.rstrip())
        elif out and out[-1]:
            out.append("")
    while out and not out[-1]:
        out.pop()
    return out


def preserved_prose(text: str) -> str | None:
    """Extract human-written prose from a SOURCE.org, dropping boilerplate.

    Returns ``None`` when the file says nothing beyond its URL.  Idempotent:
    text already rendered by :func:`render_source_org` yields back exactly the
    prose it preserved.

    Two regions, two rules (GH #1115):

    * Above :data:`NOTES_HEADING` (all of a legacy file), a line is prose
      unless it is a ``#+KEY: value`` keyword -- those are the record -- or
      boilerplate (:data:`_BOILERPLATE_RE`). Only one bare primary URL is
      boilerplate; additional bare URL occurrences are preserved as notes.
    * Below it, every line is a human's and is kept verbatim, ``#+KEY:`` lines
      included (``GhanaSPS/2017-18``'s ``#+SECONDARY_SOURCE*`` lines sit in the
      middle of a paragraph that explains them).  The one exception is
      :data:`URL_PLACEHOLDER`, which only ever got there by being re-ingested.

    Paragraph breaks are kept in both (runs of blank lines collapse to one).
    """
    record, notes = _regions(text)
    primary = _primary_url(record)
    discarded_primary = False
    head = []
    for line in record:
        if _KEYWORD_RE.match(line):
            continue
        if _BOILERPLATE_RE.match(line):
            url = _URL_RE.search(line)
            if url is None:
                continue  # SOURCE banner or the no-URL placeholder
            if (not discarded_primary and primary
                    and url.group(0).rstrip("/") == primary.rstrip("/")):
                discarded_primary = True  # rendered as the primary body URL
                continue
            # Another bare URL is recorded evidence, not primary boilerplate.
        head.append(line)
    head = _tidy(head)
    tail = _tidy([ln for ln in notes if ln.strip() != URL_PLACEHOLDER])
    keep = head + [""] + tail if head and tail else head or tail
    return "\n".join(keep) if keep else None


def parse_id_list(value: str | None) -> list[str]:
    """Parse a list-valued catalog keyword into ids, order-preserving.

    Sentinels (``none`` / ``unknown``) and blanks yield ``[]``.  Duplicates are
    dropped, because a repeated id says nothing a single one does not.
    """
    if not value:
        return []
    out: list[str] = []
    for tok in _LIST_SEP_RE.split(value.strip()):
        tok = tok.strip()
        if not tok or tok.lower() in (_ID_NONE, _ID_UNKNOWN):
            continue
        if tok not in out:
            out.append(tok)
    return out


@dataclass
class WaveProvenance:
    """Provenance record for one ``countries/{country}/{wave}/`` directory.

    ``catalog_id`` is the **primary** entry — the one whose title, idno and URL
    the other ``CATALOG_*`` keys describe.  ``catalog_ids`` is the full list of
    entries whose files this directory holds; it is *not* always a singleton
    (GH #600).  The two are kept in sync: constructing with either one fills the
    other, so every pre-#600 caller (``WaveProvenance(catalog_id="3557")``) and
    every pre-#600 reader (``prov.catalog_id``) keeps working unchanged.

    ``covers`` is a different relation and must not be confused with it: entries
    whose *content* this directory's files subsume but whose *files* we do not
    hold.  Nothing in ``covers`` is a claim to hold anything.

    ``idno`` / ``doi`` describe the release this directory HOLDS;
    ``latest_idno`` / ``latest_doi`` the catalog's newer release when it has
    one, first seen on ``latest_seen`` (GH #1128).  ``extra`` holds any other
    ``#+KEY:`` line of the record, **every occurrence** in order within each key
    (``{key: [value, ...]}``): a repeated unknown key used to keep only its
    last value, so the first line vanished on render (GH #1115, red team r1).
    A plain ``str`` value is accepted and rendered as one line.

    ``scalar_keywords`` retains every occurrence of repeated known scalar
    keys (and explicit empty values). Accessors keep their historical last
    nonempty value; rendering preserves the other recorded facts. An attempt to change
    such a scalar without resolving its stored occurrences raises rather than
    silently erasing them. The backfill's occurrence guard likewise refuses
    a reconstruction that would discard them.
    """

    country: str
    wave: str
    source: str = SOURCE_UNKNOWN          # worldbank | external | unknown
    catalog_id: str | None = None         # primary WB numeric id (None if not WB/unknown)
    idno: str | None = None               # WB string idno, e.g. NGA_2018_GHSP-W4_v03_M
    title: str | None = None
    years: str | None = None              # "2018-2019"
    repository: str | None = None         # WB collection, e.g. "lsms"
    doi: str | None = None                # study DOI, when the catalog has one
    url: str | None = None                # the source URL as recorded
    method: str | None = None             # how the id was determined
    validation: str | None = None         # how strongly the id is corroborated
    recorded: str | None = None           # ISO date we wrote the record
    note: str | None = None
    superseded_url: str | None = None     # prior URL, when we corrected one
    notes: str | None = None              # human prose preserved from the original
    extra: dict[str, list[str]] = field(default_factory=dict)
    # Append fields to preserve the pre-#600 positional constructor order.
    catalog_ids: list[str] = field(default_factory=list)  # every entry held here
    covers: list[str] = field(default_factory=list)       # entries subsumed, NOT held
    latest_idno: str | None = None        # the catalog's newer release (GH #1128)
    latest_doi: str | None = None
    latest_seen: str | None = None        # ISO date a re-stamp first saw it
    scalar_keywords: dict[str, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Keep the scalar and the list in agreement whichever one was supplied.
        if self.catalog_ids:
            self.catalog_id = self.catalog_ids[0]
        elif self.catalog_id:
            self.catalog_ids = [self.catalog_id]

    @property
    def is_worldbank(self) -> bool:
        """True when this wave is a WB study with a known numeric catalog id."""
        return self.source == SOURCE_WORLDBANK and bool(self.catalog_ids)

    @property
    def is_resolved(self) -> bool:
        """True when we know where this wave came from (WB id, or 'external').

        ``False`` only for :data:`SOURCE_UNKNOWN` — i.e. a genuine gap.
        """
        return self.is_worldbank or self.source == SOURCE_EXTERNAL

    @property
    def is_stale_release(self) -> bool:
        """True when the catalog has a newer release than the one held."""
        return bool(self.idno and self.latest_idno
                    and self.idno.strip() != self.latest_idno.strip())


def source_org_path(countries_dir: Path, country: str, wave: str) -> Path:
    """Return the path of a wave's ``Documentation/SOURCE.org``."""
    return Path(countries_dir) / country / wave / "Documentation" / "SOURCE.org"


def _classify_url(url: str | None) -> tuple[str, str | None]:
    """Classify a source URL -> ``(provenance_source, catalog_id_or_None)``."""
    if not url:
        return SOURCE_UNKNOWN, None
    if any(h in url for h in WB_HOSTS):
        m = _CATALOG_ID_RE.search(url)
        # A WB URL without /catalog/{id} (e.g. a bare host link) is still WB,
        # but we do not know the id from the URL alone.
        return SOURCE_WORLDBANK, (m.group(1) if m else None)
    if WB_DOI_PREFIX in url:
        # A WB Microdata DOI.  It IS a WB study, but the numeric id has to be
        # resolved against the catalog -- the DOI does not encode it.
        return SOURCE_WORLDBANK, None
    return SOURCE_EXTERNAL, None


def parse_source_org(text: str, country: str, wave: str) -> WaveProvenance:
    """Parse ``SOURCE.org`` text into a :class:`WaveProvenance`.

    Understands both the structured ``#+KEY: value`` form written by
    :func:`render_source_org` and the legacy bare-URL form, so a wave that
    has not been backfilled still yields a usable (if less precise) record.

    Repeated keyword lines **accumulate** rather than overwrite.  They used to
    be collapsed by a dict comprehension over ``findall``, so a second
    ``#+CATALOG_ID:`` line was silently dropped — you could not record a second
    id even by hand, and nothing said so (GH #600).

    Keywords are read from the record only, never from below
    :data:`NOTES_HEADING` (that is prose, :func:`preserved_prose`), and the
    URL comes from ``#+CATALOG_URL:`` or else from the free-text body above
    the first ``#+`` line -- not from the first URL anywhere in the file,
    which in ``GhanaSPS/2017-18`` is a ``#+SECONDARY_SOURCE:`` in the notes of
    a wave that records no source URL at all (GH #1115).
    """
    record, _notes = _regions(text)
    kw: dict[str, list[str]] = {}
    for key, val in _KEYWORD_RE.findall("\n".join(record)):
        kw.setdefault(key, []).append(val)

    def one(key: str) -> str | None:
        """The last nonempty value of a scalar keyword (legacy compatibility)."""
        vals = [v for v in kw.get(key, []) if v]
        return vals[-1] if vals else None

    def many(key: str) -> list[str]:
        """Every id under a list-valued keyword, across repeated lines."""
        out: list[str] = []
        for val in kw.get(key, []):
            for cid in parse_id_list(val):
                if cid not in out:
                    out.append(cid)
        return out

    url = _primary_url(record)

    catalog_ids = many("CATALOG_ID")

    source = one("PROVENANCE_SOURCE")
    if not source:
        # Legacy file: infer from the URL.
        source, inferred_id = _classify_url(url)
        if not catalog_ids and inferred_id:
            catalog_ids = [inferred_id]
        method = "legacy-source-url" if url else None
    else:
        method = one("PROVENANCE_METHOD")

    return WaveProvenance(
        country=country,
        wave=wave,
        source=source or SOURCE_UNKNOWN,
        catalog_ids=catalog_ids,
        covers=many("CATALOG_COVERS"),
        idno=one("CATALOG_IDNO"),
        title=one("CATALOG_TITLE"),
        years=one("CATALOG_YEARS"),
        repository=one("CATALOG_REPOSITORY"),
        doi=one("CATALOG_DOI"),
        url=url,
        method=method,
        validation=one("PROVENANCE_VALIDATION"),
        recorded=one("PROVENANCE_RECORDED"),
        note=one("PROVENANCE_NOTE"),
        superseded_url=one("PROVENANCE_SUPERSEDED_URL"),
        notes=preserved_prose(text),
        extra={k: list(v) for k, v in kw.items() if k not in _KNOWN_KEYS},
        latest_idno=one("CATALOG_LATEST_IDNO"),
        latest_doi=one("CATALOG_LATEST_DOI"),
        latest_seen=one("CATALOG_LATEST_SEEN"),
        scalar_keywords={k: list(v) for k, v in kw.items()
                         if k in _SCALAR_FIELDS
                         and (len(v) > 1 or not v[-1])},
    )


def render_source_org(prov: WaveProvenance) -> str:
    """Render a :class:`WaveProvenance` as ``SOURCE.org`` text.

    The recorded primary URL is emitted in the body and its metadata field.

    Lossless with :func:`parse_source_org` (GH #1115): every key the record
    holds is emitted -- ``extra`` after the known keys, every occurrence of a
    repeated one, an empty value as a bare ``#+KEY:`` -- and the notes are
    carried as parsed.
    """
    if (prov.catalog_ids or prov.covers) and not prov.is_worldbank:
        # A repeated PROVENANCE_SOURCE can leave the last-value accessor
        # external/unknown while the record still holds catalog identity.
        # Emitting none/unknown below would erase that identity (#1115).
        raise ValueError(
            "Cannot render catalog holdings or coverage without a worldbank "
            "primary entry; explicit provenance curation is required")
    body = prov.url if prov.url else URL_PLACEHOLDER
    lines = ["SOURCE", "", body, ""]

    if prov.is_worldbank:
        cat_id = ", ".join(prov.catalog_ids)
    elif prov.source == SOURCE_EXTERNAL:
        cat_id = _ID_NONE
    else:
        cat_id = _ID_UNKNOWN
    lines.append(f"#+CATALOG_ID: {cat_id}")

    # Entries this release SUBSUMES but whose files we do not hold.  Emitted
    # only for a WB record: "covers" without a holding is meaningless.
    if prov.is_worldbank and prov.covers:
        lines.append(f"#+CATALOG_COVERS: {', '.join(prov.covers)}")

    for key, val in (("CATALOG_IDNO", prov.idno),
                     ("CATALOG_TITLE", prov.title),
                     ("CATALOG_YEARS", prov.years),
                     ("CATALOG_REPOSITORY", prov.repository),
                     ("CATALOG_DOI", prov.doi),
                     ("CATALOG_URL", prov.url),
                     ("CATALOG_LATEST_IDNO", prov.latest_idno),
                     ("CATALOG_LATEST_DOI", prov.latest_doi),
                     ("CATALOG_LATEST_SEEN", prov.latest_seen)):
        if val:
            lines.append(f"#+{key}: {val}")

    lines.append(f"#+PROVENANCE_SOURCE: {prov.source}")
    if prov.method:
        lines.append(f"#+PROVENANCE_METHOD: {prov.method}")
    if prov.validation:
        lines.append(f"#+PROVENANCE_VALIDATION: {prov.validation}")
    recorded = prov.recorded or _dt.date.today().isoformat()
    lines.append(f"#+PROVENANCE_RECORDED: {recorded}")
    # A superseded URL equal to the current one carries no information and
    # would mask the URL it was meant to record.  Drop it.
    if prov.superseded_url and (prov.superseded_url.rstrip("/")
                                != (prov.url or "").rstrip("/")):
        lines.append(f"#+PROVENANCE_SUPERSEDED_URL: {prov.superseded_url}")
    if prov.note:
        lines.append(f"#+PROVENANCE_NOTE: {prov.note}")
    for key, values in prov.scalar_keywords.items():
        field_name = _SCALAR_FIELDS.get(key)
        if field_name is None or not values:
            raise ValueError(f"Invalid preserved scalar keyword: {key}")
        current = getattr(prov, field_name) or ""
        nonempty = [value for value in values if value]
        last = nonempty[-1] if nonempty else ""
        if key == "PROVENANCE_SOURCE":
            current, last = current.lower(), last.lower()
        if current != last:
            raise ValueError(
                f"Cannot rewrite repeated or empty #+{key}: without "
                "explicit provenance curation")
        retained = [f"#+{key}: {value}".rstrip() for value in values]
        position = next((i for i, line in enumerate(lines)
                         if line.startswith(f"#+{key}:")), len(lines))
        # Canonical output has at most one occurrence before this restoration.
        lines[position:position + (position < len(lines))] = retained
    # Keys this module has no field for are a human's (or a newer writer's):
    # emitted, never dropped.  A known key in ``extra`` would be emitted twice.
    for key, vals in prov.extra.items():
        if key in _KNOWN_KEYS:
            continue
        for val in ([vals] if isinstance(vals, str) else vals):
            lines.append(f"#+{key}: {val}".rstrip())

    # Never silently drop human-written prose.
    if prov.notes:
        lines += ["", NOTES_HEADING, prov.notes]

    return "\n".join(lines) + "\n"


def acquired_release(existing: WaveProvenance | None, *, country: str,
                     wave: str, catalog_id: str, idno: str | None,
                     doi: str | None = None, title: str | None = None,
                     years: str | None = None, repository: str | None = None,
                     url: str | None = None, method: str = "add-wave",
                     recorded: str | None = None) -> WaveProvenance:
    """The record to write once an acquisition holds release *idno* of
    catalog entry *catalog_id* (GH #1128).

    This is the one place the HELD keys move: ``idno`` / ``doi`` become the
    release acquired, and the latest-seen trio is cleared -- the newer release
    a re-stamp queued is now the one held.

    *existing* is the record already on disk, if any.  When it names the same
    primary entry, everything a human or an earlier stamp established (the
    other ids the directory holds, ``covers``, the note, the validation, the
    method, the preserved notes, unknown keys) is carried through.  When it
    names another entry, or none, only the human's prose and unknown keys
    are, and its URL is kept as the superseded one.

    *doi*, when not given, is taken from *existing* only where it describes
    *idno*: the queued ``latest_doi`` if *idno* is the queued latest release,
    the held ``doi`` if it is the release already held.  Otherwise it is left
    blank rather than describe another release.
    """
    catalog_id = str(catalog_id)
    if doi is None and existing is not None and idno:
        if existing.latest_idno and existing.latest_idno.strip() == idno.strip():
            doi = existing.latest_doi
        elif existing.idno and existing.idno.strip() == idno.strip():
            doi = existing.doi
    if (existing is not None and existing.is_worldbank
            and existing.catalog_id == catalog_id):
        keep_method = existing.method not in (None, "legacy-source-url")
        return replace(
            existing, idno=idno, doi=doi,
            title=title or existing.title, years=years or existing.years,
            repository=repository or existing.repository,
            url=url or existing.url,
            method=existing.method if keep_method else method,
            recorded=recorded,
            latest_idno=None, latest_doi=None, latest_seen=None)
    prior = existing.url if existing is not None else None
    superseded = (prior if prior and url
                  and prior.rstrip("/") != url.rstrip("/") else None)
    return WaveProvenance(
        country=country, wave=wave, source=SOURCE_WORLDBANK,
        catalog_ids=[catalog_id], idno=idno, doi=doi, title=title,
        years=years, repository=repository, url=url, method=method,
        recorded=recorded, superseded_url=superseded,
        notes=existing.notes if existing is not None else None,
        extra=dict(existing.extra) if existing is not None else {})


def read_provenance(countries_dir: Path, country: str,
                    wave: str) -> WaveProvenance:
    """Read one wave's provenance.

    A missing or unreadable ``SOURCE.org`` yields a record with
    ``source == 'unknown'`` -- never a guess.
    """
    path = source_org_path(countries_dir, country, wave)
    if not path.exists():
        return WaveProvenance(country=country, wave=wave,
                              source=SOURCE_UNKNOWN, method="no-source-org")
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        logger.debug("Could not read %s: %s", path, exc)
        return WaveProvenance(country=country, wave=wave,
                              source=SOURCE_UNKNOWN, method="unreadable")
    return parse_source_org(text, country, wave)


def write_provenance(countries_dir: Path, prov: WaveProvenance) -> Path:
    """Write a wave's ``SOURCE.org``, creating ``Documentation/`` if needed."""
    path = source_org_path(countries_dir, prov.country, prov.wave)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_source_org(prov), encoding="utf-8")
    return path


def country_provenance(countries_dir: Path, country: str,
                       waves: list[str]) -> dict[str, WaveProvenance]:
    """Read provenance for every wave of *country*.

    Every wave in *waves* gets an entry; waves with no ``SOURCE.org`` get an
    explicit ``unknown`` record rather than being omitted.
    """
    return {w: read_provenance(countries_dir, country, w) for w in waves}
