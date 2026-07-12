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
bare URL stays as the first URL in the file, so the legacy reader
(``data_access._read_source_url``, which greps the first ``http(s)://…``)
keeps working unchanged.

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
"""

from __future__ import annotations

import datetime as _dt
import logging
import re
from dataclasses import dataclass, field
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

_KEYWORD_RE = re.compile(r"^\s*#\+([A-Z_]+):\s*(.*?)\s*$", re.MULTILINE)
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

# A line that carries no information beyond the URL itself: the literal
# "SOURCE" banner, or a bare URL / org-mode [[link]].
_BOILERPLATE_RE = re.compile(
    r"^\s*(SOURCE|/?\[?\[?https?://[^\]\s]*\]?\]?)\s*$", re.IGNORECASE)


def preserved_prose(text: str) -> str | None:
    """Extract human-written prose from a SOURCE.org, dropping boilerplate.

    Returns ``None`` when the file says nothing beyond its URL.  Idempotent:
    text already rendered by :func:`render_source_org` yields back exactly the
    prose it preserved.
    """
    if NOTES_HEADING in text:
        text = text.split(NOTES_HEADING, 1)[1]
    lines = [ln.rstrip() for ln in text.splitlines()]
    keep = [ln for ln in lines
            if ln.strip()
            and not ln.lstrip().startswith("#+")
            and not _BOILERPLATE_RE.match(ln)]
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
    """

    country: str
    wave: str
    source: str = SOURCE_UNKNOWN          # worldbank | external | unknown
    catalog_id: str | None = None         # primary WB numeric id (None if not WB/unknown)
    catalog_ids: list[str] = field(default_factory=list)  # every entry held here
    covers: list[str] = field(default_factory=list)       # entries subsumed, NOT held
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
    extra: dict[str, str] = field(default_factory=dict)

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
    """
    kw: dict[str, list[str]] = {}
    for key, val in _KEYWORD_RE.findall(text):
        kw.setdefault(key, []).append(val)

    def one(key: str) -> str | None:
        """The value of a scalar keyword (the last, if a file repeats it)."""
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

    url = one("CATALOG_URL")
    if not url:
        m = _URL_RE.search(text)
        url = m.group(0).rstrip("/") if m else None

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

    known = {"CATALOG_ID", "CATALOG_COVERS", "CATALOG_IDNO", "CATALOG_TITLE",
             "CATALOG_YEARS", "CATALOG_REPOSITORY", "CATALOG_DOI",
             "CATALOG_URL",
             "PROVENANCE_SOURCE", "PROVENANCE_METHOD", "PROVENANCE_VALIDATION",
             "PROVENANCE_RECORDED", "PROVENANCE_NOTE",
             "PROVENANCE_SUPERSEDED_URL"}

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
        extra={k: v[-1] for k, v in kw.items() if k not in known},
    )


def render_source_org(prov: WaveProvenance) -> str:
    """Render a :class:`WaveProvenance` as ``SOURCE.org`` text.

    The recorded URL is emitted as the first ``http(s)://`` in the file so the
    legacy reader (``data_access._read_source_url``) keeps finding it.
    """
    body = prov.url if prov.url else "(source URL not recorded)"
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
                     ("CATALOG_URL", prov.url)):
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

    # Never silently drop human-written prose.
    if prov.notes:
        lines += ["", NOTES_HEADING, prov.notes]

    return "\n".join(lines) + "\n"


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
        text = path.read_text()
    except (OSError, UnicodeDecodeError) as exc:
        logger.debug("Could not read %s: %s", path, exc)
        return WaveProvenance(country=country, wave=wave,
                              source=SOURCE_UNKNOWN, method="unreadable")
    return parse_source_org(text, country, wave)


def write_provenance(countries_dir: Path, prov: WaveProvenance) -> Path:
    """Write a wave's ``SOURCE.org``, creating ``Documentation/`` if needed."""
    path = source_org_path(countries_dir, prov.country, prov.wave)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_source_org(prov))
    return path


def country_provenance(countries_dir: Path, country: str,
                       waves: list[str]) -> dict[str, WaveProvenance]:
    """Read provenance for every wave of *country*.

    Every wave in *waves* gets an entry; waves with no ``SOURCE.org`` get an
    explicit ``unknown`` record rather than being omitted.
    """
    return {w: read_provenance(countries_dir, country, w) for w in waves}
