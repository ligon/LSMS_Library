#!/usr/bin/env python
"""Backfill each wave directory's WB catalog id into its ``SOURCE.org``.

Every ``countries/{Country}/{wave}/`` directory should record *which* World
Bank catalog entry (or which non-WB source) it actually holds, so that
``discover_waves()`` can match on identity rather than on a wave label
reconstructed from a year range.  See :mod:`lsms_library.provenance`.

Resolution is evidence-based.  A wave is only marked resolved when the
evidence is conclusive; otherwise it is recorded as ``unknown``.  **We never
guess.**  In particular a wave with no ``SOURCE.org`` is *not* resolved by
matching its directory name against a catalog year range -- that heuristic is
exactly the bug this work exists to remove.

Resolution ladder
-----------------
1. ``source-url``      -- ``SOURCE.org`` records a ``/catalog/{id}`` URL on a
                          World Bank host.  The id is verified to exist in the
                          WB catalog for this country.
2. ``doi-lookup``      -- ``SOURCE.org`` records a WB Microdata DOI
                          (``10.48529/…``).  The DOI does not encode the id,
                          but the catalog row carries a ``doi`` field, so the
                          id is recovered by exact DOI match.
3. ``external-source`` -- ``SOURCE.org`` records a non-WB host (Ghana
                          Statistical Service, Harvard Dataverse, …).  There
                          is no WB id; recorded as ``external``.
4. ``manual-override`` -- a small, explicitly justified table (below) for
                          waves whose recorded URL is demonstrably wrong.
5. otherwise           -- **unresolved**: nothing is written (below).

A failed resolution leaves the record intact (GH #600)
------------------------------------------------------
Ruling (@ligon, 2026-10-18): *on a failed resolution, the backfill leaves the
existing record intact and writes a separate review artifact listing what it
could not resolve.  It never downgrades a recorded id to UNKNOWN.*

It used to write an ``unknown`` record instead -- so an unreachable catalog, a
missing API key, or an id the search filtered out turned every recorded
``#+CATALOG_ID:`` into ``unknown`` and discarded the hand-recorded facts beside
it.  Measured on a copy of the shipped tree with the catalog unreachable: all
130 ``SOURCE.org`` files rewritten, 104 recorded WB ids downgraded to
``unknown``.  Now every failure raises :class:`Unresolved`, which carries no
record, so there is nothing to write by accident.  A wave is unresolved when:

* its recorded id / DOI / override id is not in what the catalog returned;
* the catalog returned **no rows at all** for the country -- an outage, a
  missing key or a filter looks exactly like this, and ``_wb_catalog_search``
  cannot tell them apart, so the whole country is left alone;
* it records no evidence the ladder can use (step 5), or its country has no
  ``_COUNTRY_CATALOG`` entry;
* it records a WB id although its country is marked not-a-WB-dataset;
* the record the ladder *would* write drops something the file already holds
  -- an id, a ``covers`` entry, a ``#+KEY:`` line, the preserved prose.  The
  backfill may add to and update a record; it may not delete from one.

The review artifact is an org file, written **outside** ``countries_root()``
(the config tree is read-only at runtime; see CLAUDE.md), by default to
``platformdirs.user_cache_path("lsms_library")/provenance_review/``; override
with ``--review-file``.  It is written on every run, ``--dry-run`` included,
so a missing file never needs interpreting.  Two named tables:
``catalog_queries`` (rows the catalog returned per country -- zero means the
country was not checked at all) and ``unresolved`` (one row per wave left
intact: what the file records, and why it could not be resolved).

A record whose content would not change is not rewritten either, so
``#+PROVENANCE_RECORDED:`` stays the date the record's content was last
written rather than the date of the last run.

Usage
-----
    python scripts/backfill_wave_provenance.py --dry-run      # report only
    python scripts/backfill_wave_provenance.py                # write files
    python scripts/backfill_wave_provenance.py --country Nigeria
    python scripts/backfill_wave_provenance.py --review-file /tmp/review.org
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

from lsms_library.data_access import (
    _COUNTRY_CATALOG,
    _local_waves,
    _wb_catalog_search,
)
from lsms_library.paths import countries_root
from lsms_library.provenance import (
    SOURCE_EXTERNAL,
    SOURCE_WORLDBANK,
    VALIDATION_CATALOG_ONLY,
    VALIDATION_CONTENT,
    WB_DOI_PREFIX,
    _KEYWORD_RE,
    WaveProvenance,
    parse_source_org,
    read_provenance,
    render_source_org,
    source_org_path,
    write_provenance,
)


class Unresolved(Exception):
    """This run could not resolve a wave's provenance.  Nothing is written.

    Raised rather than returned: a failed resolution has no record, so a caller
    cannot write one by accident -- which is the whole of the GH #600 ruling.
    ``kind`` is a short slug for the review artifact; ``reason`` says what a
    human needs to check.
    """

    def __init__(self, kind: str, reason: str):
        super().__init__(reason)
        self.kind = kind
        self.reason = reason

# ---------------------------------------------------------------------------
# Manual overrides
# ---------------------------------------------------------------------------
# Each entry must cite the evidence that overrides what SOURCE.org records.
# Keep this table tiny and justified; it is not a place to park guesses.

_OVERRIDES: dict[tuple[str, str], dict] = {
    # --- Correcting a wrongly-recorded id -----------------------------------
    ("Nigeria", "2018-19"): {
        "catalog_id": "3557",
        "validation": VALIDATION_CONTENT,
        "note": (
            "Corrected from catalog 3827 (NGA_2018_LSS, 'Living Standards "
            "Survey 2018-2019').  The data files in this directory are "
            "sect*_plantingw4 / sect*_harvestw4 / nga_*_y4 -- i.e. General "
            "Household Survey Panel Wave 4 (NGA_2018_GHSP-W4, catalog 3557). "
            "Both studies span 2018-2019, so the previously recorded id was "
            "the wrong one of the two.  Every sibling Nigeria wave is also "
            "GHS-Panel (W1=1002, W2=1952, W3=2734, W5=6410)."
        ),
    },

    # --- Waves with no SOURCE.org, resolved from directory contents ---------
    ("Niger", "2011-12"): {
        "catalog_id": "2050",
        "validation": VALIDATION_CONTENT,
        "note": (
            "No SOURCE.org was recorded.  Data/ contains a directory named "
            "NER_2011_ECVMA_v01_M_Stata8 -- the World Bank idno itself -- "
            "which matches catalog 2050 (NER_2011_ECVMA_v01_M, 'National "
            "Survey on Household Living Conditions and Agriculture', "
            "2011-2012).  Identified from the data, not from the year range."
        ),
    },
    ("Senegal", "2021-22"): {
        "catalog_id": "6278",
        "validation": VALIDATION_CONTENT,
        "note": (
            "No SOURCE.org was recorded.  Data/ holds 58 ehcvm_*_sen2021 "
            "files (ehcvm_conso_sen2021, ehcvm_individu_sen2021, ...), "
            "matching catalog 6278 (SEN_2021_EHCVM-2_v01_M, 2021-2022), the "
            "only SEN EHCVM-2 entry; 4297 is the 2018-19 wave."
        ),
    },
    ("Nepal", "2003-04"): {
        "catalog_id": "74",
        # Deliberately weaker than the two above: there is no local data to
        # corroborate the id against.  Recorded, but flagged as such.
        "validation": VALIDATION_CATALOG_ONLY,
        "note": (
            "No SOURCE.org was recorded.  Matched to catalog 74 "
            "(NPL_2003_LSS-II_v01_M, 'Living Standards Survey 2003-2004, "
            "Second Round') on idno and year range.  NOT content-validated: "
            "Nepal/2003-04/Data/ is EMPTY -- Nepal's microdata is hosted by "
            "the Nepal NSO rather than the World Bank and is not in this "
            "repository (see CLAUDE.md, 'Countries Without Microdata').  So "
            "the provenance is known while the data is absent; these are two "
            "different facts and are recorded as such."
        ),
    },

    # --- Waves that are definitively NOT World Bank datasets ----------------
    # Terminal state: 'none', not 'unknown'.  Leaving these unknown would mean
    # someone re-searches them forever -- the same absent-vs-unknown black hole
    # this work exists to close, one level down.
    ("GhanaSPS", "2013-14"): {
        "source": SOURCE_EXTERNAL,
        "note": (
            "Ghana Socioeconomic Panel Survey wave 2 (EGC-ISSER, Yale "
            "University / ISSER).  Not a World Bank dataset: a GHA query "
            "across the ENTIRE WB catalog (all collections, 152 entries) "
            "returns exactly one GSPS study -- 2534, the 2009-2010 wave -- "
            "and nothing later.  Directory contents (01a_consent.dta, "
            "01b2_roster.dta, 00_comm_info.dta) are the EGC-ISSER instrument, "
            "not a WB one.  CATALOG_ID is 'none', not 'unknown'."
        ),
    },
    ("GhanaSPS", "2017-18"): {
        "source": SOURCE_EXTERNAL,
        "note": (
            "Ghana Socioeconomic Panel Survey wave 3 (EGC-ISSER, Yale "
            "University / ISSER).  Not a World Bank dataset -- see the "
            "GhanaSPS/2013-14 note; the WB catalog holds only the 2009-2010 "
            "GSPS wave (2534).  CATALOG_ID is 'none', not 'unknown'."
        ),
    },
}


def _wave_dirs(root: Path, country: str) -> list[str]:
    return _local_waves(country)


def _declared_waves(country: str) -> set[str] | None:
    """Wave *directories* the library recognises, or ``None`` if it cannot say.

    ``Country.waves`` is not always a list of directory names.  Nigeria's are
    survey rounds (``2010Q3``, ``2011Q1``) and Tanzania's are logical waves
    (``2008-09`` .. ``2014-15``); both map onto directories via
    ``wave_folder_map``.  Provenance is recorded per *directory*, so fold the
    folder names in too -- otherwise a real wave folder looks undeclared.
    """
    try:
        import lsms_library as ll
        # Provenance stamping walks every wave dir, including countries with
        # no _/data_scheme.yml yet -- the scaffold opt-in (GH #915).
        c = ll.Country(country, allow_unregistered=True)
        declared = set(c.waves)                   # sets wave_folder_map as a side effect
        folder_map = getattr(c, "wave_folder_map", None) or {}
        declared |= {str(v) for v in folder_map.values()}
        return declared
    except Exception as exc:                      # noqa: BLE001 - best effort
        print(f"  ! {country}: could not read Country.waves ({exc!r}); "
              "will only update wave dirs that already have a SOURCE.org")
        return None


def _may_write(country: str, wave: str, root: Path,
               declared: set[str] | None) -> bool:
    """May we write ``{country}/{wave}/Documentation/SOURCE.org``?

    **SOURCE.org is load-bearing.**  ``Country.waves`` (country.py) falls back
    to scanning for subdirectories that contain ``Documentation/SOURCE.org``
    when the country declares no explicit wave list.  For such a country,
    *creating* a SOURCE.org in a directory silently promotes that directory
    into a wave -- changing what the library reports and what every
    wave-iterating table must now cover.

    That is exactly what happened to ``Benin/2018-2019/`` (a stray duplicate of
    ``Benin/2018-19/``, same .dta files, no SOURCE.org): stamping it made it a
    wave, and ``sample()`` did not cover it.

    So: only ever *update* a SOURCE.org that already exists, or *create* one in
    a directory the library already declares a wave.  Never let recording
    provenance change ``Country.waves``.
    """
    if source_org_path(root, country, wave).exists():
        return True                                # updating: always safe
    if declared is None:
        return False                               # cannot verify: don't risk it
    return wave in declared                        # creating: only for real waves


def _build_catalog_index(code: str) -> tuple[dict[str, dict], dict[str, dict]]:
    """Return ``(by_id, by_doi)`` for a country, across *all* collections.

    Searching every collection (not just ``lsms``) matters: a wave we hold may
    be catalogued outside the LSMS collection.
    """
    rows = _wb_catalog_search(code, collection=None)
    by_id = {str(r["id"]): r for r in rows}
    by_doi: dict[str, dict] = {}
    for r in rows:
        doi = str(r.get("doi") or "")
        if doi:
            # Index by the bare DOI suffix so 'https://doi.org/10.48529/x'
            # and '10.48529/x' both match.
            by_doi[doi.split("doi.org/")[-1].strip().rstrip("/")] = r
    return by_id, by_doi


def _years(entry: dict) -> str | None:
    ys, ye = entry.get("year_start"), entry.get("year_end")
    if ys and ye:
        return f"{ys}-{ye}"
    return str(ys) if ys else None


def _from_entry(country: str, wave: str, entry: dict, method: str,
                today: str, note: str | None = None,
                superseded: str | None = None,
                notes: str | None = None,
                validation: str | None = None,
                also_ids: list[str] | None = None,
                covers: list[str] | None = None) -> WaveProvenance:
    # ``also_ids`` / ``covers`` are hand-recorded facts this script cannot
    # rediscover from the catalog: the OTHER entries a directory holds, and the
    # entries a held release subsumes (GH #600).  A re-stamp must carry them
    # through -- rebuilding the record from the catalog alone would silently
    # delete them, which is how a partial record gets created in the first place.
    primary = str(entry["id"])
    ids = [primary] + [i for i in (also_ids or []) if i != primary]
    return WaveProvenance(
        country=country, wave=wave,
        source=SOURCE_WORLDBANK,
        catalog_ids=ids,
        covers=list(covers or []),
        idno=entry.get("idno") or None,
        title=entry.get("title") or None,
        years=_years(entry),
        repository=entry.get("repository") or None,
        doi=entry.get("doi") or None,
        url=entry.get("url"),
        method=method,
        validation=validation,
        recorded=today,
        note=note,
        superseded_url=superseded,
        notes=notes,
    )


def resolve(country: str, wave: str, root: Path,
            by_id: dict[str, dict], by_doi: dict[str, dict],
            today: str) -> tuple[WaveProvenance, str]:
    """Resolve one wave's provenance.  Returns ``(record, outcome)``.

    Raises :class:`Unresolved` -- and so produces no record -- whenever the
    evidence does not resolve.  The existing ``SOURCE.org`` is then left as it
    is (GH #600).
    """
    existing = read_provenance(root, country, wave)
    # Human-written prose in the original file is carried through verbatim.
    keep = existing.notes

    # --- 4. Manual override (evidence beats the recorded URL / the silence) --
    ov = _OVERRIDES.get((country, wave))
    if ov:
        # 4a. Definitively not a WB dataset -> terminal 'none', not 'unknown'.
        if ov.get("source") == SOURCE_EXTERNAL:
            return WaveProvenance(
                country=country, wave=wave, source=SOURCE_EXTERNAL,
                url=ov.get("url") or existing.url,
                method="manual-override", recorded=today,
                note=ov["note"], notes=keep,
            ), "override"

        # 4b. Resolve to a specific catalog id.
        entry = by_id.get(ov["catalog_id"])
        if not entry:
            # Do NOT fall through to the recorded id: the override exists
            # because the recorded id may be the wrong one (Nigeria/2018-19
            # recorded 3827), so re-stamping it would re-assert the mistake.
            raise Unresolved(
                "override-id-not-in-catalog",
                f"_OVERRIDES assigns catalog id {ov['catalog_id']}, which is "
                "not among the entries the catalog returned for this country.")
        # Remember the URL we replaced -- but only the *original* one.  On
        # a re-run ``existing.url`` is already the corrected URL, so an
        # unguarded assignment would overwrite the record of the mistake
        # with a self-reference and lose it.
        prior = existing.superseded_url or existing.url
        canonical = (entry.get("url") or "").rstrip("/")
        superseded = (prior if prior and prior.rstrip("/") != canonical
                      else None)
        return _from_entry(country, wave, entry, "manual-override", today,
                           note=ov["note"], superseded=superseded,
                           notes=keep,
                           validation=ov.get("validation"),
                           also_ids=existing.catalog_ids[1:],
                           covers=existing.covers), "override"

    url = existing.url or ""

    # --- 1. A /catalog/{id} URL on a World Bank host ------------------------
    if existing.source == SOURCE_WORLDBANK and existing.catalog_id:
        entry = by_id.get(existing.catalog_id)
        if entry:
            # Re-running on an already-stamped tree must not rewrite history:
            # once a DOI has been resolved we record a /catalog/{id} CATALOG_URL,
            # so a naive second pass would come through here and relabel the
            # record "source-url", erasing the fact that we learned the id by
            # DOI lookup (or by manual override).  Keep the original method.
            method = (existing.method
                      if existing.method in ("doi-lookup", "manual-override",
                                             "add-wave")
                      else "source-url")
            # Everything the catalog cannot tell us -- the additional entries
            # this dir holds, the entries the held release covers, the
            # hand-written note, how strongly the id is corroborated -- is
            # carried through verbatim.  A re-stamp records what the catalog
            # says; it must not erase what a human established (GH #600).
            return _from_entry(country, wave, entry, method, today,
                               notes=keep,
                               superseded=existing.superseded_url,
                               note=existing.note,
                               validation=existing.validation,
                               also_ids=existing.catalog_ids[1:],
                               covers=existing.covers), method
        # Recorded an id the catalog did not return for this country.  Do not
        # invent a replacement -- and do not downgrade the recorded one to
        # 'unknown' either: an outage or a filter looks exactly like this.
        raise Unresolved(
            "id-not-in-catalog",
            f"SOURCE.org records WB catalog id {existing.catalog_id}, which is "
            "not among the entries the catalog returned for this country.")

    # --- 2. A WB Microdata DOI ---------------------------------------------
    if WB_DOI_PREFIX in url:
        suffix = url.split("doi.org/")[-1].strip().rstrip("/")
        entry = by_doi.get(suffix)
        if entry:
            return _from_entry(country, wave, entry, "doi-lookup", today,
                               notes=keep), "doi-lookup"
        raise Unresolved(
            "doi-not-in-catalog",
            f"SOURCE.org records WB DOI {suffix}, which matches no entry the "
            "catalog returned for this country.")

    # --- 3. A non-WB host ---------------------------------------------------
    if url and existing.source == SOURCE_EXTERNAL:
        host = url.split("//")[-1].split("/")[0]
        return WaveProvenance(
            country=country, wave=wave, source=SOURCE_EXTERNAL,
            url=url, method="external-source", recorded=today, notes=keep,
            note=f"Obtained from {host}, not the World Bank Microdata Library.",
        ), "external"

    # --- 5. Unresolved -----------------------------------------------------
    # Deliberately NOT inferred from the directory name -- year-range labels
    # collide across distinct surveys.  And not written as 'unknown' either:
    # whatever the file already says (an explicit 'unknown' with a reason, a
    # WB link with no id, prose) stays exactly as it is.
    raise Unresolved(
        "no-resolvable-evidence",
        "SOURCE.org records no catalog id, WB DOI or external URL the ladder "
        "can resolve" + ("" if source_org_path(root, country, wave).exists()
                         else " (there is no SOURCE.org)") + ".")


def _resolve_not_wb(country: str, wave: str, root: Path, reason: str | None,
                    today: str) -> tuple[WaveProvenance, str]:
    """Stamp a wave of a country ``_COUNTRY_CATALOG`` marks not-a-WB-dataset.

    Built from the existing record, with ``reason`` only as a fallback note.  It
    used to be built from ``reason`` alone (url, note, prose and nothing else),
    which on the shipped tree deleted six hand-recorded lines from each of 8
    files on a run that never touches the network: GhanaAHIES' GSS NADA idno,
    title, years, repository and sourcing note, KenyaLPS' Dataverse title,
    years, DOI and ``PROVENANCE_VALIDATION``.
    """
    existing = read_provenance(root, country, wave)
    if existing.catalog_ids:
        # A recorded WB id beats a config flag; restamping 'none' would
        # silently delete it.  A human has to settle which one is wrong.
        raise Unresolved(
            "recorded-id-not-discoverable",
            f"SOURCE.org records WB catalog id(s) "
            f"{', '.join(existing.catalog_ids)}, but _COUNTRY_CATALOG marks "
            f"{country} not a World Bank dataset.")
    stamped = (existing.source == SOURCE_EXTERNAL
               and existing.method not in (None, "legacy-source-url"))
    method = existing.method if stamped else "not-a-wb-dataset"
    # A note this branch wrote itself tracks the config; any other is a human's.
    note = (reason if method == "not-a-wb-dataset"
            else (existing.note or reason))
    return WaveProvenance(
        country=country, wave=wave, source=SOURCE_EXTERNAL,
        idno=existing.idno, title=existing.title, years=existing.years,
        repository=existing.repository, doi=existing.doi, url=existing.url,
        method=method, validation=existing.validation, recorded=today,
        note=note, superseded_url=existing.superseded_url,
        notes=existing.notes, extra=dict(existing.extra),
    ), "external"


def _keys(text: str) -> list[str]:
    """The ``#+KEY:`` names a SOURCE.org gives a value, in order, once each."""
    return list(dict.fromkeys(k for k, v in _KEYWORD_RE.findall(text) if v))


def dropped_by_write(old_text: str, new: WaveProvenance) -> list[str]:
    """What writing *new* over a SOURCE.org holding *old_text* would delete.

    The backfill may add to a record and may update a value the catalog
    re-reports; it may not delete.  Judged on what *survives the write* -- the
    rendered text, re-parsed -- because ``render_source_org`` emits only the
    keys it knows: ``GhanaSPS/2017-18``'s five ``#+SECONDARY_SOURCE*`` lines
    (a second, Dataverse vintage of ten files) would vanish on any re-stamp.

    A ``manual-override`` may replace a recorded id -- correcting one is what
    the override table is for (it records the URL it replaced) -- but may not
    delete anything else.
    """
    new_text = render_source_org(new)
    old = parse_source_org(old_text, new.country, new.wave)
    back = parse_source_org(new_text, new.country, new.wave)
    lost: list[str] = []
    if new.method != "manual-override":
        lost += [f"CATALOG_ID {i}" for i in old.catalog_ids
                 if i not in back.catalog_ids]
    lost += [f"CATALOG_COVERS {i}" for i in old.covers if i not in back.covers]
    kept = set(_keys(new_text))
    lost += [f"#+{k}" for k in _keys(old_text)
             if k not in kept and k != "PROVENANCE_RECORDED"]
    if old.url and not back.url:
        lost.append("the source URL")
    if old.notes and old.notes not in (back.notes or ""):
        lost.append("the preserved notes")
    return lost


def unchanged_by_write(old_text: str, new: WaveProvenance) -> bool:
    """Would writing *new* change nothing but ``#+PROVENANCE_RECORDED:``?

    Compared as parsed records, so a hand-edited file whose layout differs from
    ``render_source_org``'s (an extra blank line, KenyaLPS) is not rewritten
    just to be reformatted.
    """
    old = parse_source_org(old_text, new.country, new.wave)
    back = parse_source_org(render_source_org(new), new.country, new.wave)
    return replace(old, recorded=None) == replace(back, recorded=None)


def review_path(arg: str | None, root: Path, stamp: str) -> Path:
    """Where the review artifact goes.  Never inside ``countries_root()``."""
    if arg:
        path = Path(arg).expanduser()
    else:
        try:
            import platformdirs
            base = platformdirs.user_cache_path("lsms_library")
        except ImportError:
            base = Path.home() / ".cache" / "lsms_library"
        path = base / "provenance_review" / f"backfill_unresolved_{stamp}.org"
    if path.resolve().is_relative_to(Path(root).resolve()):
        raise SystemExit(
            f"Refusing to write the review artifact to {path}: it is inside "
            f"countries_root() ({root}), which is read-only at runtime.  "
            "Pass --review-file with a path outside the config tree.")
    return path


def _cell(value) -> str:
    """One org-table cell.  Empty is ``---``, which ``df_from_orgfile`` reads
    as null; a ``|`` would split the cell, so it is escaped."""
    text = " ".join(str(value).split()) if value not in (None, "") else "---"
    return text.replace("|", r"\vert{}")


def write_review(path: Path, unresolved: list[dict], queries: list[dict], *,
                 root: Path, dry_run: bool, argv: list[str],
                 when: str) -> Path:
    """Write the review artifact: what this run left intact, and why."""
    q_cols = ("Country", "Code", "Rows returned")
    u_cols = ("Country", "Wave", "Kind", "Recorded source",
              "Recorded CATALOG_ID", "Recorded method", "SOURCE.org", "Reason")

    def table(name: str, cols: tuple[str, ...], rows: list[dict]) -> list[str]:
        return ([f"#+name: {name}", "| " + " | ".join(cols) + " |",
                 "|" + "+".join("-" * (len(c) + 2) for c in cols) + "|"]
                + ["| " + " | ".join(_cell(r[c]) for c in cols) + " |"
                   for r in rows])

    lines = [
        "#+TITLE: Provenance backfill -- waves left unresolved",
        f"#+DATE: {when}",
        "",
        f"Run: =backfill_wave_provenance.py {' '.join(argv)}= "
        f"({'dry run' if dry_run else 'write'}), countries_root() = {root}",
        "",
        "Every wave in the =unresolved= table was LEFT INTACT: its SOURCE.org,",
        "if it has one, was not written (GH #600).  Settle each by hand, or",
        "re-run once the catalog answers; never re-stamp one as =unknown=.",
        "A country with 0 rows in =catalog_queries= was not checked at all.",
        "",
        "* Catalog queries",
        *table("catalog_queries", q_cols, queries),
        "",
        f"* Unresolved waves ({len(unresolved)})",
        *table("unresolved", u_cols, unresolved),
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--country", action="append",
                    help="limit to this country (repeatable)")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be written; write no SOURCE.org "
                         "(the review artifact is still written)")
    ap.add_argument("--review-file",
                    help="where to write the review artifact (default: "
                         "<user cache>/lsms_library/provenance_review/"
                         "backfill_unresolved_<timestamp>.org); must be "
                         "outside countries_root()")
    args = ap.parse_args(argv)

    root = countries_root()
    now = dt.datetime.now().replace(microsecond=0)
    today = now.date().isoformat()
    review = review_path(args.review_file, root,
                         now.strftime("%Y-%m-%dT%H%M%S"))

    countries = args.country or sorted(
        d.name for d in root.iterdir()
        if d.is_dir() and not d.name.startswith((".", "_")) and _local_waves(d.name)
    )

    counts: dict[str, int] = defaultdict(int)
    actions: dict[str, int] = defaultdict(int)
    rows: list[tuple[str, str, str, str]] = []
    unresolved: list[dict] = []
    queries: list[dict] = []

    def leave_intact(country: str, wave: str, u: Unresolved) -> None:
        existing = read_provenance(root, country, wave)
        unresolved.append({
            "Country": country, "Wave": wave, "Kind": u.kind,
            "Recorded source": existing.source,
            "Recorded CATALOG_ID": ", ".join(existing.catalog_ids),
            "Recorded method": existing.method,
            "SOURCE.org": ("present" if source_org_path(root, country,
                                                        wave).exists()
                           else "absent"),
            "Reason": u.reason,
        })
        counts["unresolved"] += 1
        rows.append((country, wave, "unresolved", u.reason[:52]))

    def stamp(country: str, wave: str, prov: WaveProvenance,
              outcome: str) -> None:
        path = source_org_path(root, country, wave)
        old = path.read_text(encoding="utf-8") if path.exists() else None
        if old is not None:
            lost = dropped_by_write(old, prov)
            if lost:
                leave_intact(country, wave, Unresolved(
                    "restamp-would-delete",
                    f"Resolved ({outcome}), but the record written would "
                    f"delete {', '.join(lost)} from SOURCE.org."))
                return
            if unchanged_by_write(old, prov):
                actions["unchanged"] += 1
                counts[outcome] += 1
                rows.append((country, wave, outcome, "unchanged"))
                return
        if not args.dry_run:
            write_provenance(root, prov)
        actions["written"] += 1
        counts[outcome] += 1
        detail = (f"{prov.catalog_id} {prov.idno or ''}".strip()
                  if prov.catalog_id else (prov.note or "")[:52])
        rows.append((country, wave, outcome, detail))

    for country in countries:
        spec = _COUNTRY_CATALOG.get(country)
        waves = _wave_dirs(root, country)
        if not waves:
            continue

        declared = _declared_waves(country)

        # Directories that are not declared waves must NOT be stamped -- doing
        # so would newly declare them (see _may_write).
        skipped = [w for w in waves if not _may_write(country, w, root, declared)]
        waves = [w for w in waves if w not in skipped]
        for wave in skipped:
            counts["skipped"] += 1
            rows.append((country, wave, "skipped",
                         "not a declared wave; stamping would create one"))
        if not waves:
            continue

        if spec is None:
            for wave in waves:
                leave_intact(country, wave, Unresolved(
                    "no-catalog-mapping",
                    f"{country} has no entry in _COUNTRY_CATALOG "
                    "(data_access.py), so there is no catalog to resolve "
                    "against."))
            continue

        if not spec.discoverable or not spec.code:
            # Explicitly non-WB (e.g. EthiopiaRHS, KenyaLPS).  Record what we
            # DO know: it is external, and why.
            for wave in waves:
                try:
                    prov, outcome = _resolve_not_wb(country, wave, root,
                                                    spec.reason, today)
                except Unresolved as u:
                    leave_intact(country, wave, u)
                    continue
                stamp(country, wave, prov, outcome)
            continue

        by_id, by_doi = _build_catalog_index(spec.code)
        queries.append({"Country": country, "Code": spec.code,
                        "Rows returned": len(by_id)})
        if not by_id:
            # _wb_catalog_search returns [] on a network error, a missing API
            # key or an empty filter alike.  No coded country has zero entries,
            # so nothing here is evidence about any wave: touch none of them.
            for wave in waves:
                leave_intact(country, wave, Unresolved(
                    "catalog-returned-nothing",
                    f"The WB catalog search for {spec.code} returned no rows "
                    "(unreachable, no API key, or filtered), so nothing was "
                    "checked."))
            continue

        for wave in waves:
            try:
                prov, outcome = resolve(country, wave, root, by_id, by_doi,
                                        today)
            except Unresolved as u:
                leave_intact(country, wave, u)
                continue
            stamp(country, wave, prov, outcome)

    write_review(review, unresolved, queries, root=root,
                 dry_run=args.dry_run, argv=argv, when=now.isoformat())

    # --- Report -------------------------------------------------------------
    width = max((len(f"{c}/{w}") for c, w, _, _ in rows), default=20)
    for country, wave, outcome, detail in rows:
        print(f"  {country + '/' + wave:<{width}}  {outcome:<11} {detail}")

    total = sum(counts.values())
    resolved = total - counts["skipped"] - counts["unresolved"]
    verb = "would be written" if args.dry_run else "written"
    print(f"\n{'DRY RUN -- no SOURCE.org written' if args.dry_run else 'Done'}")
    print(f"  wave dirs scanned : {total}")
    print(f"  resolved          : {resolved}")
    for k in sorted(k for k in counts
                    if k not in ("unresolved", "skipped")):
        if counts[k]:
            print(f"      {k:<14}: {counts[k]}")
    print(f"      {verb:<16}: {actions['written']}")
    print(f"      unchanged       : {actions['unchanged']}  "
          "(content identical; not rewritten)")
    print(f"  unresolved        : {counts['unresolved']}  "
          "(LEFT INTACT -- see the review artifact)")
    print(f"  skipped           : {counts['skipped']}  "
          "(not declared waves; stamping would create one -- see _may_write)")
    print(f"\nReview artifact: {review}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
