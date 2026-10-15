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
5. otherwise           -- recorded as ``unknown``.

Usage
-----
    python scripts/backfill_wave_provenance.py --dry-run      # report only
    python scripts/backfill_wave_provenance.py                # write files
    python scripts/backfill_wave_provenance.py --country Nigeria
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from collections import defaultdict
from pathlib import Path

from lsms_library.data_access import (
    _COUNTRY_CATALOG,
    _local_waves,
    _wb_catalog_search,
)
from lsms_library.paths import countries_root
from lsms_library.provenance import (
    SOURCE_EXTERNAL,
    SOURCE_UNKNOWN,
    SOURCE_WORLDBANK,
    VALIDATION_CATALOG_ONLY,
    VALIDATION_CONTENT,
    WB_DOI_PREFIX,
    WaveProvenance,
    read_provenance,
    source_org_path,
    write_provenance,
)

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
        c = ll.Country(country)
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
    """Resolve one wave's provenance.  Returns ``(record, outcome)``."""
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
        if entry:
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
        # Recorded an id the catalog does not have for this country.  Do not
        # invent a replacement.
        return WaveProvenance(
            country=country, wave=wave, source=SOURCE_UNKNOWN, url=url,
            method="unresolved", recorded=today, notes=keep,
            note=(f"SOURCE.org records WB catalog id {existing.catalog_id}, "
                  "but no such entry exists in this country's catalog. "
                  "Needs human review."),
        ), "unknown"

    # --- 2. A WB Microdata DOI ---------------------------------------------
    if WB_DOI_PREFIX in url:
        suffix = url.split("doi.org/")[-1].strip().rstrip("/")
        entry = by_doi.get(suffix)
        if entry:
            return _from_entry(country, wave, entry, "doi-lookup", today,
                               notes=keep), "doi-lookup"
        return WaveProvenance(
            country=country, wave=wave, source=SOURCE_UNKNOWN, url=url,
            method="unresolved", recorded=today, notes=keep,
            note=(f"SOURCE.org records WB DOI {suffix}, which did not match "
                  "any catalog entry for this country.  Needs human review."),
        ), "unknown"

    # --- 3. A non-WB host ---------------------------------------------------
    if url and existing.source == SOURCE_EXTERNAL:
        host = url.split("//")[-1].split("/")[0]
        return WaveProvenance(
            country=country, wave=wave, source=SOURCE_EXTERNAL,
            url=url, method="external-source", recorded=today, notes=keep,
            note=f"Obtained from {host}, not the World Bank Microdata Library.",
        ), "external"

    # --- 5. Unknown ---------------------------------------------------------
    return WaveProvenance(
        country=country, wave=wave, source=SOURCE_UNKNOWN, url=url or None,
        method="unresolved", recorded=today, notes=keep,
        note=("No source URL recorded for this wave; its World Bank catalog "
              "id is unknown.  Deliberately NOT inferred from the directory "
              "name -- year-range labels collide across distinct surveys."),
    ), "unknown"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--country", action="append",
                    help="limit to this country (repeatable)")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be written; change nothing")
    args = ap.parse_args(argv)

    root = countries_root()
    today = dt.date.today().isoformat()

    countries = args.country or sorted(
        d.name for d in root.iterdir()
        if d.is_dir() and not d.name.startswith((".", "_")) and _local_waves(d.name)
    )

    counts: dict[str, int] = defaultdict(int)
    rows: list[tuple[str, str, str, str]] = []

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

        if spec is None:
            for wave in waves:
                existing = read_provenance(root, country, wave)
                prov = WaveProvenance(
                    country=country, wave=wave, source=SOURCE_UNKNOWN,
                    url=existing.url, method="unresolved", recorded=today,
                    notes=existing.notes,
                    note=("No entry in _COUNTRY_CATALOG (data_access.py), so "
                          "this country's source is unrecorded."))
                if not args.dry_run:
                    write_provenance(root, prov)
                counts["unknown"] += 1
                rows.append((country, wave, "unknown", "no catalog mapping"))
            continue

        if not spec.discoverable or not spec.code:
            # Explicitly non-WB (e.g. EthiopiaRHS, KenyaLPS).  Record what we
            # DO know: it is external, and why.
            for wave in waves:
                existing = read_provenance(root, country, wave)
                prov = WaveProvenance(
                    country=country, wave=wave, source=SOURCE_EXTERNAL,
                    url=existing.url, method="not-a-wb-dataset", recorded=today,
                    note=spec.reason, notes=existing.notes)
                if not args.dry_run:
                    write_provenance(root, prov)
                counts["external"] += 1
                rows.append((country, wave, "external", "not a WB dataset"))
            continue

        by_id, by_doi = _build_catalog_index(spec.code)

        for wave in waves:
            prov, outcome = resolve(country, wave, root, by_id, by_doi, today)
            if not args.dry_run:
                write_provenance(root, prov)
            counts[outcome] += 1
            detail = (f"{prov.catalog_id} {prov.idno or ''}".strip()
                      if prov.catalog_id else (prov.note or "")[:52])
            rows.append((country, wave, outcome, detail))

    # --- Report -------------------------------------------------------------
    width = max((len(f"{c}/{w}") for c, w, _, _ in rows), default=20)
    for country, wave, outcome, detail in rows:
        print(f"  {country + '/' + wave:<{width}}  {outcome:<11} {detail}")

    total = sum(counts.values())
    stamped = total - counts["skipped"]
    resolved = stamped - counts["unknown"]
    print(f"\n{'DRY RUN -- nothing written' if args.dry_run else 'Written'}")
    print(f"  wave dirs scanned : {total}")
    print(f"  stamped           : {stamped}")
    print(f"      resolved      : {resolved}")
    for k in sorted(k for k in counts if k not in ("unknown", "skipped")):
        if counts[k]:
            print(f"          {k:<14}: {counts[k]}")
    print(f"      unknown       : {counts['unknown']}")
    print(f"  skipped           : {counts['skipped']}  "
          "(not declared waves; stamping would create one -- see _may_write)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
