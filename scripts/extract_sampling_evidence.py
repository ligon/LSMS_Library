#!/usr/bin/env python3
"""Collect candidate SAMPLING-DESIGN sentences from what is already written down.

This is the evidence-gathering half of the pattern `population.yml` already
uses: sweep the sources into one reviewable document, have a human curate it,
then promote the curated result into per-country config.  It writes NOTHING
into `countries/`; it only tells you what exists and where.

Two sources, both already in the repo:

* ``countries/{C}/_/population.yml`` -- the 2026-07 population sweep
  transcribed a great deal of design detail and then deliberately parked it,
  because a sampling design is not a universe statement.  Uganda 2005-06's
  ``notes`` for instance carries "stratified two-stage design (EA then
  household), ~750 EAs, 10 households per EA; three-stage in IDP-camp
  districts of Northern Uganda".  62% of waves have something like this.
  Everything here already carries a ``source_file`` + ``locator``.

* ``countries/{C}/_/CONTENTS.org`` -- prose, richest for the 13 countries
  with a ``* Sampling Design`` section, but a locator is only the heading.

A hit is a CANDIDATE, not a finding.  The sentence may describe a different
survey round, may be the library's own inference rather than a document's
claim, or may be contradicted elsewhere.  That is exactly why the output is
an evidence file for review rather than a generated `sampling.yml`.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import warnings

# Deliberately broad: recall matters more than precision for a review queue.
DESIGN_PAT = re.compile(
    r"(two[- ]stage|three[- ]stage|single[- ]stage|first stage|second stage|third stage"
    r"|stratif|PSU|primary sampling|enumeration area|\bEAs?\b|cluster sample"
    r"|oversampl|over-sampl|probability proportional|\bPPS\b|systematic random"
    r"|self[- ]weight|rotat|refreshment|replacement sample|sampling frame)",
    re.I,
)
SENT_SPLIT = re.compile(r"(?<=[.;])\s+")


def _sentences(text: str) -> list[str]:
    flat = " ".join(text.split())
    return [s.strip() for s in SENT_SPLIT.split(flat) if DESIGN_PAT.search(s)]


def from_population_yml(country: str) -> list[dict]:
    from lsms_library.population import population_records
    rows = []
    for wave, rec in population_records(country).items():
        for field in ("population_statement", "exclusions", "notes", "survey"):
            val = getattr(rec, field, None)
            if not val:
                continue
            for sent in _sentences(val):
                rows.append({
                    "country": country, "wave": wave, "source": "population.yml",
                    "field": field, "sentence": sent,
                    "source_file": rec.source_file or "", "locator": rec.locator or "",
                    "confidence_of_universe_record": rec.confidence,
                })
    return rows


def from_contents_org(country: str) -> list[dict]:
    from lsms_library.paths import countries_root
    path = countries_root() / country / "_" / "CONTENTS.org"
    if not path.exists():
        return []
    rows, heading = [], ""
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("*"):
            heading = line.lstrip("* ").strip()
            continue
        for sent in _sentences(line):
            rows.append({
                "country": country, "wave": "", "source": "CONTENTS.org",
                "field": heading, "sentence": sent,
                "source_file": str(path.relative_to(countries_root().parent.parent))
                if str(path).startswith(str(countries_root())) else str(path),
                "locator": f"heading: {heading}", "confidence_of_universe_record": "",
            })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("countries", nargs="*")
    ap.add_argument("--out", default=None)
    ap.add_argument("--source", choices=["all", "population", "contents"], default="all")
    args = ap.parse_args()

    warnings.simplefilter("ignore")
    from lsms_library.paths import countries_root

    names = args.countries or sorted(
        d.name for d in countries_root().iterdir()
        if (d / "_" / "data_scheme.yml").exists()
    )

    rows: list[dict] = []
    for n in names:
        if args.source in ("all", "population"):
            try:
                rows.extend(from_population_yml(n))
            except Exception as exc:
                print(f"{n}: population.yml -> {type(exc).__name__}", file=sys.stderr)
        if args.source in ("all", "contents"):
            rows.extend(from_contents_org(n))

    cols = ["country", "wave", "source", "field", "sentence",
            "source_file", "locator", "confidence_of_universe_record"]
    fh = open(args.out, "w", newline="") if args.out else sys.stdout
    try:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    finally:
        if args.out:
            fh.close()
            print(f"wrote {args.out} ({len(rows)} candidate sentences)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
