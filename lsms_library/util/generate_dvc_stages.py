#!/usr/bin/env python3
"""
Generate DVC materialization configs for LSMS countries and waves.

The generator inspects the legacy metadata (data_scheme.yml + data_info.yml)
and emits dvc.yaml files that match the structure we hand-authored for Malawi.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
import sys

from lsms_library.country import _slugify as _country_slugify
from lsms_library.yaml_utils import load_yaml as load_yaml_with_tags

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
LSMS_ROOT = REPO_ROOT / "lsms_library"
COUNTRIES_ROOT = LSMS_ROOT / "countries"

CORE_DEPS = [
    LSMS_ROOT / "cli.py",
    LSMS_ROOT / "country.py",
    LSMS_ROOT / "local_tools.py",
]

JSON_ONLY_TABLES = {"panel_ids", "updated_ids"}


@dataclass(frozen=True)
class StageEntry:
    country: str
    wave: str | None
    table: str
    fmt: str = "parquet"
    backend: str = "cli"
    target: str | None = None

    @property
    def key(self) -> str:
        country_slug = slugify(self.country)
        wave_slug = slugify(self.wave) if self.wave else ""
        table_slug = slugify(self.table)
        return f"{country_slug}::{wave_slug}::{table_slug}"


def slugify(value: str | None) -> str:
    if value is None:
        return ""
    return _country_slugify(str(value))


def load_yaml_file(path: Path) -> dict:
    if not path.exists():
        return {}
    data = load_yaml_with_tags(path)
    return data if isinstance(data, dict) else {}


def country_stage_entries(country: str, country_dir: Path) -> list[StageEntry]:
    scheme_path = country_dir / "_" / "data_scheme.yml"
    data = load_yaml_file(scheme_path)
    if not isinstance(data, dict):
        return []
    scheme = data.get("Data Scheme")
    if not isinstance(scheme, dict):
        return []
    entries: list[StageEntry] = []
    for key, value in scheme.items():
        if not isinstance(key, str):
            continue
        if key in JSON_ONLY_TABLES:
            continue
        info = value if isinstance(value, dict) else {}
        backend = str(info.get("materialize", "cli")).lower()
        backend = "make" if backend == "make" else "cli"
        target = info.get("target")
        fmt = info.get("format", "parquet")
        entries.append(
            StageEntry(
                country=country,
                wave=None,
                table=key,
                fmt=fmt,
                backend=backend,
                target=target,
            )
        )
    entries.sort(key=lambda entry: entry.table)
    return entries


def wave_tables(wave_dir: Path) -> list[str]:
    info_path = wave_dir / "_" / "data_info.yml"
    data = load_yaml_file(info_path)
    if not isinstance(data, dict):
        return []
    skip = {"Country", "Wave"}
    tables: list[str] = []
    for key in data.keys():
        if isinstance(key, str) and key not in skip:
            tables.append(key)
    return sorted(tables)


def ensure_gitkeep(var_dir: Path, dry_run: bool) -> None:
    if dry_run:
        return
    var_dir.mkdir(parents=True, exist_ok=True)
    gitkeep = var_dir / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.write_text("", encoding="utf-8")


def relative_path(path: Path, start: Path) -> str:
    return os.path.relpath(path, start).replace(os.sep, "/")


def build_materialize_block(
    entries: Iterable[StageEntry],
    target_dir: Path,
    include_wave: bool,
) -> dict:
    entries = list(entries)
    if not entries:
        return {}

    foreach: dict[str, dict[str, str | None]] = {}
    for entry in entries:
        foreach_entry: dict[str, str | None] = {
            "country": entry.country,
            "wave": entry.wave,
            "table": entry.table,
            "format": entry.fmt,
            "backend": entry.backend,
            "target": entry.target or "",
        }
        foreach[entry.key] = foreach_entry

    if include_wave:
        wave_arg = '--wave "${item.wave}"'
    else:
        wave_arg = "--all-waves"

    runner_path = relative_path(LSMS_ROOT / "util" / "run_stage.py", target_dir)

    cmd = (
        f"python3 {runner_path} "
        '--backend "${item.backend}" '
        '--country "${item.country}" '
        f"{wave_arg} "
        '--table "${item.table}" '
        '--format "${item.format}" '
        '--target "${item.target}"'
    )

    deps = [relative_path(dep, target_dir) for dep in CORE_DEPS]
    deps.append("_")

    materialize = {
        "do": {
            "cmd": cmd,
            "deps": deps,
            "outs": ["var/${item.table}.${item.format}"],
        },
        "foreach": foreach,
    }
    return {"stages": {"materialize": materialize}}


def dump_yaml(path: Path, data: dict, dry_run: bool) -> bool:
    if not data:
        return False
    text = yaml.safe_dump(data, sort_keys=False)
    current = path.read_text(encoding="utf-8") if path.exists() else None
    if current == text:
        return False
    if dry_run:
        print(f"[DRY] would write {path}")
        return True
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return True


def wave_dirs(country_dir: Path) -> Iterable[Path]:
    for child in sorted(country_dir.iterdir()):
        if not child.is_dir():
            continue
        if child.name in {"_", "var", "Documentation"}:
            continue
        if not (child / "_").is_dir():
            continue
        yield child


def generate_country(country: str, dry_run: bool) -> bool:
    country_dir = COUNTRIES_ROOT / country
    if not country_dir.is_dir():
        print(f"[WARN] Skipping unknown country {country}", file=sys.stderr)
        return False

    changed = False
    # Country-level stage
    entries = country_stage_entries(country, country_dir)
    if entries:
        ensure_gitkeep(country_dir / "var", dry_run)
        data = build_materialize_block(entries, country_dir, include_wave=False)
        changed |= dump_yaml(country_dir / "dvc.yaml", data, dry_run)

    # Wave-level stages
    for wave_dir in wave_dirs(country_dir):
        tables = wave_tables(wave_dir)
        if not tables:
            continue
        ensure_gitkeep(wave_dir / "var", dry_run)
        entries = [StageEntry(country, wave_dir.name, t) for t in tables]
        data = build_materialize_block(entries, wave_dir, include_wave=True)
        changed |= dump_yaml(wave_dir / "dvc.yaml", data, dry_run)

    return changed


def discover_countries() -> list[str]:
    countries = []
    for child in sorted(COUNTRIES_ROOT.iterdir()):
        if not child.is_dir():
            continue
        if not (child / "_").is_dir():
            continue
        countries.append(child.name)
    return countries


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Process every country with metadata.")
    group.add_argument("--countries", nargs="+", help="Specific countries to process.")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without writing files.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    countries = discover_countries() if args.all else sorted(set(args.countries or []))
    if not countries:
        print("[INFO] No countries selected.")
        return 0

    changed_any = False
    for country in countries:
        changed = generate_country(country, args.dry_run)
        changed_any |= changed
        verb = "Updated" if changed else "Unchanged"
        print(f"{verb} {country}")

    if args.dry_run:
        print("[DRY] No files were written.")
    elif not changed_any:
        print("[INFO] All dvc.yaml files already up to date.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
