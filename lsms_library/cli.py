"""Typer-powered command-line helpers for lsms_library."""

from __future__ import annotations

from collections import OrderedDict
import os
from pathlib import Path
from typing import List, Optional, Sequence

import typer
import yaml

from .country import Country, _log_issue
from .local_tools import to_parquet
from .util.generate_dvc_stages import discover_countries as _discover_dvc_countries
from .util.generate_dvc_stages import generate_country as _generate_dvc_country

app = typer.Typer(help="Command-line tools for interacting with LSMS Library data.")
cache_app = typer.Typer(help="Manage LSMS cache.")
app.add_typer(cache_app, name="cache")


class _OrderedLoader(yaml.SafeLoader):
    pass


class _OrderedDumper(yaml.SafeDumper):
    pass


def _construct_mapping(loader, node):
    loader.flatten_mapping(node)
    return OrderedDict(loader.construct_pairs(node))


_OrderedLoader.add_constructor(  # type: ignore[arg-type]
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)
_OrderedDumper.add_representer(  # type: ignore[arg-type]
    OrderedDict, yaml.representer.SafeRepresenter.represent_dict
)


def _load_yaml(path: Path) -> OrderedDict:
    if path.exists():
        data = yaml.load(path.read_text(), Loader=_OrderedLoader)
        if data is None:
            return OrderedDict()
        if not isinstance(data, OrderedDict):
            return OrderedDict(data)  # type: ignore[arg-type]
        return data
    return OrderedDict()


def _dump_yaml(data: OrderedDict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data, Dumper=_OrderedDumper, sort_keys=False))


def _available_country_dirs() -> List[str]:
    countries_root = Path(__file__).resolve().parent / "countries"
    names = [
        path.name
        for path in countries_root.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    ]
    return sorted(names)


def _print_list(items: Sequence[str], as_csv: bool) -> None:
    if as_csv:
        print(",".join(items))
    else:
        for item in items:
            print(item)


def _load_table(country: str, table: str, waves: Optional[Sequence[str]]) -> object:
    """Return the requested table, optionally aggregated across multiple waves."""

    country_obj = Country(country, preload_panel_ids=False)

    try:
        if waves is None:
            loader = getattr(country_obj, table)
            return loader()

        wave_list: List[str] = list(waves)
        if len(wave_list) == 1:
            wave_obj = country_obj[wave_list[0]]
            loader = getattr(wave_obj, table)
            return loader()

        loader = getattr(country_obj, table)
        return loader(waves=wave_list)
    except KeyError as exc:
        raise ValueError(f"Wave {exc.args[0]!r} is not available for {country}.") from exc
    except AttributeError as exc:
        raise ValueError(
            f"Table '{table}' is not available for {country}."
        ) from exc


def _materialize(
    *,
    country: str,
    waves: Sequence[str] | None,
    table: str,
    output: Path,
    file_format: str = "parquet",
    include_index: bool = True,
) -> Path:
    """Load a table through the Country/Wave API and persist it."""

    df = _load_table(country, table, waves)

    output.parent.mkdir(parents=True, exist_ok=True)

    if file_format == "parquet":
        to_parquet(df, output, index=include_index)
    elif file_format == "csv":
        df.to_csv(output, index=include_index)
    else:
        raise ValueError(f"Unsupported format '{file_format}'.")

    return output


@cache_app.command("list")
def cache_list(
    country: Optional[str] = typer.Option(None, "--country", help="Country name to inspect."),
) -> None:
    """List cached datasets."""
    countries = [country] if country else _available_country_dirs()
    countries = [c for c in countries if (Path(__file__).resolve().parent / "countries" / c).exists()]
    for name in countries:
        country_obj = Country(name, preload_panel_ids=False)
        datasets = country_obj.cached_datasets()
        if datasets:
            print(f"{name}: {', '.join(sorted(datasets))}")
        else:
            print(f"{name}: <no cache>")


@cache_app.command("clear")
def cache_clear(
    country: Optional[List[str]] = typer.Option(None, "--country", help="Country to clear (repeatable)."),
    method: Optional[List[str]] = typer.Option(None, "--method", help="Dataset/method to clear (repeatable)."),
    wave: Optional[List[str]] = typer.Option(None, "--wave", help="Wave identifier (repeatable)."),
    all_countries: bool = typer.Option(False, "--all", help="Clear cache for all countries."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show files that would be removed without deleting."),
) -> None:
    """Clear cached datasets for one or more countries."""
    if not all_countries and not country:
        raise typer.BadParameter("Provide --country or use --all to target every country.")

    target_countries = (
        _available_country_dirs() if all_countries else list(dict.fromkeys(country))
    )

    any_output = False
    for name in target_countries:
        country_obj = Country(name, preload_panel_ids=False)
        methods = list(dict.fromkeys(method)) if method else None
        files = country_obj.clear_cache(methods=methods, waves=wave or None, dry_run=dry_run)
        if files:
            any_output = True
            prefix = "[DRY RUN] " if dry_run else ""
            action = "would remove" if dry_run else "removed"
            print(f"{prefix}{name}: {action}")
            for path in files:
                print(f"  {path}")

    if not any_output:
        print("No cached files matched the provided filters.")


def _slug(value: str) -> str:
    return value.lower().replace(" ", "_").replace("-", "_")


def _register_stage(
    *,
    country: str,
    wave: str,
    table: str,
    file_format: str,
    dvc_file: Path,
    lock_file: Path | None,
) -> str:
    data = _load_yaml(dvc_file)
    stages = data.setdefault("stages", OrderedDict())
    materialize = stages.setdefault("materialize", OrderedDict())
    foreach = materialize.setdefault("foreach", OrderedDict())

    has_wave = bool(wave and wave.lower() not in {"", "all_waves"})
    wave_value = wave if has_wave else None

    if has_wave:
        stage_key = f"{_slug(country)}::{_slug(wave_value)}::{_slug(table)}"
        cmd_wave_part = " --wave ${item.wave}"
    else:
        stage_key = f"{_slug(country)}::::{_slug(table)}"
        cmd_wave_part = " --all-waves"
    out_template = "var/${item.table}.${item.format}"

    foreach[stage_key] = OrderedDict(
        (
            ("country", country),
            ("wave", wave_value),
            ("table", table),
            ("format", file_format),
        )
    )

    import os

    module_dir = Path(__file__).resolve().parent
    dvc_parent = dvc_file.parent

    (dvc_parent / "var").mkdir(parents=True, exist_ok=True)

    cli_rel = os.path.relpath(module_dir / "cli.py", dvc_parent)
    country_rel = os.path.relpath(module_dir / "country.py", dvc_parent)
    tools_rel = os.path.relpath(module_dir / "local_tools.py", dvc_parent)

    do = materialize.setdefault("do", OrderedDict())
    cmd = (
        "python3 -m lsms_library.cli materialize"
        f" --country {{item.country}}{cmd_wave_part}"
        " --table ${item.table} --format ${item.format}"
        f" --out {out_template}"
    )
    do["cmd"] = cmd

    deps = do.setdefault("deps", [])
    deps[:] = []
    deps.extend([cli_rel, country_rel, tools_rel, "_"])

    outs = do.setdefault("outs", [])
    outs[:] = []
    outs.append(out_template)

    _dump_yaml(data, dvc_file)

    if lock_file and lock_file.exists():
        lock = _load_yaml(lock_file)
        stages_map = lock.get("stages")
        if isinstance(stages_map, dict):
            removed = False
            for candidate in (
                f"materialize@{stage_key}",
                f"materialize@{stage_key.lower()}",
            ):
                if candidate in stages_map:
                    stages_map.pop(candidate)
                    removed = True
            if removed and not stages_map:
                lock.pop("stages", None)
        _dump_yaml(lock, lock_file)

    return stage_key


@app.command()
def materialize(
    country: str = typer.Option(..., "--country", help="Country name (e.g., Malawi)."),
    table: str = typer.Option(..., "--table", help="Table name to materialize."),
    wave: Optional[List[str]] = typer.Option(
        None,
        "--wave",
        help="Specify one or more waves (repeat flag). If omitted, all waves are aggregated.",
    ),
    all_waves: bool = typer.Option(
        False,
        "--all-waves/--no-all-waves",
        help="Aggregate across every available wave.",
    ),
    format: str = typer.Option(
        "parquet",
        "--format",
        case_sensitive=False,
        help="Serialization format for the output (parquet or csv).",
    ),
    out: Path = typer.Option(
        ...,
        "--out",
        help="Destination file path for the materialized data.",
    ),
    no_index: bool = typer.Option(
        False,
        "--no-index",
        help="Do not include the index when writing the file.",
    ),
) -> None:
    """Persist a table using the YAML-defined API."""

    if wave and all_waves:
        raise typer.BadParameter("Use either --wave or --all-waves, not both.", param_hint=["--wave", "--all-waves"])

    waves: Optional[Sequence[str]]
    if wave:
        waves = wave
    elif all_waves:
        waves = None
    else:
        waves = None  # default: aggregate all available waves

    output_path = _materialize(
        country=country,
        waves=waves,
        table=table,
        output=out,
        file_format=format,
        include_index=not no_index,
    )
    typer.echo(output_path)


@app.command()
def register(
    country: str = typer.Argument(..., help="Country name (e.g., Uganda)."),
    wave: str = typer.Option(..., "--wave", help="Wave identifier (e.g., 2023-24)."),
    table: str = typer.Option(..., "--table", help="Table to register."),
    format: str = typer.Option(
        "parquet",
        "--format",
        case_sensitive=False,
        help="Serialization format for the output (parquet or csv).",
    ),
    dvc_file: Path | None = typer.Option(
        None,
        "--dvc-file",
        help="Path to the DVC YAML file (defaults to per-country file).",
    ),
    lock_file: Path | None = typer.Option(
        None,
        "--lock-file",
        help="Path to the DVC lock file (defaults to per-country file).",
    ),
) -> None:
    """Register a DVC materialization stage for a table."""

    base_dir = Path(__file__).resolve().parent / "countries" / country
    has_wave = bool(wave and wave.lower() not in {"", "all_waves"})

    if dvc_file is None:
        dvc_file = base_dir / (f"{wave}/dvc.yaml" if has_wave else "dvc.yaml")
    if lock_file is None:
        lock_file = dvc_file.with_name("dvc.lock")

    dvc_file.parent.mkdir(parents=True, exist_ok=True)

    stage_key = _register_stage(
        country=country,
        wave=wave,
        table=table,
        file_format=format,
        dvc_file=dvc_file,
        lock_file=lock_file,
    )
    typer.echo(
        f"Registered stage materialize@{stage_key}.\n"
        f"Run 'dvc repro {dvc_file}:materialize@{stage_key}' to materialize."
    )


@app.command("generate-dvc")
def generate_dvc(
    country: Optional[List[str]] = typer.Option(
        None,
        "--country",
        help="Country to process (repeatable). Omit with --all to regenerate every country.",
    ),
    all_countries: bool = typer.Option(
        False,
        "--all/--no-all",
        help="Regenerate DVC configs for every known country.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--write",
        help="Show pending changes without touching files.",
    ),
) -> None:
    """Regenerate DVC stage definitions for one or more countries."""

    if not country and not all_countries:
        raise typer.BadParameter(
            "Provide --country (repeatable) or use --all.", param_hint=["--country", "--all"]
        )

    targets = sorted(set(country)) if country else _discover_dvc_countries()
    changed_any = False

    for name in targets:
        changed = _generate_dvc_country(name, dry_run)
        changed_any |= changed
        verb = "Updated" if changed else "Unchanged"
        typer.echo(f"{verb} {name}")

    if dry_run:
        typer.echo("[DRY] No files were written.")
    elif not changed_any:
        typer.echo("[INFO] All dvc.yaml files already up to date.")


@app.command()
def countries(as_csv: bool = typer.Option(False, "--as-csv", help="Emit comma-separated list.")) -> None:
    """List supported countries."""

    _print_list(_available_country_dirs(), as_csv)


@app.command()
def waves(
    country: Optional[str] = typer.Option(
        None,
        "--country",
        help="Country name (e.g., Malawi). Omit to list waves for every country.",
    ),
    all_countries: bool = typer.Option(
        False,
        "--all-countries/--no-all-countries",
        help="List all country/wave combinations.",
    ),
    as_csv: bool = typer.Option(False, "--as-csv", help="Emit comma-separated list."),
) -> None:
    """List waves available for a country."""

    if not country and not all_countries:
        raise typer.BadParameter(
            "Provide --country or --all-countries.", param_hint=["--country", "--all-countries"]
        )

    if all_countries:
        pairs = []
        for name in _available_country_dirs():
            waves = Country(name, preload_panel_ids=False).waves
            pairs.extend(f"{name},{wave}" for wave in waves)
        _print_list(pairs, as_csv)
    else:
        country_obj = Country(country, preload_panel_ids=False)  # type: ignore[arg-type]
        _print_list(country_obj.waves, as_csv)


@app.command()
def tables(
    country: Optional[str] = typer.Option(
        None,
        "--country",
        help="Country name (e.g., Malawi). Omit to list tables for every country.",
    ),
    all_countries: bool = typer.Option(
        False,
        "--all-countries/--no-all-countries",
        help="List all country/table combinations.",
    ),
    as_csv: bool = typer.Option(False, "--as-csv", help="Emit comma-separated list."),
) -> None:
    """List data tables (data scheme) for a country."""

    if not country and not all_countries:
        raise typer.BadParameter(
            "Provide --country or --all-countries.", param_hint=["--country", "--all-countries"]
        )

    if all_countries:
        pairs = []
        for name in _available_country_dirs():
            tables = Country(name, preload_panel_ids=False).data_scheme
            if not tables:
                _log_issue(name, "tables", None, ValueError("No tables defined in data scheme"))
            pairs.extend(f"{name},{table}" for table in tables)
        _print_list(pairs, as_csv)
    else:
        country_obj = Country(country, preload_panel_ids=False)  # type: ignore[arg-type]
        tables = country_obj.data_scheme
        if not tables:
            _log_issue(country, "tables", None, ValueError("No tables defined in data scheme"))
            typer.echo(f"No tables available for {country}.")
            raise typer.Exit(1)
        _print_list(tables, as_csv)


def main() -> None:  # pragma: no cover - Typer handles CLI invocation
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
