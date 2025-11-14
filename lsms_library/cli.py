"""Lightweight command-line helpers for lsms_library."""

from __future__ import annotations

import argparse
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, List, Sequence

import yaml

from .country import Country


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


def _load_table(country: str, table: str, waves: Sequence[str] | None) -> object:
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
        df.to_parquet(output, index=include_index)
    elif file_format == "csv":
        df.to_csv(output, index=include_index)
    else:
        raise ValueError(f"Unsupported format '{file_format}'.")

    return output


_DEFAULT_CMD = (
    "python3 -m lsms_library.cli materialize\n"
    "        --country ${item.country}\n"
    "        --wave ${item.wave}\n"
    "        --table ${item.table}\n"
    "        --format ${item.format}\n"
    "        --out build/${item.country}/${item.wave}/${item.table}.${item.format}"
)

_DEFAULT_DEPS = [
    "../cli.py",
    "../country.py",
    "../local_tools.py",
    "${item.country}/_",
    "${item.country}/${item.wave}/_",
]

_DEFAULT_OUTS = [
    "build/${item.country}/${item.wave}/${item.table}.${item.format}",
]


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

    stage_key = f"{_slug(country)}_{_slug(wave)}_{_slug(table)}"

    foreach[stage_key] = OrderedDict(
        (
            ("country", country),
            ("wave", wave),
            ("table", table),
            ("format", file_format),
        )
    )

    do = materialize.setdefault("do", OrderedDict())
    do.setdefault("cmd", _DEFAULT_CMD)
    deps = do.setdefault("deps", list(_DEFAULT_DEPS))
    outs = do.setdefault("outs", list(_DEFAULT_OUTS))

    # Ensure defaults are present (handles older files without placeholders)
    for item in _DEFAULT_DEPS:
        if item not in deps:
            deps.append(item)
    for item in _DEFAULT_OUTS:
        if item not in outs:
            outs.append(item)

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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lsms-library")
    subparsers = parser.add_subparsers(dest="command", required=True)

    materialize = subparsers.add_parser(
        "materialize",
        help="Persist a table using the YAML-defined API.",
    )
    materialize.add_argument("--country", required=True)
    materialize.add_argument("--table", required=True)
    wave_group = materialize.add_mutually_exclusive_group()
    wave_group.add_argument(
        "--wave",
        dest="waves",
        action="append",
        metavar="WAVE",
        help="Specify a survey wave (repeat flag to list multiple waves).",
    )
    wave_group.add_argument(
        "--all-waves",
        action="store_true",
        help="Aggregate across every available wave for the country.",
    )
    materialize.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Destination file (parquet or csv).",
    )
    materialize.add_argument(
        "--format",
        choices=("parquet", "csv"),
        default="parquet",
        help="Serialization format (default: parquet).",
    )
    materialize.add_argument(
        "--no-index",
        action="store_true",
        help="Do not include the index when writing the file.",
    )

    register = subparsers.add_parser(
        "register",
        help="Register a DVC materialization stage for a table.",
    )
    register.add_argument("--country", required=True)
    register.add_argument("--wave", required=True)
    register.add_argument("--table", required=True)
    register.add_argument(
        "--format",
        choices=("parquet", "csv"),
        default="parquet",
    )
    register.add_argument(
        "--dvc-file",
        type=Path,
        default=Path(__file__).resolve().parent / "countries" / "dvc.yaml",
        help="Path to the DVC YAML file (defaults to countries/dvc.yaml).",
    )
    register.add_argument(
        "--lock-file",
        type=Path,
        default=Path(__file__).resolve().parent / "countries" / "dvc.lock",
        help="Path to the DVC lock file (defaults to countries/dvc.lock).",
    )

    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "materialize":
        if getattr(args, "waves", None):
            waves: Sequence[str] | None = args.waves
        elif getattr(args, "all_waves", False):
            waves = None
        else:
            waves = None  # Default to stacking all waves when --wave is omitted
        output = _materialize(
            country=args.country,
            waves=waves,
            table=args.table,
            output=args.out,
            file_format=args.format,
            include_index=not args.no_index,
        )
        print(output)
    elif args.command == "register":
        stage_key = _register_stage(
            country=args.country,
            wave=args.wave,
            table=args.table,
            file_format=args.format,
            dvc_file=args.dvc_file,
            lock_file=args.lock_file,
        )
        print(
            "Registered stage materialize@{} (run 'cd lsms_library/countries && dvc repro materialize@{}' to materialize).".format(
                stage_key, stage_key
            )
        )


if __name__ == "__main__":  # pragma: no cover
    main()
