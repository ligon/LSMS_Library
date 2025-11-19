import json
from pathlib import Path
from typing import List

import typer
import typer.testing

from lsms_library import cli as lsms_cli


def _available_sample_countries() -> List[str]:
    countries_root = Path(lsms_cli.__file__).resolve().parent / "countries"
    return sorted(
        path.name for path in countries_root.iterdir() if path.is_dir() and (path / "_").exists()
    )


cli_runner = typer.testing.CliRunner()


def test_generate_dvc_dry_run(tmp_path, monkeypatch):
    sample_country = _available_sample_countries()[0]

    recorded = {}

    def fake_generate(country: str, dry_run: bool) -> bool:
        recorded.setdefault("calls", []).append((country, dry_run))
        return False

    monkeypatch.setattr(lsms_cli, "_generate_dvc_country", fake_generate)
    result = cli_runner.invoke(
        lsms_cli.app,
        ["generate-dvc", "--country", sample_country, "--dry-run"],
    )

    assert result.exit_code == 0
    assert recorded["calls"] == [(sample_country, True)]
    assert "Unchanged" in result.stdout
    assert "[DRY]" in result.stdout


def test_generate_dvc_all(monkeypatch):
    calls = []

    def fake_generate(country: str, dry_run: bool) -> bool:
        calls.append(country)
        return country.endswith("a")

    monkeypatch.setattr(lsms_cli, "_generate_dvc_country", fake_generate)
    result = cli_runner.invoke(lsms_cli.app, ["generate-dvc", "--all", "--dry-run"])

    assert result.exit_code == 0
    assert calls  # ensures we iterated over discovered countries
    assert result.stdout.count("Updated") == sum(name.endswith("a") for name in calls)
    assert "[DRY]" in result.stdout
