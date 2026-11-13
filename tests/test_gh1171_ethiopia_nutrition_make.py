"""Real Make routing with invented commands; no library builds or survey IO."""
from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest


PROJECT = Path(__file__).resolve().parents[1]
COUNTRY = PROJECT / "lsms_library/countries/Ethiopia/_"
WAVES = ("2011-12", "2013-14", "2015-16", "2018-19", "2021-22")
pytestmark = pytest.mark.skipif(
    shutil.which("make") is None or os.name == "nt",
    reason="requires POSIX GNU Make",
)


@pytest.fixture
def recipe(tmp_path):
    """Copy the real Makefile, replacing only executable scientific leaves."""
    build = tmp_path / "Ethiopia/_"
    build.mkdir(parents=True)
    shutil.copyfile(COUNTRY / "Makefile", build / "Makefile")
    (build / "categorical_mapping.org").write_text("invented prerequisite\n")
    data, events = tmp_path / "data", tmp_path / "events.jsonl"
    command = '''\
import json, os
from pathlib import Path
name = {name!r}
with Path(os.environ['RECIPE_EVENTS']).open('a') as stream:
    stream.write(json.dumps(name) + '\\n')
root = Path(os.environ['LSMS_DATA_DIR']) / 'Ethiopia/var'
root.mkdir(parents=True, exist_ok=True)
(root / (name + '.parquet')).write_bytes(b'inert Make output, not a DataFrame')
'''
    for name in ("food_acquired", "nutrition"):
        (build / (name + ".py")).write_text(command.format(name=name))
    for wave in WAVES:
        folder = build.parent / wave / "_"
        folder.mkdir(parents=True)
        (folder / "food_acquired.py").write_text(
            "raise RuntimeError('native recipes must not run in this test')\n"
        )
    env = os.environ.copy()
    env.update(
        PATH=str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", ""),
        LSMS_DATA_DIR=str(data),
        RECIPE_EVENTS=str(events),
        MAKEFLAGS="",
        LANG="C",
        LC_ALL="C",
    )
    # The leaves are stdlib commands, not imports of project or user startup.
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONSAFEPATH", None)

    def run(target, *, assume_native=False, expand_var=False):
        args = ["make", "--no-print-directory", "-rR", f"VAR_DIR={data}/Ethiopia/var"]
        if assume_native:
            args += [f"--assume-old=../{wave}/_/food_acquired.parquet" for wave in WAVES]
        if expand_var:
            args += ["--eval=print-var: ; @echo $(var)"]
        args += [target]
        return subprocess.run(
            args, cwd=build, env=env, capture_output=True, text=True, timeout=15,
        )

    def observed():
        return [json.loads(line) for line in events.read_text().splitlines()] if events.exists() else []

    return run, observed, data / "Ethiopia/var"


def test_cold_nutrition_does_not_build_legacy_food_or_native_recipes(recipe):
    run, observed, var = recipe
    result = run(str(var / "nutrition.parquet"))
    assert result.returncode == 0, result.stderr
    assert observed() == ["nutrition"]
    assert (var / "nutrition.parquet").exists()
    assert not (var / "food_acquired.parquet").exists()
    assert not (var / "fct.parquet").exists()


def test_undeclared_fct_target_refuses_without_running_nutrition(recipe):
    run, observed, var = recipe
    result = run(str(var / "fct.parquet"))
    assert result.returncode != 0
    assert "No rule to make target" in result.stderr
    assert observed() == []
    assert not var.exists()


def test_explicit_food_country_builder_remains_available(recipe):
    run, observed, var = recipe
    result = run(str(var / "food_acquired.parquet"), assume_native=True)
    assert result.returncode == 0, result.stderr
    assert observed() == ["food_acquired"]
    assert (var / "food_acquired.parquet").exists()
    assert not (var / "nutrition.parquet").exists()


def test_advertised_targets_match_actual_nutrition_writer_and_schema(recipe):
    run, observed, var = recipe
    result = run("print-var", expand_var=True)
    assert result.returncode == 0, result.stderr
    targets = result.stdout.split()
    assert str(var / "nutrition.parquet") in targets
    assert str(var / "fct.parquet") not in targets
    assert observed() == []
    tree = ast.parse((COUNTRY / "nutrition.py").read_text())
    outputs = [
        node.args[1].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "to_parquet"
        and len(node.args) > 1
        and isinstance(node.args[1], ast.Constant)
    ]
    assert outputs == ["../var/nutrition.parquet"]
    scheme = (COUNTRY / "data_scheme.yml").read_text()
    assert re.search(r"^  nutrition: !make\s*$", scheme, re.MULTILINE)
    assert not re.search(r"^  fct:", scheme, re.MULTILINE)
