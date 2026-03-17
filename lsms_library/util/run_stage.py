"""Helper for DVC materialization stages that need to call Make or the CLI."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from lsms_library.paths import data_root


def _python_bin() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    if os.name == "nt":
        bin_dir = repo_root / ".venv" / "Scripts"
        python_name = "python.exe"
    else:
        bin_dir = repo_root / ".venv" / "bin"
        python_name = "python"
    candidate = bin_dir / python_name
    return candidate if candidate.exists() else Path(sys.executable)


def _runtime_env() -> dict[str, str]:
    env = os.environ.copy()
    repo_root = Path(__file__).resolve().parents[2]
    current = env.get("PYTHONPATH")
    parts = [str(repo_root)]
    if current:
        parts.append(current)
    env["PYTHONPATH"] = os.pathsep.join(parts)
    python_bin = _python_bin()
    bin_dir = python_bin.parent
    env["PATH"] = os.pathsep.join([str(bin_dir), env.get("PATH", "")])
    env["PYTHON"] = str(python_bin)
    if (bin_dir.parent / "pyvenv.cfg").exists():
        env["VIRTUAL_ENV"] = str(bin_dir.parent)
    return env


def _compute_make_jobs() -> int | None:
    """Match the default Make parallelism used in Python helpers."""
    env_value = os.getenv("LSMS_MAKE_JOBS")
    if env_value:
        try:
            jobs = int(env_value)
        except ValueError:
            jobs = None
    else:
        cpu_count = os.cpu_count() or 2
        jobs = max(1, cpu_count // 2)
    if jobs and jobs > 1:
        return jobs
    return None


def _default_target(country: str, table: str, fmt: str) -> str:
    return str(data_root(country) / "var" / f"{table}.{fmt}")


def _run_make(target: str) -> None:
    cmd = ["make", "-s"]
    jobs = _compute_make_jobs()
    if jobs:
        cmd.append(f"-j{jobs}")
    cmd.append(target)
    subprocess.run(cmd, cwd=Path("_"), check=True, env=_runtime_env())


def _run_cli(country: str, table: str, fmt: str, wave: str | None, all_waves: bool, target: str) -> None:
    cmd = [
        str(_python_bin()),
        "-m",
        "lsms_library.cli",
        "materialize",
        "--country",
        country,
    ]
    if wave:
        cmd.extend(["--wave", wave])
    elif all_waves:
        cmd.append("--all-waves")
    else:
        cmd.append("--all-waves")
    cmd.extend(
        [
            "--table",
            table,
            "--format",
            fmt,
            "--out",
            target,
        ]
    )
    env = _runtime_env()
    env["LSMS_USE_DVC_CACHE"] = "false"
    subprocess.run(cmd, cwd=Path("_"), check=True, env=env)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a materialization stage via Make or CLI.")
    parser.add_argument("--backend", choices=["cli", "make"], default="cli")
    parser.add_argument("--country", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--wave")
    parser.add_argument("--all-waves", action="store_true")
    parser.add_argument("--target", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target = args.target or _default_target(args.country, args.table, args.format)
    if args.backend == "make":
        _run_make(target)
    else:
        _run_cli(args.country, args.table, args.format, args.wave, args.all_waves, target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
