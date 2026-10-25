"""Run a credentialed CI command without publishing its output.

The hosted Windows runner supplies its user-temp/profile ACL. On POSIX we also
set explicit owner-only modes. This is an Actions output boundary, not isolation
from other processes running as the same user (ledger #1294, sections 3-4).
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET


_STAGES = ("data-prebuild", "data-tests", "windows-script-build")
_MAX_XML_BYTES = 16 * 1024 * 1024


class _PrivateParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        # argparse's normal error includes caller-controlled argument text.
        raise ValueError("invalid arguments")


def _junit_counts(path: Path) -> dict[str, int] | None:
    """Read only numeric outcomes; XML names/text are never public (ledger §3)."""
    try:
        if path.is_symlink() or path.stat().st_size > _MAX_XML_BYTES:
            return None
        with path.open("rb") as source:
            content = source.read(_MAX_XML_BYTES + 1)
        if len(content) > _MAX_XML_BYTES or b"<!DOCTYPE" in content.upper():
            return None
        root = ET.fromstring(content)
        if root.tag not in ("testsuite", "testsuites"):
            return None
        counts = dict(tests=0, passed=0, failures=0, errors=0, skipped=0)
        for case in root.iter("testcase"):
            counts["tests"] += 1
            outcome = "passed"
            for tag, category in (("error", "errors"), ("failure", "failures"),
                                  ("skipped", "skipped")):
                if case.find(tag) is not None:
                    outcome = category
                    break
            counts[outcome] += 1
        return counts
    except (OSError, ValueError, ET.ParseError):
        return None


def _exit_status(returncode: int) -> int:
    # subprocess uses negative signal numbers on POSIX; shells use 128+signal.
    return 128 - returncode if os.name == "posix" and returncode < 0 else returncode


def main(argv: list[str] | None = None) -> int:
    stage = "ci-check"
    counts = None
    wants_junit = False
    status = 125
    try:
        parser = _PrivateParser(prog="private-ci-check", allow_abbrev=False)
        parser.add_argument("--stage", required=True, choices=_STAGES)
        parser.add_argument("--junit", metavar="FILENAME")
        parser.add_argument("command", nargs=argparse.REMAINDER)
        args = parser.parse_args(argv)
        stage = args.stage
        command = args.command
        if command[:1] == ["--"]:
            command = command[1:]
        if not command:
            raise ValueError("command required")
        wants_junit = args.junit is not None
        if wants_junit and (Path(args.junit).name != args.junit
                            or args.junit in ("", ".", "..")
                            or "/" in args.junit or "\\" in args.junit):
            raise ValueError("JUnit filename must be a single path segment")
        if not wants_junit and any("{junit}" in arg for arg in command):
            raise ValueError("JUnit placeholder requires --junit")
        # One private directory contains both streams and optional XML. Never
        # forward child output, even on startup/parse/cleanup failures (§4).
        with tempfile.TemporaryDirectory(prefix="lsms-private-ci-") as private:
            private_dir = Path(private)
            if os.name == "posix":
                private_dir.chmod(0o700)
            junit = private_dir / args.junit if wants_junit else None
            if junit is not None:
                command = [arg.replace("{junit}", str(junit)) for arg in command]
            with (private_dir / "output.log").open("xb") as output:
                if os.name == "posix":
                    os.fchmod(output.fileno(), 0o600)
                result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT,
                                        stdin=subprocess.DEVNULL, check=False)
            status = _exit_status(result.returncode)
            if junit is not None:
                if os.name == "posix" and junit.is_file() and not junit.is_symlink():
                    junit.chmod(0o600)
                counts = _junit_counts(junit)
    except KeyboardInterrupt:
        status = 130
    except Exception:
        # Includes startup, XML permissions, invalid arguments and cleanup.
        # If the command finished, its actual status remains authoritative.
        pass
    print(f"private-ci-check stage={stage} exit={status}")
    if counts is not None:
        print("junit " + " ".join(f"{key}={value}" for key, value in counts.items()))
    elif wants_junit:
        print("junit unavailable")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
