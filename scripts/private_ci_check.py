"""Run a credentialed CI command without publishing its output.

The hosted Windows runner supplies its user-temp/profile ACL. On POSIX we also
set explicit owner-only modes. This is an Actions output boundary, not isolation
from other processes running as the same user (ledger #1294, sections 3-4).

Public output is a fixed stage label and exit status, plus three opt-in
classes: numeric JUnit counts (--junit), checked-in test file/function
locators (--public-test-failures), and, for the Windows script build only,
fingerprint pins (--public-pins, GH #1716). A pin line repeats an entry of
the workflow's own EXPECT variable, which is checked-in and already public,
and adds what the child's RESULT line reports for that table. Only a decimal
row count and a 12-digit hex digest are read from the child, and only from a
line that matches windows_build_check.py's RESULT format in full, once per
table. Neither is a survey value: the digest is a 48-bit sum of row hashes.
"""
from __future__ import annotations

import argparse
import ast
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import warnings
import xml.etree.ElementTree as ET


_STAGES = ("data-prebuild", "data-tests", "windows-script-build")
_MAX_XML_BYTES = 16 * 1024 * 1024
_MAX_SOURCE_BYTES = 1024 * 1024
_MAX_PUBLIC_LOCATIONS = 32
_PUBLIC_TEST_PATH = re.compile(r"tests/(?:[A-Za-z0-9_]+/)*test_[A-Za-z0-9_]+\.py")
_PUBLIC_NAME = re.compile(r"test[A-Za-z0-9_]*")
_CLASSNAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")
# GH #1716 pins: EXPECT="Country:table=rows:digest,..." and the child's line
# f"RESULT {spec} rows=... cols=... waves=... digest={digest:012x} ({secs:.0f}s)".
_PIN_STAGE = "windows-script-build"
_PIN_SPEC = re.compile(r"[A-Z][A-Za-z_-]{0,63}:[a-z][a-z_]{0,63}")
_PIN_ROWS = re.compile(r"[0-9]{1,9}")
_PIN_DIGEST = re.compile(r"[0-9a-f]{12}")
_PIN_RESULT = re.compile(r"RESULT (?P<spec>\S{1,129}) rows=(?P<rows>[0-9]{1,9}) cols=[0-9]{1,6} "
                         r"waves=[0-9]{1,4} digest=(?P<digest>[0-9a-f]{12}) \([0-9]{1,7}s\)")
_MAX_PINS = 16
_MAX_PIN_LINE = 4096
_MAX_PIN_LOG_BYTES = 256 * 1024 * 1024


class _PrivateParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        # argparse's normal error includes caller-controlled argument text.
        raise ValueError("invalid arguments")


def _junit_root(path: Path):
    """Bound private XML parsing; never forward content or parse exceptions."""
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
        return root
    except (OSError, ValueError, ET.ParseError):
        return None


def _junit_counts(path: Path) -> dict[str, int] | None:
    """Read numeric outcomes; arbitrary XML identifiers/text remain private."""
    root = _junit_root(path)
    if root is None:
        return None
    try:
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


def _git(root: Path, *args: str) -> bytes | None:
    """Read local Git metadata without hooks, output forwarding or network."""
    try:
        result = subprocess.run(
            ["git", "--no-optional-locks", "-c", "core.fsmonitor=false",
             "-C", str(root), *args], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=2, check=False)
        return result.stdout if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def _source_head(root: Path) -> str | None:
    raw = _git(root, "rev-parse", "--verify", "HEAD^{commit}")
    if raw is None:
        return None
    value = raw.decode("ascii", errors="replace").strip()
    return value if re.fullmatch(r"[0-9a-f]{40}", value) else None


def _checked_in_tests(root: Path, revision: str, path: str):
    """Only regular committed test blobs supply printable names, never imports."""
    if not _PUBLIC_TEST_PATH.fullmatch(path) or len(path) > 240:
        return None
    entry = _git(root, "ls-tree", "-z", revision, "--", path)
    if entry is None:
        return None
    records = entry.split(b"\0")
    if len(records) != 2 or records[1]:
        return None
    fields, separator, found_path = records[0].partition(b"\t")
    parts = fields.split()
    if (not separator or len(parts) != 3 or parts[0] not in (b"100644", b"100755")
            or parts[1] != b"blob" or found_path != path.encode("ascii")
            or not re.fullmatch(b"[0-9a-f]{40}", parts[2])):
        return None
    blob = parts[2].decode("ascii")
    size = _git(root, "cat-file", "-s", blob)
    if size is None or not size.strip().isdigit() or int(size) > _MAX_SOURCE_BYTES:
        return None
    content = _git(root, "cat-file", "blob", blob)
    if content is None or len(content) != int(size):
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            module = ast.parse(content)
    except (SyntaxError, ValueError, RecursionError):
        return None
    declared = set()
    def collect(body, classes=()):
        for item in body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if len(item.name) <= 128 and _PUBLIC_NAME.fullmatch(item.name):
                    declared.add((classes, item.name))
            elif isinstance(item, ast.ClassDef):
                collect(item.body, (*classes, item.name))
    collect(module.body)
    return declared


def _junit_failure_locations(path: Path, root: Path, revision: str | None):
    """Return only source-validated file/function identifiers and numeric counts.

    Parameter IDs, classnames and all XML text are lookup inputs only. Missing,
    ambiguous, dynamic, oversized or uncommitted identities are count-only.
    """
    document = _junit_root(path)
    if document is None:
        return None
    cache, locations = {}, {}
    unmapped = {"failures": 0, "errors": 0}
    for case in document.iter("testcase"):
        outcome = "errors" if case.find("error") is not None else (
            "failures" if case.find("failure") is not None else None)
        if outcome is None:
            continue
        matched = None
        name = case.get("name", "").partition("[")[0]
        classname = case.get("classname", "")
        file = case.get("file")
        if (revision is not None and re.fullmatch(r"[0-9a-f]{40}", revision)
                and len(name) <= 128 and _PUBLIC_NAME.fullmatch(name)
                and len(classname) <= 512
                and (not classname or _CLASSNAME.fullmatch(classname))
                and (file is None or (len(file) <= 240 and _PUBLIC_TEST_PATH.fullmatch(file)))):
            components = classname.split(".") if classname else []
            candidates = ([file] if file is not None else [
                "/".join(components[:i]) + ".py" for i in range(1, min(len(components), 16) + 1)
                if components[i - 1].startswith("test_")])
            for candidate in candidates:
                if not _PUBLIC_TEST_PATH.fullmatch(candidate):
                    continue
                module = candidate[:-3].split("/")
                if components and components[:len(module)] != module:
                    continue
                classes = tuple(components[len(module):]) if components else ()
                if candidate not in cache and len(cache) < _MAX_PUBLIC_LOCATIONS:
                    cache[candidate] = _checked_in_tests(root, revision, candidate)
                if (classes, name) in (cache.get(candidate) or ()):
                    matched = (outcome, candidate, name)
                    break
        if matched is None or (matched not in locations and len(locations) >= _MAX_PUBLIC_LOCATIONS):
            unmapped[outcome] += 1
        else:
            locations[matched] = locations.get(matched, 0) + 1
    return locations, unmapped


def _expected_pins(text: str) -> dict[str, tuple[int, str]] | None:
    """Parse EXPECT strictly; anything unexpected means no pin output at all."""
    pins: dict[str, tuple[int, str]] = {}
    for item in text.split(","):
        spec, equals, want = item.partition("=")
        rows, colon, digest = want.partition(":")
        if (not equals or not colon or spec in pins or not _PIN_SPEC.fullmatch(spec)
                or not _PIN_ROWS.fullmatch(rows) or not _PIN_DIGEST.fullmatch(digest)):
            return None
        pins[spec] = (int(rows), digest)
    return pins if 0 < len(pins) <= _MAX_PINS else None


def _pin_results(log: Path, expect: str):
    """Pair each EXPECT pin with the child's unique, fully matching RESULT line.

    Returns None when EXPECT is unusable; otherwise (spec, expected, actual)
    triples in EXPECT order, where actual is None if the child printed no such
    line, more than one, or the log could not be read within its bounds.
    """
    pins = _expected_pins(expect)
    if pins is None:
        return None
    found: dict[str, list[tuple[int, str]]] = {spec: [] for spec in pins}
    readable = True
    try:
        if log.is_symlink() or log.stat().st_size > _MAX_PIN_LOG_BYTES:
            raise ValueError("pin log outside bounds")
        with log.open("rb") as source:
            while True:
                raw = source.readline(_MAX_PIN_LINE)
                if not raw:
                    break
                if not raw.endswith(b"\n"):
                    # Overlong or unterminated: skip the rest of this line.
                    while raw and not raw.endswith(b"\n"):
                        raw = source.readline(_MAX_PIN_LINE)
                    continue
                try:
                    line = raw.decode("ascii").rstrip("\r\n")
                except UnicodeDecodeError:
                    continue
                match = _PIN_RESULT.fullmatch(line)
                if match is not None and match["spec"] in found:
                    found[match["spec"]].append((int(match["rows"]), match["digest"]))
    except (OSError, ValueError):
        readable = False
    return [(spec, want, found[spec][0] if readable and len(found[spec]) == 1 else None)
            for spec, want in pins.items()]


def _exit_status(returncode: int) -> int:
    # subprocess uses negative signal numbers on POSIX; shells use 128+signal.
    return 128 - returncode if os.name == "posix" and returncode < 0 else returncode


def main(argv: list[str] | None = None) -> int:
    stage = "ci-check"
    counts = None
    wants_junit = False
    diagnostics = None
    wants_pins = False
    pins = None
    status = 125
    try:
        parser = _PrivateParser(prog="private-ci-check", allow_abbrev=False)
        parser.add_argument("--stage", required=True, choices=_STAGES)
        parser.add_argument("--junit", metavar="FILENAME")
        parser.add_argument("--public-test-failures", action="store_true")
        parser.add_argument("--public-pins", action="store_true")
        parser.add_argument("command", nargs=argparse.REMAINDER)
        args = parser.parse_args(argv)
        stage = args.stage
        command = args.command
        if command[:1] == ["--"]:
            command = command[1:]
        if not command:
            raise ValueError("command required")
        wants_junit = args.junit is not None
        if args.public_test_failures and not wants_junit:
            raise ValueError("public test locations require JUnit")
        if args.public_pins and stage != _PIN_STAGE:
            raise ValueError("public pins belong to the Windows script build")
        wants_pins = args.public_pins
        if wants_junit and (Path(args.junit).name != args.junit
                            or args.junit in ("", ".", "..")
                            or "/" in args.junit or "\\" in args.junit):
            raise ValueError("JUnit filename must be a single path segment")
        if not wants_junit and any("{junit}" in arg for arg in command):
            raise ValueError("JUnit placeholder requires --junit")
        source_root = Path(__file__).resolve().parents[1]
        source_head = _source_head(source_root) if args.public_test_failures else None
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
            if wants_pins:
                pins = _pin_results(private_dir / "output.log", os.environ.get("EXPECT", ""))
            if junit is not None:
                if os.name == "posix" and junit.is_file() and not junit.is_symlink():
                    junit.chmod(0o600)
                counts = _junit_counts(junit)
                if args.public_test_failures:
                    diagnostics = _junit_failure_locations(junit, source_root, source_head)
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
    if diagnostics is not None:
        locations, unmapped = diagnostics
        for (outcome, path, function), count in sorted(locations.items()):
            print(f"junit {outcome} {path}::{function} count={count}")
        print("junit unmapped " + " ".join(f"{key}={value}" for key, value in unmapped.items()))
    if wants_pins and pins is None:
        print("pin unavailable")
    for spec, (rows, digest), actual in pins or ():
        line = f"pin {spec} expected rows={rows} digest={digest} actual "
        if actual is None:
            print(line + "unavailable")
        else:
            verdict = "match" if actual == (rows, digest) else "mismatch"
            print(line + f"rows={actual[0]} digest={actual[1]} {verdict}")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
