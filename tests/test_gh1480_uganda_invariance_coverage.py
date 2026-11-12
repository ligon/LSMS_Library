"""Hermetic coverage/COMPLETE controls; no scientific baseline is rewritten."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
import os
import subprocess
import sys
import importlib.util

import pandas as pd
import pytest

from scripts import check_uganda_invariance as cli
from tests import test_uganda_invariance as invariance
from tests.uganda_invariance_reporting import (
    FIELDS, PROPERTY, entry_result, pytest_coverage, summarize, summary_lines,
)


KEY = "var/sample.parquet"
WAVES = ["2018-19", "2019-20"]
TABLES = {"sample", "nutrition"}


@pytest.fixture
def cohort(tmp_path):
    root = tmp_path / "data"
    path = root / "Uganda" / KEY
    path.parent.mkdir(parents=True)
    pd.DataFrame({"weight": [1.0, 2.0]}, index=pd.Index(["a", "b"], name="i")).to_parquet(path)
    entry = invariance._fingerprint(pd.read_parquet(path))
    return root, path, {KEY: entry}


def test_empty_root_counts_every_declared_entry_and_required_field(tmp_path):
    entry = {field: "pinned" for field in FIELDS}
    baseline = {KEY: entry, "2018-19/_/nutrition.parquet": entry}
    report = cli.check(tmp_path, baseline, WAVES, TABLES)
    assert (report["required_entries"], report["required_fields"]) == (2, 10)
    assert report["entry_counts"] == {"missing": 2}
    assert report["field_counts"]["not_compared"] == 10
    assert not report["coverage_complete"] and not report["invariance_passed"]


def test_full_and_partial_inventory_use_manifest_size_not_a_fixed_count(cohort):
    root, path, baseline = cohort
    full = cli.check(root, baseline, WAVES, TABLES)
    assert full["coverage_complete"] and full["invariance_passed"]
    assert full["compared_entries"] == 1 and full["checked_fields"] == 5
    baseline["var/nutrition.parquet"] = deepcopy(baseline[KEY])
    partial = cli.check(root, baseline, WAVES, TABLES)
    assert partial["entry_counts"] == {"matched": 1, "missing": 1}
    assert partial["checked_fields"] == 5 and partial["required_fields"] == 10
    assert not partial["coverage_complete"]


def test_mismatch_compares_all_five_fields_and_complete_coverage_still_fails(cohort):
    root, path, baseline = cohort
    baseline[KEY]["shape"] = [90, 1]
    baseline[KEY]["content_hash"] = "0" * 64
    report = cli.check(root, baseline, WAVES, TABLES)
    assert report["coverage_complete"] and not report["invariance_passed"]
    assert report["entry_counts"] == {"mismatch": 1}
    assert report["field_counts"] == {"passed": 3, "failed": 2, "not_compared": 0, "unpinned": 0}
    assert "shape mismatch" in report["entries"][0]["reason"]
    assert "content hash mismatch" in report["entries"][0]["reason"]


@pytest.mark.parametrize("before,after", [(["2018-19"], None), (None, ["2018-19"])])
def test_partial_wave_footer_before_or_after_read_is_not_compared(cohort, monkeypatch, before, after):
    from lsms_library import local_tools
    root, path, baseline = cohort
    replies = iter([before, after])
    monkeypatch.setattr(local_tools, "read_parquet_cache_waves", lambda path: next(replies))
    report = cli.check(root, baseline, WAVES, TABLES)
    assert report["entry_counts"] == {"partial": 1}
    assert report["checked_fields"] == 0 and not report["invariance_passed"]


def test_missing_footer_keeps_existing_comparison_contract(cohort):
    root, path, baseline = cohort
    result = cli.check(root, baseline, WAVES, TABLES)
    assert result["entries"][0]["coverage_metadata_absent"]
    assert result["invariance_passed"]


def test_corrupt_cache_error_is_distinct_from_missing_or_partial(cohort):
    root, path, baseline = cohort
    path.write_bytes(b"not parquet")
    result = cli.check(root, baseline, WAVES, TABLES)
    assert result["entry_counts"] == {"error": 1}
    assert result["checked_fields"] == 0 and not result["invariance_passed"]


def test_baseline_error_is_never_a_pass(cohort):
    root, path, baseline = cohort
    result = cli.check(root, {KEY: {"error": "historical failure"}}, WAVES, TABLES)
    assert result["entry_counts"] == {"baseline_error": 1}
    assert not result["coverage_complete"] and not result["invariance_passed"]
    assert result["field_counts"]["unpinned"] == 0


def test_absent_baseline_fields_are_not_silently_unpinned(cohort):
    root, path, baseline = cohort
    baseline[KEY].pop("content_hash")
    result = cli.check(root, baseline, WAVES, TABLES)
    assert result["entry_counts"] == {"baseline_error": 1}
    assert result["field_counts"]["unpinned"] == 0 and not result["invariance_passed"]


def test_unpinned_debt_preserves_existing_null_semantics_and_is_reported(cohort):
    root, path, baseline = cohort
    baseline[KEY]["content_hash"] = None
    result = cli.check(root, baseline, WAVES, TABLES)
    assert result["invariance_passed"] and result["required_fields"] == 4
    assert result["field_counts"]["unpinned"] == 1
    baseline[KEY] = dict.fromkeys(FIELDS)
    empty = cli.check(root, baseline, WAVES, TABLES)
    assert empty["entry_counts"] == {"unpinned": 1} and not empty["invariance_passed"]


@pytest.mark.parametrize("key", ["var/typo.parquet", "2030-31/_/sample.parquet",
                                 "../sample.parquet", "/var/sample.parquet"])
def test_unknown_baseline_keys_are_not_silently_missing_or_admitted(cohort, key):
    root, path, baseline = cohort
    result = cli.check(root, {key: baseline[KEY]}, WAVES, TABLES)
    assert result["entry_counts"] == {"unknown_key": 1}
    assert not result["invariance_passed"]


def test_extra_file_names_are_separate_from_required_baseline_scope(cohort):
    root, path, baseline = cohort
    (path.parent / "extra.parquet").write_bytes(b"must not decode this extra file")
    result = cli.check(root, baseline, WAVES, TABLES)
    assert result["invariance_passed"]
    assert result["unknown_files"] == ["var/extra.parquet"]


@pytest.mark.parametrize("mode,strict,expected", [
    ("full", False, 0), ("full", True, 0), ("missing", False, 0),
    ("missing", True, 1), ("mismatch", False, 1), ("mismatch", True, 1),
    ("error", False, 1), ("error", True, 1),
])
def test_cli_exit_and_json_report(cohort, tmp_path, monkeypatch, mode, strict, expected):
    import lsms_library
    root, path, baseline = cohort
    monkeypatch.setattr(lsms_library, "Country", lambda *a, **kw: SimpleNamespace(
        waves=WAVES, resources={"Data Scheme": dict.fromkeys(TABLES)}))
    monkeypatch.setattr(invariance, "_load_baseline", lambda: baseline)
    if mode == "missing":
        root = tmp_path / "empty"
    elif mode == "mismatch":
        baseline[KEY]["content_hash"] = "0" * 64
    elif mode == "error":
        path.write_bytes(b"not parquet")
    report = tmp_path / "report.json"
    args = ["--data-root", str(root), "--report", str(report)] + (["--complete"] if strict else [])
    assert cli.main(args) == expected
    result = json.loads(report.read_text())
    assert result["required_entries"] == 1 and result["required_fields"] == 5
    assert result["data_root"] == str(root.resolve())


def test_cli_requires_explicit_root_before_any_country_or_cache_access(monkeypatch):
    import lsms_library
    def forbidden(*args, **kwargs):
        pytest.fail("implicit country/cache access")
    monkeypatch.setattr(lsms_library, "Country", forbidden)
    with pytest.raises(SystemExit) as exc:
        cli.main(["--complete"])
    assert exc.value.code == 2


def test_import_does_not_discover_or_probe_a_configured_cache(monkeypatch):
    from lsms_library.paths import data_root
    source = Path(invariance.__file__)
    forbidden_path = Path("/invented-unreadable-shared-cache")
    monkeypatch.setenv("LSMS_UGANDA_ROOT", str(forbidden_path))
    original = Path.is_dir
    def guarded(path):
        if path == forbidden_path:
            pytest.fail("invariance import probed a configured data root")
        return original(path)
    monkeypatch.setattr(Path, "is_dir", guarded)
    spec = importlib.util.spec_from_file_location("_isolated_invariance_import", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.BASELINE == invariance._load_baseline()


@pytest.mark.parametrize("where", ["cache", "baseline", "parquet"])
def test_json_output_cannot_overwrite_cache_or_baseline(cohort, tmp_path, monkeypatch, where):
    root, path, baseline = cohort
    fake_baseline = tmp_path / "invented-baseline.json"
    fake_baseline.write_text(json.dumps(baseline))
    monkeypatch.setattr(invariance, "BASELINE_PATH", fake_baseline)
    target = {"cache": root / "Uganda/report.json", "baseline": fake_baseline,
              "parquet": tmp_path / "arbitrary.parquet"}[where]
    before = path.read_bytes(), fake_baseline.read_bytes()
    with pytest.raises(SystemExit) as exc:
        cli.main(["--data-root", str(root), "--report", str(target), "--complete"])
    assert exc.value.code == 2
    assert before == (path.read_bytes(), fake_baseline.read_bytes())


def test_deselected_entries_remain_required_and_unknown_report_keys_are_named():
    baseline = {KEY: dict.fromkeys(FIELDS, "pinned")}
    terminal = SimpleNamespace(stats={"deselected": [_report(KEY)]})
    report = pytest_coverage(terminal, baseline)
    assert report["entry_counts"] == {"deselected": 1} and not report["coverage_complete"]
    terminal = SimpleNamespace(stats={"passed": [_report("var/typo.parquet")]})
    report = pytest_coverage(terminal, baseline)
    assert report["unknown_result_keys"] == ["var/typo.parquet"]


def _report(key, *, status="passed", when="call", result=None, reason=""):
    return SimpleNamespace(nodeid=f"tests/test_uganda_invariance.py::test_parquet_matches_baseline[{key}]",
                           when=when, passed=status == "passed", failed=status == "failed",
                           skipped=status == "skipped", longrepr=reason,
                           user_properties=[] if result is None else [(PROPERTY, json.dumps(result))])


def test_pytest_summary_counts_gated_and_not_run_keys_without_data_io(monkeypatch):
    baseline = {KEY: dict.fromkeys(FIELDS, "pinned"), "var/nutrition.parquet": dict.fromkeys(FIELDS, "pinned")}
    terminal = SimpleNamespace(stats={"skipped": [_report(KEY, status="skipped", when="setup", reason="needs S3 credentials")]})
    monkeypatch.setattr(Path, "rglob", lambda *args: pytest.fail("reporter scanned a data root"))
    result = pytest_coverage(terminal, baseline)
    assert result["entry_counts"] == {"credential_skipped": 1, "not_run": 1}
    assert result["checked_fields"] == 0 and result["required_fields"] == 10
    assert "INCOMPLETE: 0/2" in summary_lines(result)[0]


def test_serialized_worker_property_counts_all_fields_and_teardown_error():
    baseline = {KEY: dict.fromkeys(FIELDS, "pinned")}
    result = entry_result(KEY, baseline[KEY], "matched")
    result["fields"] = dict.fromkeys(FIELDS, "passed")
    passed = _report(KEY, result=result)
    complete = pytest_coverage(SimpleNamespace(stats={"passed": [passed]}), baseline)
    assert complete["invariance_passed"] and complete["checked_fields"] == 5
    terminal = SimpleNamespace(stats={"passed": [passed], "error": [_report(KEY, status="failed", when="teardown")]})
    error = pytest_coverage(terminal, baseline)
    assert error["coverage_complete"] and not error["invariance_passed"]
    assert error["entry_counts"] == {"error": 1}


def test_unknown_report_key_is_visible_and_never_verifies():
    baseline = {KEY: dict.fromkeys(FIELDS, "pinned")}
    result = entry_result(KEY, baseline[KEY], "matched")
    result["fields"] = dict.fromkeys(FIELDS, "passed")
    report = summarize(baseline, {KEY: result, "surprise": result})
    assert report["unknown_result_keys"] == ["surprise"] and not report["invariance_passed"]


def _run_invented_coverage(tmp_path, mode, *, workers=2, selection=None):
    """Run actual copied comparator/hooks with invented files and fatal IO guards."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    project = tmp_path / "project"
    tests = project / "tests"
    (tests / "fixtures").mkdir(parents=True)
    repo = Path(__file__).resolve().parents[1]
    for name in ("test_uganda_invariance.py", "uganda_invariance_reporting.py"):
        (tests / name).write_bytes((repo / "tests" / name).read_bytes())
    (tests / "__init__.py").write_text("")
    data = tmp_path / "private-data"
    path = data / "Uganda" / KEY
    path.parent.mkdir(parents=True)
    frame = pd.DataFrame({"weight": [1.0]}, index=pd.Index(["invented"], name="i"))
    frame.to_parquet(path)
    entry = invariance._fingerprint(pd.read_parquet(path))
    if mode == "empty":
        # Use a different empty root; do not delete even invented artifacts.
        data = tmp_path / "empty-private-data"
        (data / "Uganda").mkdir(parents=True)
    elif mode == "partial":
        table = pa.Table.from_pandas(frame)
        metadata = dict(table.schema.metadata or {})
        metadata[b"lsms_cache_waves"] = json.dumps(["2019-20"]).encode()
        pq.write_table(table.replace_schema_metadata(metadata), path)
    elif mode == "mismatch":
        entry["content_hash"] = "0" * 64
    elif mode == "error":
        path.write_bytes(b"not parquet")
    baseline = {KEY: entry}
    if selection is not None:
        for key in ("var/nutrition.parquet", "2019-20/_/sample.parquet"):
            target = data / "Uganda" / key
            target.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(target)
            baseline[key] = entry
        (tests / "test_worker.py").write_text("def test_other_worker():\n    assert True\n")
    (tests / "fixtures/uganda_baseline.json").write_text(json.dumps(baseline))
    guard = '''
def pytest_sessionstart(session):
    import pathlib,sys,lsms_library
    from lsms_library.paths import countries_root,data_root
    import os
    expected=pathlib.Path(os.environ['COVERAGE_CODE_ROOT']).resolve()
    assert pathlib.Path(lsms_library.__file__).resolve().is_relative_to(expected)
    assert countries_root().resolve()==expected/'lsms_library/countries'
    assert data_root().resolve()==pathlib.Path(os.environ['LSMS_DATA_DIR']).resolve()
    def profile(frame,event,arg):
        if event=='call' and (frame.f_globals.get('__name__') or '').startswith('lsms_library') and frame.f_code.co_name in {'get_dataframe','get_data_file','grab_data','run_make_target','to_parquet','_compute_panel_ids'}:
            raise AssertionError('Invented coverage test attempted a source/build')
    sys.setprofile(profile)
'''
    (project / "conftest.py").write_text((repo / "conftest.py").read_text() + guard)
    env = os.environ.copy()
    env.update(LSMS_DATA_DIR=str(data), LSMS_UGANDA_ROOT=str(data / "Uganda"),
               COVERAGE_CODE_ROOT=str(repo), LSMS_CONFIG_DIR=str(tmp_path / "empty-config"),
               LSMS_S3_CREDS=str(tmp_path / "empty-config/no-creds"), LSMS_SKIP_AUTH="1",
               LSMS_COUNTRIES_ROOT=str(repo / "lsms_library/countries"), PYTHONPATH=str(repo))
    args = [sys.executable, "-m", "pytest", "-n", str(workers), "--dist=loadfile", "-q"]
    if selection == "never_collected":
        args += ["tests/test_worker.py"]
    elif selection is not None:
        args += ["tests/test_uganda_invariance.py", "tests/test_worker.py"]
        args += ["-k", {"all_deselected": "other_worker", "partial_selection": "not nutrition",
                        "no_tests_selected": "no_such_selected_test"}[selection]]
    proc = subprocess.run(args, cwd=project, env=env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
    (tmp_path / "inner-pytest.log").write_text(proc.stdout)
    return proc


@pytest.mark.parametrize("mode,exitcode,coverage,checked", [
    ("full", 0, "COMPLETE", 5), ("empty", 0, "INCOMPLETE", 0),
    ("partial", 0, "INCOMPLETE", 0), ("mismatch", 1, "COMPLETE", 5),
    ("error", 1, "INCOMPLETE", 0),
])
def test_actual_xdist_report_transport_on_invented_private_cohort(tmp_path, mode, exitcode, coverage, checked):
    proc = _run_invented_coverage(tmp_path, mode)
    assert proc.returncode == exitcode, proc.stdout
    expected_entries = 1 if checked else 0
    assert f"coverage {coverage}: {expected_entries}/1 entries, {checked}/5 required field comparisons" in proc.stdout
    assert "verification PASSED" in proc.stdout if mode == "full" else "verification NOT PASSED" in proc.stdout
    assert "unknown files=not inventoried" in proc.stdout


@pytest.mark.parametrize("workers", [0, 2])
@pytest.mark.parametrize("selection", ["all_deselected", "partial_selection", "never_collected", "no_tests_selected"])
def test_actual_collection_scope_survives_serial_and_xdist_selection(tmp_path, workers, selection):
    proc = _run_invented_coverage(tmp_path, "full", workers=workers, selection=selection)
    assert proc.returncode == (5 if selection == "no_tests_selected" else 0), proc.stdout
    if selection == "never_collected":
        assert "Uganda baseline comparison coverage" not in proc.stdout
        assert "Uganda invariance coverage" not in proc.stdout
    else:
        compared, fields, deselected = (2, 10, 1) if selection == "partial_selection" else (0, 0, 3)
        assert f"INCOMPLETE: {compared}/3 entries, {fields}/15 required field comparisons" in proc.stdout
        assert f"deselected={deselected}" in proc.stdout
        assert "verification NOT PASSED" in proc.stdout


def test_collection_evidence_without_reports_and_absent_scope_do_not_do_io(monkeypatch):
    baseline = {KEY: dict.fromkeys(FIELDS, "pinned")}
    terminal = SimpleNamespace(stats={})
    monkeypatch.setattr(Path, "rglob", lambda *a: pytest.fail("reporter scanned a data root"))
    assert pytest_coverage(terminal, baseline) is None
    nodeid = _report(KEY).nodeid
    result = pytest_coverage(terminal, baseline, collected=[nodeid])
    assert result["entry_counts"] == {"not_run": 1}
    assert result["checked_fields"] == 0 and not result["invariance_passed"]
    result = pytest_coverage(terminal, baseline, collected=[nodeid, nodeid], deselected=[nodeid])
    assert result["entry_counts"] == {"deselected": 1}
    assert result["required_entries"] == 1
