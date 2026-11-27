"""Strict Site Q diagnostics stay local even when Feature wraps the failure."""

import csv
import json
from types import SimpleNamespace
import warnings

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bench.feature_audit import cluster, scan
from lsms_library import Feature, quantity_audit as q


_HOUSEHOLD = "invented-siteq-private-household"
_PLOT = "invented-siteq-private-plot"


@pytest.fixture
def invented_site_q(monkeypatch):
    frame = pd.DataFrame({
        "t": "2021-22", "i": [_HOUSEHOLD, *range(1, 40)],
        "plot_id": [_PLOT, *range(1, 40)], "crop": "Maize", "input": "Urea",
        "u": "Kg", "Quantity": [9000.0, *([3.0] * 39)],
    }).set_index(["t", "i", "plot_id", "crop", "input", "u"])
    before = frame.copy(deep=True)

    class FixtureCountry:
        def plot_inputs(self, **kwargs):
            return q.check_quantities(frame, country="Niger", table="plot_inputs")

    def feature(table):
        f = Feature(table)
        f._countries = ["Niger"]
        return f

    monkeypatch.setattr(scan.ll, "Country", lambda *_a, **_kw: FixtureCountry())
    monkeypatch.setattr(scan.ll, "Feature", feature)
    monkeypatch.setattr(scan, "load_feature", lambda c, _feature: c.plot_inputs())
    monkeypatch.setattr(scan, "is_this_feature_sane", lambda *_a, **_kw: SimpleNamespace(checks=[]))
    monkeypatch.setenv("LSMS_QUANTITY_STRICT", "1")
    monkeypatch.setattr(q, "_QUANTITY_LEDGER", {})
    yield FixtureCountry().plot_inputs
    assert_frame_equal(frame, before, check_exact=True)


@pytest.mark.parametrize("phase", ["1a", "1b", "2"])
def test_strict_site_q_report_never_serializes_local_rows(invented_site_q, phase):
    with pytest.raises(q.QuantityImplausibleError) as local:
        invented_site_q()
    assert _HOUSEHOLD in str(local.value) and _PLOT in str(local.value)

    if phase == "1a":
        findings, country, rows = scan._scan_one_country("plot_inputs", "Niger")
        assert country == "Niger" and rows is None
        failure, = [f for f in findings if f.check == "build"]
        assert failure.status == "error" and failure.severity == "A"
    elif phase == "1b":
        findings, result = scan.phase1b_assembly("plot_inputs", ["Niger"], {"Niger": 40})
        assert result.empty
        failure, = [f for f in findings if f.check == "runtime_warning"]
        assert failure.status == "warn"
    else:
        findings = scan._scan_one_kwarg("plot_inputs", ["Niger"], {}, {}, {}, None, {"Niger": 40})
        failure, = [f for f in findings if f.check == "runtime_warning"]
        assert failure.status == "warn"
    assert "QuantityImplausibleError" in failure.detail
    records = [finding.to_record() for finding in findings]
    packed = json.dumps([records, cluster.cluster(records)])
    assert not any(marker in packed for marker in (_HOUSEHOLD, _PLOT, "9,000", "9000"))
    assert "strict mode" in failure.detail.lower()

    with pytest.raises(q.QuantityImplausibleError) as after:
        invented_site_q()
    assert str(after.value) == str(local.value)
    reports = q.quantity_reports("Niger", "plot_inputs")
    report, = reports
    assert report["n_implausible"] == 1
    assert _HOUSEHOLD in repr(report["offenders"])  # Still useful locally.


def test_nested_audit_does_not_unwind_an_outer_call(invented_site_q):
    def nested():
        _, _, inner, _ = scan._build(invented_site_q)
        assert "QuantityImplausibleError" in inner and _HOUSEHOLD not in inner
        invented_site_q()

    _, _, outer, _ = scan._build(nested)
    assert "QuantityImplausibleError" in outer and _HOUSEHOLD not in outer
    with pytest.raises(q.QuantityImplausibleError, match=_HOUSEHOLD):
        invented_site_q()


def test_normal_local_warning_and_unrelated_exception_unchanged(invented_site_q, monkeypatch):
    monkeypatch.delenv("LSMS_QUANTITY_STRICT")
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        invented_site_q()
    assert any(_HOUSEHOLD in str(w.message) and w.category is q.QuantityImplausibleWarning
               for w in captured)

    def unrelated():
        raise ValueError("unrelated failure containing the words QuantityImplausibleError")

    _, _, error, _ = scan._build(unrelated)
    assert error == "ValueError: unrelated failure containing the words QuantityImplausibleError"


def test_strict_coverage_snapshot_and_render_do_not_expose_rows(invented_site_q, tmp_path):
    from docs_hooks import coverage_matrix as docs_coverage
    from lsms_library import coverage_matrix as coverage, diagnostics

    country = SimpleNamespace(plot_inputs=invented_site_q)
    env = {"load_feature": diagnostics.load_feature}
    options = dict(avail_by_wave={"2021-22": {"plot_inputs"}})
    rows = coverage.grade_feature("Niger", "plot_inputs", ["2021-22"], country, env, **options)
    broken, = rows
    assert broken["tier"] == "broken"
    assert broken["detail"].startswith("QuantityImplausibleError:")
    snapshot = tmp_path / "coverage.csv"
    coverage.save_snapshot(pd.DataFrame(rows, columns=coverage.COLUMNS), snapshot, merge=False)
    recovered = coverage.coverage(snapshot=snapshot)
    assert recovered.iloc[0]["detail"] == broken["detail"]
    with snapshot.open(newline="") as saved:
        html = docs_coverage.render_fragment(list(csv.DictReader(saved)))
    assert "QuantityImplausibleError:" in html
    for surface in (broken["detail"], snapshot.read_text(), html):
        assert not any(marker in surface for marker in (_HOUSEHOLD, _PLOT, "9,000", "9000"))

    cheap, = coverage.grade_feature(
        "Niger", "plot_inputs", ["2021-22"], country, env, readiness=False, **options
    )
    assert cheap["tier"] == "declared"
    with pytest.raises(q.QuantityImplausibleError, match=_HOUSEHOLD):
        invented_site_q()
