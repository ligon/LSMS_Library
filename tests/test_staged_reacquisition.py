"""GH1149 Option C: synthetic releases, real acquisition control flow.

No downloaded microdata or credentials are used. The member-list fixture names
held sidecars and the additions/removal documented in1149/978/1127. Its payloads
and archive-relative directory choices are synthetic, not recovered WB zips.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import urllib.error
import urllib.request
import zipfile

import pytest

from lsms_library import data_access as da
from lsms_library import provenance as pv


def _zip(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload in members:
            archive.writestr(name, payload)
    return buffer.getvalue()


def _sidecar(path, content=b"held", *, legacy=False, raw=True):
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.md5(content.replace(b"\r\n", b"\n") if legacy else content).hexdigest()
    path.with_name(path.name + ".dvc").write_text(
        f"outs:\n- md5: {digest}\n"
        + ("" if legacy else "  hash: md5\n")
        + f"  path: {path.name}\n")
    if raw:
        path.write_bytes(content)
    return path


@pytest.fixture
def download(monkeypatch):
    state = {"payload": b"", "failed": False}

    def urlopen(*args, **kwargs):
        if state["failed"]:
            raise urllib.error.URLError("synthetic interrupted download")
        return io.BytesIO(state["payload"])

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return state


def _extract(data, download, members):
    download["payload"] = _zip(members)
    report = {}
    result = da._extract_all_from_zip(
        "https://synthetic.invalid/release.zip", "synthetic.zip", data,
        "synthetic-key", report)
    return result, report


@pytest.mark.parametrize("archive_path", ["MODULE.DTA", "replacement/MODULE.DTA"])
def test_reacquisition_uses_unique_case_insensitive_nested_sidecar(
        tmp_path, download, archive_path):
    data = tmp_path / "Data"
    held = _sidecar(data / "Post Planting Wave 5/Household/module.dta")
    sidecar = held.with_name(held.name + ".dvc")
    before = sidecar.read_bytes()
    result, report = _extract(data, download, [(archive_path, b"revised")])
    assert result == report["members"] == [held]
    assert held.read_bytes() == b"revised"
    assert sidecar.read_bytes() == before
    assert [r["path"] for r in report["revised"]] == [held]
    assert report["skipped"] == report["leftovers"] == []
    assert not (data / archive_path).exists()


def test_identical_existing_file_is_verified_without_rewriting(tmp_path, download):
    data = tmp_path / "Data"
    held = _sidecar(data / "nested/a.dta")
    before = held.stat()
    result, report = _extract(data, download, [("flat/A.DTA", b"held")])
    assert result == []
    assert report["members"] == [held]
    assert report["revised"] == report["skipped"] == []
    assert (held.stat().st_ino, held.stat().st_mtime_ns) == (before.st_ino, before.st_mtime_ns)


def test_sidecar_without_raw_copy_is_materialized_and_compared(tmp_path, download):
    data = tmp_path / "Data"
    held = _sidecar(data / "nested/a.dta", raw=False)
    result, report = _extract(data, download, [("a.dta", b"held")])
    assert result == report["members"] == [held]
    assert held.read_bytes() == b"held"
    assert report["revised"] == []


@pytest.mark.parametrize("extension", [".dta", ".csv", ".sav", ".dta.gz"])
def test_new_wave_and_new_members_keep_zip_relative_paths(tmp_path, download, extension):
    data = tmp_path / "Data"
    name = "release/Household/a" + extension
    result, report = _extract(data, download, [(name, b"new"), ("readme.txt", b"ignored")])
    assert result == report["members"] == [data / name]
    assert (data / name).read_bytes() == b"new"
    assert not (data / "readme.txt").exists()


def test_same_named_new_members_in_distinct_directories_stay_distinct(tmp_path, download):
    data = tmp_path / "Data"
    result, report = _extract(data, download, [("PP/a.dta", b"pp"), ("PH/a.dta", b"ph")])
    assert result == [data / "PP/a.dta", data / "PH/a.dta"]
    assert [p.read_bytes() for p in result] == [b"pp", b"ph"]
    assert report["members"] == result


def test_ambiguous_sidecars_refuse_before_any_placement(tmp_path, download):
    data = tmp_path / "Data"
    a = _sidecar(data / "PP/a.dta")
    b = _sidecar(data / "PH/A.DTA")
    download["payload"] = _zip([("new.dta", b"new"), ("a.dta", b"new")])
    with pytest.raises(ValueError, match=r"Ambiguous sidecars.*PP/a.dta.dvc|Ambiguous sidecars.*PH/A.DTA.dvc") as caught:
        da._extract_all_from_zip("https://synthetic.invalid/zip", "zip", data, "key", {})
    assert "PP/a.dta.dvc" in str(caught.value) and "PH/A.DTA.dvc" in str(caught.value)
    assert a.read_bytes() == b.read_bytes() == b"held"
    assert not (data / "new.dta").exists()


@pytest.mark.parametrize("names,tracked", [
    (["a.dta", "A.DTA"], False),
    (["one/a.dta", "two/A.DTA"], True),
    (["a.dta", "a.dta/child.dta"], False),
])
def test_incoming_destination_collisions_refuse_before_placement(tmp_path, download, names, tracked):
    data = tmp_path / "Data"
    if tracked:
        _sidecar(data / "held/a.dta")
    download["payload"] = _zip([(n, b"new") for n in names])
    with pytest.raises(ValueError, match="destination"):
        da._extract_all_from_zip("https://synthetic.invalid/zip", "zip", data, "key", {})
    assert not (data / names[0]).exists()
    if tracked:
        assert (data / "held/a.dta").read_bytes() == b"held"


@pytest.mark.parametrize("name", ["../escaped.dta", "/absolute.dta", "C:/absolute.dta", r"..\escaped.dta"])
def test_escaping_archive_members_refuse_before_placement(tmp_path, download, name):
    data = tmp_path / "Data"
    download["payload"] = _zip([("first.dta", b"new"), (name, b"new")])
    with pytest.raises(ValueError, match="Unsafe archive member"):
        da._extract_all_from_zip("https://synthetic.invalid/zip", "zip", data, "key", {})
    assert not (data / "first.dta").exists()


def test_destination_symlink_cannot_escape_data(tmp_path, download):
    data = tmp_path / "Data"
    data.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (data / "linked").symlink_to(outside, target_is_directory=True)
    download["payload"] = _zip([("first.dta", b"new"), ("linked/a.dta", b"new")])
    with pytest.raises(ValueError, match="escapes Data"):
        da._extract_all_from_zip("https://synthetic.invalid/zip", "zip", data, "key", {})
    assert not (data / "first.dta").exists() and list(outside.iterdir()) == []


def test_revised_hardlinked_raw_does_not_modify_held_blob(tmp_path, download):
    data = tmp_path / "Data"
    held = _sidecar(data / "nested/a.dta")
    blob = tmp_path / "held-blob"
    os.link(held, blob)
    before = blob.stat()
    _extract(data, download, [("a.dta", b"revised")])
    assert held.read_bytes() == b"revised" and blob.read_bytes() == b"held"
    assert held.stat().st_ino != blob.stat().st_ino
    assert (blob.stat().st_mtime_ns, blob.stat().st_ino) == (before.st_mtime_ns, before.st_ino)


def test_late_invalid_checksum_refuses_before_replacing_earlier_held_file(tmp_path, download):
    data = tmp_path / "Data"
    first = _sidecar(data / "nested/a.dta")
    second = _sidecar(data / "nested/b.dta")
    second.with_name(second.name + ".dvc").write_text("outs:\n- md5: invalid\n")
    download["payload"] = _zip([("a.dta", b"new"), ("b.dta", b"new")])
    with pytest.raises(ValueError, match="Cannot compare sidecar checksum"):
        da._extract_all_from_zip("https://synthetic.invalid/zip", "zip", data, "key", {})
    assert first.read_bytes() == second.read_bytes() == b"held"


def test_every_revision_and_leftover_is_reported_before_placement(
        tmp_path, download, monkeypatch, caplog):
    data = tmp_path / "Data"
    for name in ("a.dta", "b.dta", "retired-one.dta", "retired-two.dta"):
        _sidecar(data / "nested" / name)
    actual_replace = os.replace

    def checked_replace(src, dst):
        assert sum("Revised release member:" in r.message for r in caplog.records) == 2
        assert sum("Release does not ship sidecar:" in r.message for r in caplog.records) == 2
        return actual_replace(src, dst)

    monkeypatch.setattr(os, "replace", checked_replace)
    with caplog.at_level("WARNING", logger="lsms_library.data_access"):
        _, report = _extract(data, download, [("a.dta", b"new"), ("b.dta", b"new")])
    assert len(report["revised"]) == len(report["leftovers"]) == 2


@pytest.mark.parametrize("legacy,changed", [(False, True), (True, False)])
def test_sidecar_hash_convention_preserves_raw_and_legacy_crlf_policy(
        tmp_path, download, legacy, changed):
    data = tmp_path / "Data"
    held = _sidecar(data / "nested/a.csv", b"x\r\n1\r\n", legacy=legacy)
    result, report = _extract(data, download, [("a.csv", b"x\n1\n")])
    assert result == [held]
    assert bool(report["revised"]) is changed
    assert held.read_bytes() == b"x\n1\n"


def test_leftovers_compare_full_resolved_membership_not_only_basename(tmp_path, download):
    data = tmp_path / "Data"
    old = _sidecar(data / "retired/old.dta")
    held = _sidecar(data / "PP/a.dta")
    _, report = _extract(data, download, [("release/a.dta", b"held"), ("new/b.dta", b"new")])
    assert report["leftovers"] == [old.with_name(old.name + ".dvc")]
    assert old.read_bytes() == b"held" and held.read_bytes() == b"held"


def test_differing_untracked_raw_stays_unverified(tmp_path, download):
    data = tmp_path / "Data"
    data.mkdir()
    (data / "a.dta").write_bytes(b"unverified-old")
    result, report = _extract(data, download, [("a.dta", b"new"), ("b.dta", b"new")])
    assert result == report["members"] == [data / "b.dta"]
    assert report["skipped"] == [data / "a.dta"]
    assert (data / "a.dta").read_bytes() == b"unverified-old"


def test_byte_verification_does_not_use_checksum_equality_as_a_shortcut(
        tmp_path, download, monkeypatch):
    from dvc_data.hashfile import hash as dvc_hash
    data = tmp_path / "Data"
    data.mkdir()
    (data / "a.dta").write_bytes(b"old")
    monkeypatch.setattr(dvc_hash, "fobj_md5", lambda *args, **kwargs: "0" * 32)
    result, report = _extract(data, download, [("a.dta", b"new")])
    assert result == report["members"] == []
    assert report["skipped"] == [data / "a.dta"]
    assert (data / "a.dta").read_bytes() == b"old"


def test_staging_failure_never_places_earlier_member(tmp_path, download, monkeypatch):
    data = tmp_path / "Data"
    held = _sidecar(data / "nested/a.dta")
    download["payload"] = _zip([("a.dta", b"new"), ("b.dta", b"new")])
    real_open = zipfile.ZipFile.open

    def fail_second(archive, member, *args, **kwargs):
        if getattr(member, "filename", member) == "b.dta":
            raise OSError("synthetic staging failure")
        return real_open(archive, member, *args, **kwargs)

    monkeypatch.setattr(zipfile.ZipFile, "open", fail_second)
    report = {}
    result = da._extract_all_from_zip(
        "https://synthetic.invalid/zip", "zip", data, "key", report)
    assert result == report["members"] == []
    assert "staging failure" in report["error"]
    assert held.read_bytes() == b"held" and not (data / "b.dta").exists()


@pytest.mark.parametrize("country,expected", [("Nigeria", (104, 2, 2, 0)), ("Ethiopia", (67, 0, 1, 1))])
def test_documented_release_member_lists_with_synthetic_payloads(
        tmp_path, download, country, expected):
    fixture = json.loads((Path(__file__).parent / "fixtures/reacquisition_members.json").read_text())
    case = fixture["cases"][country]
    real_data = Path(da.__file__).parent / "countries" / country / case["wave"] / "Data"
    data = tmp_path / "Data"
    revised, removed = set(case["revised"]), set(case["removed"])
    members = []
    unchanged = []
    for row in case["held"]:
        relative = Path(row["held_path"])
        real_sidecar = real_data / (str(relative) + ".dvc")
        assert hashlib.sha256(real_sidecar.read_bytes()).hexdigest() == row["sidecar_sha256"]
        payload = b"synthetic-held:" + str(relative).encode()
        held = _sidecar(data / relative, payload)
        if relative.name in removed:
            continue
        incoming = b"synthetic-revised:" + payload if relative.name in revised else payload
        members.append((relative.name.upper(), incoming))
        if relative.name not in revised:
            unchanged.append((held, held.stat().st_ino, held.stat().st_mtime_ns, payload))
    members.extend(("synthetic-new/" + name, b"synthetic-new:" + name.encode()) for name in case["new"])
    result, report = _extract(data, download, members)
    assert (len(unchanged), len(report["revised"]), len(case["new"]), len(report["leftovers"])) == expected
    assert len(result) == expected[1] + expected[2]
    assert len(report["members"]) == expected[0] + expected[1] + expected[2]
    assert {r["path"].name for r in report["revised"]} == revised
    assert {p.name.removesuffix(".dvc") for p in report["leftovers"]} == removed
    assert report["error"] is None and report["skipped"] == []
    for held, inode, mtime, payload in unchanged:
        assert held.read_bytes() == payload
        assert (held.stat().st_ino, held.stat().st_mtime_ns) == (inode, mtime)
    for name in removed:
        assert any(p.name == name for p in data.rglob("*.dta"))


@pytest.fixture
def acquisition(tmp_path, download, monkeypatch):
    root = tmp_path / "countries"
    monkeypatch.setattr(da, "_COUNTRIES_DIR", root)
    monkeypatch.setattr(da, "permissions", lambda path=None: {"wb_api": "read"})
    monkeypatch.setattr(da.config, "microdata_api_key", lambda: "synthetic")
    monkeypatch.setattr(da, "_get_catalog_idno", lambda url: "NGA_2023_GHSP-W5_v02_M")
    monkeypatch.setattr(da, "_find_stata_zip_url", lambda *args: ("https://synthetic.invalid/zip", "zip"))
    monkeypatch.setattr(da, "_wb_catalog_search", lambda *args, **kwargs: [])
    source = root / "Nigeria/2023-24/Documentation/SOURCE.org"
    source.parent.mkdir(parents=True)
    before = ("SOURCE\n\nhttps://microdata.worldbank.org/index.php/catalog/6410\n\n"
              "#+CATALOG_ID: 6410\n#+CATALOG_IDNO: NGA_2023_GHSP-W5_v01_M\n"
              "#+CATALOG_LATEST_IDNO: NGA_2023_GHSP-W5_v02_M\n"
              "#+PARTNER_NOTE: retained fact\n")
    source.write_text(before)
    return root, source, before


@pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
@pytest.mark.parametrize("receipt", ["complete", "partial", "other-same-length", "exception"])
def test_nested_push_confirms_actual_members_and_keeps_provenance_until_complete(
        acquisition, download, monkeypatch, entrypoint, receipt):
    root, source, before = acquisition
    data = root / "Nigeria/2023-24/Data"
    held = _sidecar(data / "PP/a.dta")
    old = _sidecar(data / "retired/old.dta")
    download["payload"] = _zip([("A.DTA", b"held"), ("new/b.dta", b"new")])
    expected = {held, data / "new/b.dta"}
    seen = []

    def push(paths, **kwargs):
        assert source.read_text() == before
        actual = {root / p for p in paths}
        assert actual == expected and old not in actual
        seen.append(actual)
        if receipt == "exception":
            raise RuntimeError("synthetic push failure")
        if receipt == "partial":
            return list(actual)[:1]
        if receipt == "other-same-length":
            return [old, old]
        return list(actual)

    monkeypatch.setattr(da, "push_to_cache_batch", push)

    def run():
        if entrypoint == "populate_and_push":
            return da.populate_and_push("Nigeria", "2023-24")
        return da.add_wave("Nigeria", "6410", wave="2023-24", confirm=False, push=True, verbose=False)

    if receipt == "exception":
        with pytest.raises(RuntimeError, match="synthetic push failure"):
            run()
    else:
        run()
    assert len(seen) == 1
    if receipt == "complete":
        assert pv.read_provenance(root, "Nigeria", "2023-24").idno == "NGA_2023_GHSP-W5_v02_M"
        assert "#+PARTNER_NOTE: retained fact" in source.read_text()
    else:
        assert source.read_text() == before
    assert ("Nigeria", "2023-24") not in da._ACQUIRED_RELEASES


def test_all_verified_identical_members_can_record_release_without_rewrite(acquisition, download):
    root, source, before = acquisition
    held = _sidecar(root / "Nigeria/2023-24/Data/PP/a.dta")
    endpoint = held.stat()
    download["payload"] = _zip([("a.dta", b"held")])
    assert da.add_wave("Nigeria", "6410", wave="2023-24", confirm=False, push=False, verbose=False) == [held]
    assert pv.read_provenance(root, "Nigeria", "2023-24").idno == "NGA_2023_GHSP-W5_v02_M"
    assert (held.stat().st_ino, held.stat().st_mtime_ns) == (endpoint.st_ino, endpoint.st_mtime_ns)


def test_confirmation_decline_preserves_source_and_data(acquisition, download, monkeypatch):
    root, source, before = acquisition
    monkeypatch.setattr("builtins.input", lambda prompt: "n")
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **kw: pytest.fail("decline must not download"))
    assert da.add_wave("Nigeria", "6410", wave="2023-24", push=False, verbose=False) == []
    assert source.read_text() == before and not (root / "Nigeria/2023-24/Data").exists()


@pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
@pytest.mark.parametrize("failure", ["empty-archive", "download-error"])
def test_no_acquired_members_leave_no_completion_receipt(
        acquisition, download, entrypoint, failure):
    root, source, before = acquisition
    download["payload"] = _zip([])
    download["failed"] = failure == "download-error"
    if entrypoint == "populate_and_push":
        assert da.populate_and_push("Nigeria", "2023-24") == []
    else:
        assert da.add_wave("Nigeria", "6410", wave="2023-24", confirm=False, push=False, verbose=False) == []
    assert source.read_text() == before
    assert ("Nigeria", "2023-24") not in da._ACQUIRED_RELEASES


@pytest.mark.parametrize("failure", ["staging", "placement"])
def test_partial_failure_never_records_complete_release(acquisition, download, monkeypatch, failure):
    root, source, before = acquisition
    data = root / "Nigeria/2023-24/Data"
    held = _sidecar(data / "nested/a.dta")
    download["payload"] = _zip([("a.dta", b"new"), ("b.dta", b"new")])
    if failure == "staging":
        real = zipfile.ZipFile.open

        def fail(archive, member, *args, **kwargs):
            if getattr(member, "filename", member) == "b.dta":
                raise OSError("synthetic staging failure")
            return real(archive, member, *args, **kwargs)

        monkeypatch.setattr(zipfile.ZipFile, "open", fail)
    else:
        real = os.replace

        def fail(src, dst, *args, **kwargs):
            if Path(dst).name == "b.dta":
                raise OSError("synthetic placement failure")
            return real(src, dst, *args, **kwargs)

        monkeypatch.setattr(os, "replace", fail)
    da.add_wave("Nigeria", "6410", wave="2023-24", confirm=False, push=False, verbose=False)
    assert source.read_text() == before
    assert held.read_bytes() == (b"held" if failure == "staging" else b"new")
    assert not (data / "b.dta").exists()
    assert ("Nigeria", "2023-24") not in da._ACQUIRED_RELEASES
