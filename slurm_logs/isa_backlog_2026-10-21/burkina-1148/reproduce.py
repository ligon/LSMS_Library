"""Local-only aggregate census for GH #1148; never acquire a source file.

Raw copies stay in --out. Only aggregate JSON is emitted. Missing blobs are
reported without calling an acquisition layer. Existing status disagreements
remain disagreements; this script computes no production exposure or rate.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket

import pandas as pd
import pyreadstat
import yaml

import lsms_library
from lsms_library.data_access import get_data_file
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root, data_root


def digest(path, algorithm="sha256"):
    return hashlib.new(algorithm, path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--l1", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--seed", type=Path)
    parser.add_argument("--seed-manifest", type=Path)
    args = parser.parse_args()
    checkout, out = args.checkout.resolve(), args.out.resolve()
    assert Path.cwd().resolve() == checkout
    assert Path(lsms_library.__file__).resolve().is_relative_to(checkout)
    assert Path(countries_root()).resolve() == checkout / "lsms_library/countries"
    assert Path(data_root()).resolve() == out / "data"
    os.umask(0o077)
    out.mkdir(mode=0o700, parents=True, exist_ok=True)

    def offline(*_args, **_kwargs):
        raise RuntimeError("Source acquisition/networking forbidden in this census")

    socket.socket = offline
    wave = checkout / "lsms_library/countries/Burkina_Faso/2014"
    source_manifest, status_summary, frames = [], [], {}
    for sidecar in sorted(wave.rglob("*.dvc")):
        info = yaml.safe_load(sidecar.read_text())["outs"][0]
        md5 = info["md5"]
        candidates = [args.l1 / md5[:2] / md5[2:],
                      args.l1 / "files/md5" / md5[:2] / md5[2:]]
        blob = next((p for p in candidates if p.is_file()), None)
        item = {"sidecar": str(sidecar.relative_to(checkout)), **info,
                "held": blob is not None}
        source_manifest.append(item)
        if blob is None:
            continue
        assert digest(blob, "md5") == md5, sidecar
        raw = out / "held" / str(sidecar.relative_to(countries_root()))[:-4]
        raw.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not raw.exists():
            shutil.copyfile(blob, raw)
        assert digest(raw, "md5") == md5
        # Obtain the already-present private path through the sanctioned layer.
        assert get_data_file(raw) == raw
        item["sha256"] = digest(raw)
        if raw.suffix != ".dta":
            continue
        _, metadata = pyreadstat.read_dta(raw, metadataonly=True)
        item.update(rows=metadata.number_rows, columns=metadata.number_columns)
        fields = [c for c in metadata.column_names
                  if re.search(r"res_entr|^A13$", c)]
        if not fields or not re.search(r"_p[1-4]_", raw.name):
            continue
        df = get_dataframe(raw, convert_categoricals=False)
        keys = ["zd", "menage"]
        assert set(keys).issubset(df.columns)
        for field in fields:
            x = df[keys + [field]]
            grouped = x.groupby(keys, dropna=False)[field]
            values = grouped.agg(lambda s: tuple(sorted(set(s.dropna()))))
            status_summary.append({
                "file": raw.name, "status": field, "keys": keys,
                "rows": len(x), "hh": len(values),
                "key_null_rows": int(x[keys].isna().any(axis=1).sum()),
                "row_statuses": {str(k): int(v) for k, v in
                                 x[field].value_counts(dropna=False).items()},
                "hh_status_sets": {str(k): int(v) for k, v in
                                   values.value_counts().items()},
                "same_status_duplicate_rows": len(x) - len(x.drop_duplicates()),
                "conflict_hh": int(values.map(len).gt(1).sum()),
                "all_missing_hh": int(values.map(len).eq(0).sum()),
            })
            if "_10032015" in raw.name:
                continue
            assert values.map(len).le(1).all(), raw.name
            # There is at most one observed code: first() only removes identical
            # repetitions and null copies here; it never resolves a conflict.
            observed = grouped.first().rename(raw.name)
            passage = int(re.search(r"_p([1-4])_", raw.name)[1])
            frames.setdefault(passage, []).append(observed)

    missing = [x["sidecar"] for x in source_manifest if not x["held"]]
    (out / "source-manifest.json").write_text(json.dumps(source_manifest, indent=2) + "\n")
    (out / "status-summary.json").write_text(json.dumps(status_summary, indent=2) + "\n")
    if missing:
        print(json.dumps({"missing_held_blobs": missing, "acquired": 0}, indent=2))
        return
    matrices, result, resolved = {}, {}, {}
    for passage, sources in frames.items():
        matrix = pd.concat(sources, axis=1)
        sets = matrix.apply(lambda r: tuple(sorted(set(r.dropna()))), axis=1)
        matrices[passage] = matrix
        resolved[passage] = sets.map(lambda s: s[0] if len(s) == 1 else float("nan"))
        result[str(passage)] = {
            "union_hh": len(matrix),
            "null_result_hh": int(sets.map(len).eq(0).sum()),
            "conflict_hh": int(sets.map(len).gt(1).sum()),
            "accepted_unambiguous": int(resolved[passage].isin([1, 2]).sum()),
        }
    panel = matrices[1].index
    all_keys = panel
    for matrix in matrices.values():
        all_keys = all_keys.union(matrix.index)
    masks = {}
    for passage, codes in resolved.items():
        x = codes.reindex(panel)
        result[str(passage)].update(
            p1_keys_absent=int((~panel.isin(codes.index)).sum()),
            outside_p1=int((~codes.index.isin(panel)).sum()))
        masks[str(passage)] = {
            "accepted_1_2": int(x.isin([1, 2]).sum()),
            "abandon_3": int(x.eq(3).sum()),
            "absence_or_refusal_4_5": int(x.isin([4, 5]).sum()),
            "other_or_undecoded_6_7_9": int(x.isin([6, 7, 9]).sum()),
            "unknown_absent_missing_or_conflict": int(x.isna().sum()),
        }
        assert sum(masks[str(passage)].values()) == len(panel)
    aggregate = {"sidecars": len(source_manifest), "held": len(source_manifest),
                 "p1_panel_keys": len(panel), "union_keys": len(all_keys),
                 "source_coverage": result, "p1_panel_status_masks": masks,
                 "production_exposure": "not computed"}

    if args.seed or args.seed_manifest:
        assert args.seed and args.seed_manifest
        assert digest(args.seed_manifest) == "e53acbe4b0f5918e244fc808ee326035ed86c04ec15475e2ff09339ccaaba5f0"
        manifest = json.loads(args.seed_manifest.read_text())
        records = {k: v for k, v in manifest.items() if k.startswith("Burkina_Faso/")}
        for name, entry in records.items():
            src, dst = args.seed / name, out / "data" / name
            original = src.stat()
            assert digest(src) == entry["sha256"], name
            dst.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not dst.exists():
                shutil.copyfile(src, dst)
            assert digest(dst) == entry["sha256"]
            assert (original.st_dev, original.st_ino) != (dst.stat().st_dev, dst.stat().st_ino)
            assert src.stat() == original
        aggregate["verified_private_l2_files"] = len(records)

    (out / "aggregate.json").write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n")
    print(json.dumps(aggregate, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
