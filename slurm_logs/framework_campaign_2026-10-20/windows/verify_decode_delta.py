"""Aggregate-only proof of the GH #805 Windows digest movement.

Run after windows_build_check.py has built shocks/assets in a private
LSMS_DATA_DIR; never records household keys or individual records.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess

import pandas as pd
import lsms_library as ll
from lsms_library import config
from lsms_library.paths import data_root


def fingerprint(df):
    # Identical normalization to scripts/windows_build_check.py.
    flat = df.reset_index()
    cols = sorted(flat.columns)
    flat = flat[cols].sort_values(cols, kind="mergesort").reset_index(drop=True)
    hashed = pd.util.hash_pandas_object(flat.astype(str), index=False)
    return {
        "digest": f"{int(hashed.sum()) & 0xFFFFFFFFFFFF:012x}",
        "normalized_hash_sha256": hashlib.sha256(hashed.to_numpy().tobytes()).hexdigest(),
    }


top = Path(subprocess.check_output(
    ["git", "rev-parse", "--show-toplevel"], text=True).strip()).resolve()
assert Path(ll.__file__).resolve().is_relative_to(top), ll.__file__
explicit_root = os.environ.pop("LSMS_DATA_DIR")
assert explicit_root.strip(), "Set LSMS_DATA_DIR to a private cache explicitly"
try:
    production_root = Path(config.data_dir() or Path.home() / ".local/share/lsms_library").resolve()
finally:
    os.environ["LSMS_DATA_DIR"] = explicit_root
private_root = Path(explicit_root).resolve()
assert private_root != production_root, "Refusing the configured production cache"
assert private_root != (Path.home() / ".local/share/lsms_library").resolve()
assert data_root().resolve() == private_root

uga = ll.Country("Uganda")
current = uga.shocks()
old = current.copy(deep=True)
expected = {"HowCoped0": 862, "HowCoped1": 756, "HowCoped2": 394}
changed_by_wave = {}
for col, want in expected.items():
    strings = current[col].astype("string")
    mask = strings.str.contains("etc\u2026)", regex=False, na=False)
    assert int(mask.sum()) == want, (col, int(mask.sum()))
    assert not strings.str.contains("\u0085", regex=False, na=False).any()
    waves = current.index.get_level_values("t")[mask]
    counts = pd.Series(waves).value_counts().to_dict()
    changed_by_wave[col] = counts
    def previous(value):
        return value.replace("etc\u2026)", "etc\u0085)") if isinstance(value, str) else value
    assert str(current[col].dtype) == "string", col
    old[col] = old[col].map(previous).astype(current[col].dtype)
    assert current[col].dtype == old[col].dtype, col
    delta = ~((current[col].astype(object) == old[col].astype(object)) |
              (current[col].isna() & old[col].isna()))
    assert int(delta.sum()) == want, col

pd.testing.assert_index_equal(current.index, old.index, exact=True)
pd.testing.assert_index_equal(current.columns, old.columns, exact=True)
other = current.columns.difference(list(expected))
pd.testing.assert_frame_equal(current[other], old[other], check_exact=True)
assert current.shape == old.shape
assert list(current.dtypes) == list(old.dtypes)

forward = old.copy(deep=True)
for col in expected:
    def repaired(value):
        return value.replace("etc\u0085)", "etc\u2026)") if isinstance(value, str) else value
    forward[col] = forward[col].map(repaired).astype(current[col].dtype)
pd.testing.assert_frame_equal(current, forward, check_exact=True)

now_fp, old_fp = fingerprint(current), fingerprint(old)
assert len(current) == 14457
assert now_fp["digest"] == "0ab2ea4c79dd", now_fp
assert old_fp["digest"] == "64a53fa7af55", old_fp
assets = uga.assets()
assets_fp = fingerprint(assets)
assert len(assets) == 116808
assert assets_fp["digest"] == "9202ed542a14", assets_fp
report = {
    "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "package": str(Path(ll.__file__).resolve()),
    "data_root": os.environ["LSMS_DATA_DIR"],
    "shared_l1_only": str((Path(os.environ["LSMS_DATA_DIR"]) / "dvc-cache").resolve()),
    "shocks": {
        "shape": list(current.shape), "columns": list(current.columns),
        "index_names": list(current.index.names),
        "index_dtypes": [str(current.index.get_level_values(n).dtype) for n in current.index.names],
        "dtypes": {str(k): str(v) for k, v in current.dtypes.items()},
        "current": now_fp, "counterfactual_old_decoding": old_fp,
        "changed_cells": sum(expected.values()), "changed_by_wave": changed_by_wave,
        "change": "Only HowCoped0/1/2: etc<U+0085>) -> etc<U+2026>)",
        "exact_index_columns_dtypes_other_values_unchanged": True,
        "exact_roundtrip_frame_equal": True,
    },
    "assets": {"shape": list(assets.shape), **assets_fp},
}
print(json.dumps(report, indent=2, sort_keys=True))
