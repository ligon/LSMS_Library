"""Lock-free check that every DVC sidecar's blob is reachable on the S3 remote.

GH #750: sidecars can be ``dvc add``ed and never pushed, and nothing
complains until someone needs the file.  ``data_access.unpushed_blobs()``
answers the same question through ``dvc status --cloud``, which takes the
global ``.dvc/tmp/lock`` and walks every sidecar through DVC's index (slow
on Lustre, and it serialises against every other DVC user).  This script
never invokes ``dvc``: it lists the remote prefix once (paginated), parses
the sidecars directly, and set-differences the md5s.

Usage::

    .venv/bin/python scripts/dvc_remote_reachability.py \
        [--countries Uganda Malawi] [--out report.tsv] [--md5-local]

Output: one TSV row per *missing* blob (sidecar path, md5, size, kind,
whether a local workspace copy exists, and -- with ``--md5-local`` --
whether that copy's md5 matches the sidecar, i.e. is pushable as-is), plus
a per-country / per-kind summary on stderr.  Exit status 1 if anything is
missing, so CI can use it.
"""
from __future__ import annotations

import argparse
import configparser
import hashlib
import re
import sys
from collections import Counter
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
COUNTRIES = REPO / "lsms_library" / "countries"
DVC_DIR = COUNTRIES / ".dvc"

# DVC 2.x remote layout: <prefix>/<md5[:2]>/<md5[2:]>
# DVC 3.x remote layout: <prefix>/files/md5/<md5[:2]>/<md5[2:]>
_KEY_RE = re.compile(r"([0-9a-f]{2})/([0-9a-f]{30}(?:\.dir)?)$")


def _remote_from_config() -> tuple[str, str, dict[str, str]]:
    cp = configparser.ConfigParser()
    cp.read(DVC_DIR / "config")
    default = cp["core"]["remote"]
    # DVC writes section headers as ['remote "name"']; configparser keeps
    # the surrounding single quotes in the section name.
    want = f'remote "{default}"'
    names = [s for s in cp.sections() if s.strip("'") == want]
    if not names:
        raise KeyError(f"no section for remote {default!r} in {DVC_DIR / 'config'}")
    sect = cp[names[0]]
    url = sect["url"]
    assert url.startswith("s3://"), url
    bucket, _, prefix = url[len("s3://"):].partition("/")
    creds = configparser.ConfigParser()
    creds.read(DVC_DIR / sect.get("credentialpath", "s3_creds"))
    conf = configparser.ConfigParser()
    conf.read(DVC_DIR / sect.get("configpath", "s3_config"))
    kw = {
        "aws_access_key_id": creds["default"]["aws_access_key_id"],
        "aws_secret_access_key": creds["default"]["aws_secret_access_key"],
        "region_name": conf["default"].get("region", "us-west-1"),
    }
    return bucket, prefix.rstrip("/"), kw


def list_remote_md5s(bucket: str, prefix: str, kw: dict[str, str],
                     log=print) -> set[str]:
    import boto3
    s3 = boto3.client("s3", **kw)
    md5s: set[str] = set()
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            n += 1
            m = _KEY_RE.search(obj["Key"])
            if m:
                md5s.add(m.group(1) + m.group(2))
    log(f"[remote] {n} objects under s3://{bucket}/{prefix}/, "
        f"{len(md5s)} content-addressed")
    return md5s


def iter_sidecars(countries: list[str] | None):
    roots = ([COUNTRIES / c for c in countries] if countries
             else [COUNTRIES])
    for root in roots:
        for p in sorted(root.rglob("*.dvc")):
            # rglob matches the .dvc/ config directory itself and anything
            # under it; sidecars are regular files outside that directory.
            if p.is_dir() or ".dvc" in p.parts[:-1]:
                continue
            try:
                doc = yaml.safe_load(p.read_text()) or {}
            except yaml.YAMLError as e:
                print(f"[warn] unparsable sidecar {p}: {e}", file=sys.stderr)
                continue
            for out in doc.get("outs", []) or []:
                md5 = out.get("md5")
                if not md5:
                    continue
                target = p.parent / out.get("path", p.stem)
                yield p, md5, int(out.get("size") or 0), target


def kind_of(rel: Path) -> str:
    return "documentation" if "Documentation" in rel.parts else "data"


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--countries", nargs="*", default=None)
    ap.add_argument("--out", type=Path, default=None,
                    help="TSV report of missing blobs (default: stdout)")
    ap.add_argument("--md5-local", action="store_true",
                    help="md5 any local workspace copy of a missing blob "
                         "and report whether it matches the sidecar")
    args = ap.parse_args(argv)

    bucket, prefix, kw = _remote_from_config()
    remote = list_remote_md5s(bucket, prefix, kw,
                              log=lambda m: print(m, file=sys.stderr))

    rows = []
    total = Counter()
    missing = Counter()
    for sidecar, md5, size, target in iter_sidecars(args.countries):
        rel = sidecar.relative_to(COUNTRIES)
        country = rel.parts[0]
        kind = kind_of(rel)
        total[(country, kind)] += 1
        if md5 in remote:
            continue
        missing[(country, kind)] += 1
        local = target.exists()
        match = ""
        if local and args.md5_local and not md5.endswith(".dir"):
            match = "yes" if file_md5(target) == md5 else "no"
        rows.append((str(rel), md5, size, kind,
                     "yes" if local else "no", match))

    header = "sidecar\tmd5\tsize\tkind\tlocal_copy\tlocal_md5_matches"
    out = open(args.out, "w") if args.out else sys.stdout
    with out:
        print(header, file=out)
        for r in rows:
            print("\t".join(map(str, r)), file=out)

    # summary
    n_missing = sum(missing.values())
    n_total = sum(total.values())
    print(f"[summary] {n_missing} of {n_total} sidecar blobs missing from "
          f"the remote ({100 * n_missing / max(n_total, 1):.1f}%); "
          f"{sum(size for *_, size, _, _, _ in rows) / 1e6:.1f} MB", file=sys.stderr)
    for kind in ("documentation", "data"):
        k_missing = sum(v for (c, k), v in missing.items() if k == kind)
        k_total = sum(v for (c, k), v in total.items() if k == kind)
        print(f"[summary] {kind:14s} {k_missing:5d} / {k_total:5d}", file=sys.stderr)
    print("[summary] by country (missing/total, documentation | data):",
          file=sys.stderr)
    for c in sorted({c for c, _ in total}):
        d, dt = missing[(c, "documentation")], total[(c, "documentation")]
        x, xt = missing[(c, "data")], total[(c, "data")]
        if d or x:
            print(f"  {c:20s} doc {d:4d}/{dt:4d}   data {x:4d}/{xt:5d}",
                  file=sys.stderr)
    if args.md5_local:
        pushable = sum(1 for r in rows if r[5] == "yes")
        stale = sum(1 for r in rows if r[5] == "no")
        print(f"[summary] local copies: {pushable} md5-match (pushable as-is), "
              f"{stale} differ from their sidecar", file=sys.stderr)
    return 1 if rows else 0


if __name__ == "__main__":
    raise SystemExit(main())
