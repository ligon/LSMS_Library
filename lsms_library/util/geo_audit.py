"""Audit and manage cluster_features GPS configurations.

Four modes:

  audit      Scan for geovariables files and report coverage gaps.
  generate   Emit YAML snippets for DVC-tracked geo files not yet configured.
  download   Fetch geovariables from the World Bank Microdata Library (NADA API).
  ingest     After placing downloaded geo files in the Data directory,
             run ``dvc add`` and print the YAML to paste into data_info.yml.

Usage:
    python -m lsms_library.util.geo_audit                       # audit
    python -m lsms_library.util.geo_audit --generate            # YAML for gaps
    python -m lsms_library.util.geo_audit download Uganda 2013-14
    python -m lsms_library.util.geo_audit download --all        # all missing
    python -m lsms_library.util.geo_audit ingest Uganda 2013-14 Data/UNPS_Geovars_1314.dta

Environment variables:
    MICRODATA_API_KEY   World Bank Microdata Library API key (required for download).
                        Register at https://microdata.worldbank.org to obtain one.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import yaml


# Patterns that identify geovariables / GPS files in Data directories.
GEO_PATTERNS = [
    "*geovars*",
    "*geovariables*",
    "*Geovars*",
    "*Geovariables*",
    "*GEOVARS*",
    "*grappe_gps*",
    "*agri_gps*",
]

COUNTRIES_DIR = Path(__file__).resolve().parent.parent / "countries"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_geo_files(country_dir: Path) -> dict[str, list[Path]]:
    """Return {wave: [geo_dvc_paths]} for DVC-tracked geo files."""
    results: dict[str, list[Path]] = {}
    for wave_dir in sorted(country_dir.iterdir()):
        if not wave_dir.is_dir() or wave_dir.name.startswith(("_", ".", "var")):
            continue
        geo_files: list[Path] = []
        for pattern in GEO_PATTERNS:
            for dvc_file in wave_dir.rglob(f"Data*/{pattern}.dvc"):
                geo_files.append(dvc_file)
            for dvc_file in wave_dir.rglob(f"Data*/**/{pattern}.dvc"):
                geo_files.append(dvc_file)
        if geo_files:
            seen: set[Path] = set()
            unique = [f for f in geo_files if f not in seen and not seen.add(f)]
            results[wave_dir.name] = unique
    return results


def check_configured(country_dir: Path, wave: str) -> bool:
    """Check if cluster_features already references a geo file via dfs."""
    data_info = country_dir / wave / "_" / "data_info.yml"
    if not data_info.exists():
        return False
    try:
        with open(data_info) as f:
            cfg = yaml.safe_load(f)
    except Exception:
        return False
    cf = cfg.get("cluster_features", {})
    if not isinstance(cf, dict):
        return False
    return bool(cf.get("dfs") and cf.get("df_geo"))


def read_source_url(country_dir: Path, wave: str) -> str | None:
    """Read the World Bank catalog URL from SOURCE.org."""
    source = country_dir / wave / "Documentation" / "SOURCE.org"
    if not source.exists():
        return None
    try:
        text = source.read_text()
        # Match plain URL or org-mode link [[url]]
        m = re.search(r'https?://[^\s\]\)]+', text)
        return m.group(0).rstrip("/") if m else None
    except Exception:
        return None


def guess_lat_lon_vars(geo_path: str) -> tuple[str, str]:
    """Guess lat/lon variable names based on file naming conventions."""
    lower = geo_path.lower()
    if "unps_geovars" in lower:
        return "lat_mod", "lon_mod"
    if "nga_householdgeovariables" in lower or "nga_householdgeovars" in lower:
        if lower.endswith(".csv"):
            return "LAT_DD_MOD", "LON_DD_MOD"
    return "lat_dd_mod", "lon_dd_mod"


def guess_idxvar(country_dir: Path, wave: str) -> dict:
    """Read the cluster index variable from existing cluster_features config."""
    data_info = country_dir / wave / "_" / "data_info.yml"
    if not data_info.exists():
        return {"v": "grappe"}
    try:
        with open(data_info) as f:
            cfg = yaml.safe_load(f)
        cf = cfg.get("cluster_features", {})
        if cf.get("dfs"):
            main = cf.get("df_main", {})
            return main.get("idxvars", {"v": "grappe"})
        return cf.get("idxvars", {"v": "grappe"})
    except Exception:
        return {"v": "grappe"}


def data_path_for_yaml(dvc_path: Path, wave_dir: Path) -> str:
    """Get the data file path as it should appear in data_info.yml."""
    data_file = dvc_path.with_suffix("")  # strip .dvc
    try:
        for data_dir in wave_dir.iterdir():
            if data_dir.name.startswith("Data") and data_dir.is_dir():
                try:
                    rel = data_file.relative_to(data_dir)
                    if data_dir.name != "Data":
                        return f"../{data_dir.name}/{rel}"
                    return str(rel)
                except ValueError:
                    continue
    except Exception:
        pass
    return data_file.name


def generate_snippet(country: str, wave: str, geo_files: list[Path],
                     country_dir: Path) -> str:
    """Generate a YAML snippet for df_geo."""
    hh_geo = next(
        (gf for gf in geo_files if "plot" not in gf.name.lower()),
        geo_files[0],
    )
    wave_dir = country_dir / wave
    file_path = data_path_for_yaml(hh_geo, wave_dir)
    lat_var, lon_var = guess_lat_lon_vars(file_path)
    idxvars = guess_idxvar(country_dir, wave)
    v_var = idxvars.get("v", "grappe")

    lines = [f"# {country} {wave}: add to cluster_features"]
    lines.append("    df_geo:")
    lines.append(f"        file: {file_path}")
    lines.append("        idxvars:")
    if isinstance(v_var, list):
        lines.append("            v:")
        for v in v_var:
            lines.append(f"                - {v}")
    else:
        lines.append(f"            v: {v_var}")
    lines.append("        myvars:")
    lines.append(f"            Latitude: {lat_var}")
    lines.append(f"            Longitude: {lon_var}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_audit(args: argparse.Namespace) -> None:
    """Audit geo coverage across all countries."""
    gaps = 0
    configured = 0

    for country_dir in sorted(COUNTRIES_DIR.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        country = country_dir.name
        geo_by_wave = find_geo_files(country_dir)
        if not geo_by_wave:
            continue

        for wave, geo_files in sorted(geo_by_wave.items()):
            is_configured = check_configured(country_dir, wave)
            hh_files = [f for f in geo_files if "plot" not in f.name.lower()] or geo_files

            if is_configured:
                configured += 1
                print(f"  OK  {country}/{wave}: "
                      f"{', '.join(f.stem for f in hh_files)}")
            else:
                gaps += 1
                print(f"  GAP {country}/{wave}: "
                      f"{', '.join(f.stem for f in hh_files)}")
                if args.generate:
                    print(generate_snippet(country, wave, geo_files, country_dir))
                    print()

    print(f"\nSummary: {configured} configured, {gaps} gaps")

    # Report waves that have cluster_features but no geo data in DVC
    print("\nWaves with cluster_features but no geo files in DVC:")
    print("(download from the World Bank Microdata Library, then run 'ingest')\n")
    found_any = False
    for country_dir in sorted(COUNTRIES_DIR.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        country = country_dir.name
        geo_by_wave = find_geo_files(country_dir)

        for wave_dir in sorted(country_dir.iterdir()):
            if not wave_dir.is_dir() or wave_dir.name.startswith(("_", ".", "var", "TODO")):
                continue
            wave = wave_dir.name
            # Skip if geo data already exists for this wave
            if wave in geo_by_wave:
                continue
            # Check if this wave has cluster_features configured (without geo)
            di = wave_dir / "_" / "data_info.yml"
            if not di.exists():
                continue
            try:
                with open(di) as f:
                    cfg = yaml.safe_load(f)
                if "cluster_features" not in cfg:
                    continue
            except Exception:
                continue

            source_url = read_source_url(country_dir, wave)
            download_url = f"{source_url}/get-microdata" if source_url else "URL not found"
            print(f"  {country}/{wave}: {download_url}")
            found_any = True

    if not found_any:
        print("  (none)")


def _extract_catalog_id(url: str) -> str | None:
    """Extract the numeric catalog ID from a World Bank Microdata URL."""
    m = re.search(r'/catalog/(\d+)', url)
    return m.group(1) if m else None


def _nada_api_base(url: str) -> str:
    """Derive the NADA API base from a catalog URL."""
    parsed = urlparse(url)
    # Most are microdata.worldbank.org; some are statsghana etc.
    return f"{parsed.scheme}://{parsed.netloc}/index.php/api"


def _resolve_idno(api_base: str, catalog_id: str,
                  api_key: str) -> str | None:
    """Resolve a numeric catalog ID to the NADA string idno via search."""
    import urllib.request
    import json

    # Search with enough results and filter by id
    url = f"{api_base}/catalog/search?ps=500&collection=lsms"
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", api_key)
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  Search API error: {e}", file=sys.stderr)
        return None

    rows = data.get("result", {}).get("rows", [])
    for row in rows:
        if str(row.get("id")) == str(catalog_id):
            return row.get("idno")

    # If not found in lsms collection, try a broader search
    url = f"{api_base}/catalog/search?ps=50&id={catalog_id}"
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", api_key)
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  Broader search API error: {e}", file=sys.stderr)
        return None

    rows = data.get("result", {}).get("rows", [])
    for row in rows:
        if str(row.get("id")) == str(catalog_id):
            return row.get("idno")

    return None


# Cache for idno lookups (populated once per session)
_idno_cache: dict[str, str] = {}


def _get_idno(api_base: str, catalog_id: str, api_key: str) -> str | None:
    """Get idno for a catalog ID, with caching."""
    if catalog_id in _idno_cache:
        return _idno_cache[catalog_id]

    # On first call, populate cache from LSMS collection search
    if not _idno_cache:
        import urllib.request
        import json

        print("  Fetching LSMS catalog index...", end=" ", flush=True)
        # Paginate to get all entries
        offset = 0
        while True:
            url = (f"{api_base}/catalog/search"
                   f"?ps=500&collection=lsms&offset={offset}")
            req = urllib.request.Request(url)
            req.add_header("X-API-KEY", api_key)
            req.add_header("Accept", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read())
            except Exception as e:
                print(f"error: {e}")
                break
            rows = data.get("result", {}).get("rows", [])
            if not rows:
                break
            for row in rows:
                _idno_cache[str(row.get("id"))] = row.get("idno", "")
            found = data.get("result", {}).get("found", 0)
            offset += len(rows)
            if offset >= found:
                break
        print(f"{len(_idno_cache)} entries.")

    return _idno_cache.get(catalog_id)


def _find_geo_resources(api_base: str, idno: str,
                        api_key: str) -> list[dict]:
    """Query the NADA API for geovariables resources in a catalog entry."""
    import urllib.request
    import json

    url = f"{api_base}/catalog/{idno}/resources"
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", api_key)
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  API error for {idno}: {e}", file=sys.stderr)
        return []

    resources = data.get("resources", [])
    if not resources:
        resources = data if isinstance(data, list) else []
    geo_resources = []
    geo_keywords = ["geovars", "geovariable", "gps", "geo_"]
    for r in resources:
        title = (r.get("title", "") + r.get("filename", "")).lower()
        if any(kw in title for kw in geo_keywords):
            # Prefer household-level over plot-level
            if "plot" not in title:
                geo_resources.append(r)
    return geo_resources


def _download_resource(api_base: str, idno: str, resource: dict,
                       dest_dir: Path, api_key: str) -> Path | None:
    """Download a single resource file from the NADA catalog."""
    import urllib.request

    resource_id = resource.get("resource_id") or resource.get("id")
    filename = resource.get("filename", f"geovars_{idno}.dta")

    # NADA download endpoint — uses the numeric survey_id, not idno
    survey_id = resource.get("survey_id", "")
    download_url = (f"{api_base.rsplit('/api', 1)[0]}"
                    f"/catalog/{survey_id}/download/{resource_id}/"
                    f"{filename}")

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename

    if dest.exists():
        print(f"  Already exists: {dest}")
        return dest

    print(f"  Downloading {filename} ...")
    req = urllib.request.Request(download_url)
    req.add_header("X-API-KEY", api_key)

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            with open(dest, "wb") as f:
                while True:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
        print(f"  Saved to {dest}")
        return dest
    except Exception as e:
        print(f"  Download failed: {e}", file=sys.stderr)
        if dest.exists():
            dest.unlink()
        return None


def _find_missing_geo_waves() -> list[tuple[str, str, Path]]:
    """Find all country/wave pairs that have cluster_features but no geo data."""
    missing = []
    for country_dir in sorted(COUNTRIES_DIR.iterdir()):
        if not country_dir.is_dir() or country_dir.name.startswith("."):
            continue
        geo_by_wave = find_geo_files(country_dir)

        for wave_dir in sorted(country_dir.iterdir()):
            if not wave_dir.is_dir() or wave_dir.name.startswith(("_", ".", "var", "TODO")):
                continue
            wave = wave_dir.name
            if wave in geo_by_wave:
                continue
            di = wave_dir / "_" / "data_info.yml"
            if not di.exists():
                continue
            try:
                with open(di) as f:
                    cfg = yaml.safe_load(f)
                if "cluster_features" not in cfg:
                    continue
            except Exception:
                continue
            missing.append((country_dir.name, wave, country_dir))
    return missing


def cmd_download(args: argparse.Namespace) -> None:
    """Download geovariables files from the World Bank Microdata Library."""
    api_key = args.api_key or os.environ.get("MICRODATA_API_KEY")
    if not api_key:
        print("Error: API key required. Set MICRODATA_API_KEY or use --api-key.",
              file=sys.stderr)
        print("Register at https://microdata.worldbank.org to get an API key.",
              file=sys.stderr)
        sys.exit(1)

    if args.all:
        targets = _find_missing_geo_waves()
        if not targets:
            print("No missing geo files found.")
            return
        print(f"Found {len(targets)} waves missing geo data.\n")
    else:
        if not args.country or not args.wave:
            print("Error: specify --all or provide COUNTRY WAVE", file=sys.stderr)
            sys.exit(1)
        country_dir = COUNTRIES_DIR / args.country
        targets = [(args.country, args.wave, country_dir)]

    downloaded = []
    for country, wave, country_dir in targets:
        print(f"\n{country}/{wave}:")
        source_url = read_source_url(country_dir, wave)
        if not source_url:
            print("  No SOURCE.org URL found, skipping.")
            continue

        catalog_id = _extract_catalog_id(source_url)
        if not catalog_id:
            print(f"  Could not extract catalog ID from {source_url}, skipping.")
            continue

        api_base = _nada_api_base(source_url)

        # Only use worldbank.org API — other hosts (statsghana) may not work
        if "worldbank.org" not in api_base:
            print(f"  Non-World Bank source ({api_base}), skipping.")
            continue

        # Resolve numeric catalog ID to string idno
        idno = _get_idno(api_base, catalog_id, api_key)
        if not idno:
            print(f"  Could not resolve catalog {catalog_id} to idno, skipping.")
            continue
        print(f"  idno: {idno}")

        resources = _find_geo_resources(api_base, idno, api_key)
        if not resources:
            print(f"  No geovariables resources found for {idno}.")
            continue

        wave_dir = country_dir / wave
        data_dir = wave_dir / "Data"
        for resource in resources:
            result = _download_resource(api_base, idno, resource,
                                        data_dir, api_key)
            if result:
                downloaded.append((country, wave, result))

        # Rate-limit to be polite
        time.sleep(1)

    if downloaded:
        print(f"\n{'='*60}")
        print(f"Downloaded {len(downloaded)} file(s). Next steps:\n")
        for country, wave, path in downloaded:
            rel = path.relative_to(COUNTRIES_DIR / country / wave)
            print(f"  python -m lsms_library.util.geo_audit ingest "
                  f"{country} {wave} {rel}")
    else:
        print("\nNo files downloaded.")


def cmd_ingest(args: argparse.Namespace) -> None:
    """After placing a downloaded geo file, run dvc add and print YAML."""
    country_dir = COUNTRIES_DIR / args.country
    if not country_dir.is_dir():
        print(f"Error: country directory not found: {country_dir}", file=sys.stderr)
        sys.exit(1)

    wave_dir = country_dir / args.wave
    if not wave_dir.is_dir():
        print(f"Error: wave directory not found: {wave_dir}", file=sys.stderr)
        sys.exit(1)

    file_path = wave_dir / args.file
    if not file_path.exists():
        print(f"Error: file not found: {file_path}", file=sys.stderr)
        print(f"\nPlace the downloaded geovariables file at:", file=sys.stderr)
        print(f"  {file_path}", file=sys.stderr)
        sys.exit(1)

    # Run dvc add
    print(f"Running: dvc add {file_path}")
    result = subprocess.run(
        ["dvc", "add", str(file_path)],
        cwd=str(COUNTRIES_DIR),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"dvc add failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"  Created {file_path}.dvc")

    # Generate YAML snippet
    dvc_path = file_path.with_suffix(file_path.suffix + ".dvc")
    lat_var, lon_var = guess_lat_lon_vars(str(file_path))
    idxvars = guess_idxvar(country_dir, args.wave)
    v_var = idxvars.get("v", "grappe")

    # Path as it should appear in data_info.yml (relative to Data/)
    yaml_path = args.file
    if yaml_path.startswith("Data/"):
        yaml_path = yaml_path[5:]

    print(f"\nAdd this to {wave_dir / '_' / 'data_info.yml'}:\n")
    print("cluster_features:")
    print("    dfs:")
    print("        - df_main")
    print("        - df_geo")
    print("    df_main:")
    print("        ...  # (keep existing file/idxvars/myvars)")
    print("    df_geo:")
    print(f"        file: {yaml_path}")
    print("        idxvars:")
    if isinstance(v_var, list):
        print("            v:")
        for v in v_var:
            print(f"                - {v}")
    else:
        print(f"            v: {v_var}")
    print("        myvars:")
    print(f"            Latitude: {lat_var}")
    print(f"            Longitude: {lon_var}")
    print("    merge_on:")
    print("        - v")
    print("    final_index:")
    print("        - t")
    print("        - v")

    # Remind about git
    print(f"\nDon't forget:")
    print(f"  git add {dvc_path}")
    print(f"  git add {wave_dir / '_' / 'data_info.yml'}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Audit and manage cluster_features GPS coverage")
    sub = parser.add_subparsers(dest="command")

    # Default (no subcommand) = audit
    parser.add_argument("--generate", action="store_true",
                        help="Emit YAML snippets for unconfigured geo files")

    dl = sub.add_parser("download",
                         help="Download geovariables from World Bank NADA API")
    dl.add_argument("country", nargs="?",
                    help="Country name (directory name)")
    dl.add_argument("wave", nargs="?",
                    help="Wave identifier (e.g. 2013-14)")
    dl.add_argument("--all", action="store_true",
                    help="Download all missing geo files")
    dl.add_argument("--api-key",
                    help="NADA API key (or set MICRODATA_API_KEY env var)")

    ingest = sub.add_parser("ingest",
                            help="DVC-add a downloaded geo file and emit YAML")
    ingest.add_argument("country", help="Country name (directory name)")
    ingest.add_argument("wave", help="Wave identifier (e.g. 2013-14)")
    ingest.add_argument("file",
                        help="Path to geo file relative to wave dir "
                             "(e.g. Data/UNPS_Geovars_1314.dta)")

    args = parser.parse_args(argv)

    if args.command == "download":
        cmd_download(args)
    elif args.command == "ingest":
        cmd_ingest(args)
    else:
        cmd_audit(args)


if __name__ == "__main__":
    main()
