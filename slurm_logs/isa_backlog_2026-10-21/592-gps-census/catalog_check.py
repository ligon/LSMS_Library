"""GH #592: does the WB catalog release for a wave carry household / EA coordinates?

Read-only.  Uses the PUBLIC NADA endpoints (no API key, no download):
    /api/catalog/{idno}/data_files          -- the release's file list
    /api/catalog/{idno}/variables?page=N    -- every documented variable

Usage:
    python -I catalog_check.py CACHE_DIR OUT_JSON IDNO [IDNO ...]

CACHE_DIR holds the raw API responses (NOT committed -- untrusted data, and
large); OUT_JSON gets a compact summary: file count, variable count, files
whose name/description looks geographic, and variables whose name/label looks
like a coordinate.  Aggregates and file/variable NAMES only -- no values.
"""
import json
import os
import re
import sys
import urllib.request

BASE = "https://microdata.worldbank.org/index.php/api/catalog"
GEO_FILE = re.compile(r"geo|gps|offset|coord", re.I)
COORD_VAR = re.compile(r"gps|latitud|longitud|coord|geovar|offset|lat_|lon_|_lat\b|_lon\b|^lat|^lon", re.I)
NOT_COORD = re.compile(r"relat|populat|isolat|calculat|latrine|plat|long ?term|how long|longer|collat|translat|regulat", re.I)


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "lsms_library"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def fetch(cache, idno):
    fpath = os.path.join(cache, f"{idno}.datafiles.json")
    vpath = os.path.join(cache, f"{idno}.allvars.json")
    if not os.path.exists(fpath):
        json.dump(_get(f"{BASE}/{idno}/data_files"), open(fpath, "w"))
    if not os.path.exists(vpath):
        allv, page, total = [], 1, 0
        while True:
            d = _get(f"{BASE}/{idno}/variables?page={page}")
            v = d.get("variables", [])
            allv.extend(v)
            total = d.get("total", 0)
            if not v or len(allv) >= total:
                break
            page += 1
        json.dump({"idno": idno, "total": total, "variables": allv}, open(vpath, "w"))
    return json.load(open(fpath)), json.load(open(vpath))


def summarise(idno, files, variables):
    df = files.get("datafiles", {})
    df = list(df.values()) if isinstance(df, dict) else list(df)
    fid_name = {f.get("file_id"): f.get("file_name") for f in df}
    geo_files = [
        {"file": f.get("file_name"), "cases": f.get("case_count"), "vars": f.get("var_count")}
        for f in df
        if GEO_FILE.search(f.get("file_name") or "")
        or GEO_FILE.search((f.get("description") or "").split("\n")[0])
    ]
    coord_vars = []
    for v in variables.get("variables", []):
        txt = f"{v.get('name') or ''} {v.get('labl') or ''}"
        if COORD_VAR.search(txt) and not NOT_COORD.search(txt):
            coord_vars.append({"file": fid_name.get(v.get("fid")), "name": v.get("name"),
                               "label": (v.get("labl") or "")[:90]})
    return {"idno": idno, "n_datafiles": len(df),
            "n_variables": len(variables.get("variables", [])),
            "n_variables_total_reported": variables.get("total"),
            "geo_named_files": geo_files, "coordinate_like_variables": coord_vars}


def main(cache, out, idnos):
    os.makedirs(cache, exist_ok=True)
    res = []
    for idno in idnos:
        files, variables = fetch(cache, idno)
        res.append(summarise(idno, files, variables))
    json.dump(res, open(out, "w"), indent=1)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3:])
