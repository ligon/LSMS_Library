"""Scan pytest outputs for ids / coordinates.  Writes matched lines to a
PRIVATE file (job dir) and prints counts only."""
import re, sys
PAT = {
    "ea_code_8": re.compile(r"(?<![\d.\w])[1-3]\d{7}(?![\d.\w])"),
    "panel_id": re.compile(r"(?<![\d\w])\d{4}-\d{3}(?![\d\w])"),
    "case_id_12": re.compile(r"(?<![\d\w])\d{12}(?![\d\w])"),
    "lat": re.compile(r"-(?:9|1[0-7])\.\d{3,}"),
    "lon": re.compile(r"(?<![\d.\w])3[2-6]\.\d{3,}"),
}
priv = open(sys.argv[1], "w")
for path in sys.argv[2:]:
    lines = open(path, errors="replace").read().splitlines()
    n_failed = sum(l.startswith("FAILED ") for l in lines)
    n_err = sum(l.startswith("ERROR ") for l in lines)
    # report lines: assertion explanations / exception text, captured output
    in_fail = False; hits = {k: 0 for k in PAT}; hits_all = {k: 0 for k in PAT}
    for l in lines:
        if "= FAILURES =" in l or "= ERRORS =" in l: in_fail = True
        if "short test summary info" in l: in_fail = False
        is_report = l.startswith("E ") or l.startswith("E\t") or (in_fail and not l.startswith("    ") and not l.startswith(">"))
        for k, rx in PAT.items():
            if rx.search(l):
                hits_all[k] += 1
                if is_report:
                    hits[k] += 1; print(f"{path}: [{k}] {l[:400]}", file=priv)
                else:
                    print(f"{path}: (source/other) [{k}] {l[:200]}", file=priv)
    print(f"{path.split('/')[-1]}: FAILED={n_failed} ERROR={n_err} | report-line hits {hits} | any-line hits {hits_all}")
