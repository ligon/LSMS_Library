"""GH #731: which raw attainment labels decode to U6 (and its neighbours) in GhanaLSS?

Reads each wave's declared source variable for ``Educational Attainment``
straight off the source file via ``get_dataframe`` (no mapping applied) and
tabulates the raw labels, per wave.  Run from a shell with
LSMS_COUNTRIES_ROOT pinned to the worktree and a private LSMS_DATA_DIR.
"""
import os, re, sys
import pandas as pd

def main():
    from lsms_library.paths import countries_root
    from lsms_library.local_tools import get_dataframe
    root = countries_root()
    assert 'wt-glss-731-705' in str(root), root
    waves = {
        '1987-88': ('Y03I.DAT', 'GRADE'),
        '1988-89': ('Y03I.DAT', 'GRADE'),
        '1991-92': ('S2.DTA', 's2q2'),
        '1998-99': ('SEC2A.DTA', 's2aq2'),
        '2005-06': ('parta/sec2a.dta', 's2aq2'),
        '2012-13': ('PARTA/SEC2a.dta', 's2aq2'),
        '2016-17': ('g7sec2.dta', 's2aq2'),
    }
    pat = re.compile(r'^\s*(?:[ULul][1-9]|[Aa][12]|Sixth Form|Year .*|Level .*|[Pp][Ss][1-3]|[Tt][1-4])\s*$')
    pd.set_option('display.width', 200)
    for w, (fn, var) in waves.items():
        path = root / 'GhanaLSS' / w / 'Data' / fn
        print(f"\n===== {w}  {fn}:{var} =====")
        try:
            df = get_dataframe(str(path))
        except Exception as e:
            print("READ FAILED:", type(e).__name__, e); continue
        if var not in df.columns:
            print("columns:", list(df.columns)[:40]); print("VAR MISSING"); continue
        s = df[var]
        print("dtype:", s.dtype, " n =", len(s), " null =", int(s.isna().sum()))
        vc = s.astype(str).value_counts(dropna=False)
        print("--- full value_counts (raw, stringified) ---")
        print(vc.to_string())
        hits = {k: v for k, v in vc.items() if pat.match(str(k))}
        print("--- ladder-code hits (U/L/A/T/PS/Year/Level) ---")
        print(hits if hits else "NONE")

if __name__ == '__main__':
    main()
