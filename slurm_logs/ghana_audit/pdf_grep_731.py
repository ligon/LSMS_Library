"""Grep a PDF's text layer page by page (pdfminer), printing matches with context.

Usage: python pdf_grep_731.py <pdf> <regex> [--ctx N] [--pages a-b]
Read-only.  GH #731 / #705 evidence gathering.
"""
import re, sys, argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('pdf'); ap.add_argument('regex')
    ap.add_argument('--ctx', type=int, default=2)
    ap.add_argument('--pages', default=None)
    ap.add_argument('--maxhits', type=int, default=60)
    a = ap.parse_args()
    from pdfminer.high_level import extract_pages
    from pdfminer.layout import LTTextContainer
    rx = re.compile(a.regex, re.I)
    lo, hi = (1, 10**6)
    if a.pages:
        p = a.pages.split('-'); lo = int(p[0]); hi = int(p[-1])
    hits = 0
    for pno, page in enumerate(extract_pages(a.pdf), start=1):
        if pno < lo or pno > hi: continue
        text = ''.join(el.get_text() for el in page if isinstance(el, LTTextContainer))
        lines = text.splitlines()
        for i, ln in enumerate(lines):
            if rx.search(ln):
                hits += 1
                s = max(0, i - a.ctx); e = min(len(lines), i + a.ctx + 1)
                print(f"--- p{pno} l{i+1} ---")
                print('\n'.join(lines[s:e]))
                if hits >= a.maxhits:
                    print("[maxhits reached]"); return
    print(f"[done: {hits} hits]")

if __name__ == '__main__':
    main()
