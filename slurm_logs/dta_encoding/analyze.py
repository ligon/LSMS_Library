"""Summarise the .dta encoding sweep.  Usage: analyze.py sweep_<jobid>.jsonl"""
import json, sys, collections
from pathlib import Path

rows=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
dec = {}
for l in open(Path(__file__).parent/'manifest.tsv'):
    a,b,d = l.rstrip('\n').split('\t'); dec[a]=int(d)

def country(r):
    p = r['sidecar'].split('/')
    return p[2] if len(p)>3 else '?'
def wave(r):
    p = r['sidecar'].split('/')
    return p[3] if len(p)>4 else '?'

ok  = [r for r in rows if 'error' not in r]
err = [r for r in rows if 'error' in r]
print(f"files probed           : {len(rows)}   (readable {len(ok)}, unreadable {len(err)})")
print(f"  of which DECLARED    : {sum(dec.get(r['sidecar'],0) for r in ok)}")

fmt = collections.Counter(r['format'] for r in ok)
enc = collections.Counter(r['pandas_encoding'] for r in ok)
print(f"\nformat versions        : {dict(sorted(fmt.items()))}")
print(f"pandas encoding chosen : {dict(enc)}")

warned = [r for r in ok if r['unicode_warning']]
print(f"\n*** UnicodeWarning fired : {len(warned)} files ***")
for r in warned:
    print(f"     {country(r)}/{wave(r)}  fmt={r['format']} nonascii={r['n_nonascii_labels']}"
          f" declared={dec.get(r['sidecar'],0)}  {r['sidecar'].split('/')[-1]}")

na = [r for r in ok if r['n_nonascii_labels']>0]
print(f"\nfiles with >=1 non-ASCII value label : {len(na)}"
      f"  ({sum(r['n_nonascii_labels'] for r in na)} labels)")

# signature census
sig = collections.Counter()
for r in ok:
    for k,v in (r.get('signatures') or {}).items(): sig[k]+=v
print(f"non-ASCII label signatures (label counts): {dict(sig)}")
print("   c1     = byte in U+0080-U+009F  -> cp1252-read-as-latin-1 signature")
print("   double = latin-1->utf-8 strict round-trip changes it -> double-encoded at source")
print("   ge256  = codepoint >=256 -> genuine utf-8 decode (cannot have warned)")
print("   nort   = no latin-1->utf-8 round trip possible")

drops = [r for r in ok if r.get('n_coerce_drops',0)>0]
print(f"\nfiles where _coerce_label WOULD drop characters (if encoding= were passed): {len(drops)}")
print(f"  total labels affected: {sum(r['n_coerce_drops'] for r in drops)}")

# per country
print("\n%-24s %6s %6s %8s %8s %8s" % ("country","files","warn","nonASCII","dblenc","wouldDrop"))
byc = collections.defaultdict(lambda: [0,0,0,0,0])
for r in ok:
    c=byc[country(r)]
    c[0]+=1; c[1]+=int(r['unicode_warning']); c[2]+=r['n_nonascii_labels']
    c[3]+=(r.get('signatures') or {}).get('double',0); c[4]+=r.get('n_coerce_drops',0)
for k in sorted(byc, key=lambda k:-byc[k][2]):
    v=byc[k]
    if v[2] or v[1]: print("%-24s %6d %6d %8d %8d %8d"%(k,*v))
if err:
    print(f"\nunreadable ({len(err)}), first 5:")
    for r in err[:5]: print("   ",country(r), r['error'][:90])
