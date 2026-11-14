#!/usr/bin/env python3
"""Append one TRIAGE.tsv row under an exclusive flock.
usage: python3 -I append_row.py NUMBER TITLE COUNTRIES TIER STATUS EVIDENCE ACTION EFFORT DEPENDS_ON NOTES [--replace]
Tabs/newlines inside fields are replaced by spaces. Refuses a duplicate NUMBER unless --replace."""
import sys, fcntl, os
args = [a for a in sys.argv[1:] if a != '--replace']
replace = '--replace' in sys.argv
if len(args) != 10:
    sys.exit('need exactly 10 fields, got %d' % len(args))
STAT = {'resolved','partial','open','needs-decision','needs-acquisition','excluded-in-flight','not-a-bug'}
ACT = {'close','narrow','split','none','ask'}
if args[4] not in STAT: sys.exit('bad status %r' % args[4])
if args[6] not in ACT: sys.exit('bad action %r' % args[6])
if args[3] not in {'A','B','C','D','-'}: sys.exit('bad tier %r' % args[3])
row = '\t'.join(' '.join(a.split()) for a in args) + '\n'
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'TRIAGE.tsv')
with open(path + '.lock', 'a') as lk:
    fcntl.flock(lk, fcntl.LOCK_EX)
    lines = open(path).read().splitlines(True) if os.path.exists(path) else []
    keys = [l.split('\t', 1)[0] for l in lines]
    if args[0] in keys:
        if not replace: sys.exit('duplicate row for %s (use --replace)' % args[0])
        lines = [l for l in lines if l.split('\t', 1)[0] != args[0]]
        lines.append(row)
        open(path, 'w').write(''.join(lines))
    else:
        open(path, 'a').write(row)
    fcntl.flock(lk, fcntl.LOCK_UN)
print('ok', args[0])
