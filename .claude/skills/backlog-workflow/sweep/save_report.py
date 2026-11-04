#!/usr/bin/env python3
"""save_report.py <transcript.jsonl> <REPORT.md> -- save a subagent's final message.

Claude Code subagents cannot write report files outside their worktree, so a country agent's
REPORT.md arrives as its final message.  This copies the last assistant text out of the
agent's JSONL transcript (the `output_file` the Agent tool names) without printing it into
the coordinator's context.
"""
import json
import sys

last = None
for line in open(sys.argv[1]):
    try:
        r = json.loads(line)
    except ValueError:
        continue
    m = r.get('message') or {}
    if m.get('role') != 'assistant':
        continue
    c = m.get('content')
    txt = c if isinstance(c, str) else '\n'.join(
        x.get('text', '') for x in (c or []) if x.get('type') == 'text')
    if txt.strip():
        last = txt
with open(sys.argv[2], 'w') as fh:
    fh.write((last or '(no final text found)') + '\n')
print(sys.argv[2], len(last or ''), 'chars')
