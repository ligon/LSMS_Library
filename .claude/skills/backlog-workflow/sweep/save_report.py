#!/usr/bin/env python3
"""Copy an explicitly supplied UTF-8 task report/final-text artifact, byte for byte.

save_report.py --report-artifact /approved/task/final.txt --out /approved/REPORT.md
save_report.py --dispatch-evidence-dir /supplied/evidence --out /approved/REPORT.md

The latter reads only report.md. Never supply a native transcript, session,
SQLite database, home/state file or auth/config artifact. In-process agents can
return final text to their coordinator; agents with a supported evidence grant
may write there. This helper does not discover or extract native messages.
Ledger 1423 §§3–5: explicit artifact transport replaces native transcript extraction.
"""
import argparse
import os
import pathlib
import stat
import tempfile


def checked_path(value):
    path = pathlib.Path(os.path.abspath(value))
    native_parts = {'.codex', '.claude', '.sucoder', '.config', '.cache', 'sessions', 'transcripts'}
    if native_parts.intersection(path.parts):
        raise ValueError('native home/session/transcript paths are prohibited')
    for key in ('CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'SUCODER_HOME'):
        native = os.environ.get(key)
        if native and path.is_relative_to(pathlib.Path(os.path.abspath(native))):
            raise ValueError('native home/state paths are prohibited')
    # Metadata only, before opening: no symlink traversal into native state.
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('report paths must not traverse symlinks')
    if path.suffix.lower() not in {'.md', '.txt'}:
        raise ValueError('only explicit .md/.txt task reports are supported')
    return path


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    source = ap.add_mutually_exclusive_group(required=True)
    source.add_argument('--report-artifact')
    source.add_argument('--dispatch-evidence-dir')
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    os.umask(0o077)
    src = checked_path(a.report_artifact if a.report_artifact else
                       str(pathlib.Path(a.dispatch_evidence_dir) / 'report.md'))
    dst = checked_path(a.out)
    with os.fdopen(os.open(src, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as f:
        if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
            raise ValueError('source must be a regular task report')
        report = f.read()
    if not report.decode('utf-8').strip():
        raise ValueError('empty report; coordinator must obtain actual final text')
    dst.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=dst.parent, delete=False) as f:
            temporary = f.name
            f.write(report)
        os.replace(temporary, dst)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
    print(dst, len(report), 'bytes')


if __name__ == '__main__':
    main()
