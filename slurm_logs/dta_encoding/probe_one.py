#!/usr/bin/env python
"""Probe ONE .dta blob for encoding pathology.  Emits one JSON line on stdout.

Reads the L1 DVC blob directly (the same bytes ``get_dataframe`` reads after
``_ensure_dvc_pulled``).  Deliberately pandas-only -- no lsms_library import,
no DVC CLI, no network.

Usage:  probe_one.py <sidecar-relpath> <blob-path>
"""
import sys, json, warnings, unicodedata

def coerce(v, enc):
    """Verbatim copy of ligonlibrary.dataframes._coerce_label (v0.2.0)."""
    return str(v).encode(enc, errors="ignore").decode("utf-8", errors="ignore")

def classify(s):
    """Return the encoding signature of a non-ASCII string."""
    tags = []
    if any(0x80 <= ord(c) <= 0x9F for c in s):
        tags.append('c1')                    # cp1252-read-as-latin-1 signature
    if any(ord(c) >= 256 for c in s):
        tags.append('ge256')                 # genuine UTF-8 decode (cannot have warned)
    else:
        try:
            rt = s.encode('latin-1', 'strict').decode('utf-8', 'strict')
            if rt != s:
                tags.append('double')        # double-encoded (Senegal pattern)
        except (UnicodeEncodeError, UnicodeDecodeError):
            tags.append('nort')              # latin-1->utf-8 round trip impossible
    return tags

def main(rel, blob):
    import pandas as pd
    rec = {'sidecar': rel, 'blob': blob}
    try:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            with pd.io.stata.StataReader(blob) as r:
                vl = r.value_labels()
                rec['format'] = int(r._format_version)
                rec['pandas_encoding'] = r._encoding
            rec['unicode_warning'] = any(x.category is UnicodeWarning for x in w)
    except Exception as e:
        rec['error'] = f'{type(e).__name__}: {e}'[:200]
        print(json.dumps(rec)); return

    n_lbl = n_nonascii = 0
    sigs = {}
    # coercion damage: what would _coerce_label do if encoding= were passed?
    n_coerce_changes = n_coerce_drops = 0
    examples = []
    for lblset, mapping in vl.items():
        for code, label in mapping.items():
            s = str(label); n_lbl += 1
            if not any(ord(c) > 127 for c in s):
                continue
            n_nonascii += 1
            for t in classify(s):
                sigs[t] = sigs.get(t, 0) + 1
            out = coerce(s, 'latin-1')
            if out != s:
                n_coerce_changes += 1
                # a DROP = a non-ASCII char vanished with no replacement
                if len(out) < len(s) and len(out.encode()) < len(s.encode()):
                    try:
                        ref = s.encode('latin-1', 'strict').decode('utf-8', 'strict')
                        clean = (out == ref)
                    except Exception:
                        clean = False
                    if not clean:
                        n_coerce_drops += 1
                        if len(examples) < 5:
                            examples.append({'lblset': str(lblset), 'code': str(code),
                                             'in_cps': [ord(c) for c in s][:60],
                                             'out_cps': [ord(c) for c in out][:60]})
    rec.update(n_value_labels=n_lbl, n_nonascii_labels=n_nonascii, signatures=sigs,
               n_coerce_changes=n_coerce_changes, n_coerce_drops=n_coerce_drops,
               drop_examples=examples)
    print(json.dumps(rec))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
