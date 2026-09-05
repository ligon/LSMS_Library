#!/usr/bin/env python
"""Apply GH #782 FIX 2: canonicalise cross-wave Preferred / Aggregate Labels.

Operates on org tables cell-wise with EXACT full-cell matching (never substring:
'Eggs' -> 'Egg' must not touch 'Garden Eggs'), preserving each column's original
display width and the file's CRLF line endings and ISO-8859-1 encoding.
"""
import sys
from pathlib import Path

ROOT = Path('lsms_library/countries/GhanaLSS')
ENC = 'ISO-8859-1'


def edit_table(path, table, columns, renames, dry=False):
    """Rename exact cell values in `columns` of the org table named `table`."""
    raw = path.read_bytes().decode(ENC)
    lines = raw.split('\n')
    out, inside, header, hits = [], False, None, []
    for line in lines:
        eol = '\r' if line.endswith('\r') else ''
        body = line[:-1] if eol else line
        if body.strip().startswith('#+name:'):
            inside = body.strip().split(':', 1)[1].strip() == table
            header = None
            out.append(line)
            continue
        if not (inside and body.lstrip().startswith('|')):
            if inside and body.strip() and not body.lstrip().startswith('|'):
                inside = False
            out.append(line)
            continue
        if set(body.replace('|', '').replace('+', '').strip()) <= {'-'}:
            out.append(line)
            continue
        parts = body.split('|')
        vals = [p.strip() for p in parts]
        if header is None:
            header = vals
            out.append(line)
            continue
        changed = False
        for col in columns:
            if col not in header:
                continue
            k = header.index(col)
            if k >= len(parts):
                continue
            if vals[k] in renames:
                new = renames[vals[k]]
                width = len(parts[k])
                parts[k] = (' ' + new).ljust(width)[:width] if len(' ' + new + ' ') <= width \
                    else ' ' + new + ' '
                hits.append((col, vals[k], new))
                changed = True
        out.append(('|'.join(parts) + eol) if changed else line)
    if hits and not dry:
        path.write_bytes('\n'.join(out).encode(ENC))
    return hits


# (waves, {old: new}) -- the country-level _/food_items.org `food_label` table is
# the authority; every target below is that table's own Preferred Label.
PLAN = [
    (['2005-06', '2012-13', '2016-17'], {'Condiments': 'Condiment'}),
    (['1991-92', '1998-99', '2005-06', '2012-13', '2016-17'], {'Eggs': 'Egg'}),
    (['1991-92', '1998-99'], {'Other Beverage': 'Other Beverages'}),
    (['1991-92', '1998-99'], {'Other Flour': 'Other Flours'}),
    (['1991-92', '1998-99', '2005-06', '2012-13'], {'Other Grain': 'Other Grains'}),
    (['2016-17'], {'Other Vegetable': 'Other Vegetables'}),
    (['2012-13'], {'Tea bags': 'Tea'}),
    (['2016-17'], {'Tea bag': 'Tea'}),
    # encoding artefact on the Aggregate axis only (2012-13 code 262)
    (['2012-13'], {'Cooked Rice and Stew \xc3\x8a \xc3\x8a': 'Cooked Rice and Stew'}),
]

if __name__ == '__main__':
    dry = '--dry' in sys.argv
    total = 0
    for waves, ren in PLAN:
        for w in waves:
            p = ROOT / w / '_' / 'categorical_mapping.org'
            h = edit_table(p, 'harmonize_food',
                           ['Preferred Label', 'Aggregate Label'], ren, dry)
            for col, a, b in h:
                print(f'  {w:8} {col:16} {a!r} -> {b!r}')
            total += len(h)
    print(f'total wave-table cells changed: {total}')
