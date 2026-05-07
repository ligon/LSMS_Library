"""Edit Org-mode table columns in place.

Generic helper for adding or updating a derived column in any
``#+name: <table>`` block.  Used most prominently to manage the
``Aggregate Label`` and ``Aggregate (short)`` columns in country-level
``categorical_mapping.org`` files, but applies to any named Org table.

Library usage::

    from lsms_library.util.orgtbl import add_or_update_column
    n, unmapped = add_or_update_column(
        Path('Malawi/_/categorical_mapping.org'),
        table_name='harmonize_food',
        column_name='Aggregate (short)',
        mapping=short_label_dict,             # {Aggregate Label: short}
        source_column='Aggregate Label',
        insert_after='Aggregate Label',
    )

CLI usage::

    python -m lsms_library.util.orgtbl \\
        lsms_library/countries/Malawi/_/categorical_mapping.org \\
        --table harmonize_food \\
        --column 'Aggregate (short)' \\
        --mapping lsms_library/countries/Malawi/_/aggregate_short.yml \\
        --source-column 'Aggregate Label' \\
        --insert-after 'Aggregate Label'

The mapping YAML / JSON is a flat dict from ``source_column`` value to
``column_name`` value::

    Avocado: Avocado
    Banana: Banana
    Beer & Liquor: Beer
    ...

Source values that don't appear in the mapping are written as ``???``
and counted in the returned ``unmapped`` count.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Mapping


def _split_row(line: str) -> list[str]:
    s = line.strip()
    if s.startswith('|'):
        s = s[1:]
    if s.endswith('|'):
        s = s[:-1]
    return [c.strip() for c in s.split('|')]


def _is_separator(line: str) -> bool:
    s = line.strip()
    return (s.startswith('|-') or s.startswith('|+')
            or (s.startswith('|') and all(c in '|-+ ' for c in s)))


def add_or_update_column(
    orgpath: Path,
    table_name: str,
    column_name: str,
    mapping: Mapping[str, str],
    *,
    source_column: str | None = None,
    insert_after: str | None = None,
    unmapped_marker: str = '???',
) -> tuple[int, int]:
    """Add or update *column_name* in the Org table named *table_name* at *orgpath*.

    For each data row, look up the value of *source_column* (or the first
    column if not specified) in *mapping* and write the corresponding
    value to *column_name*.  If *column_name* doesn't already exist as a
    header it is created -- after *insert_after* if specified, otherwise
    at the end.  If it does exist, every row's cell in that column is
    overwritten (i.e., this is idempotent on re-run).

    Source values not present in *mapping* get *unmapped_marker* and
    contribute to the returned unmapped count.

    Returns ``(n_data_rows, n_unmapped)``.

    The whole rewrite preserves left-justified equal-padded column
    alignment (computed from the new max width per column).  Org-mode
    realignment in an editor is non-destructive on top of this.
    """
    text = orgpath.read_text()
    lines = text.splitlines(keepends=False)

    name_re = re.compile(rf'^#\+name:\s*{re.escape(table_name)}\s*$',
                         re.IGNORECASE)
    try:
        start = next(i for i, ln in enumerate(lines) if name_re.match(ln))
    except StopIteration as exc:
        raise KeyError(f"#+name: {table_name} not found in {orgpath}") from exc

    i = start + 1
    while i < len(lines) and not lines[i].lstrip().startswith('|'):
        i += 1
    if i == len(lines):
        raise ValueError(f"No table body after #+name: {table_name}")
    header_idx = i

    j = header_idx
    while j < len(lines):
        if lines[j].strip() == '':
            break
        if not (lines[j].lstrip().startswith('|') or _is_separator(lines[j])):
            break
        j += 1
    end_idx = j

    table_lines = lines[header_idx:end_idx]

    header_cells = _split_row(table_lines[0])
    if source_column is None:
        source_column = header_cells[0]
    if source_column not in header_cells:
        raise KeyError(f"source column {source_column!r} not in table "
                       f"{table_name!r}; have {header_cells}")
    src_idx = header_cells.index(source_column)

    if column_name in header_cells:
        col_idx = header_cells.index(column_name)
        new_header = header_cells[:]
        column_existed = True
    else:
        column_existed = False
        if insert_after is None:
            col_idx = len(header_cells)
            new_header = header_cells + [column_name]
        else:
            if insert_after not in header_cells:
                raise KeyError(f"insert_after column {insert_after!r} not "
                               f"in table {table_name!r}")
            col_idx = header_cells.index(insert_after) + 1
            new_header = (header_cells[:col_idx] + [column_name]
                          + header_cells[col_idx:])

    new_data = []
    unmapped = 0
    for ln in table_lines[1:]:
        if _is_separator(ln):
            new_data.append('SEPARATOR')
            continue
        cells = _split_row(ln)
        src_val = cells[src_idx]
        if src_val in mapping:
            new_val = str(mapping[src_val])
        else:
            new_val = unmapped_marker
            unmapped += 1
        if column_existed:
            # pad row in case it's somehow short
            while len(cells) <= col_idx:
                cells.append('')
            cells[col_idx] = new_val
        else:
            cells = cells[:col_idx] + [new_val] + cells[col_idx:]
        new_data.append(cells)

    # Compute column widths from header + data
    n_cols = len(new_header)
    widths = [len(new_header[c]) for c in range(n_cols)]
    for row in new_data:
        if row == 'SEPARATOR':
            continue
        # Pad short rows defensively
        while len(row) < n_cols:
            row.append('')
        for c in range(n_cols):
            widths[c] = max(widths[c], len(row[c]))

    def fmt_row(cells):
        return ('| ' + ' | '.join(c.ljust(w) for c, w in zip(cells, widths))
                + ' |')

    def fmt_sep():
        return '|' + '+'.join('-' * (w + 2) for w in widths) + '|'

    out_lines = [fmt_row(new_header)]
    for row in new_data:
        out_lines.append(fmt_sep() if row == 'SEPARATOR' else fmt_row(row))

    new_lines = lines[:header_idx] + out_lines + lines[end_idx:]
    new_text = '\n'.join(new_lines) + ('\n' if text.endswith('\n') else '')
    orgpath.write_text(new_text)

    n_data_rows = sum(1 for r in new_data if r != 'SEPARATOR')
    return n_data_rows, unmapped


def _load_mapping(path: Path) -> dict[str, str]:
    suffix = path.suffix.lower()
    if suffix in ('.yml', '.yaml'):
        try:
            import yaml
        except ImportError as exc:
            raise ImportError("PyYAML required to read .yml mappings; "
                              "install pyyaml") from exc
        loaded = yaml.safe_load(path.read_text())
    elif suffix == '.json':
        import json
        loaded = json.loads(path.read_text())
    else:
        raise ValueError(f"unsupported mapping file extension {suffix!r}; "
                         "use .yml/.yaml or .json")
    if not isinstance(loaded, dict):
        raise ValueError(f"mapping file {path} must be a flat dict, "
                         f"got {type(loaded).__name__}")
    return {str(k): str(v) for k, v in loaded.items()}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Add or update a derived column in an Org table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split('CLI usage::')[1].strip()[:1500],
    )
    ap.add_argument('orgfile', type=Path,
                    help='path to the .org file containing the table')
    ap.add_argument('--table', required=True,
                    help='Org table name (the value after #+name:)')
    ap.add_argument('--column', required=True,
                    help='target column to add/update')
    ap.add_argument('--mapping', type=Path, required=True,
                    help='YAML/JSON file with {source_value: target_value} dict')
    ap.add_argument('--source-column', default=None,
                    help='source column to look up (default: first column)')
    ap.add_argument('--insert-after', default=None,
                    help='header name to insert new column after '
                         '(default: end; ignored if column already exists)')
    ap.add_argument('--unmapped-marker', default='???',
                    help="value to write for unmapped rows (default: '???')")
    args = ap.parse_args(argv)

    mapping = _load_mapping(args.mapping)

    n, unmapped = add_or_update_column(
        args.orgfile, args.table, args.column, mapping,
        source_column=args.source_column,
        insert_after=args.insert_after,
        unmapped_marker=args.unmapped_marker,
    )
    print(f"Wrote {args.orgfile}")
    print(f"  Data rows: {n}, unmapped: {unmapped}")
    return 0 if unmapped == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
