"""Apply one harmonize_food mutation to a THROWAWAY clone's categorical_mapping.org.

usage: python mutate.py <clone> <mutation>
mutations: none | extra_cell (code 15 2013Q1 -> Semovita) | drop_cell (code 15 2012Q3 -> ---)
           | agg (Semovita row Aggregate -> Wheat Flour) | other_cell (code 16 2012Q3 -> 'Maize flourX')
           | drop_row (delete the label-only Semovita row)
"""
import sys
from pathlib import Path

clone, mutation = Path(sys.argv[1]), sys.argv[2]
org = clone / 'lsms_library/countries/Nigeria/_/categorical_mapping.org'
lines = org.read_text().split('\n')
start = next(i for i, x in enumerate(lines) if x.strip() == '#+NAME: harmonize_food')
end = next(i for i in range(start + 1, len(lines)) if not lines[i].startswith('|') and i > start + 1)


def row_index(pred):
    hits = [i for i in range(start + 1, end) if pred(lines[i].split('|'))]
    assert len(hits) == 1, (mutation, hits)
    return hits[0]


def code_row(code):
    return row_index(lambda cells: len(cells) > 3 and cells[1].strip() == str(code))


def set_cell(i, col, value):
    cells = lines[i].split('|')
    width = len(cells[col])
    cells[col] = (' ' + value).ljust(width)
    lines[i] = '|'.join(cells)


changed = 0
if mutation == 'extra_cell':
    set_cell(code_row(15), 7, 'Semovita'); changed = 1           # 2013Q1
elif mutation == 'drop_cell':
    set_cell(code_row(15), 6, '---'); changed = 1                # 2012Q3
elif mutation == 'agg':
    i = row_index(lambda cells: len(cells) > 3 and cells[1].strip() == '' and cells[2].strip() == 'Semovita')
    lines[i] = lines[i].replace('Other grains and flour', 'Wheat Flour'); changed = 1
elif mutation == 'other_cell':
    set_cell(code_row(16), 6, 'Maize flourX'); changed = 1
elif mutation == 'drop_row':
    i = row_index(lambda cells: len(cells) > 3 and cells[1].strip() == '' and cells[2].strip() == 'Semovita')
    del lines[i]; changed = 1
elif mutation != 'none':
    raise SystemExit(f'unknown mutation {mutation}')
org.write_text('\n'.join(lines))
print(f'mutation {mutation}: changed={changed}', flush=True)
