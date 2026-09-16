"""Publish a reviewed Ghana partition while preserving nutritional crosswalks.

No microdata or estimation. Reuses the explicit partition constructor and
Org column writer; see .coder/ledger/ghanalss-aggregate-validation.md.
"""

import argparse
from pathlib import Path
import tempfile

import pandas as pd
from pandas.testing import assert_frame_equal
import yaml

from lsms_library.local_tools import df_from_orgfile
from lsms_library.util.orgtbl import _split_row, add_or_update_column

from .ghana_inventory import draft_partition


def publish(orgpath, specification, delivered, output):
    original = df_from_orgfile(orgpath, name='harmonize_food')
    existing = set(original['Preferred Label'])
    membership, _, missing = draft_partition(existing | set(delivered), specification)
    if missing:
        raise ValueError(f'Selected members absent from the publication universe: {missing}')
    additions = sorted(set(delivered)-existing)
    lines = orgpath.read_text().splitlines()
    named = next(i for i, line in enumerate(lines)
                 if line.lower().strip() == '#+name: harmonize_food')
    start = next(i for i in range(named+1, len(lines)) if lines[i].lstrip().startswith('|'))
    header = _split_row(lines[start])
    end = start
    while end < len(lines) and lines[end].lstrip().startswith('|'):
        end += 1
    rows = []
    for label in additions:
        cells = ['']*len(header)
        cells[header.index('Preferred Label')] = label
        rows.append('| ' + ' | '.join(cells) + ' |')
    proposed = '\n'.join(lines[:end]+rows+lines[end:])+'\n'
    # Validate the complete edit before replacing the destination. New rows
    # have no wave or FCT values: documentary matches are not nutrient codes.
    with tempfile.TemporaryDirectory(prefix='ghana-aggregate-') as directory:
        scratch = Path(directory)/'mapping.org'
        scratch.write_text(proposed)
        nrows, unmapped = add_or_update_column(
            scratch, 'harmonize_food', 'Aggregate Label', membership.to_dict(),
            source_column='Preferred Label', insert_after='Preferred Label')
        if unmapped:
            raise ValueError(f'{unmapped} country rows have no Aggregate assignment.')
        result = df_from_orgfile(scratch, name='harmonize_food')
        preserved = original.columns.drop('Aggregate Label', errors='ignore')
        assert_frame_equal(result.loc[:len(original)-1, preserved], original[preserved])
        blank = preserved.drop('Preferred Label')
        if result.loc[len(original):, blank].fillna('').ne('').any().any():
            raise ValueError('Added labels must not extend wave/FCT crosswalks.')
        orgpath.write_text(scratch.read_text())
    output.write_text(yaml.safe_dump(membership.to_dict(), sort_keys=True))
    return dict(rows=nrows, appended_rows=len(additions), preferred=len(membership),
                aggregate=membership.nunique(), delivered=len(set(delivered)),
                delivered_aggregates=membership.reindex(delivered).nunique())


def main():
    root = Path(__file__).resolve().parents[2]
    reports = Path(__file__).parent/'ghanalss'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--org-file', type=Path,
                        default=root/'lsms_library/countries/GhanaLSS/_/categorical_mapping.org')
    parser.add_argument('--specification', type=Path, default=reports/'selected_groups.yml')
    parser.add_argument('--inventory', type=Path, default=reports/'item_review.csv')
    parser.add_argument('--output', type=Path, default=reports/'selected_partition.yml')
    args = parser.parse_args()
    spec = yaml.safe_load(args.specification.read_text())
    delivered = pd.read_csv(args.inventory)['Preferred Label']
    print(publish(args.org_file, spec, delivered, args.output))


if __name__ == '__main__':
    main()
