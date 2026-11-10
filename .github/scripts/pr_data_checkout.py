"""Exact PR merge checkout guard; no data, tokens or platform API calls."""
from __future__ import annotations

import json
import os
import re
import subprocess


def verify_checkout(actual, parents, merge, base, head):
    """Refuse moving refs, wrong source parents or non-synthetic commits."""
    identities = [actual, merge, base, head, *parents]
    if any(not isinstance(value, str) or not re.fullmatch('[0-9a-f]{40}', value)
           for value in identities):
        raise ValueError('Expected exact commit SHAs')
    if actual != merge or parents != [base, head]:
        raise ValueError('Checkout is not the event synthetic merge with exact base/head parents')
    return {'merge': actual, 'base': base, 'head': head}


def main():
    actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    parents = subprocess.check_output(['git', 'show', '-s', '--format=%P', 'HEAD'], text=True).split()
    receipt = verify_checkout(actual, parents, os.environ['EXPECTED_MERGE'],
                              os.environ['EXPECTED_BASE'], os.environ['EXPECTED_HEAD'])
    print(json.dumps({'tested_source': receipt}, sort_keys=True))


if __name__ == '__main__':
    main()
