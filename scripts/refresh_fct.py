"""Explicit FDC snapshot refresh. Nutrition builds never call this implicitly.

Use the country's existing curation, nutrient parser and per-kg harmonizer.
No Country/API table, Make recipe or survey input is constructed here.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import datetime
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
from urllib.parse import urlsplit
import warnings

ROOT = Path(__file__).resolve().parents[1]


def _module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _country_inputs(country, countries):
    from lsms_library import local_tools
    if Path(local_tools.__file__).resolve() != ROOT / 'lsms_library/local_tools.py':
        raise RuntimeError('refresh imported another checkout')
    df_from_orgfile = local_tools.df_from_orgfile
    common = countries / country / '_'
    if country == 'Ethiopia':
        curation = common / 'categorical_mapping.org'
        labels_path = common / 'nutrient_labels.org'
        foods = df_from_orgfile(curation, name='harmonize_food')
        foods = foods.astype({'FTC Code': 'Int64', 'FDC ID': 'Int64'})
        addition = foods[['Preferred Label', 'FDC ID']]
        helper = _module('_refresh_ethiopia_fct', common / 'fct_tools.py')
        labels = df_from_orgfile(labels_path)
        harmonize = lambda raw: helper.harmonize_nutrient(raw, labels)
        sources = [curation, labels_path, common / 'fct_tools.py']
        selected = addition.dropna()
    else:
        curation = common / 'nutrition.org'
        addition = df_from_orgfile(curation, name='fct_addition', encoding='ISO-8859-1')
        helper = _module('_refresh_uganda_fct', common / 'fct_addition.py')
        harmonize = helper.harmonize_nutrient
        sources = [curation, common / 'fct_addition.py']
        selected = addition[['Preferred Label', 'FDC ID']]
    if selected['Preferred Label'].duplicated().any():
        raise ValueError('duplicate curated food labels require source review')
    ids = []
    for value in selected['FDC ID']:
        number = int(value)
        if number != value or number <= 0:
            raise ValueError('FDC IDs must be positive integers')
        ids.append(number)
    return addition, helper.nutrient_df, harmonize, selected['Preferred Label'].tolist(), ids, sources


@contextmanager
def _bounded_requests(ids, key, evidence, receipt):
    # Preserve the original country helper/parser, adding only a finite
    # transport timeout, exact curated endpoint scope and public response custody.
    import fooddatacentral.fooddatacentral as provider
    import fooddatacentral as fdc
    receipt['fdc_parser_sha256'] = _sha(provider.__file__)
    receipt['endpoint_template'] = 'https://api.nal.usda.gov/fdc/v1/food/{fdc_id}'
    remaining = Counter(ids)
    memo = {}
    receipt['owned_response_memo_hits'] = []
    original = provider.requests.get
    cached_parser = fdc.nutrients
    loggers = [logging.getLogger(n) for n in ('requests', 'urllib3', 'urllib3.connectionpool')]
    logger_states = [logger.disabled for logger in loggers]

    def get(url, *, params):
        parsed = urlsplit(url)
        prefix = '/fdc/v1/food/'
        if (parsed.scheme != 'https' or parsed.netloc != 'api.nal.usda.gov'
                or not parsed.path.startswith(prefix) or parsed.query or parsed.fragment):
            raise ValueError('unexpected FDC endpoint')
        number = int(parsed.path[len(prefix):])
        if remaining[number] <= 0 or tuple(params) != (('api_key', key),):
            raise ValueError('request outside curated acquisition scope')
        remaining[number] -= 1
        if number in memo:
            receipt['owned_response_memo_hits'].append(number)
            return memo[number]
        row = {'fdc_id': number, 'ordinal': len(receipt['requests']) + 1, 'state': 'started'}
        receipt['requests'].append(row)
        # An interrupted HTTP call is consumed/ambiguous, never invisible.
        (evidence / 'RECEIPT.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
        try:
            response = original(url, params=params, timeout=(10, 30), allow_redirects=False)
            memo[number] = response
            row['status_code'] = response.status_code
            row['state'] = 'returned'
            if response.status_code == 200:
                body = response.content
                if key.encode() in body:
                    raise ValueError('response unexpectedly contains a credential')
                raw = evidence / ('response-%03d-%d.json' % (row['ordinal'], number))
                raw.write_bytes(body)
                row.update(response_file=raw.name, response_bytes=len(body), response_sha256=_sha(raw))
                # Retain the actual response before checking identity. A
                # successful endpoint alone does not identify its payload.
                returned_id = response.json().get('fdcId')
                row['response_identity_matches'] = type(returned_id) is int and returned_id == number
                if not row['response_identity_matches']:
                    raise ValueError('FDC response identity differs from requested food')
            return response
        except Exception as error:
            row['state'] = 'failed'
            row['error_type'] = type(error).__name__
            raise
        finally:
            (evidence / 'RECEIPT.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')

    try:
        provider.requests.get = get
        # An explicit refresh must obtain new responses, not reuse this
        # dependency's process-wide lru_cache. The parser body is unchanged;
        # restore the wrapper and its existing cache without clearing it.
        fdc.nutrients = getattr(cached_parser, '__wrapped__', cached_parser)
        receipt['parser_cache_policy'] = 'scoped unchanged __wrapped__ parser; original cache restored untouched'
        for logger in loggers:
            logger.disabled = True
        yield
        if any(remaining.values()):
            raise ValueError('not all curated FDC requests completed')
    finally:
        provider.requests.get = original
        fdc.nutrients = cached_parser
        for logger, state in zip(loggers, logger_states):
            logger.disabled = state


def refresh(country, output=None, evidence_dir=None, replace=False):
    if country not in ('Ethiopia', 'Uganda'):
        raise ValueError('unsupported refresh country')
    cfg = _module('_refresh_lsms_config', ROOT / 'lsms_library/config.py')
    countries = Path(cfg.get('countries_dir', env_var='LSMS_COUNTRIES_ROOT')
                     or ROOT / 'lsms_library/countries')
    output = Path(output) if output else countries / country / '_' / 'fct_usda.csv'
    if output.exists() and not replace:
        raise FileExistsError('snapshot exists; use --replace only for an intentional refresh')
    if output.is_symlink():
        raise ValueError('snapshot path must not be a symlink')
    addition, nutrient, harmonize, labels, ids, sources = _country_inputs(country, countries)
    key = cfg.get('fdc_api_key')
    if not isinstance(key, str) or not key.strip():
        raise ValueError('set fdc_api_key in user config or FDC_API_KEY for this explicit refresh')
    key = key.strip()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    evidence = Path(evidence_dir) if evidence_dir else Path(os.environ.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))) / 'lsms_library/fdc-refresh' / (country + '-' + stamp)
    if evidence.exists() or evidence.is_symlink():
        raise FileExistsError('fresh evidence directory required')
    evidence.mkdir(mode=0o700, parents=True)
    receipt = dict(country=country, started_utc=stamp, status='running', units='per kg, original country harmonizer',
                   requests=[], requested_fdc_ids=ids, source_sha256={str(p): _sha(p) for p in sources},
                   requested_food_labels=labels,
                   script_sha256=_sha(__file__), key_lookup='config.get(fdc_api_key); value never retained', automatic_retries=0)
    temporary = None
    try:
        with warnings.catch_warnings(record=True) as observed:
            with _bounded_requests(ids, key, evidence, receipt):
                raw = nutrient(addition, key)
            receipt['warning_types'] = [type(w.message).__name__ for w in observed]
        if raw.columns.tolist() != labels or not raw.columns.is_unique:
            raise ValueError('incomplete FDC food response; final snapshot not written')
        result = harmonize(raw)
        if result.index.tolist() != labels or not result.index.is_unique:
            raise ValueError('harmonized snapshot lost a curated food')
        receipt['source_closure'] = (all(_sha(path) == h for path, h in receipt['source_sha256'].items())
                                     and _sha(__file__) == receipt['script_sha256'])
        if not receipt['source_closure']:
            raise ValueError('refresh source changed during acquisition')
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode='w', dir=output.parent, prefix='.fct-refresh-', suffix='.csv', delete=False) as file:
            temporary = Path(file.name)
            result.to_csv(file)
        # Preserve exact decimal round-trip for the newly introduced Ethiopia
        # snapshot. Uganda's existing consumer parsing remains unchanged.
        import pandas as pd
        import numpy as np
        check = pd.read_csv(temporary, index_col=0, float_precision='round_trip')
        receipt['csv_axis_metadata'] = dict(original_index_name=result.index.name, original_column_name=result.columns.name,
                                             csv_index_name=check.index.name, csv_column_name=check.columns.name)
        # CSV does not store the column-axis name. Record that format loss;
        # do not coerce dtypes, order, masks or nutrient values to compare.
        pd.testing.assert_frame_equal(check, result.rename_axis(index=check.index.name, columns=check.columns.name), check_exact=True)
        np.testing.assert_array_equal(check.isna().to_numpy(), result.isna().to_numpy(), strict=True)
        for column in result:
            left, right = check[column], result[column]
            pd.testing.assert_extension_array_equal(left.array, right.array, check_exact=True, rtol=0, atol=0, check_dtype=True)
            mask = right.notna().to_numpy()
            np.testing.assert_array_equal(left[mask].to_numpy(), right[mask].to_numpy(), strict=True)
        receipt.update(status='complete', rows=len(result), columns=len(result.columns), csv_sha256=_sha(temporary), csv_bytes=temporary.stat().st_size)
        os.replace(temporary, output)
        temporary = None
        receipt['output'] = str(output)
        return receipt
    except Exception as error:
        receipt.update(status='failed', error_type=type(error).__name__)
        raise
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        (evidence / 'RECEIPT.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--country', choices=['Ethiopia', 'Uganda'], required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--evidence-dir', type=Path)
    parser.add_argument('--replace', action='store_true')
    args = parser.parse_args(argv)
    try:
        # Config warnings may contain malformed user YAML: never echo them.
        with warnings.catch_warnings(record=True):
            receipt = refresh(args.country, args.output, args.evidence_dir, args.replace)
    except Exception as error:
        print('FCT refresh failed: ' + type(error).__name__ + '; no implicit retry', file=sys.stderr)
        return 1
    print(json.dumps({k: receipt[k] for k in ['country', 'rows', 'columns', 'csv_sha256', 'csv_bytes']}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
