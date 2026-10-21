"""Run the existing release corpus validator with explicit paths and workers.

Reuses the October 14 driver, its sanctioned Country loading, and its existing
fingerprint/metadata measurements; source and cache roots must be explicit.
Detailed results stay in the chosen private output directory.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import importlib.util
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--data-dir', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--snapshot-root', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--country')
    args = parser.parse_args()
    repo, data, out = (p.resolve() for p in (args.repo, args.data_dir, args.out))
    production = (Path.home() / '.local/share/lsms_library').resolve()
    assert data != production and not data.is_relative_to(production)
    assert args.workers > 0
    assert Path.cwd().resolve() == repo
    assert Path(os.environ['LSMS_DATA_DIR']).resolve() == data
    assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == repo / 'lsms_library/countries'
    out.mkdir(parents=True, exist_ok=True)

    prior = repo / 'slurm_logs/release_landing_2026-10-14/corpus/run_corpus.py'
    spec = importlib.util.spec_from_file_location('prior_corpus_driver', prior)
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    driver.TREE, driver.DATA, driver.OUT = repo, data, out
    driver.SHARED = args.snapshot_root.resolve()
    driver.SHA = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    if args.country:
        return driver.worker(args.country)

    inventory = driver.inventory()
    countries = [r['country'] for r in inventory if 'tables' in r]
    script = Path(__file__).resolve()

    def one(country):
        cmd = [sys.executable, str(script), '--repo', str(repo), '--data-dir', str(data),
               '--out', str(out), '--snapshot-root', str(args.snapshot_root),
               '--country', country]
        with (out / (country + '.log')).open('w', encoding='utf-8') as stream:
            result = subprocess.run(cmd, cwd=repo, stdout=stream, stderr=subprocess.STDOUT)
        return {'country': country, 'exit_code': result.returncode}

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(one, countries))
    driver.dump(out / 'completion.json', {
        'source_sha': driver.SHA, 'processes': results,
        'inventory_errors': [r for r in inventory if 'inventory_error' in r],
    })
    # The existing corpus includes known source/wiring errors. Summarize every
    # result for comparison; an error remains nonzero, never a blanket pass.
    return int(any(r['exit_code'] for r in results)
               or any('inventory_error' in r for r in inventory))


if __name__ == '__main__':
    raise SystemExit(main())
