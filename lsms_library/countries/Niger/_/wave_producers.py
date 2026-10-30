"""Produce the declared inputs of Niger's five native country compilers.

Country/Wave own registration, declarations and folder mapping (ledger
1287-niger-producer-routing §§2–5). Keep native scripts on their existing
Make rules; use the sanctioned wave loader only for YAML-only declarations.
This adapter does not finalize frames or decide country-cache freshness.
Adapted from rescued dispatch a52a7ab8071a4d698251c6c790ded919 source WIP;
that failed attempt supplies no accepted runtime or test result.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import os
from pathlib import Path

import pandas as pd

from lsms_library import Country
from lsms_library import local_tools as tools
from lsms_library.country import (
    _build_wave_script_target, _wave_build_drops, _wave_grain_audit,
    wave_datetime_audit,
)


TABLES = ('crop_production', 'plot_inputs', 'livestock', 'plot_labor',
          'people_last7days')


def discover(table):
    """Return contributors and existing metadata dependencies, before any build."""
    if table not in TABLES:
        raise ValueError(f'Unsupported Niger country producer: {table}')
    country = Country('Niger')
    contributors, dependencies = [], []
    for year in country.waves:
        wave = country[year]
        directory = wave.file_path / '_'
        # Undeclared waves' metadata still matters: adding/removing a YAML
        # declaration must retrigger discovery. Never require an absent file.
        for name in ('data_info.yml', 'mapping.py', f'{wave.wave_folder}.py'):
            path = directory / name
            if path.is_file():
                dependencies.append(path)
        if table not in wave.data_scheme:
            continue
        contributors.append(wave)
        script = directory / f'{table}.py'
        if script.is_file():
            dependencies.append(script)
        else:
            # A null entry still declares a table, but must not fall through
            # to grab_data's script-cache reader (ledger integration §§2,4).
            metadata = wave.resources.get(table)
            identity = (f'Niger {table}: logical wave {wave.year}, '
                        f'physical folder {wave.wave_folder}')
            if isinstance(metadata, dict) and any(
                    str(metadata.get(key, '')).lower() == 'make'
                    for key in ('materialize', 'backend')):
                raise NotImplementedError(
                    f'{identity} declares a scriptless Make builder; this '
                    'producer cannot dispatch that builder yet. Supply a '
                    'reviewed native producer or implement explicit Make '
                    'builder routing; refusing cached-script fallback.')
            if not metadata:
                raise FileNotFoundError(
                    f'{identity} declares the table but has no native script '
                    'or executable YAML extraction metadata. Restore a '
                    'reviewed producer, or retire the declaration explicitly; '
                    'refusing cached-script fallback.')
            if not isinstance(metadata, dict):
                raise TypeError(f'{identity} has invalid YAML extraction metadata')
    return country, contributors, list(dict.fromkeys(dependencies))


@contextmanager
def _uncached_yaml_loads():
    """Bypass logical YAML caches while assembling one physical output.

    The CLI is a serial dedicated process. This process-global environment
    scope is not safe for concurrent producer threads or concurrent unrelated
    cache users: callers must serialize the entire producer invocation.
    It does not bypass native-script cache reads (ledger integration §4).
    """
    present = 'LSMS_NO_CACHE' in os.environ
    previous = os.environ.get('LSMS_NO_CACHE')
    os.environ['LSMS_NO_CACHE'] = '1'
    try:
        yield
    finally:
        if present:
            os.environ['LSMS_NO_CACHE'] = previous
        else:
            os.environ.pop('LSMS_NO_CACHE', None)


def produce(table):
    """Build every declared input in order; propagate errors before compilation."""
    country, waves, _ = discover(table)
    # Native scripts can emit several logical years from one physical folder.
    # YAML grab_data instead filters to wave.year (country.py); retain every
    # logical contribution, then write the compiler's physical input once.
    # This is routing/concatenation only (ledger §§2,4), never a reduction.
    folders = {}
    for wave in waves:
        folders.setdefault(wave.file_path.resolve(), []).append(wave)
    for members in folders.values():
        wave = members[0]
        script = wave.file_path / '_' / f'{table}.py'
        if script.is_file():
            # Preserve the former unconditional native-script execution and
            # its Make prerequisites, using the framework's checked runner.
            target = Path(wave.wave_folder) / '_' / f'{table}.parquet'
            _build_wave_script_target(country.file_path / '_', target, force=True)
        else:
            # Physical output may equal one member's logical cache path.
            # Never read our previous composite as a logical contribution.
            with _uncached_yaml_loads():
                frames = [member.grab_data(table) for member in members]
                frame = frames[0] if len(frames) == 1 else pd.concat(frames)
            # grab_data need not write (empty/LSMS_NO_CACHE), and its YAML
            # cache path uses the logical year. Compilers read physical folders.
            # Serialize that same pre-finalize frame, carrying existing audits.
            destination = (tools.data_root('Niger') / wave.wave_folder / '_'
                           / f'{table}.parquet')
            destination.parent.mkdir(parents=True, exist_ok=True)
            tools.to_parquet(
                frame, destination, absolute_path=True,
                grain_audit=_wave_grain_audit('Niger', table, wave.wave_folder),
                datetime_audit=wave_datetime_audit('Niger', table, wave.wave_folder),
                build_drops=[record for member in members for record in
                             (_wave_build_drops('Niger', table, member.year) or [])]
                            or None,
            )


def make_path(path):
    """Escape a prerequisite filename for GNU Make, without shell evaluation."""
    value = str(path)
    if '\n' in value or '\r' in value:
        raise ValueError('Niger producer dependency contains a newline')
    return (value.replace('\\', '\\\\').replace('$', '$$')
            .replace('#', '\\#').replace(' ', '\\ ').replace(':', '\\:'))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dependencies', action='store_true')
    parser.add_argument('table', choices=TABLES)
    args = parser.parse_args(argv)
    if args.dependencies:
        _, _, dependencies = discover(args.table)
        # Do not emit a partially discovered prerequisite list on failure.
        print(' '.join(make_path(path) for path in dependencies))
    else:
        produce(args.table)


if __name__ == '__main__':
    main()
