#!/usr/bin/env python3
"""Simple test of YAML tag loading without pandas dependency."""

import yaml
import tempfile
import os


class TypedColumn:
    """Base class for typed column specifications."""
    def __init__(self, source_column: str, **kwargs):
        self.source_column = source_column
        self.params = kwargs

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.source_column}', {self.params})"


class DatetimeColumn(TypedColumn):
    pass


class RuralColumn(TypedColumn):
    pass


class UrbanColumn(TypedColumn):
    pass


class SexColumn(TypedColumn):
    pass


class IntColumn(TypedColumn):
    pass


# YAML Constructor functions
def datetime_constructor(loader, node):
    source = loader.construct_scalar(node)
    return DatetimeColumn(source)


def rural_constructor(loader, node):
    source = loader.construct_scalar(node)
    return RuralColumn(source)


def urban_constructor(loader, node):
    source = loader.construct_scalar(node)
    return UrbanColumn(source)


def sex_constructor(loader, node):
    source = loader.construct_scalar(node)
    return SexColumn(source)


def int_constructor(loader, node):
    source = loader.construct_scalar(node)
    return IntColumn(source)


# Register tags
yaml.SafeLoader.add_constructor('!datetime', datetime_constructor)
yaml.SafeLoader.add_constructor('!rural', rural_constructor)
yaml.SafeLoader.add_constructor('!urban', urban_constructor)
yaml.SafeLoader.add_constructor('!sex', sex_constructor)
yaml.SafeLoader.add_constructor('!int', int_constructor)


# Test YAML
example_yaml = """
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
    myvars:
        Rural: !rural reside
        Region: region

household_roster:
    file: HH/gsec2.dta
    idxvars:
        i: hhid
        pid: pid
    myvars:
        Sex: !sex h2q3
        Age: !int h2q8

interview_date:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
    myvars:
        date: !datetime interview_start
"""

# Test loading
with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(example_yaml)
    temp_path = f.name

try:
    with open(temp_path) as fh:
        result = yaml.safe_load(fh)
    print("✓ Successfully loaded YAML with tags\n")
    print("cluster_features.myvars:")
    for k, v in result['cluster_features']['myvars'].items():
        print(f"  {k}: {v}")
    print("\nhousehold_roster.myvars:")
    for k, v in result['household_roster']['myvars'].items():
        print(f"  {k}: {v}")
    print("\ninterview_date.myvars:")
    for k, v in result['interview_date']['myvars'].items():
        print(f"  {k}: {v}")

    # Verify types
    print("\n" + "="*50)
    print("Type verification:")
    print(f"  Rural is RuralColumn: {isinstance(result['cluster_features']['myvars']['Rural'], RuralColumn)}")
    print(f"  Sex is SexColumn: {isinstance(result['household_roster']['myvars']['Sex'], SexColumn)}")
    print(f"  date is DatetimeColumn: {isinstance(result['interview_date']['myvars']['date'], DatetimeColumn)}")
    print(f"  Age is IntColumn: {isinstance(result['household_roster']['myvars']['Age'], IntColumn)}")

finally:
    os.unlink(temp_path)
