#!/usr/bin/env python3
"""
Test YAML tags where tag name becomes the output column name.
Syntax: !Rural reside means "create Rural column from reside with Rural semantics"
"""

import yaml
import tempfile
import os


class TypedColumn:
    """Base class for typed column specifications."""
    def __init__(self, source_column: str, output_name: str = None):
        self.source_column = source_column
        self.output_name = output_name  # Derived from tag name

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.source_column}' -> '{self.output_name}')"


class RuralColumn(TypedColumn):
    """Rural=1, Urban=0 encoding (standard)"""
    pass


class UrbanColumn(TypedColumn):
    """Urban=1, Rural=0 encoding (inverted) - for columns named 'urban'"""
    pass


class SexColumn(TypedColumn):
    """Sex/Gender with standard Male/Female encoding"""
    pass


class DateTimeColumn(TypedColumn):
    """Datetime parsing"""
    pass


class IntColumn(TypedColumn):
    """Integer type"""
    pass


# Generic constructor factory
def make_constructor(column_class, output_name):
    """Factory to create constructors that know their output column name."""
    def constructor(loader, node):
        source = loader.construct_scalar(node)
        return column_class(source, output_name=output_name)
    return constructor


# Register tags
yaml.SafeLoader.add_constructor('!Rural', make_constructor(RuralColumn, 'Rural'))
yaml.SafeLoader.add_constructor('!Urban', make_constructor(UrbanColumn, 'Rural'))  # Output still 'Rural'
yaml.SafeLoader.add_constructor('!Sex', make_constructor(SexColumn, 'Sex'))
yaml.SafeLoader.add_constructor('!DateTime', make_constructor(DateTimeColumn, 'date'))
yaml.SafeLoader.add_constructor('!Age', make_constructor(IntColumn, 'Age'))


# Test YAML - Version 1: Using block mapping with tags
example_yaml_v1 = """
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
    myvars:
        Rural: !Rural reside
        Region: region
        District: district

household_roster:
    file: HH/gsec2.dta
    idxvars:
        i: hhid
        pid: pid
    myvars:
        Sex: !Sex h2q3
        Age: !Age h2q8
        Relation: h2q4
"""

# Test YAML - Version 2: Even more compact (if we can make it work)
# This would require special handling but is the cleanest
example_yaml_v2 = """
cluster_features:
    myvars:
        !Rural reside
        Region: region
"""

print("="*60)
print("Testing Version 1: Key: !Tag value")
print("="*60)

with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(example_yaml_v1)
    temp_path = f.name

try:
    with open(temp_path) as _f:
        result = yaml.safe_load(_f)
    print("✓ Successfully loaded YAML\n")

    print("cluster_features.myvars:")
    for k, v in result['cluster_features']['myvars'].items():
        print(f"  {k}: {v}")

    print("\nhousehold_roster.myvars:")
    for k, v in result['household_roster']['myvars'].items():
        print(f"  {k}: {v}")

    print("\n" + "="*60)
    print("Type verification:")
    print(f"  Rural is RuralColumn: {isinstance(result['cluster_features']['myvars']['Rural'], RuralColumn)}")
    print(f"  Sex is SexColumn: {isinstance(result['household_roster']['myvars']['Sex'], SexColumn)}")
    print(f"  Age is IntColumn: {isinstance(result['household_roster']['myvars']['Age'], IntColumn)}")
    print(f"  Region is str: {isinstance(result['cluster_features']['myvars']['Region'], str)}")

finally:
    os.unlink(temp_path)


print("\n" + "="*60)
print("Testing Version 2: !Tag value (no key)")
print("="*60)
print("Attempting to parse YAML without explicit key...")

with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(example_yaml_v2)
    temp_path = f.name

try:
    with open(temp_path) as _f:
        result = yaml.safe_load(_f)
    print("✓ Loaded, but structure may be unexpected:")
    print(f"myvars content: {result['cluster_features']['myvars']}")
    print("\nNote: YAML doesn't support '!Tag value' as a mapping entry.")
    print("We need to use 'key: !Tag value' syntax for dict entries.")
except Exception as e:
    print(f"✗ Failed as expected: {e}")
    print("\nYAML requires explicit keys in mappings.")
finally:
    os.unlink(temp_path)


print("\n" + "="*60)
print("SYNTAX COMPARISON")
print("="*60)
print("""
Option A (proposed syntax - requires key):
    myvars:
        Rural: !Rural reside
        Sex: !Sex h2q3

Option B (even cleaner - but may require list instead of dict):
    myvars:
        - !Rural reside     # Returns {'Rural': RuralColumn('reside')}
        - !Sex h2q3         # Returns {'Sex': SexColumn('h2q3')}
        - Region: region    # Regular dict entry

Option C (original proposal):
    myvars:
        Rural: !rural reside
        Sex: !sex h2q3

Comparison:
    Option A: Tag name matches output column name (eliminates redundancy)
    Option B: Most compact, but myvars becomes a list of dicts
    Option C: Tag name is lowercase type, separate from column name

Recommendation: Option A is cleanest while maintaining dict structure
""")
