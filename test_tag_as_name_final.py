#!/usr/bin/env python3
"""Test the tag-as-name syntax without pandas dependency."""

import yaml
import tempfile
import os


class TypedColumn:
    """Base class for typed column specifications."""
    output_name = None

    def __init__(self, source_column: str):
        self.source_column = source_column

    def __repr__(self):
        output = self.output_name or self.__class__.__name__.replace('Column', '')
        return f"{self.__class__.__name__}('{self.source_column}' -> '{output}')"


# Define column types
class RuralColumn(TypedColumn):
    output_name = 'Rural'

class UrbanColumn(TypedColumn):
    output_name = 'Rural'  # Inverted, but output is still Rural

class SexColumn(TypedColumn):
    output_name = 'Sex'

class AgeColumn(TypedColumn):
    output_name = 'Age'

class DateTimeColumn(TypedColumn):
    output_name = 'date'

class RegionColumn(TypedColumn):
    output_name = 'Region'

class DistrictColumn(TypedColumn):
    output_name = 'District'

class HouseholdIdColumn(TypedColumn):
    output_name = 'i'

class StringColumn(TypedColumn):
    pass


# Constructor factory
def make_constructor(column_class):
    def constructor(loader, node):
        source = loader.construct_scalar(node)
        return column_class(source)
    return constructor


# Register tags
TYPE_REGISTRY = {
    'Rural': RuralColumn,
    'Urban': UrbanColumn,
    'Sex': SexColumn,
    'Age': AgeColumn,
    'DateTime': DateTimeColumn,
    'Region': RegionColumn,
    'District': DistrictColumn,
    'HouseholdId': HouseholdIdColumn,
    'String': StringColumn,
}

for tag_name, column_class in TYPE_REGISTRY.items():
    yaml.SafeLoader.add_constructor(f'!{tag_name}', make_constructor(column_class))


# Test YAML
example = """
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: !HouseholdId hhid
        v: !String s1aq04a
    myvars:
        Rural: !Rural reside
        Region: !Region region
        District: !District district

household_roster:
    file: HH/gsec2.dta
    idxvars:
        i: !HouseholdId hhid
        pid: pid
    myvars:
        Sex: !Sex h2q3
        Age: !Age h2q8
        Relation: h2q4

interview_date:
    file: HH/gsec1.dta
    idxvars:
        i: !HouseholdId hhid
    myvars:
        date: !DateTime interview_start
"""

# Test loading
with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(example)
    temp_path = f.name

try:
    result = yaml.safe_load(open(temp_path))

    print("="*70)
    print("✓ Successfully loaded YAML with tag-as-name syntax")
    print("="*70)

    print("\ncluster_features:")
    print("  idxvars:")
    for k, v in result['cluster_features']['idxvars'].items():
        print(f"    {k}: {v}")
    print("  myvars:")
    for k, v in result['cluster_features']['myvars'].items():
        print(f"    {k}: {v}")

    print("\nhousehold_roster:")
    print("  myvars:")
    for k, v in result['household_roster']['myvars'].items():
        print(f"    {k}: {v}")

    print("\ninterview_date:")
    print("  myvars:")
    for k, v in result['interview_date']['myvars'].items():
        print(f"    {k}: {v}")

    print("\n" + "="*70)
    print("Type Verification:")
    print("="*70)
    cluster_myvars = result['cluster_features']['myvars']
    print(f"  Rural is RuralColumn: {isinstance(cluster_myvars['Rural'], RuralColumn)}")
    print(f"  Region is RegionColumn: {isinstance(cluster_myvars['Region'], RegionColumn)}")

    household_myvars = result['household_roster']['myvars']
    print(f"  Sex is SexColumn: {isinstance(household_myvars['Sex'], SexColumn)}")
    print(f"  Age is AgeColumn: {isinstance(household_myvars['Age'], AgeColumn)}")
    print(f"  Relation is str (untyped): {isinstance(household_myvars['Relation'], str)}")

    interview_myvars = result['interview_date']['myvars']
    print(f"  date is DateTimeColumn: {isinstance(interview_myvars['date'], DateTimeColumn)}")

    print("\n" + "="*70)
    print("SYNTAX DEMONSTRATION")
    print("="*70)
    print("""
This YAML uses tag-as-name syntax where:
  - Tag name (e.g., !Rural) = Semantic type = Output column name
  - Value (e.g., reside) = Source column in data file

Example:
  Rural: !Rural reside

Reads as: "Rural column, of Rural-type, sourced from 'reside'"

Benefits:
  ✓ No redundancy between tag and output name
  ✓ Self-documenting (Rural means rural/urban indicator)
  ✓ Mixes with untyped columns naturally
  ✓ Prevents encoding bugs (e.g., !Urban explicitly inverted)

Special case - Inverted encoding:
  Rural: !Urban urban

This says: "Source column 'urban' has inverted encoding (Urban=1),
            output as 'Rural' with standard encoding (Rural=1)"
""")

finally:
    os.unlink(temp_path)
