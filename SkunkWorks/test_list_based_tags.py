#!/usr/bin/env python3
"""
Test list-based YAML tags for maximum conciseness.
Each tag returns a dict with output_name: TypedColumn mapping.
"""

import yaml
import tempfile
import os


class TypedColumn:
    def __init__(self, source_column: str):
        self.source_column = source_column
    def __repr__(self):
        return f"{self.__class__.__name__}('{self.source_column}')"


class RuralColumn(TypedColumn):
    output_name = 'Rural'

class SexColumn(TypedColumn):
    output_name = 'Sex'

class AgeColumn(TypedColumn):
    output_name = 'Age'

class DateTimeColumn(TypedColumn):
    output_name = 'date'


# Constructors that return dicts
def rural_constructor(loader, node):
    source = loader.construct_scalar(node)
    return {RuralColumn.output_name: RuralColumn(source)}

def sex_constructor(loader, node):
    source = loader.construct_scalar(node)
    return {SexColumn.output_name: SexColumn(source)}

def age_constructor(loader, node):
    source = loader.construct_scalar(node)
    return {AgeColumn.output_name: AgeColumn(source)}

def datetime_constructor(loader, node):
    source = loader.construct_scalar(node)
    return {DateTimeColumn.output_name: DateTimeColumn(source)}


# Register tags
yaml.SafeLoader.add_constructor('!Rural', rural_constructor)
yaml.SafeLoader.add_constructor('!Sex', sex_constructor)
yaml.SafeLoader.add_constructor('!Age', age_constructor)
yaml.SafeLoader.add_constructor('!DateTime', datetime_constructor)


# Test: List-based syntax
example_yaml = """
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
    myvars:
        - !Rural reside
        - Region: region
        - District: district

household_roster:
    file: HH/gsec2.dta
    idxvars:
        i: hhid
        pid: pid
    myvars:
        - !Sex h2q3
        - !Age h2q8
        - Relation: h2q4

interview_date:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
    myvars:
        - !DateTime interview_start
"""

print("="*70)
print("Testing List-Based Tags")
print("="*70)
print("\nYAML Syntax:")
print(example_yaml)

with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(example_yaml)
    temp_path = f.name

try:
    with open(temp_path) as f:
        result = yaml.safe_load(f)

    print("\n" + "="*70)
    print("Loaded Structure:")
    print("="*70)

    # myvars is now a list of dicts, need to merge
    cluster_myvars_list = result['cluster_features']['myvars']
    print(f"\ncluster_features.myvars (raw list): {cluster_myvars_list}")

    # Merge list of dicts into single dict
    cluster_myvars = {}
    for item in cluster_myvars_list:
        if isinstance(item, dict):
            cluster_myvars.update(item)
        else:
            print(f"Warning: Unexpected item type: {type(item)}")

    print(f"\ncluster_features.myvars (merged):")
    for k, v in cluster_myvars.items():
        print(f"  {k}: {v}")

    household_myvars_list = result['household_roster']['myvars']
    household_myvars = {}
    for item in household_myvars_list:
        if isinstance(item, dict):
            household_myvars.update(item)

    print(f"\nhousehold_roster.myvars:")
    for k, v in household_myvars.items():
        print(f"  {k}: {v}")

    interview_myvars_list = result['interview_date']['myvars']
    interview_myvars = {}
    for item in interview_myvars_list:
        if isinstance(item, dict):
            interview_myvars.update(item)

    print(f"\ninterview_date.myvars:")
    for k, v in interview_myvars.items():
        print(f"  {k}: {v}")

    print("\n" + "="*70)
    print("Type Verification:")
    print("="*70)
    print(f"  Rural is RuralColumn: {isinstance(cluster_myvars['Rural'], RuralColumn)}")
    print(f"  Sex is SexColumn: {isinstance(household_myvars['Sex'], SexColumn)}")
    print(f"  date is DateTimeColumn: {isinstance(interview_myvars['date'], DateTimeColumn)}")

finally:
    os.unlink(temp_path)


print("\n" + "="*70)
print("COMPARISON: Three Syntax Options")
print("="*70)
print("""
Option A: Dict with tag-as-name (your suggestion)
---------------------------------------------------
myvars:
    Rural: !Rural reside     # Some redundancy but clean
    Sex: !Sex h2q3
    Region: region           # Untyped columns mixed in naturally

Pros: Clean, dict structure maintained, mixed typed/untyped natural
Cons: Small redundancy (Rural appears twice)


Option B: List with tags (just tested above)
---------------------------------------------------
myvars:
    - !Rural reside          # Most concise!
    - !Sex h2q3
    - Region: region         # Untyped as dict entry

Pros: Maximum conciseness, no redundancy
Cons: myvars is list of dicts (needs merging in code)


Option C: Dict with lowercase tags (original)
---------------------------------------------------
myvars:
    Rural: !rural reside     # Tag is type, key is output name
    Sex: !sex h2q3
    Region: region

Pros: Separation of concerns (name vs type)
Cons: More redundancy, less obvious for standard cases


RECOMMENDATION FOR YOUR USE CASE:
==================================
Given that variable names map 1-to-1 with types (Rural, Sex, Age, etc.),
I recommend **Option A** (your suggestion):

    Rural: !Rural reside
    Sex: !Sex h2q3

Why:
- Natural YAML dict structure (easy to process)
- Eliminates the lowercase tag vs uppercase name redundancy
- Mixed typed/untyped columns work seamlessly
- Only ~5 characters longer than Option B
- No special list-merging logic needed in country.py

For special cases where output name ≠ semantic type, we can still use:
    CustomName: !Rural reside    # Output "CustomName" with Rural semantics
""")
