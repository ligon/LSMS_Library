#!/usr/bin/env python3
"""
Reconsidering Option B: List-based YAML tags

The user correctly points out that merging a list of dicts is trivial.
Let's examine the real tradeoffs more carefully.
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


# Constructors that return dicts (for list-based syntax)
def rural_constructor(loader, node):
    source = loader.construct_scalar(node)
    return {'Rural': RuralColumn(source)}

def sex_constructor(loader, node):
    source = loader.construct_scalar(node)
    return {'Sex': SexColumn(source)}


yaml.SafeLoader.add_constructor('!Rural', rural_constructor)
yaml.SafeLoader.add_constructor('!Sex', sex_constructor)


# Compare Option A vs Option B side by side
option_a_yaml = """
# Option A: Dict-based (tag-as-name)
cluster_features:
    myvars:
        Rural: !Rural reside
        Sex: !Sex h2q3
        Region: region
        District: district
"""

option_b_yaml = """
# Option B: List-based (most concise)
cluster_features:
    myvars:
        - !Rural reside
        - !Sex h2q3
        - Region: region
        - District: district
"""

print("="*70)
print("COMPARING OPTION A vs OPTION B")
print("="*70)

# Test Option A
print("\n" + "="*70)
print("Option A: Dict-based")
print("="*70)
print(option_a_yaml)

with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(option_a_yaml)
    temp_path_a = f.name

result_a = yaml.safe_load(open(temp_path_a))
myvars_a = result_a['cluster_features']['myvars']

print("Loaded structure:")
print(f"  type(myvars): {type(myvars_a)}")
print(f"  myvars: {myvars_a}")
print("\nDirect access:")
print(f"  myvars['Rural']: {myvars_a['Rural']}")
print(f"  myvars['Region']: {myvars_a['Region']}")

os.unlink(temp_path_a)


# Test Option B
print("\n" + "="*70)
print("Option B: List-based")
print("="*70)
print(option_b_yaml)

with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(option_b_yaml)
    temp_path_b = f.name

result_b = yaml.safe_load(open(temp_path_b))
myvars_b_list = result_b['cluster_features']['myvars']

print("Loaded structure (raw):")
print(f"  type(myvars): {type(myvars_b_list)}")
print(f"  myvars: {myvars_b_list}")

# Merge list of dicts (the "objection")
myvars_b = {}
for item in myvars_b_list:
    myvars_b.update(item)

print("\nAfter merging (trivial operation):")
print(f"  type(myvars): {type(myvars_b)}")
print(f"  myvars: {myvars_b}")
print("\nDirect access:")
print(f"  myvars['Rural']: {myvars_b['Rural']}")
print(f"  myvars['Region']: {myvars_b['Region']}")

print("\n✓ After merging, Option B has identical access patterns to Option A")

os.unlink(temp_path_b)


# Now analyze the REAL tradeoffs
print("\n" + "="*70)
print("REAL TRADEOFFS ANALYSIS")
print("="*70)

print("""
My Original Objection:
  "myvars is list of dicts (needs merging in code)"

User's Point:
  Merging is trivial (2-3 lines). What's the real objection?

Let me reconsider...

OPTION A (Dict-based: Rural: !Rural reside)
-------------------------------------------
Pros:
  + Native dict structure - no merging needed
  + Familiar pattern for key-value mappings
  + One less operation in the loading pipeline
  + Existing code expects dict

Cons:
  - Redundancy: "Rural" appears twice
  - Less "YAML-like" for a collection of variable definitions
  - 5 more characters per line

OPTION B (List-based: - !Rural reside)
---------------------------------------
Pros:
  + MOST CONCISE: No redundancy at all
  + More "YAML-like": Lists for collections of items
  + Semantically clearer: "list of variable definitions"
  + Visually cleaner (bullet points)

Cons:
  - Requires merge operation (BUT IT'S TRIVIAL!)
  - Different structure than current code expects
  - One additional step in processing

The Merge "Objection" Examined:
--------------------------------
""")

merge_code = '''
# The "expensive" merge operation:
def merge_myvars(myvars_list):
    """Convert list of dicts to single dict."""
    result = {}
    for item in myvars_list:
        if isinstance(item, dict):
            result.update(item)
    return result

# That's it. That's the whole objection.
# 5 lines of trivial code.
'''

print(merge_code)

print("""
Computational Cost:
  - O(n) where n = number of variables (typically 3-10)
  - Each update() is O(k) where k = keys in dict (usually 1)
  - Total: O(n), happens once at YAML load time
  - Negligible cost

Code Complexity:
  - 5 lines of straightforward code
  - No edge cases, no error handling needed
  - Could be a one-liner: dict(ChainMap(*myvars_list))

Integration Cost:
  - Add merge in Wave.resources property OR
  - Add merge in column_mapping() OR
  - Teach column_mapping() to handle lists directly

MY REVISED POSITION:
====================
The "list of dicts" objection is WEAK. The merge is trivial.

The real question is: Which syntax is CLEARER for users?

Option B is:
  ✓ More concise (no redundancy)
  ✓ More "YAML-like" (lists for collections)
  ✓ Visually cleaner
  ✓ Semantically appropriate (collection of variable defs)

Option A is:
  ✓ Matches current code structure
  ✓ One less step in pipeline
  ✓ Familiar key-value pattern

RECOMMENDATION REVISED:
=======================
For NEW implementation, I'd choose Option B because:
  - The syntax is objectively cleaner
  - Users write YAML more often than we modify code
  - The merge is genuinely trivial
  - YAML semantics are important for readability

For INCREMENTAL adoption on existing codebase:
  - Option A is safer (less disruption)
  - Backward compatibility easier
  - Gradual migration possible

USER'S QUESTION IMPLIES:
  "If the merge is trivial, why not choose the cleaner syntax?"

ANSWER:
  You're right. The merge objection doesn't hold up.
  Option B IS cleaner.
  The only legitimate reason to prefer Option A is:
    "Less code disruption in existing system"

For a greenfield implementation, Option B wins.
For retrofitting existing code, Option A is safer.

But since we're ADDING a new feature (tags), we could choose
the cleaner syntax (Option B) and handle the merge gracefully.
""")


print("\n" + "="*70)
print("IMPLEMENTATION COMPARISON")
print("="*70)

print("""
Where to handle the merge (Option B):
--------------------------------------

Approach 1: In Wave.resources property
```python
@property
def resources(self):
    register_yaml_tags()
    with open(info_path, 'r') as file:
        data = yaml.safe_load(file)

    # Normalize myvars from list to dict
    for section in data.values():
        if isinstance(section, dict) and 'myvars' in section:
            myvars = section['myvars']
            if isinstance(myvars, list):
                section['myvars'] = merge_myvars(myvars)

    return data
```

Approach 2: In column_mapping()
```python
def column_mapping(self, request, data_info = None):
    files = data_info.get('file')
    idxvars = data_info.get('idxvars')
    myvars = data_info.get('myvars')

    # NEW: Handle list format
    if isinstance(myvars, list):
        myvars = merge_myvars(myvars)

    # ... rest of existing code unchanged ...
```

Approach 3: Support both formats
```python
def normalize_myvars(myvars):
    '''Accept list or dict, return dict.'''
    if isinstance(myvars, list):
        return merge_myvars(myvars)
    return myvars
```

All approaches are simple. The merge is NOT a real obstacle.
""")

print("\n" + "="*70)
print("CONCLUSION")
print("="*70)
print("""
My original objection was OVERSTATED.

The real tradeoff is:
  Option A: Familiar, less disruption, slightly more verbose
  Option B: Cleaner, more "YAML-like", requires trivial merge

For user-facing syntax, CLARITY WINS.

Option B is objectively cleaner:
  - !Rural reside          (7 chars + tag + value)
vs
  Rural: !Rural reside     (12 chars + tag + value)

If the merge is truly trivial (it is), then we should choose
the cleaner user-facing syntax and pay the tiny implementation cost.

USER IS RIGHT: The list-of-dicts objection doesn't hold water.

NEW RECOMMENDATION: Option B unless there's a strong reason
to minimize code changes (then Option A for safety).
""")
