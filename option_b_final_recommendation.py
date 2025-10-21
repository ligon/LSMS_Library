#!/usr/bin/env python3
"""
Final comparison: Option A vs Option B with proper constructors
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


# ============================================================================
# Option A: Constructors return TypedColumn directly
# ============================================================================

def rural_constructor_a(loader, node):
    source = loader.construct_scalar(node)
    return RuralColumn(source)  # Returns TypedColumn

def sex_constructor_a(loader, node):
    source = loader.construct_scalar(node)
    return SexColumn(source)


# ============================================================================
# Option B: Constructors return dict with output_name as key
# ============================================================================

def rural_constructor_b(loader, node):
    source = loader.construct_scalar(node)
    return {'Rural': RuralColumn(source)}  # Returns dict

def sex_constructor_b(loader, node):
    source = loader.construct_scalar(node)
    return {'Sex': SexColumn(source)}


# ============================================================================
# Test Option A
# ============================================================================

print("="*70)
print("OPTION A: Dict with tag-as-name (Rural: !Rural reside)")
print("="*70)

# Fresh loader for Option A
loader_a = yaml.SafeLoader
loader_a.yaml_constructors = loader_a.yaml_constructors.copy()
loader_a.add_constructor('!Rural', rural_constructor_a)
loader_a.add_constructor('!Sex', sex_constructor_a)

option_a_yaml = """
myvars:
    Rural: !Rural reside
    Sex: !Sex h2q3
    Region: region
    District: district
"""

print("YAML:")
print(option_a_yaml)

result_a = yaml.load(option_a_yaml, Loader=loader_a)
myvars_a = result_a['myvars']

print("Result:")
print(f"  Type: {type(myvars_a)}")
print(f"  Structure: {myvars_a}")
print("\nCharacter count per tagged line:")
print("  'Rural: !Rural reside' = 21 chars")
print("  'Sex: !Sex h2q3' = 15 chars")
print("\nPros:")
print("  ✓ Native dict - no processing needed")
print("  ✓ Direct access: myvars['Rural']")
print("  ✓ Mixed typed/untyped natural")
print("\nCons:")
print("  ✗ 'Rural' appears twice (redundant)")
print("  ✗ More verbose")


# ============================================================================
# Test Option B
# ============================================================================

print("\n" + "="*70)
print("OPTION B: List-based (- !Rural reside)")
print("="*70)

# Fresh loader for Option B
loader_b = yaml.SafeLoader
loader_b.yaml_constructors = loader_b.yaml_constructors.copy()
loader_b.add_constructor('!Rural', rural_constructor_b)
loader_b.add_constructor('!Sex', sex_constructor_b)

option_b_yaml = """
myvars:
    - !Rural reside
    - !Sex h2q3
    - Region: region
    - District: district
"""

print("YAML:")
print(option_b_yaml)

result_b = yaml.load(option_b_yaml, Loader=loader_b)
myvars_b_list = result_b['myvars']

print("Result (before merge):")
print(f"  Type: {type(myvars_b_list)}")
print(f"  Structure: {myvars_b_list}")

# THE TRIVIAL MERGE
def merge_myvars(myvars_list):
    result = {}
    for item in myvars_list:
        if isinstance(item, dict):
            result.update(item)
    return result

myvars_b = merge_myvars(myvars_b_list)

print("\nResult (after merge):")
print(f"  Type: {type(myvars_b)}")
print(f"  Structure: {myvars_b}")
print("\nCharacter count per tagged line:")
print("  '- !Rural reside' = 16 chars (24% shorter)")
print("  '- !Sex h2q3' = 12 chars (20% shorter)")
print("\nPros:")
print("  ✓ Most concise - zero redundancy")
print("  ✓ More 'YAML-like' (list of items)")
print("  ✓ Visually cleaner (bullets)")
print("  ✓ After merge: identical to Option A")
print("\nCons:")
print("  ✗ Requires trivial merge (5 lines of code)")


# ============================================================================
# Side-by-side YAML comparison
# ============================================================================

print("\n" + "="*70)
print("SIDE-BY-SIDE YAML COMPARISON")
print("="*70)

print("""
Option A (Dict-based)          Option B (List-based)
-------------------------      -------------------------
myvars:                        myvars:
    Rural: !Rural reside           - !Rural reside
    Sex: !Sex h2q3                 - !Sex h2q3
    Region: region                 - Region: region
    District: district             - District: district

Redundancy: Rural appears 2x   Redundancy: NONE
Verbosity: Higher              Verbosity: Lower
Processing: None needed        Processing: Trivial merge
YAML idiom: Key-value pairs    YAML idiom: List of items
""")


# ============================================================================
# Real-world example comparison
# ============================================================================

print("\n" + "="*70)
print("REAL-WORLD EXAMPLE: Uganda 2019-20 cluster_features")
print("="*70)

option_a_real = """
# Option A
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
        v: s1aq04a
    myvars:
        Rural: !Rural reside
        Region: !Region region
        District: !District district
"""

option_b_real = """
# Option B
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
        v: s1aq04a
    myvars:
        - !Rural reside
        - !Region region
        - !District district
"""

print("Option A:")
print(option_a_real)
print("\nOption B:")
print(option_b_real)

import sys
print("\nLine count:")
print(f"  Option A: {len(option_a_real.strip().split(chr(10)))} lines")
print(f"  Option B: {len(option_b_real.strip().split(chr(10)))} lines")

print("\nCharacter count:")
print(f"  Option A: {len(option_a_real)} chars")
print(f"  Option B: {len(option_b_real)} chars")
print(f"  Difference: {len(option_a_real) - len(option_b_real)} chars saved ({100*(len(option_a_real) - len(option_b_real))/len(option_a_real):.1f}%)")


# ============================================================================
# Final recommendation
# ============================================================================

print("\n" + "="*70)
print("FINAL RECOMMENDATION")
print("="*70)

print("""
USER'S QUESTION: "Why not use Option B if merge is trivial?"

ANSWER: You're absolutely right. The merge IS trivial.

My original objection was based on:
  1. "One more processing step" - TRUE but trivial
  2. "Different from current structure" - TRUE but we're adding new feature
  3. "List of dicts complexity" - FALSE, it's actually simple

The REAL considerations are:

FAVOR Option B when:
  ✓ User-facing syntax clarity is priority #1
  ✓ Minimizing redundancy matters
  ✓ You want most "YAML-like" idiom (lists for collections)
  ✓ Willing to add 5-line merge function

FAVOR Option A when:
  ✓ Minimizing code changes is priority #1
  ✓ Want identical structure to existing myvars
  ✓ Dict access pattern feels more natural
  ✓ Small redundancy acceptable for structural familiarity

MY UPDATED RECOMMENDATION:
==========================
For LSMS Library, I now recommend OPTION B because:

1. YAML files are user-facing - clarity matters most
2. Users edit YAML 100x more often than we modify country.py
3. 5-10 char reduction per line adds up (60+ myvars per file)
4. List syntax is semantically correct (collection of definitions)
5. The merge is genuinely trivial (could be 1-liner)
6. We're ADDING a new feature, not retrofitting

Implementation:
--------------
Add this to Wave.resources property or column_mapping():

def merge_myvars(myvars):
    '''Normalize list or dict to dict.'''
    if isinstance(myvars, list):
        result = {}
        for item in myvars:
            if isinstance(item, dict):
                result.update(item)
        return result
    return myvars  # Already a dict

That's literally it. The entire "cost" of Option B.

RECOMMENDATION: Use Option B (list-based) syntax.
  - !Rural reside
  - !Sex h2q3
  - Region: region

It's cleaner, more concise, and more "YAML-like".
The merge is too trivial to be a real objection.

User was right to question this.
""")
