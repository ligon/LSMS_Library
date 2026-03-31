# YAML Tag Syntax Recommendation

## Summary

After testing multiple approaches, we recommend using **tags that match output column names**:

```yaml
myvars:
    Rural: !Rural reside      # Rural-type column from 'reside'
    Sex: !Sex h2q3            # Sex-type column from 'h2q3'
    Age: !Age h2q8            # Age-type column from 'h2q8'
    Region: region            # Untyped columns mixed naturally
```

This syntax:
- ✅ Eliminates redundancy (tag name = semantic type = output name)
- ✅ Maintains natural dict structure (easy to process)
- ✅ Reads clearly: "Rural column of Rural-type from reside"
- ✅ Allows mixed typed/untyped columns seamlessly

## Three Tested Options

### Option A: Tag-as-Name (RECOMMENDED)

```yaml
cluster_features:
    myvars:
        Rural: !Rural reside
        Sex: !Sex h2q3
        Region: region
```

**Test result:** ✅ Works perfectly

**Pros:**
- Natural YAML dict structure
- Tag name = output column name (eliminates redundancy vs lowercase tags)
- Mixed typed/untyped columns work naturally
- Easy to process in country.py (already expects dict)

**Cons:**
- Minor redundancy: "Rural" appears twice (but conveys useful info both times)

**Processing in Python:**
```python
# myvars is a dict
for key, value in myvars.items():
    if isinstance(value, TypedColumn):
        # It's a tagged column
        source = value.source_column
        transformer = value.get_transformer()
    else:
        # It's a plain string (untyped column)
        source = value
        transformer = None
```

---

### Option B: List-Based Tags (Most Concise)

```yaml
cluster_features:
    myvars:
        - !Rural reside       # Returns {'Rural': RuralColumn('reside')}
        - !Sex h2q3           # Returns {'Sex': SexColumn('h2q3')}
        - Region: region      # Regular dict entry
```

**Test result:** ✅ Works, but myvars is a list of dicts

**Pros:**
- Maximum conciseness (no name duplication at all)
- Reads very cleanly

**Cons:**
- myvars becomes a list of dicts (needs merging)
- Changes existing structure (country.py expects dict)

**Processing in Python:**
```python
# myvars is a list of dicts - need to merge
myvars_dict = {}
for item in myvars:
    if isinstance(item, dict):
        myvars_dict.update(item)

# Then process merged dict...
```

---

### Option C: Lowercase Type Tags (Original Proposal)

```yaml
cluster_features:
    myvars:
        Rural: !rural reside   # Tag is type, key is output name
        Sex: !sex h2q3
        Region: region
```

**Test result:** ✅ Works perfectly

**Pros:**
- Separates semantic type (!rural) from output name (Rural)
- Allows flexibility: `CustomName: !rural reside`

**Cons:**
- More redundancy: Rural/!rural essentially convey same info
- Less obvious that tag and name should match
- Lowercase/uppercase distinction can be confusing

---

## Recommendation: Use Option A

For the LSMS Library use case, **Option A (tag-as-name)** is the best choice because:

1. **Variable names already encode semantic types**
   - "Rural" is always a rural/urban indicator
   - "Sex" is always gender
   - "Age" is always age
   - Tag name matching output name makes this explicit

2. **Minimal code changes**
   - country.py already processes myvars as a dict
   - Just add type checking: `if isinstance(value, TypedColumn)`

3. **Clear semantics**
   - `Rural: !Rural reside` reads as "Rural column, of Rural-type, from reside"
   - The small redundancy actually reinforces correctness

4. **Flexibility for edge cases**
   - Can still do: `MyRural: !Rural reside` if needed
   - Or even: `Rural: !Urban urban` (output name ≠ tag for inverted columns)

## Implementation Examples

### Standard Semantic Types

```yaml
# Geographic
Rural: !Rural reside          # Standard Rural=1, Urban=0
Region: !Region region        # Categorical region
District: !District district  # Categorical district

# Demographics
Sex: !Sex h2q3               # Standard Male/Female
Age: !Age h2q8               # Integer age
Relation: !Relation h2q4     # Categorical relationship

# Temporal
date: !DateTime interview_start   # Parse as datetime
year: !Year year_col              # Integer year
```

### Handling Inversions

For inverted encodings (like Uganda's "urban" column):

```yaml
# Option 1: Use different tag name
Rural: !Urban urban          # Tag indicates inversion, output still "Rural"

# Option 2: Custom output name
Rural_Inverted: !Rural urban  # Output name indicates special handling
```

### Mixed Typed/Untyped

```yaml
myvars:
    Rural: !Rural reside     # Typed
    Sex: !Sex h2q3           # Typed
    Region: region           # Untyped (processed as before)
    Custom: custom_col       # Untyped
```

## Type Registry

Proposed semantic types (tags):

| Tag | Output Name | Semantic Meaning | Example Source |
|-----|-------------|------------------|----------------|
| `!Rural` | Rural | Rural=1, Urban=0 | reside |
| `!Urban` | Rural | Urban=1, inverted to Rural=1 | urban |
| `!Sex` | Sex | Male/Female categorical | h2q3 |
| `!Age` | Age | Integer age in years | h2q8 |
| `!Relation` | Relation | Relationship to head | h2q4 |
| `!DateTime` | date | Datetime parsing | interview_start |
| `!Year` | year | Integer year | year_col |
| `!Month` | month | Integer month (1-12) | month_col |
| `!Day` | day | Integer day (1-31) | day_col |
| `!Region` | Region | Categorical region | region |
| `!District` | District | Categorical district | district |
| `!Int` | (varies) | Generic integer | count_col |
| `!Float` | (varies) | Generic float | amount_col |
| `!Category` | (varies) | Generic categorical | category_col |

## Integration with country.py

Minimal changes needed in `country.py:column_mapping()`:

```python
def map_formatting_function(var_name, value, format_id_function = False):
    """Applies formatting functions if available, otherwise uses defaults."""

    # NEW: Handle TypedColumn instances from tags
    if isinstance(value, TypedColumn):
        # Extract source column and transformation
        return (value.source_column, value.get_transformer())

    # Existing logic unchanged...
    if isinstance(value, list) and isinstance(value[-1], dict):
        if value[-1].get('mapping'):
            # ... existing mapping logic ...
```

**Total changes:** ~10 lines

## Migration Path

### Phase 1: Add tag support (backward compatible)
- Register YAML tag constructors
- Update `country.py:column_mapping()` to handle TypedColumn
- All existing YAML files work unchanged

### Phase 2: Migrate high-priority variables
- Start with Rural (critical due to inversion bug)
- Then Sex, Age, DateTime
- Country by country

### Phase 3: Complete migration
- Migrate remaining typed columns
- Document patterns
- Create tag registry

## Example: Uganda 2019-20 Migration

**Before:**
```yaml
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
        v: s1aq04a
    myvars:
        Region: region
        Rural: urban           # Ambiguous: is this inverted?
        District: district

household_roster:
    file: HH/gsec2.dta
    idxvars:
        i: hhid
        pid: pid
    myvars:
        Sex: h2q3             # What do codes mean?
        Age: h2q8             # What type?
```

**After:**
```yaml
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: !HouseholdId hhid
        v: !String s1aq04a
    myvars:
        Region: !Region region
        Rural: !Urban urban    # EXPLICIT: inverted encoding
        District: !District district

household_roster:
    file: HH/gsec2.dta
    idxvars:
        i: !HouseholdId hhid
        pid: !PersonId pid
    myvars:
        Sex: !Sex h2q3        # EXPLICIT: standard Male/Female
        Age: !Age h2q8        # EXPLICIT: integer age
```

**Benefits:**
- Clear that Rural comes from inverted "urban" column
- Sex encoding is standardized
- Age is explicitly typed
- Self-documenting for future users

## Security Note

All tag constructors use `yaml.safe_load()` - no arbitrary code execution possible.

## Conclusion

**Recommended syntax:**
```yaml
Rural: !Rural reside
Sex: !Sex h2q3
```

**Why it's optimal for LSMS Library:**
- Variable names ≡ semantic types (Rural, Sex, Age)
- Tag-as-name eliminates redundancy vs lowercase tags
- Natural YAML dict structure (minimal code changes)
- Explicit typing prevents bugs (Rural inversion issue)
- Self-documenting and maintainable

---

**Next step:** Implement tag registration and integration with `country.py`
