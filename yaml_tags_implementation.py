#!/usr/bin/env python3
"""
YAML tags implementation for LSMS Library using tag-as-name syntax.

This module provides custom YAML tags where the tag name matches the semantic type
and output column name, eliminating redundancy.

Example:
    Rural: !Rural reside     # Rural-type column from 'reside' source
    Sex: !Sex h2q3          # Sex-type column from 'h2q3' source
"""

import yaml
import pandas as pd
from typing import Callable, Dict, Any


class TypedColumn:
    """
    Base class for typed column specifications.

    Each TypedColumn knows:
    - source_column: The column to read from the source file
    - How to transform the data (via apply() method)
    """

    # Override in subclasses if output name should differ from class name
    output_name = None

    def __init__(self, source_column: str):
        self.source_column = source_column

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.source_column}')"

    def apply(self, df: pd.DataFrame) -> pd.Series:
        """Apply the type transformation to extract/transform column from dataframe."""
        raise NotImplementedError(f"{self.__class__} must implement apply()")

    def get_transformer(self) -> Callable:
        """Return a function that can be used by df_data_grabber."""
        return lambda x: self.apply(x) if isinstance(x, pd.DataFrame) else x


# ============================================================================
# Geographic/Location Types
# ============================================================================

class RuralColumn(TypedColumn):
    """
    Rural/Urban indicator with standard Rural=1, Urban=0 encoding.

    Handles multiple case variations and ensures consistent output.
    """
    output_name = 'Rural'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        standard_mapping = {
            'Rural': 1, 'rural': 1, 'RURAL': 1,
            'Urban': 0, 'urban': 0, 'URBAN': 0,
            1: 1, 0: 0
        }
        return df[self.source_column].map(lambda x: standard_mapping.get(x, x))


class UrbanColumn(TypedColumn):
    """
    Urban/Rural indicator with INVERTED encoding (Urban=1, Rural=0).

    This is for source columns named 'urban' (like Uganda) where the encoding
    is inverted. The output will be corrected to standard Rural=1, Urban=0.
    """
    output_name = 'Rural'  # Output is still 'Rural'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        # Source has Urban=1, we want Rural=1 in output, so invert
        inverted_mapping = {
            'Urban': 1, 'urban': 1, 'URBAN': 1,
            'Rural': 0, 'rural': 0, 'RURAL': 0,
            1: 1, 0: 0
        }
        result = df[self.source_column].map(lambda x: inverted_mapping.get(x, x))
        # Invert: source urban=1 -> output Rural=0 (urban)
        #         source urban=0 -> output Rural=1 (rural)
        return 1 - result


class RegionColumn(TypedColumn):
    """Categorical region variable."""
    output_name = 'Region'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return df[self.source_column].astype('category')


class DistrictColumn(TypedColumn):
    """Categorical district variable."""
    output_name = 'District'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return df[self.source_column].astype('category')


# ============================================================================
# Demographic Types
# ============================================================================

class SexColumn(TypedColumn):
    """
    Sex/Gender variable with standard Male/Female encoding.

    Handles common numeric codes (1=Male, 2=Female) and string variations.
    """
    output_name = 'Sex'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        standard_mapping = {
            1: 'Male', 2: 'Female',
            '1': 'Male', '2': 'Female',
            'M': 'Male', 'F': 'Female',
            'Male': 'Male', 'Female': 'Female',
            'male': 'Male', 'female': 'Female',
            'MALE': 'Male', 'FEMALE': 'Female'
        }
        return df[self.source_column].map(standard_mapping).astype('category')


class AgeColumn(TypedColumn):
    """Age in years as integer."""
    output_name = 'Age'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df[self.source_column], errors='coerce').astype('Int64')


class RelationColumn(TypedColumn):
    """Relationship to household head as categorical."""
    output_name = 'Relation'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return df[self.source_column].astype('category')


# ============================================================================
# Temporal Types
# ============================================================================

class DateTimeColumn(TypedColumn):
    """
    DateTime parsing with flexible format handling.

    TODO: Support format specifications and component dates
    """
    output_name = 'date'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_datetime(df[self.source_column], errors='coerce')


class YearColumn(TypedColumn):
    """Year as integer."""
    output_name = 'year'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df[self.source_column], errors='coerce').astype('Int64')


class MonthColumn(TypedColumn):
    """Month (1-12) as integer."""
    output_name = 'month'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df[self.source_column], errors='coerce').astype('Int64')


class DayColumn(TypedColumn):
    """Day of month (1-31) as integer."""
    output_name = 'day'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df[self.source_column], errors='coerce').astype('Int64')


# ============================================================================
# Generic Types
# ============================================================================

class IntColumn(TypedColumn):
    """Generic integer type."""

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df[self.source_column], errors='coerce').astype('Int64')


class FloatColumn(TypedColumn):
    """Generic float type."""

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df[self.source_column], errors='coerce').astype('Float64')


class StringColumn(TypedColumn):
    """Generic string type."""

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return df[self.source_column].astype('string')


class CategoryColumn(TypedColumn):
    """Generic categorical type."""

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return df[self.source_column].astype('category')


# ============================================================================
# ID Types
# ============================================================================

class HouseholdIdColumn(TypedColumn):
    """Household identifier."""
    output_name = 'i'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return df[self.source_column].astype('string')


class PersonIdColumn(TypedColumn):
    """Person identifier within household."""
    output_name = 'pid'

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return df[self.source_column].astype('string')


# ============================================================================
# YAML Constructor Registration
# ============================================================================

def make_constructor(column_class):
    """Factory to create YAML constructors for TypedColumn classes."""
    def constructor(loader, node):
        source = loader.construct_scalar(node)
        return column_class(source)
    return constructor


# Type registry mapping tag names to column classes
TYPE_REGISTRY: Dict[str, type] = {
    # Geographic
    'Rural': RuralColumn,
    'Urban': UrbanColumn,
    'Region': RegionColumn,
    'District': DistrictColumn,

    # Demographic
    'Sex': SexColumn,
    'Age': AgeColumn,
    'Relation': RelationColumn,

    # Temporal
    'DateTime': DateTimeColumn,
    'Year': YearColumn,
    'Month': MonthColumn,
    'Day': DayColumn,

    # Generic
    'Int': IntColumn,
    'Float': FloatColumn,
    'String': StringColumn,
    'Category': CategoryColumn,

    # IDs
    'HouseholdId': HouseholdIdColumn,
    'PersonId': PersonIdColumn,
}


def register_yaml_tags(registry: Dict[str, type] = None):
    """
    Register all custom YAML tags with the SafeLoader.

    Args:
        registry: Optional custom type registry. Uses TYPE_REGISTRY if None.
    """
    if registry is None:
        registry = TYPE_REGISTRY

    for tag_name, column_class in registry.items():
        tag = f'!{tag_name}'
        yaml.SafeLoader.add_constructor(tag, make_constructor(column_class))


def load_yaml_with_tags(yaml_path: str) -> Dict[str, Any]:
    """
    Load a YAML file with custom tag support.

    Args:
        yaml_path: Path to YAML file

    Returns:
        Parsed YAML structure with TypedColumn instances
    """
    register_yaml_tags()
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example YAML with tag-as-name syntax
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
        pid: !PersonId pid
    myvars:
        Sex: !Sex h2q3
        Age: !Age h2q8
        Relation: !Relation h2q4

interview_date:
    file: HH/gsec1.dta
    idxvars:
        i: !HouseholdId hhid
    myvars:
        date: !DateTime interview_start
"""

    import tempfile
    import os

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write(example)
        temp_path = f.name

    try:
        result = load_yaml_with_tags(temp_path)

        print("✓ Successfully loaded YAML with tag-as-name syntax\n")
        print("cluster_features.myvars:")
        for k, v in result['cluster_features']['myvars'].items():
            print(f"  {k}: {v}")

        print("\nType verification:")
        myvars = result['cluster_features']['myvars']
        print(f"  Rural is RuralColumn: {isinstance(myvars['Rural'], RuralColumn)}")
        print(f"  Region is RegionColumn: {isinstance(myvars['Region'], RegionColumn)}")

        print("\nidxvars:")
        for k, v in result['cluster_features']['idxvars'].items():
            print(f"  {k}: {v}")

    finally:
        os.unlink(temp_path)
