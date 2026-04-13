#!/usr/bin/env python3
"""
Prototype implementation of YAML tags for LSMS Library type system.

This module defines custom YAML tags for explicit typing in data_info.yml files,
addressing inconsistencies in how variables like Rural, Sex, dates, etc. are handled.
"""

import yaml
import pandas as pd
from typing import Any, Dict, Callable


class TypedColumn:
    """Base class for typed column specifications."""

    def __init__(self, source_column: str, dtype: str = None, **kwargs):
        self.source_column = source_column
        self.dtype = dtype
        self.params = kwargs

    def __repr__(self):
        return f"{self.__class__.__name__}({self.source_column}, {self.params})"

    def apply(self, df: pd.DataFrame, target_column: str) -> pd.Series:
        """Apply the type transformation to a dataframe column."""
        raise NotImplementedError


class DatetimeColumn(TypedColumn):
    """Datetime type with optional format and component handling."""

    def apply(self, df: pd.DataFrame, target_column: str) -> pd.Series:
        format_str = self.params.get('format')
        errors = self.params.get('errors', 'coerce')
        components = self.params.get('components')

        if components:
            # Handle year/month/day components
            return pd.to_datetime(df[components], errors=errors)
        else:
            # Single column datetime
            return pd.to_datetime(df[self.source_column], format=format_str, errors=errors)


class BinaryColumn(TypedColumn):
    """Binary column with custom true/false mappings."""

    def apply(self, df: pd.DataFrame, target_column: str) -> pd.Series:
        mapping = self.params.get('mapping', {})
        return df[self.source_column].map(mapping)


class RuralColumn(BinaryColumn):
    """
    Rural indicator with standardized Rural=1, Urban=0 encoding.
    Use UrbanColumn for inverted encoding.
    """

    def apply(self, df: pd.DataFrame, target_column: str) -> pd.Series:
        # Standard mapping: Rural=1, Urban=0
        # Handle multiple case variations
        standard_mapping = {
            'Rural': 1, 'rural': 1, 'RURAL': 1,
            'Urban': 0, 'urban': 0, 'URBAN': 0,
            1: 1, 0: 0  # If already numeric
        }
        return df[self.source_column].map(lambda x: standard_mapping.get(x, x))


class UrbanColumn(BinaryColumn):
    """
    Urban indicator with Urban=1, Rural=0 encoding (inverted from RuralColumn).
    This is for columns like Uganda's 'urban' column.
    """

    def apply(self, df: pd.DataFrame, target_column: str) -> pd.Series:
        # Inverted mapping: Urban=1, Rural=0
        # Then invert to make final output Rural=1, Urban=0
        standard_mapping = {
            'Urban': 1, 'urban': 1, 'URBAN': 1,
            'Rural': 0, 'rural': 0, 'RURAL': 0,
            1: 1, 0: 0
        }
        # Get the value and invert it
        result = df[self.source_column].map(lambda x: standard_mapping.get(x, x))
        # Invert: if source has urban=1, we want Rural=0 in output (which means urban)
        # Actually, the target column name is "Rural", so if source is "urban" column:
        # source urban=1 -> output Rural=0 (false)
        # source urban=0 -> output Rural=1 (true)
        return 1 - result


class CategoryColumn(TypedColumn):
    """Categorical type with optional mapping."""

    def apply(self, df: pd.DataFrame, target_column: str) -> pd.Series:
        mapping = self.params.get('mapping')
        col = df[self.source_column]

        if mapping:
            col = col.map(mapping)

        return col.astype('category')


class SexColumn(CategoryColumn):
    """Sex/Gender column with standard Male/Female encoding."""

    def apply(self, df: pd.DataFrame, target_column: str) -> pd.Series:
        # Standard mapping from common numeric codes
        default_mapping = self.params.get('mapping', {
            1: 'Male',
            2: 'Female',
            'M': 'Male',
            'F': 'Female',
            'Male': 'Male',
            'Female': 'Female'
        })
        return df[self.source_column].map(default_mapping).astype('category')


# YAML Constructor functions
def datetime_constructor(loader, node):
    """Construct DatetimeColumn from !datetime tag."""
    if isinstance(node, yaml.ScalarNode):
        # Simple case: !datetime column_name
        source = loader.construct_scalar(node)
        return DatetimeColumn(source)
    elif isinstance(node, yaml.MappingNode):
        # Complex case: !datetime{format: "%Y-%m-%d", source: column_name}
        value = loader.construct_mapping(node)
        source = value.pop('source', value.pop('_source', None))
        if source is None:
            raise ValueError("!datetime tag requires 'source' key in mapping")
        return DatetimeColumn(source, **value)
    else:
        raise ValueError(f"Invalid node type for !datetime: {type(node)}")


def binary_constructor(loader, node):
    """Construct BinaryColumn from !binary tag."""
    value = loader.construct_mapping(node)
    source = value.pop('source')
    mapping = value.pop('mapping', None)
    return BinaryColumn(source, mapping=mapping, **value)


def rural_constructor(loader, node):
    """Construct RuralColumn from !rural tag."""
    source = loader.construct_scalar(node)
    return RuralColumn(source)


def urban_constructor(loader, node):
    """Construct UrbanColumn from !urban tag."""
    source = loader.construct_scalar(node)
    return UrbanColumn(source)


def category_constructor(loader, node):
    """Construct CategoryColumn from !category tag."""
    if isinstance(node, yaml.ScalarNode):
        source = loader.construct_scalar(node)
        return CategoryColumn(source)
    elif isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node)
        source = value.pop('source')
        return CategoryColumn(source, **value)


def sex_constructor(loader, node):
    """Construct SexColumn from !sex tag."""
    if isinstance(node, yaml.ScalarNode):
        source = loader.construct_scalar(node)
        return SexColumn(source)
    elif isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node)
        source = value.pop('source')
        return SexColumn(source, **value)


def int_constructor(loader, node):
    """Construct integer typed column from !int tag."""
    source = loader.construct_scalar(node)
    return TypedColumn(source, dtype='int64')


def float_constructor(loader, node):
    """Construct float typed column from !float tag."""
    source = loader.construct_scalar(node)
    return TypedColumn(source, dtype='float64')


def str_constructor(loader, node):
    """Construct string typed column from !str tag."""
    source = loader.construct_scalar(node)
    return TypedColumn(source, dtype='str')


# Register all constructors
def register_yaml_tags():
    """Register all custom YAML tags with the loader."""
    yaml.SafeLoader.add_constructor('!datetime', datetime_constructor)
    yaml.SafeLoader.add_constructor('!binary', binary_constructor)
    yaml.SafeLoader.add_constructor('!rural', rural_constructor)
    yaml.SafeLoader.add_constructor('!urban', urban_constructor)
    yaml.SafeLoader.add_constructor('!category', category_constructor)
    yaml.SafeLoader.add_constructor('!sex', sex_constructor)
    yaml.SafeLoader.add_constructor('!int', int_constructor)
    yaml.SafeLoader.add_constructor('!float', float_constructor)
    yaml.SafeLoader.add_constructor('!str', str_constructor)


# Utility function to load YAML with tags
def load_yaml_with_tags(yaml_path: str) -> Dict:
    """Load a YAML file with custom tag support."""
    register_yaml_tags()
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


# Example usage
if __name__ == "__main__":
    # Example YAML content
    example_yaml = """
cluster_features:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
    myvars:
        Rural: !rural reside
        Region: region
        District: district

household_roster:
    file: HH/gsec2.dta
    idxvars:
        i: hhid
        pid: pid
    myvars:
        Sex: !sex h2q3
        Age: !int h2q8
        Relation: !category h2q4

interview_date:
    file: HH/gsec1.dta
    idxvars:
        i: hhid
    myvars:
        date: !datetime interview_start
"""

    # Test loading
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write(example_yaml)
        temp_path = f.name

    try:
        result = load_yaml_with_tags(temp_path)
        print("Loaded YAML with tags:")
        print(f"Rural column spec: {result['cluster_features']['myvars']['Rural']}")
        print(f"Sex column spec: {result['household_roster']['myvars']['Sex']}")
        print(f"Date column spec: {result['interview_date']['myvars']['date']}")
    finally:
        import os
        os.unlink(temp_path)
