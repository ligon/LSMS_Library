"""
Helpers for loading YAML configuration with project-specific tags.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, IO

import yaml


class SchemeLoader(yaml.SafeLoader):
    """YAML loader that understands LSMS_Library-specific tags."""


def _construct_make(loader: SchemeLoader, node: yaml.Node) -> dict[str, Any]:
    """
    Interpret ``!make`` tagged nodes.

    Supports three shapes:
    - ``key: !make`` (bare tag) -> {"materialize": "make"}
    - ``key: !make food_expenditures.parquet`` (scalar) -> {"materialize": "make", "target": "..."}
    - ``key: !make {target: ..., cache_path: ...}`` (mapping) -> mapping + materialize flag
    """
    mapping: dict[str, Any]
    if isinstance(node, yaml.MappingNode):
        mapping = loader.construct_mapping(node, deep=True)
    elif isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
        mapping = {}
        if value not in (None, ""):
            mapping["target"] = value
    else:
        raise TypeError(f"Unsupported YAML node for !make tag: {type(node).__name__}")

    mapping.setdefault("materialize", "make")
    return mapping


SchemeLoader.add_constructor("!make", _construct_make)


def load_yaml(stream: IO[str] | str | Path) -> Any:
    """
    Load YAML content using :class:`SchemeLoader`, returning ``{}`` when empty.

    ``stream`` can be an open text IO object, a string containing YAML, or a path.
    """
    if isinstance(stream, (str, Path)) and not hasattr(stream, "read"):
        with open(stream, "r", encoding="utf-8") as handle:
            data = yaml.load(handle, Loader=SchemeLoader)
    else:
        data = yaml.load(stream, Loader=SchemeLoader)
    return data or {}
