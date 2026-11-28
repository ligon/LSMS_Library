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


class UniqueKeySchemeLoader(SchemeLoader):
    """Refuse explicit duplicates before merge flattening can hide their origin.

    SchemeLoader still constructs the document, including intentional inherited
    merge overrides. The raw graph is checked once, so recursive/shared aliases
    neither loop nor count a reused mapping as a duplicate declaration.
    """

    _duplicate_key_context = 'while constructing a mapping'

    def _duplicate_key_error(self, key, first, repeated):
        return yaml.constructor.ConstructorError(
            self._duplicate_key_context, first, f'found duplicate key {key!r}', repeated)

    def __init__(self, stream):
        self._key_marks = {}
        super().__init__(stream)

    def compose_node(self, parent, index):
        # Alias nodes retain their anchor's mark. Capture the actual occurrence
        # so a duplicate aliased key points to the repeated use, not its anchor.
        mark = self.peek_event().start_mark
        node = super().compose_node(parent, index)
        if isinstance(parent, yaml.MappingNode) and index is None:
            self._key_marks[id(parent), len(parent.value)] = mark
        return node

    def construct_document(self, node):
        visited = set()

        def check(node):
            if id(node) in visited:
                return
            visited.add(id(node))
            if isinstance(node, yaml.MappingNode):
                seen = {}
                for index, (key_node, value_node) in enumerate(node.value):
                    mark = self._key_marks.get((id(node), index), key_node.start_mark)
                    merge = key_node.tag == 'tag:yaml.org,2002:merge'
                    if merge:
                        key = '<<'  # SafeLoader handles this by tag, not key shape.
                    elif key_node.tag == 'tag:yaml.org,2002:value':
                        key = self.construct_scalar(key_node)
                    else:
                        key = self.construct_object(key_node, deep=True)
                    try:
                        token = (merge, key)
                        hash(token)
                    except TypeError:
                        raise yaml.constructor.ConstructorError(
                            'while constructing a mapping', node.start_mark,
                            'found unhashable key', mark) from None
                    if token in seen:
                        raise self._duplicate_key_error(key, seen[token], mark)
                    seen[token] = mark
                    check(key_node)
                    check(value_node)
            elif isinstance(node, yaml.SequenceNode):
                for child in node.value:
                    check(child)

        check(node)
        return super().construct_document(node)


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
