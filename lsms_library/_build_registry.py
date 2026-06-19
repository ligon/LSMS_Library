#!/usr/bin/env python3
"""Build-path transform registry + content fingerprint (L2 cache, step 2).

A *leaf* module: it imports nothing from the ``lsms_library`` package, so any
module may import it without re-introducing an import cycle.

``@build_transform`` tags a function whose output is baked into the cached L2
parquet -- a *build-path* transform.  ``build_transforms_fingerprint(table)``
returns a content hash of the transitive ``lsms_library`` closure of every
tagged function relevant to ``table``: the function's (AST-normalised) source,
plus the source of the ``lsms_library``-owned callables it reaches and the
module-level constants it reads.  Folded into ``Country._table_cache_hash`` and
``Wave._input_hash``, this versions build-path *code* -- closing the GH #522
blind spot -- without invalidating the warm cache on read-path edits.

Design + rationale: ``SkunkWorks/build_transform_cache_hash.org`` (rev 4).

Two genuinely hard sub-problems, each with a red-test in
``tests/test_build_transform_hash.py``:

- *resolution* -- ``co_names`` is a flat list of bare strings;
  ``getattr(own_module, name)`` misses the function-level
  ``from .mod import name`` cycle-dodge idiom (GH #514's
  ``_ADDITIVE_MEASURE_COLUMNS``, ``grab_data``'s ``apply_derived``).  The AST
  ``ImportFrom`` walk in :func:`_import_map` closes that; the drift guard fails
  on any unresolved reference.
- *serialization* -- a default ``repr`` of a referenced object would embed a
  ``0x`` identity address and make the fingerprint non-deterministic.
  :func:`_ser` is an explicit type-dispatch with an else-*reject*; a red-test
  asserts no ``0x[0-9a-f]+`` appears in any fingerprint part.
"""
from __future__ import annotations

import ast
import functools
import hashlib
import importlib
import inspect
import textwrap
from typing import Callable

# qualname -> (callable, tables).  ``tables == ()`` means "every table".
_BUILD_TRANSFORMS: dict[str, tuple[Callable, tuple]] = {}


def build_transform(*, tables=None):
    """Mark a function as a build-path entry point.

    ``tables`` scopes which per-table fingerprints fold it in; ``None`` / empty
    means every table.  Returns the function **unchanged** (a tag, not a
    wrapper) so binding and call semantics are byte-for-byte identical.
    """
    def deco(fn):
        _BUILD_TRANSFORMS[f"{fn.__module__}.{fn.__qualname__}"] = (fn, tuple(tables or ()))
        return fn
    return deco


def registered_entry_points() -> frozenset:
    """Qualnames of every ``@build_transform``-tagged function (drift guard)."""
    return frozenset(_BUILD_TRANSFORMS)


def _is_ours(obj) -> bool:
    return (getattr(obj, "__module__", "") or "").startswith("lsms_library")


def _unwrap(fn):
    # follow functools.wraps / lru_cache __wrapped__ chains to the real function
    # (a cached helper is a _lru_cache_wrapper with no __code__ / source).
    try:
        return inspect.unwrap(fn)
    except ValueError:
        return fn


def _source(fn) -> str:
    return textwrap.dedent(inspect.getsource(fn))  # dedent: methods are class-indented


def _norm(fn) -> str:
    # ast.dump drops comments + normalises whitespace, so a comment-only edit
    # does not invalidate (docstring nodes are still included -- by design).
    try:
        return ast.dump(ast.parse(_source(fn)))
    except (OSError, SyntaxError, TypeError):
        return f"<unsourced {fn.__module__}.{fn.__qualname__}>"


def _import_map(fn) -> dict:
    """``name -> object`` for every ``from .mod import name`` in ``fn``.

    Parses the function once and resolves both module-level and function-level
    (cycle-dodge) relative imports -- the references ``getattr(own_module, ...)``
    silently drops.  Returns ``{name: None}`` for an unresolved target so the
    drift guard can distinguish "saw the import, could not resolve" from "never
    referenced".
    """
    out: dict = {}
    try:
        tree = ast.parse(_source(fn))
    except (OSError, SyntaxError, TypeError):
        return out
    parent = fn.__module__.rsplit(".", 1)[0]
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            try:
                tgt = importlib.import_module("." * node.level + (node.module or ""), package=parent)
            except Exception:
                tgt = None
            for alias in node.names:
                out[alias.asname or alias.name] = getattr(tgt, alias.name, None) if tgt else None
    return out


def _ser(obj, seen, parts) -> str:
    """Deterministic serialisation of a referenced build constant.

    Explicit type-dispatch with an else-*reject*: an unhandled type raises
    rather than leaking a default ``repr`` (which would embed a ``0x`` identity
    address).  ``lsms_library`` callables are replaced by their qualname and
    their source is folded into ``parts`` (so a callable held in a registry dict
    -- e.g. ``_DERIVED_TRANSFORMERS`` -- is versioned by *code*, not address).
    """
    if isinstance(obj, (str, int, float, bool, type(None))):
        return repr(obj)
    if callable(obj) and _is_ours(obj):
        parts += _closure_parts(obj, seen)
        return f"<fn {obj.__module__}.{obj.__qualname__}>"
    if isinstance(obj, dict):
        return "{" + ",".join(
            f"{k!r}:{_ser(v, seen, parts)}"
            for k, v in sorted(obj.items(), key=lambda kv: repr(kv[0]))
        ) + "}"
    if isinstance(obj, (list, tuple, set, frozenset)):
        xs = obj if isinstance(obj, (list, tuple)) else sorted(obj, key=repr)
        return type(obj).__name__ + "[" + ",".join(_ser(v, seen, parts) for v in xs) + "]"
    raise TypeError(
        f"build constant of un-serialisable type {type(obj).__name__!r}; add a "
        f"case to lsms_library._build_registry._ser rather than leak an identity repr"
    )


# Types we know how to fold into the fingerprint as a referenced constant.
_CONST_TYPES = (dict, list, tuple, set, frozenset, str, int, float, bool)


def _closure_parts(fn, seen) -> list:
    fn = _unwrap(fn)
    if not hasattr(fn, "__code__"):
        return []                       # builtin / C / unresolvable callable
    qn = f"{fn.__module__}.{fn.__qualname__}"
    if qn in seen:
        return []
    seen.add(qn)
    parts = [qn + "=" + _norm(fn)]
    mod = importlib.import_module(fn.__module__)
    imap = None  # parsed lazily, only if getattr misses
    for name in fn.__code__.co_names:
        obj = getattr(mod, name, None)
        if obj is None:
            if imap is None:
                imap = _import_map(fn)
            obj = imap.get(name)
            if obj is None:
                continue
        if callable(obj) and _is_ours(obj):
            parts += _closure_parts(obj, seen)
        elif isinstance(obj, _CONST_TYPES):
            parts.append(f"{fn.__module__}.{name}=" + _ser(obj, seen, parts))
    return parts


@functools.lru_cache(maxsize=None)
def build_transforms_fingerprint(table: str | None = None) -> str:
    """sha256 of the build-path closure relevant to ``table`` (every tagged
    transform when ``table`` is ``None``).

    Memoised: source is fixed per process, so this is ~free after the first call
    per table -- the v0.7.0 fast path is preserved.  Call
    :func:`build_transforms_fingerprint.cache_clear` in tests that re-tag.
    """
    seen: set = set()
    parts: list = []
    for _qn, (fn, tables) in sorted(_BUILD_TRANSFORMS.items()):
        if table is not None and tables and table not in tables:
            continue
        parts += _closure_parts(fn, seen)
    return hashlib.sha256("\x1f".join(sorted(set(parts))).encode()).hexdigest()
