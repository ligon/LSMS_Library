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
import sys
import textwrap
import types
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


# Modules excluded from the closure recursion: data ACCESS (DVC / S3 fetch +
# credentials) and path/location code.  Neither transforms parquet *content* --
# the source bytes are already versioned by the DVC sidecar md5
# (source_fingerprint) and the config by the per-table hashes -- so descending
# into them is pure over-invalidation (an S3-credential or path-layout edit
# would needlessly rebuild every cache).  Transform code in local_tools
# (df_data_grabber, format_id, get_dataframe, ...) is NOT excluded.  Keep this
# set tiny and access-only; a transform must never live here.
_EXCLUDED_MODULES = frozenset({"lsms_library.data_access", "lsms_library.paths"})


def _is_ours(obj) -> bool:
    m = getattr(obj, "__module__", "") or ""
    return m.startswith("lsms_library") and m not in _EXCLUDED_MODULES


# Cache/hash MECHANISM callables: they compute or compare the embedded cache
# hash and never touch DataFrame CONTENT.  Excluded per-callable so folding them
# does not make the content fingerprint self-referential -- editing the cache
# layer would otherwise rebuild every cache, and the Wave._input_hash /
# Country._table_cache_hash methods would recurse into the fingerprint machinery
# itself (they CALL build_transforms_fingerprint).  This lets us re-tag
# Wave.grab_data (body IS a content transform: check_adding_t, the >1e99
# sentinel filter, the dfs merge, map_index) and resolve self.<method> build
# calls (column_mapping, formatting_functions) without dragging the cache in.
# DELIBERATELY NOT excluded: to_parquet -- it mutates content on write
# (astype/reset_index/set_index/replace), so it is genuine build-path code.
# The local_tools entries were verified write/hash-only.
_EXCLUDED_CALLABLES = frozenset({
    "lsms_library.local_tools.cache_freshness",
    "lsms_library.local_tools.read_parquet_cache_hash",
    "lsms_library.local_tools.stamp_parquet_hash",
    "lsms_library.local_tools.source_fingerprint",
    "lsms_library.local_tools.cached_file_hash",
    "lsms_library.country.Wave._input_hash",          # the hash mechanism itself
    "lsms_library.country.Country._table_cache_hash",  # (would self-reference)
})


def _is_build_callable(obj) -> bool:
    """A recursable build-path callable: ours, not in an excluded module, and
    not a cache/hash-mechanism primitive (which never transforms content)."""
    if not (callable(obj) and _is_ours(obj)):
        return False
    qn = f"{getattr(obj, '__module__', '')}.{getattr(obj, '__qualname__', '')}"
    return qn not in _EXCLUDED_CALLABLES


def _unwrap(fn):
    # follow functools.wraps / lru_cache __wrapped__ chains to the real function
    # (a cached helper is a _lru_cache_wrapper with no __code__ / source).
    try:
        return inspect.unwrap(fn)
    except ValueError:
        return fn


def _source(fn) -> str:
    return textwrap.dedent(inspect.getsource(fn))  # dedent: methods are class-indented


def _strip_docstrings(tree):
    # Docstrings are documentation, not build logic -- drop them so a docstring
    # edit doesn't invalidate, and a stale pasted repr/address in a docstring
    # (country.py column_mapping has one) can't land in the fingerprint.
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            b = node.body
            if (b and isinstance(b[0], ast.Expr) and isinstance(getattr(b[0], "value", None), ast.Constant)
                    and isinstance(b[0].value.value, str)):
                node.body = b[1:] or [ast.Pass()]
    return tree


def _norm(fn) -> str:
    # ast.dump drops comments + normalises whitespace; we also strip docstrings,
    # so neither a comment nor a docstring edit invalidates -- only logic does.
    try:
        return ast.dump(_strip_docstrings(ast.parse(_source(fn))))
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
        if _is_build_callable(obj):     # excluded cache primitives: name only, no recurse
            parts += _closure_parts(obj, seen)
        return f"<fn {obj.__module__}.{obj.__qualname__}>"
    if isinstance(obj, dict):
        return "{" + ",".join(
            f"{k!r}:{_ser(v, seen, parts)}"
            for k, v in sorted(obj.items(), key=lambda kv: repr(kv[0]))
        ) + "}"
    if isinstance(obj, (list, tuple)):
        return type(obj).__name__ + "[" + ",".join(_ser(v, seen, parts) for v in obj) + "]"
    if isinstance(obj, (set, frozenset)):
        # Sort by SERIALISED form, not repr(obj): repr of a set element is
        # PYTHONHASHSEED-sensitive when the element is itself a set/frozenset.
        return type(obj).__name__ + "[" + ",".join(sorted(_ser(v, seen, parts) for v in obj)) + "]"
    raise TypeError(
        f"build constant of un-serialisable type {type(obj).__name__!r}; add a "
        f"case to lsms_library._build_registry._ser rather than leak an identity repr"
    )


# Types we know how to fold into the fingerprint as a referenced constant.
_CONST_TYPES = (dict, list, tuple, set, frozenset, str, int, float, bool)


def _all_co_names(code) -> set:
    """Every global/attribute name referenced by ``code`` AND its nested code
    objects (comprehensions, lambdas, nested defs).

    CPython keeps names referenced inside a nested scope out of the enclosing
    function's ``co_names`` -- so a callable used only from an inner helper
    (e.g. ``df_data_grabber``, called only from ``grab_data``'s nested
    ``get_data``) would otherwise be invisible to the walk: a silent
    under-invalidation (stale cache, GH #522).  Recursing ``co_consts`` closes
    that.  (``co_freevars`` are closure cells, not global refs, so excluded.)
    """
    names = set(code.co_names)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names |= _all_co_names(const)
    return names


@functools.lru_cache(maxsize=None)
def _module_classes(module_name: str) -> tuple:
    mod = importlib.import_module(module_name)
    return tuple(v for v in vars(mod).values()
                 if isinstance(v, type) and getattr(v, "__module__", "") == module_name)


def _resolve_methods(module_name: str, name: str) -> list:
    """Resolve a bare attribute name (a ``self.X`` reference) against the methods
    and properties of the classes defined in ``module_name``.

    ``getattr(module, name)`` only finds module GLOBALS, so a build method called
    on self -- e.g. ``Wave.column_mapping`` / ``Wave.formatting_functions``, which
    drive YAML-route extraction -- is otherwise dropped (a stale-cache miss, GH
    #522).  A name can live on more than one class (``formatting_functions`` on
    both Wave and Country), so return all build-relevant matches.  The hash
    machinery (Wave._input_hash, Country._table_cache_hash) is filtered by
    _is_build_callable / _EXCLUDED_CALLABLES so the walk can't self-reference."""
    out = []
    for cls in _module_classes(module_name):
        m = getattr(cls, name, None)
        if isinstance(m, property):
            m = m.fget
        if _is_build_callable(m):
            out.append(m)
    return out


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
    # sorted(): _all_co_names is a set, so iteration order must be pinned.
    for name in sorted(_all_co_names(fn.__code__)):
        if name.startswith("__") and name.endswith("__"):
            continue                    # dunder (e.g. __file__) -> import metadata / abs path: non-deterministic
        obj = getattr(mod, name, None)
        if obj is None:
            if imap is None:
                imap = _import_map(fn)
            obj = imap.get(name)
        if obj is None:
            # Not a module global / import -> maybe a self.<method> build call.
            for m in _resolve_methods(fn.__module__, name):
                parts += _closure_parts(m, seen)
            continue
        if _is_build_callable(obj):
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
    # ast.dump's output format drifts across Python minor versions, so pin it:
    # a 3.11 -> 3.12 move then invalidates uniformly rather than silently.
    parts: list = [f"py={sys.version_info.major}.{sys.version_info.minor}"]
    for _qn, (fn, tables) in sorted(_BUILD_TRANSFORMS.items()):
        if table is not None and tables and table not in tables:
            continue
        parts += _closure_parts(fn, seen)
    return hashlib.sha256("\x1f".join(sorted(set(parts))).encode()).hexdigest()


@functools.lru_cache(maxsize=None)
def framework_imports_fingerprint(paths: tuple) -> str:
    """Fingerprint of the lsms_library framework callables imported by the given
    build-route .py files (the SCRIPT-path ``_/{table}.py`` scripts and the
    country/wave modules they import) plus their closures.

    The script route runs out-of-process (``make`` invokes the script), so the
    in-process closure walk from the tagged entry points cannot reach the
    framework helpers a script uses (e.g. ``conversion_table_matching_global``,
    ``melt_visit_intervals``, ``get_categorical_mapping``).  Those helpers DO
    transform content baked into the L2 parquet -- the residual GH #522 hazard
    for the script route.  A script's ``from lsms_library.X import name`` (any
    scope) and ``import lsms_library.X [as Y]`` + attribute use are statically
    resolvable, so we fold their closures in.  Returns "" when no framework
    import is found.  Memoised per path-tuple (content fixed per process)."""
    seen: set = set()
    parts: list = []
    for path in paths:
        try:
            tree = ast.parse(open(path, encoding="utf-8").read())
        except (OSError, SyntaxError, UnicodeDecodeError, ValueError):
            continue
        targets = []          # (module_name, attr)
        aliases = {}          # local alias -> lsms_library module name
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("lsms_library"):
                for a in node.names:
                    targets.append((node.module, a.name))
            elif isinstance(node, ast.Import):
                for a in node.names:
                    if a.name.startswith("lsms_library"):
                        aliases[a.asname or a.name] = a.name
        if aliases:
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) \
                        and node.value.id in aliases:
                    targets.append((aliases[node.value.id], node.attr))
        for modname, attr in sorted(set(targets)):
            try:
                obj = getattr(importlib.import_module(modname), attr, None)
            except Exception:
                obj = None
            if _is_build_callable(obj):
                parts += _closure_parts(obj, seen)
    return hashlib.sha256("\x1f".join(sorted(set(parts))).encode()).hexdigest() if parts else ""
