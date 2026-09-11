"""Derived values -- served numbers that are not survey answers.

GhanaLSS 1987-88 and 1988-89 ``food_acquired`` used to serve, on the
``s='produced'`` side, ``Y12B.VFOODCPD`` -- "How much would it cost to buy the
amount they ate each time?", the value of ONE EATING OCCASION -- as the row's
``Expenditure``, beside a purchased side whose value is a fortnight's recall.
Nothing in the frame distinguished the two.  Any commensurable number the
library serves there is a *construction* from four raw answers under a stated
assumption, and the census in ``SkunkWorks/derived_values.org`` found about
410 such sites across the corpus, a fifth of them disclosed nowhere a user
looks.

The precept is to respect the information in the original survey.  It is
satisfied only when all of the following hold (``SkunkWorks/derived_values.org``,
"The precept, and the two ways of failing it"):

1. the raw answers are retrievable exactly, by their original names, at the
   served grain -- :meth:`Country.derivation_inputs`;
2. the served row is labelled derived in a way that survives concatenation,
   caching and ``Feature()`` -- the ``Derivation`` COLUMN, carrying the
   registry key (``NA`` on a reported row; two keys on one row are joined
   with ``+`` in sorted order);
3. the derivation is a named, tested, reproducible function whose
   assumptions are stated with their evidence -- the registry,
   ``countries/<C>/_/derivations.yml``;
4. there is exactly ONE construction per parquet: a derivation function takes
   no options.  A user who wants the value under a different assumption
   re-derives it from (1).

Three carriers, because no single one does all three jobs: the column (row
prominence), the registry (the derivation as an evidenced object), and
``df.attrs['derivations']`` (a per-frame summary, attached in
``Country._finalize_result`` -- which is in ``_build_registry._EXCLUDED_CALLABLES``,
so it moves no cache hash).

The registry
------------
``countries/<C>/_/derivations.yml`` is a sibling of ``population.yml`` and
``recall.yml`` (a ``.yml`` sibling moves no cache hash; a block in
``data_scheme.yml`` would be read as a required column -- see
``country._SCHEME_NON_COLUMN_KEYS`` -- and would cold-rebuild the corpus).  Its
top level is a mapping from KEY to entry.  Framework-level derivations (an
empty country slot) live in ``lsms_library/derivations.yml`` beside the
canonical ``data_info.yml``.

Key format: ``country::table::name``, three slots, an EMPTY slot meaning
"all".  The wave is NOT in the key -- every served row already carries ``t``
-- so the wave scope lives in the entry (``waves: [...]`` or ``all``).  A rule
that genuinely differs by wave is a different derivation with a different
name.  A country file's entries must carry that country in the first slot; the
framework file's entries must carry an empty one.

``function`` and ``inputs`` are dotted callables, ``module:function``.  A
country module is spelled ``lsms_library.countries.<C>._.<module>`` and is
resolved BY PATH through :func:`paths.countries_root` (``countries/<C>/_/`` is
not an importable package), so ``LSMS_COUNTRIES_ROOT`` is honoured.  YAML plus
dotted callables rather than a decorator: a decorator registers only when the
module is imported, so a corpus listing would have to import every country
module (and executing their module-level data reads).  The YAML is readable
without importing anything.  A drift test keeps the two honest in both
directions.

The ``basis`` vocabulary is this file's OWN (:data:`BASES`): the recall
ladder's four evidence rungs plus ``modelling-choice``, for an assumption no
document can supply.  ``recall.BASIS_LADDER`` is deliberately not reused --
its loader rejects anything outside its ladder, and a modelling choice is not
evidence and must not be laundered into one.  Naming it is the point.

The loader refuses an entry missing ``rule``, ``function``, ``inputs``,
``raw_variables`` or ``assumptions``, or an assumption without a ``basis`` --
the ``PopulationRecord`` discipline: a record that cannot say what it did and
why is not a record.
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import inspect
import json
import sys
import warnings
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from .paths import countries_root
from .yaml_utils import load_yaml

__all__ = [
    "ATTRS_KEY", "COLUMN", "KEY_SEP", "BASES", "REQUIRED_FIELDS",
    "DerivationRecord", "parse_key", "format_key",
    "derivation_records", "framework_records", "records_for",
    "resolve_callable", "attach", "merge_attrs", "derivations_table",
    "derive_function_names",
]

#: ``df.attrs`` key holding ``{country: {key: {'rows', 'columns', 'in'}}}``.
ATTRS_KEY = "derivations"
#: The row-level column carrying the registry key (``NA`` on a reported row).
COLUMN = "Derivation"
#: Separator between the three key slots, and between two keys on one row.
KEY_SEP = "::"
MULTI_SEP = "+"

#: The basis vocabulary for an assumption.  This module's OWN -- see the
#: module docstring for why ``recall.BASIS_LADDER`` is not reused.
BASES = (
    "questionnaire",     # the instrument or its manual states it
    "variable-label",    # a Stata variable label states it
    "config-comment",    # a maintainer asserted it in repo config or prose
    "filename",          # inferred from a source filename -- a GUESS
    "modelling-choice",  # no document supplies it; the library chose it
)

#: An entry without any of these is not a record.
REQUIRED_FIELDS = ("rule", "function", "inputs", "raw_variables", "assumptions")

#: Every field a record may carry, in declaration order.
_FIELDS = (
    "key", "country", "table", "name", "waves", "columns", "rows",
    "rule", "function", "inputs", "raw_variables", "assumptions",
    "validated_against", "contents", "since",
)


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------

def parse_key(key: str) -> tuple[str, str, str]:
    """``'country::table::name'`` -> ``(country, table, name)``.

    Exactly three slots; ``country`` and ``table`` may be empty (meaning
    "all"); ``name`` may not.  Slots are stripped of whitespace.
    """
    if not isinstance(key, str):
        raise ValueError(f"derivation key must be a string, got {key!r}")
    parts = [p.strip() for p in key.split(KEY_SEP)]
    if len(parts) != 3:
        raise ValueError(
            f"derivation key {key!r} must have exactly three '{KEY_SEP}'-separated "
            f"slots, country{KEY_SEP}table{KEY_SEP}name (an empty slot means 'all')")
    country, table, name = parts
    if not name:
        raise ValueError(f"derivation key {key!r} has an empty name slot")
    if MULTI_SEP in key:
        raise ValueError(
            f"derivation key {key!r} may not contain '{MULTI_SEP}': it is the "
            f"separator between two keys on one row")
    return country, table, name


def format_key(country: str, table: str, name: str) -> str:
    return KEY_SEP.join((country or "", table or "", name))


# ---------------------------------------------------------------------------
# the record
# ---------------------------------------------------------------------------

class DerivationRecord(dict):
    """One registered derivation: what was done, to what, and why.

    An immutable ``dict`` subclass, for the reasons ``PopulationRecord`` gives:
    ``pandas.io.parquet`` serialises ``df.attrs`` with a bare ``json.dumps``,
    and pandas propagates ``attrs`` only when every input compares EQUAL, so
    the record must be JSON-serialisable and compare by value -- equal to the
    plain dict a parquet round-trips it back as.  Lists (``waves``,
    ``columns``, ``assumptions``) stay lists for that reason; ``__hash__``
    goes through ``json.dumps`` because a tuple of items would choke on them.
    Reading an absent optional field as an attribute returns ``None``.
    """

    _FIELDS = _FIELDS
    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        data = dict(*args, **kwargs)
        unknown = sorted(set(data) - set(self._FIELDS))
        if unknown:
            raise TypeError(
                f"DerivationRecord got unexpected field(s) {unknown}; "
                f"known fields are {list(self._FIELDS)}")
        dict.__init__(self, {k: data[k] for k in self._FIELDS
                             if data.get(k) is not None})

    def __getattr__(self, name: str) -> Any:
        if name in self._FIELDS:
            return self.get(name)
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}")

    def __repr__(self) -> str:
        inner = ", ".join(f"{k}={self[k]!r}" for k in self._FIELDS if k in self)
        return f"DerivationRecord({inner})"

    def _immutable(self, *args: Any, **kwargs: Any):
        raise TypeError(
            "DerivationRecord is immutable: it is returned from an lru_cached "
            "loader and rides on df.attrs, so a mutation would be visible to "
            "every other reader.  Build a new record, or take dict(record).")

    __setitem__ = _immutable
    __delitem__ = _immutable
    update = _immutable
    setdefault = _immutable
    pop = _immutable
    popitem = _immutable
    clear = _immutable
    __ior__ = _immutable

    def __hash__(self) -> int:                     # dict sets this to None
        return hash(json.dumps(self, sort_keys=True, default=str))

    def __reduce__(self):
        return (self.__class__, (dict(self),))

    @property
    def bases(self) -> tuple[str, ...]:
        """The distinct assumption bases present, in ladder order."""
        present = {a.get("basis") for a in (self.get("assumptions") or [])}
        return tuple(b for b in BASES if b in present)

    def covers_wave(self, wave: str) -> bool:
        waves = self.get("waves", "all")
        return waves == "all" or str(wave) in {str(w) for w in waves}

    @classmethod
    def from_config(cls, key: str, block: Mapping[str, Any],
                    file_country: str | None) -> "DerivationRecord":
        """Build from one YAML entry, refusing an incomplete one.

        ``file_country`` is the country whose ``_/derivations.yml`` the entry
        sits in, or ``None`` for the framework file; the key's country slot
        must agree with it.
        """
        country, table, name = parse_key(key)
        expected = file_country or ""
        if country != expected:
            where = (f"countries/{file_country}/_/derivations.yml" if file_country
                     else "lsms_library/derivations.yml (framework)")
            raise ValueError(
                f"{key}: key country slot {country!r} does not match the file it "
                f"sits in ({where} -> expected {expected!r})")
        if not isinstance(block, Mapping):
            raise ValueError(f"{key}: entry must be a mapping, got {type(block).__name__}")
        missing = [f for f in REQUIRED_FIELDS if not block.get(f)]
        if missing:
            raise ValueError(
                f"{key}: derivation record is missing {missing}.  A record that "
                f"cannot say what it did (rule, function), to what (inputs, "
                f"raw_variables) and why (assumptions) is not a record.")
        unknown = sorted(set(block) - set(_FIELDS) - {"key", "country", "table", "name"})
        if unknown:
            raise ValueError(f"{key}: unknown field(s) {unknown}; known: {list(_FIELDS)}")
        assumptions = block["assumptions"]
        if not isinstance(assumptions, list) or not assumptions:
            raise ValueError(f"{key}: assumptions must be a non-empty list")
        clean_assumptions = []
        for a in assumptions:
            if not isinstance(a, Mapping) or not a.get("text"):
                raise ValueError(f"{key}: each assumption needs a text: {a!r}")
            basis = a.get("basis")
            if not basis:
                raise ValueError(
                    f"{key}: assumption {a.get('text')!r} has no basis.  A basis "
                    f"is what stops a modelling choice being read as a fact.")
            if basis not in BASES:
                raise ValueError(f"{key}: basis {basis!r} is not one of {BASES}")
            clean_assumptions.append({k: a[k] for k in ("text", "basis", "evidence")
                                      if a.get(k) is not None})
        for spec_field in ("function", "inputs"):
            spec = block[spec_field]
            if not isinstance(spec, str) or ":" not in spec:
                raise ValueError(
                    f"{key}: {spec_field} must be a dotted 'module:function', got {spec!r}")
        raw = block["raw_variables"]
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise ValueError(f"{key}: raw_variables must be a non-empty list")
        waves = block.get("waves", "all")
        if waves != "all":
            if isinstance(waves, str):
                waves = [waves]
            if not isinstance(waves, list) or not waves:
                raise ValueError(f"{key}: waves must be a non-empty list or 'all'")
            waves = [str(w) for w in waves]
        columns = block.get("columns") or []
        if isinstance(columns, str):
            columns = [columns]
        rows = block.get("rows")
        if rows is not None and not isinstance(rows, Mapping):
            raise ValueError(f"{key}: rows must be a mapping selector "
                             f"(e.g. {{s: produced}}) or null, got {rows!r}")
        return cls(
            key=key, country=country or None, table=table or None, name=name,
            waves=waves, columns=list(columns),
            rows=dict(rows) if rows is not None else None,
            rule=str(block["rule"]).strip(), function=block["function"],
            inputs=block["inputs"], raw_variables=[str(r) for r in raw],
            assumptions=clean_assumptions,
            validated_against=block.get("validated_against"),
            contents=block.get("contents"),
            since=str(block["since"]) if block.get("since") is not None else None,
        )


# ---------------------------------------------------------------------------
# config loading
# ---------------------------------------------------------------------------

def derivations_yml_path(country: str) -> Path:
    """Resolved via ``countries_root()`` so ``LSMS_COUNTRIES_ROOT`` is honoured."""
    return Path(countries_root()) / country / "_" / "derivations.yml"


def framework_yml_path() -> Path:
    """The framework file, beside the canonical ``data_info.yml``."""
    return Path(str(files("lsms_library") / "derivations.yml"))


def _load(path: Path, file_country: str | None) -> dict[str, DerivationRecord]:
    if not path.exists():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, Mapping):
        raise ValueError(f"{path}: top level must be a mapping of key -> entry")
    out: dict[str, DerivationRecord] = {}
    for key, block in data.items():
        out[str(key)] = DerivationRecord.from_config(str(key), block, file_country)
    return out


@lru_cache(maxsize=None)
def derivation_records(country: str) -> dict[str, DerivationRecord]:
    """``{key: DerivationRecord}`` for one country; ``{}`` when it has none.

    Cached, so records are immutable.  Clear with
    ``derivation_records.cache_clear()`` after editing config in a session.
    A malformed file RAISES: that is a config bug someone must fix.
    """
    return _load(derivations_yml_path(country), country)


@lru_cache(maxsize=None)
def framework_records() -> dict[str, DerivationRecord]:
    """Entries of ``lsms_library/derivations.yml`` (empty country slot)."""
    return _load(framework_yml_path(), None)


def records_for(country: str, table: str | None = None
                ) -> dict[str, DerivationRecord]:
    """The country's records plus the framework's, optionally for one table.

    An entry with an empty table slot applies to every table.
    """
    out = dict(derivation_records(country))
    out.update(framework_records())
    if table is not None:
        out = {k: r for k, r in out.items() if r.table in (None, table)}
    return out


# ---------------------------------------------------------------------------
# dotted callables
# ---------------------------------------------------------------------------

_COUNTRY_MODULE_PREFIX = "lsms_library.countries."


def _load_module_by_path(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    # Registered BEFORE exec, as importlib's recipe does, so the module is
    # findable by name afterwards (inspect / pickle / sys.modules lookups).
    sys.modules[name] = mod
    try:
        spec.loader.exec_module(mod)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return mod


@lru_cache(maxsize=None)
def _resolve_module(module: str):
    """``lsms_library.countries.<C>._.<mod>`` by PATH; anything else by import."""
    if module.startswith(_COUNTRY_MODULE_PREFIX):
        rest = module[len(_COUNTRY_MODULE_PREFIX):].split(".")
        # <C>._.<mod>  or  <C>.<wave>._.<mod>
        if len(rest) < 3 or rest[-2] != "_":
            raise ImportError(
                f"{module}: a country module is spelled "
                f"lsms_library.countries.<C>._.<module> (or <C>.<wave>._.<module>)")
        path = Path(countries_root()).joinpath(*rest[:-1]) / f"{rest[-1]}.py"
        if not path.exists():
            raise ImportError(f"{module}: {path} does not exist")
        return _load_module_by_path(path, "_derivations_" + "_".join(rest))
    return importlib.import_module(module)


def resolve_callable(spec: str):
    """``'module:function'`` -> the callable.  Raises if either half is missing."""
    if not isinstance(spec, str) or ":" not in spec:
        raise ValueError(f"expected 'module:function', got {spec!r}")
    module, _, fn_name = spec.partition(":")
    mod = _resolve_module(module.strip())
    fn = getattr(mod, fn_name.strip(), None)
    if fn is None or not callable(fn):
        raise AttributeError(f"{spec}: {module} has no callable {fn_name!r}")
    return fn


def derive_function_names(module_path: Path) -> list[str]:
    """Top-level ``def derive_*`` names in a source file, WITHOUT importing it.

    The reverse direction of the drift test: every such function in a country
    module must be registered.  ``ast`` rather than import, because a country
    module executes data reads at module level.
    """
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    return sorted(n.name for n in tree.body
                  if isinstance(n, ast.FunctionDef) and n.name.startswith("derive_"))


# ---------------------------------------------------------------------------
# attrs
# ---------------------------------------------------------------------------

def _key_counts(df: pd.DataFrame) -> dict[str, int]:
    """Rows per key in the ``Derivation`` column; a ``'a+b'`` value counts under each."""
    if COLUMN not in df.columns:
        return {}
    col = df[COLUMN]
    col = col[col.notna()].astype(str)
    if col.empty:
        return {}
    parts = col.str.split(MULTI_SEP, regex=False).explode().str.strip()
    parts = parts[parts != ""]
    return {str(k): int(v) for k, v in parts.value_counts().items()}


def summary_for_frame(df: pd.DataFrame, country: str, table: str | None
                      ) -> dict[str, dict[str, Any]]:
    """``{key: {'rows': n, 'columns': [...], 'in': table}}`` for one frame.

    Counts come from the frame's own ``Derivation`` column, so a frame sliced
    to one wave reports only what it carries.  A registered entry for this
    table that the frame does not carry is listed with ``rows: 0`` -- the
    design's deletion case ("a row that was dropped has nowhere to carry a
    key"), and an honest zero for a slice that excludes the derived waves.
    """
    counts = _key_counts(df)
    registry = records_for(country, table) if table else records_for(country)
    out: dict[str, dict[str, Any]] = {}
    for key, n in counts.items():
        rec = registry.get(key)
        out[key] = {"rows": n,
                    "columns": list(rec.columns) if rec is not None else [],
                    "in": table}
    for key, rec in registry.items():
        if key not in out:
            out[key] = {"rows": 0, "columns": list(rec.columns), "in": table}
    return out


def attach(df: pd.DataFrame, country: str, table: str | None) -> None:
    """Attach ``df.attrs['derivations'] = {country: summary}``.

    Never raises: a metadata annotation that can break a data call is worse
    than no annotation (the rule ``population.attach`` and ``recall.attach``
    follow).  Attaches nothing when there is nothing to say.
    """
    try:
        summary = summary_for_frame(df, country, table)
        if not summary:
            return
        existing = df.attrs.get(ATTRS_KEY)
        merged = dict(existing) if isinstance(existing, dict) else {}
        merged[country] = summary
        df.attrs[ATTRS_KEY] = merged
    except Exception as exc:                        # pragma: no cover - never fatal
        warnings.warn(f"{country}/{table}: could not attach the derivations "
                      f"summary ({type(exc).__name__}: {exc}); the data are unaffected.")


def merge_attrs(frames: Iterable[pd.DataFrame]) -> dict[str, dict[str, Any]]:
    """Union the ``derivations`` attrs of several frames, for ``Feature()`` assembly.

    ``attrs`` survive an operation only when every input agrees, so a
    cross-country ``concat`` always lands in the ``{}`` case; a re-attach is
    therefore load-bearing (``CLAUDE.md``, "Panel ID Transitive Chains").
    """
    out: dict[str, dict[str, Any]] = {}
    for f in frames:
        got = getattr(f, "attrs", {}).get(ATTRS_KEY)
        if isinstance(got, dict):
            for c, summ in got.items():
                out.setdefault(c, {}).update(summ)
    return out


# ---------------------------------------------------------------------------
# the corpus table
# ---------------------------------------------------------------------------

TABLE_COLUMNS = ["key", "country", "table", "name", "waves", "columns", "rows",
                 "bases", "n_assumptions", "validated_against", "since",
                 "function", "inputs"]


def derivations_table(country: str | list[str] | None = None) -> pd.DataFrame:
    """Every registered derivation, one row per key -- the coverage-matrix shape.

    Assembled from the packaged per-country ``_/derivations.yml`` files plus
    the framework file, so it works from an installed wheel.  A derivation the
    census found that is not in this table is a visible gap.
    """
    from .catalog import countries as _countries
    if country is None:
        names = list(_countries())
    elif isinstance(country, str):
        names = [country]
    else:
        names = list(country)
    recs: list[DerivationRecord] = []
    for name in names:
        recs.extend(derivation_records(name).values())
    if country is None:
        recs.extend(framework_records().values())
    rows = []
    for r in recs:
        rows.append({
            "key": r.key, "country": r.country, "table": r.table, "name": r.name,
            "waves": "all" if r.waves == "all" else ", ".join(r.waves),
            "columns": ", ".join(r.columns or []),
            "rows": json.dumps(r.rows) if r.rows else None,
            "bases": "+".join(r.bases),
            "n_assumptions": len(r.assumptions or []),
            "validated_against": r.validated_against, "since": r.since,
            "function": r.function, "inputs": r.inputs,
        })
    df = pd.DataFrame(rows, columns=TABLE_COLUMNS)
    return df.set_index("key").sort_index()


def call_inputs(record: DerivationRecord, wave: str | None = None) -> pd.DataFrame:
    """Run the entry's ``inputs`` callable and return the raw-input frame.

    A callable with a ``wave`` parameter is called once per wave in the
    entry's scope (or the one ``wave`` given) and the frames concatenated;
    one without is called bare.  The frame is at whatever grain the inputs
    have -- nothing requires one row per served row.
    """
    fn = resolve_callable(record.inputs)
    params = inspect.signature(fn).parameters
    if "wave" in params:
        if wave is not None:
            waves = [str(wave)]
        elif record.waves == "all":
            raise ValueError(
                f"{record.key}: inputs callable takes a wave but the entry is "
                f"scoped to all waves; pass wave=...")
        else:
            waves = list(record.waves)
        frames = [fn(w) for w in waves]
        return pd.concat(frames) if len(frames) > 1 else frames[0]
    return fn()
