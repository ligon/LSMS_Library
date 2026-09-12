"""Cross-country Feature class for assembling harmonized DataFrames."""

from __future__ import annotations

import inspect
import warnings
from pathlib import Path
from typing import Any, Callable, Mapping, NamedTuple


_UNSET = object()


def _method_parameters(method: Any) -> set[str]:
    """Parameter names a generated Country method accepts (for kwarg forwarding)."""
    try:
        return set(inspect.signature(method).parameters)
    except (TypeError, ValueError):
        return set()

import pandas as pd
import yaml
from importlib.resources import files

from .yaml_utils import load_yaml
from .currency import CURRENCY_LEVEL, is_monetary_table
from .paths import countries_root
from .errors import LabelUnavailableError
from . import population as _population
# NOT `from . import recall` / `from . import derivations`: the package binds
# PUBLIC CALLABLES over both submodule names (`ll.recall(...)` from
# `recall_table`, `ll.derivations(...)` = `derivations_table`), and
# `from package import name` prefers the package ATTRIBUTE over the submodule.
# This module is imported by `__init__` before those rebinds, so the submodule
# form happens to work today and would silently start importing a FUNCTION if
# `__init__`'s import order ever changed.  Name the symbols instead.
from .recall import ATTRS_KEY as _RECALL_ATTRS_KEY
from .recall import merge_attrs as _recall_merge_attrs
from .derivations import ATTRS_KEY as _DERIVATIONS_ATTRS_KEY
from .derivations import merge_attrs as _derivations_merge_attrs


# ---------------------------------------------------------------------------
# attrs carriers: metadata that rides on `df.attrs` and dies at the concat
# ---------------------------------------------------------------------------
#
# `attrs` survive an operation only when every input AGREES; any disagreement
# -- including one side having none -- yields {}.  (Measured on the pinned
# pandas 3.0.2; all seven cells are pinned by
# tests/test_population.py::TestAttrsSurvival, and the table is in CLAUDE.md
# under "Panel ID Transitive Chains and the attrs Flag".)
#
# Cross-country assembly is a `pd.concat` over frames whose records DIFFER BY
# DESIGN -- one per country, which is the entire point of each record -- so it
# lands in the {} case on every call with more than one country.  The capture
# below is therefore not belt-and-braces: it is the only thing keeping these
# records alive.  Do not "simplify" it away after testing concat with matching
# inputs and watching it preserve.  Same family of hazard as the
# `id_converted` bug.
#
# This used to be hardcoded for `population` alone, and `recall` (GH #851) and
# `derivations` (SkunkWorks/derived_values.org) each shipped a `merge_attrs`
# with NO caller -- so both died here silently (GH #873).  A fourth carrier is
# now one entry in the tuple below.

class _AttrsCarrier(NamedTuple):
    """One metadata record that must be captured before, and re-attached after,
    the cross-country ``concat``.

    ``keys``
        The ``df.attrs`` keys this carrier owns.  Captured verbatim per country.
    ``attach``
        ``(result, sources) -> None``.  ``sources`` is the list of captured
        ``attrs`` sub-mappings for the KEPT countries only, in target order.
        This is the carrier module's OWN ``merge_attrs`` -- the union rule
        belongs to the module that defines the record, never here.
    """
    name: str
    keys: tuple[str, ...]
    attach: Callable[[pd.DataFrame, list[Mapping[str, Any]]], None]


def _merge_into(key: str, merge: Callable[[Any], dict]) -> Callable[..., None]:
    """Adapt a ``merge_attrs(frames) -> dict`` to the ``attach`` protocol.

    ``population.merge_attrs`` already writes onto its target; ``recall`` and
    ``derivations`` return the union instead.  An empty union is not written,
    so a table with no such record keeps an absent key rather than an empty
    dict -- "no record" and "an empty record" must not become the same thing.
    """
    def _attach(result: pd.DataFrame, sources: list[Mapping[str, Any]]) -> None:
        merged = merge(sources)
        if merged:
            result.attrs[key] = merged
    return _attach


_ATTRS_CARRIERS: tuple[_AttrsCarrier, ...] = (
    _AttrsCarrier(
        "population",
        (_population.ATTRS_KEY, _population.ATTRS_RESOLUTION_KEY),
        _population.merge_attrs,
    ),
    _AttrsCarrier(
        "recall",
        (_RECALL_ATTRS_KEY,),
        _merge_into(_RECALL_ATTRS_KEY, _recall_merge_attrs),
    ),
    _AttrsCarrier(
        "derivations",
        (_DERIVATIONS_ATTRS_KEY,),
        _merge_into(_DERIVATIONS_ATTRS_KEY, _derivations_merge_attrs),
    ),
)

#: Every ``attrs`` key any carrier owns -- what the per-country capture keeps.
_CARRIED_ATTRS_KEYS: tuple[str, ...] = tuple(
    k for carrier in _ATTRS_CARRIERS for k in carrier.keys)


def _load_global_columns() -> dict[str, dict[str, Any]]:
    """Load the Columns section from the global data_info.yml."""
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("Columns", {})


def _all_known_features() -> set[str]:
    """Every table any country declares in its data_scheme.yml, plus the
    runtime-derived tables -- i.e. the set of valid ``Feature(...)`` names.
    Used to reject typos with a helpful suggestion rather than silently
    returning an empty frame.
    """
    names: set[str] = set(_DERIVED_SOURCE)
    try:
        for f in Path(countries_root()).glob("*/_/data_scheme.yml"):
            try:
                ds = (load_yaml(f) or {}).get("Data Scheme") or {}
                names.update(ds.keys() if isinstance(ds, dict) else ds)
            except Exception:
                continue
    except Exception:
        pass
    return names


def _index_info_section() -> dict[str, Any]:
    """The global ``Index Info`` block of data_info.yml (``{}`` if absent)."""
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    section = (data or {}).get("Index Info") or {}
    return section if isinstance(section, dict) else {}


def _canonical_index_levels(table_name: str) -> list[str]:
    """Return the canonical index level names for *table_name*.

    Reads the global ``Index Info: index_info`` section of data_info.yml,
    whose values are tuple strings like ``(t, v, i)``.  Returns ``[]`` when
    the table is not listed (no canonical reshaping is then attempted).
    """
    spec = (_index_info_section().get("index_info") or {}).get(table_name)
    if not isinstance(spec, str):
        return []
    cleaned = spec.strip()
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = cleaned[1:-1]
    return [tok.strip() for tok in cleaned.split(",") if tok.strip()]


def _level_aliases(table_name: str) -> dict[str, str]:
    """``{country's level name: canonical level name}`` for *table_name*.

    Reads ``Index Info: level_aliases`` (GH #569).  A country that names the
    same axis differently -- Ethiopia's ``plot_id`` for Uganda's ``plot``,
    Malawi's ``crop`` for Uganda's ``j`` -- would otherwise present a divergent
    index shape and be excluded from the assembly.  Renaming is declarative on
    purpose: it is the fact ``transformations._CROP_LEVELS`` / ``_PLOT_LEVELS``
    already encode for the ``Country()`` path, stated once where the canonical
    index is stated.
    """
    aliases = (_index_info_section().get("level_aliases") or {}).get(table_name)
    if not isinstance(aliases, dict):
        return {}
    return {str(k): str(v) for k, v in aliases.items()}


def _missing_level_sentinels(table_name: str) -> dict[str, str]:
    """``{canonical level: sentinel value}`` for *table_name*.

    Reads ``Index Info: missing_level_sentinels``.  Twin of -- and preferred
    over -- ``fabricate_missing_levels`` (#506), which fills with ``pd.NA``: a
    NULL on a declared index level is a DEFERRED SILENT DELETION, because
    ``groupby(dropna=True)`` removes the row in whichever aggregation runs
    first (CLAUDE.md "Grain Collapse" 3b; the ``u`` and ``condition`` notes in
    data_info.yml make the same argument for the same reason).

    Declaring a level here also licenses PROMOTION -- see
    :func:`_align_to_canonical_levels`.
    """
    spec = (_index_info_section().get("missing_level_sentinels") or {}).get(table_name)
    if not isinstance(spec, dict):
        return {}
    return {str(k): v for k, v in spec.items()}


def _rename_index_levels(df: pd.DataFrame, aliases: dict[str, str],
                         country: str, table_name: str) -> pd.DataFrame:
    """Rename this country's index levels to their canonical names (GH #569).

    A no-op when the frame declares no aliased level.  If BOTH the alias and
    its canonical target are already present the rename is skipped and warned
    about -- renaming would produce two levels with one name, and the country
    means something by the distinction that this table does not know.
    """
    if not aliases or df.index.names is None:
        return df
    names = list(df.index.names)
    todo = {src: dst for src, dst in aliases.items() if src in names}
    if not todo:
        return df
    clash = [src for src, dst in todo.items() if dst in names]
    if clash:
        warnings.warn(
            f"{table_name}: {country} carries both an aliased index level and "
            f"its canonical target {[(c, todo[c]) for c in clash]}; the rename "
            f"was skipped (it would create duplicate level names). The frame "
            f"keeps its own index shape."
        )
        return df
    return df.rename_axis(index=todo)


def _align_to_canonical_levels(
    df: pd.DataFrame, canonical_levels: list[str], sentinels: dict[str, Any],
    country: str, table_name: str, report: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Give *df* every canonical level for which a sentinel is declared.

    Three branches per declared level, in this order:

    1. **already an index level** -- nothing to do;
    2. **present as a COLUMN** -- promoted into the index, with nulls filled by
       the sentinel.  Lossless, and strictly better than fabricating a sentinel
       on top of real values: Mali, Nigeria and Tanzania each report a genuine
       harvest unit in a ``u`` column rather than in the index, and promoting it
       is what keeps them in the assembly instead of excluded;
    3. **absent entirely** -- fabricated as a constant sentinel level.

    Never ``pd.NA``: every value written here is the declared string sentinel,
    because a null on a declared index level is deleted by the next ``groupby``.

    **Branch 3 cannot make the index non-unique; BRANCH 2 CAN.**  An earlier
    version of this docstring claimed neither could, and that was wrong in the
    dangerous direction.  Adding a *constant* level distinguishes nothing, so it
    refines nothing and collides with nothing -- branch 3 is structurally safe.
    Promotion is not: a column holding BOTH a null (filled with the sentinel)
    and the LITERAL sentinel value maps two otherwise-identical rows onto one
    index tuple.  No corpus country is in that state today -- Mali, Nigeria and
    Tanzania's ``u`` columns carry 0 literal ``'Unknown'`` values -- but that is
    a fact about the data, not a property of the code, and it can change with
    any wave.

    So the collision is GUARDED rather than assumed away: a non-unique index
    after ``set_index`` is routed through :func:`_collapse_duplicate_index`, the
    same audited collapse the core uses, which files a grain report and raises
    ``GrainCollapseError`` under ``LSMS_GRAIN_STRICT`` when the colliding rows
    disagree.  Never a silent ``first()``, and never a silent pass-through of a
    non-unique index into ``pd.concat``.

    Reporting.  With ``report`` supplied, what happened is recorded there and
    :meth:`Feature.__call__` emits ONE aggregated warning per call and puts the
    record on ``df.attrs['canonical_alignment']`` -- the same Contract-B shape
    ``labels=`` degradation uses.  Per-country warnings would fire a dozen
    times on every ``Feature('crop_production')()`` for behaviour the config
    explicitly asks for, and a warning nobody reads is how #323 survived its
    first fix.  With ``report`` omitted (direct callers, tests) it warns
    per-country instead, so the helper still describes itself.
    """
    if not canonical_levels or not sentinels:
        return df
    names = list(df.index.names)
    if any(n is None for n in names):
        # An unnamed level cannot be round-tripped through reset_index /
        # set_index, and a frame that reached here unnamed has a bigger problem
        # than a missing sentinel (the #325 collapse warning covers it).
        return df
    todo = [lvl for lvl in canonical_levels
            if lvl in sentinels and lvl not in names]
    if not todo:
        return df

    flat = df.reset_index()
    promoted, fabricated = [], []
    for lvl in todo:
        sentinel = sentinels[lvl]
        if lvl in flat.columns:
            col = flat[lvl]
            if isinstance(col.dtype, pd.CategoricalDtype):
                col = col.astype(object)
            filled = col.where(col.notna(), sentinel)
            n_filled = int(col.isna().sum())
            flat[lvl] = filled
            promoted.append((lvl, n_filled))
        else:
            flat[lvl] = sentinel
            fabricated.append(lvl)
    out = flat.set_index(names + todo)
    out.attrs = dict(df.attrs)  # single-input ops keep attrs; be explicit anyway
    if promoted and not out.index.is_unique:
        # A promoted column held a null AND the literal sentinel on rows that are
        # otherwise identical -- the one way this function can collide.  Route it
        # through the SAME audited collapse the core uses so a destructive
        # collapse is reported (and fatal under LSMS_GRAIN_STRICT) rather than
        # shipping a silently non-unique Feature index.  `attrs` are re-attached
        # because `groupby().agg()` is not a single-input op.
        collapsed = _collapse_duplicate_index(out, table_name, country)
        collapsed.attrs = dict(out.attrs)
        n_lost = len(out) - len(collapsed)
        if report is not None:
            report.setdefault("promotion_collisions", {})[country] = {
                lvl: sentinels[lvl] for lvl, _ in promoted}
        else:
            warnings.warn(
                f"{table_name}: promoting {[lvl for lvl, _ in promoted]} for "
                f"{country} made the index NON-UNIQUE -- the column holds both "
                f"nulls (filled with the sentinel) and the literal sentinel "
                f"value, so {n_lost} row(s) collided onto an existing key. "
                f"Collapsed through the audited core collapse; any destructive "
                f"loss is reported separately as a GrainCollapseWarning."
            )
        out = collapsed
    if report is not None:
        if promoted:
            report.setdefault("promoted", {})[country] = {
                lvl: n for lvl, n in promoted}
        if fabricated:
            report.setdefault("fabricated", {})[country] = {
                lvl: sentinels[lvl] for lvl in fabricated}
        return out
    if promoted:
        warnings.warn(
            f"{table_name}: promoted column(s) "
            f"{[lvl for lvl, _ in promoted]} to index level(s) for {country} "
            f"(canonical index levels carried as columns); "
            f"{ {lvl: n for lvl, n in promoted} } null value(s) filled with the "
            f"declared sentinel."
        )
    if fabricated:
        warnings.warn(
            f"{table_name}: {country} does not record "
            f"{fabricated}; added as constant sentinel index level(s) "
            f"{ {lvl: sentinels[lvl] for lvl in fabricated} } so the frame keeps "
            f"the canonical shape. No rows were added, removed or collapsed."
        )
    return out


def _select_kept_shape(
    frames: list[pd.DataFrame], canonical_levels: list[str],
) -> tuple[list[pd.DataFrame], list[pd.DataFrame], tuple[Any, ...] | None]:
    """Choose the single index shape the assembly keeps.

    pandas cannot stack frames whose index DEPTH/NAMES differ into a named
    MultiIndex -- it falls back to an unnamed object index, collapsing the WHOLE
    feature (issue #512).  So one shape has to win.  The rule, in order:

    1. the CANONICAL shape (``['country'] + canonical_levels``) if any frame has
       it -- a country that moves TOWARD the declared index must never be the one
       excluded, which is exactly the regression this fixes (a Malawi frame that
       gained the canonical ``condition`` level lost 131,379 rows to the
       pre-#775 rule);
    2. otherwise the shape carried by the most frames (the historical "modal"
       rule, kept for features with no ``index_info`` entry);
    3. ties broken by total ROWS, then
    4. by the shape's level names -- the lexicographically smallest tuple.

    Steps 3-4 are what make this independent of the ORDER the caller listed the
    countries in.  ``Counter.most_common`` breaks a tie by insertion order, so
    ``Feature(f)(['A','B','C'])`` and ``Feature(f)(['C','B','A'])`` returned
    different countries for the same data (GH #775).

    Returns ``(kept, dropped, winning_shape)``; ``winning_shape`` is ``None``
    when every frame already agrees (nothing to choose).
    """
    if len(frames) <= 1:
        return frames, [], None
    shape_of = lambda f: tuple(f.index.names)
    shapes = {shape_of(f) for f in frames}
    if len(shapes) == 1:
        return frames, [], None

    canonical_shape = tuple(["country"] + list(canonical_levels)) if canonical_levels else None
    if canonical_shape is not None and canonical_shape in shapes:
        winner = canonical_shape
    else:
        stats: dict[tuple[Any, ...], list[int]] = {}
        for f in frames:
            s = shape_of(f)
            acc = stats.setdefault(s, [0, 0])
            acc[0] += 1
            acc[1] += len(f)
        # Sort key: most frames, then most rows, then the SMALLEST level-name
        # tuple (negated ranks so a single `min` expresses all three, and so
        # the third criterion is a genuine lexicographic minimum rather than
        # whichever way `max` happened to fall).
        def _rank(s: tuple[Any, ...]) -> tuple[Any, ...]:
            n_frames, n_rows = stats[s]
            return (-n_frames, -n_rows,
                    tuple("" if n is None else str(n) for n in s))
        winner = min(stats, key=_rank)
    kept = [f for f in frames if shape_of(f) == winner]
    dropped = [f for f in frames if shape_of(f) != winner]
    return kept, dropped, winner


def _fabricates_missing_levels(table_name: str) -> bool:
    """Whether *table_name* opts into NaN-fabrication of missing canonical
    levels (``Index Info: fabricate_missing_levels`` in data_info.yml).

    When True, ``_harmonize_country_frame`` adds any missing canonical index
    levels to a reduced-index country as a ``pd.NA`` level (so every country
    shares the full canonical shape and is KEPT), instead of leaving it reduced
    to be modal-excluded.  Use only where the reduced shape is legitimate
    variation -- e.g. interview_date's single-visit countries vs per-visit
    EHCVM ones (#506).
    """
    info_path = files("lsms_library") / "data_info.yml"
    with open(info_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    lst = data.get("Index Info", {}).get("fabricate_missing_levels", []) or []
    return table_name in lst


# Tables whose measure columns are ADDITIVE across a dropped recall/visit level.
# When collapsing the duplicate index left after dropping that level, these must
# be SUMMED (not reduced via first(), which undercounts the cross-country total).
# Motivating case (GH #501): GhanaLSS food_acquired carries a per-visit level
# (~12 repeated visits over a month); CONTENTS.org states the visits are summed.
# Keeping first() there silently kept only ~48% of total Quantity.
#
# This is the ONE reduction policy core keeps (see the NO-AGGREGATION-IN-CORE
# contract in SkunkWorks/grain_aggregation_policy.org and D1 of
# slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org).  It is legitimate only
# where the sum is provably LOSSLESS for the named column.  It is read from BOTH
# collapse sites: _collapse_duplicate_index below (Feature, cross-country) and
# country._normalize_dataframe_index (Country, per-table) -- so a table listed
# here is summed consistently on both access paths.  Do NOT add a column here to
# paper over a broken identifier or a missing index level: duplicates on a
# declared index usually mean the GRAIN IS WRONG, and a reducer would only put a
# signature on the corpse (D1).  Add a column only when the sum reconstructs
# exactly the quantity the coarser grain is DEFINED as.
#
# assets (GH #323): Nigeria W2's `sect5b_plantingw2` is a PER-UNIT ROSTER -- one
# row per individual unit owned, enumerated by `item_seq` (1..15), each with its
# own reported Value.  The canonical assets grain is (t, i, j), so those rows
# arrive as duplicates and first() kept ONE UNIT and discarded the rest:
# N576,299,043 true -> N429,001,558 kept -> N147,297,485 (25.6%) DESTROYED, in
# EACH of t=2012Q3 and t=2013Q1.  Summing Value is exactly lossless: the value of
# a household's holding of item j IS the sum over the units it owns (hh 10001
# owns 4 beds worth 7000+3000+6000+5000 = 21,000).
#   * `Quantity` must stay first(): it comes from the SEPARATE, already-clean
#     sect5a grid (one row per (i, j)) and the wave's `dfs:` merge REPEATS it
#     across the item_seq rows -- verified, 0 groups where it varies.  Summing it
#     would multiply the unit count by itself (4 beds -> 16).
#   * `Age` also stays first().  It is genuinely per-unit (it varies within 7,029
#     groups), so NO reducer is lossless at (t, i, j) -- the four beds are 10, 6,
#     10 and 6 years old and that fact cannot be carried by one row.  first()
#     keeps unit #1's age.  Retaining the detail needs the `item_seq` level to
#     survive, which it currently cannot: the extra idxvar is dropped by the
#     `dfs:` merge in Wave.grab_data (#323 Site 4), and Nigeria's other waves
#     have no item_seq column to declare.  Tracked as a residual, not fixed here;
#     the Age loss is now REPORTED rather than silenced (see the residual re-audit
#     in _collapse_duplicate_index and country._normalize_dataframe_index).
#
# WHERE THIS DEPARTS FROM THE RECORDED PRESCRIPTION, AND WHY.
# `Nigeria/_/data_scheme.yml` on the (unmerged) #625 branch carries a "KNOWN OPEN
# DEFECT" note that diagnoses all of the above correctly and then prescribes TWO
# edits: (1) `data_info.yml : index_info assets -> (t, i, j, item_seq)` and
# (2) this dict entry, with "Age wants mean".  Only (2) is done here.  Both
# departures were checked, not assumed:
#
#   * (1) IS INERT TODAY.  Declaring `item_seq` in Nigeria 2012-13's `assets`
#     idxvars AND in its `final_index` was measured cold (2026-07-21): the frame
#     comes back with index ['i','t','j'] regardless -- identical rows and
#     identical Value -- because the `dfs:` merge drops the extra idxvar before
#     `final_index` is applied (#323 Site 4).  Nigeria cannot EMIT `item_seq` at
#     all, so promoting it to canonical would make `item_seq` a level no country
#     supplies: `have_all_canonical` would go False for all 25 assets countries,
#     silently disabling the canonical level-reordering guard (GH #498), and
#     _collapse_duplicate_index would never fire -- making entry (2) dead code.
#     The note's own reasoning ("item_seq is exactly the missing level") is right
#     about the DATA and wrong about the PLUMBING.  Site 4 has to be fixed first.
#   * "Age wants mean" is superseded by two decisions taken after that note was
#     written (both 2026-07-13): the grain contract's P2 -- every output cell
#     holds an OBSERVED value or NA, which a mean violates by construction
#     (tests/test_gh323_grain_contract.py) -- and the retirement of the last
#     aggregation in core, the cluster-GPS `.mean()` at Site 2, after which
#     SkunkWorks/grain_aggregation_policy.org "has no exception left in it".
#     A mean Age would reintroduce exactly the exception just retired.
_ADDITIVE_MEASURE_COLUMNS = {
    "food_acquired": ("Quantity", "Expenditure"),
    "assets": ("Value",),
}


def _collapse_duplicate_index(df: pd.DataFrame, table_name: str,
                              country: str | None = None) -> pd.DataFrame:
    """Collapse duplicate index tuples left after dropping an extra index level.

    For additive-measure tables (GH #501) sum the additive columns and re-derive
    any unit-``Price`` column from the summed totals (price is per-unit, NOT
    additive).  Otherwise keep the first row per group (the historical default).

    GH #323: this is the SECOND of the two core ``.first()`` collapses named in
    SkunkWorks/grain_aggregation_policy.org.  Like the one in
    ``country._normalize_dataframe_index`` it is audited before it destroys
    anything -- a Feature() assembly must not be a place where a country quietly
    loses rows that ``Country(name).table()`` would have returned.
    """
    # Lazy import: country.py imports _ADDITIVE_MEASURE_COLUMNS from here, so a
    # module-level import back would cycle.
    from .country import (_audit_index_collapse, _record_grain_report,
                          _sum_min_count_1)

    additive = _ADDITIVE_MEASURE_COLUMNS.get(table_name)
    present = [c for c in (additive or ()) if c in df.columns]

    report = _audit_index_collapse(df, list(df.index.names))
    if report is not None and present and not report.get("unauditable"):
        # The additive SUM is lossless over the columns it RECONCILES -- the
        # measures themselves plus a ``Price`` re-derived from the summed totals --
        # so re-audit on what it does NOT fix rather than silencing wholesale.
        # `assets` carries a per-unit `Age` no reducer preserves at (t, i, j);
        # silencing the whole report would trade a recovered `Value` for a silent
        # `Age` destruction.  Kept identical to country._normalize_dataframe_index
        # so the two access paths agree (GH #323).
        reconciled = list(present)
        if "Price" in df.columns and {"Expenditure", "Quantity"} <= set(df.columns):
            reconciled.append("Price")
        residual = _audit_index_collapse(df.drop(columns=reconciled),
                                         list(df.index.names))
        if residual is None:
            report = None
        elif not residual.get("unauditable"):
            # An UNAUDITABLE residual is never silenced -- "we could not check" is
            # not "it is fine".
            report = dict(report, destroyed=residual["destroyed"],
                          conflicting_groups=residual["conflicting_groups"],
                          additive_reconciled=reconciled)
    if report is not None:
        report.update(country=country, table=table_name, wave=None,
                      site="Feature._harmonize_country_frame",
                      additive=bool(present))
        _record_grain_report(report)

    grouped = df.groupby(level=list(df.index.names), observed=True)
    if not present:
        return grouped.first()
    # `min_count=1`, not a bare `sum`: an all-NA group must stay NA rather than
    # become a fabricated 0.0.  Same reducer as country._normalize_dataframe_index
    # -- the two sites read one policy dict and must apply it identically (#323).
    agg = {c: (_sum_min_count_1 if c in present else "first") for c in df.columns}
    out = grouped.agg(agg)
    if "Price" in out.columns and {"Expenditure", "Quantity"} <= set(out.columns):
        out["Price"] = out["Expenditure"] / out["Quantity"].where(out["Quantity"] != 0)
    return out


def _harmonize_country_frame(
    df: pd.DataFrame, canonical_levels: list[str], country: str, table_name: str,
    fabricate_missing: bool = False,
    aliases: dict[str, str] | None = None,
    sentinels: dict[str, Any] | None = None,
    alignment_report: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Coerce a single country's frame toward the canonical shape before concat.

    Defensive net for cross-country assembly (GH #325): a stray extra index
    level or an all-NaN leaked column on ONE country otherwise makes
    ``pd.concat`` fall back to an unnamed object index of stringified tuples
    for the WHOLE feature.  This drops all-NaN columns and removes index
    levels that are not part of the canonical index (only when every
    canonical level is present, so legitimately-reduced frames are left
    alone).

    By default it never fabricates missing levels.  When ``fabricate_missing``
    (a per-feature opt-in, #506), any canonical level absent from this country's
    index is added as a ``pd.NA`` level so reduced-index countries share the
    full canonical shape and are KEPT (rather than modal-excluded in __call__).

    ``aliases`` (``Index Info: level_aliases``, GH #569) renames a country's own
    level names to the canonical ones FIRST -- ``plot_id`` -> ``plot``, ``crop``
    -> ``j`` -- so alignment can proceed by name.  ``sentinels``
    (``Index Info: missing_level_sentinels``) then gives the frame any declared
    canonical level it still lacks, promoting it from a COLUMN where the country
    has one and otherwise fabricating a constant sentinel.  Both run before the
    all-NaN column drop and the #498 reorder, so a promoted column is never
    dropped as "all NaN" and the reorder sees the finished level set.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df

    # Canonical NAME harmonization (#569), then canonical LEVEL-SET alignment.
    # Order matters: aliasing first, or `_align_to_canonical_levels` would see
    # `plot` as missing on a country that spells it `plot_id` and fabricate a
    # sentinel on top of a level that is right there.
    df = _rename_index_levels(df, aliases or {}, country, table_name)
    df = _align_to_canonical_levels(
        df, canonical_levels, sentinels or {}, country, table_name,
        report=alignment_report)

    # Drop columns that are entirely missing (e.g. a `date`/`v` column left
    # populated only on other countries).  Concat re-introduces them as NaN
    # where another country supplies values, so no information is lost.
    all_nan = [c for c in df.columns if df[c].isna().all()]
    if all_nan:
        warnings.warn(
            f"{table_name}: dropping all-NaN column(s) {all_nan} from {country} "
            "before cross-country concat"
        )
        df = df.drop(columns=all_nan)

    # Remove undeclared extra index levels so every country shares the same
    # MultiIndex names.  Only act when all canonical levels are present and
    # there is at least one extra (keeps single-country reductions intact).
    if canonical_levels and isinstance(df.index, pd.MultiIndex):
        names = list(df.index.names)
        # Opt-in (#506): fabricate any missing canonical level as a pd.NA index
        # level so a legitimately-reduced country keeps the full canonical shape
        # (and is KEPT, not modal-excluded).  E.g. interview_date single-visit
        # countries gain visit=NaN to stack with per-visit EHCVM countries.
        missing = [lvl for lvl in canonical_levels if lvl not in names]
        if fabricate_missing and missing:
            flat = df.reset_index()
            for lvl in missing:
                flat[lvl] = pd.NA
            df = flat.set_index(names + missing)
            names = list(df.index.names)
        have_all_canonical = all(lvl in names for lvl in canonical_levels)
        if have_all_canonical:
            # Put the canonical levels in canonical ORDER (then any extras).  Do
            # this even when there is no extra level to drop, so that the
            # positional set_names in __call__ aligns levels by MEANING, not
            # position: a country whose per-country index is correctly *named*
            # but *ordered* e.g. [i, t, v] would otherwise have its t/v/i values
            # scrambled under the canonical [t, v, i] labels (GH #498).
            ordered = [lvl for lvl in canonical_levels if lvl in names] + \
                      [n for n in names if n not in canonical_levels]
            if ordered != names:
                try:
                    df = df.reorder_levels(ordered)
                    names = ordered
                except (ValueError, TypeError):
                    pass
            # Remove undeclared extra index levels so every country shares the
            # same MultiIndex names (keeps single-country reductions intact).
            extra = [n for n in names if n not in canonical_levels]
            if extra and len(names) > len(extra):
                warnings.warn(
                    f"{table_name}: dropping extra index level(s) {extra} from "
                    f"{country} before cross-country concat"
                )
                df = df.droplevel(extra)
                if not df.index.is_unique:
                    df = _collapse_duplicate_index(df, table_name, country)

    return df


# Derived tables and the source table they require in data_scheme.yml
_DERIVED_SOURCE = {
    'household_characteristics': 'household_roster',
    'food_expenditures': 'food_acquired',
    'food_prices': 'food_acquired',
    'food_quantities': 'food_acquired',
}


def _discover_countries_for_table(table_name: str) -> list[str]:
    """Find all countries whose data_scheme.yml declares the given table.

    For derived tables (e.g. household_characteristics), discovers
    countries that have the source table (e.g. household_roster).
    """
    # If this is a derived table, look for its source instead
    lookup_name = _DERIVED_SOURCE.get(table_name, table_name)

    countries_dir = countries_root()
    result = []
    for entry in sorted(Path(countries_dir).iterdir()):
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        scheme_path = entry / "_" / "data_scheme.yml"
        if not scheme_path.exists():
            continue
        with open(scheme_path, "r", encoding="utf-8") as f:
            data = load_yaml(f)
        if not isinstance(data, dict):
            continue
        scheme = data.get("Data Scheme", {})
        if isinstance(scheme, dict) and (table_name in scheme or lookup_name in scheme):
            result.append(entry.name)
    return result


class Feature:
    """Assemble a single harmonized DataFrame for a table across countries.

    Parameters
    ----------
    table_name : str
        The table to load (e.g. ``'household_roster'``, ``'cluster_features'``).
    trust_cache : bool, optional
        If *True*, read existing cached parquets without validation (fast).
        Can be overridden per-call. Default ``False``.

    Examples
    --------
    >>> import lsms_library as ll
    >>> roster = ll.Feature('household_roster')
    >>> roster.countries          # which countries have this table
    >>> df = roster(['Mali', 'Uganda'])  # load specific countries
    >>> df = roster()                    # load all available countries
    >>> df = roster(trust_cache=True)    # fast read from cache
    """

    def __init__(self, table_name: str, trust_cache: bool = False) -> None:
        self.table_name = table_name
        self.trust_cache = trust_cache
        self._countries: list[str] | None = None

    def __repr__(self) -> str:
        return f"Feature({self.table_name!r})"

    def __getattribute__(self, name: str) -> Any:
        # Proxy the per-feature docstring from a representative Country method so
        # `Feature('food_expenditures').__doc__` mirrors
        # `Country(...).food_expenditures.__doc__` (GH #508).  Built lazily on
        # first access and cached; the class docstring is left intact.
        if name == "__doc__":
            cached = object.__getattribute__(self, "__dict__").get("_proxied_doc", _UNSET)
            if cached is not _UNSET:
                return cached
            doc = object.__getattribute__(self, "_build_proxied_doc")()
            object.__getattribute__(self, "__dict__")["_proxied_doc"] = doc
            return doc
        return object.__getattribute__(self, name)

    def _build_proxied_doc(self) -> str | None:
        """The underlying ``Country(...).<table>`` docstring, prefixed with the
        cross-country contract.  Falls back to the class docstring on any error."""
        try:
            from . import Country
            ctries = self.countries
            if not ctries:
                return type(self).__doc__
            base = getattr(Country(ctries[0]), self.table_name).__doc__ or ""
            return (
                f"Cross-country Feature for {self.table_name!r} -- mirrors "
                f"``Country(...).{self.table_name}`` with *waves* -> *countries* "
                f"(prepends a ``country`` index level; cross-country defaults "
                f"differ, e.g. ``currency='index'``).\n\n{base}"
            )
        except Exception:
            return type(self).__doc__

    @property
    def countries(self) -> list[str]:
        """Countries that declare this table in their data_scheme.yml."""
        if self._countries is None:
            self._countries = _discover_countries_for_table(self.table_name)
        return self._countries

    @property
    def columns(self) -> list[str]:
        """Required columns from the global data_info.yml for this table."""
        all_columns = _load_global_columns()
        table_cols = all_columns.get(self.table_name, {})
        return [
            col for col, meta in table_cols.items()
            if isinstance(meta, dict) and meta.get("required", False)
        ]

    def _attach_carried_attrs(self, result: pd.DataFrame,
                              captured: dict[str, dict[str, Any]]) -> None:
        """Re-attach every ``attrs`` carrier to the assembled frame (GH #873).

        Population (GH #603/#601), recall (GH #851) and derivations
        (``SkunkWorks/derived_values.org``) all die at the cross-country
        ``concat`` -- their records differ by design, so it lands in the ``{}``
        case -- and each ships its own ``merge_attrs``.  This walks
        :data:`_ATTRS_CARRIERS` and calls them; a fourth carrier is one entry
        there and no code here.

        Only the countries actually present in *result* contribute: a frame
        dropped by the modal-index-shape filter above is not in the answer, so
        it must not be in the answer's metadata or in the warning.

        Each carrier is attached under its own ``try``, so one broken record
        cannot suppress the others.  A metadata annotation must never break a
        data call -- the same rule ``population.attach`` obeys on the Country
        side -- but the failure is LOUD: a silently absent record is
        indistinguishable from a homogeneous pool, which is the one thing this
        must not look like.

        Only population reports at pooling time.  A warning for the other two
        was considered and declined on 2026-09-11 (``derived_values.org``
        §"No warning at pooling time"): at that level the ``attrs`` summary is
        itself the signal, and a warning would repeat it as noise.
        """
        try:
            kept = set(result.index.get_level_values("country").unique())
        except Exception as exc:
            # Deliberately broader than the (KeyError, ValueError) this used to
            # catch: that pair sat INSIDE an outer `except Exception` that also
            # covered this line, and the outer one is now per-carrier.  Never
            # let working out WHICH countries are kept break a data call --
            # but say so (the docstring's LOUD rule): falling back to every
            # captured country can over-report a dropped one.
            warnings.warn(
                f"{self.table_name}: could not read the country level to decide "
                f"which attrs records to re-attach ({type(exc).__name__}: "
                f"{exc}); re-attaching all {len(captured)} captured records")
            kept = set(captured)
        in_answer = [v for k, v in captured.items() if k in kept]

        for carrier in _ATTRS_CARRIERS:
            sources = [{k: src[k] for k in carrier.keys if k in src}
                       for src in in_answer]
            try:
                carrier.attach(result, sources)
            except Exception as exc:
                warnings.warn(
                    f"{self.table_name}: could not attach the {carrier.name} "
                    f"record ({type(exc).__name__}: {exc}). The result is "
                    f"unaffected, but df.attrs[{carrier.name!r}] may be "
                    f"incomplete"
                    + (" and no comparability warning was computed."
                       if carrier.name == "population" else ".")
                )
                continue
            if carrier.name != "population":
                continue
            try:
                report = _population.pool_report(
                    _population.records_from_attrs(result), self.table_name)
            except Exception as exc:
                warnings.warn(
                    f"{self.table_name}: could not attach the population record "
                    f"({type(exc).__name__}: {exc}). The result is unaffected, but "
                    f"df.attrs['population'] may be incomplete and no comparability "
                    f"warning was computed."
                )
                continue
            if report:
                warnings.warn(report,
                              _population.PopulationHeterogeneityWarning,
                              stacklevel=3)

    def __call__(self, countries: list[str] | None = None, trust_cache: bool | None = None,
                 currency: str | None = 'index', numeraire: str | None = None,
                 **kwargs: Any) -> pd.DataFrame:
        """Load and concatenate data across countries.

        Parameters
        ----------
        countries : list of str, optional
            Countries to include. Defaults to all available countries.
        trust_cache : bool, optional
            If *True*, read existing cached parquets without validation.
            Defaults to the instance-level setting from ``__init__``.
        currency : {'index', 'column', None}, optional
            Attach the ISO 4217 currency code to monetary tables.  Defaults to
            ``'index'`` here (cross-country stacking is exactly where mixed
            currencies are silently incommensurable) -- unlike single-country
            ``Country(...)`` calls, which default to ``None``.  A no-op for
            non-monetary tables (there is nothing to label).  See
            :func:`lsms_library.currency.attach_currency`.
        numeraire : str, optional
            Convert monetary columns to a comparable basis -- a target column of
            ``conversion_factors.org`` (e.g. ``'PPP-2017'``).  Supersedes
            ``currency`` (the converted frame is labelled with the target).  A
            no-op for non-monetary tables.  See
            :func:`lsms_library.conversion.convert`.
        **kwargs
            Any other per-feature option the underlying
            ``Country(...).<table>`` method accepts (e.g. ``market``,
            ``labels``, ``units``, ``age_cuts``) is forwarded to each country
            (GH #508 -- ``Feature`` mirrors the ``Country`` method interface,
            swapping *waves* -> *countries*).  ``market`` adds an ``m`` index
            level across countries.  A kwarg a country's method does not accept
            is ignored with a warning.

        Returns
        -------
        pd.DataFrame
            DataFrame with a ``country`` index level prepended.
        """
        from . import Country

        if currency is not None and currency not in {'index', 'column'}:
            raise ValueError(
                f"currency must be 'index', 'column', or None; got {currency!r}"
            )
        monetary = is_monetary_table(self.table_name)
        if numeraire is not None and monetary:
            from .conversion import conversion_targets
            if numeraire not in conversion_targets():
                raise ValueError(
                    f"Unknown numeraire {numeraire!r}; available: {conversion_targets()}"
                )
        effective_trust_cache = trust_cache if trust_cache is not None else self.trust_cache
        # Reject an unknown / mistyped table name with a helpful error instead of
        # silently returning an empty DataFrame (e.g. Feature('food_expenditure')).
        if not self.countries:
            import difflib
            sugg = difflib.get_close_matches(
                self.table_name, sorted(_all_known_features()), n=1
            )
            hint = f" Did you mean {sugg[0]!r}?" if sugg else ""
            raise ValueError(
                f"Unknown feature {self.table_name!r}: no country declares it and "
                f"it is not a runtime-derived table.{hint}"
            )
        targets = countries if countries is not None else self.countries
        frames: list[pd.DataFrame] = []
        canonical_levels = _canonical_index_levels(self.table_name)
        fabricate_missing = _fabricates_missing_levels(self.table_name)  # #506
        aliases = _level_aliases(self.table_name)                       # #569
        sentinels = _missing_level_sentinels(self.table_name)           # #569/#775
        # Structured record of every canonical level PROMOTED from a column or
        # FABRICATED as a sentinel, aggregated into one warning after the loop.
        alignment_report: dict[str, Any] = {}

        # numeraire supersedes currency; both are no-ops for non-monetary tables.
        # Either way the output carries a `currency` index level (relabelled to
        # the basis token for numeraire), so widen the canonical index to keep it.
        use_numeraire = numeraire if monetary else None
        pass_currency = None if use_numeraire else (currency if monetary else None)
        if (use_numeraire is not None or pass_currency == 'index') and canonical_levels:
            canonical_levels = canonical_levels + [CURRENCY_LEVEL]
        # `market` (forwarded below) is applied by Country._add_market_index,
        # which DROPS the `v` level and inserts an `m` level.  Mirror that in the
        # canonical level list -- drop `v`, add `m` -- so (a) _harmonize_country_frame
        # can still reorder each frame to canonical order (with `v` gone, requiring
        # every *original* canonical level to be present would skip the reorder),
        # and (b) `expected_names` matches the real per-country nlevels, letting the
        # GH#326 set_names restoration fire instead of leaving a trailing unnamed
        # level (issue #511: food_prices(market='Region') -> [..., 's', None]).
        if kwargs.get("market") is not None and canonical_levels:
            canonical_levels = [lvl for lvl in canonical_levels if lvl != "v"]
            if "m" not in canonical_levels:
                canonical_levels = canonical_levels + ["m"]

        # Countries dropped because they cannot honour a labels=X request (no
        # curated column).  Accumulated and reported ONCE after the loop, with a
        # df.attrs marker -- distinct from genuine per-country build failures.
        labels_unavailable: list[str] = []

        # The metadata records each country's frame arrived with: population
        # (GH #603), recall (GH #851) and derivations -- see _ATTRS_CARRIERS
        # above for the rule that makes this capture load-bearing rather than
        # belt-and-braces, and GH #873 for the two that used to be dropped.
        # Captured HERE, before _harmonize_country_frame / pd.concat, and
        # re-attached after assembly for the KEPT frames only -- so `attrs`
        # describes what the caller actually receives.
        captured_attrs: dict[str, dict[str, Any]] = {}

        for name in targets:
            try:
                c = Country(name, trust_cache=effective_trust_cache)
                method = getattr(c, self.table_name)
                # Build the per-country call: currency/numeraire with the monetary
                # gating above, plus any extra per-feature kwargs (market, labels,
                # units, age_cuts, ...) this country's generated method actually
                # accepts -- mirroring the Country(...).<table> interface (GH #508).
                call_kwargs: dict[str, Any] = {}
                if use_numeraire is not None:
                    call_kwargs["numeraire"] = use_numeraire
                elif pass_currency is not None:
                    call_kwargs["currency"] = pass_currency
                accepted = _method_parameters(method)
                for key, val in kwargs.items():
                    if key in accepted:
                        call_kwargs[key] = val
                    elif val is not None:
                        warnings.warn(
                            f"{self.table_name}: {name}'s method does not accept "
                            f"{key!r}; ignored for this country"
                        )
                df = method(**call_kwargs)
                if not isinstance(df, pd.DataFrame) or df.empty:
                    warnings.warn(
                        f"No data for {self.table_name} in {name}"
                    )
                    continue
                captured_attrs[name] = {
                    k: df.attrs[k] for k in _CARRIED_ATTRS_KEYS
                    if k in df.attrs
                }
                # Coerce toward the canonical shape so one country's stray
                # column / extra index level can't collapse the whole
                # concatenated index to object tuples (GH #325).
                df = _harmonize_country_frame(
                    df, canonical_levels, name, self.table_name, fabricate_missing,
                    aliases=aliases, sentinels=sentinels,
                    alignment_report=alignment_report,
                )
                # Prepend country as an index level
                df = pd.concat({name: df}, names=["country"])
                frames.append(df)
            except LabelUnavailableError:
                # Country curates no such label column -> degrade, don't conflate
                # with a build failure.  Reported once after the loop (Contract B).
                labels_unavailable.append(name)
                continue
            except Exception as e:  # broad catch intentional: surface per-country failures as warnings
                # Cross-country aggregation must not crash on one country's
                # implementation error; the warning carries the specific type.
                warnings.warn(
                    f"Failed to load {self.table_name} for {name}: {type(e).__name__}: {e}"
                )

        def _mark_labels_unavailable(result: pd.DataFrame, n_kept: int) -> pd.DataFrame:
            """Contract B: one aggregated warning + a df.attrs marker for the
            countries dropped because they curate no requested label column."""
            if not labels_unavailable:
                return result
            warnings.warn(
                f"{self.table_name}: labels={kwargs.get('labels')!r} unavailable "
                f"for {len(labels_unavailable)} country(ies) {labels_unavailable} "
                f"-- they curate no such label column, or lack the target "
                f"entirely, and were dropped from the assembly (kept {n_kept}). "
                f"Add the column or pass a country subset to silence this."
            )
            result.attrs['labels_unavailable'] = list(labels_unavailable)
            return result

        if not frames:
            return _mark_labels_unavailable(pd.DataFrame(), 0)

        # pandas cannot stack frames whose index DEPTH/NAMES differ into a named
        # MultiIndex -- it falls back to an unnamed object index, collapsing the
        # WHOLE feature (issue #512: EthiopiaRHS's documented (t,i) reduced assets
        # stacked with item-level (t,i,j); also surfaces under labels='Aggregate'
        # when most countries KeyError-drop and a j-less survivor remains).  One
        # shape has to win; the divergent frame(s) are excluded with a loud, named
        # warning and stay available via Country(name).<table>().
        #
        # WHICH shape wins is _select_kept_shape's job: the declared canonical
        # index when a frame has it, else the historical modal rule with an
        # order-independent tie-break (GH #775).
        kept, dropped, winner = _select_kept_shape(frames, canonical_levels)
        if dropped:
            dropped_names = [f.index.get_level_values("country")[0]
                             for f in dropped if len(f)]
            canonical_shape = (tuple(["country"] + list(canonical_levels))
                               if canonical_levels else None)
            why = ("the canonical index declared in data_info.yml"
                   if winner == canonical_shape else
                   "the modal index shape among the frames built")
            warnings.warn(
                f"{self.table_name}: excluded {len(dropped)} country frame(s) "
                f"{dropped_names} with a divergent index shape from the "
                f"cross-country assembly (kept {why}: {list(winner)}); "
                f"stacking heterogeneous index depths would collapse the whole "
                f"result to an unnamed index. Access the excluded data via "
                f"Country(name).{self.table_name}()."
            )
        frames = kept

        result = pd.concat(frames)
        n_kept = result.index.get_level_values("country").nunique()

        # Canonical-index alignment, reported ONCE (Contract B shape).  Only for
        # the countries actually KEPT -- a frame excluded above is not in the
        # answer, so it must not be in the answer's metadata (same rule
        # `_attach_carried_attrs` obeys).
        if alignment_report:
            try:
                kept_names = set(result.index.get_level_values("country").unique())
            except (KeyError, ValueError):
                kept_names = set(targets)
            record = {
                kind: {c: v for c, v in per_country.items() if c in kept_names}
                for kind, per_country in alignment_report.items()
            }
            record = {k: v for k, v in record.items() if v}
            if record:
                result.attrs['canonical_alignment'] = record
                promoted = record.get('promoted', {})
                fabricated = record.get('fabricated', {})
                bits = []
                if promoted:
                    bits.append(
                        f"promoted a canonical level carried as a COLUMN into "
                        f"the index for {sorted(promoted)} "
                        f"(nulls filled with the declared sentinel: {promoted})")
                if fabricated:
                    bits.append(
                        f"added constant sentinel level(s) for countries whose "
                        f"instrument does not record them: {fabricated}")
                collisions = record.get('promotion_collisions', {})
                if collisions:
                    bits.append(
                        f"PROMOTION COLLIDED for {sorted(collisions)}: the "
                        f"promoted column held both nulls and the literal "
                        f"sentinel {collisions}, so rows collapsed onto an "
                        f"existing key -- this country contributes FEWER rows "
                        f"than Country(name).{self.table_name}() returns; the "
                        f"collapse was audited (see any GrainCollapseWarning)")
                tail = (". See df.attrs['canonical_alignment']."
                        if collisions else
                        ". No rows were added, removed or collapsed; see "
                        "df.attrs['canonical_alignment'].")
                warnings.warn(
                    f"{self.table_name}: aligned country frames to the canonical "
                    f"index declared in data_info.yml -- " + "; ".join(bits) + tail
                )

        # GH #603/#601 -- SURFACE, then WARN.  Never fence: every country that
        # got this far is in `result`, and the population record is metadata
        # about it, not a filter on it.  #603 proposed excluding `specialized`
        # frames by default and @ligon declined; a default that silently drops
        # data is the same disease as one that silently pools it.  Since #873
        # the same hook carries `recall` and `derivations`, which have no
        # warning of their own by decision.
        self._attach_carried_attrs(result, captured_attrs)

        # GH #326: pd.concat can leave the (structurally-consistent) index
        # levels UNNAMED, forcing callers to index positionally instead of
        # `groupby('country')`.  When the level count matches the canonical
        # shape (country + declared levels), restore the names — the per-country
        # frames were already coerced to canonical order by
        # _harmonize_country_frame above.  The nlevels-mismatch (genuinely
        # heterogeneous) case is left to the warning below.
        expected_names = ["country"] + canonical_levels
        if (result.index.nlevels == len(expected_names)
                and list(result.index.names) != expected_names):
            result.index = result.index.set_names(expected_names)

        # Surface (rather than silently return) the pathological case where
        # heterogeneous per-country indices left an unnamed level -- either a full
        # fallback to an unnamed object index (GH #325, names == [None]) or a
        # PARTIAL collapse (e.g. ['country', None, None]) that previously slipped
        # through silently (issue #512, labels='Aggregate' variant).
        if len(frames) > 1 and None in list(result.index.names):
            shapes = {
                f.index.get_level_values(0)[0] if len(f) else "?":
                    list(f.index.names)
                for f in frames
            }
            warnings.warn(
                f"{self.table_name}: cross-country index collapsed to an "
                f"unnamed object index; per-country index names differ: {shapes}"
            )

        return _mark_labels_unavailable(result, n_kept)
