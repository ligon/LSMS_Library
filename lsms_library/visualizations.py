"""Standard charts over harmonized LSMS tables, tuned for a notebook.

Design notes, because the choices are deliberate
------------------------------------------------
**Colour never encodes sex.**  In a population pyramid the mirrored layout
already encodes sex unambiguously -- left is one sex, right is the other --
so spending the colour channel on it says nothing the geometry has not
already said.  Colour is therefore free for the dimension that *does* carry
information: the conditioning variable (``by=``).  Unconditioned, a pyramid
is drawn in a single ink.  (The conventional blue/pink is also editorially
loaded in a way survey demography does not need.)

**A chart states the population it represents.**  ``Country.population``
records, per wave, what the survey's own documentation says its sample
covers -- and those universes differ: Ethiopia's ESS wave 1 was designed for
rural areas and small towns.  A pyramid captioned only "Ethiopia 2011-12"
invites a reader to treat it as national.  The caption is not decoration; it
is the difference between a chart and a misleading chart.

**The weighting basis is stated, never implied.**  Weights are normalised to
within-wave mean 1 at API time -- but that mean is taken over *households*,
while a roster is one row per *person*.  The weighted person-total is
therefore ``sum(size_h * w_h)``, which equals the headcount only when weight
and household size are uncorrelated, and they are not: GhanaLSS 2016-17 has
59,864 people but a weighted total of 54,421.5, because larger households
carry smaller weights.  So weighting moves both the shape *and* the level,
and neither is announced by the picture.  The subtitle always says which
basis produced it.

**One wave per pyramid.**  Pooling waves pools universes (see above), so a
multi-wave frame is not silently concatenated: the most recent wave is drawn
and the choice is reported.
"""
from __future__ import annotations

import warnings
from typing import Any

import pandas as pd

__all__ = ["population_pyramid", "coordinate_map", "PALETTE"]

#: The chart palette.  Teal/ochre rather than blue/pink: colour distinguishes
#: the ``by=`` groups, never the sexes, and the pair stays separable under the
#: common forms of colour blindness.
PALETTE = {
    "ink": "#23313A",      # type and axis: slate with real hue, not a tinted black
    "rule": "#CFD6DA",     # hairlines
    "bar": "#3D6B7D",      # the single ink, and the first ``by=`` group
    "alt": "#B08A3E",      # the second ``by=`` group
}

#: Preferred faces first; matplotlib falls through to whatever exists.  Named
#: as a stack rather than pinned so the chart improves on a machine with good
#: fonts instead of failing on one without them.
_FONT_STACK = ["Inter", "Source Sans Pro", "Helvetica Neue", "Helvetica",
               "Arial", "DejaVu Sans"]

_MISSING_MPL = (
    "population_pyramid() needs matplotlib, which is not installed.\n"
    "matplotlib is declared in this project's `test` dependency group, not "
    "its main dependencies, so a plain install does not pull it in.\n"
    "Install it with:  pip install matplotlib"
)


def _require_pyplot():
    """Import pyplot lazily, with an error that says what to do about it."""
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise ImportError(_MISSING_MPL) from exc
    return plt


def _resolve_roster(data: Any, wave: str | None) -> tuple[pd.DataFrame, str, str | None]:
    """Return ``(roster, label, country_name)`` for a Country, a name, or a frame."""
    from .country import Country

    country = None
    if isinstance(data, str):
        country = Country(data)
    elif isinstance(data, Country):
        country = data

    if country is not None:
        roster = country.household_roster()
        name = country.name
    else:
        roster = data
        name = None
        if roster is None or not isinstance(roster, pd.DataFrame):
            raise TypeError(
                "population_pyramid() takes a Country, a country name, or a "
                f"household_roster DataFrame; got {type(data).__name__}"
            )

    if "t" in roster.index.names:
        waves = list(pd.unique(roster.index.get_level_values("t").dropna()))
        if wave is None and len(waves) > 1:
            wave = sorted(waves)[-1]
            warnings.warn(
                f"{name or 'roster'} covers {len(waves)} waves and their sampled "
                f"populations may differ; drawing the most recent ({wave}). "
                "Pass wave= to choose another.",
                stacklevel=3,
            )
        elif wave is None:
            wave = waves[0] if waves else None
        if wave is not None:
            roster = roster.xs(wave, level="t", drop_level=False)

    label = f"{name} {wave}".strip() if name else (str(wave) if wave else "roster")
    return roster, label, name


def _attach(roster: pd.DataFrame, country_name: str | None,
            weights: bool | str, by: str | None) -> tuple[pd.DataFrame, str]:
    """Join ``weight`` / the ``by=`` column from ``sample()`` onto the roster.

    ``Rural``, ``strata`` and the weights live on ``sample()`` at ``(i, t)``
    grain, not on the roster -- so conditioning and weighting need the same
    join, which is why they are done together here.
    """
    from .country import Country

    df = roster.reset_index()
    basis = "unweighted"
    wanted = [c for c in ([by] if by else []) if c not in df.columns]
    need_w = bool(weights) and not (isinstance(weights, str) and weights in df.columns)

    if country_name and (wanted or need_w):
        try:
            smp = Country(country_name).sample().reset_index()
        except Exception as exc:
            warnings.warn(f"sample() unavailable for {country_name} ({exc}); "
                          "drawing unweighted counts.", stacklevel=3)
            smp = None
        if smp is not None:
            keys = [k for k in ("i", "t") if k in df.columns and k in smp.columns]
            cols = keys + [c for c in (wanted + (["weight"] if need_w else []))
                           if c in smp.columns]
            if keys and len(cols) > len(keys):
                df = df.merge(smp[cols].drop_duplicates(keys), on=keys, how="left")

    if weights is True:
        if "weight" in df.columns and df["weight"].notna().any():
            df["_w"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
            basis = "weighted by the survey's sampling weights"
        else:
            df["_w"] = 1.0
            warnings.warn("no sampling weights available; drawing unweighted counts.",
                          stacklevel=3)
    elif isinstance(weights, str):
        if weights not in df.columns:
            raise KeyError(f"weights={weights!r} is not a column of this table")
        df["_w"] = pd.to_numeric(df[weights], errors="coerce").fillna(0.0)
        basis = f"weighted by {weights}"
    else:
        df["_w"] = 1.0

    if by and by not in df.columns:
        raise KeyError(
            f"by={by!r} is not available; it is neither a roster column nor a "
            "column of sample()."
        )
    return df, basis


def _universe(country_name: str | None, wave: str | None) -> str | None:
    """One line saying what this sample represents, from the population record."""
    if not (country_name and wave):
        return None
    try:
        from .country import Country
        rec = Country(country_name).population.get(wave)
    except Exception:
        return None
    if rec is None:
        return None
    tag = getattr(rec, "universe_tag", None)
    if not tag or tag == "unrecorded":
        return None
    tag = str(tag)
    # "national-claimed" means no document names the population; saying
    # "national claimed" reads as a typo, so keep the qualifier parenthetical.
    if tag.endswith("-claimed"):
        text = f"{tag[:-len('-claimed')].replace('-', ' ')} (claimed)"
    else:
        text = tag.replace("-", " ")
    conf = getattr(rec, "confidence", None)
    tail = f", stated with {conf} confidence" if conf and conf != "high" else ""
    return f"Represents: {text}{tail}"


def population_pyramid(data, wave=None, *, weights=True, by=None, bin_width=5,
                       max_age="auto", ghost=None, ax=None, title=None,
                       colors=None):
    """Draw an age-sex pyramid for one survey wave.

    Parameters
    ----------
    data : Country, str, or DataFrame
        A country, its name, or a ``household_roster`` frame.
    wave : str, optional
        Which wave to draw.  Defaults to the most recent, with a warning when
        the frame holds more than one -- their sampled populations may differ.
    weights : bool or str, default True
        ``True`` uses the survey's sampling weights, falling back to counts
        with a warning when a country has none.  ``False`` draws counts.  A
        string names a column to weight by.
    ghost : bool or str, optional
        Draw a second weighting as a hairline outline over the filled bars,
        for comparison.  Takes the same values as ``weights``, so the common
        case is ``weights=True, ghost=False`` -- "weighted, with unweighted
        ghosted".  The two series differ in level as well as shape (see the
        module docstring), and both differences are far easier to read as one
        overlay than as two charts the eye must hold in memory.
    by : str, optional
        Split the pyramid on a category, e.g. ``by='Rural'``.  Looked up on
        the roster, then on ``sample()``.  This is what colour encodes.
    bin_width : int, default 5
        Age-band width in years.  Pass ``1`` to expose age heaping -- LSMS
        ages pile onto multiples of five, which a 5-year band hides.
    max_age : int or "auto", default "auto"
        Age at which the closing ``N+`` band starts.  ``"auto"`` puts it just
        above the oldest band that would still be **visible** when drawn: a
        band thinner than about a pixel adds a row of white space and stretches
        the vertical scale for nobody.  Malawi records ages up to 119 in bands
        of four people (0.01% of its largest band); drawing to the data maximum
        would add a dozen empty rows.  The test is geometric, not a tuned
        percentage, so it travels to any country and adapts to the figure size
        and dpi it is actually drawn at.  Pass an integer to fix it.
    ax : matplotlib Axes, optional
    title, colors : optional overrides.

    Returns
    -------
    matplotlib.axes.Axes
    """
    plt = _require_pyplot()
    import numpy as np

    pal = {**PALETTE, **(colors or {})}
    roster, label, cname = _resolve_roster(data, wave)
    wave_used = label.split()[-1] if cname else wave
    df, basis = _attach(roster, cname, weights, by)

    ghost_basis = None
    if ghost is not None:
        if ghost is False:
            df["_g"] = 1.0
            ghost_basis = "unweighted"
        elif ghost is True:
            if "weight" in df.columns and df["weight"].notna().any():
                df["_g"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
                ghost_basis = "weighted"
            else:
                df["_g"] = 1.0
                ghost_basis = "unweighted"
        elif isinstance(ghost, str):
            if ghost not in df.columns:
                raise KeyError(f"ghost={ghost!r} is not a column of this table")
            df["_g"] = pd.to_numeric(df[ghost], errors="coerce").fillna(0.0)
            ghost_basis = f"weighted by {ghost}"
        else:
            raise TypeError("ghost= takes True, False, a column name, or None")

    df = df[df["Sex"].isin(["M", "F"])].copy()
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    df = df[df["Age"].notna() & (df["Age"] >= 0)]
    if df.empty:
        raise ValueError(f"no usable Age/Sex rows for {label}")

    # Created before the auto rule below, which measures the axes in pixels.
    if ax is None:
        _, ax = plt.subplots(figsize=(7.2, 5.6), dpi=130)
    fig = ax.figure

    raw = (np.floor(df["Age"] / bin_width) * bin_width).astype(int)
    oldest = int(np.ceil(df["Age"].max()))

    if isinstance(max_age, str):
        if max_age != "auto":
            raise ValueError(f"max_age must be an int or 'auto'; got {max_age!r}")
        # A band earns its own row only if it would render at least ~1.5px
        # wide.  Half-axis is span*1.12 + gutter(=0.115*span), so the full
        # axis spans about 2.47*span data units; converting a pixel budget
        # through that ratio makes the rule independent of the country, the
        # weighting and the figure size.
        per = (df.assign(_b=raw).groupby(["_b", "Sex"])["_w"].sum()
                 .unstack(fill_value=0.0))
        widest = float(per.to_numpy().max()) if per.size else 0.0
        try:
            ax_px = float(ax.get_window_extent().width)
        except Exception:                     # no renderer yet
            ax_px = float(fig.get_size_inches()[0] * fig.dpi) * 0.78
        thresh = 1.5 * (2.47 * widest) / max(ax_px, 1.0)
        # The test is whether the CLOSING band would be visible -- not whether
        # the last individual band is.  Those differ, and the difference shows:
        # closing just above the last visible band leaves a labelled row with
        # no bar in it (GhanaLSS 2016-17 drew an empty "100+"), which reads as
        # a rendering fault.  The closing band accumulates everything at or
        # above its edge, so its total falls monotonically as the edge rises;
        # take the highest edge that still clears the threshold.
        cand = sorted((int(b) for b in per.index), reverse=True)
        top = oldest
        for e in cand:
            closing = per.loc[[b for b in per.index if int(b) >= e]].sum()
            if float(closing.max()) >= thresh:
                top = e
                break
        # never collapse the chart to a stub, never exceed the data
        max_age = int(min(max(top, 6 * bin_width), max(oldest, bin_width)))

    edges = list(range(0, max_age + bin_width, bin_width))
    df["_band"] = np.minimum(raw, max_age)

    groups = [None]
    if by is not None:
        groups = [g for g in pd.unique(df[by].dropna())][:2]
        if len(groups) < 2:
            groups = groups or [None]

    def totals(sub, col="_w"):
        s = sub.groupby("_band")[col].sum()
        return np.array([float(s.get(e, 0.0)) for e in edges])

    span = max(totals(df[df.Sex == "M"]).max(), totals(df[df.Sex == "F"]).max())
    if ghost is not None:
        span = max(span,
                   totals(df[df.Sex == "M"], "_g").max(),
                   totals(df[df.Sex == "F"], "_g").max())
    span = span or 1.0
    gutter = span * 0.115          # centre channel that carries the age labels
    h = bin_width * (0.42 if len(groups) > 1 else 0.82)

    for gi, g in enumerate(groups):
        sub = df if g is None else df[df[by] == g]
        colour = pal["bar"] if gi == 0 else pal["alt"]
        off = 0.0 if len(groups) == 1 else (bin_width * 0.22) * (1 if gi else -1)
        for sex, sign in (("M", -1), ("F", 1)):
            vals = totals(sub[sub.Sex == sex])
            ax.barh([e + bin_width / 2 + off for e in edges],
                    [sign * v for v in vals], height=h,
                    left=[sign * gutter] * len(edges),
                    color=colour, edgecolor="none",
                    label=(str(g) if (g is not None and sex == "F") else None),
                    zorder=2)

    # Thin the spine labels when the bands are dense -- at bin_width=1 all
    # 60+ of them collide into an unreadable smear.  The top band is always
    # labelled because it is the open-ended one.
    if ghost is not None:
        # A hairline outline rather than a second translucent fill: two
        # alpha-blended fills make a third colour in the overlap and the
        # reader must decode which layer is which.  An outline is
        # unambiguous -- where it hugs the bar edge the two agree, where it
        # stands proud or falls short you read direction and size at once --
        # and it spends no colour, keeping that channel for `by=`.
        for sex, sign in (("M", -1), ("F", 1)):
            vals = totals(df[df.Sex == sex], "_g")
            ax.barh([e + bin_width / 2 for e in edges],
                    [sign * v for v in vals], height=bin_width * 0.86,
                    left=[sign * gutter] * len(edges),
                    facecolor="none", edgecolor=pal["ink"], linewidth=1.3,
                    zorder=4)

    step = 1 if len(edges) <= 20 else max(1, int(round(5 / bin_width)))
    for k, e in enumerate(edges):
        if e < max_age and k % step:
            continue
        if e >= max_age:
            txt = f"{e}+"
        elif bin_width == 1:
            txt = f"{e}"                      # "58", not "58–58"
        else:
            txt = f"{e}–{e + bin_width - 1}"
        ax.text(0, e + bin_width / 2, txt, ha="center", va="center",
                fontsize=7.6, color=pal["ink"], zorder=3)

    ax.axvline(0, color=pal["rule"], lw=0.8, zorder=1)
    lim = span * 1.12 + gutter
    ax.set_xlim(-lim, lim)
    ax.set_ylim(-bin_width * 0.4, max_age + bin_width * 1.1)
    ax.set_yticks([])
    ticks = [t for t in ax.get_xticks() if abs(t) > gutter and abs(t) <= lim]
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{abs(t):,.0f}" for t in ticks])
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(pal["rule"])
    ax.tick_params(axis="x", colors=pal["ink"], labelsize=8, length=3,
                   color=pal["rule"])

    # Below the tick labels, in axes coordinates: at data coordinates these
    # sat on the same row as the x tick labels and collided with them.
    for frac, word in ((0.25, "men"), (0.75, "women")):
        ax.annotate(word, xy=(frac, 0), xycoords="axes fraction",
                    xytext=(0, -26), textcoords="offset points",
                    ha="center", va="top", fontsize=9.5, color=pal["ink"])

    n = len(df)
    ax.set_title(title or label, loc="left", fontsize=12.5, color=pal["ink"],
                 pad=16, fontweight="semibold")
    sub_bits = [f"{n:,} people", basis]
    if ghost_basis is not None:
        sub_bits.append(f"outline: {ghost_basis}")
    if bin_width == 1:
        sub_bits.append("single-year bands")
    if oldest > max_age + bin_width:
        # The closing band holds them, but it can be too thin to see; saying
        # so is the difference between "nobody older" and "too few to draw".
        sub_bits.append(f"oldest recorded {oldest}")
    ax.annotate(", ".join(sub_bits), xy=(0, 1), xycoords="axes fraction",
                xytext=(0, 6), textcoords="offset points",
                fontsize=9, color=pal["ink"], alpha=0.75)

    caption = _universe(cname, wave_used)
    if caption:
        fig.text(0.005, -0.005, caption, fontsize=7.8, color=pal["ink"],
                 alpha=0.7, ha="left", va="top")

    if by is not None and len(groups) > 1:
        # A legend titled "Rural" above entries "Rural"/"Urban" reads as an
        # error.  Title it only when the column name is not itself a value.
        names = {str(g) for g in groups}
        leg = ax.legend(title=(None if by in names else by), frameon=False,
                        fontsize=8.5, loc="upper right", title_fontsize=8.5)
        if leg.get_title() is not None:
            leg.get_title().set_color(pal["ink"])

    for item in ([ax.title] + ax.get_xticklabels()):
        item.set_fontfamily(_FONT_STACK)
    fig.tight_layout()
    return ax


# ---------------------------------------------------------------------------
# Maps
# ---------------------------------------------------------------------------

#: Basemap for the interactive map: country shapes, borders and place names.
#: OpenStreetMap because it needs NO API KEY.  CartoDB positron is prettier and
#: was the first choice, but folium now warns "CartoDB tiles now require an API
#: key" -- the tile URL is still emitted, so the map renders markers on a BLANK
#: background and looks merely empty rather than broken.  A basemap that
#: silently fails is worse than a plainer one that works.
_DEFAULT_TILES = "OpenStreetMap"

_MISSING_FOLIUM = (
    "coordinate_map(interactive=True) needs folium, which is not installed.\n"
    "folium is pure Python (leaflet + jinja2) and renders inline in Jupyter.\n"
    "Install it with:  pip install folium\n"
    "Or pass interactive=False for a static matplotlib scatter."
)


def _radius_by_area(values, r_max=14.0, r_floor=2.0, scale="area"):
    """Marker radii, and how many were floored.

    Returns ``(radii, n_floored)``.

    ``scale='area'`` makes AREA proportional to the value, i.e. radius as its
    square root.  Radius-proportional symbols are the standard lie of this
    chart type -- Uganda 2013-14's cluster weights span 197x, which drawn as a
    radius reads as ~38,000x by area.

    ``scale='log'`` makes area proportional to ``log1p(value)`` instead.  It
    is not a prettier version of the same thing: it deliberately understates
    differences, so a 197x range reads as 5x, and the caption must say so.

    **The floor is the honest problem with ``'area'``, and the caller reports
    it.**  Spanning 197x within a 14px budget puts the smallest markers at 1px,
    so a floor is needed to keep them visible -- and the floor breaks exactly
    the proportionality the mode promises.  Measured on Uganda: 188 of 620
    markers (30%) are floored, overstating their area by up to 4x.  Hence the
    count comes back with the radii rather than being swallowed: a caption
    claiming "area proportional to weight" while it is untrue for a third of
    the markers is the kind of quiet falsehood this module exists to avoid.
    ``'log'`` needs no floor at all (min radius 2.8px on the same data).
    """
    import numpy as np

    v = np.asarray(values, dtype=float)
    if scale not in ("area", "log"):
        raise ValueError(f"scale must be 'area' or 'log'; got {scale!r}")
    if scale == "log":
        v = np.log1p(np.clip(v, 0, None))
    top = np.nanmax(v) if v.size else 1.0
    if not np.isfinite(top) or top <= 0:
        return np.full(v.shape, r_floor), 0
    true_r = r_max * np.sqrt(np.clip(v, 0, None) / top)
    n_floored = int((true_r < r_floor).sum())
    return np.maximum(r_floor, true_r), n_floored


def _apply_where(df, where):
    """Restrict ``df`` to the rows ``where`` selects; return ``(df, text)``.

    ``where`` is a ``{column: value}`` / ``{column: [values]}`` dict (all
    conditions must hold) or a callable ``df -> boolean mask``.  A condition
    that selects nothing raises with the values the column actually holds --
    a case or spelling mismatch (Guinea-Bissau spells its regions three ways
    across two tables, GH #811) should be visible, not an empty map.
    """
    if where is None:
        return df, None
    if callable(where):
        mask = pd.Series(where(df), index=df.index).astype(bool)
        text = getattr(where, "__name__", "filter")
        if text == "<lambda>":
            text = "filter"
    elif isinstance(where, dict):
        mask = pd.Series(True, index=df.index)
        parts = []
        for col, val in where.items():
            if col not in df.columns:
                raise KeyError(f"where: {col!r} is not a column of this table; "
                               f"columns: {', '.join(map(str, df.columns))}")
            vals = list(val) if isinstance(val, (list, tuple, set)) else [val]
            hit = df[col].isin(vals)
            if not hit.any():
                have = sorted(map(str, pd.unique(df[col].dropna())))[:20]
                raise ValueError(f"where: {col} in {vals!r} matches no rows; "
                                 f"{col} holds: {', '.join(have)}")
            mask &= hit
            parts.append(f"{col} = {vals[0]}" if len(vals) == 1
                         else f"{col} in {{{', '.join(map(str, vals))}}}")
        text = ", ".join(parts)
    else:
        raise TypeError("where= takes a {column: value} dict or a callable "
                        f"df -> mask; got {type(where).__name__}")
    out = df[mask].copy()
    if out.empty:
        raise ValueError(f"where ({text}) selects no rows")
    return out, text


def _cluster_frame(data, wave, size, where=None):
    """Build ``(frame, label, country, dropped)`` for the cluster leading case.

    ``where`` is applied after the ``size`` join and before the coordinate
    check, so ``dropped`` counts the selected clusters that lack coordinates,
    not the whole wave's.
    """
    from .country import Country

    country = Country(data) if isinstance(data, str) else data
    if not isinstance(country, Country):
        return data, "coordinates", None, 0

    cf = country.cluster_features()
    if "t" in cf.index.names:
        waves = list(pd.unique(cf.index.get_level_values("t").dropna()))
        if wave is None and len(waves) > 1:
            wave = sorted(waves)[-1]
            warnings.warn(f"{country.name} has {len(waves)} waves; mapping the "
                          f"most recent ({wave}). Pass wave= to choose another.",
                          stacklevel=3)
        elif wave is None:
            wave = waves[0] if waves else None
        if wave is not None:
            cf = cf.xs(wave, level="t", drop_level=False)

    df = cf.reset_index()

    if size is not None and size not in df.columns:
        try:
            smp = country.sample().reset_index()
            if wave is not None and "t" in smp.columns:
                smp = smp[smp["t"] == wave]
            if size in smp.columns and "v" in smp.columns and "v" in df.columns:
                agg = smp.groupby("v")[size].sum()
                df = df.merge(agg.rename(size), left_on="v", right_index=True,
                              how="left")
        except Exception as exc:
            warnings.warn(f"could not join {size!r} from sample(): {exc}",
                          stacklevel=3)

    df, where_text = _apply_where(df, where)
    have = df[["Latitude", "Longitude"]].notna().all(axis=1) if \
        {"Latitude", "Longitude"} <= set(df.columns) else pd.Series(False, index=df.index)
    dropped = int((~have).sum())
    df = df[have].copy()
    title = f"{country.name} {wave or ''}".strip()
    if where_text:
        title += f" \u00b7 {where_text}"
    if df.empty:
        raise ValueError(f"{title}: no selected cluster has coordinates")
    return df, title, country.name, dropped


def coordinate_map(data, wave=None, *, size=None, where=None, lat="Latitude",
                   lon="Longitude", label=None, interactive=True, scale="area",
                   colors=None, tiles=None):
    """Map point coordinates, optionally sizing each marker by a third variable.

    The leading case is survey clusters sized by the sampling weight they
    carry::

        coordinate_map('Uganda', wave='2013-14', size='weight')

    Given a ``Country`` (or its name) the cluster coordinates come from
    ``cluster_features`` and ``size`` is summed from ``sample()`` over each
    cluster.  Given a DataFrame, ``lat``/``lon``/``size`` name its columns and
    nothing is joined.

    Restrict the map with ``where``: one region, one stratum, rural clusters
    only::

        coordinate_map('Guinea-Bissau', size='weight', where={'Region': 'bafata'})
        coordinate_map('Uganda', wave='2013-14', size='weight',
                       where=lambda df: df.Rural == 'Rural')

    Parameters
    ----------
    where : dict or callable, optional
        ``{column: value}`` or ``{column: [values]}`` -- every condition must
        hold -- or a callable taking the assembled frame and returning a
        boolean mask.  For a ``Country`` the frame carries the wave's
        ``cluster_features`` columns (``Region``, ``Rural``, ...) plus the
        summed ``size``; for a DataFrame, its own columns.  A condition that
        matches nothing raises and lists the values the column holds, so a
        spelling mismatch is loud.  The filter is named in the title, and the
        no-coordinates count is for the selected clusters only.
    size : str, optional
        Column whose value sets marker AREA (not radius -- see
        :func:`_radius_by_area`).  ``None`` draws uniform markers.
    scale : {'area', 'log'}, default 'area'
        ``'area'`` makes marker area proportional to ``size``.  ``'log'``
        uses ``log1p(size)`` instead, which compresses a wide range into a
        legible one -- Uganda's 197x weight span reads as 5x -- and needs no
        size floor.  It understates differences by construction, so the
        caption says which was used.
    interactive : bool, default True
        Render a pan/zoom Leaflet map via ``folium``, which displays inline in
        Jupyter and can be saved as standalone HTML.  ``False`` draws a static
        matplotlib scatter and needs no extra dependency.
    label : str, optional
        Column shown in a marker's tooltip (interactive only).  Defaults to
        the cluster id ``v`` when present.

    Returns
    -------
    ``folium.Map`` when ``interactive``, else a matplotlib ``Axes``.

    Notes
    -----
    **Coordinates are cluster fixes, not household locations.**  The published
    GPS is one point per cluster, stamped onto each of its households; it was
    never per-household.  Mapping households would draw the same point many
    times and imply a precision the data does not carry.  Survey coordinates
    are also commonly offset before publication to protect respondents, so
    treat position as approximate.

    **Clusters without coordinates are counted and reported**, never dropped
    silently: Uganda 2013-14 publishes coordinates for 619 of 706 clusters, so
    a map that said nothing would omit 12% of the sample without a trace.
    """
    pal = {**PALETTE, **(colors or {})}
    df, title, cname, dropped = _cluster_frame(data, wave, size, where)
    if not isinstance(df, pd.DataFrame):
        raise TypeError("coordinate_map() takes a Country, a country name, or "
                        f"a DataFrame; got {type(data).__name__}")
    for col in (lat, lon):
        if col not in df.columns:
            raise KeyError(f"{col!r} is not a column of this table")
    if cname is None:  # DataFrame path: _cluster_frame did not see ``where``
        df, where_text = _apply_where(df, where)
        if where_text:
            title += f" \u00b7 {where_text}"
        dropped = int((df[lat].isna() | df[lon].isna()).sum())
    df = df[df[lat].notna() & df[lon].notna()].copy()
    if df.empty:
        raise ValueError("no rows with usable coordinates")

    n_floored = 0
    if size is not None and size in df.columns:
        vals = pd.to_numeric(df[size], errors="coerce").fillna(0.0)
        radii, n_floored = _radius_by_area(vals, scale=scale)
    else:
        vals, radii = None, [5.0] * len(df)

    if label is None:
        label = "v" if "v" in df.columns else None

    note = [f"{len(df):,} points"]
    if dropped:
        note.append(f"{dropped} cluster(s) without coordinates, not shown")
    if vals is not None:
        note.append(f"marker area ∝ {size}" if scale == "area"
                    else f"marker area ∝ log({size})")
        if n_floored:
            # Say it rather than let the caption over-claim for these markers.
            note.append(f"{n_floored} at the minimum size, area overstated")
    caption = "; ".join(note)

    if not interactive:
        plt = _require_pyplot()
        import numpy as np
        _, ax = plt.subplots(figsize=(6.4, 6.4), dpi=130)
        ax.scatter(df[lon], df[lat], s=[3.14 * r * r for r in radii],
                   facecolor=pal["bar"], edgecolor="white", linewidth=0.4,
                   alpha=0.75)
        # One degree of longitude is cos(latitude) degrees of latitude, so an
        # "equal" aspect is only right on the equator and stretches every other
        # map east-west.  Uganda sits at ~1 N so the error is invisible there;
        # Niger at ~17 N is already 4.5% too wide, and it grows with latitude.
        # This is the plate-carree correction, not a projection: the static
        # fallback is a scatter of coordinates, and says so.
        mid_lat = float(pd.to_numeric(df[lat], errors="coerce").mean())
        ax.set_aspect(1.0 / max(np.cos(np.radians(mid_lat)), 0.05),
                      adjustable="datalim")
        ax.set_xlabel("longitude (°E)", fontsize=8.5, color=pal["ink"])
        ax.set_ylabel("latitude (°N)", fontsize=8.5, color=pal["ink"])
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(pal["rule"])
        ax.tick_params(colors=pal["ink"], labelsize=8, color=pal["rule"])
        ax.set_title(title, loc="left", fontsize=12.5, color=pal["ink"],
                     pad=14, fontweight="semibold")
        ax.annotate(caption, xy=(0, 1), xycoords="axes fraction",
                    xytext=(0, 6), textcoords="offset points",
                    fontsize=9, color=pal["ink"], alpha=0.75)
        ax.figure.tight_layout()
        return ax

    try:
        import folium
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise ImportError(_MISSING_FOLIUM) from exc

    m = folium.Map(
        location=[float(df[lat].mean()), float(df[lon].mean())],
        tiles=tiles or _DEFAULT_TILES, zoom_start=6, control_scale=True,
    )
    for (_, row), r in zip(df.iterrows(), radii):
        tip = None
        if label and label in df.columns:
            tip = f"{label} {row[label]}"
            if vals is not None:
                tip += f" — {size} {float(row[size]):,.2f}"
        folium.CircleMarker(
            location=[float(row[lat]), float(row[lon])], radius=float(r),
            color=pal["bar"], weight=1, fill=True, fill_color=pal["bar"],
            fill_opacity=0.55, tooltip=tip,
        ).add_to(m)
    folium.map.Marker(
        [float(df[lat].min()), float(df[lon].min())],
        icon=folium.DivIcon(html=(
            f'<div style="font:11px/1.4 system-ui,sans-serif;color:{pal["ink"]};'
            f'background:rgba(255,255,255,.85);padding:4px 7px;border-radius:3px;'
            f'white-space:nowrap"><b>{title}</b><br>{caption}<br>'
            f'cluster fixes, positions approximate</div>')),
    ).add_to(m)
    try:
        m.fit_bounds([[float(df[lat].min()), float(df[lon].min())],
                      [float(df[lat].max()), float(df[lon].max())]])
    except Exception:
        pass
    return m
