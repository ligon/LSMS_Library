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
within-wave mean 1 at API time, so a weighted and an unweighted pyramid have
the same total and differ only in shape -- which makes the two impossible to
tell apart by eye.  The subtitle always says which one you are looking at.

**One wave per pyramid.**  Pooling waves pools universes (see above), so a
multi-wave frame is not silently concatenated: the most recent wave is drawn
and the choice is reported.
"""
from __future__ import annotations

import warnings
from typing import Any

import pandas as pd

__all__ = ["population_pyramid", "PALETTE"]

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
                       max_age="auto", ax=None, title=None, colors=None):
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
        visible = [int(b) for b in per.index if float(per.loc[b].max()) >= thresh]
        top = (max(visible) + bin_width) if visible else oldest
        # never collapse the chart to a stub, never exceed the data
        max_age = int(min(max(top, 6 * bin_width), max(oldest, bin_width)))

    edges = list(range(0, max_age + bin_width, bin_width))
    df["_band"] = np.minimum(raw, max_age)

    groups = [None]
    if by is not None:
        groups = [g for g in pd.unique(df[by].dropna())][:2]
        if len(groups) < 2:
            groups = groups or [None]

    def totals(sub):
        t = sub.groupby("_band")["_w"].sum()
        return np.array([float(t.get(e, 0.0)) for e in edges])

    span = max(totals(df[df.Sex == "M"]).max(), totals(df[df.Sex == "F"]).max()) or 1.0
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
