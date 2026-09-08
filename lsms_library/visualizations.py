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

**A Lorenz curve is scale-free, so its basis has to be said.**  Cumulative
shares cancel every unit: currency, deflator, recall period and ``numeraire=``
all leave the curve exactly where it was.  That is what makes it comparable
across waves -- and what lets a quiet choice move the answer with nothing in
the picture to betray it.  Uganda 2013-14, per person, person-weighted: Gini
0.50 on cash food purchases, 0.34 on all recorded food acquisition, because
rural households eat what they grow and cash-only counts none of it (ledger
§4; 0.56 household-weighted, 0.48 on household totals unweighted).  Whether
the poorest half are people or households, whether the survey weights were
used, and what became of the households with no recorded purchase each move
it again.  So ``lorenz_curve`` never prints a Gini without naming the
measure, the unit, the weighting and the omitted count beside it.
"""
from __future__ import annotations

import warnings
from typing import Any

import pandas as pd

__all__ = ["population_pyramid", "coordinate_map", "lorenz_curve", "PALETTE"]

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
    "{caller}() needs matplotlib, which is not installed.\n"
    "matplotlib is an ordinary dependency of lsms_library (since v0.11.0), so "
    "a normal install carries it; a hand-built or partial environment may not.\n"
    "Install it with:  pip install matplotlib"
)


def _require_pyplot(caller="population_pyramid"):
    """Import pyplot lazily, with an error that says what to do about it.

    ``caller`` names the chart in the message so a Lorenz-curve error does not
    talk about a pyramid.
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise ImportError(_MISSING_MPL.format(caller=caller)) from exc
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
            weights: bool | str, by: str | None,
            noun: str = "roster") -> tuple[pd.DataFrame, str]:
    """Join ``weight`` / the ``by=`` column from ``sample()`` onto the roster.

    ``Rural``, ``strata`` and the weights live on ``sample()`` at ``(i, t)``
    grain, not on the roster -- so conditioning and weighting need the same
    join, which is why they are done together here.  Any frame keyed by
    ``(i, t)`` can use it; ``noun`` is what the error calls that frame
    ("roster" for the pyramid, "household-frame" for the Lorenz curve).
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
            f"by={by!r} is not available; it is neither a {noun} column nor a "
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
# Lorenz curves
# ---------------------------------------------------------------------------

#: Most waves one ``lorenz_curve`` overlay will draw.  The ramp below has to
#: keep every adjacent pair of steps at least ~0.06 apart in lightness to stay
#: readable, and with the far end capped at :data:`_RAMP_CAP` that budget is
#: spent after five steps.
_MAX_WAVES = 5

#: How far toward white the OLDEST wave of a multi-wave ramp is mixed.  The
#: design asked for "about 55%"; the dataviz palette validator said no -- at
#: 0.55 the light end sits at 1.92:1 against a white surface, under its 2:1
#: floor -- and at 0.45 five steps crowd below the 0.06 lightness gap.  0.50
#: clears both (light end 2.09:1; every gap >= 0.06 for three and for five).
_RAMP_CAP = 0.50


def _lorenz(x, w):
    """Lorenz ordinates and Gini coefficient for weighted values.

    Returns ``(F, L, gini)``: ``F`` is the cumulative share of weight and
    ``L`` the cumulative share of ``x * w``, both taken in ascending order of
    ``x`` and prefixed with 0, so ``(F, L)`` is exactly the polyline to draw.
    The Gini is the trapezoid rule on that same polyline,
    ``1 - sum((F_k - F_{k-1}) * (L_k + L_{k-1}))``.  The rule is *exact* for
    the polygon drawn, so the number printed and the shape seen are one fact
    -- there is no approximation between them to explain (ledger §5).

    Tied values are merged (weights summed) before cumulating, so duplicating
    a row and doubling its weight give identical output.  A zero-weight row
    is dropped: it cannot move the curve.

    Raises ``ValueError`` on a negative value or weight -- a Lorenz curve is
    defined for non-negative values, and a negative expenditure is a data
    defect worth seeing, not clipping -- on a non-finite input, when no weight
    is positive, and when ``sum(x * w) == 0``: everyone at zero has no shares
    to distribute.
    """
    import numpy as np

    x = pd.to_numeric(pd.Series(x), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    w = pd.to_numeric(pd.Series(w), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    if x.ndim != 1 or x.shape != w.shape:
        raise ValueError("x and w must be one-dimensional and the same length; "
                         f"got {x.shape} and {w.shape}")
    if not (np.isfinite(x).all() and np.isfinite(w).all()):
        raise ValueError("x and w must be finite; drop or fill missing rows first")
    if (x < 0).any():
        raise ValueError(f"{int((x < 0).sum())} negative value(s): a Lorenz curve "
                         "is defined for non-negative values only, and a negative "
                         "expenditure is a data defect worth seeing")
    if (w < 0).any():
        raise ValueError(f"{int((w < 0).sum())} negative weight(s)")
    keep = w > 0
    x, w = x[keep], w[keep]
    if w.size == 0:
        raise ValueError("no row has a positive weight")
    vals, inv = np.unique(x, return_inverse=True)
    wsum = np.bincount(inv, weights=w)
    total = float((vals * wsum).sum())
    if total <= 0:
        raise ValueError("every value is zero; there are no shares to distribute")
    F = np.concatenate([[0.0], np.cumsum(wsum) / wsum.sum()])
    L = np.concatenate([[0.0], np.cumsum(vals * wsum) / total])
    gini = 1.0 - float(np.sum(np.diff(F) * (L[1:] + L[:-1])))
    return F, L, gini


def _ramp(base, n, cap=_RAMP_CAP):
    """``n`` colours of ONE hue, oldest first.

    The last is ``base`` at full ink; each earlier step is mixed toward white
    by an equal increment, the first reaching ``cap``.  A list of waves is an
    ordered series, so it takes an ordered ramp: two categorical hues would
    say "different kinds", and waves are not kinds.
    """
    from matplotlib.colors import to_hex, to_rgb

    rgb = to_rgb(base)
    out = []
    for k in range(n):
        f = 0.0 if n == 1 else cap * (n - 1 - k) / (n - 1)
        out.append(to_hex(tuple(c + (1.0 - c) * f for c in rgb)))
    return out


def _sum_to_households(obj, column=None):
    """Sum a long table (or Series) to one number per ``(i, t)`` household.

    Every index level but ``i`` and ``t`` is summed away -- ``j``, ``s``,
    ``u``, ``v`` alike.  Returns a float Series named ``_x`` indexed by
    ``(i, t)`` (or ``i`` alone when the input has no ``t``).
    """
    s = obj if isinstance(obj, pd.Series) else obj[column]
    s = pd.to_numeric(s, errors="coerce")
    names = [n for n in s.index.names if n is not None]
    if "i" not in names:
        raise ValueError("the measure needs an 'i' (household) index level to be "
                         f"summed to household grain; it has {names or 'none'}")
    keys = [k for k in ("i", "t") if k in names]
    out = s.groupby(level=keys, observed=True).sum()
    return out.rename("_x")


def _country_measure(country, waves, value, basis):
    """Household-grain welfare measure for one Country and a list of waves.

    Returns ``(series, names, no_inkind)`` where ``series`` is ``_x`` indexed
    by ``(i, t)``; ``names`` is a dict of the words the chart uses for the
    measure (``axis`` for the y-label, ``text`` for the subtitle, ``short``
    for the direct label, ``absent`` for the no-record disclosure); and
    ``no_inkind`` is True when ``basis='total'`` changed nothing because the
    ``s`` level holds only ``'purchased'``.
    """
    if value is None:
        kw = {"waves": waves}
        if basis is not None:
            kw["basis"] = basis
        fe = country.food_expenditures(**kw)
        if not isinstance(fe, pd.DataFrame) or "Expenditure" not in fe.columns:
            raise ValueError(f"{country.name}.food_expenditures() returned no "
                             "'Expenditure' column")
        total = basis == "total"
        no_inkind = False
        if total and "s" in fe.index.names:
            # Read the level that is already here rather than calling the API
            # twice: if every row is 'purchased', 'total' had nothing to add.
            held = set(map(str, pd.unique(fe.index.get_level_values("s").dropna())))
            no_inkind = held <= {"purchased"}
        names = {
            "axis": "food spending",
            "text": "all recorded food acquisition" if total else "food purchases",
            "short": "spending",
            "absent": "acquisition" if total else "purchase",
        }
        return _sum_to_households(fe, "Expenditure"), names, no_inkind

    if isinstance(value, pd.Series):
        s = value
        if "t" not in s.index.names:
            raise ValueError("a Series passed as value= must carry a 't' index "
                             "level so it can be matched to the wave(s) drawn")
        s = s[s.index.get_level_values("t").isin(waves)]
        name = str(s.name) if s.name is not None else "value"
        names = {"axis": name, "text": name, "short": name, "absent": name}
        return _sum_to_households(s), names, False

    if isinstance(value, str):
        method = getattr(country, value, None)
        if not callable(method):
            raise KeyError(f"{value!r} is not a table of {country.name}; "
                           f"tables: {', '.join(country.data_scheme)}")
        tbl = method(waves=waves)
        if not isinstance(tbl, pd.DataFrame):
            raise ValueError(f"{country.name}.{value}() did not return a DataFrame")
        if "Expenditure" not in tbl.columns:
            # A wide item-by-household matrix would sum to a perfectly
            # plausible curve -- which is exactly why it must not: the shape
            # is a defect in the table (GH #817), and a chart that summed it
            # would hide that defect behind a correct-looking picture.
            raise ValueError(
                f"{country.name}.{value}() has no 'Expenditure' column ({len(tbl.columns)} "
                "columns); lorenz_curve(value=) takes a long expenditure table, one "
                "'Expenditure' per (household, item) row.  A wide one-column-per-item "
                "table is a defect in that table, not a shape for the chart to "
                "accommodate -- see GH #817.  Sum it yourself and pass the result as "
                "a Series indexed by (i, t) if you mean to draw it anyway."
            )
        names = {"axis": value, "text": value, "short": value, "absent": value}
        return _sum_to_households(tbl, "Expenditure"), names, False

    raise TypeError("value= takes None, a table name, or a pandas Series indexed by "
                    f"(i, t); got {type(value).__name__}")


def _household_sizes(country, waves):
    """``size`` per ``(i, t)`` from ``household_characteristics``.

    ``exp(log HSize)`` rounded to an integer.  The resident filter is that
    table's decision (ledger §3), so a household whose every member failed
    it has ``log HSize = -inf`` and comes back as size 0 -- the caller counts
    and drops those rather than dividing by zero.
    """
    import numpy as np

    hc = country.household_characteristics(waves=waves)
    if not isinstance(hc, pd.DataFrame) or "log HSize" not in hc.columns:
        raise ValueError(f"{country.name}.household_characteristics() has no "
                         "'log HSize' column, so household size is unavailable")
    ls = pd.to_numeric(hc["log HSize"], errors="coerce")
    size = np.exp(ls.to_numpy(dtype=float, na_value=np.nan))
    size = np.where(np.isfinite(size), np.round(size), np.nan)
    out = hc.reset_index()[["i", "t"]].copy()
    out["size"] = size
    return out.drop_duplicates(["i", "t"])


def _select_waves(held, wave, name):
    """Resolve ``wave`` against the waves ``held``; returns ``(waves, is_list)``.

    Same rule as the other two charts: with several waves and no choice, the
    most recent is drawn and the choice is reported.
    """
    held = [str(h) for h in held]
    if wave is None:
        if len(held) > 1:
            pick = sorted(held)[-1]
            warnings.warn(
                f"{name} covers {len(held)} waves and their sampled populations "
                f"may differ; drawing the most recent ({pick}). Pass wave= to "
                "choose another, or a list of waves for an overlay.",
                stacklevel=3,
            )
            return [pick], False
        return (held[:1] or [None]), False
    if isinstance(wave, (list, tuple)):
        chosen = [str(w) for w in wave]
        if not chosen:
            raise ValueError("wave= list is empty")
        if len(chosen) > _MAX_WAVES:
            raise ValueError(f"wave= lists {len(chosen)} waves; at most {_MAX_WAVES} "
                             "can share one chart and still be told apart")
        if len(set(chosen)) != len(chosen):
            raise ValueError(f"wave= repeats a wave: {chosen}")
        missing = [w for w in chosen if w not in held]
        if missing:
            raise ValueError(f"{name} has no wave {missing}; held: {held}")
        return chosen, True
    wave = str(wave)
    if wave not in held:
        raise ValueError(f"{name} has no wave {wave!r}; held: {held}")
    return [wave], False


def _by_groups(df, by):
    """Resolve ``by`` (a column, or ``(column, [a, b])``) to at most two groups.

    Returns ``(column, groups, n_outside)``.  More than two values in the
    column is an error naming them -- the pyramid's ``groups[:2]`` silently
    discards the rest, and Uganda's ``Region`` has four -- so picking two of
    many is done explicitly through the tuple form.
    """
    if isinstance(by, (list, tuple)):
        if len(by) != 2 or not isinstance(by[0], str):
            raise TypeError("by= takes a column name or a (column, [value, value]) tuple")
        col, pick = by[0], [v for v in by[1]]
        if len(pick) != 2:
            raise ValueError(f"by=({col!r}, ...) must pick exactly two values; got {pick}")
    else:
        col, pick = by, None
    held = list(pd.unique(df[col].dropna()))
    if pick is None:
        if len(held) > 2:
            raise ValueError(
                f"by={col!r} holds {len(held)} values ({', '.join(map(str, held))}); "
                "colour can separate two.  Pass by=(column, [value, value]) to "
                "pick which two to draw."
            )
        groups = held
    else:
        absent = [p for p in pick if p not in held]
        if absent:
            raise ValueError(f"by={col!r} has no value {absent}; it holds: "
                             f"{', '.join(map(str, held))}")
        groups = pick
    n_outside = int((df[col].notna() & ~df[col].isin(groups)).sum())
    return col, groups, n_outside


def lorenz_curve(data, wave=None, *, value=None, per="person", weights=True,
                 basis=None, by=None, zeros=None, size=None, ax=None,
                 title=None, colors=None):
    """Draw the Lorenz curve of household spending for one survey wave.

    The cumulative share of spending held by the poorest fraction of the
    population, on the unit square, against the equality diagonal; the Gini
    coefficient -- twice the area between them -- is printed on the chart.
    The curve is scale-free (see the module docstring), so every *other*
    choice that moves it is stated in the subtitle: whose spending, per
    person or per household, weighted how, and what became of households
    with nothing recorded.

    Parameters
    ----------
    data : Country, str, or DataFrame
        A country, its name, or a frame.  A frame is either an
        expenditure-shaped table (a ``j`` index level and an ``Expenditure``
        column, summed over every level but ``(i, t)``) or a household-grain
        frame whose welfare column ``value`` names.
    wave : str or list of str, optional
        One wave (default: the most recent, with a warning when several are
        held), or a list of up to five drawn as separate labelled curves in
        an ordered ramp of one hue -- most recent in the full ink.  A list
        together with ``by`` is an error: colour has one job.
    value : None, str, or pandas.Series, optional
        The measure.  ``None`` is ``food_expenditures``.  A string names
        another table of the country, which must carry an ``Expenditure``
        column (long shape, summed to household grain); a table without one
        raises, citing GH #817, rather than summing a wide matrix into a
        plausible-looking curve.  A Series indexed by ``(i, t)`` is a
        user-computed measure -- "food plus non-food" is two API calls and an
        add, not a kwarg.  On the frame path a string names the welfare
        column.
    per : {'person', 'household'}, default 'person'
        ``'person'`` divides spending by household size and weights each
        household by ``size * weight`` -- the literature's standard, in which
        each person counts once.  ``'household'`` uses household totals
        weighted by ``weight``.  Size is ``exp(log HSize)`` from
        ``household_characteristics`` for a Country (resident-filtered, as
        that table decides), or the ``size`` column of a frame.
    weights : bool or str, default True
        Same contract as :func:`population_pyramid`: the survey's weights
        from ``sample()``, falling back to unweighted with a warning; ``False``
        for unweighted; a column name.
    basis : {'purchased', 'total'}, optional
        Passed through to ``food_expenditures``: ``None`` follows its default
        (cash purchases only); ``'total'`` is all recorded acquisition value.
        Meaningful only for the food measure; anywhere else it is an error.
    by : str or (str, [value, value]), optional
        Split into two curves on a column of the frame or of ``sample()``
        (``Rural``, ``strata``, ``Region`` ...); colour encodes it.  A column
        with more than two values raises and names them; the tuple form picks
        two of many.
    zeros : {'drop', 'include'}, optional
        What to do with households in the wave's ``sample()`` that have no
        row in the measure.  They are *absent*, not zero, because
        ``food_expenditures`` drops zero rows (ledger §4) -- on Uganda 2013-14
        the 30 such households are true cash zeros with own-production rows.
        ``None`` resolves to ``'drop'``: omit them and say how many.
        ``'include'`` draws them at zero.  Frame path: not accepted -- there
        is no sample frame to compare against.
    size : str, optional
        Frame path only: the household-size column (default ``'size'``).
    ax : matplotlib Axes, optional
    title, colors : optional overrides.

    Returns
    -------
    matplotlib.axes.Axes

    Notes
    -----
    Counts in the subtitle are of households actually drawn (and, per
    person, the *headcount* ``sum(size)`` -- never the weighted person total,
    which is not a count).  With several curves the counts are totals over
    all of them; each curve's Gini sits in its legend entry.  Every
    disclosure ("30 with no recorded purchase, not drawn", "2 without a
    usable roster, not drawn", "no in-kind value recorded") is a measured
    count that appears only when it is non-zero.
    """
    import textwrap

    import numpy as np
    from matplotlib.ticker import PercentFormatter

    from .country import Country

    plt = _require_pyplot("lorenz_curve")
    pal = {**PALETTE, **(colors or {})}

    if per not in ("person", "household"):
        raise ValueError(f"per must be 'person' or 'household'; got {per!r}")
    if zeros not in (None, "drop", "include"):
        raise ValueError(f"zeros must be None, 'drop' or 'include'; got {zeros!r}")
    if by is not None and isinstance(wave, (list, tuple)):
        raise ValueError("wave=[...] and by= cannot be combined: colour encodes "
                         "either the wave or the group, not both")
    if basis is not None and value is not None:
        raise TypeError("basis= applies only to the food_expenditures measure; "
                        f"it has no meaning for value={value!r}")

    country = None
    if isinstance(data, str):
        country = Country(data)
    elif isinstance(data, Country):
        country = data

    disclose = {}          # measured counts -> subtitle, only when non-zero
    no_inkind = False
    if country is not None:
        # ---- Country path: measure, size, sample, all at (i, t) ---------
        if size is not None:
            raise TypeError("size= names a column of a household frame; for a "
                            "Country, size comes from household_characteristics")
        cname = country.name
        waves, is_list = _select_waves(country.waves, wave, cname)
        measure, names, no_inkind = _country_measure(country, waves, value, basis)
        if "t" not in measure.index.names:
            raise ValueError("the measure has no 't' level; cannot place it in a wave")
        hh = measure.reset_index()
        hh = hh[hh["t"].astype(str).isin(waves)].copy()
        hh["t"] = hh["t"].astype(str)
        if hh.empty:
            raise ValueError(f"{cname}: no {names['text']} rows in wave(s) {waves}")

        # The interviewed households: the set that defines `zeros`.  Read
        # here only for that set -- `_attach` joins the weights itself, and
        # handing it a frame that already carries `weight` would make its
        # merge suffix both copies and fall back to unweighted.
        try:
            smp = country.sample()
        except Exception as exc:
            warnings.warn(f"sample() unavailable for {cname} ({exc}); cannot tell "
                          "which households have no recorded value.", stacklevel=2)
            smp = None
        if smp is not None and {"i", "t"} <= set(smp.index.names):
            si = smp.reset_index()[["i", "t"]]
            si["t"] = si["t"].astype(str)
            si = si[si["t"].isin(waves)].drop_duplicates()
            have = pd.MultiIndex.from_frame(hh[["i", "t"]])
            absent = pd.MultiIndex.from_frame(si).difference(have)
            disclose["zeros"] = len(absent)
            if zeros == "include" and len(absent):
                add = absent.to_frame(index=False)
                add["_x"] = 0.0
                hh = pd.concat([hh, add], ignore_index=True)
        else:
            disclose["zeros"] = 0

        if per == "person":
            sizes = _household_sizes(country, waves)
            sizes["t"] = sizes["t"].astype(str)
            hh = hh.merge(sizes, on=["i", "t"], how="left")
            bad = hh["size"].isna() | (hh["size"] <= 0)
            disclose["no_roster"] = int(bad.sum())
            hh = hh[~bad].copy()
        hh = hh.set_index(["i", "t"])
        label_single = f"{cname} {waves[0]}"
        label = cname if is_list else label_single
    else:
        # ---- Frame path ------------------------------------------------
        frame = data
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("lorenz_curve() takes a Country, a country name, or a "
                            f"DataFrame; got {type(data).__name__}")
        if zeros is not None:
            raise TypeError("zeros= needs a Country: on a frame there is no "
                            "sample() to say which households are missing")
        if basis is not None:
            raise TypeError("basis= needs a Country: it is passed to "
                            "food_expenditures(), which a frame has already been")
        cname = None
        long_shape = "j" in frame.index.names
        if long_shape:
            col = value if isinstance(value, str) else "Expenditure"
            if col not in frame.columns:
                raise KeyError(f"{col!r} is not a column of this frame; columns: "
                               f"{', '.join(map(str, frame.columns))}")
            hh = _sum_to_households(frame, col).reset_index()
            if size is not None:
                raise TypeError("size= names a column of a household-grain frame; "
                                "this frame is item-level (it has a 'j' level)")
        else:
            if value is None:
                if "Expenditure" not in frame.columns:
                    raise ValueError(
                        "pass value= naming the welfare column of this household "
                        f"frame; it has no 'Expenditure' column (columns: "
                        f"{', '.join(map(str, frame.columns))})")
                col = "Expenditure"
            elif isinstance(value, str):
                col = value
                if col not in frame.columns:
                    raise KeyError(f"value={col!r} is not a column of this frame; "
                                   f"columns: {', '.join(map(str, frame.columns))}")
            else:
                raise TypeError("on a frame, value= names a column (str)")
            hh = frame.reset_index().copy()
            hh["_x"] = pd.to_numeric(hh[col], errors="coerce")
            size_col = size or "size"
            if per == "person":
                if size_col not in hh.columns:
                    raise ValueError(
                        f"per='person' needs a household-size column ({size_col!r}) "
                        "on the frame; pass size= to name it, or per='household'")
                hh["size"] = pd.to_numeric(hh[size_col], errors="coerce")
        word = "spending" if col == "Expenditure" else str(col)
        names = {"axis": word, "text": word, "short": word, "absent": word}
        if "t" in hh.columns:
            waves, is_list = _select_waves(pd.unique(hh["t"].dropna()), wave, "frame")
            hh["t"] = hh["t"].astype(str)
            hh = hh[hh["t"].isin([str(w) for w in waves])].copy()
        else:
            if isinstance(wave, (list, tuple)):
                raise ValueError("wave=[...] needs a 't' level or column on the frame")
            waves, is_list = [wave], False
        hh = hh[hh["_x"].notna()].copy()
        if per == "person":
            bad = hh["size"].isna() | (hh["size"] <= 0)
            disclose["no_roster"] = int(bad.sum())
            hh = hh[~bad].copy()
        if hh.empty:
            raise ValueError("no household has a usable value")
        keys = [k for k in ("i", "t") if k in hh.columns]
        if keys:
            hh = hh.set_index(keys)
        label = str(waves[0]) if (waves[0] is not None and not is_list) else "households"

    # ---- weights and the `by` column, joined from sample() -----------------
    by_col = by[0] if isinstance(by, (list, tuple)) else by
    df, wbasis = _attach(hh, cname, weights, by_col, noun="household-frame")
    noun = "people" if per == "person" else "households"

    if per == "person":
        df["_y"] = df["_x"] / df["size"]
        df["_wy"] = df["_w"] * df["size"]
    else:
        df["_y"] = df["_x"]
        df["_wy"] = df["_w"]
    unweighted = ~(df["_wy"] > 0)
    disclose["no_weight"] = int(unweighted.sum())
    df = df[~unweighted].copy()
    if df.empty:
        raise ValueError(f"no household left to draw for {label}")

    # ---- series ------------------------------------------------------------
    series = []
    if by is not None:
        col, groups, n_out = _by_groups(df, by)
        disclose["outside"] = n_out
        disclose["by_na"] = int(df[col].isna().sum())
        gnames = {str(g) for g in groups}
        for gi, g in enumerate(groups):
            sub = df[df[col] == g]
            series.append((str(g), pal["bar"] if gi == 0 else pal["alt"], sub))
        legend_title = None if (col in gnames or len(groups) < 2) else col
    elif is_list:
        colours = _ramp(pal["bar"], len(waves))
        order = sorted(waves)
        for wv, colour in zip(order, colours):
            # `_attach` hands back a flat frame: `t` is a column here.
            series.append((wv, colour, df[df["t"].astype(str) == wv]))
        legend_title = None
    else:
        series.append((None, pal["bar"], df))
        legend_title = None

    curves = []
    n_hh = 0
    n_people = 0.0
    for lab, colour, sub in series:
        if sub.empty:
            raise ValueError(f"no household to draw for {lab!r}")
        F, L, gini = _lorenz(sub["_y"], sub["_wy"])
        curves.append((lab, colour, F, L, gini))
        n_hh += len(sub)
        if per == "person":
            n_people += float(sub["size"].sum())

    # ---- draw --------------------------------------------------------------
    if ax is None:
        _, ax = plt.subplots(figsize=(5.8, 5.8), dpi=130)
    fig = ax.figure

    # The diagonal: a solid hairline in the rule colour, unlabelled.  Dashing
    # reads as a projection or a threshold, and a legend entry for the line
    # of equality names nothing the reader does not already know.
    ax.plot([0, 1], [0, 1], color=pal["rule"], lw=0.9, zorder=1)
    one = len(curves) == 1
    for lab, colour, F, L, gini in curves:
        ax.plot(F, L, color=colour, lw=1.8, zorder=3, solid_joinstyle="round",
                label=(None if lab is None else f"{lab}, Gini {gini:.2f}"))
        if one:
            # The shaded gap IS half the Gini: the number printed and the
            # shape seen are the same fact.  With two or more curves the
            # fills would overlap into a third colour, so none is drawn.
            ax.fill_between(F, F, L, color=colour, alpha=0.12, linewidth=0, zorder=2)
            l50 = float(np.interp(0.5, F, L))
            ax.scatter([0.5], [l50], s=16, color=colour, zorder=4)
            # Below and to the right of the dot: the curve is convex and
            # rising, so that side is always clear of it.  When the curve
            # sits too low for the text to fit beneath, put it in the gap
            # above instead.
            txt = f"the poorest half of {noun}:\n{l50:.0%} of {names['short']}"
            below = l50 >= 0.12
            ax.annotate(txt, xy=(0.5, l50), xytext=(8, -9 if below else 8),
                        textcoords="offset points", ha="left",
                        va="top" if below else "bottom", fontsize=8.6,
                        color=pal["ink"], linespacing=1.25, zorder=5)

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect("equal")
    ticks = [0, 0.25, 0.5, 0.75, 1.0]
    ax.set_xticks(ticks)
    ax.set_yticks(ticks)
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    ax.set_xlabel(f"share of {noun}, poorest first", fontsize=8.5, color=pal["ink"])
    ax.set_ylabel(f"share of {names['axis']}", fontsize=8.5, color=pal["ink"])
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(pal["rule"])
    ax.tick_params(colors=pal["ink"], labelsize=8, length=3, color=pal["rule"])
    ax.grid(False)

    # ---- subtitle: every bit is a measured count or a stated basis ----------
    bits = []
    if one:
        bits.append(f"Gini {curves[0][4]:.2f}")
    bits.append(f"{n_hh:,} households")
    if per == "person":
        bits.append(f"{int(round(n_people)):,} people")
    bits.append(f"{names['text']} per {per}")
    bits.append(wbasis)
    k = disclose.get("zeros", 0)
    if k:
        fate = "drawn at zero" if zeros == "include" else "not drawn"
        bits.append(f"{k:,} with no recorded {names['absent']}, {fate}")
    m = disclose.get("no_roster", 0)
    if m:
        bits.append(f"{m:,} without a usable roster, not drawn")
    if disclose.get("no_weight", 0):
        bits.append(f"{disclose['no_weight']:,} without a sampling weight, not drawn")
    if disclose.get("outside", 0):
        bits.append(f"{disclose['outside']:,} outside the two {by_col} values picked, "
                    "not drawn")
    if disclose.get("by_na", 0):
        bits.append(f"{disclose['by_na']:,} with no {by_col} value, not drawn")
    if no_inkind:
        bits.append("no in-kind value recorded")

    # Six comma-joined bits do not fit one line of a 5.8-inch square, so wrap
    # to the axes' measured width and make room above with the title pad.
    # Lines are packed a whole bit at a time: "30 with no recorded purchase,
    # / not drawn" split across a line break reads as two statements.
    fig.tight_layout()
    try:
        ax_pt = float(ax.get_window_extent().width) * 72.0 / fig.dpi
    except Exception:                         # no renderer yet
        ax_pt = fig.get_size_inches()[0] * 72.0 * 0.78
    width = max(40, int(ax_pt / (9 * 0.52)))
    lines = []
    for bit in bits:
        if lines and len(lines[-1]) + 2 + len(bit) <= width:
            lines[-1] += ", " + bit
        else:
            lines.extend(textwrap.wrap(bit, width=width, break_long_words=False))
    lines = [ln + "," for ln in lines[:-1]] + lines[-1:]
    ax.annotate("\n".join(lines), xy=(0, 1), xycoords="axes fraction",
                xytext=(0, 6), textcoords="offset points", ha="left", va="bottom",
                fontsize=9, color=pal["ink"], alpha=0.75, linespacing=1.2)
    ax.set_title(title or label, loc="left", fontsize=12.5, color=pal["ink"],
                 pad=16 + 10.8 * (len(lines) - 1), fontweight="semibold")

    # One universe line per distinct caption; waves sharing a tag share a line.
    captions = {}
    for wv in waves:
        cap = _universe(cname, wv)
        if cap:
            captions.setdefault(cap, []).append(str(wv))
    if captions:
        if len(captions) == 1:
            text = next(iter(captions))
        else:
            text = "\n".join(f"{', '.join(ws)}: {cap}" for cap, ws in captions.items())
        fig.text(0.005, -0.005, text, fontsize=7.8, color=pal["ink"], alpha=0.7,
                 ha="left", va="top")

    if len(curves) > 1 or (by is not None and curves[0][0] is not None):
        leg = ax.legend(title=legend_title, frameon=False, fontsize=8.5,
                        loc="upper left", title_fontsize=8.5)
        if leg.get_title() is not None:
            leg.get_title().set_color(pal["ink"])

    for item in ([ax.title] + ax.get_xticklabels() + ax.get_yticklabels()):
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
        plt = _require_pyplot("coordinate_map")
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
