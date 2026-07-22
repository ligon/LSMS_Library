import numpy as np
import pandas as pd
from collections import defaultdict
from cfe.df_utils import use_indices
import warnings
import json

if __name__=='__main__':
    import sys
    sys.path.append('../../_')
    sys.path.append('../../../_')
    from local_tools import format_id, get_dataframe
else:
    from lsms_library.local_tools import format_id, get_dataframe

def District(x):
    """Canonical District form across all Uganda waves.

    Pre-2018 waves source ``District`` from numeric Stata columns
    (``h1aq1``, ``h1aq1a``) which df_data_grabber stringifies to
    ``'101.0'``-style float-strings (CLAUDE.md: ``format_id`` is
    auto-applied to ``idxvars`` but NOT to ``myvars``).  Post-2018
    waves source from string columns (``district_name`` / ``district``)
    which need no normalisation but ``format_id`` is a no-op on them.

    Defining a country-level ``District`` formatter routes every
    Uganda ``District`` myvar through the canonical normalizer so
    cross-wave District values share an int-string encoding.
    GH #161.
    """
    return format_id(x)


def v(x):
    """Canonical cluster-id form for Uganda's ``v`` myvar across all tables.

    Uganda declares ``v`` as a ``myvar`` in ``sample`` (and elsewhere)
    rather than an ``idxvar``, so the auto-applied ``format_id`` does
    not fire and numeric cluster codes like ``10120402`` get
    float-stringified to ``'10120402.0'`` -- which silently fails to
    join against ``cluster_features.v`` and ``household_characteristics.v``
    where ``v`` is in ``idxvars`` and *does* go through ``format_id``.

    Defining a country-level ``v`` formatter routes every Uganda
    ``v`` myvar through ``format_id``, restoring the cross-table
    invariant.  ``format_id`` is a no-op on already-canonical strings
    (e.g., the ``parish_name`` strings in 2018-19, 2019-20) and maps
    empty strings to ``None`` (cleaning up the 565-row ``v == ''``
    leak in 2009-10's sample).

    GH #196.
    """
    return format_id(x)


# ---------------------------------------------------------------------------
# GH #323 -- country-level df_edit hooks.
#
# Both of the functions below are dispatched by the framework as the named
# table's ``df_edit`` hook (``Wave.column_mapping`` ->
# ``final_mapping['df_edit'] = formatting_functions.get(request)``), for EVERY
# Uganda wave, because they live in the country module.  A wave that needs
# something different defines a function of the same name in its own
# ``_/mapping.py``, which wins.
#
# They exist because the framework's fallbacks are silent: a non-unique declared
# index is reduced with ``groupby().first()``, which takes the first NON-NULL
# value of each column INDEPENDENTLY and can therefore return a row that exists
# in no source record.  Neither of these hooks aggregates.  They make the grain
# EXPLICIT and then either prove the reduction lossless or say -- out loud --
# exactly what it could not resolve.  See
# SkunkWorks/grain_aggregation_policy.org and
# slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org.
# ---------------------------------------------------------------------------

_P7_MEMBER_CATEGORY = 'Household members'


def people_last7days(df):
    """Keep the HOUSEHOLD-MEMBER rows of a long-form GSEC15A (GH #323).

    The question ("how many people were present in the last 7 days?") is asked
    separately for household MEMBERS and for VISITORS.  Through 2013-14 the two
    live in parallel COLUMNS and the YAML maps ``Men/Women/Boys/Girls`` onto the
    member block, leaving the visitor block unused:

    ===========  =========  =====================  =======================
    Wave         File       Member cols (used)     Visitor cols (ignored)
    ===========  =========  =====================  =======================
    2005-06      GSEC14     ``h14q1``--``h14q4``   ``h14q5``--``h14q8``
    2009-10      GSEC15A    ``h15a1``--``h15a4``   ``h15a5``--``h15a8``
    2013-14      GSEC15     ``T6FQ01a``--``d``     ``T6FQ01e``--``h``
    ===========  =========  =====================  =======================

    (Verified against the Stata variable labels: ``h14q1`` = "male adults hh
    members", ``h14q5`` = "male adults visitors", and so on.)

    From 2018-19 the questionnaire went LONG: the same distinction moved out of
    the column suffix and into a ROW category.  ``GSEC15A`` now carries exactly
    two rows per ``hhid``, discriminated by ``CEA01`` ("Household member/visitor")
    in {'Household members', 'Visitors'}, with ``CEA01A``--``CEA01D`` holding the
    counts *for the selected category*.  The extraction was never updated: the
    declared index stayed ``(i, t)``, so both rows collided on one tuple and
    ``_normalize_dataframe_index`` reduced them with ``groupby().first()`` --
    keeping whichever row the file happened to list first, which is a coin flip.

    The wave YAML now also extracts ``CEA01`` as the temporary myvar
    ``_category``; this hook selects the member rows and drops it, after which
    ``(i, t)`` is genuinely unique and the collapse never fires.

    Members-only is not a judgement call -- it is what the three earlier waves
    above already do.  If you want "people fed" = members + visitors, that is a
    DIFFERENT table and should be declared as one.

    On a wave whose member/visitor split is in COLUMNS there is no ``_category``
    to filter on and this is a pass-through -- but the uniqueness of the declared
    index is asserted either way, so a future long-form wave that forgets the
    ``_category`` myvar fails loudly here instead of silently returning visitor
    counts for half its households.
    """
    if '_category' in df.columns:
        cat = df['_category'].astype(str).str.strip()
        keep = cat == _P7_MEMBER_CATEGORY
        if not keep.any():
            raise ValueError(
                f"people_last7days: no rows with _category == "
                f"{_P7_MEMBER_CATEGORY!r}; observed categories are "
                f"{sorted(cat.unique())}.  The source's member/visitor label "
                f"changed -- fix the filter rather than letting the (i, t) "
                f"collapse pick a row at random (GH #323)."
            )
        df = df[keep].drop(columns='_category')
    if not df.index.is_unique:
        n_dup = int(df.index.duplicated().sum())
        raise ValueError(
            f"people_last7days: {n_dup} duplicate rows on the declared index "
            f"{list(df.index.names)}.  Uganda's post-2018 GSEC15A is LONG "
            f"(one row per member/visitor category); declare CEA01 as the "
            f"`_category` myvar so this hook can select the member rows.  "
            f"Collapsing them with .first() returns VISITOR counts for about "
            f"half the households (GH #323)."
        )
    return df


def cluster_features(df):
    """Project the household-grain cover page onto the ``(t, v)`` cluster grain.

    Uganda builds ``cluster_features`` from GSEC1, the household COVER PAGE, so
    the extraction is one row per HOUSEHOLD while the table is declared at
    ``(t, v)`` -- roughly a 4-10x inflation.  The reduction is intended (GH #161);
    what was not intended is that it was left UNDECLARED, so it fell through to
    the framework's ``groupby().first()``.  That reducer skips NA per column, so
    where the households of a cluster disagree it does not even return one of
    their rows -- it assembles a composite out of the first non-null value of
    each column independently, a "household" that appears nowhere in the survey.

    This hook makes the projection explicit and hands it to ``reduce_to_agreed``
    (``lsms_library.build_transforms``), whose contract is *lossless or loud*:

    * a cluster whose households AGREE collapses to that agreed row -- silently,
      because nothing is lost;
    * a household that reports nothing where another reports a value is an
      ABSENCE, not a contradiction, so the observed value survives (this is the
      bulk of Uganda's apparent "destruction": 2005-06 loses 2,551 rows to the
      Site-1 audit but has ZERO clusters whose households actually disagree on
      Region/Rural/District -- they differ only in whether the geovar carried a
      GPS fix);
    * a cluster whose households genuinely DISAGREE gets ``<NA>`` in the
      offending column plus a ``GrainConflictWarning`` naming it.

    ``on_conflict='na'`` rather than the default ``'raise'``, deliberately, and
    the reason is that Uganda's residual disagreements are properties of the
    SURVEY, not defects we can configure away:

    1. ``comm`` (2005-06 .. 2011-12) is the *2005-06 EA of origin*, which the
       panel carries forward when it tracks a mover.  A household that moved
       district still reports its origin EA, so its District/Region legitimately
       differ from its EA-mates'.  93 clusters in 2010-11, 106 in 2011-12.
    2. ``v`` is a PARISH from 2013-14 on, and a parish contains several
       enumeration areas -- some urban, some rural, each with its own geovar
       fix.  ``Rural`` and the GPS are therefore not parish attributes at all
       (70 / 65 / 122 / 122 clusters conflict on Rural in 2013-14 / 2015-16 /
       2018-19 / 2019-20).

    Neither can be resolved from this table, and picking a winner is the bug.
    ``<NA>`` + a warning is the honest answer; per-household ``Rural`` / ``Region``
    remain exact in ``sample()``, which is their proper home.

    NOT reduced with an average.  Where the within-cluster GPS varies at all it
    varies by a MEDIAN of 4.6-42.9 km and by up to 584 km (measured, all waves):
    that is a broken cluster key, not a scatter of points around a centroid, and
    averaging it would smear two places into one and hide the evidence -- the
    same argument that retired core's ``.mean()`` on 2026-07-13.

    Rows whose ``v`` is missing are dropped HERE, with a count, rather than
    disappearing inside a ``groupby(dropna=True)`` further downstream (GH #323
    Site 3: delete and report, decision D2).  A cluster with no identifier cannot
    be addressed by any consumer.
    """
    from lsms_library.build_transforms import reduce_to_agreed

    flat = df.reset_index()
    # The household level has no place in a cluster-grain table, and leaving it
    # in would make every multi-household cluster look like a conflict (`i` is
    # distinct by construction).  Drop it whether it arrived as an index level
    # or -- via the `dfs:` merge -- as a column.
    flat = flat.drop(columns=[c for c in ('i', 'index') if c in flat.columns])
    missing = [lvl for lvl in ('t', 'v') if lvl not in flat.columns]
    if missing:
        raise ValueError(
            f"cluster_features: {missing} absent from the extracted frame "
            f"(have {list(flat.columns)}).  The wave's data_info.yml must "
            f"supply `v` either as an idxvar or as a myvar."
        )
    n_blank = int(flat['v'].isna().sum())
    if n_blank:
        warnings.warn(
            f"Uganda/cluster_features: dropping {n_blank:,} household row(s) "
            f"with no cluster id (`v` is missing).  They cannot be addressed by "
            f"any consumer, and keeping them would put an unnamed cluster in a "
            f"table keyed on the cluster.  GH #323 (Site 3).",
            stacklevel=2,
        )
        flat = flat[flat['v'].notna()]
    return reduce_to_agreed(flat.set_index(['t', 'v']), on_conflict='na')


def _format_agsec_hhid(s, t):
    """Canonical household id for Uganda's AGSEC (agricultural) modules.

    Uganda's **2013-14** AGSEC modules (AGSEC2*--AGSEC6*) encode ``HHID``
    as a stripped *integer* (e.g. ``1100401``), whereas the GSEC
    household modules, :meth:`Country.sample`, and the panel-id map
    (``updated_ids['2013-14']``) all use the dashed string form
    (``H00110-04-01``).  The integer is exactly that string with the
    ``'H'``, the leading zeros, and the dashes removed -- so the two are
    a deterministic, invertible pair (verified: all 2394 distinct AGSEC
    ids round-trip to valid GSEC ids).

    Left unfixed, every AGSEC-derived feature's ``i`` for 2013-14 matches
    neither ``sample()``'s ids nor the panel-id map: ``_join_v_from_sample``
    yields 100% NaN ``v`` and the whole wave is silently dropped from the
    derived tables, while ``id_walk`` cannot canonicalize the ids (its
    source keys are the ``H...`` strings).  See GH #256 and the v-join
    warning in country.py for the failure mode.

    Only 2013-14 is affected: 2015-16 AGSEC already carries the H-form,
    and 2018-19 / 2019-20 use their own (numeric / hash) canonical ids
    that must **not** be reformatted -- hence the decode is gated on the
    wave.  For every other wave this is exactly ``s.apply(format_id)``.
    """
    out = s.apply(format_id)
    if t != '2013-14':
        return out

    def _decode(x):
        if pd.isna(x):
            return x
        xs = str(x)
        if xs.isdigit():
            return f'H{xs[:-4].zfill(5)}-{xs[-4:-2]}-{xs[-2:]}'
        return xs

    return out.apply(_decode)


# Data to link household ids across waves
Waves = {'2005-06':(),
         '2009-10':(), # ID of parent household  in ('GSEC1.dta',"HHID",'HHID_parent'), but not clear how to use
         '2010-11':(),
         '2011-12':(),
         '2013-14':('GSEC1.dta','HHID','HHID_old'),
         '2015-16':('gsec1.dta','HHID','hh',lambda s: s.replace('-05-','-04-')),
         '2018-19':('GSEC1.dta','hhid','t0_hhid'),
         '2019-20':('HH/gsec1.dta','hhid','hhidold')}

def harmonized_unit_labels(key='Code', value='Preferred Label'):
    """Return the {Code: Preferred Label} mapping from the ``u`` table
    in ``lsms_library/countries/Uganda/_/categorical_mapping.org``.

    Replaces the older CSV-based pipeline (``unitlabels.py`` emitting
    ``unitlabels.csv``); the org table is now the single source of truth
    for unit-label canonicalisation.  See GH #223 for the cross-country
    roadmap and Tier 1 convention (Malawi, Mali, Nigeria, Senegal,
    Burkina Faso).

    The org table reuses the ``---`` sentinel for empty cells (per
    ``df_from_orgfile``).  To preserve compatibility with the previous
    CSV-based behaviour -- which carried explicit ``'---'`` strings as
    the canonical label for unit codes that exist but lack a meaningful
    label -- we restore those NaN-valued labels to the literal
    ``'---'`` string.  Codes are coerced to ``int`` so the mapping
    matches the float-typed ``u`` index values produced by the wave
    scripts (``hash(1) == hash(1.0)``; a string key would not match).
    """
    from lsms_library.local_tools import get_categorical_mapping

    raw = get_categorical_mapping(tablename='u',
                                  idxvars=key,
                                  **{value: value})

    unitlabels = {}
    for k, v in raw.items():
        try:
            int_k = int(k)
        except (TypeError, ValueError):
            int_k = k
        if pd.isna(v):
            unitlabels[int_k] = '---'
        else:
            unitlabels[int_k] = str(v).strip()
    return unitlabels

def harmonized_food_labels(fn=None,key='Code',value='Preferred Label'):
    """Return the ``{Code: <value>}`` food-label mapping (default ``value``
    is ``Preferred Label``).

    Unit #0 migration (2026-06-14): the canonical food-label table is now
    the ``harmonize_food`` table inside
    ``lsms_library/countries/Uganda/_/categorical_mapping.org`` (formerly
    the standalone ``food_items.org``).  When ``fn`` is ``None`` (the
    default — used by ``food_acquired`` and ``nutrition.org``) the mapping
    is read from that org table via ``get_categorical_mapping``, mirroring
    ``harmonized_unit_labels`` so foods and units share one source-of-truth
    file and become joinable with crop / community-price ``j`` axes.

    A non-``None`` ``fn`` keeps the legacy ``|``-delimited org-CSV reader
    so the *nonfood* path (``nonfood_items.org``) is unaffected.

    Codes are coerced to ``int`` so the mapping matches the integer item
    codes carried in the ``j`` index of ``food_acquired`` (the raw
    ``.dta`` item codes are ``int16``; ``hash(100) == hash(100.0)`` but a
    string key would not match).
    """
    if fn is None:
        from lsms_library.local_tools import get_categorical_mapping

        raw = get_categorical_mapping(tablename='harmonize_food',
                                      idxvars=key,
                                      **{value: value})

        labels = {}
        for k, v in raw.items():
            try:
                int_k = int(k)
            except (TypeError, ValueError):
                int_k = k
            if pd.isna(v):
                continue
            labels[int_k] = str(v).strip()
        return labels

    # Legacy path: explicit standalone org-CSV file (e.g. nonfood_items.org).
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[[key,value]].dropna()
    food_items = food_items.set_index(key)

    return food_items.squeeze().str.strip().to_dict()


def food_acquired(fn,myvars):

    df = get_dataframe(fn,convert_categoricals=False)

    df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})

    # Replace missing unit values
    df['units'] = df['units'].fillna('---')

    df = df.set_index(['HHID','item','units']).dropna(how='all')

    df.index.names = ['i','j','u']  # HHID='i', item='j', units='u'


    # Fix type of hhids if need be
    if df.index.get_level_values('i').dtype ==float:
        fix = dict(zip(df.index.levels[0],df.index.levels[0].astype(int).astype(str)))
        df = df.rename(index=fix,level=0)

    df = df.rename(index=harmonized_food_labels(),level='j')
    unitlabels = harmonized_unit_labels()
    df = df.rename(index=unitlabels,level='u')

    if not 'market' in df.columns:
        df['market'] = df.filter(regex='^market').median(axis=1)

    # Compute unit values
    df['unitvalue_home'] = df['value_home']/df['quantity_home']
    df['unitvalue_away'] = df['value_away']/df['quantity_away']
    df['unitvalue_own'] = df['value_own']/df['quantity_own']
    df['unitvalue_inkind'] = df['value_inkind']/df['quantity_inkind']

    # Get list of units used in current survey
    units = list(set(df.index.get_level_values('u').tolist()))

    unknown_units = set(units).difference(unitlabels.values())
    if len(unknown_units):
        warnings.warn("Dropping some unknown unit codes!")
        print(unknown_units)
        df = df.loc[df.index.isin(unitlabels.values(),level='u')]

    with open('../../_/conversion_to_kgs.json','r') as f:
        conversion_to_kgs = pd.Series(json.load(f))

    conversion_to_kgs.name='Kgs'
    conversion_to_kgs.index.name='u'

    df = df.join(conversion_to_kgs,on='u')
    df = df.astype(float)

    return df


def food_acquired_to_canonical(df):
    """Reshape Uganda wide-form ``food_acquired`` to canonical long form.

    Phase 3 of GH #169 / DESIGN_food_acquired_canonical_2026-05-05.org.

    Inputs
    ------
    df : DataFrame
        Output of :func:`food_acquired` (one row per ``(i, j, u)`` triple)
        plus a ``t`` index level supplied by the caller (so the canonical
        index ``(t, i, j, u, s)`` is producible without inferring the wave
        label here).  Recognized columns:

        * ``value_home``, ``value_away``, ``quantity_home``, ``quantity_away``
          (consumption-location splits of *purchased* food; folded together
          here because the home/away distinction is consumption-location,
          not acquisition-source -- see design doc)
        * ``value_own``, ``quantity_own``  (own production)
        * ``value_inkind``, ``quantity_inkind``  (in-kind receipts)
        * ``market`` (market price per unit ``u``; preserved for ``s='purchased'``)
        * ``farmgate`` (farmgate price per unit ``u``; preserved for ``s='produced'``)

        Other columns (``unitvalue_*``, ``Kgs``, ``market_home``/``_away``/
        ``_own``) are ignored -- ``unitvalue_*`` are derived (= value/quantity)
        and ``Kgs`` is per-unit metadata not in the canonical schema.

    Output
    ------
    DataFrame indexed by canonical ``(t, i, j, u, s)`` with columns
    ``Quantity``, ``Expenditure``, ``Price``.  ``s`` ∈
    ``{'purchased', 'produced', 'inkind'}``.

    Reshape rules
    -------------
    Each input row becomes up to 3 long-form rows:

    * ``s = 'purchased'`` -- ``Quantity = quantity_home + quantity_away``,
      ``Expenditure = value_home + value_away``, ``Price = market``
    * ``s = 'produced'``  -- ``Quantity = quantity_own``,
      ``Expenditure = value_own``, ``Price = farmgate``
    * ``s = 'inkind'``    -- ``Quantity = quantity_inkind``,
      ``Expenditure = value_inkind``, ``Price = NaN`` (Uganda surveys
      do not record an imputed valuation distinct from value_inkind)

    Rows are kept where EITHER ``Quantity > 0`` OR ``Expenditure > 0``.
    Expenditure-only rows (HH reported a food expenditure with no
    quantity — common in Uganda's GSEC15b for food consumed away from
    home) are legitimate data and are carried through with NaN
    ``Quantity``.  Matches the shared
    :func:`lsms_library.transformations.food_acquired_to_canonical` rule.

    Notes
    -----
    - The ``home``/``away`` consumption-location distinction is dropped
      at the canonical layer per DESIGN_food_acquired_canonical_2026-05-05.
      Users who care about it can read the wave-level pre-canonical
      DataFrame directly.
    - ``v`` is intentionally absent -- the framework joins it from
      ``sample()`` at API time.  See CLAUDE.md "## ``sample()`` and
      Cluster Identity".
    - ``Price`` is carried for purchased / produced rows from the
      survey-reported ``market`` / ``farmgate`` columns.  The framework's
      ``food_prices_from_acquired`` currently re-derives Price from
      ``Expenditure / Quantity_kg`` and ignores a stored Price; the
      stored Price preserves the survey-reported information for
      consumers reading the wave parquet directly, and is forward-
      compatible with a future framework change to prefer stored Price
      where available (per DESIGN doc).
    """
    work = df.reset_index()

    # Build the three per-source pieces.
    def _make(source_label, qty, expenditure, price):
        out = pd.DataFrame({
            't': work['t'].values,
            'i': work['i'].values,
            'j': work['j'].values,
            'u': work['u'].values,
            's': source_label,
            'Quantity': pd.to_numeric(qty, errors='coerce').values,
            'Expenditure': pd.to_numeric(expenditure, errors='coerce').values,
            'Price': pd.to_numeric(price, errors='coerce').values,
        })
        return out

    # Purchased: fold home + away.  Sum with min_count=1 so a row with
    # both NaN stays NaN (and is dropped below); a row with one value
    # populated keeps that value.
    purchased_qty = work[['quantity_home', 'quantity_away']].sum(
        axis=1, min_count=1)
    purchased_val = work[['value_home', 'value_away']].sum(
        axis=1, min_count=1)
    purchased_price = (work['market']
                       if 'market' in work.columns else pd.Series(np.nan,
                                                                   index=work.index))

    purchased = _make('purchased', purchased_qty, purchased_val,
                      purchased_price)
    produced  = _make('produced',  work['quantity_own'], work['value_own'],
                      work['farmgate'] if 'farmgate' in work.columns
                      else pd.Series(np.nan, index=work.index))
    inkind    = _make('inkind',    work['quantity_inkind'],
                      work['value_inkind'],
                      pd.Series(np.nan, index=work.index))

    from lsms_library.transformations import _finalize_canonical_food_acquired

    out = pd.concat([purchased, produced, inkind], ignore_index=True)
    # Filter (qty>0 | exp>0; expenditure-only rows kept with NaN Quantity --
    # GH #246 C-2) and aggregate genuine source-data duplicates (e.g. two
    # ``Other (Specify)`` rows under one canonical key) via the shared tail
    # (GH #251): Quantity/Expenditure summed with min_count=1, per-unit
    # Price averaged.
    out = _finalize_canonical_food_acquired(out)
    return out


def nonfood_expenditures(fn='', purchased=None, away=None, produced=None,
                         given=None, item='item', HHID='HHID'):
    """Uganda non-food expenditures from a single .dta file.

    Aggregates across three or four source columns (purchased, away,
    produced, given) at the (HHID, item) level and returns a wide
    matrix (HHID rows x item columns) of total expenditures.

    Replaces the prior lsms.tools.get_food_expenditures-based
    implementation with an inline pandas groupby+sum; the upstream
    lsms dependency is being retired.
    """
    if __name__ == '__main__':
        from local_tools import get_dataframe
    else:
        from lsms_library.local_tools import get_dataframe

    nonfood_items = harmonized_food_labels(
        fn='../../_/nonfood_items.org', key='Code', value='Preferred Label')

    # Read source file via the repo's standard entry point.
    df = get_dataframe(fn, convert_categoricals=False)

    # Gather source columns (skip None entries).
    source_cols = {
        'purchased': purchased,
        'away':      away,
        'produced':  produced,
        'given':     given,
    }
    source_cols = {k: v for k, v in source_cols.items() if v is not None}

    # Project down to the columns we need and rename.
    keep = [HHID, item] + list(source_cols.values())
    df = df[keep].copy()
    rename_map = {HHID: 'HHID', item: 'itmcd'}
    rename_map.update({v: k for k, v in source_cols.items()})
    df = df.rename(columns=rename_map)

    # Coerce itmcd to numeric, drop missing item codes, cast to int.
    df['itmcd'] = pd.to_numeric(df['itmcd'], errors='coerce')
    df = df.dropna(subset=['itmcd'])
    df['itmcd'] = df['itmcd'].astype(int)

    # Handle HHID-as-float-string (see upstream lsms.tools lines 101-108).
    try:
        first = df['HHID'].iloc[0]
        if isinstance(first, str) and first.split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError, IndexError):
        pass

    # Replace itmcd codes with preferred labels BEFORE groupby so that
    # items sharing a label are merged naturally by the groupby.
    # Keep only rows with a recognized item code.
    df['itmcd'] = df['itmcd'].replace(nonfood_items)
    df = df[df['itmcd'].isin(nonfood_items.values())]

    # Sum source columns, groupby HHID+itmcd (now label names).
    active_sources = list(source_cols.keys())
    df['total'] = df[active_sources].sum(axis=1, min_count=1)
    wide = df.groupby(['HHID', 'itmcd'])['total'].sum().unstack('itmcd')
    wide = wide.fillna(0)

    # Match the old output's index/column names.
    wide.index.name = 'j'
    wide.columns.name = 'i'
    return wide


def id_walk(df, updated_ids, hh_index='i'):
    '''
    Updates household IDs in panel data across different waves separately.

    Parameters:
        df (DataFrame): Panel data with a MultiIndex, including 't' for wave and 'i' (default) for household ID.
        updated_ids (dict): A dictionary mapping each wave to another dictionary that maps original household IDs to updated IDs.
            Format:
                {wave_1: {original_id: new_id, ...},
                 wave_2: {original_id: new_id, ...}, ...}
        hh_index (str): Index name for the household ID level (default is 'i').

    Example:
        updated_ids = {
            '2013-14': {'0001-001': '101012150028', '0009-001': '101015620053', '0005-001': '101012150022'},
            '2016-17': {'0001-002': '0001-001', '0003-001': '0005-001', '0005-001': '0009-001'}
        }

        In this example, IDs are updated independently for each wave.
        Because the same original household ID across different waves may not represent the same household.
        Specifically, household '0005-001' in wave '2016-17' corresponds to household '0009-001' from wave '2013-14', not '0005-001' from '2013-14'.

    The function handles these wave-specific mappings separately, ensuring accurate household identification over time.
    '''
    index_names = list(df.index.names or [])
    if not index_names:
        raise ValueError("Dataframe must have a named MultiIndex for id_walk.")
    if 't' not in index_names:
        raise KeyError("Index must contain a 't' level for wave identifiers.")

    household_level = hh_index
    fallback_used = False
    if household_level not in index_names:
        for candidate in ('i', 'j'):
            if candidate in index_names:
                household_level = candidate
                fallback_used = True
                break
        else:
            # fallback to the first non-'t' level (or level 0)
            non_wave_levels = [name for name in index_names if name != 't']
            if not non_wave_levels:
                raise KeyError("Cannot determine household index level for id_walk.")
            household_level = non_wave_levels[0]
            fallback_used = True

    household_level_pos = index_names.index(household_level)

    if fallback_used and household_level != hh_index:
        warnings.warn(
            f"id_walk expected index level '{hh_index}' but found '{household_level}'. "
            "Proceeding with the detected household index."
        )

    #seperate df into different waves:
    dfs = {}
    waves = df.index.get_level_values('t').unique()
    for wave in waves:
        dfs[wave] = df[df.index.get_level_values('t') == wave].copy()
    #update ids for each wave
    for wave, df_wave in dfs.items():
        #update ids
        if wave in updated_ids:
            df_wave = df_wave.rename(index=updated_ids[wave], level=household_level)
            #update the dataframe with the new ids
            dfs[wave] = df_wave
        else:
            continue
    #combine the updated dataframes
    df = pd.concat(dfs.values(), axis=0)

    if 'i' not in df.index.names or household_level != 'i':
        df.index = df.index.set_names('i', level=household_level_pos)

    # df= df.rename(index=updated_ids,level=['t', household_level])
    df.attrs['id_converted'] = True
    return df


# ---------------------------------------------------------------------
# plot_features (GH #167 Phase 1 pilot)
# ---------------------------------------------------------------------

ACRES_PER_HECTARE = 2.471053814671653  # 1 ha = 2.471... acres
HECTARES_PER_ACRE = 1.0 / ACRES_PER_HECTARE  # 0.404686 ha / acre


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a Code -> Preferred Label dict from categorical_mapping.org.

    Mirrors the shape of ``harmonized_unit_labels`` but returns integer
    keys directly (no '---' sentinel restoration; for our use NaN
    Preferred Labels mean "leave the column NaN").
    """
    from lsms_library.local_tools import get_categorical_mapping

    raw = get_categorical_mapping(tablename=tablename, idxvars=key,
                                  **{value: value})
    out = {}
    for k, v in raw.items():
        try:
            int_k = int(k)
        except (TypeError, ValueError):
            int_k = k
        if pd.isna(v) or str(v).strip() in ('---', ''):
            out[int_k] = pd.NA
        else:
            out[int_k] = str(v).strip()
    return out


def _harmonize_acquire_table():
    """Load harmonize_acquire from categorical_mapping.org and return
    a {(Wave, File, Code): Preferred Label} dict for three-key lookup.

    The acquire codes mean different things across waves (GH #167), so
    the table is wave-keyed.  Codes absent from the table map to NaN
    (no silent default)."""
    from lsms_library.local_tools import df_from_orgfile

    # Resolve org file relative to this module (search ../../_ first so
    # wave-script CWDs still find it).
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, 'categorical_mapping.org'),
        os.path.abspath(os.path.join('..', '..', '_', 'categorical_mapping.org')),
        'categorical_mapping.org',
    ]
    orgfn = next((c for c in candidates if os.path.exists(c)), candidates[0])

    df = df_from_orgfile(orgfn, name='harmonize_acquire',
                         set_columns=True, to_numeric=True)
    out = {}
    for _, row in df.iterrows():
        wv = str(row['Wave']).strip()
        f = str(row['File']).strip()
        c = row['Code']
        try:
            c = int(c)
        except (TypeError, ValueError):
            pass
        lab = row.get('Preferred Label')
        if pd.isna(lab):
            continue
        out[(wv, f, c)] = str(lab).strip()
    return out


def _map_codes(series, code_map):
    """Map a categorical or numeric Series through ``code_map`` (a
    {int: str} dict).  Returns a string Series with NaN where the
    code is not in the map."""
    if series is None:
        return None
    # Stata categoricals come through as strings if convert_categoricals
    # is True.  We need the underlying integer code for the map lookup.
    if pd.api.types.is_categorical_dtype(series) or series.dtype == object:
        # Reverse map: build {label: code} from the categorical, then
        # invert.  Easier path: re-read the value labels from the raw
        # data.  But for our purposes, the harmonized tables were
        # written against the integer codes; if the wave loaded
        # categoricals, the label string IS the value.  So we map
        # by code only when the dtype is numeric; otherwise treat the
        # string value as already canonical (lowercased + spelling
        # normalized via the harmonize table's *value* column).
        # ---
        # Simpler approach: build a reverse lookup of label_lower -> code,
        # then look up.  For tenure/soil/water tables, the value_labels
        # are the source-survey labels, not our preferred labels — so
        # the simple path doesn't work.  We require an int-keyed series.
        # If we received strings, attempt to recover the int code by
        # forcing convert_categoricals=False in the caller, OR by
        # parsing the label.  For now, raise — callers must pass
        # numeric codes.
        raise TypeError(
            f"_map_codes expects a numeric Series (raw Stata codes); "
            f"got dtype={series.dtype}.  Re-load the source with "
            f"convert_categoricals=False, or pass the integer-coded "
            f"underlying values.")
    # Numeric: convert to nullable Int64 (NaN-safe) and map
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_features_for_wave(t, source_2a, source_2b, colmap):
    """Build canonical ``plot_features`` for one Uganda UNPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2013-14"``), used as the ``t`` index value.
    source_2a, source_2b : pd.DataFrame | None
        Raw AGSEC2A (owned parcels) and AGSEC2B (use-rights parcels)
        DataFrames, loaded via ``get_dataframe(..., convert_categoricals=False)``
        so the categorical columns carry integer codes.  ``None`` is
        permitted when a wave lacks one of the source files.
    colmap : dict
        Per-wave column-name map.  Required keys (for any source the
        caller passes):
            hhid           — household id column
            parcel_id      — within-HH parcel sequence column
        Optional keys (NaN where omitted or absent in source):
            area_gps       — GPS-measured parcel area (acres)
            area_est       — farmer-estimated parcel area (acres)
            tenure_system  — Tenure System question (a2aq7 / s2aq7 / ...)
            acquire        — How-acquired question (a2aq8 / s2aq8 / ...)
            soil_type      — Soil-type question
            water_source   — Main-water-source question
            certificate    — Formal certificate-of-title question
                             (a2aq25 / a2aq23 / s2aq23; AGSEC2A only —
                             use-rights parcels in AGSEC2B are never
                             titled, so the question isn't asked there).
            erosion        — Erosion-control / water-harvesting-facility
                             question (a2aq24 / a2aq22a / s2aq22a in
                             AGSEC2A; a2bq23 / a2bq20a / s2aq22a in
                             AGSEC2B).  May be a numeric method code
                             (8/'None' -> no facility, other -> facility)
                             or, in 2009-10, a free-text method string
                             (empty -> NA, any non-empty code -> facility).
        Each value is the column name in the corresponding source df.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares, float), ``AreaUnit`` (str, always 'acres'),
        ``Tenure`` (str), ``TenureSystem`` (str), ``SoilType`` (str),
        ``Irrigated`` (bool nullable), and the GAP-6 parity item columns
        ``SelfReportedArea`` (hectares, float — the *reported* farmer
        estimate, distinct from ``Area`` which prefers the GPS measure),
        ``PlotOwned`` (bool nullable), ``PlotCertificate`` (bool
        nullable), and ``ErosionProtection`` (bool nullable).  GPS
        columns (Latitude / Longitude) are reserved in the canonical
        Columns block but not emitted here — Uganda's DMS encoding in
        AGSEC2A is non-standard and mostly NaN; revisit in a follow-up.

    Notes on the GAP-6 reported item columns (audit add, 2026-06-14)
    ----------------------------------------------------------------
    These are REPORTED per-parcel attributes the UNPS questionnaire
    records, mirroring the WB LSMS-ISA harmonised Plot_dataset item
    fields ``self_reported_area`` / ``plot_owned`` / ``plot_certificate``
    / ``erosion_protection`` (UGA_UNPS1.do:396-650).  We store the
    reported booleans only; the WB aggregates ``farm_size`` (Σ area),
    ``nb_plots`` / ``nb_fallow_plots`` (counts) and ``soil_fertility_index``
    (PCA over geospatial soil-quality rasters) are transformations and are
    NOT stored.  ``plot_slope`` and the soil-quality tags are dropped
    entirely: they come from the GPS ``geovars`` raster file keyed on the
    household (not the parcel) and are not survey-reported plot attributes.
    """
    tenure_system_map = _harmonized_codes('harmonize_tenure_system')
    soil_map = _harmonized_codes('harmonize_soil')
    water_map = _harmonized_codes('harmonize_water')
    acquire_map = _harmonize_acquire_table()

    pieces = []
    for letter, src in (('A', source_2a), ('B', source_2b)):
        if src is None or src.empty:
            continue

        # Build the per-row canonical frame
        c = colmap  # alias
        hh = _format_agsec_hhid(src[c['hhid']], t)
        parcel = src[c['parcel_id']].apply(format_id)
        plot_id = parcel.astype(str) + f'_{letter}'

        # Area: prefer GPS-measured, fall back to farmer estimate.
        area_acres = pd.Series(pd.NA, index=src.index, dtype='Float64')
        if c.get('area_gps') in src.columns:
            area_acres = pd.to_numeric(src[c['area_gps']], errors='coerce').astype('Float64')
        if c.get('area_est') in src.columns:
            est = pd.to_numeric(src[c['area_est']], errors='coerce').astype('Float64')
            area_acres = area_acres.where(area_acres.notna(), est)
        # Plausibility clamp: > 2500 acres (~1000 ha) is a data-entry
        # error for Ugandan smallholder parcels (observed 8093 ha in
        # 2005-06); drop to NaN so AreaUnit follows, rather than poison
        # area-weighted aggregates downstream (GH #167).
        area_acres = area_acres.where((area_acres <= 2500) | area_acres.isna(), pd.NA)
        area_ha = area_acres * HECTARES_PER_ACRE

        area_unit = pd.Series(['acres'] * len(src), index=src.index, dtype='string')
        # Where area is NaN, leave AreaUnit NaN too (no measurement = no unit).
        area_unit = area_unit.where(area_acres.notna(), pd.NA)

        # SelfReportedArea (GAP-6 parity): the farmer-estimated parcel
        # size, kept as its own REPORTED column in hectares.  Distinct
        # from Area, which prefers the GPS measure and only falls back to
        # this estimate.  WB: area_self_reported = A2aq5 * 0.404686.
        self_reported_ha = pd.Series(pd.NA, index=src.index, dtype='Float64')
        if c.get('area_est') in src.columns:
            sr_acres = pd.to_numeric(src[c['area_est']], errors='coerce').astype('Float64')
            # Same plausibility clamp as Area (drop > 2500 acres).
            sr_acres = sr_acres.where((sr_acres <= 2500) | sr_acres.isna(), pd.NA)
            self_reported_ha = sr_acres * HECTARES_PER_ACRE

        # TenureSystem (Freehold/Leasehold/Mailo/Customary/...)
        tenure_system = pd.Series(pd.NA, index=src.index, dtype='string')
        ts_col = c.get('tenure_system')
        if ts_col and ts_col in src.columns:
            tenure_system = _map_codes(src[ts_col], tenure_system_map)

        # Tenure: wave-keyed acquire-mode code -> canonical tenure.  The
        # same raw code means different things across waves (e.g. 2B
        # code 1 = 'purchased' in 2005-06 but 'agreement' in 2009-15),
        # so the lookup is keyed on (wave, File, Code).  Unmapped codes
        # ('Do not know', etc.) and absent acquire columns stay NaN --
        # NO silent file-letter default (GH #167; the old default
        # mislabelled ~85% of 2005-06 2B rows and made 2A content-free).
        acq_col = c.get('acquire')
        tenure = pd.Series(pd.NA, index=src.index, dtype='string')
        if acq_col and acq_col in src.columns:
            acq = src[acq_col].astype('Int64')
            tenure = acq.map(lambda code: acquire_map.get((t, f'2{letter}', int(code)))
                             if pd.notna(code) else pd.NA).astype('string')

        # SoilType
        soil_type = pd.Series(pd.NA, index=src.index, dtype='string')
        soil_col = c.get('soil_type')
        if soil_col and soil_col in src.columns:
            soil_type = _map_codes(src[soil_col], soil_map)

        # Irrigated boolean derived from water_source
        irrigated = pd.Series(pd.NA, index=src.index, dtype='boolean')
        water_col = c.get('water_source')
        if water_col and water_col in src.columns:
            water_label = _map_codes(src[water_col], water_map)
            irrigated = (water_label == 'Irrigated').astype('boolean')
            # Where water_label is NaN, leave irrigated as NaN too
            irrigated = irrigated.where(water_label.notna(), pd.NA)

        # PlotCertificate (GAP-6 parity): reported formal/customary
        # certificate-of-title flag.  Source codes 1-3 = a certificate
        # type (title / customary / occupancy) -> True; 4 = 'No document'
        # -> False; anything else / missing -> NA.  Only AGSEC2A (owned
        # parcels) asks this — use-rights AGSEC2B parcels are never
        # titled, so the column is absent there and stays NA.
        # WB: recode A2aq25 (1/3 = 1 "Yes") (4 = 0 "No").
        plot_certificate = pd.Series(pd.NA, index=src.index, dtype='boolean')
        cert_col = c.get('certificate')
        if cert_col and cert_col in src.columns:
            cert = pd.to_numeric(src[cert_col], errors='coerce').astype('Int64')
            plot_certificate = pd.Series(pd.NA, index=src.index, dtype='boolean')
            plot_certificate = plot_certificate.mask(cert.isin([1, 2, 3]), True)
            plot_certificate = plot_certificate.mask(cert == 4, False)

        # PlotOwned (GAP-6 parity): reported ownership flag derived from
        # the how-acquired question.  AGSEC2A acquire codes 1 (Purchased)
        # / 2 (Inherited/gift) -> owned (True); 3 (Leased-in) / 4 (Just
        # walked in) -> not owned (False); 5 (Do not know) / 6 (Other) ->
        # NA.  In the 2018-19+ merged code list, 6/7 (given by local
        # authorities / government) are treated as owned and 8/9
        # (agreement / without agreement) as use-rights (not owned).
        # AGSEC2B parcels are use-rights by construction -> False.
        # A parcel with a certificate is owned regardless of acquire
        # mode (WB: replace plot_owned = 1 if plot_certificate==1).
        plot_owned = pd.Series(pd.NA, index=src.index, dtype='boolean')
        acq_owned_codes = [1, 2, 6, 7]
        acq_notowned_codes = [3, 4, 8, 9]
        if letter == 'B':
            # Use-rights parcels: never owned (acquire codes are
            # agreement / without-agreement / other).
            plot_owned = pd.Series(False, index=src.index, dtype='boolean')
            # But leave NA where the source row carries no acquire info
            # at all (defensive — keeps parity with the 2A path).
            if acq_col and acq_col in src.columns:
                acq_b = src[acq_col].astype('Int64')
                plot_owned = plot_owned.where(acq_b.notna(), pd.NA)
        elif acq_col and acq_col in src.columns:
            acq_a = src[acq_col].astype('Int64')
            plot_owned = pd.Series(pd.NA, index=src.index, dtype='boolean')
            plot_owned = plot_owned.mask(acq_a.isin(acq_owned_codes), True)
            plot_owned = plot_owned.mask(acq_a.isin(acq_notowned_codes), False)
        # Certificate implies ownership.
        plot_owned = plot_owned.mask(plot_certificate == True, True)

        # ErosionProtection (GAP-6 parity): reported presence of an
        # erosion-control / water-harvesting facility on the parcel.
        # The source column is either a numeric method code (later
        # waves: 8 / 'none' -> no facility -> False, any other listed
        # method -> True) or, in 2009-10, a free-text multi-method
        # string (empty -> NA, any non-empty code -> a facility is
        # present -> True).  WB: encode then recode 'None' levels -> 0,
        # else -> 1 (UGA_UNPS1.do:644-648).
        erosion_protection = pd.Series(pd.NA, index=src.index, dtype='boolean')
        erosion_col = c.get('erosion')
        if erosion_col and erosion_col in src.columns:
            es = src[erosion_col]
            if pd.api.types.is_numeric_dtype(es):
                en = pd.to_numeric(es, errors='coerce').astype('Int64')
                erosion_protection = pd.Series(pd.NA, index=src.index, dtype='boolean')
                # 8 == 'None' in the method codelist.
                erosion_protection = erosion_protection.mask(en == 8, False)
                erosion_protection = erosion_protection.mask(
                    en.notna() & (en != 8), True)
            else:
                # Free-text multi-method string (2009-10 / 2010-11).  The
                # column holds concatenated method-letter codes; a genuine
                # method present -> a facility exists (True).  Placeholder
                # non-answers ('', '0', '-', '.', 'none') carry no
                # facility information and stay NA.  We deliberately do
                # NOT reproduce the WB encode-then-recode-by-level logic
                # (UGA_UNPS2.do:641-648), which is brittle to the
                # alphabetical encode order.
                es_str = es.astype('string').str.strip()
                placeholder = {'', '0', '-', '.', 'none'}
                is_placeholder = es_str.isna() | es_str.str.lower().isin(placeholder)
                erosion_protection = pd.Series(pd.NA, index=src.index, dtype='boolean')
                erosion_protection = erosion_protection.mask(~is_placeholder, True)

        # GPS deferred for v1.  Uganda's DMS encoding in AGSEC2A is
        # non-standard (Minutes ranges 0-99, Seconds 0-999) and the
        # columns are mostly NaN; revisit when a maintainer with
        # Uganda-specific knowledge can confirm the encoding.
        # Canonical Latitude / Longitude in data_info.yml stay
        # reserved for future countries (e.g. Ethiopia ESS) that
        # have decimal-degree plot GPS.

        piece = pd.DataFrame({
            't':                t,
            'i':                hh.values,
            'plot_id':          plot_id.values,
            'Area':             area_ha.values,
            'AreaUnit':         area_unit.values,
            'Tenure':           tenure.values,
            'TenureSystem':     tenure_system.values,
            'SoilType':         soil_type.values,
            'Irrigated':        irrigated.values,
            'SelfReportedArea': self_reported_ha.values,
            'PlotOwned':        plot_owned.values,
            'PlotCertificate':  plot_certificate.values,
            'ErosionProtection': erosion_protection.values,
        })
        pieces.append(piece)

    if not pieces:
        return pd.DataFrame(
            columns=['Area','AreaUnit','Tenure','TenureSystem',
                     'SoilType','Irrigated','SelfReportedArea',
                     'PlotOwned','PlotCertificate','ErosionProtection'])

    df = pd.concat(pieces, ignore_index=True)
    df = df.set_index(['t', 'i', 'plot_id'])
    return df



# ----------------------------------------------------------------------
# crop_production  (GAP 1 — item-level post-harvest crop module)
# ----------------------------------------------------------------------
#
# Grain: (t, i, plot, j, u, season).  One row per *reported* harvest
# record for a crop on a plot.  Stores REPORTED values only — Quantity
# (native harvest unit u), Quantity_sold, Value_sold, harvest_month and
# the intercropped / perennial flags.  No harvest_kg / yield / main_crop /
# value-share — those are transformations over these item rows.
#
# Source: AGSEC5A (season 1) + AGSEC5B (season 2), the UNPS post-harvest
# crop module.  Column names AND the unit/condition column semantics drift
# across waves (see slurm notes in the wave scripts), so each wave passes
# an explicit colmap.  Some newer waves (2018-19, 2019-20) record two
# harvest "conditions" per (plot, crop) in parallel _1 / _2 column sets;
# we emit one row per non-empty condition rather than summing them.
#
# plot id mirrors the WB harmonised plot_id = hhid-parcel-plot; its parcel
# component (hhid-parcel) is the same parcel that plot_features keys on
# (plot_features uses the coarser parcel grain with an _A/_B source tag).

_CROP_TABLE = 'harmonize_crop'
_HARVEST_UNIT_TABLE = 'harvest_units'


def _crop_label_map():
    return _harmonized_codes(_CROP_TABLE)


def _harvest_unit_map():
    return _harmonized_codes(_HARVEST_UNIT_TABLE)


def _to_int_code(series):
    """Coerce a (possibly categorical/float/str) code column to Int64."""
    if series is None:
        return None
    if pd.api.types.is_categorical_dtype(series):
        # When convert_categoricals=False the categories ARE the codes;
        # otherwise fall back to numeric coercion of the string form.
        try:
            return series.astype('Int64')
        except (TypeError, ValueError):
            return pd.to_numeric(series.astype(str), errors='coerce').astype('Int64')
    return pd.to_numeric(series, errors='coerce').astype('Int64')


def crop_production_for_wave(t, df5a, df5b, df4a, colmap):
    """Build canonical ``crop_production`` for one Uganda UNPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2013-14"``).
    df5a, df5b : pd.DataFrame | None
        Raw AGSEC5A (season 1) / AGSEC5B (season 2) post-harvest crop
        modules, loaded with ``convert_categoricals=False`` so code
        columns carry integer codes.  ``None`` permitted.
    df4a : pd.DataFrame | None
        Raw AGSEC4A plot-crop roster (for the intercropped flag and,
        where available, the perennial flag).  ``None`` permitted; when
        absent the flags are NaN.
    colmap : dict
        Per-(season) column maps keyed by ``'A'`` / ``'B'``.  Each value
        is a dict with keys:
            hhid, parcel, plot, crop       — id columns
            conditions : list of dicts, one per parallel harvest-record
                         set, each with keys:
                qty           — reported harvest quantity column
                unit          — reported harvest unit code column (or None)
                qty_sold      — reported quantity sold column (or None)
                value_sold    — reported sale value column (or None)
                month         — harvest-end month code column (or None)
        plus an optional top-level key ``cf`` listing per-condition CF
        columns (unused for storage; documented for transformations).
    intercrop_map : (passed via colmap['intercrop']) optional dict
            file_hhid, file_parcel, file_plot, flag, [perennial]
        describing how to read the intercropped flag from ``df4a``.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot, j, u, season)`` with columns
        ``Quantity`` (Float64), ``Quantity_sold`` (Float64),
        ``Value_sold`` (Float64), ``harvest_month`` (Int64 1-12) and
        ``intercropped`` (boolean).  The ``perennial`` / ``planting_month``
        lookups are wired but not emitted — no current Uganda wave
        populates them cleanly (they would be all-null).
    """
    crop_map = _crop_label_map()
    unit_map = _harvest_unit_map()

    # --- intercropped / perennial / planting from AGSEC4A (plot-crop) ---
    inter_lookup = {}      # (hh, parcel, plot) -> bool   (plot-level flag)
    perennial_lookup = {}  # (hh, parcel, plot, crop) -> bool
    planting_lookup = {}   # (hh, parcel, plot, crop) -> Int month
    ic = colmap.get('intercrop')
    if df4a is not None and ic is not None:
        hh4 = _format_agsec_hhid(df4a[ic['hhid']], t)
        pa4 = df4a[ic['parcel']].apply(format_id)
        pl4 = df4a[ic['plot']].apply(format_id)
        key3 = list(zip(hh4, pa4, pl4))
        if ic.get('flag') and ic['flag'] in df4a.columns:
            flagcode = _to_int_code(df4a[ic['flag']])
            # 1 = mono/No, 2 = Yes  (recode mirrors WB: 2 -> True)
            for k, c in zip(key3, flagcode):
                if pd.notna(c):
                    inter_lookup[k] = bool(int(c) == 2)
        if ic.get('crop') and ic['crop'] in df4a.columns:
            crop4 = _to_int_code(df4a[ic['crop']])
            key4 = list(zip(hh4, pa4, pl4, crop4))
            if ic.get('perennial') and ic['perennial'] in df4a.columns:
                per = _to_int_code(df4a[ic['perennial']])
                for k, c in zip(key4, per):
                    if pd.notna(c):
                        perennial_lookup[k] = bool(int(c) == 2)
            if ic.get('planting_month') and ic['planting_month'] in df4a.columns:
                pm = _to_int_code(df4a[ic['planting_month']])
                for k, m in zip(key4, pm):
                    if pd.notna(m) and 1 <= int(m) <= 12:
                        planting_lookup[k] = int(m)

    pieces = []
    for season, df5 in (('A', df5a), ('B', df5b)):
        if df5 is None or len(df5) == 0:
            continue
        cm = colmap.get(season)
        if cm is None:
            continue

        hh = _format_agsec_hhid(df5[cm['hhid']], t)
        parcel = df5[cm['parcel']].apply(format_id)
        plot = df5[cm['plot']].apply(format_id) if cm.get('plot') and cm['plot'] in df5.columns else pd.Series(['']*len(df5), index=df5.index)
        plot_id = hh.astype(str) + '-' + parcel.astype(str) + '-' + plot.astype(str)
        crop_code = _to_int_code(df5[cm['crop']])
        j = crop_code.map(lambda c: crop_map.get(int(c), pd.NA) if pd.notna(c) else pd.NA)

        for cond in cm['conditions']:
            qcol = cond.get('qty')
            if not qcol or qcol not in df5.columns:
                continue
            qty = pd.to_numeric(df5[qcol], errors='coerce')

            # reported native unit
            if cond.get('unit') and cond['unit'] in df5.columns:
                ucode = _to_int_code(df5[cond['unit']])
                u = ucode.map(lambda c: unit_map.get(int(c), pd.NA) if pd.notna(c) else pd.NA)
            else:
                u = pd.Series([pd.NA]*len(df5), index=df5.index, dtype='object')

            qsold = pd.to_numeric(df5[cond['qty_sold']], errors='coerce') if cond.get('qty_sold') in df5.columns else pd.Series([pd.NA]*len(df5), index=df5.index)
            vsold = pd.to_numeric(df5[cond['value_sold']], errors='coerce') if cond.get('value_sold') in df5.columns else pd.Series([pd.NA]*len(df5), index=df5.index)

            if cond.get('month') and cond['month'] in df5.columns:
                hm = _to_int_code(df5[cond['month']])
                hm = hm.where((hm >= 1) & (hm <= 12), pd.NA)
            else:
                hm = pd.Series([pd.NA]*len(df5), index=df5.index, dtype='Int64')

            piece = pd.DataFrame({
                't':             t,
                'i':             hh.values,
                'plot':          plot_id.values,
                'j':             j.values,
                'u':             u.values,
                'season':        season,
                'Quantity':      qty.values,
                'Quantity_sold': qsold.values,
                'Value_sold':    vsold.values,
                'harvest_month': hm.values,
            })
            # intercropped flag (plot-level) joined from AGSEC4A.  The
            # perennial_lookup / planting_lookup hooks exist for future
            # waves but no current Uganda wave populates them cleanly, so
            # those columns are not emitted (they would be all-null).
            k3 = list(zip(hh.values, parcel.values, plot.values))
            piece['intercropped'] = [inter_lookup.get(k, pd.NA) for k in k3]
            pieces.append(piece)

    cols = ['Quantity', 'Quantity_sold', 'Value_sold', 'harvest_month',
            'intercropped']
    if not pieces:
        return pd.DataFrame(columns=cols)

    df = pd.concat(pieces, ignore_index=True)

    # Drop rows with no crop label and no quantity at all (empty source
    # rows / land-status placeholders with nothing reported).
    df = df[df['j'].notna()]
    # Keep rows even when Quantity is NaN but a sale was reported; drop
    # only when ALL reported measures are missing.
    measure_cols = ['Quantity', 'Quantity_sold', 'Value_sold']
    df = df[df[measure_cols].notna().any(axis=1)]

    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').astype('Float64')
    df['Quantity_sold'] = pd.to_numeric(df['Quantity_sold'], errors='coerce').astype('Float64')
    df['Value_sold'] = pd.to_numeric(df['Value_sold'], errors='coerce').astype('Float64')
    df['harvest_month'] = pd.to_numeric(df['harvest_month'], errors='coerce').astype('Int64')
    df['intercropped'] = df['intercropped'].astype('boolean')

    # u may be NaN (e.g. 2018-19 harvest side has no unit label); fill
    # with a sentinel so it can be an index level without null-index
    # failures, but ONLY where Quantity is present (a reported quantity
    # with no unit).  Where there's no quantity at all, leave the unit
    # sentinel too.
    df['u'] = df['u'].astype('object').where(df['u'].notna(), 'Unknown')

    df = df.set_index(['t', 'i', 'plot', 'j', 'u', 'season'])
    # Collapse exact-duplicate index tuples (same plot/crop/unit/season
    # reported twice) by summing the reported quantities — this is NOT an
    # aggregation across distinct items, just de-duplication of repeated
    # identical source rows so the index is unique.
    if not df.index.is_unique:
        num = df[['Quantity', 'Quantity_sold', 'Value_sold']].groupby(level=df.index.names).sum(min_count=1)
        firstcols = df[['harvest_month', 'intercropped']].groupby(level=df.index.names).first()
        df = num.join(firstcols)
    return df


# Per-wave column maps for crop_production_for_wave.  The harvest UNIT is
# the column whose value labels decode to Kg/Sack/Bunch (the harvest_units
# scheme) — empirically a5aq6c for 2009-16 (NOT a5aq6b, which is the
# Fresh/Dry condition; the WB .do's A5aq6b/A5aq6c unit/condition rename is
# inverted for these actual UNPS files).  2018-19's harvest side carries
# no unit label (-> u='Unknown'); 2019-20 keeps WB names s5aq06b_1.
CROP_COLMAPS = {
    '2009-10': {
        'A': {'hhid': 'HHID', 'parcel': 'a5aq1', 'plot': 'a5aq3', 'crop': 'a5aq5',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': None}]},
        'B': {'hhid': 'HHID', 'parcel': 'a5bq1', 'plot': 'a5bq3', 'crop': 'a5bq5',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': None}]},
        # 2009-10 AGSEC4A uses a non-standard column layout (a4aq1/a4aq2/
        # a4aq4, no parcel/plot/cropID in the form the join needs), so the
        # intercrop flag is not cleanly joinable -> intercropped is NaN
        # this wave.
        'intercrop': None,
    },
    '2010-11': {
        'A': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': None}]},
        'B': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': None}]},
        'intercrop': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
                      'flag': 'a4aq3', 'crop': 'cropID'},
    },
    '2011-12': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': 'a5aq6f'}]},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': 'a5bq6f'}]},
        'intercrop': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                      'flag': 'a4aq3', 'crop': 'cropID'},
    },
    '2013-14': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': 'a5aq6f'}]},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': 'a5bq6f'}]},
        'intercrop': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                      'flag': 'a4aq16', 'crop': 'cropID'},
    },
    '2015-16': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5aq6a', 'unit': 'a5aq6c',
                              'qty_sold': 'a5aq7a', 'value_sold': 'a5aq8',
                              'month': 'a5aq6f'}]},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
              'conditions': [{'qty': 'a5bq6a', 'unit': 'a5bq6c',
                              'qty_sold': 'a5bq7a', 'value_sold': 'a5bq8',
                              'month': 'a5bq6f'}]},
        'intercrop': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                      'flag': 'a4aq16', 'crop': 'cropID'},
    },
    '2018-19': {
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 's5aq06a_1', 'unit': None,
                              'qty_sold': 's5aq07a_1', 'value_sold': 's5aq08_1',
                              'month': 's5aq06f_1'}]},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 's5bq06a_1', 'unit': None,
                              'qty_sold': 's5bq07a_1', 'value_sold': 's5bq08_1',
                              'month': 's5bq06f_1'}]},
        'intercrop': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                      'flag': 's4aq16', 'crop': 'cropID'},
    },
    '2019-20': {
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [
                  {'qty': 's5aq06a_1', 'unit': 's5aq06b_1',
                   'qty_sold': 's5aq07a_1', 'value_sold': 's5aq08_1',
                   'month': 's5aq06f_1'},
                  {'qty': 's5aq06a_2', 'unit': 's5aq06b_2',
                   'qty_sold': 's5aq07a_2', 'value_sold': 's5aq08_2',
                   'month': 's5aq06f_2'},
              ]},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
              'conditions': [{'qty': 's5bq06a_1', 'unit': 's5bq06b_1',
                              'qty_sold': 's5bq07a_1', 'value_sold': 's5bq08_1',
                              'month': 's5bq06f_1'}]},
        'intercrop': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                      'flag': 's4aq16', 'crop': 'cropID'},
    },
}


# ---------------------------------------------------------------------------
# plot_inputs  (GAP 2 — item-level agricultural inputs)
# ---------------------------------------------------------------------------
#
# One row per REPORTED input applied to a plot, grain (t, i, plot, input).
# Source: AGSEC3A (season-1 plot-input module) + AGSEC3B (season-2) for the
# fertilizer / pesticide blocks, and AGSEC4A (plot-crop roster) for the seed
# block.  ``plot`` mirrors the crop_production / WB harmonised plot_id =
# hhid-parcel-plot, so a plot_inputs row joins crop_production on
# (t, i, plot) and plot_features on the parcel component.
#
# The UNPS plot-input module records four input *blocks* per plot, each a
# fixed column group:
#   organic fertilizer   used / qty / purchased? / purchased-qty / purch-value
#   inorganic fertilizer used / type / qty / purchased? / purch-qty / value
#   pesticide/herbicide  used / type / unit / qty / purchased? / purch-qty / value
# and the seed block (AGSEC4A, plot-crop grain):
#   seed                 qty / unit / seed-type(Trad/Improved) / improved-type
#                        / purchase-value [/ purchased? in 2009-10/2010-11]
#
# ``input`` carries the FINEST identity the source records, via the
# harmonize_input table (Code | Preferred Label).  Fertilizer/pesticide
# sub-types live in distinct Code ranges so one table disambiguates:
#   10           Seed
#   20           Organic Fertilizer
#   30 / 31..34  Inorganic Fertilizer  (Nitrate/Phosphate/Potash/Mixed)
#   40 / 41..49  Pesticide             (Insecticide/Fungicide/...)
# Reported attribute columns: Quantity + native unit ``u``, Purchased (bool),
# Quantity_purchased, Improved (bool, seed rows only), crop (j, seed rows
# where the source records the seed's crop, on harmonize_crop labels).
# NO seed_kg / nitrogen_kg / any-use flags — those are transformations.

_INPUT_TABLE = 'harmonize_input'
# pesticide unit scheme is a tiny 1=Kg / 2=Litres code (a3aq24a / a3aq28a),
# distinct from the UNPS harvest/seed unit scheme reused for seed via
# harvest_units.  Stored in its own harmonize_pesticide_unit table.
_PEST_UNIT_TABLE = 'harmonize_pesticide_unit'

# input-block -> harmonize_input base Code.  Inorganic/pesticide refine by
# adding the source type code (1..4 / 1..6,96 -> 96 folds to 9) so e.g.
# Nitrate inorganic = 31, Fungicide pesticide = 43.
_INPUT_BASE = {'seed': 10, 'organic': 20, 'inorganic': 30, 'pesticide': 40}


def _input_label_map():
    return _harmonized_codes(_INPUT_TABLE)


def _pest_unit_map():
    return _harmonized_codes(_PEST_UNIT_TABLE)


def _input_code(block, type_code):
    """Resolve the harmonize_input Code for a block + native type code.

    ``type_code`` may be NaN (block used but type unreported -> base code).
    Pesticide ``96`` ("Other") folds to base+9 so the table stays compact.
    """
    base = _INPUT_BASE[block]
    if block in ('inorganic', 'pesticide') and pd.notna(type_code):
        tc = int(type_code)
        if tc == 96:
            tc = 9
        if 1 <= tc <= 9:
            return base + tc
    return base


def _recode_yes(series):
    """Map a 1/2 (Yes/No) coded column to a nullable boolean (1->True,
    2->False; everything else -> NA).  UNPS uses 1=Yes, 2=No."""
    code = _to_int_code(series)
    out = pd.Series(pd.NA, index=series.index, dtype='boolean')
    out[code == 1] = True
    out[code == 2] = False
    return out


def plot_inputs_for_wave(t, df3a, df3b, df4a, colmap):
    """Build canonical ``plot_inputs`` for one Uganda UNPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2011-12"``).
    df3a, df3b : pd.DataFrame | None
        Raw AGSEC3A (season 1) / AGSEC3B (season 2) plot-input modules,
        loaded with ``convert_categoricals=False`` so code columns carry
        integer codes.  ``None`` permitted (season absent).
    df4a : pd.DataFrame | None
        Raw AGSEC4A plot-crop roster, for the seed block.  ``None``
        permitted; when absent no seed rows are emitted.
    colmap : dict
        Per-wave column map; see ``INPUT_COLMAPS``.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot, input)`` with columns
    ``Quantity`` (Float64), ``u`` (object, native unit label), ``Purchased``
    (boolean), ``Quantity_purchased`` (Float64), ``Improved`` (boolean,
    seed rows), and ``j`` (object crop label, seed rows where recorded).
    Reported values only; missing-in-wave columns are NaN.
    """
    input_map = _input_label_map()
    unit_map = _harvest_unit_map()        # seed unit reuses harvest scheme
    pest_unit_map = _pest_unit_map()
    crop_map = _crop_label_map()

    pieces = []

    # ---- fertilizer / pesticide blocks from AGSEC3A / AGSEC3B ----
    for season, df3 in (('A', df3a), ('B', df3b)):
        if df3 is None or len(df3) == 0:
            continue
        cm = (colmap.get(season) or {}).get('inputs')
        if not cm:
            continue
        hh = _format_agsec_hhid(df3[cm['hhid']], t)
        parcel = df3[cm['parcel']].apply(format_id)
        plot = (df3[cm['plot']].apply(format_id)
                if cm.get('plot') and cm['plot'] in df3.columns
                else pd.Series([''] * len(df3), index=df3.index))
        plot_id = hh.astype(str) + '-' + parcel.astype(str) + '-' + plot.astype(str)

        for block in ('organic', 'inorganic', 'pesticide'):
            b = cm.get(block)
            if not b:
                continue
            # A block "applies" to a plot when its used-flag is Yes, OR
            # (no used-flag column, e.g. inorganic in 2011+) when a type
            # or quantity is reported.
            type_code = (_to_int_code(df3[b['type']])
                         if b.get('type') and b['type'] in df3.columns
                         else pd.Series([pd.NA] * len(df3), index=df3.index, dtype='Int64'))
            qty = (pd.to_numeric(df3[b['qty']], errors='coerce')
                   if b.get('qty') and b['qty'] in df3.columns
                   else pd.Series([np.nan] * len(df3), index=df3.index))

            if b.get('used') and b['used'] in df3.columns:
                used = _recode_yes(df3[b['used']])
                applied = (used == True)
            else:
                applied = type_code.notna() | qty.notna()

            if not applied.any():
                continue

            # native unit: pesticide carries a 1=Kg/2=Litre unit column;
            # organic/inorganic are implicitly kg (no unit column).
            if b.get('unit') and b['unit'] in df3.columns:
                ucode = _to_int_code(df3[b['unit']])
                u = ucode.map(lambda c: pest_unit_map.get(int(c), pd.NA)
                              if pd.notna(c) else pd.NA)
            elif block in ('organic', 'inorganic'):
                u = pd.Series(['Kg'] * len(df3), index=df3.index, dtype='object')
            else:
                u = pd.Series([pd.NA] * len(df3), index=df3.index, dtype='object')

            purchased = (_recode_yes(df3[b['purchased']])
                         if b.get('purchased') and b['purchased'] in df3.columns
                         else pd.Series([pd.NA] * len(df3), index=df3.index, dtype='boolean'))
            qpur = (pd.to_numeric(df3[b['purchased_qty']], errors='coerce')
                    if b.get('purchased_qty') and b['purchased_qty'] in df3.columns
                    else pd.Series([np.nan] * len(df3), index=df3.index))

            input_code = type_code.map(lambda c: _input_code(block, c))
            input_label = input_code.map(lambda c: input_map.get(int(c), pd.NA)
                                         if pd.notna(c) else pd.NA)

            piece = pd.DataFrame({
                't': t,
                'i': hh.values,
                'plot': plot_id.values,
                'input': input_label.values,
                'Quantity': qty.values,
                'u': u.values,
                'Purchased': purchased.values,
                'Quantity_purchased': qpur.values,
                'Improved': pd.Series([pd.NA] * len(df3), dtype='boolean').values,
                'j': pd.Series([pd.NA] * len(df3), dtype='object').values,
            })
            piece = piece[applied.values]
            pieces.append(piece)

    # ---- seed block from AGSEC4A (plot-crop grain) ----
    sc = colmap.get('seed')
    if df4a is not None and len(df4a) and sc:
        hh = _format_agsec_hhid(df4a[sc['hhid']], t)
        parcel = df4a[sc['parcel']].apply(format_id)
        plot = (df4a[sc['plot']].apply(format_id)
                if sc.get('plot') and sc['plot'] in df4a.columns
                else pd.Series([''] * len(df4a), index=df4a.index))
        plot_id = hh.astype(str) + '-' + parcel.astype(str) + '-' + plot.astype(str)

        crop_code = (_to_int_code(df4a[sc['crop']])
                     if sc.get('crop') and sc['crop'] in df4a.columns
                     else pd.Series([pd.NA] * len(df4a), index=df4a.index, dtype='Int64'))
        j = crop_code.map(lambda c: crop_map.get(int(c), pd.NA)
                          if pd.notna(c) else pd.NA)

        qty = (pd.to_numeric(df4a[sc['qty']], errors='coerce')
               if sc.get('qty') and sc['qty'] in df4a.columns
               else pd.Series([np.nan] * len(df4a), index=df4a.index))
        if sc.get('unit') and sc['unit'] in df4a.columns:
            ucode = _to_int_code(df4a[sc['unit']])
            u = ucode.map(lambda c: unit_map.get(int(c), pd.NA)
                          if pd.notna(c) else pd.NA)
        else:
            u = pd.Series([pd.NA] * len(df4a), index=df4a.index, dtype='object')

        # Improved: seed-type 1=Traditional, 2=Improved.
        stype = (_to_int_code(df4a[sc['seed_type']])
                 if sc.get('seed_type') and sc['seed_type'] in df4a.columns
                 else pd.Series([pd.NA] * len(df4a), index=df4a.index, dtype='Int64'))
        improved = pd.Series(pd.NA, index=df4a.index, dtype='boolean')
        improved[stype == 2] = True
        improved[stype == 1] = False

        purchased = (_recode_yes(df4a[sc['purchased']])
                     if sc.get('purchased') and sc['purchased'] in df4a.columns
                     else pd.Series([pd.NA] * len(df4a), index=df4a.index, dtype='boolean'))
        qpur = (pd.to_numeric(df4a[sc['purchased_qty']], errors='coerce')
                if sc.get('purchased_qty') and sc['purchased_qty'] in df4a.columns
                else pd.Series([np.nan] * len(df4a), index=df4a.index))

        seed_label = input_map.get(_INPUT_BASE['seed'], 'Seed')
        piece = pd.DataFrame({
            't': t,
            'i': hh.values,
            'plot': plot_id.values,
            'input': seed_label,
            'Quantity': qty.values,
            'u': u.values,
            'Purchased': purchased.values,
            'Quantity_purchased': qpur.values,
            'Improved': improved.values,
            'j': j.values,
        })
        # Keep a seed row when any seed measure is reported (qty, improved,
        # purchased, or a crop label) — a plot-crop with a recorded seed.
        keep = (piece['Quantity'].notna() | piece['Improved'].notna()
                | piece['Purchased'].notna() | piece['j'].notna())
        pieces.append(piece[keep.values])

    cols = ['Quantity', 'u', 'Purchased', 'Quantity_purchased', 'Improved']
    if not pieces:
        return pd.DataFrame(columns=cols)

    df = pd.concat(pieces, ignore_index=True)
    df = df[df['input'].notna()]

    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').astype('Float64')
    df['Quantity_purchased'] = pd.to_numeric(df['Quantity_purchased'], errors='coerce').astype('Float64')
    df['Purchased'] = df['Purchased'].astype('boolean')
    df['Improved'] = df['Improved'].astype('boolean')
    # u may be NaN (a reported pesticide with no unit label); sentinel so it
    # is not an issue for downstream code that strings the column.
    df['u'] = df['u'].astype('object').where(df['u'].notna(), 'Unknown')
    # j is the seed's crop (on harmonize_crop labels) for seed rows, and a
    # 'n/a' sentinel for fertilizer/pesticide rows (no crop linkage).  It is
    # an INDEX level so per-crop seed rows on a multi-crop plot stay distinct
    # (the maintainer's "plot-crop seed grain") rather than collapsing —
    # 37.9% of seeded plots in 2011-12 carry >1 crop.  The sentinel keeps the
    # level non-null so it is a valid index level.
    df['j'] = df['j'].astype('object').where(df['j'].notna(), 'n/a')

    df = df.set_index(['t', 'i', 'plot', 'input', 'j'])
    # Collapse only EXACT-duplicate (t,i,plot,input,j) tuples — the same input
    # identity reported twice for the same plot-crop (e.g. a fertilizer block
    # appearing in both AGSEC3A passes, or a seed row repeated).  This is
    # de-duplication of the index grain, NOT cross-item aggregation: Quantity
    # / purchased quantity sum, flags/unit take first.
    if not df.index.is_unique:
        num = df[['Quantity', 'Quantity_purchased']].groupby(level=df.index.names).sum(min_count=1)
        flags = df[['Purchased', 'Improved']].groupby(level=df.index.names).max()
        us = df[['u']].groupby(level=df.index.names).first()
        df = num.join(flags).join(us)
        df = df[cols]
    return df


# Per-wave column maps for plot_inputs_for_wave.
#
# Two questionnaire vintages:
#   2009-10 / 2010-11 (older numbering):
#     organic   used a3aq4  qty a3aq5  purch a3aq6  pqty a3aq7
#     inorganic used a3aq14 type a3aq15 qty a3aq16 purch a3aq17 pqty a3aq18
#     pesticide used a3aq26 type a3aq27 unit a3aq28a qty a3aq28b purch a3aq29 pqty a3aq30
#     seed (AGSEC4A): NO qty/unit; purch a4aq10, seed_type a4aq13  (qty absent)
#   2011-12 / 2013-14 / 2015-16 / 2018-19 / 2019-20 (newer numbering, s-prefix
#   for 2018+):
#     organic   used .4  qty .5  purch .6  pqty .7
#     inorganic used .13 type .14 qty .15 purch .16 pqty .17
#     pesticide used .22 type .23 unit .24a qty .24b purch .25 pqty .26
#     seed (AGSEC4A): qty .11a unit .11b seed_type .13  (purch via value .15;
#                     no explicit purchased y/n -> Purchased NA)
INPUT_COLMAPS = {
    '2009-10': {
        'A': {'hhid': 'HHID', 'parcel': 'a3aq1', 'plot': 'a3aq3',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'a3aq1', 'plot': 'a3aq3',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq14', 'type': 'a3aq15', 'qty': 'a3aq16', 'purchased': 'a3aq17', 'purchased_qty': 'a3aq18'},
                  'pesticide': {'used': 'a3aq26', 'type': 'a3aq27', 'unit': 'a3aq28a', 'qty': 'a3aq28b', 'purchased': 'a3aq29', 'purchased_qty': 'a3aq30'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'a3bq1', 'plot': 'a3bq3',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'a3bq1', 'plot': 'a3bq3',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq14', 'type': 'a3bq15', 'qty': 'a3bq16', 'purchased': 'a3bq17', 'purchased_qty': 'a3bq18'},
                  'pesticide': {'used': 'a3bq26', 'type': 'a3bq27', 'unit': 'a3bq28a', 'qty': 'a3bq28b', 'purchased': 'a3bq29', 'purchased_qty': 'a3bq30'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'a4aq2', 'plot': 'a4aq4', 'crop': 'a4aq6',
                 'purchased': 'a4aq10', 'seed_type': 'a4aq13'},
    },
    '2010-11': {
        'A': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq14', 'type': 'a3aq15', 'qty': 'a3aq16', 'purchased': 'a3aq17', 'purchased_qty': 'a3aq18'},
                  'pesticide': {'used': 'a3aq26', 'type': 'a3aq27', 'unit': 'a3aq28a', 'qty': 'a3aq28b', 'purchased': 'a3aq29', 'purchased_qty': 'a3aq30'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq14', 'type': 'a3bq15', 'qty': 'a3bq16', 'purchased': 'a3bq17', 'purchased_qty': 'a3bq18'},
                  'pesticide': {'used': 'a3bq26', 'type': 'a3bq27', 'unit': 'a3bq28a', 'qty': 'a3bq28b', 'purchased': 'a3bq29', 'purchased_qty': 'a3bq30'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid', 'crop': 'cropID',
                 'purchased': 'a4aq10', 'seed_type': 'a4aq13'},
    },
    '2011-12': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq13', 'type': 'a3aq14', 'qty': 'a3aq15', 'purchased': 'a3aq16', 'purchased_qty': 'a3aq17'},
                  'pesticide': {'used': 'a3aq22', 'type': 'a3aq23', 'unit': 'a3aq24a', 'qty': 'a3aq24b', 'purchased': 'a3aq25', 'purchased_qty': 'a3aq26'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq13', 'type': 'a3bq14', 'qty': 'a3bq15', 'purchased': 'a3bq16', 'purchased_qty': 'a3bq17'},
                  'pesticide': {'used': 'a3bq22', 'type': 'a3bq23', 'unit': 'a3bq24a', 'qty': 'a3bq24b', 'purchased': 'a3bq25', 'purchased_qty': 'a3bq26'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
                 'qty': 'a4aq11a', 'unit': 'a4aq11b', 'seed_type': 'a4aq13'},
    },
    '2013-14': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq13', 'type': 'a3aq14', 'qty': 'a3aq15', 'purchased': 'a3aq16', 'purchased_qty': 'a3aq17'},
                  'pesticide': {'used': 'a3aq22', 'type': 'a3aq23', 'unit': 'a3aq24a', 'qty': 'a3aq24b', 'purchased': 'a3aq25', 'purchased_qty': 'a3aq26'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq13', 'type': 'a3bq14', 'qty': 'a3bq15', 'purchased': 'a3bq16', 'purchased_qty': 'a3bq17'},
                  'pesticide': {'used': 'a3bq22', 'type': 'a3bq23', 'unit': 'a3bq24a', 'qty': 'a3bq24b', 'purchased': 'a3bq25', 'purchased_qty': 'a3bq26'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
                 'qty': 'a4aq11a', 'unit': 'a4aq11b', 'seed_type': 'a4aq13'},
    },
    '2015-16': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3aq4', 'qty': 'a3aq5', 'purchased': 'a3aq6', 'purchased_qty': 'a3aq7'},
                  'inorganic': {'used': 'a3aq13', 'type': 'a3aq14', 'qty': 'a3aq15', 'purchased': 'a3aq16', 'purchased_qty': 'a3aq17'},
                  'pesticide': {'used': 'a3aq22', 'type': 'a3aq23', 'unit': 'a3aq24a', 'qty': 'a3aq24b', 'purchased': 'a3aq25', 'purchased_qty': 'a3aq26'},
              }},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'inputs': {
                  'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
                  'organic':   {'used': 'a3bq4', 'qty': 'a3bq5', 'purchased': 'a3bq6', 'purchased_qty': 'a3bq7'},
                  'inorganic': {'used': 'a3bq13', 'type': 'a3bq14', 'qty': 'a3bq15', 'purchased': 'a3bq16', 'purchased_qty': 'a3bq17'},
                  'pesticide': {'used': 'a3bq22', 'type': 'a3bq23', 'unit': 'a3bq24a', 'qty': 'a3bq24b', 'purchased': 'a3bq25', 'purchased_qty': 'a3bq26'},
              }},
        'seed': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID', 'crop': 'cropID',
                 'qty': 'a4aq11a', 'unit': 'a4aq11b', 'seed_type': 'a4aq13'},
    },
    '2018-19': {
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                  'organic':   {'used': 's3aq04', 'qty': 's3aq05', 'purchased': 's3aq06', 'purchased_qty': 's3aq07'},
                  'inorganic': {'used': 's3aq13', 'type': 's3aq14', 'qty': 's3aq15', 'purchased': 's3aq16', 'purchased_qty': 's3aq17'},
                  'pesticide': {'used': 's3aq22', 'type': 's3aq23', 'unit': 's3aq24a', 'qty': 's3aq24b', 'purchased': 's3aq25', 'purchased_qty': 's3aq26'},
              }},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                  'organic':   {'used': 's3bq04', 'qty': 's3bq05', 'purchased': 's3bq06', 'purchased_qty': 's3bq07'},
                  'inorganic': {'used': 's3bq13', 'type': 's3bq14', 'qty': 's3bq15', 'purchased': 's3bq16', 'purchased_qty': 's3bq17'},
                  'pesticide': {'used': 's3bq22', 'type': 's3bq23', 'unit': 's3bq24a', 'qty': 's3bq24b', 'purchased': 's3bq25', 'purchased_qty': 's3bq26'},
              }},
        'seed': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
                 'qty': 's4aq11a', 'unit': 's4aq11b', 'seed_type': 's4aq13'},
    },
    '2019-20': {
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                  'organic':   {'used': 's3aq04', 'qty': 's3aq05', 'purchased': 's3aq06', 'purchased_qty': 's3aq07'},
                  'inorganic': {'used': 's3aq13', 'type': 's3aq14', 'qty': 's3aq15', 'purchased': 's3aq16', 'purchased_qty': 's3aq17'},
                  'pesticide': {'used': 's3aq22', 'type': 's3aq23', 'unit': 's3aq24a', 'qty': 's3aq24b', 'purchased': 's3aq25', 'purchased_qty': 's3aq26'},
              }},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'inputs': {
                  'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
                  'organic':   {'used': 's3bq04', 'qty': 's3bq05', 'purchased': 's3bq06', 'purchased_qty': 's3bq07'},
                  'inorganic': {'used': 's3bq13', 'type': 's3bq14', 'qty': 's3bq15', 'purchased': 's3bq16', 'purchased_qty': 's3bq17'},
                  'pesticide': {'used': 's3bq22', 'type': 's3bq23', 'unit': 's3bq24a', 'qty': 's3bq24b', 'purchased': 's3bq25', 'purchased_qty': 's3bq26'},
              }},
        'seed': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid', 'crop': 'cropID',
                 'qty': 's4aq11a', 'unit': 's4aq11b', 'seed_type': 's4aq13'},
    },
}


# ---------------------------------------------------------------------------
# livestock  (GAP 4 — item-level livestock roster)
# ---------------------------------------------------------------------------
#
# One row per REPORTED species/herd a household owns, grain (t, i, animal).
# Source: the UNPS livestock roster (AGSEC6A large ruminants / AGSEC6B small
# ruminants / AGSEC6C poultry & other) that UGA_UNPS1.do:710-720 reads only to
# collapse to a single engaged-livestock y/n binary.  We keep the PRE-collapse
# roster and harmonise the animal type to the canonical SPECIES axis via the
# harmonize_species table (Code | Preferred Label).
#
# Reported item-level columns (missing-in-wave -> NaN):
#   HeadCount      head owned now              (q5 / q5a / q3a / s..q03a)
#   HeadAcquired   head bought to raise        (q12 / q13a / s..q13a)
#   HeadSold       head sold alive             (q14 / q14a / s..q14a)
#   Value          reported per-head value     (q6 'value if sold one today',
#                  2009-10/2010-11; q14b 's..q14b' 'avg value of each sold' for
#                  2011-12+ which dropped the sell-today question)
# Herd value (= Value x HeadCount), TLU, and the WB engaged-livestock binary
# (= groupby.any over these rows) are TRANSFORMATIONS, never stored here.
#
# Per-wave the animal type lives in different columns and two encodings:
#   * integer codes 1-42 (2010-11 6B/6C via q3, all of 2011-12+) -> harmonize_species
#   * label STRINGS (2009-10 all sections; 2010-11 6A q2) -> _species_string_map()
# The COLMAP 'type' entry names the column; 'type_kind' is 'code' or 'string'.

_SPECIES_TABLE = 'harmonize_species'


def _species_code_map():
    """Code (int 1-42) -> canonical species Preferred Label."""
    return _harmonized_codes(_SPECIES_TABLE)


# 2009-10 (all sections) and 2010-11 AGSEC6A carry the animal type as a label
# string with NO integer value labels.  Map those wave-specific strings to the
# same canonical species the integer codes resolve to.  Keys are lower-cased
# and whitespace-stripped; truncated UNPS strings ('backyard chicke', 'parent
# stock fo', 'geese and other', 'behives') are included verbatim.
_SPECIES_STRING_MAP = {
    # cattle / draft
    'calves': 'Cattle', 'bulls': 'Cattle', 'oxen': 'Cattle',
    'bulls and oxen': 'Cattle', 'heifer': 'Cattle', 'cows': 'Cattle',
    'heifer and cows': 'Cattle',
    'donkeys': 'Donkeys',
    'mules / horses': 'Horses', 'mules/horses': 'Horses', 'horses': 'Horses',
    # small ruminants
    'male goats': 'Goats', 'female goats': 'Goats',
    'male sheep': 'Sheep', 'female sheep': 'Sheep',
    'pigs': 'Pigs',
    # poultry & other (2009-10 strings, some truncated)
    'backyard chicke': 'Chicken', 'backyard chicken': 'Chicken',
    'parent stock fo': 'Chicken', 'parent stock for broilers': 'Chicken',
    'parent stock for layers': 'Chicken',
    'layers': 'Chicken', 'pullet chicken': 'Chicken', 'growers': 'Chicken',
    'broilers': 'Chicken',
    'turkeys': 'Other Poultry', 'ducks': 'Other Poultry',
    'geese and other': 'Other Poultry', 'geese and other birds': 'Other Poultry',
    'rabbits': 'Rabbits',
    'behives': 'Bees', 'beehives': 'Bees',
}


def _species_string_map():
    return dict(_SPECIES_STRING_MAP)


def livestock_for_wave(t, df6a, df6b, df6c, colmap):
    """Build canonical ``livestock`` for one Uganda UNPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2013-14"``).
    df6a, df6b, df6c : pd.DataFrame | None
        Raw AGSEC6A (large ruminants) / AGSEC6B (small ruminants) /
        AGSEC6C (poultry & other) livestock-roster modules.  ``None``
        permitted (section absent in a wave).  Code-typed sections must
        be loaded with ``convert_categoricals=False`` (integer codes);
        string-typed sections (2009-10, 2010-11 6A) with
        ``convert_categoricals=True`` (label strings) -- the wave script
        loads each section with the kind its colmap declares.
    colmap : dict
        Per-section column maps keyed by ``'A'`` / ``'B'`` / ``'C'``.  Each
        value is a dict with keys:
            hhid       — household id column
            type       — animal-type column (int code or label string)
            type_kind  — 'code' (map via harmonize_species) | 'string'
                         (map via _species_string_map)
            headcount  — head owned now column (or None)
            acquired   — head bought column (or None)
            sold       — head sold column (or None)
            value      — reported per-head value column (or None)

    Returns
    -------
    pd.DataFrame indexed ``(t, i, animal)`` with Float64 columns
        ``HeadCount``, ``HeadAcquired``, ``HeadSold``, ``Value``.
        One row per (household, species) -- duplicate species rows within
        a household (e.g. exotic + indigenous cattle, both mapping to
        'Cattle') are summed for the head columns and value-weighted-mean
        for ``Value`` so the (t, i, animal) index is unique.  NO TLU, NO
        herd-value total, NO engaged binary (all transformations).
    """
    code_map = _species_code_map()
    str_map = _species_string_map()

    pieces = []
    for section, df6 in (('A', df6a), ('B', df6b), ('C', df6c)):
        if df6 is None or len(df6) == 0:
            continue
        cm = colmap.get(section)
        if cm is None:
            continue

        hh = _format_agsec_hhid(df6[cm['hhid']], t)

        # --- resolve canonical species ---
        tcol = cm['type']
        if tcol not in df6.columns:
            continue
        if cm.get('type_kind') == 'string':
            keys = df6[tcol].astype('string').str.strip().str.lower()
            animal = keys.map(lambda s: str_map.get(s, pd.NA) if pd.notna(s) else pd.NA)
        else:
            # Per-section integer-code overrides (for waves whose code scheme
            # diverges from the shared harmonize_species table -- e.g. the
            # 2010-11 AGSEC6B roster, whose codes 17/19/20/21 mean different
            # species than the 2011-12+ scheme baked into the table).
            ov = cm.get('code_overrides') or {}
            wmap = {**code_map, **ov}
            code = _to_int_code(df6[tcol])
            animal = code.map(lambda c: wmap.get(int(c), pd.NA) if pd.notna(c) else pd.NA)

        def _num(key):
            col = cm.get(key)
            if col and col in df6.columns:
                return pd.to_numeric(df6[col], errors='coerce')
            return pd.Series([pd.NA] * len(df6), index=df6.index, dtype='Float64')

        piece = pd.DataFrame({
            't':            t,
            'i':            hh.values,
            'animal':       animal.values,
            'HeadCount':    _num('headcount').values,
            'HeadAcquired': _num('acquired').values,
            'HeadSold':     _num('sold').values,
            'Value':        _num('value').values,
        })
        pieces.append(piece)

    cols = ['HeadCount', 'HeadAcquired', 'HeadSold', 'Value']
    if not pieces:
        return pd.DataFrame(columns=cols)

    df = pd.concat(pieces, ignore_index=True)

    # Drop rows that carry no species label and rows with nothing reported
    # at all (empty roster placeholders).  A household line that lists a
    # species but reports zero/NaN everywhere is dropped (it did not own
    # that animal -- the roster pre-lists every species).
    df = df[df['animal'].notna()]
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    measure = ['HeadCount', 'HeadAcquired', 'HeadSold']
    # Keep a row when ANY head measure is a positive number (owned now,
    # bought, or sold).  Value alone (per-head price with no head count)
    # is not evidence of ownership.
    has_head = (df[measure].fillna(0) > 0).any(axis=1)
    df = df[has_head]

    # Collapse to (t, i, animal): the roster splits a species across
    # exotic/indigenous and age/sex lines; those are the SAME species for
    # this axis, so sum the head columns and take a head-weighted mean of
    # the reported per-head Value (NaN where no line reported a value).
    df['_w'] = df['HeadCount'].fillna(0)
    df['_vw'] = df['Value'] * df['_w']
    grouped = df.groupby(['t', 'i', 'animal'], dropna=False)
    out = grouped[measure].sum(min_count=1)
    vw_sum = grouped['_vw'].sum(min_count=1)
    w_sum = grouped['_w'].sum(min_count=1)
    value = vw_sum / w_sum.where(w_sum > 0, pd.NA)
    # Fall back to a simple mean of reported values where head weights are
    # all zero/NaN but a value was nonetheless reported.
    plain = grouped['Value'].mean()
    out['Value'] = value.where(value.notna(), plain)

    for c in cols:
        out[c] = pd.to_numeric(out[c], errors='coerce').astype('Float64')
    return out


# Per-wave column maps for livestock_for_wave.  ``type`` is the animal-type
# column; ``type_kind`` 'code' (harmonize_species) or 'string'
# (_species_string_map).  Value is the reported per-head animal value: the
# 'sell one today' question (q6) where the wave asks it (2009-10, 2010-11),
# else the 'average value of each sold' (q14b) for 2011-12+.
LIVESTOCK_COLMAPS = {
    '2009-10': {
        'A': {'hhid': 'HHID', 'type': 'a6aq2', 'type_kind': 'string',
              'headcount': 'a6aq5', 'acquired': 'a6aq12', 'sold': 'a6aq14', 'value': 'a6aq6'},
        'B': {'hhid': 'HHID', 'type': 'a6bq2', 'type_kind': 'string',
              'headcount': 'a6bq5', 'acquired': 'a6bq12', 'sold': 'a6bq14', 'value': 'a6bq6'},
        'C': {'hhid': 'HHID', 'type': 'a6cq2', 'type_kind': 'string',
              'headcount': 'a6cq5', 'acquired': 'a6cq12', 'sold': 'a6cq14', 'value': 'a6cq6'},
    },
    '2010-11': {
        # 6A carries the type as a string (a6aq2); 6B/6C carry the integer
        # 'Livestock code' (a6bq3 / a6cq3, codes 13-21 / 31-42).
        'A': {'hhid': 'HHID', 'type': 'a6aq3', 'type_kind': 'code',
              'headcount': 'a6aq5a', 'acquired': 'a6aq12', 'sold': 'a6aq14', 'value': 'a6aq6'},
        # 2010-11 AGSEC6B code scheme diverges from 2011-12+: codes 17/19/20/21
        # mean (17=Indigenous male goats, 19=Indigenous male sheep, 20=Indig.
        # female sheep, 21=Indig. male goats [dup]) -- overridden here so they
        # resolve to the right species despite the shared table's 2011-12+
        # meanings (17=Pigs, 19=Goats, 20/21=Sheep).  Codes 13-16/18 already
        # agree with the table.  No Pigs code in 2010-11 6B.
        'B': {'hhid': 'HHID', 'type': 'a6bq3', 'type_kind': 'code',
              'headcount': 'a6bq5a', 'acquired': 'a6bq12', 'sold': 'a6bq14', 'value': 'a6bq6',
              'code_overrides': {17: 'Goats', 19: 'Sheep', 20: 'Sheep', 21: 'Goats'}},
        'C': {'hhid': 'HHID', 'type': 'a6cq3', 'type_kind': 'code',
              'headcount': 'a6cq5a', 'acquired': 'a6cq12', 'sold': 'a6cq14', 'value': 'a6cq6'},
    },
    '2011-12': {
        'A': {'hhid': 'HHID', 'type': 'lvstid', 'type_kind': 'code',
              'headcount': 'a6aq3a', 'acquired': 'a6aq13a', 'sold': 'a6aq14a', 'value': 'a6aq14b'},
        'B': {'hhid': 'HHID', 'type': 'lvstid', 'type_kind': 'code',
              'headcount': 'a6bq3a', 'acquired': 'a6bq13a', 'sold': 'a6bq14a', 'value': 'a6bq14b'},
        'C': {'hhid': 'HHID', 'type': 'lvstid', 'type_kind': 'code',
              'headcount': 'a6cq3a', 'acquired': 'a6cq13a', 'sold': 'a6cq14a', 'value': 'a6cq14b'},
    },
    '2013-14': {
        'A': {'hhid': 'HHID', 'type': 'LiveStockID', 'type_kind': 'code',
              'headcount': 'a6aq3a', 'acquired': 'a6aq13a', 'sold': 'a6aq14a', 'value': 'a6aq14b'},
        'B': {'hhid': 'HHID', 'type': 'ALiveStock_Small_ID', 'type_kind': 'code',
              'headcount': 'a6bq3a', 'acquired': 'a6bq13a', 'sold': 'a6bq14a', 'value': 'a6bq14b'},
        'C': {'hhid': 'HHID', 'type': 'APCode', 'type_kind': 'code',
              'headcount': 'a6cq3a', 'acquired': 'a6cq13a', 'sold': 'a6cq14a', 'value': 'a6cq14b'},
    },
    '2015-16': {
        'A': {'hhid': 'HHID', 'type': 'LiveStockID', 'type_kind': 'code',
              'headcount': 'a6aq3a', 'acquired': 'a6aq13a', 'sold': 'a6aq14a', 'value': 'a6aq14b'},
        'B': {'hhid': 'HHID', 'type': 'ALiveStock_Small_ID', 'type_kind': 'code',
              'headcount': 'a6bq3a', 'acquired': 'a6bq13a', 'sold': 'a6bq14a', 'value': 'a6bq14b'},
        'C': {'hhid': 'HHID', 'type': 'APCode', 'type_kind': 'code',
              'headcount': 'a6cq3a', 'acquired': 'a6cq13a', 'sold': 'a6cq14a', 'value': 'a6cq14b'},
    },
    '2018-19': {
        'A': {'hhid': 'hhid', 'type': 'LiveStockID', 'type_kind': 'code',
              'headcount': 's6aq03a', 'acquired': 's6aq13a', 'sold': 's6aq14a', 'value': 's6aq14b'},
        'B': {'hhid': 'hhid', 'type': 'ALiveStock_Small_ID', 'type_kind': 'code',
              'headcount': 's6bq03a', 'acquired': 's6bq13a', 'sold': 's6bq14a', 'value': 's6bq14b'},
        'C': {'hhid': 'hhid', 'type': 'APCode', 'type_kind': 'code',
              'headcount': 's6cq03a', 'acquired': 's6cq13a', 'sold': 's6cq14a', 'value': 's6cq14b'},
    },
    '2019-20': {
        'A': {'hhid': 'hhid', 'type': 'LiveStockID', 'type_kind': 'code',
              'headcount': 's6aq03a', 'acquired': 's6aq13a', 'sold': 's6aq14a', 'value': 's6aq14b'},
        'B': {'hhid': 'hhid', 'type': 'ALiveStock_Small_ID', 'type_kind': 'code',
              'headcount': 's6bq03a', 'acquired': 's6bq13a', 'sold': 's6bq14a', 'value': 's6bq14b'},
        'C': {'hhid': 'hhid', 'type': 'APCode', 'type_kind': 'code',
              'headcount': 's6cq03a', 'acquired': 's6cq13a', 'sold': 's6cq14a', 'value': 's6cq14b'},
    },
}


def _anthro_num(series, length):
    """Coerce an anthropometry measure column to Float64, treating
    non-positive sentinels (0 / negatives) as missing.

    Uganda's GSEC6 anthropometry records 0.0 in the height columns where
    a child was measured on the OTHER height instrument (lying vs.
    standing), and the WB cleaning code (UGA_UNPS{1..8}.do) treats those
    as ``.`` before feeding ``zscore06``.  Weight/height/MUAC are strictly
    positive physical quantities, so a 0 or negative reading is an absent
    or invalid measurement, not a real value.
    """
    if series is None:
        return pd.Series([pd.NA] * length, dtype='Float64')
    v = pd.to_numeric(series, errors='coerce')
    v = v.where(v > 0, other=pd.NA)
    return v.astype('Float64')


def anthropometry_for_wave(t, df, colmap):
    """Build the canonical ``anthropometry`` table for one Uganda UNPS wave.

    Item-level, individual-grain ``(t, i, pid)``.  Carries ONLY the
    REPORTED body measures the GSEC6 anthropometry module records:

        Weight     — kg            (q27 / q27a / s6q27a)
        Height     — cm            (lying-down q28a/s6q28a2 preferred,
                                    falling back to standing q28b/s6q28b2,
                                    matching the WB ``gen height = ...;
                                    replace height = ...b if height==.``)
        MUAC       — mid-upper-arm circumference, cm  (NaN for Uganda —
                     no UNPS wave records arm circumference; the column is
                     declared for cross-country schema parity only)
        Age_months — child age in months as recorded by the survey
                     (q04 / q4; <NA> in 2018-19 / 2019-20, which dropped
                     the in-module age-in-months question — the WB code
                     sets ``age_months=.`` for those waves and derives the
                     child age from the roster instead)

    NO z-scores (haz06/waz06/whz06/bmiz06), NO wasting/stunting: those are
    WHO-2006 reference-population TRANSFORMS computed at query time, never
    stored.  Sex is NOT carried here — the GSEC6 module records only the
    caregiver's relationship, not the child's sex; the child's ``Sex``
    (and integer ``Age``) join from ``household_roster`` on the shared
    ``(t, i, pid)`` key.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2009-10"``).
    df : pd.DataFrame
        Raw GSEC6 anthropometry module for the wave.
    colmap : dict
        Column map with keys ``hhid``, ``pid``, ``weight``,
        ``height_lying``, ``height_standing``, ``age_months`` (any of the
        measure keys may be ``None`` when the wave lacks that column).

    Returns
    -------
    pd.DataFrame indexed ``(t, i, pid)`` with Float64 columns
        ``Weight``, ``Height``, ``MUAC``, ``Age_months``.  One row per
        measured individual: a roster line is kept only when AT LEAST ONE
        of Weight / Height is reported (the module pre-lists every child,
        and rows with no measurement at all are empty placeholders).
    """
    n = len(df)
    hh = df[colmap['hhid']].apply(format_id)
    pid = df[colmap['pid']].apply(format_id)

    weight = _anthro_num(df.get(colmap.get('weight')), n)

    lying = _anthro_num(df.get(colmap.get('height_lying')), n)
    standing = _anthro_num(df.get(colmap.get('height_standing')), n)
    # Lying-down length is recorded for the youngest children; standing
    # height for older ones.  Prefer whichever is present (lying first,
    # mirroring the WB ``gen height = ...a; replace = ...b if height==.``).
    height = lying.where(lying.notna(), standing)

    muac = _anthro_num(df.get(colmap.get('muac')), n)

    age_months = _anthro_num(df.get(colmap.get('age_months')), n)

    out = pd.DataFrame({
        't':          t,
        'i':          hh.values,
        'pid':        pid.values,
        'Weight':     weight.values,
        'Height':     height.values,
        'MUAC':       muac.values,
        'Age_months': age_months.values,
    })

    # Keep only rows that carry at least one body measure.
    has_measure = out['Weight'].notna() | out['Height'].notna() | out['MUAC'].notna()
    out = out[has_measure]

    # Drop rows missing an id (cannot key into the roster).
    out = out[out['i'].notna() & out['pid'].notna()]

    # A roster may list a child twice (e.g. re-measured); keep the row with
    # the most non-null measures so the (t, i, pid) index is unique.
    out['_nn'] = out[['Weight', 'Height', 'MUAC', 'Age_months']].notna().sum(axis=1)
    out = (out.sort_values('_nn', ascending=False)
              .drop_duplicates(['t', 'i', 'pid'])
              .drop(columns='_nn'))

    out = out.set_index(['t', 'i', 'pid'])
    for c in ['Weight', 'Height', 'MUAC', 'Age_months']:
        out[c] = pd.to_numeric(out[c], errors='coerce').astype('Float64')
    return out


# Per-wave column maps for anthropometry_for_wave.  Source files / variable
# names from the GSEC6 anthropometry module, confirmed against the actual
# .dta via get_dataframe (Stata is case-insensitive but pandas is not, so
# the casing below is the real on-disk casing, NOT the WB .do casing):
#   2009-10  GSEC6.dta      weight h6q27   height h6q28a/b   age h6q04
#   2010-11  GSEC6.dta      weight h6q27   height h6q28a/b   age h6q4
#   2011-12  GSEC6A.dta     weight h6q27   height h6q28a/b   age h6q4
#   2013-14  GSEC6_1.dta    weight h6q27a  height h6q28a/b   age h6q4
#   2015-16  gsec6_1.dta    weight h6q27a  height h6q28a/b   age h6q4
#   2018-19  GSEC6_5.dta    weight s6q27a  height s6q28a2/b2 (no in-module age)
#   2019-20  HH/gsec6_5.dta weight s6q27a  height s6q28a2/b2 (no in-module age)
# No UNPS wave records MUAC (arm circumference); 'muac' is therefore None
# everywhere and Uganda's MUAC column is NaN throughout.
ANTHRO_COLMAPS = {
    '2009-10': {'hhid': 'HHID', 'pid': 'PID', 'weight': 'h6q27',
                'height_lying': 'h6q28a', 'height_standing': 'h6q28b',
                'muac': None, 'age_months': 'h6q04'},
    '2010-11': {'hhid': 'HHID', 'pid': 'PID', 'weight': 'h6q27',
                'height_lying': 'h6q28a', 'height_standing': 'h6q28b',
                'muac': None, 'age_months': 'h6q4'},
    '2011-12': {'hhid': 'HHID', 'pid': 'PID', 'weight': 'h6q27',
                'height_lying': 'h6q28a', 'height_standing': 'h6q28b',
                'muac': None, 'age_months': 'h6q4'},
    '2013-14': {'hhid': 'HHID', 'pid': 'PID', 'weight': 'h6q27a',
                'height_lying': 'h6q28a', 'height_standing': 'h6q28b',
                'muac': None, 'age_months': 'h6q4'},
    '2015-16': {'hhid': 'hhid', 'pid': 'pid', 'weight': 'h6q27a',
                'height_lying': 'h6q28a', 'height_standing': 'h6q28b',
                'muac': None, 'age_months': 'h6q4'},
    '2018-19': {'hhid': 'hhid', 'pid': 'PID', 'weight': 's6q27a',
                'height_lying': 's6q28a2', 'height_standing': 's6q28b2',
                'muac': None, 'age_months': None},
    '2019-20': {'hhid': 'hhid', 'pid': 'pid', 'weight': 's6q27a',
                'height_lying': 's6q28a2', 'height_standing': 's6q28b2',
                'muac': None, 'age_months': None},
}

# Per-wave anthropometry source file (relative to the wave's _/ dir).
ANTHRO_FILES = {
    '2009-10': '../Data/GSEC6.dta',
    '2010-11': '../Data/GSEC6.dta',
    '2011-12': '../Data/GSEC6A.dta',
    '2013-14': '../Data/GSEC6_1.dta',
    '2015-16': '../Data/gsec6_1.dta',
    '2018-19': '../Data/GSEC6_5.dta',
    '2019-20': '../Data/HH/gsec6_5.dta',
}


# ---------------------------------------------------------------------------
# plot_labor  (GAP 3 — item-level plot-labor person-days)
# ---------------------------------------------------------------------------
#
# One row per REPORTED labor source on a plot-season, grain
# (t, i, plot, source, season).  Source: the AGSEC3A (season 1) / AGSEC3B
# (season 2) plot-input modules -- the SAME files plot_inputs reads -- whose
# labor block the WB code (UGA_UNPS1.do:467-509, and the corresponding block
# in UNPS2..8) reads only to build the per-parcel SUM columns
# total_labor_days / total_family_labor_days / total_hired_labor_days and the
# median-wage hired_labor_value.  We keep the PRE-collapse REPORTED rows:
#   PersonDays  -- reported person-days of that source on the plot-season
#   Wage        -- reported cash paid to HIRED labor (NaN for family/other)
# The WB sums and median-wage valuation are TRANSFORMATIONS, never stored here
# (total_labor_days = groupby(['t','i']).PersonDays.sum(); hired_labor_value =
# median-wage x hired PersonDays).
#
# ``source`` is a tiny controlled vocabulary emitted directly by this builder
# (no per-wave code lookup needed) -- the three canonical labels below.  It is
# NOT a categorical_mapping table because the builder assigns the label, it is
# not decoded from a raw survey code.
LABOR_SOURCES = ('family', 'hired', 'other')

# ``season`` is carried in the grain (mirroring crop_production) because the
# SAME (hhid-parcel-plot) plot id appears in BOTH AGSEC3A and AGSEC3B for ~64%
# of plots, and the labor question is asked per plot-SEASON.  Collapsing the
# two seasons into one (plot, source) row would require summing across seasons
# -- a transformation.  Keeping ``season`` in {A, B} preserves the reported
# per-plot-season rows.  ``plot`` itself is the bare hhid-parcel-plot (no
# season suffix) so a plot_labor row joins crop_production / plot_inputs /
# plot_features on (t, i, plot) [+ season for crop_production].

# Per-wave column maps for plot_labor_for_wave.  Three questionnaire vintages:
#
#   2009-10 / 2010-11 (UNPS1/2):
#     family  q39  (single cell, person-days)   gate used q38 (q38==0 -> 0)
#     hired   q42a/q42b/q42c (man/woman/child)  gate hired-flag q41 (q41==2 -> 0)
#     wage    q43
#   2011-12 (UNPS3):
#     family  q32  (single cell)                no separate gate
#     hired   q35a/q35b/q35c                     gate q34 (q34==2 -> 0)
#     wage    q36
#     other   q47/q48/q49 (exchange man/woman/child days)  [only this wave]
#   2013-14 / 2015-16 (UNPS4/5):
#     family  q33a_1 .. q33e_1 (up to 5 family-worker day cells)  no gate
#     hired   q35a/q35b/q35c                     gate q34 (q34==2 -> 0)
#     wage    q36
#   2018-19 (UNPS7):
#     family  ABSENT  (the WB code sets total_family_labor_days = .)
#     hired   s3aq35a/b/c                        gate s3aq34 (==2 -> 0)
#     wage    s3aq36
#     season B: the AGSEC3B labor block is not present (only s3bq35b), so no
#               season-B labor rows are emitted for this wave.
#   2019-20 (UNPS8):
#     family  in a SEPARATE AGSEC3A_1 family-roster file that is NOT in-repo,
#             so no family rows are emitted (documented partial).
#     hired   s3aq35a/b/c                        gate s3aq34 (==2 -> 0)
#     wage    s3aq36
#
# Prefix is the only season-B difference (a3aq->a3bq, s3aq->s3bq) except the
# 2011-12 "other" cells, which are upper-case A3AQ47.. / A3BQ47.. in the raw
# .dta.  Each season sub-map names: hhid/parcel/plot id columns, the family
# day cell(s) (``family`` -> list), the hired day cells (``hired`` -> list) +
# its used-flag (``hired_flag``), the ``wage`` cell, and the optional ``other``
# day cells (``other`` -> list).  A missing key means that source is not
# recorded for that wave/season.
LABOR_COLMAPS = {
    '2009-10': {
        'A': {'hhid': 'HHID', 'parcel': 'a3aq1', 'plot': 'a3aq3',
              'family': ['a3aq39'], 'family_flag': 'a3aq38',
              'hired': ['a3aq42a', 'a3aq42b', 'a3aq42c'], 'hired_flag': 'a3aq41',
              'wage': 'a3aq43'},
        'B': {'hhid': 'HHID', 'parcel': 'a3bq1', 'plot': 'a3bq3',
              'family': ['a3bq39'], 'family_flag': 'a3bq38',
              'hired': ['a3bq42a', 'a3bq42b', 'a3bq42c'], 'hired_flag': 'a3bq41',
              'wage': 'a3bq43'},
    },
    '2010-11': {
        'A': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
              'family': ['a3aq39'], 'family_flag': 'a3aq38',
              'hired': ['a3aq42a', 'a3aq42b', 'a3aq42c'], 'hired_flag': 'a3aq41',
              'wage': 'a3aq43'},
        'B': {'hhid': 'HHID', 'parcel': 'prcid', 'plot': 'pltid',
              'family': ['a3bq39'], 'family_flag': 'a3bq38',
              'hired': ['a3bq42a', 'a3bq42b', 'a3bq42c'], 'hired_flag': 'a3bq41',
              'wage': 'a3bq43'},
    },
    '2011-12': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'family': ['a3aq32'],
              'hired': ['a3aq35a', 'a3aq35b', 'a3aq35c'], 'hired_flag': 'a3aq34',
              'wage': 'a3aq36',
              'other': ['A3AQ47', 'A3AQ48', 'A3AQ49']},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'family': ['a3bq32'],
              'hired': ['a3bq35a', 'a3bq35b', 'a3bq35c'], 'hired_flag': 'a3bq34',
              'wage': 'a3bq36',
              'other': ['A3BQ47', 'A3BQ48', 'A3BQ49']},
    },
    '2013-14': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'family': ['a3aq33a_1', 'a3aq33b_1', 'a3aq33c_1', 'a3aq33d_1', 'a3aq33e_1'],
              'hired': ['a3aq35a', 'a3aq35b', 'a3aq35c'], 'hired_flag': 'a3aq34',
              'wage': 'a3aq36'},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'family': ['a3bq33a_1', 'a3bq33b_1', 'a3bq33c_1', 'a3bq33d_1', 'a3bq33e_1'],
              'hired': ['a3bq35a', 'a3bq35b', 'a3bq35c'], 'hired_flag': 'a3bq34',
              'wage': 'a3bq36'},
    },
    '2015-16': {
        'A': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'family': ['a3aq33a_1', 'a3aq33b_1', 'a3aq33c_1', 'a3aq33d_1', 'a3aq33e_1'],
              'hired': ['a3aq35a', 'a3aq35b', 'a3aq35c'], 'hired_flag': 'a3aq34',
              'wage': 'a3aq36'},
        'B': {'hhid': 'HHID', 'parcel': 'parcelID', 'plot': 'plotID',
              'family': ['a3bq33a_1', 'a3bq33b_1', 'a3bq33c_1', 'a3bq33d_1', 'a3bq33e_1'],
              'hired': ['a3bq35a', 'a3bq35b', 'a3bq35c'], 'hired_flag': 'a3bq34',
              'wage': 'a3bq36'},
    },
    '2018-19': {
        # No family-labor cells; season-B labor block absent (only s3bq35b).
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'hired': ['s3aq35a', 's3aq35b', 's3aq35c'], 'hired_flag': 's3aq34',
              'wage': 's3aq36'},
    },
    '2019-20': {
        # Family labor lives in a separate AGSEC3A_1 roster not in-repo.
        'A': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'hired': ['s3aq35a', 's3aq35b', 's3aq35c'], 'hired_flag': 's3aq34',
              'wage': 's3aq36'},
        'B': {'hhid': 'hhid', 'parcel': 'parcelID', 'plot': 'pltid',
              'hired': ['s3bq35a', 's3bq35b', 's3bq35c'], 'hired_flag': 's3bq34',
              'wage': 's3bq36'},
    },
}


def _labor_persondays(df, cols, flag_col=None, flag_zero_code=None):
    """Sum the REPORTED person-day cells in ``cols`` row-wise (NaN where ALL
    cells are missing, mirroring Stata ``egen rowtotal(), missing``).  When a
    ``flag_col`` is given, set the result to 0 where the flag equals
    ``flag_zero_code`` (the survey "did you use this source? No" branch), again
    mirroring the WB ``replace ... = 0 if flag==code`` logic.

    The sum is over the survey's own disaggregated cells of a SINGLE labor
    question (man/woman/child day cells, or per-family-worker day cells); it is
    the reported person-day quantity that question measures, NOT a cross-row /
    cross-plot aggregate (those stay in transformations)."""
    present = [c for c in cols if c in df.columns]
    if not present:
        return pd.Series([np.nan] * len(df), index=df.index)
    block = df[present].apply(lambda s: pd.to_numeric(s, errors='coerce'))
    # rowtotal(..., missing): NaN only when every cell is NaN, else sum.
    out = block.sum(axis=1, min_count=1)
    if flag_col is not None and flag_col in df.columns and flag_zero_code is not None:
        flag = _to_int_code(df[flag_col])
        out = out.where(flag != flag_zero_code, 0.0)
    return out


def plot_labor_for_wave(t, df3a, df3b, colmap):
    """Build canonical ``plot_labor`` for one Uganda UNPS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2011-12"``).
    df3a, df3b : pd.DataFrame | None
        Raw AGSEC3A (season 1) / AGSEC3B (season 2) plot-input modules,
        loaded with ``convert_categoricals=False`` so code columns carry
        integer codes.  ``None`` permitted (season absent).
    colmap : dict
        Per-wave column map; see ``LABOR_COLMAPS``.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot, source, season)`` with REPORTED
    columns ``PersonDays`` (Float64) and ``Wage`` (Float64, hired rows only;
    NaN for family/other).  One row per (plot, source, season) with a non-NaN
    reported PersonDays value.
    """
    pieces = []
    for season, df3 in (('A', df3a), ('B', df3b)):
        if df3 is None or len(df3) == 0:
            continue
        cm = (colmap or {}).get(season)
        if not cm:
            continue
        hh = _format_agsec_hhid(df3[cm['hhid']], t)
        parcel = df3[cm['parcel']].apply(format_id)
        plot = (df3[cm['plot']].apply(format_id)
                if cm.get('plot') and cm['plot'] in df3.columns
                else pd.Series([''] * len(df3), index=df3.index))
        plot_id = hh.astype(str) + '-' + parcel.astype(str) + '-' + plot.astype(str)

        # Reported person-days per source (one Series each, aligned to df3).
        source_days = {}
        if cm.get('family'):
            source_days['family'] = _labor_persondays(
                df3, cm['family'], cm.get('family_flag'),
                flag_zero_code=0 if cm.get('family_flag') else None)
        if cm.get('hired'):
            source_days['hired'] = _labor_persondays(
                df3, cm['hired'], cm.get('hired_flag'), flag_zero_code=2)
        if cm.get('other'):
            source_days['other'] = _labor_persondays(df3, cm['other'])

        # Reported hired wage (cash paid to hired labor).
        wage = (pd.to_numeric(df3[cm['wage']], errors='coerce')
                if cm.get('wage') and cm['wage'] in df3.columns
                else pd.Series([np.nan] * len(df3), index=df3.index))

        for source, days in source_days.items():
            piece = pd.DataFrame({
                't': t,
                'i': hh.values,
                'plot': plot_id.values,
                'source': source,
                'season': season,
                'PersonDays': days.values,
                'Wage': (wage.values if source == 'hired'
                         else np.full(len(df3), np.nan)),
            })
            # Keep only rows with a reported person-day value (drop the
            # question-not-asked NaNs); a plot with 0 reported days for a
            # source that WAS asked is a genuine reported zero and stays.
            piece = piece[piece['PersonDays'].notna()]
            if len(piece):
                pieces.append(piece)

    if not pieces:
        return pd.DataFrame(
            columns=['PersonDays', 'Wage'],
            index=pd.MultiIndex.from_arrays(
                [[]] * 5, names=['t', 'i', 'plot', 'source', 'season']))

    out = pd.concat(pieces, ignore_index=True)
    out['PersonDays'] = out['PersonDays'].astype('Float64')
    out['Wage'] = out['Wage'].astype('Float64')
    # Collapse exact-duplicate (plot, source, season) keys that can arise when
    # a wave repeats a plot row; keep the max reported days / wage so the index
    # is unique without summing distinct reported observations.
    out = (out.groupby(['t', 'i', 'plot', 'source', 'season'], dropna=False)
              .agg({'PersonDays': 'max', 'Wage': 'max'}))
    return out
