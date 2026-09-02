"""Build cluster_features for Albania 2004.

This wave needed a SEMANTIC fix, not just a de-duplication (GH #323).

Source: w3_hh_basic.dta -- the household cover page of the 2004 panel follow-up
(1,797 households).  Two independent defects:

1. SILENT COLLAPSE.  cluster_features is CLUSTER grain (index ``(t, v)``), but
   the wave declared an extra ``i: chid`` idxvar, so 1,797 household rows reached
   ``_normalize_dataframe_index``, which dropped the household level and
   collapsed the duplicates with ``groupby().first()`` -- discarding 1,347 rows
   with no warning.

2. WRONG VALUES (this is the one with teeth).  ``m0_q01`` is the household's
   ORIGINAL 2002 PSU (verified: 1,713 of the 1,714 non-sentinel ``(m0_q01,
   m0_q02)`` keys exist in 2002's ``(psu, hh)``), but ``m0_distr`` is the
   household's CURRENT district.  A household that MOVED keeps its original PSU
   code and acquires a new district -- so in 2004 district is a HOUSEHOLD
   attribute, not a cluster attribute, and ``first()`` was handing a mover's
   district to the whole cluster.

   ``m0_orhh`` does NOT identify the movers (all conflicted households are
   flagged 'yes' = original), so 2004 carries NO internal signal that recovers a
   cluster's district.  The only deterministic source is the 2002 PSU -> district
   map, which is verified single-valued for all 450 PSUs.  Majority vote is a
   strictly weaker fallback and FAILS outright on PSUs 43/44/47/52/53 -- each a
   1-vs-1 KUKES/TIRANE tie that ``first()`` resolves only by source row order.

   Anchoring on 2002 also fixes a class of error that NO within-2004 check can
   see: 49 real PSUs contain a single household, so a lone mover makes its PSU
   look perfectly "unanimous".  PSU 223 is exactly that -- one household, reported
   ELBASAN, actually GRAMSH.

3. PHANTOM CLUSTERS.  ``m0_q01`` in {995, 999} are ADMINISTRATIVE sentinels, not
   clusters: 995 = 39 split-off/new households (all ``m0_orhh`` = 'no'), 999 = 44
   original households that moved or could not be traced (all 'yes').  None of
   the 83 keys into 2002's (psu, hh).  They pooled 83 households spread over 23
   districts into 2 fabricated cluster rows; they are excluded here (and
   ``sample.py`` now emits ``v = <NA>`` for those households instead of citing
   995/999 as real cluster ids).

Net effect: 450 cluster rows -> 448; Region corrected on 6 PSUs (16, 223, 259,
280, 297, 344).  Region keeps this wave's district-NAME semantics (it is not
switched to 2002's numeric district code), so downstream consumers see the same
kind of value they always did -- just the right one.
"""
import sys
import warnings
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import get_dataframe, format_id, to_parquet

sys.path.append(str(Path(__file__).parent.parent.parent / '_'))
from albania import cluster_reduce, ALBANIA_2004_SENTINEL_PSUS  # noqa: E402


def _norm_district(s):
    """Canonical district name.

    The 2004 district labels are mojibake: Korçë appears both as 'KORCE' and as
    'KOR\\x80E' (a mis-decoded Ç) -- two spellings of ONE district.  Left alone
    they would make the district code -> name map below ambiguous for code 14.
    """
    return str(s).replace('\x80', 'C').upper().strip()


# --- the household cover page --------------------------------------------
df = get_dataframe('../Data/w3_hh_basic.dta')
df = df[['m0_q01', 'm0_distr']].copy()
df['psu'] = df['m0_q01'].astype('Int64')
df['district'] = df['m0_distr'].map(_norm_district).replace(
    {'NAN': pd.NA, 'NONE': pd.NA, '<NA>': pd.NA})

sentinel = df['psu'].isin(ALBANIA_2004_SENTINEL_PSUS)
if sentinel.any():
    warnings.warn(
        f"Albania/2004 cluster_features: excluding {int(sentinel.sum())} household(s) "
        f"whose m0_q01 is an administrative sentinel {ALBANIA_2004_SENTINEL_PSUS} "
        f"(split-off / untraceable households, NOT sampling clusters) -- they would "
        f"otherwise fabricate 2 phantom cluster rows (GH #323).",
        RuntimeWarning,
    )
real = df[~sentinel].copy()

# --- the 2002 anchor: psu -> district CODE (verified single-valued) -------
d02 = get_dataframe('../../2002/Data/metadata_cl.dta')[['psu', 'm0_q1a']].copy()
d02['psu'] = d02['psu'].astype('Int64')
d02['code'] = d02['m0_q1a'].astype(str)

codes = d02.groupby('psu', observed=True)['code'].nunique(dropna=True)
assert (codes <= 1).all(), (
    "Albania/2002 district is NOT constant within psu -- the 2004 anchor is "
    "invalid; refusing to guess (GH #323)."
)
psu2code = d02.groupby('psu', observed=True)['code'].first().to_dict()

# --- district CODE -> district NAME --------------------------------------
# Learn the correspondence only from PSUs where the name is WELL IDENTIFIED:
# unanimous AND holding >= 2 households.  The >= 2 guard is essential -- a
# single-household PSU whose lone household moved is "unanimous" on the WRONG
# district (PSU 223: n=1, reports ELBASAN, truly GRAMSH), and would otherwise
# poison the map.
size = real.groupby('psu', observed=True)['district'].size()
nun = real.groupby('psu', observed=True)['district'].nunique(dropna=True)

code2name: dict[str, set] = {}
for psu in real['psu'].dropna().unique():
    if nun.get(psu, 0) == 1 and size.get(psu, 0) >= 2:
        name = real.loc[real['psu'] == psu, 'district'].dropna().iloc[0]
        code2name.setdefault(psu2code.get(psu), set()).add(name)

ambiguous = {c: n for c, n in code2name.items() if len(n) > 1}
if ambiguous:
    # Never guess: a code we cannot pin to one name yields <NA> downstream.
    warnings.warn(
        f"Albania/2004 cluster_features: {len(ambiguous)} district code(s) map to "
        f"more than one district name on well-identified PSUs; clusters carrying "
        f"them get <NA> Region rather than a guess (GH #323): {ambiguous}",
        RuntimeWarning,
    )
resolved = {c: next(iter(n)) for c, n in code2name.items() if len(n) == 1}

# --- Region = the district of the PSU, not of whoever happened to be first ---
real['Region'] = real['psu'].map(
    lambda p: resolved.get(psu2code.get(p), pd.NA)).astype('string')

unresolved = sorted(real.loc[real['Region'].isna(), 'psu'].dropna().unique().tolist())
if unresolved:
    warnings.warn(
        f"Albania/2004 cluster_features: {len(unresolved)} cluster(s) have no "
        f"resolvable district from the 2002 anchor; emitting <NA> rather than "
        f"guessing (GH #323).  Clusters: {unresolved[:20]}",
        RuntimeWarning,
    )

src = pd.DataFrame({
    'v': real['psu'].apply(format_id),
    'Region': real['Region'],
})

# Region is constant within v BY CONSTRUCTION now; the check still runs, so a
# future source change that breaks the invariant surfaces instead of hiding.
out = cluster_reduce(src, columns=['Region'], wave='2004')

to_parquet(out, 'cluster_features.parquet')
