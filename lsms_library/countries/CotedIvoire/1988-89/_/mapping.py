"""Wave-scoped formatting hooks for CotedIvoire 1988-89 (CILSS).

GH #323.  The 1988-89 person files carry REPEATED person keys -- the raw source
itself has rows that collide on (CLUST, NH, PID), which is the declared index
(t, i, pid).  The framework collapsed them with groupby().first(), silently
deleting 4 rows (2 in household_roster, 2 in individual_education).

This is a SOURCE data-quality defect confined to ONE cluster -- CLUST 122,
households NH 19 and NH 21 -- and it is deliberately handled HERE, in the
1988-89 wave module, rather than by a country-wide dedup rule.  A country-wide
rule would paper over 4 rows in 2 households and hide the next occurrence; a
wave-scoped, loudly-warning hook keeps the defect visible.  No other CotedIvoire
wave has a duplicated person key (verified against every wave's L2-wave parquet).

The two roster collisions are NOT the same kind of thing, and are not treated
the same:

  (CLUST 122, NH 19, PID 2)  SEX=F REL=10 AGEY=46
                             SEX=F REL=10 AGEY=45
      Identical person attributes; a 1-year age discrepancy.  One person,
      DOUBLE-ENTERED.  Collapse -- but say so, and name the discarded age.

  (CLUST 122, NH 21, PID 10) SEX=F AGEY=10 REL=3 (Son/Daughter)
                             SEX=M AGEY=10 REL=3 (Son/Daughter)
      The SEX disagrees, so these two rows CANNOT be one person's consistent
      record.  Either the household has two 10-year-old children who were given
      the same PID by the enumerator (plausible: it is a polygamous household
      with two spouses, and it ALREADY contains two same-age different-sex
      children as separate pids -- 11=F,8 and 12=M,8), or it has one child whose
      sex was corrupted on one of two entries.

      NOTHING IN THE SOURCE ADJUDICATES THIS.  (HHEXP88.DAT carries an HHSIZE8
      of 13 for this household, equal to the roster ROW count, but HHSIZE8 is
      itself built by counting rows of this same defective file -- it counts the
      duplicate row either way -- so it is NOT independent evidence.  It is not
      a mere row count -- it differs from the row count in 369/1600 households --
      but that does not make it able to see through the duplicate.)

      So we keep BOTH REPORTED RECORDS, re-keying the collision with an
      obviously-synthetic pid ('10' and '10_2').  This fabricates no
      MEASUREMENT -- every Sex/Age/Relationship value emitted is exactly as
      reported; only the key label is synthetic.  Silently keeping one row
      instead would assert a Sex with a coin-flip chance of being wrong for that
      person AND delete a reported observation, which is the class-1
      (silently-wrong) failure this issue is about.  The residual ambiguity is
      real and is disclosed in CONTENTS.org; the warning below names the
      household every time it is built.

individual_education duplicates pid 1 and pid 2 in the SAME defective household
(122, 21).  Those pairs agree on the emitted column, so they collapse as
double-entries (groupby.first() takes the first NON-NULL per column, preferring
the more complete row).
"""
import warnings

import pandas as pd


def _resolve_pid_collisions(df, table, identity=()):
    """Make the declared (t, i, pid) index unique, LOUDLY.

    identity: columns on which a disagreement proves the rows are DIFFERENT
    PEOPLE rather than one person entered twice.  Rows that disagree on an
    identity column are re-keyed (both kept); rows that agree are collapsed.
    Every collision -- of either kind -- emits a RuntimeWarning naming the key,
    so these 4 rows can never again change silently.
    """
    if df.empty or df.index.is_unique:
        return df

    levels = [lvl for lvl in df.index.names if lvl is not None]
    flat = df.reset_index()
    value_cols = [c for c in flat.columns if c not in levels]
    dup_mask = flat.duplicated(subset=levels, keep=False)

    pieces = [flat[~dup_mask]]
    for key, grp in flat[dup_mask].groupby(levels, observed=True, sort=False):
        keyt = key if isinstance(key, tuple) else (key,)
        keyd = dict(zip(levels, keyt))

        split = [c for c in identity
                 if c in grp.columns and grp[c].dropna().nunique() > 1]
        if split:
            # Mutually inconsistent on an identity column -> distinct people
            # sharing one source pid.  Keep every reported record.
            grp = grp.copy()
            suffixes = [''] + [f'_{n}' for n in range(2, len(grp) + 1)]
            grp['pid'] = [f'{p}{s}' for p, s
                          in zip(grp['pid'].astype(str), suffixes)]
            conflict = {c: grp[c].dropna().astype(str).tolist() for c in split}
            warnings.warn(
                f"CotedIvoire/1988-89/{table}: person key {keyd} is shared by "
                f"{len(grp)} rows that DISAGREE on {conflict} -- they cannot be "
                f"one person.  Keeping every reported record and re-keying the "
                f"collision as pid {grp['pid'].tolist()} (synthetic key; no "
                f"measurement invented).  Source defect in CLUST 122; the "
                f"two-people vs corrupted-sex reading is NOT decidable from the "
                f"source -- see 1988-89/_/mapping.py. (GH #323)",
                RuntimeWarning,
            )
            pieces.append(grp)
            continue

        # Agrees on identity -> one person, double-entered.  Collapse, but name
        # every column the two entries disagreed on and the value being dropped.
        disagree = {c: sorted(grp[c].dropna().astype(str).unique())
                    for c in value_cols if grp[c].dropna().nunique() > 1}
        collapsed = grp.groupby(levels, observed=True, as_index=False).first()
        kept = {c: collapsed.iloc[0][c] for c in disagree}
        warnings.warn(
            f"CotedIvoire/1988-89/{table}: person key {keyd} is double-entered "
            f"({len(grp)} identical-identity rows); collapsing to one. "
            + (f"The entries disagree on {disagree}; keeping {kept} and "
               f"DISCARDING the other value(s). " if disagree else "")
            + "Source defect in CLUST 122. (GH #323)",
            RuntimeWarning,
        )
        pieces.append(collapsed)

    out = pd.concat(pieces, ignore_index=True).set_index(levels)
    assert out.index.is_unique, (
        f"CotedIvoire/1988-89/{table}: index {levels} still not unique after "
        f"resolving pid collisions"
    )
    return out


def household_roster(df):
    """Resolve the (CLUST 122) duplicated person keys.  See module docstring.

    `Sex` is the identity column: a person has one sex, so two rows under one
    pid that disagree on Sex are two people (or one corrupt entry) -- never a
    clean double-entry to be collapsed on a coin flip.
    """
    return _resolve_pid_collisions(df, 'household_roster', identity=('Sex',))


def individual_education(df):
    """Resolve the (CLUST 122, NH 21) duplicated pids 1 and 2.

    No identity column: the education file carries no person attributes that
    could prove two rows are different people, and the colliding pairs agree on
    the emitted column.  Collapse as double-entries, loudly.
    """
    return _resolve_pid_collisions(df, 'individual_education', identity=())
