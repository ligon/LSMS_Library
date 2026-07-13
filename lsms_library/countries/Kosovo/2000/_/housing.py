"""Kosovo 2000 housing.  GH #323.

WHY THIS IS A SCRIPT AND NOT YAML
---------------------------------
``DWELLING.dta`` section 3a is a **roster over dwelling STRUCTURES**, not a
household-level table.  The questionnaire (``Documentation/hheng.pdf`` p.82)
tells the enumerator to *"LIST ALL STRUCTURES BEFORE ASKING Q.2-6"* and then
asks, for each of the 8 structure types (Tent, Prefab/container, Damaged
house, Damaged apartment, Intact house, Intact apartment, Barracks, Others):

    s3a_q01  "Does your household use [STRUCTURE]?"   YES..1  NO...2

so a household contributes up to 8 rows -- 7,047 rows over 2,865 households.
Only the YES rows carry dwelling detail: all 4,168 NO rows have q02..q06
entirely NaN (screener negatives).  The true key of the source is
``(hhid, s3a_q0a)``, which is unique.  (Use ``s3a_q0a``, the full-text
dwelling type -- ``s3a_q00`` is the 8-char truncated Stata label and collides
"Damaged house"/"Damaged apartment", giving 596 spurious duplicates.)

Declared ``(t, i)`` and left on the YAML path, this roster reached
``_normalize_dataframe_index`` with 4,182 duplicate index tuples and was
collapsed with ``groupby().first()``.  Because ``GroupBy.first()`` is
**skipna per column** it does not pick a row -- it FRANKENSTEINS one: ``Type``
came from roster row 0 (usually a structure the household had answered "No"
to) while ``Rooms``/``Tenure`` came from the first non-null value, i.e. the
actually-occupied structure.  524 of the 2,850 single-occupancy households
(18.4%) were served a dwelling type they had explicitly said No to, and the
per-column skipna is exactly why nothing ever looked obviously broken.

A second, independent bug in the same cell: ``Electricity`` was mapped to
``s3a_q01`` -- the occupancy screener -- so all 2,865 published values were
meaningless (525 read "No" purely because roster row 0 was an unoccupied
structure; the truth is 24).  Electricity lives in the adjacent AMENITIES.dta
module, which is clean household-level data.

WHAT THIS SCRIPT DOES  (the `aggregation:` block in ../../_/data_scheme.yml
documents the policy; THIS FILE ENFORCES IT)
-------------------------------------------------------------------------
1. Filter the roster to the OCCUPIED structures (``s3a_q01 == 'Yes'``):
   7,047 -> 2,878 rows over 2,863 households.
2. Reduce the 13 genuinely multi-structure households:
     Rooms = SUM of s3a_q04 over the occupied structures.  q04 is "How many
       separations (rooms) does the [STRUCTURE] have" -- a PER-STRUCTURE
       count -- so `first` is provably the wrong reducer.
     Type / Walls / Tenure = the PRIMARY structure, defined as the unique
       argmax of s3a_q02 ("How large is the part of the [STRUCTURE] your
       household uses", sq.m).
   For 3 households (202, 11501, 12401) that argmax is TIED, so the primary
   structure is genuinely UNDETERMINED.  We do NOT break the tie with
   ``idxmax()`` -- that resolves by row order, which is a positional guess
   dressed up as a rule.  The structure-specific columns are left <NA> for
   those households (class-2: silently missing beats silently wrong); Rooms
   (a tie-independent sum) and the AMENITIES columns are unaffected and are
   retained.
3. Source Electricity/Water/Toilet from AMENITIES.dta (s3b_q07 "Does your
   household have access to electricity?", s3b_q01 "main source of water to
   wash", s3b_q05 "type of toilet"; hheng.pdf p.83).  Unique on hhid.
4. Drop -- LOUDLY -- the households with no occupied structure at all.
5. Assert the (t, i) index is unique before writing, so the roster collapse
   can never silently regress.
"""
import sys
import warnings

import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, format_id

t = '2000'

# ---------------------------------------------------------------- structures
d = get_dataframe('../Data/DWELLING.dta')
d['i'] = d['hhid'].map(format_id)

# Stata gives back categoricals; go to plain strings so groupby/compare behave
# (unordered categoricals blow up in groupby -- see CLAUDE.md).
def _s(col):
    return col.astype(str).replace({'nan': pd.NA, 'None': pd.NA, '<NA>': pd.NA, '': pd.NA})

d['Type'] = _s(d['s3a_q0a'])
d['Walls'] = _s(d['s3a_q03'])
d['Tenure'] = _s(d['s3a_q06'])
d['Occupied'] = _s(d['s3a_q01'])
d['sqm'] = pd.to_numeric(d['s3a_q02'], errors='coerce')
d['rooms'] = pd.to_numeric(d['s3a_q04'], errors='coerce')

# '0.0' is not a valid wall-material code (the questionnaire codes are
# 1 MUD / 2 STONES / 3 WOOD / 4 BRICKS / 5 CEMENT BLOCKS / 6 OTHER).
d.loc[d['Walls'].eq('0.0'), 'Walls'] = pd.NA

# 1. ------------------------------------------------- occupancy-screener filter
occ = d[d['Occupied'].eq('Yes')].copy()

dropped = sorted(set(d['i'].dropna()) - set(occ['i'].dropna()))
if dropped:
    warnings.warn(
        f"Kosovo/2000 housing: {len(dropped)} household(s) reported NO occupied "
        f"dwelling structure (every s3a_q01 == 'No' or blank) and are DROPPED "
        f"rather than guessed at: {dropped}.  GH #323.",
        RuntimeWarning,
    )

# 2. ------------------------------------------- reduce multi-structure households
n_struct = occ.groupby('i')['i'].transform('size')
max_sqm = occ.groupby('i')['sqm'].transform('max')
is_max = occ['sqm'].eq(max_sqm) & max_sqm.notna()
n_at_max = is_max.groupby(occ['i']).transform('sum')

# The primary structure is well-determined when the household occupies exactly
# one structure, or when a single structure strictly maximises the area the
# household uses.  A tie leaves it UNDETERMINED -- no row is primary.
primary = (n_struct.eq(1)) | (is_max & n_at_max.eq(1))

tied = sorted(occ.loc[n_struct.gt(1) & ~occ['i'].isin(occ.loc[primary, 'i']), 'i'].unique())
if tied:
    warnings.warn(
        f"Kosovo/2000 housing: {len(tied)} multi-structure household(s) {tied} have "
        f"a TIED argmax on s3a_q02 (area used), so the primary structure is "
        f"undetermined; Type/Walls/Tenure are left <NA> for them rather than "
        f"resolved by row order.  Rooms (a sum) and the AMENITIES columns are "
        f"unaffected.  GH #323.",
        RuntimeWarning,
    )

# Rooms: SUM over occupied structures (min_count=1 keeps all-NaN as NaN).
rooms = occ.groupby('i')['rooms'].sum(min_count=1)

# Structure-specific attributes: from the primary structure only.
prim = occ[primary].set_index('i')[['Type', 'Walls', 'Tenure']]
assert prim.index.is_unique, "more than one primary structure per household"

df = pd.DataFrame(index=rooms.index)
df = df.join(prim)                      # tied households -> <NA>, by construction
df['Rooms'] = rooms

# 3. ------------------------------------------------------- household amenities
a = get_dataframe('../Data/AMENITIES.dta')
a['i'] = a['hhid'].map(format_id)
amen = pd.DataFrame({'i': a['i']})
amen['Electricity'] = _s(a['s3b_q07'])
amen['Water'] = _s(a['s3b_q01']).replace({
    'Centrali': 'Centralized pipeline',
    'Standing': 'Standing water pipe',
    'Well': 'Well',
    'Spring': 'Spring',
    'Brought ': 'Brought in water (truck)',
    'Public t': 'Public tap',
    'Other': 'Other',
})
amen['Toilet'] = _s(a['s3b_q05']).replace({
    'Flush to': 'Flush toilet',
    'Latrine': 'Latrine',
    'No toile': 'No toilet',
})
assert not amen['i'].duplicated().any(), "AMENITIES.dta is not unique on hhid"
df = df.join(amen.set_index('i'), how='left')

# The Stata value labels are 8-char truncated; spell them out from the
# questionnaire's code lists (hheng.pdf p.82).  The Tenure expansion is carried
# over VERBATIM from the mapping that used to live in the wave data_info.yml --
# moving the table to a script must not silently un-expand these labels.
df['Walls'] = df['Walls'].replace({'Cement b': 'Cement blocks'})
df['Tenure'] = df['Tenure'].replace({
    'Built pe': 'Built personally',
    'Purchase': 'Purchased',
    'Inherite': 'Inherited',
    'Borrowed': 'Borrowed',
    'Occupied': 'Occupied',
    'From emp': 'From employer',
    'Assigned': 'Assigned',
    'Donated': 'Donated',
    'Swapped': 'Swapped',
    'Others': 'Others',
})

# 4. --------------------------------------------------------------------- emit
df['t'] = t
df = (df.reset_index().rename(columns={'index': 'i'})
        .set_index(['t', 'i'])
        .sort_index())
df = df[['Type', 'Walls', 'Rooms', 'Tenure', 'Electricity', 'Water', 'Toilet']]

# 5. The guard that makes the GH #323 collapse impossible to reintroduce
#    silently: if the roster ever again reaches the canonical index unreduced,
#    this raises instead of being groupby().first()-ed away.
assert df.index.is_unique, (
    f"housing index (t, i) is not unique: "
    f"{int(df.index.duplicated().sum())} duplicate tuple(s)"
)

to_parquet(df, 'housing.parquet')
