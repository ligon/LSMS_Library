# GH #323 — Kazakhstan: declared aggregation for a non-unique canonical index

Branch: `fix/323-kazakhstan` (base: `development`)
Class: **INTENDED_AGGREGATION** (+ one genuinely-wrong cell)
Rows recovered: **0** — and that is the honest answer. See §3.

---

## 1. Instrument validation (done FIRST)

The L2-**country** parquet (`{data_root}/{C}/var/{t}.parquet`) is written
POST-collapse; the L2-**wave** parquet holds the truth. A scanner pointed at
`var/` returns a false zero. Before trusting any number, the scanner was
validated on the known positives:

| probe | required | got |
|---|---|---|
| `Mali/2014-15/_/household_roster.parquet` | 32,026 | **32,026** ✓ |
| `Guyana/1992/_/housing.parquet` | 311 | **311** ✓ |

A first attempt used `yaml.safe_load`, which **choked on the `!make` tag** and
silently reported *nothing* for every Mali table — a whole country of false
negatives. Fixed (multi-constructor) before any Kazakhstan number was believed.

All four Kazakhstan cells then reproduced exactly: `cluster_features` 7089,
`sample` 5228, `household_roster` 3, `individual_education` 3.

## 2. The source (KLSS 1996: 1,996 households / 7,224 persons / 135 clusters)

`KZ96SMP_PUF.dta`, `KZ96REL.dta`, `KZ96EDU_PUF.dta` are each 7,224 rows and all
**person-level**. Verified from source:

- `rn` is a **globally unique** household id:
  `nunique(rn) == nunique(oblast,rn) == nunique(loc_nr,rn) == 1996`.
  So collapsing on `rn` merges **no** two households and loses **none**.
- `sample` (household grain) ← person rows: **0 of 1,996** households carry a
  non-constant `loc_nr`/`oblast`/`type_loc`. The projection is
  **value-preserving**; `first()` was right *by luck, not by contract*.
- `cluster_features` (cluster grain) ← person rows: exactly **2 of 135**
  clusters are internally inconsistent in the source:
  - `loc_nr=126` — 50 households `urban`, 1 (`rn=1264`) `rural`
  - `loc_nr=5`  — 9 households `Akmolins`, 1 (`rn=43`) `Aktobink`
- `household_roster` / `individual_education`: household `rn=805` has
  `personnr [1,2,2,3,3,4,4]` — persons 2, 3, 4 each appear **twice**, and each
  pair is **identical in all 30 (REL) / 26 (EDU) non-serial columns** (same sex,
  same age, same `dob` to the day). Duplicate *records*, not people.

## 3. The one genuinely-wrong cell (class-1)

`first()` != majority on exactly **one** cell:

    cluster_features, v=126, Rural:  first() -> 'Rural'   (the MINORITY: 1 of 51)
                                     majority -> 'Urban'  (50 of 51)

That value shipped, silently, in the API. **This is the only thing that was
actually wrong.** No household and no person is lost anywhere, which is why
`rows_recovered = 0` — recovering rows here would be *manufacturing* them.

## 4. Two collapse sites, not one

This is the part that would have made a naive fix a no-op:

| site | tables | behaviour |
|---|---|---|
| `_normalize_dataframe_index` (country.py) | sample, household_roster, individual_education | collapses + warns (GH #323) |
| **`Wave.cluster_features()`** (country.py) | cluster_features | collapses **per wave** → the country index is already unique, so **the #323 warning never fires** |

`Wave.cluster_features()` justified its `.first()` with a comment claiming
Region/Rural are *"invariant within a cluster by construction of the LSMS-ISA
sampling design."* That is **prose, not enforcement**, and Kazakhstan cluster
126 falsifies it. Fixing only `_normalize_dataframe_index` would have left the
wrong cell in place.

## 5. Fix — declare the collapse, and make the reducer assert

Framework (`country.py`), a **single shared reducer** wired into **both** sites:

    aggregation: unique   # verify-then-collapse; RAISES if a group disagrees
    aggregation: dedupe   # drop exact dupes; RAISES if a collision is NOT identical
    aggregation: mode     # stated majority; WARNS naming the disagreeing groups
    aggregation: first    # legacy, explicitly opted into (lossy by design)
    aggregation: sum      # additive measures

Kazakhstan (`_/data_scheme.yml`):

| table | policy | why |
|---|---|---|
| `sample` | `unique` | 0/1996 non-constant → provably value-preserving; RAISES the day that stops being true |
| `cluster_features` | `mode` | fixes v=126 (`Rural`→`Urban`); warns naming clusters 5 and 126 |
| `household_roster` | `dedupe` | drops rn=805's 3 identical copies; RAISES on any future NON-identical `(i,pid)` collision |
| `individual_education` | `dedupe` | same rn=805 duplication |

**Rejected: hand-patching `rn=1264` (rural→urban) and `rn=43` (Aktobink→Akmolins)
at a mapping layer.** That overwrites *source* values on an inference and bakes
a guess in as fact — the Guyana failure mode. `mode` reaches the same answer for
these two clusters while remaining honest that it is a majority rule, and it is
never silent.

## 6. Non-vacuous guards

`unique` and `dedupe` both **pass** on today's data. A guard that cannot fire is
not a guard, so each raise-path is exercised with an **injected** violation in
`tests/test_declared_aggregation.py`:

- `test_unique_RAISES_when_the_projection_would_lose_information`
- `test_dedupe_RAISES_on_a_non_identical_collision`
- `test_mode_takes_the_majority_not_the_first_row` (minority placed **first**, so
  a `first()` regression fails the test)

## 7. Landmine found: `aggregation:` was already taken

Nine countries (Albania, Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau,
Malawi, Niger, Senegal, Togo) **already** declare `aggregation:` on
`interview_date` — as a **mapping** (`{visit: first}`), a forward-looking
per-level grain policy that *nothing currently reads*. A scalar-only validator
would have **raised on all nine**. `_declared_aggregation` therefore accepts both
shapes: mapping → `None` (legacy, untouched); scalar → the #323 policy.

## 8. Numbers

| table | before | after |
|---|---|---|
| `sample` | 1,996 rows + **silent** 5,228-row collapse warning | 1,996 rows, collapse **verified value-preserving** (`unique`), no warning |
| `cluster_features` | 135 rows, **v=126 `Rural` = 'Rural'** (minority) | 135 rows, **v=126 `Rural` = 'Urban'** (majority) + warning naming clusters 5, 126 |
| `household_roster` | 7,220 rows + silent 3-row collapse | 7,220 rows, dupes **asserted identical** |
| `individual_education` | 3,190 rows + silent 3-row collapse | 3,190 rows, dupes **asserted identical** |

Row counts are unchanged by design (`rows_recovered = 0`). What changed is that
every collapse is now **declared and enforced**, and one wrong value is right.

Two API counts are *not* caused by #323 and are deliberately left alone:
- roster 7,220 (not 7,221): `rn=26/personnr=5` is a **blank placeholder row**
  (sex, age, relhead=0, monhh all NaN) — correctly dropped, pre-existing.
- education 3,190: `bw003_` is NaN for 4,033 persons (no post-secondary
  attainment recorded) — pre-existing NaN drop, not an index collapse.

## 9. Regression: proof nothing else moved

**Logical (covers all 40 countries).** The new reducer runs only when
`_declared_aggregation()` returns a *scalar*. Across **all 419 tables in all 40
countries**, exactly **4** resolve to a scalar policy — Kazakhstan's. Everywhere
else the branch is unreachable and the original statements execute verbatim.
`_SCHEME_SKIP_KEYS` has a single consumer (`_enforce_declared_dtypes`), guarded
by `col not in df.columns`; no table has a column named `aggregation`, so adding
the key is a strict no-op.

**Empirical (cold, vs the TRUE base `d572d8a9`).** 26 tables chosen to cover
every path the change can reach — 9 `cluster_features` (the `Wave` i-collapse),
7 mapping-form `interview_date` (the legacy `aggregation:` shape), 6
duplicate-index tables in *other* countries (live #323 cells), and Kazakhstan's 4:

    tables compared : 26
    IDENTICAL       : 25
    CHANGED         : 1
      Kazakhstan/cluster_features   135:e8c3361d8301d7b2 -> 135:f07a6a5dfb1eb4b6

The single change is the intended one (same 135 rows; the cluster-126 `Rural`
flip). Kazakhstan's `sample` / `household_roster` / `individual_education` hash
**identically** — their collapse was already value-preserving, so only the
*enforcement* changed, not the values. Other countries' #323 cells (Guyana,
Kosovo, Malawi, Niger) are untouched: they belong to their own fix agents.

Three instrument failures were caught and fixed before any result was trusted:
`yaml.safe_load` choking on the `!make` tag (silently zeroed all of Mali); a
`pgrep` that matched *other agents'* pytest; and a probe DataFrame built from
Series with a mismatched index, which pandas reindex-aligned to almost all-NaN
(this one briefly made the `unique` guard *look* vacuous — it is not).

**Baseline drift.** The main checkout has advanced 7 commits past this branch's
base (incl. `c8c25f68`, a spellings fix that changes data in 5 countries).
Comparing against it manufactured spurious diffs, so BEFORE was rebuilt from a
detached worktree at `d572d8a9`.

## 10. Residual / not done

**The default is still `first()` + warning.** The #323 umbrella calls for the
default on an *undeclared* non-unique index to become a hard ERROR. Flipping it
here would break the ~13 other countries still mid-fix and violates "break
nothing else", so this change is **additive**: it lands the vocabulary + the
enforcement machinery + Kazakhstan's declarations. The default flip is a
one-line change to `_normalize_dataframe_index` once every country declares.
