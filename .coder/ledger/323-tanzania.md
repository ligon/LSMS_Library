# Prior-Art Ledger — GH #323 (Tanzania)

**Search tier used:** ripgrep + git floor (gitnexus not consulted; the blast radius
was established empirically instead — see §6).

## §1 Task, restated
`_normalize_dataframe_index` collapses a non-unique DECLARED index with
`groupby().first()`. In Tanzania this fires on two tables that look like one bug
but are not:

* **`cluster_features`** (declared index `(t, v)`) is fed the **household** grain.
  `Region`/`District`/`Rural` come from the household cover page — *where the
  household was interviewed* — while `v` is the household's **original sampling
  EA**, which the NPS panel carries forward unchanged when it tracks a mover or a
  split-off. So the geo columns are household attributes, not cluster attributes,
  and they are **not** constant within a cluster. `.first()` therefore does not
  merely dedup — it **MISLABELS**. *class-1: silently WRONG.*
* **`interview_date`** (and, undetected by the audit, **`sample`**) hit the same
  source replication: `upd4_hh_a.dta` is keyed on the panel-tracking line `UPHI`
  (29,250/29,250 unique), not on the household (`r_hhid`, `round`) → 16,540. The
  extra rows are exact replicas. Here `.first()` is **value-preserving**.
  *class-0: right answer, undeclared mechanism.*

Applying one fix to both would re-close #323 while leaving the class alive.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4100` | drops undeclared index levels, then `groupby().first()` + #323 warning | indirectly | **extend** (honor a declared policy) |
| `Wave.cluster_features` | `country.py:1168` | **the real collapse site** — collapses HH→cluster with `.first()` *before* the framework sees it (GH #161) | no | **extend** |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py:101` | the ONE existing "declared aggregation" precedent (`food_acquired` → sum) | yes | precedent, not reused |
| `_declared_index_levels` | `country.py:3723` | parses `index: (t, v)` | yes | reuse |
| `Country._materialization_entry` | `country.py` | raw `data_scheme.yml` entry for a table | yes | reuse |
| `aggregation:` key | 9 countries' `data_scheme.yml` | **already exists**, documentation-only ("nothing reads this yet"), keyed by INDEX LEVEL (`{visit: first}`) | no | **must not hijack** — see §4 |

## §3 Definitions & conventions in force
- `v` = sampling cluster; owned by `cluster_features`, joined onto household
  tables at API time by `_join_v_from_sample` (`CLAUDE.md`, "sample() and Cluster
  Identity"). **Consequence:** a wrong `sample().v` propagates to *every*
  Tanzania household table.
- class-1 (silently WRONG) vs class-2 (silently MISSING): per the task standard,
  **class-2 is strictly safer**. When the answer is undetermined, drop loudly.
- Tanzania multi-round `2008-15/` folder: `.claude/skills/multi-round-waves.md`.

## §4 Invariants & assumptions
- **Round 1 is the baseline — nobody has moved yet.** 409 clusters, **0**
  within-cluster geo conflicts. Conflicts appear only from round 2 (229 / 337 /
  63). This is the proof that the missing level is the HOUSEHOLD and that it is
  load-bearing.
- **One household = one vote.** Geo is constant within `(i, t, v)` — verified 0
  of 16,599. So deduping the UPHI replication cannot skew the ballot.
- **`District` is `''`, not NaN, in rounds 1–2** (7,189 rows). A plain `mode()`
  would elect the empty string and ship it as a district *name*. The reducer must
  treat blanks as non-votes. *(Not in the original diagnosis; found by inspection.)*
- **`aggregation:` is already taken.** 9 countries carry a documentation-only
  `aggregation: {visit: first}` on `interview_date`, keyed by index LEVEL. A
  column-keyed reader would silently hijack them → `_declared_aggregation` drops
  keys that name a declared index level and returns `None` when nothing remains.
- The framework's `dropna(how='all')` (`country.py:2217`) deletes a cluster row
  whose every column resolved to `<NA>` — this is why 6 unresolvable clusters
  disappear rather than appearing with null geography.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| household→cluster reduction | **new** (`_majority`) | `first` mislabels 210 cells; `mode` elects `''`; no existing reducer refuses to guess |
| declaration mechanism | **new** (`aggregation:` in `data_scheme.yml`, read by `_declared_aggregation`) | the only precedent (`_ADDITIVE_MEASURE_COLUMNS`) is a hardcoded dict keyed by table name — not per-country, so it cannot express "Tanzania's cluster_features" |
| where the reduction runs | **extend `Wave.cluster_features`** | it is the actual collapse site; the framework's `_normalize_dataframe_index` never sees the household grain |
| `interview_date` / `sample` dedup | **extend the wave scripts** (assert + `drop_duplicates`) | the collapse is already correct; it just needs to be declared and verified |

## §6 Open questions for the human
- **The class is NOT fully closed, and I will not claim it is.** A repo-wide
  *raising* guard would fail every one of the ~13 still-unfixed countries, which
  violates "prove you broke nothing else". I prototyped the **static** config
  guard (wave emits an index level absent from the declared index) and measured
  it: it fires on ~25 countries, but **most are false positives** (`emits ['v']`
  where `v` is functionally determined by the household, so dropping it creates
  no duplicates and loses nothing). The correct instrument is the **data-driven**
  duplicate check the framework already performs — it should become a CI-blocking
  test with an explicit, shrinking `KNOWN_BROKEN` list once the other countries
  land. That belongs in its own coordinated PR.
- **Separate, pre-existing bug found in passing (NOT #323, not fixed here).**
  `Country('Tanzania').interview_date()` **silently loses two entire waves on the
  warm-cache path**: 22,427 rows cold → **16,534** warm; 2019-20 and 2020-21
  vanish. Cause: the L2-country parquet stores `Int_t` as a *string*, and the two
  eras use different formats — `'2008-12-13 00:00:00'` (space) vs
  `'2019-09-28T10:47:45'` (ISO-8601 `T`). `_enforce_canonical_dtypes` coerces to
  datetime, the `T`-form fails → `NaT` for all 5,893 rows, and then
  `dropna(how='all')` (`country.py:2217`) deletes them, because `v` sits in the
  index so `Int_t` is the only data column. Reproduces on pristine `development`.
  Deserves its own issue.

---
### Phase 3 — verification
- `_majority` — **OK (anchored §4/§5)**: strict >50% of non-null votes; blank
  strings are not votes; no majority → `pd.NA` + `RuntimeWarning`. Refuses to
  guess, per §3's class-2 rule.
- `_declared_aggregation` — **OK (anchored §4)**: ignores legacy level-keyed
  entries; the 9 legacy countries provably read as `None` (measured).
- `_reduce_declared` — **OK (anchored §4)**: `dropna=True` matches the framework's
  other groupbys, so Tanzania's 545 null-`clusterid` households do not condense
  into a phantom `<NA>` cluster.
- `Wave.cluster_features` — **OK (anchored §2)**: policy branch only; countries
  declaring nothing keep the exact historical `first()`/`mean()` path. Proven by
  40 byte-identical country/table fingerprints across 20 countries.
- Tanzania wave scripts — **OK (anchored §4)**: each dedup now asserts the
  invariant that makes it safe, so a future wave that breaks it fails loudly.
