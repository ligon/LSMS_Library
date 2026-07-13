# Prior-Art Ledger — GH #323 (Serbia and Montenegro)

**Search tier used:** ripgrep + git (gitnexus not consulted; the target is two
config files and one framework call-site, all located by grep from the reported
symbol `_normalize_dataframe_index`).

## §1 Task, restated

`Country('Serbia and Montenegro').cluster_features()` declares index `(t, v)`
with payload `Region` / `Rural`. Both waves' `_/data_info.yml` sourced that
CLUSTER-level table from the PERSON-level demography file
(`{wave} 1 demography.dta`) — the same file `household_roster` reads — with
`idxvars: {v: mesto, i: rbd}`. The extraction therefore emitted one row per
PERSON (2002: 19,725; 2003: 8,027) for a table that has 618 / 301 entities.
The surplus rows were collapsed silently somewhere downstream. Task: make the
extraction cluster-level so the declared index is unique by construction, and
make the reduction's precondition CHECKED rather than assumed.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave.cluster_features()` | `lsms_library/country.py:1168` | **The actual collapser.** Explicitly-defined Wave method (GH #161): if `'i' in df.index.names`, `groupby(level=non-i).agg({c: 'first', Lat/Lon: 'mean'})`. **No warning.** | no | leave alone (class fix, not mine) — my fix makes it a no-op for Serbia |
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | The GH #323 site: collapses a non-unique DECLARED index via `groupby().first()` + `RuntimeWarning`. **Never reached for cluster_features** — see §4. | partly | not the culprit here |
| `df_edit` hook | `lsms_library/country.py:801`, applied `:1053`/`:981` | A function in the country/wave module whose *name matches a declared data_scheme table* is dispatched as that table's frame-level hook, after extraction, before index normalization. | via countries | **reuse** — this is the fix's vehicle |
| `Country.formatting_functions` | `lsms_library/country.py:1351` | Loads `_/{name.lower()}.py` + `_/mapping.py`; Wave layers its own on top. For this country: `_/serbia and montenegro.py`. | — | reuse (new module) |
| `aggregation:` key | `data_scheme.yml` (Togo `:91`, Niger `:114`) | **INERT.** Only ever appears in a `_skip` set (`country.py:2387`, `diagnostics.py:174`). `diagnostics.py:171-173` calls it "documentary/forward-looking". | n/a | **NOT usable as enforcement** — see §6 |
| `melt_visit_intervals` | `lsms_library/local_tools.py:2151` | Precedent for a shared `df_edit`-hook helper that changes a table's grain. | yes | pattern reference only |

## §3 Definitions & conventions in force

- `cluster_features` owns `v`; index `(t, v)`; it is the ONLY feature allowed to
  declare `v` — per `CLAUDE.md` "`sample()` and Cluster Identity".
- A cluster table has **no household dimension**. Serbia's `i: rbd` was doubly
  wrong: bare `rbd` is the *within-cluster household number* (1..11), not a
  household key. The real key is the composite `[mesto, rbd]` (6,386 / 2,548
  households), which `household_roster` and `sample` correctly declare.
- Two build paths (YAML vs. script) — per `CLAUDE.md` "Two Build Paths". This
  table stays on the **YAML path**; the hook is the sanctioned escape valve for
  a transformation YAML can't express.

## §4 Invariants & assumptions

- **The GH #323 `RuntimeWarning` never fires for `cluster_features` — in any
  country.** `Wave.cluster_features()` (`country.py:1180`) strips and collapses
  the `i` level *before* the frame ever reaches `_normalize_dataframe_index`,
  which consequently always sees a unique `(t, v)` index. Verified by tracing a
  cold build: `grab_data` → 19,725 rows `(t,v,i)` non-unique;
  `_normalize_dataframe_index` ← 618 rows `(t,v)` unique; zero warnings raised.
  This is a *second, entirely silent* collapse site, distinct from #323's.
- **The GH #161 comment states an UNCHECKED precondition**: "Region/Rural/District
  are invariant within a cluster by construction of the LSMS-ISA sampling design"
  (`country.py:1175-1178`). It is prose. Nothing verifies it.
- **For Serbia that precondition HOLDS** (verified against source `.dta`):
  clusters with >1 distinct `stratum` = 0/618 (2002), 0/301 (2003); same for
  `tip` and `okrug`; zero NaNs in `mesto`/`stratum`/`tip`. So `first()` provably
  could not pick wrong, **no data was ever lost, and this cell recovers 0 rows.**
  The bug is the *silence and the unchecked assumption*, not a wrong number.
- `drop_duplicates(['mesto','stratum','tip'])` → 618 / 301 rows = `nunique(mesto)`
  exactly ⇒ the dedup key is unique. This is what licenses the collapse.
- v0.8.0 content-hash invalidation **does** cover this fix (`data_info.yml` is
  hashed): a warm pre-fix cache holding the 19,725-row wave parquet rebuilds to
  618 with no manual `cache clear`. Verified empirically.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| cluster-grain reduction | **new** (`cluster_features` df_edit hook) | Must run at EXTRACTION so the declared index is unique by construction. No existing helper does assert-then-dedup. |
| invariance enforcement | **new** (raise in the hook) | `aggregation:` is inert (§2); a `data_scheme.yml`/`CONTENTS.org` note is prose. Only executable code enforces. |
| the reducer itself | **reuse semantics of `first`** | Legitimate ONLY because the payload is provably cluster-invariant — so the hook *checks* that precondition instead of trusting it. |
| `i: rbd` idxvar | **delete** | Meaningless for a cluster table, and not a valid household key. |

## §6 Open questions for the human

- **`aggregation:` in `data_scheme.yml` is a no-op.** Togo and Niger both declare
  `aggregation: {visit: first}` on `interview_date`; nothing in the library reads
  it as a policy (`country.py:2387` / `diagnostics.py:174` only *skip* it when
  enumerating columns). Anyone "declaring" an aggregation there today is writing
  documentation, not enforcement. The class-wide #323 fix was expected to lean on
  this key — it must first be given teeth.
- **`Wave.cluster_features()`'s GH #161 collapse is unguarded for every other
  country.** Serbia is now extracted at cluster grain and no longer depends on it,
  but any country whose `cluster_features` declares `i` in `idxvars` (Albania,
  confirmed; and others) still gets a silent unchecked `.first()`. The safe class
  fix is to add the same invariance check *there* (warn, or raise) — it would be a
  no-op wherever the LSMS-ISA design assumption actually holds, and would surface
  the countries where it does not. Deliberately NOT done here: it is library code
  shared by ~14 countries whose payloads I have not validated, and hard-failing
  them is exactly the outcome the task brief warns against.

---
### Phase 3 — verification

- `cluster_features(df)` (`countries/Serbia and Montenegro/_/serbia and montenegro.py`)
  — **OK (anchored on §2, §4, §5)**: uses the existing `df_edit` dispatch
  (`country.py:801`); performs the *declared* dedup and RAISES if the §4
  invariance precondition is violated. Not a reinvention of
  `_normalize_dataframe_index` — it runs earlier, so that site is never asked to
  guess; not a reinvention of `Wave.cluster_features()`'s reducer — it makes that
  branch a no-op by removing the `i` level entirely.
- `data_info.yml` × 2 — **OK (anchored on §3)**: drops `i: rbd`, leaving
  `idxvars: {v: mesto}`, matching the declared `(t, v)` cluster grain.
- Output — **unchanged by construction**: all four Serbia tables byte-identical
  before/after (`assert_frame_equal`); 0 rows recovered, as predicted in §4.
