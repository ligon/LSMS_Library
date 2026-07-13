# Prior-Art Ledger — GH #323 (Iraq)

**Search tier used:** ripgrep + git (gitnexus not consulted; the surface is one
function in `country.py` plus two country YAMLs).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py`) collapses a non-unique
DECLARED index with `groupby().first()`, silently discarding the dropped rows.
Iraq is reported as affected in two cells: `2006-07/cluster_features` (14,861 of
17,822 rows) and `2012/cluster_features` (22,318 of 25,146).

For Iraq this is **not** the data-loss class. `cluster_features` is declared
`(t, v)` — a CLUSTER-level table — but both waves extract it from the
household-level COVER PAGE (the same file `sample` reads). The "extra"
granularity being dropped is the household `i`, which a cluster-level table must
not retain. The collapse is therefore a correct household→cluster projection; it
was merely **undeclared**, and an undeclared `.first()` resolves genuine
within-cluster conflicts by source order. `rows_recovered = 0` — and that zero
is the finding.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4173` | reorder/drop index levels, then collapse duplicates | indirectly | **extend** (new declared branch) |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py` | per-table SUM policy for `food_acquired` (GH #514) | yes | precedent — the *other* declared collapse |
| `aggregation:` scheme key | `data_scheme.yml` ×9 countries | `{visit: first}` grain policy (SkunkWorks/`grain_aggregation_policy.org`) | **no consumer** | reuse the KEY, add the scalar form |
| `_SCHEME_SKIP_KEYS` | `country.py:3788` | meta-keys excluded from the column list | — | extend (add `aggregation`) |
| `Iraq/_/mapping.py:Rural` | Iraq `_/` | parses the 2006-07 `xstrat` design-stratum label → Urban/Rural | — | reuse (explains 2006-07 constancy) |

**Prior art actually found, and reused:** the `aggregation:` key already exists,
is already reserved as a meta-key in two places (`country.py:2387`,
`diagnostics.py:174`), and already has a design doc — but is **read by nothing**.
It was pure prose. This task supplies its first consumer rather than inventing a
competing key. The design doc itself (`grain_aggregation_policy.org:155`) names
`country._normalize_dataframe_index`'s `groupby().first()` as the GH #323 site.

## §3 Definitions & conventions in force

- `cluster_features`: canonical index `(t, v)` — `lsms_library/data_info.yml:16`.
  It OWNS `v` and carries no `i`; it is in the v-join skip set (per `CLAUDE.md`,
  "`sample()` and Cluster Identity").
- `aggregation:` (MAPPING form): grain-collapse policy, `{level: reducer}` —
  `SkunkWorks/grain_aggregation_policy.org`. Distinct contract; left inert.
- class-1 (silently WRONG) vs class-2 (silently MISSING): class-2 is strictly
  safer — per the task standard.

## §4 Invariants & assumptions

- **A cluster-level column must be CONSTANT within its cluster.** This is the
  invariant the projection depends on. It was *asserted in a comment* in both
  waves' `data_info.yml` ("Constant within cluster (verified, 2961 clusters)")
  and enforced by nothing — the leak. **Prose is not enforcement.**
- 2006-07 `Rural` derives from the **design stratum label** (`xstrat`,
  "… - urban"/"… - rural") ⇒ constant within cluster *by construction*.
- 2012 `Rural` derives from **q00_16, a per-household question** ⇒ within-cluster
  disagreement is *possible*. Verified: it occurs in exactly one cluster.
- The 2012 `stratum` does **not** encode urbanicity (114 of 118 strata are mixed
  on q00_16), so **no design variable can adjudicate the conflict.** This is the
  fact that forces NA over a guess.
- The L2-**country** parquet is written POST-collapse; the L2-**wave** parquet
  holds the truth. Any instrument must read the wave tier (validated below).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| declared-collapse policy | **reuse key / new consumer** | `aggregation:` existed with no consumer; scalar form is structurally distinguishable (`isinstance(str)`) from the inert dict form |
| `unique` reducer | **new** (`_collapse_declared_unique`) | no existing reducer *checks* an invariant; `first` discards, `sum` double-counts. Neither fits a projection whose correctness rests on constancy |
| cluster 964 `Rural` | **NA + warn** | no principled rule exists (see §4); majority (7/9) would be a heuristic, not a determination |
| Iraq row recovery | **none (0 rows)** | the collapse is lossless; the correct tables ARE 2,961 / 2,828 rows |

### Why not `first`, `sum`, or majority?

- `first` — what's there now: silently takes whichever row sorted first. For
  cluster 964 that is RURAL, the **2-of-9 minority**. Source order is not evidence.
- `sum` — meaningless for a categorical projection; would double-count.
- **majority** — tempting (7/9 URBAN) and it *would* have flipped 964 to the
  likelier value, but it is a guess dressed as an answer: it silently
  manufactures a value the source does not determine, and it would fail silently
  on a 50/50 split. Rejected as class-1.
- **`unique` + NA on conflict** — chosen. Where the invariant holds (2,961 +
  2,827 of 2,828 clusters) the value is *provably* the only one present, so the
  projection is exactly lossless. Where it does not hold, the value is genuinely
  unknowable and we say so. Class-2.

The check is **per-column**, so cluster 964 keeps its unambiguous `Region=ERBIL`
and loses only the ambiguous `Rural`. Dropping the whole row would have destroyed
good data to hide one bad cell.

**Durability note (why this beats the existing warning).** The #323
`RuntimeWarning` fires only on a COLD build, so in warm operation the collapse
stays baked in the cache and never re-announces itself — *the bug hides behind
the cache it poisoned*. The `pd.NA` written here is **data**, not a log line: it
is baked into the parquet and survives the cache. The fix cannot be re-hidden.

## §6 Open questions for the human

- **The CLASS fix is only half-landed, deliberately.** This task supplies the
  declaration mechanism and applies it to Iraq. It does **not** flip the default
  for an *undeclared* non-unique index from `warn + .first()` to `raise`. That
  flip is the real #323 class fix, but it is a cross-country breaking change:
  ~13 other countries / ~44 cells are still undeclared and other fix-agents are
  mid-flight on this same function. Flipping it here would break them and would
  make "nothing else moved" false. **Recommend the orchestrator flip the default
  once the per-country agents have landed**; `_collapse_declared_unique` is the
  mechanism that makes that flip survivable. Iraq's declaration is only correct
  *because* the invariant is checked.
- Should cluster 964's `Rural` be recoverable from an external frame (e.g. the
  2012 EA listing / census frame)? If IHSES-II documentation gives an
  authoritative EA urbanicity, the NA could become a value. Blocks nothing.

---
### Phase 3 — verification

- `_collapse_declared_unique` (`country.py`) — **OK (anchored on §5)**: new; no
  existing reducer enforces an invariant, and it reuses the existing
  `aggregation:` key rather than minting one.
- `_normalize_dataframe_index` — **OK (anchored on §2/§4)**: extended with a
  declared branch placed *before* the additive and `.first()` paths; the
  undeclared path is byte-for-byte unchanged (regression-tested).
- `_SCHEME_SKIP_KEYS += 'aggregation'` — **OK (anchored on §2)**: behaviourally
  inert (the pre-existing `col not in df.columns` guard already skipped it);
  brings this set in line with the two that already list it.
- Iraq `data_scheme.yml` / both `data_info.yml` — **OK (anchored on §4)**: the
  false claim "one row per cluster" (the file is one row per HOUSEHOLD) is
  corrected, but per §4 the *comment* is not the fix — the enforced invariant is.
- `rows_recovered = 0` — **OK (anchored on §1)**: not a failure to recover; the
  correct tables are 2,961 / 2,828 rows. The behavioural change is that cluster
  964's urbanicity stops being silently wrong.
