# Prior-Art Ledger — GH #637, Tanzania `groupby().first()` key-soundness review

**Search tier used:** ripgrep + git (gitnexus not consulted; the work is
config/script archaeology in one country tree, and the authoritative prior art
turned out to be in-file comments rather than the call graph).

## §1 Task, restated

GH #637 inventories 49 `groupby(...).first()` aggregation sites.  Its own
correction thread establishes that the per-column NA-skip is a **correct
completion** (`NaN` is absence, not contradiction — `reduce_to_agreed` produces
the same composite deliberately), so the issue is **not** a list of bugs.  It is
an inventory of sites whose correctness depends on **the key being sound**: the
composite is wrong only where the duplicate rows describe **different real
entities**, which is a broken *identifier* and, per GH #323 decision D1, must be
fixed as an identifier — never with a reducer or an `aggregation:` key.

This ledger covers Tanzania's block: the 17 wave-level
`groupby(level=index.names).first()` sites in `2008-15/`, `2019-20/`, `2020-21/`,
the two country-level concatenators (`_/anthropometry.py`,
`_/individual_education.py`), and the two `_/tanzania.py` sites
(`people_last7days_for_wave`, `community_prices_for_wave`).  `cluster_features`
is out of scope — PR #642 owns it.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| the UPHI-replication diagnosis | `Tanzania/2008-15/_/sample.py:54-79` | Documents that `upd4_hh_a.dta` is keyed on the panel-tracking LINE (UPHI): 29,250 rows → 16,540 household-rounds, group sizes 1..11.  Asserts the payload is invariant across lines rather than trusting it. | yes (`test_gh323_tanzania_cluster_key.py`, PR #642) | **reuse** — the same replication explains housing / food_coping / shocks |
| same, restated | `Tanzania/2008-15/_/interview_date.py:41-48`, `cluster_features.py:51` | Two more modules that already name the replication | yes | reuse |
| `map_08_15` | `Tanzania/_/tanzania.py:28` | Builds the panel linkage from `(r_hhid, round, UPHI)`; emits **r_hhid-keyed** composite ids | via `panel_ids.py` | reuse — fixes the namespace `shocks` must live in |
| `updated_ids.json` | `Tanzania/_/updated_ids.json` | `id_walk` mapping; keys are r_hhid strings (`'0001-001' → '01010140020171'`) | yes | reuse |
| `id_walk` | `Tanzania/_/tanzania.py` | Renames the `i` level only | yes | reuse |
| GH #114 | closed issue | `map_08_15` maps split-offs onto the parent's canonical id (a known, separately-tracked collapse) | — | cite, don't touch |
| GH #132 | closed issue | Filed the 60% duplicate `(t, i, Shock)` rate this review's fix is downstream of | — | cite |

## §3 Definitions & conventions in force

- **`NaN` is absence, not contradiction** — GH #637 correction comment; pinned by
  `test_nan_is_absence_not_contradiction` in `tests/test_gh323_explicit_reducers.py`.
  Therefore `.first(skipna=False)` is a **regression**, not a fix.
- **D1** — a key that merges distinct entities is fixed at the *identifier*, never
  with a reducer or an `aggregation:` key (GH #323).
- **`i`** — the household id, per `lsms_library/data_info.yml`; for Tanzania it is
  `r_hhid` (2008-15), `sdd_hhid` (2019-20), `y5_hhid` (2020-21), as used by
  `sample.py` and `household_roster`.
- **`UPHI`** — "Universal Panel Household Identifier" in the upd4 files: a
  panel-tracking **line**, 1..14,985, NOT a household.  `(round, UPHI)` → exactly
  one `r_hhid`; one `(round, r_hhid)` holds up to 11 UPHI.
- **`r_hhid` is not a stable panel id** — its format changes by round (14-digit R1,
  16-digit R2, `NNNN-NNN` R3/R4) and rounds 3-4 reuse 1,857 strings, so `t` is
  load-bearing in every 2008-15 key (`sample.py:62-71`).

## §4 Invariants & assumptions

- **The upd4 household-level modules replicate a household-round once per
  descendant line; the individual-level ones do not.**  `upd4_hh_i1` (housing),
  `upd4_hh_h` (food_coping) and `upd4_hh_r` (shocks) carry `UPHI`; `upd4_hh_b`
  (roster), `_v` (anthropometry), `_c` (education), `_g` (satisfaction) carry
  `(r_hhid, UPI)` and no `UPHI`, and are duplicate-free.
- **Where the replication occurs, the payload is byte-identical across lines.**
  Measured, not assumed — see §5.  A collapse is therefore de-replication, not a
  choice, and must stay guarded by an assert so a future re-release cannot make
  it a silent choice.
- **"exact" is not evidence of one entity** (GH #637 / PR #646 trap 1).  Housing
  was therefore corroborated by lineage: a round-1 household with k lines maps to
  k DISTINCT round-4 households in 174 of the 211 cases observed.
- **Invariance can be an artefact of missingness** (trap 2).  Not triggered here:
  every column tested is densely populated (0 null `UPI`, 0 null `sdd_hhid` /
  `y5_hhid`, `hr_01` present on all 449,435 shock rows).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| 2008-15 `shocks.i` | **new value, existing convention** | Was `UPHI`; set to `r_hhid`, the id `sample` / `household_roster` / `updated_ids.json` already use.  Not a new identifier — the country's existing one. |
| the collapse that `i = r_hhid` re-exposes | **reuse** `sample.py`'s pattern | assert payload invariance, then `.first()`.  0 of 134,035 duplicate `(round, r_hhid, hr_00)` groups differ on any of the nine `hr_*` columns. |
| housing / food_coping collapses | **keep** | Same replication; 0 of 8,488 and 0 of 6,814 duplicate groups differ on ANY source column. Comment only. |
| the other 15 sites | **keep** | `.first()` never fires; the keys are unique in the source.  Comment only. |
| `community_prices` collision | **defer** | 261 (2019-20) / 1,281 (2020-21) colliding groups, **100%** spanning >1 source item code and **0** repeating a code — the deliberate `harmonize_community_price` label lump, so no additional level exists that would not break the shared `j`.  The residue is arbitrary resolution of two OBSERVED prices (GH #323 Option B), explicitly not #637. |

## §6 Open questions for the human

- **`community_prices`**: 243/261 and 1,149/1,281 colliding groups hold genuinely
  different observed prices (cluster `01-06-47-85`, 2020-21, "Millet & Sorghum
  (grain)" per Kg: 2,000 **and** 16,000 TSh; `.first()` keeps 2,000).  D1 forbids
  a reducer and no level is available.  Blocks: whether the feature should carry
  a source-code level at the cost of the cross-feature `j` join, or accept an
  explicit documented summary.
- **2008-15 `shocks` and GH #114**: with `i = r_hhid` the table now participates
  in `id_walk`, which for Tanzania maps split-offs onto the parent's canonical id
  (#114).  Measured here as **0 collisions** on shocks, so nothing is lost today —
  but shocks now shares that convention, for better or worse.

---
### Phase 3 — verification

- `Tanzania/2008-15/_/shocks.py` `i` — **OK (anchored on §3, §5)**: adopts the
  country's existing household id rather than inventing one; namespace verified
  against `sample()` (0 unknown `(t, i)` pairs).
- `Tanzania/2008-15/_/shocks.py` assert-then-collapse — **OK (anchored on §4)**:
  copies `sample.py:76-79`'s pattern rather than re-deriving one.
- All other sites — **OK (anchored on §5)**: comment-only; no behaviour change.
- No `.first(skipna=False)`, no `aggregation:` key, nothing under
  `lsms_library/*.py` outside `countries/Tanzania/**` — **OK (anchored on §3)**.
