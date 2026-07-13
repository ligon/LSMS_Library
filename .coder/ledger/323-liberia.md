# Prior-Art Ledger — GH #323 (Liberia)

**Search tier used:** ripgrep + git floor (gitnexus not consulted; all edits are
country-config, no library symbol changed).

## §1 Task, restated

`Country('Liberia').cluster_features()` returned 32 rows on the declared `(t, v)`
index when the survey has 250 enumeration areas. The Liberia 2018-19
`data_info.yml` wired `v` to `ea_code`, which is **not** an EA identifier — it is
an EA *serial number*, unique only within `(county_code, district_code,
clan_code)`. The downstream `.first()` collapse then picked one arbitrary
household's attributes per bucket and discarded the rest.

This is **class-1 (silently WRONG)**, not class-2 (silently missing): the API
returned confidently-labelled rows whose labels were fiction. `ea_code` 12 alone
held 621 households drawn from 54 real EAs spanning **all 14 counties**, and every
one of them was stamped `county='bong'`.

Because `_join_v_from_sample()` propagates `sample`'s `v` to every household-level
table, the conflation was library-wide for Liberia — `sample` declared
`myvars: v: ea_code` too, so all 2,986 households carried one of 32 meaningless
buckets rather than one of 250 real EAs.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Wave.cluster_features` | `country.py:1168` | **Dedicated silent `.first()`** collapse of the `i` level (GH #161). Emits **no warning at all** | no guard | root cause of silence — see §4 |
| `_normalize_dataframe_index` | `country.py:4100` | Collapses a non-unique declared index via `groupby().first()`; warns (GH #323) | partial | never reached for `cluster_features` (§4) |
| `_join_v_from_sample` | `country.py:2134` | Injects `sample.v` into every household-level table | yes | blast-radius multiplier |
| `map_formatting_function` | `country.py:746` | Resolves YAML → formatting fn; `format_id_function=True` for idxvars, `False` for myvars | — | the `.0`-suffix trap |
| `get_formatting_functions` | `local_tools.py:2227` | Registers **every callable in the wave's `mapping.py`** by name | — | **reused** as the enforcement hook |
| `format_id` | `local_tools.py:1641` | `302003132.0` → `'302003132'` | yes | reused in `v()` |
| `mapping.py::shocks` | Liberia `2018-19/_/mapping.py` | Existing `df_edit` post-processor precedent | yes | pattern followed |

## §3 Definitions & conventions in force

- `v` = sampling cluster / EA. `sample` is its single source of truth; per
  `CLAUDE.md` ("`sample()` and Cluster Identity") only `cluster_features` owns `v`
  in its declared index.
- **`format_id` is auto-applied to `idxvars` but NOT to `myvars`** —
  `CLAUDE.md`, "Gotchas with Teeth"; enforced at `country.py:801-802`.
- `aggregation:` in `data_scheme.yml` is a `{level: reducer}` grain-policy map.
  **It is inert** — listed in the `_skip` meta-key set at `country.py:2387` and
  `diagnostics.py:230`, and Malawi's own `data_scheme.yml:90` says so outright:
  *"nothing reads this yet — it documents the intended reduction"*.

## §4 Invariants & assumptions

- **`ea_unique` is the true EA key** — verified from source, not assumed:
  - it is a **string prefix of `hhid` for all 2,986 households** (2986/2986);
  - `region`, `county_code`, `district_code`, `clan_code`, `locality` are each
    constant within it — **zero** violating groups;
  - 250 groups of 10–13 households each (an LSMS EA), vs `ea_code`'s 32 groups of
    11–621.
  - `ea_code` gives 16–18 violating groups per attribute.
- **`ea_code` is a dirty mixed-type object column** — both `'132'` (str) and
  `12.0` (float) appear. A hand-built `(county, district, clan, ea_code)`
  composite therefore yields **258** groups for 250 real EAs (it splits 9 real
  EAs in two). Use `ea_unique` directly; do **not** reconstruct a composite.
- **`Wave.cluster_features()` collapses `i` with a silent `.first()` and emits no
  #323 warning.** Its justifying comment asserts cluster attributes are
  *"invariant within a cluster by construction of the LSMS-ISA sampling design"* —
  the exact invariant Liberia violated. It is **asserted, never checked**.
  Consequence: **zero** GH #323 warnings fired for Liberia even on a cold build.
  **39 waves across ~20 countries declare `i:` in `cluster_features` idxvars and
  take this same unguarded path** — see §6.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| cluster key `v` | **reuse** `ea_unique` (already materialized in source) | §4: proven identifying; no derivation needed |
| `v` string encoding on the myvar side | **reuse** `format_id` via a `v()` fn in `mapping.py` | myvars skip auto-`format_id`; float64 `ea_unique` would emit `'302003132.0'` and NaN the join |
| HH→cluster reduction | **new** guarded `cluster_features(df)` `df_edit` hook | `aggregation:` is inert (§3) — declaring it would be *prose, not enforcement*. The hook asserts constancy and raises, so the reduction is a lossless projection (exact-duplicate rows), not an arbitrary pick |

## §6 Open questions for the human

- **The framework hole is bigger than Liberia.** `Wave.cluster_features()`
  (`country.py:1168`) performs an unguarded `.first()` and does **not** emit the
  GH #323 warning. Any #323 scanner keyed on that warning **structurally cannot
  see any `cluster_features` cell** — Liberia produced 0 warnings while being
  86%-wrong. 39 waves / ~20 countries take this path (Albania, Burkina Faso,
  Cambodia, China, Guatemala, Guyana, Kazakhstan, Kosovo, Malawi, Niger, Pakistan,
  Serbia, South Africa, Tajikistan, Tanzania, Uganda, …). Most are likely fine
  (real cluster keys), but **none is checked**. Recommend hoisting this ledger's
  constancy guard into `Wave.cluster_features()` as a library-level warning.
  Deliberately **not** done here: it is a library-code edit (`.pth`-pinned, so
  unverifiable from this worktree) and would violate the "every other country
  byte-identical" constraint on this task.

---
### Phase 3 — verification

- `mapping.py::v` — **OK (anchored on §3/§5)**: delegates to `format_id`; makes the
  myvar side agree character-for-character with the idxvar side. Verified: join
  went 22/32 mismatched → 0/250 mismatched.
- `mapping.py::cluster_features` — **OK (anchored on §4/§5)**: asserts the exact
  invariant `country.py:1168` merely assumes, then de-duplicates exact-duplicate
  rows. Fault-injected (reverting `v: ea_code`) → raises
  `ValueError: {'Region': 16, 'County': 17}`, matching the independent source
  analysis. **Enforcement, not prose.**
- `data_info.yml` (both blocks) — **OK (anchored on §4)**: rekeyed in lockstep;
  changing only one would have desynchronised `sample` ↔ `cluster_features`.
- No `REINVENTION`: `format_id` and the `df_edit` hook mechanism are both existing
  tested machinery (§2). No `CONTRADICTION`.
