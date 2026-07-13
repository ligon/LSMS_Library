# Prior-Art Ledger — GH #323 (China)

**Search tier used:** ripgrep + git floor (gitnexus not consulted; the blast radius
is one country's `_/` config tree and the call sites were read directly in
`country.py`).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py:4100`) collapses a
non-unique DECLARED index with `groupby().first()`, silently dropping the surplus
rows. China 1995-97 contributes 2979 such rows across three tables:
`cluster_features` (2972 of 3002), `household_roster` (4 of 3002),
`individual_education` (3 of 2843).

The task is to make China's declared indexes unique **at the source**, so the
collapse is never reached — not to make the collapse smarter. China splits into
two genuinely different root causes (one extraction bug, one set of real source
duplicates) and they need different fixes.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4100` | reorders/drops index levels, then collapses duplicates via `groupby().first()` (warns, GH #323) | `tests/test_normalize_index_j_preserved.py` | **do not touch** — class-wide fix is a separate agent's scope |
| `Wave.cluster_features` | `country.py:1168-1201` | **second, un-warned collapse**: `groupby(level != 'i').agg(first/mean)` whenever `i` is in the index (GH #161) | partially (`test_uganda_v_grain_invariants.py`) | avoid entirely by removing `i` from idxvars |
| df_edit hook dispatch | `country.py:801`, applied `country.py:981-982` | a function in the country/wave module whose **name matches a table** becomes that table's frame-level hook, run inside `grab_data` **before** any normalize/collapse | via existing hooks | **reuse** — this is the lever |
| `china.plot_features` | `China/_/china.py:10` | existing precedent: `drop_duplicates()` + cumcount-suffix to resolve `(i, plot_id)` collisions (GH #513) | — | **extend the pattern** |
| `china.individual_education` | `China/_/china.py:79` | existing hook, bins years→attainment (#495) | — | **extend** (add dedup) |
| `mapping.v` / `mapping.Region` | `China/1995-97/_/mapping.py` | `v = hid//100`; `Region = 7 if hid//10000 <= 3 else 8` | — | **reuse unchanged** (values must not move) |

Key mechanical finding: `map_index` (`local_tools.py:2103`) only remaps `w`→`t`,
`u` NA-labels, and `j`→`i`. It never renames `v`→`i`, so a `(t, v)` table with no
`i` level is safe. Confirmed before removing `i` from idxvars.

## §3 Definitions & conventions in force

- `cluster_features` index `(t, v)`, and it **owns** `v`: "Do NOT put `v` in
  feature `data_scheme.yml` indexes other than `cluster_features`" — `CLAUDE.md`,
  "`sample()` and Cluster Identity".
- `household_roster` / `individual_education` index `(t, i, pid)` —
  `China/_/data_scheme.yml`.
- `v` for China = `hid // 100`, a 3-digit village code; 30 villages, 787
  households — `China/_/CONTENTS.org`, "PSU identification".
- Region: counties 1–3 → province 7 (Hebei); 4–6 → province 8 (Liaoning) —
  same section. Pre-existing; **not re-litigated here** (see §6).
- "class-2 (silently MISSING) is strictly safer than class-1 (silently WRONG)" —
  the task standard; drives the decision to RAISE on any undocumented collision.

## §4 Invariants & assumptions

- **The L2-country parquet (`{C}/var/`) is written POST-collapse; the L2-wave
  parquet (`{C}/{wave}/_/`) holds the truth.** Any scan of `var/` for #323 returns
  a false zero. Instrument validated on the known positives (Mali 32,026 dup rows;
  Guyana 311) *before* trusting any China number — `CLAUDE.md` cache tiers.
- The #323 warning fires **only on a cold build** (`LSMS_NO_CACHE=1`); a warm cache
  has the loss already baked in. All verification here is cold.
- `groupby().first()` is **column-wise first-non-null, not first-row** — it can
  fabricate a row belonging to neither input. Load-bearing for hid 10108, where the
  two colliding rows are two *different people* (son vs daughter-in-law).
- Region is a deterministic function of `v` (county digit) — this is what made the
  old cluster_features collapse *accidentally* lossless. It was **never checked**.
  Now asserted in `china.cluster_features`.
- China's `hid` **town digit is always 0** — the household file carries no town.
  NPT0101 encodes a real town. This is the mechanical reason the community
  questionnaire cannot be bridged to `v` (see §6).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| village universe for `cluster_features` | **reuse** `TOTEXP.DTA` (the file `sample` already reads) | guarantees `cluster_features.v` ⊇ `sample.v` by construction; 787 HH rows, not 3002 person rows |
| `v`, `Region` derivation | **reuse** `mapping.py` unchanged | API values must not move; this fix removes a mechanism, not a number |
| one-row-per-village reduction | **extend** the existing df_edit-hook pattern (`plot_features`) | runs inside `grab_data`, before both collapses; no Makefile/`materialize:` plumbing needed |
| roster / education dedup | **extend** same pattern | `drop_duplicates()` is lossless for hid 30132 (byte-identical rows) |
| hid 10108 mis-keyed pid | **new**, narrowly scoped + asserted | no existing machinery; must not generalise (see §6) |
| `_normalize_dataframe_index` change | **none — out of scope** | class-wide fix belongs to a separate agent; touching shared code would collide across worktrees |

## §6 Open questions for the human

1. **NPT0101.DTA is the "right" source for `cluster_features` and I deliberately
   did not use it.** It is a genuine one-row-per-village community questionnaire
   (31 rows, real `prov`/`county`/`town`/`village`). But its village codes cannot
   be linked to the household-derived `v`, and I established the link is *not
   recoverable*:
   - different coding schemes, no arithmetic bridge (hid's town digit is always 0;
     NPT0101's is 1–3);
   - no file in the wave carries both `hid` and a geography column (checked all
     90+ `.DTA`);
   - not inferrable — sample size per village is fixed by design (main village 50
     HH, others 20) and uncorrelated with NPT0101's village population `bi1a`
     (127–1034), so there is no signal to align on;
   - provably ambiguous anyway — county 83 has **6** villages in NPT0101 but only
     5 in the household data, and nothing says which one was not sampled.

   Using it would require a positional guess. I declined. If someone has the CLSS
   codebook mapping village codes to `hid`, `cluster_features` should be re-sourced
   from NPT0101 (it would also give `Rural` and real geography). **Region is
   unaffected either way** — province is constant within county, so it does not
   depend on the village-level correspondence.

2. The county→province map (1–3 → prov 7, 4–6 → prov 8) is inherited from
   `mapping.py` / `CONTENTS.org` and is *consistent* with NPT0101 (3 counties per
   province, 15/15 villages) but not independently *provable* from the data — the
   counts are symmetric under permutation. Preserved as-is (values unchanged);
   flagged, not fixed.

---
### Phase 3 — verification

- `china.cluster_features` (new hook) — **OK (anchored on §2, §4)**: reduces to one
  row per village via `drop_duplicates` and asserts `(t, v)` unique, which *is* the
  "Region is a function of v" invariant of §4 — a village with two Regions emits two
  rows and raises instead of silently keeping one. Reuses `mapping.Region` (§5).
- `china.household_roster` (new hook) — **OK (anchored on §4)**: `drop_duplicates()`
  for hid 30132 (lossless by construction); narrow, asserted erratum for hid 10108.
  The mis-key rule requires a real `(i, pid)` collision, so identical twins (distinct
  pids) can never match it — the over-generalisation the task warned against.
- `china.individual_education` (extended) — **OK (anchored on §4)**: dedup only, **no**
  mis-key rule. S02 has a single value column, so "identical on all non-pid fields" is
  far too weak a signature to justify deleting a row; any residual collision raises.
- `_normalize_dataframe_index` — **not touched** (§5). Deliberate: China now never
  reaches it with a duplicate, but the *class* fix is another agent's scope.
- No `REINVENTION`: the df_edit hook + `drop_duplicates` pattern is `plot_features`'
  (GH #513), reused rather than re-derived.
