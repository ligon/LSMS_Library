# Prior-Art Ledger — GH #323 (Benin, Togo)

**Search tier used:** ripgrep + git floor (gitnexus not consulted; the work is
config-scoped to two countries and the framework symbol was read directly).
The decisive prior art was found by `git diff origin/development...fix/323-cotedivoire`.

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py`) reorders a table's index
to the levels declared in the country's `_/data_scheme.yml`, and when the result
is NOT unique it collapses it with `groupby().first()` — silently discarding the
dropped rows. Benin and Togo both trip this on **`plot_inputs`**, and in exactly
one way, the same way CotedIvoire did (its case B):

`plot_inputs` declares `index: (t, i, input, crop, u)`. `harmonize_input` maps
**every** seed label onto the single `input` value `Seed`, so `crop` is the only
level left to tell two seed line-items apart. But `harmonize_seed_crop` was
**non-injective**: four labels shared one `Autre crop` catch-all bucket, and
*three* of them actually occur in the EHCVM `s16b` roster of both countries. When
a household reports two of them in the same unit, both rows land on one index
tuple and `groupby().first()` throws one away.

Measured (2026-07-13, `LSMS_NO_CACHE=1`, wave frame vs. API):

| country | wave    | wave rows | API rows (pre-fix) | destroyed | conflicting groups |
|---------|---------|-----------|--------------------|-----------|--------------------|
| Benin   | 2018-19 | 10,605    | 10,534             | **71**    | 64                 |
| Togo    | 2018    | 13,733    | 13,565             | **168**   | 162                |

Of Togo's 168, **165** are #323 destruction; the other **3** are *hollow* rows
(`Quantity`, `Purchased`, `Quantity_purchased` all `<NA>`) removed by the
framework's deliberate `dropna(how='all')` safety net — see §4.

(The issue text quotes 68/61 and 156/153. Those are the same defect counted more
conservatively: they exclude the groups whose duplicate rows carry *identical*
values, where `first()` loses no information. Benin 64−3=61 groups / 71−3=68 rows;
Togo 162−9=153 / 165−9=156. Both accountings go to **0**.)

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py` | reorders to the declared index; `groupby().first()` collapse when non-unique — **the silent row-eater** | — | **untouched** (owned centrally by PR #614) |
| `dropna(how='all')` | `lsms_library/country.py:2217` | universal safety net: drops rows where every non-index column is NaN | — | untouched; explains Togo's residual 3 |
| `harmonize_seed_crop` | `lsms_library/countries/{Benin,Togo}/_/categorical_mapping.org` | EHCVM seed-type label → the seed's crop (the `s16b` roster has no crop column) | now yes | **extend** — split the catch-all |
| `harmonize_input` | same files | `s16bq01` label → canonical `input`; maps **all** seed labels to `Seed` | — | reuse as-is (correct: `input` is the input *identity*, the crop lives in `crop`) |
| `_finish_plot_inputs` | `Benin/2018-19/_/plot_inputs.py`, `Togo/2018/_/plot_inputs.py` | coerces dtypes, fills `CROP_NA` / `UNIT_NA` sentinels, sets the declared index | now yes | **extend** — add the uniqueness guard |
| CotedIvoire #323 fix | `fix/323-cotedivoire` (`origin`) | the identical defect + fix on the sibling EHCVM country | `tests/test_gh323_cotedivoire.py` | **the pattern followed here** |

**Prior art is decisive.** CotedIvoire is EHCVM, the `s16b` labels are
standardized across EHCVM countries, and Benin's and Togo's
`harmonize_seed_crop` tables were copied *verbatim* from the Niger EHCVM
reference — so they inherited the identical bug. The fix here is CIV's fix,
label-for-label (`Autre céréale`, `Autre tubercule`), so the EHCVM family stays
consistent.

## §3 Definitions & conventions in force

- **The #323 doctrine**: duplicates on a declared index mean **the identifier is
  broken or a level is missing** — *never* that a reducer should be declared.
  Fix the index. (`docs/323-grain-collapse-sites`; `fix/323-cotedivoire` ledger.)
- **EHCVM index conventions**: `v: grappe`, `i: [grappe, menage]` — per
  `CLAUDE.md` §"Gotchas with Teeth". Benin and Togo are both EHCVM 2018-19.
- **`plot_inputs` grain**: `(t, i, input, crop, u)` per each country's
  `_/data_scheme.yml`. EHCVM records inputs at **household × input**, not
  plot × input (no parcel-level input question), so `plot` is deliberately not a
  level. `plot_inputs` is *not* registered in `data_info.yml`'s `index_info`.
- **Automatic categorical mappings** apply on a name match (`CLAUDE.md`). There
  is **no** table named `crop`, `input` or `u` in either country's
  `categorical_mapping.org`, so those index levels are **not** remapped at API
  time; the wave script's values are final. (The library-level
  `lsms_library/categorical_mapping/u.org` *does* canonicalize `u` at API time —
  `Kilogramme`→`Kg`. It is injective over the units in use, so it neither causes
  nor hides this defect.)
- Everything else: `STANDING.md` §3.

## §4 Invariants & assumptions

- **`harmonize_seed_crop` must stay INJECTIVE over the labels actually present.**
  This is the invariant the defect violated, and it is now *enforced*, not merely
  documented: a uniqueness assertion on the declared index at the end of each
  wave script fails the build loudly. Verified by re-widening the bucket and
  watching both builds die (Benin: 47 dup tuples; Togo: 59).
- **`Purchased` is NOT an index level and must not become one.** Togo's colliding
  rows differ in `Purchased` (grappe 101 / menage 9: 16 Charrette received as a
  gift from another household vs. 3 Charrette bought at the market -- read from
  s16bq05 / s16bq08 / s16bq04; this line had the two reversed until 2026-11-13,
  because the served `Purchased` was then the gift flag s16bq05), which makes
  adding it to the index look tempting.
  It is wrong: `Purchased` is a measured *attribute* of a line-item, not part of
  its identity — it would not separate two distinct seeds that were both
  purchased, and it would corrupt the declared grain. Evidence that the
  identifier (not a missing level) is the fault: **0** of the 226 colliding
  groups across both countries repeat the *same* raw `s16bq01` label — every
  single collision is between two **distinct** reported items pooled by the lossy
  crop map. A counterfactually fully-injective crop map drives destroyed rows to
  0 in both countries. See §6 for the residual.
- **Hollow rows are dropped by design.** `country.py:2217` `dropna(how='all')`
  removes rows where every non-index column is NaN. Three Togo rows qualify
  (`529004`/Fungicide, `169011`×2/Seed — the household named the input and
  reported nothing about it). This is table-agnostic framework behaviour, it
  destroys no reported value, and it is **not** this fix's business.
- **`lsms_library/*.py` is off-limits here** — the core `_normalize_dataframe_index`
  fix is owned by PR #614 and a separate Site-2 PR. This change is config/script
  only.
- Repo-wide landmines: `STANDING.md` §4.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| the collapse itself (`_normalize_dataframe_index`) | **reuse, untouched** | owned centrally (PR #614); a per-country patch is exactly the failure mode this task was scoped to avoid |
| seed → crop resolution | **extend `harmonize_seed_crop`** | the table already exists and is the right place; it was merely lossy |
| making the map injective | **reuse the CIV pattern** (`Autre céréale` / `Autre tubercule`) | identical defect on a sibling EHCVM country; same source labels; keeps the EHCVM family label-consistent |
| the build-time guard | **reuse the CIV pattern** (index-uniqueness assert at the tail of the wave script) | prose does not enforce; this fails the build loudly on any future re-widening |
| a reducer / `groupby().agg()` on plot_inputs | **rejected** | violates the #323 doctrine (§3) — fix the identifier, never declare a reducer |
| adding `Purchased` to the index | **rejected** | see §4 — attribute, not identifier; 0 same-label collisions prove the crop map is the whole fault |

## §6 Open questions for the human

- **Residual (guarded) non-injectivity, inherited from CIV.** Two label pairs
  still share a Preferred Label, and both have **0 rows today** in both
  countries, so neither can collide:
  - the bare code `20` → `Autre crop` (an unlabelled category that leaked into
    the label column), alongside `Autres semences`;
  - `Semences de sésame` → `Sésame`, alongside the source's typo
    `Semences de césame` (13 rows Benin / 103 Togo — the *only* variant that
    occurs). These two ARE the same crop, so pooling them is correct, not lossy.

  CIV made the same call. If a future EHCVM wave ships rows under `20`
  co-occurring with `Autres semences`, the build assertion fires — loudly, by
  design — rather than silently eating a row. Flagging it so the choice is
  visible rather than implicit.
- **`Autre céréale` / `Autre tubercule` have no `harmonize_food` counterpart.**
  `Autre crop` does (`Autre (à préciser)` → `Autre crop`). The other two are new
  labels living only in `harmonize_seed_crop`. There is no runtime coupling (the
  `crop` level is not auto-mapped, and `plot_inputs` is not in `index_info`), and
  CIV did the same. If `plot_inputs` is ever registered in `index_info` and its
  `crop` level harmonized against `harmonize_food` cross-country, these two
  labels will need homes there.

---
### Phase 3 — verification

- `harmonize_seed_crop` (Benin, Togo `_/categorical_mapping.org`) — **OK (anchored on §2, §4, §5)**: extends the existing table rather than adding new machinery; the split labels are CIV's, so no divergence inside the EHCVM family.
- uniqueness assertion in `Benin/2018-19/_/plot_inputs.py`, `Togo/2018/_/plot_inputs.py` — **OK (anchored on §4, §5)**: reuses the CIV guard pattern verbatim; negative-tested (re-widening the bucket fails both builds).
- **no reducer declared, no index level added** — **OK (anchored on §3, §4)**: the #323 doctrine is honoured; the identifier was lossy and the identifier is what changed.
- **no `lsms_library/*.py` touched** — **OK (anchored on §4)**: `git diff --stat` confirms the change is confined to `lsms_library/countries/{Benin,Togo}/**` plus a new test.
- **REINVENTION check** — none. The one thing this task could have reinvented is the CIV fix; it was found first (`git diff origin/development...fix/323-cotedivoire`) and followed rather than re-derived.

**Result (2026-07-13, `LSMS_NO_CACHE=1`):**

| country | wave rows | API rows before | API rows after | destroyed before → after |
|---------|-----------|-----------------|----------------|--------------------------|
| Benin   | 10,605    | 10,534          | **10,605**     | 71 → **0** |
| Togo    | 13,733    | 13,565          | **13,730**     | 165 → **0** (+3 hollow rows dropped by design, §4) |

Duplicate tuples on the declared index: **0** in both. Rows were **recovered**
(+71 Benin, +165 Togo), never lost. Both worked examples now carry distinct crop
keys and both line-items survive.
