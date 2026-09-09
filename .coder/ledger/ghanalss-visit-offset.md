# Prior-Art Ledger — ghanalss-visit-offset

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`.

**Search tier used:** ripgrep + git floor. GitNexus MCP is connected this session
but its FTS extension failed to load (`LOAD fts failed` on every call), so symbol
search was done with `rg`; blast radius reported from that sweep, per
`CLAUDE.md` §"Code intelligence: GitNexus is OPTIONAL".

## §1 Task, restated

GhanaLSS `food_acquired` is built by seven wave-level scripts that emit the
canonical long form with a country-local `visit` index level (design decision D1,
`slurm_logs/DESIGN_ghanalss_food_acquired_2026-06-15.org:103-119`: keep the
repeated-visit recall structure, let the derived tables sum it out). Each script
stacks two source modules — Section 9B (purchases) and Section 8H (consumption of
own produce) — whose per-visit columns are named `s9bq{n}…` and `s8hq{n}…`. The
scripts write **`n` itself** into `visit`. But `n` is the questionnaire's
*question number*, and the two modules number from different origins, so the same
calendar visit lands on two different `visit` values depending on `s`. Fix the
mapping so `visit` is the calendar visit each module's own Stata labels name, and
record the GLSS visit/recall design in `GhanaLSS/_/CONTENTS.org`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `add_visit_level` | `lsms_library/build_transforms.py:546` | Appends a **constant** `visit` level to a `food_acquired` frame so single-recall waves share an index shape with repeated-recall waves. Defines `visit` as the "recall-occasion" level; Burkina Faso EMC's four quarterly passages are `visit = 1..4`. Raises if `visit` already present. | yes (GH #323 motivating case) | **not applicable** — GhanaLSS already has a per-occasion level; this stamps a constant. Cited as the convention oracle only. |
| `melt_visit_intervals` | `lsms_library/local_tools.py:2705` | `df_edit` helper for `interview_date`: melts per-visit timestamps onto a `visit` level. States the output contract: "`visit` is an ordinal `Int64` (1, 2, 3, ...)". | yes | **not applicable** (different table) — cited as the second, independent statement of the ordinal convention. |
| `food_acquired_to_canonical` | `lsms_library/build_transforms.py:34` | Canonical `food_acquired` helper; `drop_columns=('visit',)` default drops EHCVM `vague`. | yes | not used by the GhanaLSS wave scripts (they hand-build the frame). |
| `_stack_visits` | `GhanaLSS/2016-17/_/food_acquired.py:98` | Wave-local reshaper: takes an explicit `visits` iterable, loops it, writes `part['visit'] = v`. **This is where the raw stem number becomes `visit`.** | no | **extend** — needs a stem→occasion mapping, not a raw stem. |
| `food_expenditures_from_acquired` | `lsms_library/transformations.py:903` | Derived table; groups by `['t','i','j','s']`, so `visit` is summed out. | yes | unchanged — the fix is invisible to it (see §4). |

## §3 Definitions & conventions in force

- `visit`: the **recall occasion**, not a survey round. `build_transforms.py:548,556`
  ("recall-occasion"; single-recall waves get `visit = 1`) and
  `local_tools.py:2739` ("`visit` is an ordinal `Int64` (1, 2, 3, ...)") both
  describe a **single-module** frame, where the ordinal and the enumerator's
  calendar visit coincide. Neither settles a two-module table whose modules start
  at different visits; see Phase 3 and §5.
- `visit` is NOT EHCVM's `vague` (a sample split, not a repeated measure) —
  `build_transforms.py:566-569`.
- GhanaLSS declares `food_acquired: (t, v, i, j, u, s, visit)` —
  `GhanaLSS/_/data_scheme.yml:17`, wave-level `(t, i, j, u, s, visit)` at `:68`.
- D1 (keep `visit`, aggregate in derived tables) —
  `slurm_logs/DESIGN_ghanalss_food_acquired_2026-06-15.org:103-119`.

## §4 Invariants & assumptions

- **Derived tables must be bit-identical after the fix.** `food_expenditures`,
  `food_quantities`, `food_prices` all group by index level *name*, never by
  `visit` *value* (`transformations.py:980,1048,1237`), so renumbering `visit`
  cannot move them. This is the invariant that bounds the blast radius; verify it,
  do not assume it.
- The frame must stay unique on `(t,i,j,u,s,visit)` — D1's bonus property
  (`DESIGN…:115-119`) is that GhanaLSS needs no `groupby().first()` collapse.
  Renumbering 8H `3..8 → 2..7` is injective within `s='produced'` and cannot
  collide with `s='purchased'` rows, which differ in `s`. Re-check row counts.
- Editing a wave script changes its text hash → L2 cache invalidation for
  GhanaLSS `food_acquired` (`CLAUDE.md` §Cache Behavior). Expected, not a bug.
- 8H questions 1 and 2 are **screeners**, not visits ("HH consume any home
  produce", "No. of months eat home produce" — 1998-99 `SEC8H.DTA` labels), which
  is *why* the consumption stems start at 3. Any wave whose 8H stems start
  elsewhere needs its own evidence before renumbering.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| meaning of `visit` | the **calendar visit**, consistent with §3's recall-occasion sense | A per-module 1..N ordinal is *not* general: GLSS5 fields 9B from the 2nd visit and 8H from the 3rd, so counting from 1 within each module misaligns them by one. The calendar number aligns them, is what every wave's Stata labels state (so it is checkable against the source), and is the same clock `interview_date`'s `visit` uses. |
| stem → calendar map | **new**, per wave, evidence-gated | The offset is a property of each round's questionnaire, not a constant. Only waves with direct label evidence are renumbered (§6). |
| derived-table behaviour | **no change** | Invariant in §4. |

## §6 Open questions for the human

- **1991-92 (GLSS3) is left unchanged.** Its `S9B.DTA` / `S8H.DTA` carry **no
  variable labels at all**, so its offsets have none of the evidence the other
  four waves have (9B `q2..q10`, 8H `q3..q12` — note 9B itself starts at q2 there,
  unlike every later wave). Documented as unverified rather than renumbered on a
  guess. Resolving it needs the GLSS3 questionnaire
  (`1991-92/Documentation/GHA_1991_GLSS_Household_Questionnaire_EN.pdf`).
- Reference period is still nowhere in the schema: GLSS7 `food_expenditures` is a
  6 × 5-day = **30-day** figure, GLSS5's window differs again, and nothing records
  either. The canonical `RecallWindow` column (GH #817) is per-row and Togo-only,
  so a wave-constant window has no home. Out of scope here; worth an issue.

---
### Phase 3 — verification

- `_stack_visits` (2016-17) — **OK (anchored on §2, §5)**: extended, not
  reinvented; `visits` became an explicit `{question number -> calendar visit}`
  map. Measured as a pure relabel against the pre-fix script on the real build:
  1,124,871 rows both ways, `Quantity` / `Expenditure` / `Price` sums identical
  to the cent, and the old index shifted by (+1 purchased, -1 produced) is
  `.equals()` the new index. New index unique.
- Wave visit maps (1998-99, 2005-06, 2012-13, 2016-17) — **OK (anchored on §4)**:
  each built and its delivered `visit x s` crosstab matches the wave's own Stata
  labels. 1998-99 → both modules 2..7; 2005-06 → purchased 2..11, produced 3..11
  (the real asymmetry §5 predicted); 2016-17 → both 2..7.
- **CONTRADICTION found and corrected in this ledger, not in the code**: §3's
  first reading was that `visit` must be a 1..N ordinal, from
  `add_visit_level` / `melt_visit_intervals`. GLSS5 falsifies that as a
  *general* rule for a two-module table — its modules do not share a first
  occasion, so per-module ordinals misalign by one. Those two helpers describe a
  single-module frame, where ordinal and calendar coincide. Resolved in favour of
  the calendar visit; §5 records the reason.
- `tests/test_ghanalss_visit_alignment.py` — **OK (anchored on §4)**: two tiers,
  matching `test_ghanalss_food_label_canonical`. Static tier (8 tests) passes and
  needs no microdata.
- `tests/test_ghanalss_nutrition.py` `DAYS = 30.0` — **OK**: not a change of
  behaviour. The constant was already 30; the comment called it "an
  approximation of ~7 visits over about a month". Six five-day recalls tile days
  1-30 exactly, so 30 is the recall length and that caveat was removed rather
  than softened.
- **Out of scope, observed not fixed**: 1998-99 casts `visit` to `str`
  (`food_acquired.py:118`) while the other waves leave it integer, so the
  country-level concat gives `visit` an object dtype with mixed types. Real,
  pre-existing, and independent of the offset.
