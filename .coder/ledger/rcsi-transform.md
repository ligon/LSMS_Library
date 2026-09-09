# Prior-Art Ledger — GH #863 (rcsi transform, Phase 1 of WORKPLAN.org)

> Per-task ledger. Inherits `STANDING.md` §0 — cites it rather than re-copying.

**Search tier used:** ripgrep + git (floor). GitNexus MCP did not connect this
session (CONNECTION_CLOSED); no `.gitnexus/` index in this worktree. Per
CLAUDE.md's substitutes table, used `rg`/`grep` across
`lsms_library/transformations.py`, `lsms_library/countries/*/_/data_scheme.yml`,
`lsms_library/countries/{Malawi,Ethiopia,Nigeria,Tanzania}/_/{country}.py`, and
`Malawi/_/CONTENTS.org` in place of `gitnexus_query`/`gitnexus_impact`. The two
EPAR `.do` files GH #863 cites were read directly (`sed -n` on
`reference/epar/LSMS-Agricultural-Indicators-Code/Malawi IHS/Malawi IHS Wave
1/EPAR_UW_Malawi_IHS_W1.do` lines 4515-4540, 5807-5811, and `Tanzania NPS/
Tanzania NPS Wave 5/EPAR_UW_Tanzania_NPS_W5.do` lines 2605-2622) rather than
trusted from the issue body — every line number below and in the docstrings
is re-verified against that read, and the `hh_h02a..e -> strategy1..5 ->
LessPreferred/LimitPortion/ReduceMeals/RestrictAdults/BorrowFood` rename
order the weight mapping depends on is confirmed there, not assumed. Blast
radius for the new symbols (`rcsi`, `rcsi_phase`, `_RCSI_WFP_WEIGHTS`): none —
both are new top-level names in `transformations.py`, called by nothing else in
the corpus (`rg -n '\brcsi\b' --type py lsms_library/` before this change
returned zero hits, matching GH #863's own evidence). No existing call site to
break.

## §1 Task, restated

GH #863 asks for an `rcsi(food_coping, *, weights=None)` MECHANICAL reduction
in `transformations.py` (analyst-callable, house style of `tlu` /
`livestock_engaged` / `dependency_ratio`), plus a separate `rcsi_phase`
helper with partitioning closed intervals (avoiding EPAR's own `rcsi == 19`
phase gap). `food_coping` is an item feature at grain `(t, i, Strategy)` with
an integer `Days` column (0-7), declared by six countries
(Malawi/Ethiopia/Tanzania/Nigeria/Mali/Burkina_Faso). The design question is
the Strategy vocabulary per country — the default WFP five-term formula must
not be silently applied to a wider battery.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `tlu` | `lsms_library/transformations.py:2626` | house pattern for a species-keyed weighted sum with a `ValueError` on missing required column, silent-skip on absent species | via corpus sanity | pattern reused for `rcsi`'s shape |
| `dependency_ratio` | `lsms_library/transformations.py:2714` | house pattern for a `(t,i)`-grain ratio reduction, `ValueError` on missing index levels, drop-not-impute on the undefined case | via corpus sanity | pattern reused; `rcsi` inserted immediately after it per the task brief |
| `Malawi/_/malawi.py:food_coping_for_wave` | `:1168-1227` | builds Malawi's five-strategy `food_coping`; `dropna(subset=['Days'])` at `:1212` after coercing out-of-range values to NaN at `:1204` | audited cold, GH #637 (`Malawi/_/CONTENTS.org:6-66`) | reuse as evidence, not reimplemented |
| `Ethiopia/_/ethiopia.py:food_coping_for_wave` | `:797-836`, `FOOD_COPING_STRATEGIES` at `:785-794` | 8-item battery; explicit docstring "Rows with a missing Days value are dropped (the strategy was not answered)" at `:814-815` | none new needed | cited as evidence for the missing-row rule |
| `Nigeria/_/nigeria.py:food_coping_for_wave` | `FOOD_COPING_ITEMS` above `:955`, function `:967-1004` | 9-item battery; docstring "a household with all-missing items contributes no rows" at `:985-987` | — | cited as evidence |
| `Tanzania/_/data_scheme.yml:87-102` | comment | 8-item battery, 5 canonical + `LimitVariety`/`NoFood`/`WholeDayWithout` | — | cited for the unrecognised-label design |

**Reuse-search result:** no existing `rcsi`/`rCSI`/"coping strategies index"
symbol anywhere in `lsms_library/` before this change (`rg -c -i 'rcsi'
lsms_library/*.py` = 0). Nothing to reuse; building new is correct.

## §3 Definitions & conventions in force

- Canonical `food_coping` grain `(t, i, Strategy)`, column `Days` (int, 0-7) —
  per every country's `data_scheme.yml` `food_coping:` block (grepped across
  all six: Ethiopia:257-273, Burkina_Faso:135-151, Malawi:156-168,
  Nigeria:301-311, Tanzania:87-102, Mali:97-109). **Not** in
  `lsms_library/data_info.yml` (no top-level `food_coping:` schema block
  there beyond its `Feature Vocabulary` list entry at `:716` — the per-country
  `data_scheme.yml` comments are the only documented Strategy vocabulary,
  confirmed by grep of `data_info.yml` for `food_coping|Strategy`).
- Standard WFP rCSI weights (LessPreferred=1, BorrowFood=2, LimitPortion=1,
  RestrictAdults=3, ReduceMeals=1) match EPAR's Malawi coefficients exactly
  once EPAR's `strategy1..5` renaming is unwound against the Malawi
  `data_scheme.yml:162-164` label order — see GH #863 body and the
  `_RCSI_WFP_WEIGHTS` comment in `transformations.py`.
- MECHANICAL reduction / analyst-callable convention: `CLAUDE.md` doesn't use
  this phrase directly, but every sibling function in this section of
  `transformations.py` (`tlu`, `livestock_engaged`, `dependency_ratio`,
  `farm_size`) is a plain function with no `@build_transform` tag — see §4.

## §4 Invariants & assumptions

- Per STANDING.md §4: pandas-3.0 targets (no `inplace=`), `get_dataframe`/
  `to_parquet` for IO (not touched by this task — `rcsi` takes an
  already-loaded DataFrame, no new IO).
- **Task-specific, confirmed by evidence (not assumed):** an absent
  `(t, i, Strategy)` row in `food_coping` means "not answered / invalid,
  dropped", never "used 0 days" (which is a *stored* `Days=0` row). See the
  three citations in §2 and in the `rcsi` docstring. Consequence: `rcsi`
  computes `wide.dropna(how='any')` over the weighted-strategy columns
  BEFORE the weighted sum — a household missing any weighted strategy gets
  no score at all, not a truncated one.
- **Strategy vocabulary is per-country, not universal.** Malawi/
  Burkina_Faso/Mali field exactly the standard five; Ethiopia/Tanzania field
  8; Nigeria fields 9. Default `weights=None` covers only the standard five;
  any other label present raises `ValueError` naming it (task requirement,
  echoing GH #863 "What it must NOT do").
- **No cache-hash movement.** `rcsi`/`rcsi_phase` carry no `@build_transform`
  tag (confirmed: `grep -n '@build_transform' lsms_library/transformations.py`
  matches nothing in this file at all — the decorator lives only in
  `build_transforms.py`, `local_tools.py`, `country.py`). They are outside
  `_build_registry._EXCLUDED_CALLABLES`'s concern entirely because they are
  never on any build path — analyst-callable, like `tlu`/`dependency_ratio`/
  `farm_size` beside them. **Empirically confirmed**, not just argued from the
  decorator's absence: `build_transforms_fingerprint(t)` for
  `food_coping`/`crop_production`/`food_acquired`/`household_roster`/
  `plot_features`/`livestock` is byte-identical before (`git stash`) and after
  this change — the walk from every `@build_transform` root never reaches
  `rcsi`/`rcsi_phase` because nothing calls them.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| weighted sum over `food_coping` strategies | new (`rcsi`) | no prior symbol; house pattern from `tlu`/`dependency_ratio` reused for shape |
| phase categorisation | new (`rcsi_phase`), kept SEPARATE from `rcsi` | task requirement; also lets an analyst apply custom cutoffs without recomputing the score |
| missing-row handling | new design decision, evidenced (§4) | no existing corpus convention for this specific case; decided from the three citations, not by analogy |

## §6 Open questions for the human

- None blocking. One judgment call recorded rather than escalated: the
  `rcsi_phase` label strings (`'Phase 1'`..`'Phase 4'`) are generic rather
  than EPAR's own IPC-styled names (its `.do` file's `label var` text reads
  "IPC Phase 1, minimal food insecurity", etc.,
  `EPAR_UW_Malawi_IHS_W1.do:4533-4536`) — chosen to avoid asserting an
  IPC/CH food-security-phase-classification claim the library does not
  otherwise make. The ONLY source in hand for the cutoff values themselves
  (3, 18, 42) is EPAR's own code (`:4529-4532`, verified by direct read of
  the `.do` file, not copied from the GH issue body) — not an independently
  verified "WFP standard"; the docstring and this ledger say so rather than
  asserting a threshold provenance we have not checked.

---
### Phase 3 — verification

- `rcsi` (`lsms_library/transformations.py`, after `dependency_ratio`) —
  OK (anchored on §2, §4): new symbol, no existing rcsi machinery to
  contradict or reinvent; missing-row rule anchored on the three citations
  in §4, not assumed.
- `rcsi_phase` (`lsms_library/transformations.py`) — OK (anchored on §4 /
  GH #863 body): partitioning intervals verified by test
  (`tests/test_rcsi.py`) over every integer 0..60, including the `rcsi==19`
  boundary EPAR's own cutoffs mis-handle.
- `_RCSI_WFP_WEIGHTS` — OK (anchored on §3): weights cross-checked against
  EPAR's Malawi `.do` coefficients under the country's own label order, not
  copied positionally.
