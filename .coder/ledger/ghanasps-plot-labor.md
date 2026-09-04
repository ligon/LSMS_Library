# Prior-Art Ledger — GhanaSPS `plot_labor` (GH #729, #140)

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md`; cites
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git floor (the gitnexus MCP server failed to
connect this session — CONNECT_TIMEOUT on `gitnexus`, `github`, `aristotle`).

## §1 Task, restated

Wire the `plot_labor` feature for GhanaSPS's three waves (2009-10, 2013-14,
2017-18) at the index `(t, i, plot_id, season, stage, source)`, with `v`
joined from `sample()` at API time. `stage` is kept as an index level (owner's
ruling, 2026-09-02), which is a divergence from the two existing `plot_labor`
countries (Uganda `(t, i, plot, source, season)`, Nigeria `(t, i, plot,
source)`) and is why `plot_labor` stays out of `Index Info > index_info` in
the canonical `lsms_library/data_info.yml`. The hired-labour payment is
reported as a RATE with its unit, not as the corpus's cash `Wage`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Nigeria` `plot_labor` block | `countries/Nigeria/_/data_scheme.yml:~150` | `(t, i, plot, source)`, `PersonDays` + `Wage`; `source` ∈ {family, hired, other}; country script, `materialize: make` | yes | reuse the *concept* (PersonDays = within-question row-sum; `Wage` NOT reusable — it is cash paid) |
| `Uganda` `plot_labor` block | `countries/Uganda/_/data_scheme.yml:335` | `(t, i, plot, source, season)`; season ∈ {A, B}; `PersonDays` + `Wage` | yes | reuse: season as a level, PersonDays semantics, the "no cross-source totals" rule |
| `harmonize_labor_source` | `countries/Nigeria/_/categorical_mapping.org:838` | the canonical registry for the `source` taxonomy (family / hired / other) | — | extend: GhanaSPS needs `self`, `communal`, `casual`, `permanent` |
| `ghanasps.plot_features` | `countries/GhanaSPS/_/ghanasps.py:427` | the plot hook; `_PLOT_HECTARES_PER_UNIT` | yes | cite only — no hook needed here |
| `2009-10/_/crop_production.py` | `countries/GhanaSPS/2009-10/_/crop_production.py` | the wave-script template: two-season melt, `season` assigned from the FILE, `format_id` on `hhno`/`plotno`, `to_parquet` | yes (`tests/test_ghanasps_crop_production.py`) | **reuse the `plot_id` derivation verbatim**: `plot_id = format_id(plotno)`, `i = format_id(hhno)` (W1) / `i = FPrimary` (W2/W3) |
| `currency._monetary_columns` | `lsms_library/currency.py:127` | unions `_DEFAULT_MONETARY` + canonical `Columns` + **the country's `data_scheme.yml`** | `tests/test_currency.py` | reuse: declaring `monetary: true` in the GhanaSPS block is the documented extension point (currency.py module docstring) |
| `currency._all_monetary_columns` | `lsms_library/currency.py:165` | union used only by `conversion.convert` — does **not** read country schemes | — | **gap, reported not fixed** (canonical `data_info.yml` is out of scope) |
| `_join_v_from_sample` skip set | `lsms_library/country.py:4588` | skips a table whose `index_info` index omits `v`, or that is in `skip_extra` | yes | cite: `plot_labor` is in *neither*, so `v` IS joined |
| `Wave.data_scheme` | `lsms_library/country.py:719` | a wave "has" a table if `_/{table}.py` exists **or** `data_info.yml` declares it | — | reuse: three wave scripts, no `data_info.yml` blocks |

## §3 Definitions & conventions in force

- **PersonDays**: "reported person-days of that source on the plot-season …
  the row-sum of the survey's own man/woman/child day cells of ONE labor
  question", `countries/Uganda/_/data_scheme.yml:366-372`. Cross-plot /
  cross-source totals are transformations and are never stored.
- **`Wage` is CASH PAID**, not a rate: `countries/Nigeria/_/data_scheme.yml`
  ("Σ man/woman/child of (reported daily wage × hired days)") and
  `countries/Uganda/_/data_scheme.yml` ("reported cash paid to HIRED labor").
  GhanaSPS reports a rate + a unit, which is a different quantity — hence
  `WageRate*` / `WageUnit` and **no `Wage` column** (owner's ruling).
- **Monetary columns**: the union of `_DEFAULT_MONETARY`, the canonical
  `Columns` block, and the country's own `data_scheme.yml`
  (`lsms_library/currency.py:20-30, 127-158`). `GhanaSPS: GHS` in
  `lsms_library/data_info.yml` `Currency:`.
- **Site B null-read guard**: a **required** declared column that is 100% null
  in *any* wave's `t` slice warns (fatal under `LSMS_READ_STRICT=1`);
  `optional: true` exempts it — `CLAUDE.md`, "The Silent All-Null Read".
- **Grain collapse**: duplicates on the declared index mean the identifier is
  broken; core aggregates nothing (`CLAUDE.md`, GH #323). `LSMS_GRAIN_STRICT=1`.
- **GhanaSPS keys**: `(hhno, plot_no)` in 2009-10, `(FPrimary, plotid)` later;
  `plot_id` is a panel id W1→W2 but **not** W2→W3
  (`GhanaSPS/_/CONTENTS.org`, "Plot features … a 1:1 key within each wave").
- **A cross-wave instrument artefact is documented, never folded away**: the
  local precedent is `Tenure`'s `sharecropped_in` (2009-10 only) and
  `crop_production`'s `Cocoyam Leaves` / `Crop (part)` labels (2009-10 only),
  both in `GhanaSPS/_/data_scheme.yml`.

## §4 Invariants & assumptions (the landmines for THIS task)

1. **W1's `stage` and `season` are assigned by the script.** Anchored on the
   A-number the producer stamped in each variable label (A290–A298 → land
   preparation, …) and on the questionnaire's own eight section headers.
   The script **asserts** the label A-numbers, so a wrong file→stage map is a
   build failure rather than a silent mislabel.
2. **W3's `stageid` is an ORDINAL, not a stage code.** `stageid == 1` spans
   every stage name (Clearing 2,285 / Planting 1,130 / … / Post-harvest 4).
   Key W3 on `stagename`. W2's `stagenum` *is* the stage (4,693 rows each).
3. **W2/W3 `*days` columns are DAYS PER WORKER**, not person-days
   ("Approximately how many days ON AVERAGE did EACH OF …", Part M M7/M8,
   M12/M13, M19/M20, M26/M27). PersonDays = workers × days per sex.
   `personaldays` is the respondent alone and is already person-days.
4. **W1 has NO labour-payment question.** Section 4 IX is A289–A362 and asks
   only days / hours / workers. `WageRate*` / `WageUnit` are therefore 100%
   null in the 2009-10 `t` slice → they MUST be `optional: true`. Same for
   `Hours`, which only 2009-10 asks.
5. **`_finalize_result`'s `dropna(how='all')`** removes a row all of whose
   columns are null — so a row must carry PersonDays to survive.
6. **Negative sentinels**: −10 (90 cells) and −1 (34 cells) in the W1 day /
   hour / worker cells; −1 in W2's three pay cells (24 / 591 / 832). The
   2009-10 CODE_BOOK documents neither. Treated as missing, counted.
7. `LSMS_READ_STRICT=1` cannot build any GhanaSPS household table on this
   branch — it aborts inside `sample` on `Rural`/`weight`/`panel_weight`
   (`GhanaSPS/_/CONTENTS.org`). Assert
   `null_read_reports(country='GhanaSPS', table='plot_labor') == []` directly.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| `plot_id` / `i` | **reuse** | identical to `plot_features` / `crop_production`: `format_id(hhno)`+`format_id(plot_no)` (W1), `FPrimary`+`plotid` (W2/W3) |
| `PersonDays` | **extend** | Uganda's within-question definition, but the multiplication differs per wave (W1 days×workers over 3 sex groups; W2/W3 workers×days-per-worker over 2, plus `personaldays` alone) |
| `Wage` | **new / renamed** | GhanaSPS reports a rate + unit; Uganda's and Nigeria's `Wage` is cash paid. Not the same quantity — do not reuse the name (owner's ruling) |
| `WageRateMen` / `WageRateWomen` / `WageRateChildren` | **new** | three separately reported cells; combining them needs weights that do not exist for a `Per acre` rate. Core does not aggregate |
| `WageUnit` | **new** | `hiredpayunit`; the area members reuse `plot_features`' `AreaUnit` spellings (Acres / Poles / Ropes / Plot) so a per-area rate can be converted with `plot_features().Area` |
| `Hours` | **new** | W1 only; the person-day-weighted mean hours per person-day, i.e. what multiplies `PersonDays` to give person-hours |
| `stage` vocabulary | **new** (`harmonize_stage`) | no corpus precedent — Uganda and Nigeria carry no stage level |
| `source` vocabulary | **extend** `harmonize_labor_source` | Nigeria's 3 labels + `self`, `communal` (W2/W3) and `casual`, `permanent` (W1) |
| the v-join | **reuse** | `_join_v_from_sample`; `plot_labor` is in neither exclusion list |

## §6 Open questions for the human

- `WageRate` was named as ONE column in the brief; delivered as three
  (`WageRateMen` / `WageRateWomen` / `WageRateChildren`) because the three
  rates are separately reported and no weighting exists for a per-acre rate.
  If a single headline column is wanted, it is a transformation, not a change
  to this table.
- W1's `casual` / `permanent` are kept as distinct `source` values rather
  than folded to `hired`. A consumer who wants the canonical `hired` sums
  them; the reverse is impossible.
- `_all_monetary_columns()` does not see country-declared monetary columns, so
  `lsms_library.conversion.convert` would not scale `WageRate*` on a bare
  frame. `attach_currency` (the API-time path) is unaffected. Fixing it means
  editing `lsms_library/currency.py` or the canonical `data_info.yml`, both
  out of scope.

---
### Phase 3 — verification (filled at task end)

- `2009-10/_/plot_labor.py` — OK (anchored on §4.1, §5): season/stage assigned
  from the file and **asserted** against the A-numbers in the `.dta` variable
  labels; `plot_id` derivation identical to `crop_production.py` (§5).
- `2013-14/_/plot_labor.py`, `2017-18/_/plot_labor.py` — OK (§4.2, §4.3):
  W3 keyed on `stagename` not `stageid`; PersonDays = workers × days-per-worker.
- `WageRateMen/Women/Children`, `WageUnit`, `Hours` — OK (§4.4, §5): declared
  `optional: true`, so the Site B guard stays silent on the waves that do not
  ask them; `monetary: true` on the three rates (§3).
- `harmonize_stage`, `harmonize_labor_source`, `WageUnit` tables — OK (§3):
  cross-wave instrument artefacts documented with their containment relations,
  never folded (the `Tenure` / `Cocoyam Leaves` precedent).
- No REINVENTION found: nothing in `transformations.py`, `country.py` or a
  sibling country computes GhanaSPS labour person-days.
- No CONTRADICTION found: `Wage` is deliberately absent (§3) rather than
  redefined, so Uganda's and Nigeria's definition stands untouched.
