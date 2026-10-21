# Prior-Art Ledger - #1118 Ethiopia plot-input units

**Search tier used:** ripgrep + git floor; no index refresh.
**Baseline:** `f8d236694c26fe9e709c5548f02c70f74ea72c33`.

## §1 Task, restated

Normalize missing or blank `u` in Ethiopia's registered `plot_inputs` build to
the existing `U_UNKNOWN` sentinel, including genuine use-only observations.
The #1041 ruling quoted in #1118 applies even when Quantity is absent.
Retain every reported Applied observation and every other value, key,
multiplicity and attribute. This is Ethiopia's part of #1118; other countries,
sales (#820), cover keys (#660), factors and unit curation are outside scope.

## §2 Existing machinery

| symbol | path:line | purpose | reuse |
|--------|-----------|---------|-------|
| `plot_inputs_for_wave` | `lsms_library/countries/Ethiopia/_/ethiopia.py:1872` | builds five ESS waves from plot, crop and purchase sections | extend final unit normalization only |
| `U_UNKNOWN` | `lsms_library/transformations.py:3744` | existing `Unknown` missing-unit sentinel | reuse |
| `_yesno_bool`, `_plot_id_from` | `lsms_library/countries/Ethiopia/_/ethiopia.py:1858` | reported use and holder/parcel/field identity | preserve |
| `plot_inputs.py` | `lsms_library/countries/Ethiopia/_/plot_inputs.py:1` | five-wave concatenation and `id_walk` | preserve |
| `get_dataframe`, `to_parquet`, content-hash gates | `STANDING.md §2/§4` | sanctioned IO and invalidation | reuse |
| organic/NPS regression | `tests/test_867_nitrogen_content.py:275` | NPS conversion and refusal of quantityless organic rows | retain and run targeted selection |

## §3 Definitions & conventions in force

- Inherit `STANDING.md §3/§4`, `CLAUDE.md` and
  `lsms_library/data_info.yml` for schema, IO, cache and identifier contracts.
- `Ethiopia/_/CONTENTS.org:1315`: "one row per input APPLIED to a plot
  (or plot-crop)".
- `Ethiopia/_/CONTENTS.org:1334`: holder/crop purchase fields attach "ONLY
  where the (holder, crop) maps to exactly ONE plot" in waves 2-5.
- `Ethiopia/_/CONTENTS.org:1357`: Applied is "the sole datum for organic /
  pesticide rows, which carry NO quantity in ESS". These rows are real input
  observations; the Nigeria non-input-question exclusion does not apply.
- `transformations.py:3744` defines `U_UNKNOWN = 'Unknown'`; the sentinel
  does not imply a conversion factor or a zero amount.

## §4 Invariants & assumptions

- Held-source diagnosis at baseline: 180,900 public rows; 37,370 null units,
  no blank-string units. All missing-unit rows have missing Quantity and
  Applied=True. Input counts: Manure 27,261; Compost 4,295; Other Organic
  2,651; Pesticide 3,163. Wave counts: 9,141 / 8,881 / 8,879 / 5,567 / 4,902.
- All five direct helper missing-unit counts match distinct eligible source-use
  keys from private held section3/section4 data. Evidence is private at
  `/local/job40255479/control/isa-backlog-20261021/Ethiopia/1118/`.
- Do not filter on amount availability: reported seed or fertilizer fields can
  retain a `Kg` row even when Quantity is absent. Keep nonblank units verbatim.
- Preserve source predicates, the unique-plot purchase join, every index,
  all non-unit columns, dtypes, row multiplicities and `id_converted` attrs.
- Build changes must invalidate through existing hashes; private cold and
  fresh-process warm builds must agree. Do not repair or promote shared caches.
- Use exact checkout import/config assertions, physically private held L1,
  offline sockets, numerical threads=1 and bounded loadfile workers.

## §5 Reuse decision

| behavior | decision | reason |
|----------|----------|--------|
| absent unit | reuse `U_UNKNOWN` | #1041 already settles missing/blank units |
| final helper string column | extend | one build boundary covers all five waves |
| reported use-only rows | preserve | source and CONTENTS agree they are observations |
| joins, identifiers, quantities, factors, Feature assembly | reuse unchanged | no defect in this task boundary |

## §6 Open questions for the human

None. The standing #1041 ruling settles Ethiopia's missing-unit semantics.
Implementation waits for the dispatcher's explicit boundary confirmation.

### Phase 3 - verification

Pending implementation and private cold/warm, source-use, comparator and
targeted-test checks. This ledger is committed before implementation.
