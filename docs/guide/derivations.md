# Derived Values

Some numbers the library serves are not survey answers. They are
**constructions** from survey answers, made by the library under an
assumption. When that is true, the row says so.

## Why

The project's precept is to respect the information in the original survey.
GhanaLSS 1987-88 and 1988-89 `food_acquired` used to fail it. On the
`s='produced'` side the served `Expenditure` was `Y12B.VFOODCPD`, the answer to
12B q5 — *"How much would it cost to buy the amount they ate each time?"*, the
value of **one eating occasion**. On the `s='purchased'` side it was
`Y12A.CFOODBLV`, *"How much have they spent since my visit?"*, a fortnight's
recall. Same table, same columns, same `visit = 1`, same `u = 'Value'`, and
nothing in the frame told them apart. A user pooling the two sides — or pooling
these waves with 2016-17's six five-day recalls — was comparing one meal with
thirty days and could not know it
(`SkunkWorks/derived_values.org`, "Context: one meal beside a fortnight").

Two obvious responses both fail the precept:

- **Serve the raw answer and document the window in prose.** The frame still
  says `Expenditure`; the prose is read by nobody at the moment of pooling.
  This was the state of the 1980s waves for years, and
  `GhanaLSS/_/CONTENTS.org` described it correctly the whole time.
- **Serve the constructed number silently.** Commensurable, and
  indistinguishable from a survey answer — what the food kg inference did
  before GH #850.

So a value the library computed must **say so on the row**, and the raw answers
behind it must be retrievable by their original names. The full statement of
the precept — four conditions, one of which is that there is exactly **one**
construction per cached parquet — is in
`SkunkWorks/derived_values.org`, "The precept, and the two ways of failing it",
and in the module docstring of `lsms_library/derivations.py`.

## What you see

### The `Derivation` column

A nullable string column (dtype `string`) on the served table. `NA` on a
reported row; on a derived row, the **registry key** of the derivation that
produced the row's value. Two derivations on one row are joined with `+` in
sorted order.

```python
import lsms_library as ll

fa = ll.Country('GhanaLSS').food_acquired()
fa['Derivation'].notna().sum()                    # 44112  (of 5,338,617 rows)
fa.loc[fa['Derivation'].notna(), 'Derivation'].unique()
# ['GhanaLSS::food_acquired::12b-fortnight']
```

The key sits on **exactly** the rows a registered function produced, and
nowhere else. In the EHCVM countries' `food_acquired` the purchased rows carry
a key; the `s='produced'` rows (own production, quantity `s07bq04`) and the
`s='inkind'` rows (gifts, quantity `s07bq05`) do not, because a reported
quantity is not a construction (commit `6e39518b6`, GH #876).

The column is declared `optional: true` in the canonical
`lsms_library/data_info.yml` (under `Columns: food_acquired`), so a country
with no registered derivation never emits it and `Feature()`'s concat fills
`NA`.

### `df.attrs['derivations']`

A per-frame summary: `{country: {key: {'rows': n, 'columns': [...], 'in':
table}}}`, counted from the frame's own `Derivation` column, so a frame sliced
to one wave reports only what it carries.

```python
fa.attrs['derivations']
# {'GhanaLSS': {'GhanaLSS::food_acquired::12b-fortnight':
#    {'rows': 44112, 'columns': ['Expenditure', 'Quantity'], 'in': 'food_acquired'}}}
```

A registered entry the frame does **not** carry is listed with `rows: 0` —
an honest zero for a slice that excludes the derived waves, and the only place
a *deletion* can be reported at all (a dropped row has nowhere to carry a key).
See `derivations.summary_for_frame`.

### Both survive `Feature()`, by different means

The **column** rides through concat, groupby, cache and parquet — that is why
it is a column and not `attrs` (`SkunkWorks/derived_values.org`, "The
`Derivation` column"). The **attrs summary** does not: pandas propagates
`attrs` only when every input agrees, so a cross-country concat always drops
them. `Feature.__call__` therefore re-attaches it explicitly, through
`feature._ATTRS_CARRIERS` — a registry of `(name, keys, attach)` triples
(population, recall, derivations) captured per country before the concat and
re-attached afterwards for the kept countries only (GH #873, 2026-09-12).

Measured on the pair the issue names,
`Feature('food_acquired')(['GhanaLSS', 'Guatemala'])` — 1,919,331 rows —
`attrs['derivations']` is `{GhanaLSS}` (the only country of the two with a
registry), where before #873 it was `None`
(`SkunkWorks/derived_values.org`, "DONE 2026-09-12 (GH #873)").

There is **no warning** at `Feature()` level, deliberately (decided
2026-09-11). At that level the `attrs` summary already says what is below and
where to look; a warning would repeat it as noise. `Country(...)` calls have
the column itself.

## How to look it up

```python
import lsms_library as ll

ll.derivations()                       # every registered derivation, corpus-wide
ll.Country('Togo').derivations()       # this country's entries
ll.Country('Togo').derivations('food_acquired')   # ... for one table
```

`ll.derivations()` (`derivations.derivations_table`) returns one row per key,
indexed by the key, with columns `country, table, name, waves, columns, rows,
bases, n_assumptions, validated_against, since, function, inputs`. As of
2026-09-12 it has **9 rows**: GhanaLSS's 12B entry plus one per EHCVM country
(Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau, Mali, Niger, Senegal, Togo).
It is assembled from the packaged YAML without importing any country module, so
it works from an installed wheel.

`Country(c).derivations()` returns `{key: DerivationRecord}`. A
`DerivationRecord` is an immutable `dict` subclass, like `PopulationRecord`:
`rec.rule` and `rec['rule']` both work, and an absent optional field reads as
`None` through the attribute rather than raising.

### The raw answers

`Country(c).derivation_inputs(key, wave=...)` runs the entry's `inputs`
callable and gives you back the **exact survey answers under their original
variable names**, re-read from the source through `get_dataframe`:

```python
c = ll.Country('GhanaLSS')
raw = c.derivation_inputs('GhanaLSS::food_acquired::12b-fortnight', wave='1987-88')
raw.shape          # (20808, 5)
raw.index.names    # ['t', 'i', 'j']
list(raw.columns)  # ['FOODCD', 'MFOODCLY', 'TFOODC', 'UTFOODC', 'VFOODCPD']
```

Three things to know about that frame:

- **It joins to the served rows.** That is a promise the *framework* keeps, not
  the inputs callable: `Country.derivation_inputs` re-keys `i` through
  `updated_ids` exactly as `_finalize_result` does for every served table.
  Without it, GhanaLSS 1988-89 serves `'101332'` / `'101332_1'` (a panel re-key
  plus a split-household suffix) where `Y12B.DAT` says `'200103'`, and "the
  exact inputs of this row" would not be findable from the row. On the EHCVM
  side, three waves' `mapping.py` sweep value-label mojibake out of every
  string index level, so the served `j` is `'Arachide grillee'` where an
  unswept extraction says `'Arachide grillÃ©e'` — skipping that step cost
  80,563 of CotedIvoire's 218,224 purchased keys (37%) before it was measured
  (commit `6d1bd05f4`).
- **It is at the INPUT grain, which is not the served grain.** Nothing requires
  one row per served row. In 12B several `FOODCD` harmonise to one `j`, so
  `(t, i, j)` is not unique and `FOODCD` is kept so the row can be read against
  the questionnaire; in EHCVM one source row becomes up to three served rows
  (one per `s`). The join is many-to-one — do not ask `merge` to
  `validate='1:1'`.
- **Nothing is cached.** It is slow and exact, every time.

Two other pointers on the same subject: `Country(c).notes()` surfaces the
country's `CONTENTS.org`, which each entry's `contents` field points into;
`Country(c).population` and `df.attrs['recall']` are the sibling records for
what a sample represents and what recall window a wave used.

## How to read a registry entry

The registry is `countries/<C>/_/derivations.yml`, a sibling of
`population.yml` and `recall.yml`. Framework-level entries (an empty country
slot) live in `lsms_library/derivations.yml`, which is currently empty. One
entry per key:

| field | meaning |
|---|---|
| `rule` | the construction, written out, with the constants |
| `function` | dotted `module:function` implementing it |
| `inputs` | dotted `module:function` returning the raw answers |
| `raw_variables` | the source variables read, in questionnaire order |
| `assumptions` | a list; each has `text`, a `basis`, and usually `evidence` |
| `validated_against` | what the construction was checked against, and how well |
| `contents` | a pointer into the country's `CONTENTS.org` |
| `since` | the date the entry landed |
| `waves` | a list, or `all` — the wave scope |
| `columns` | which served columns the derivation produces |
| `rows` | a selector saying which rows carry the key, e.g. `{s: produced}` |

The loader (`derivations.DerivationRecord.from_config`) **refuses** an entry
missing `rule`, `function`, `inputs`, `raw_variables` or `assumptions`, or an
assumption without a `basis` — the `PopulationRecord` discipline: a record that
cannot say what it did, to what, and why is not a record. The remaining fields
are optional.

`basis` is drawn from this module's own five-value vocabulary
(`derivations.BASES`): `questionnaire`, `variable-label`, `config-comment`,
`filename`, `modelling-choice`. The first four are the recall ladder's evidence
rungs. The fifth **names a choice no document supplies**; it is not evidence
and is deliberately not laundered into one. `recall.BASIS_LADDER` is not
reused, for exactly that reason.

### The key

`country::table::name` — three slots, `::`-separated. An **empty** slot means
"all": `::household_roster::age-dob` would be a framework-level rule applying
to every country. The `name` slot may not be empty, and a key may not contain
`+`, which is the separator between two keys on one row.

The **wave is not in the key**: every served row already carries `t`, so it
would repeat the index, and a rule spanning two rounds would need two identical
entries. Scope lives in the entry's `waves:`. Where a rule genuinely differs by
wave it is a different derivation with a different *name*, each scoped to its
waves — which is more informative than a wave number and lets `ll.derivations()`
filter by wave from the entries (decided 2026-09-11).

### One construction, no options

A derivation function takes **no options**, by design (decided 2026-09-11). The
cache is content-addressed, and a cached parquet must be one identifiable
construction rather than one of several. A user who wants the value under a
different assumption **re-derives it from the raw inputs** — which is what
`derivation_inputs` is for. The alternatives are described in each function's
docstring so you know what to compute.

## Worked example 1: GhanaLSS 12B

Key `GhanaLSS::food_acquired::12b-fortnight`, waves 1987-88 and 1988-89, rows
`{s: produced}`, columns `Expenditure` and `Quantity`, since 2026-09-11.
Function `ghanalss.derive_12b_fortnight_value`.

Section 12B never asks a "since my last visit" question. It asks, per food:
`MFOODCLY` (in how many of the last 12 months it was eaten), `TFOODC` (how many
times per unit), `UTFOODC` (the unit of that count) and `VFOODCPD` (the value of
one occasion). The served value is

```
Expenditure = VFOODCPD * TFOODC * (14 / days_per_unit[UTFOODC]) * MFOODCLY / 12
days_per_unit = {3: 1, 4: 7, 5: 30.4, 6: 91.3, 7: 182.6, 8: 365}
Quantity    = Expenditure            # u = 'Value'
```

(the 12B legend is DAY 3, WEEK 4, MONTH 5, QUARTER 6, HALF YEAR 7, YEAR 8).
That is the **year-average fortnight**: GSS's own annual construction
`EXPEND.HPFOOD` divided by 26. The worked example in the registry: eaten in 6
of 12 months, 12 times a month, 10 cedis each time → 120 a month in season, 720
a year (`HPFOOD`), 55.3 for an in-season fortnight, and **27.6** for a
fortnight drawn at random from the year, the 6/12 being the chance the
interview fortnight lands in season.

The `months / 12` factor is the entry's `modelling-choice` assumption. The form
does not record *which* months were cited, so the in-season figure is right for
some households and zero for the rest, unknowably; the year average is unbiased
over the sample. The in-season alternative (drop `months / 12`) and GSS's
annual figure (this × 26) are both computable from `derivation_inputs`.

`validated_against`: summing `function(...) × 26` per household reproduces
`EXPEND.HPFOOD` — GSS's own annual construction from the same section — with
**median ratio 1.018** in both rounds (n = 2,253 / 2,437 households).
Delivered: **44,112** served produced rows carry the key.

## Worked example 2: EHCVM section 7B

Key `<Country>::food_acquired::7day-purchase-at-last-purchase-unit-value`, one
entry per EHCVM country (eight of them), rows `{s: purchased}`, column
`Expenditure`, since 2026-09-12. One function for all eight,
`lsms_library.ehcvm.derive_ehcvm_purchase_value`; GH #876.

Section 7B is headed *"Consommation alimentaire des 7 derniers jours et achat
des 30 derniers jours"*. Before #876 the served `Expenditure` was `s07bq08`
straight — "Valeur du [PRODUIT] achete la derniere fois", the value of **one**
purchase, which `s07bq06` places up to 30 days back — sitting on the same row
as a **7-day** quantity. One row, two clocks, and nothing in the frame said so.
`s07bq05`, the gift quantity, was read nowhere at all.

The served value is now

```
Quantity    = (s07bq03a - s07bq04 - s07bq05).clip(lower=0)
Expenditure = Quantity * (s07bq08 / s07bq07a)
```

— the 7-day consumption net of own production *and* of gifts received, times
the unit value of the household's last purchase — so `Expenditure / Quantity`
is exactly that unit value and the two numbers on the row refer to the same
thing. The quantity is the survey's own accounting identity (construction "C",
@ligon 2026-09-12); the two alternatives measured against it were A, the gross
7-day consumption `s07bq03a`, and B, net of own production only.

`Expenditure` is **NA, never a guess**, in three arms: no last purchase
reported (`s07bq07a` or `s07bq08` missing or not positive); the last purchase
made in a different unit *code* from the consumption; the same code in a
different *size* (Petit / Moyen / Grand / Taille unique), which is a different
container and so a different price per unit. No EHCVM wave ships a
unit-conversion table — `lsms_library/categorical_mapping/ehcvm_units.org` is a
code→label codebook with no factors — so there is no exact conversion and NA is
the honest answer. The share runs from **9.7%** of served purchased rows (Benin
2018-19) to **25.6%** (Niger 2021-22), counted on the *served* denominator:
purchased rows of the delivered table, after the canonical reshape and the
`(t, v, i, j, u, s)` collapse, because that is what a user sees (the correction
in commit `6d1bd05f4`; each country's `derivations.yml` breaks its own count
into the three arms).

`validated_against` is `ehcvm_conso_*.dta`, the survey's own UEMOA aggregate.
On the rows where an `Achat` row exists, the units match and the candidate
constructions differ, this rule × 365/7 reproduces `depan(Achat)` within 1% for
**99.5%** of 434 Togo 2018 rows and **100%** of 35 Guinea-Bissau 2018-19 rows —
against 13.1% / 48.6% for construction B and 0.0% / 0.0% for A. **Those two
waves are the only ones where the check can run**: the other ten waves' `conso`
files key products in a different code space, and CotedIvoire ships none
(commit `6e39518b6`). The instrument is identical in all twelve — the same
`s07bq0*` variables with the same labels — which is the basis for applying the
rule there, and the other six countries' entries cite the Togo and
Guinea-Bissau numbers rather than claiming their own.

One more result from the same check, and it is why the gift rows carry no
value: `s07bq05 × unit_value` reproduces `depan(Don)` within 1% for 99.7% of
1,255 Togo rows. So UEMOA's gift value **is** the gift quantity priced at the
last purchase's unit value — an imputation by the people who built the
aggregate, not an answer anybody gave. The library checks it and does not serve
it: gifts are served as their own `s='inkind'` rows in the consumption unit,
with `Expenditure` NA and no `Derivation` key.

## What is NOT labelled

**Read-time derived tables carry the summary, not the column.**
`food_expenditures`, `food_prices`, `food_quantities` and
`household_characteristics` are computed at read time from `food_acquired` /
`household_roster`. A derived table did not derive those rows, so it points at
the table that did rather than relabelling them (decided 2026-09-11):

```python
fe = ll.Country('GhanaLSS').food_expenditures()
'Derivation' in fe.columns        # False
fe.attrs['derivations']
# {'GhanaLSS': {'GhanaLSS::food_acquired::12b-fortnight':
#    {'rows': 44112, 'columns': ['Expenditure', 'Quantity'], 'in': 'food_acquired'}}}
```

The `'in': 'food_acquired'` is the pointer: the row label stops at the table
where the derivation was made. Pinned by
`tests/test_ghanalss_12b.py` (the derived food tables must not be asked to
reduce the string column).

**A derivation already visible in the frame is exempt.** The column is a signal
only if it is rare. An index value that changes the vocabulary (`u = 'Value'`,
which says in the vocabulary that `Quantity` is a currency amount) or a per-row
source column of its own (`KgFactorSource` on the food kg factors) already
announces itself; the key is for the derivations that are otherwise invisible.
Two carriers must never both fire on one row
(`lsms_library/data_info.yml`, the `Derivation` note).

**Most of the corpus is not registered yet.** A read-only census of every wave
script, country module, `mapping.py` and `data_info.yml` (2026-09-11) found
about **410** sites where the library constructs a served value, of which the
sweep judged roughly a fifth to be disclosed nowhere a user looks and about
fifty to be "same column, different rule across waves" within one country. Nine
are registered today. The census — per-group reports with `path:line`, the rule
and how it is disclosed today — is in
`slurm_logs/2026-09-11_derived_values/`, summarised in
`SkunkWorks/derived_values.org`, "What the corpus already does". Its
12B-shaped cases (Nigeria W4's 30-day transaction under a 7-day key, Ethiopia's
residual `produced`, Niger's annual-average week in a last-7-days table,
Uganda's `intercropped`) were each filed as their own issue; several have since
been fixed. Until a site is registered, the only disclosure is the country's
`CONTENTS.org`, so **an unlabelled number is not thereby a reported one** — a
frame with no `Derivation` column says the country has no registered
derivation, not that it has none.

## For maintainers

Registering a derivation — the fit rule, the files, where to put the function
and the inputs callable so you do not invalidate a country's caches, and the
tests to mirror — is in `AGENTS.md` (`CLAUDE.md`), section "Derived Values
(2026-09-11)". The design, its alternatives and the census are in
`SkunkWorks/derived_values.org`; the field contract and the loader's refusals
are in the module docstring of `lsms_library/derivations.py`; the mechanism's
contract is pinned by `tests/test_derivations.py`.
