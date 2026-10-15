# Prior-Art Ledger — ghanalss-produced-qp

**Search tier used:** ripgrep + git floor (gitnexus MCP did not connect this
session — see `CLAUDE.md` "Code intelligence: GitNexus is OPTIONAL"; substitution
declared, not silently skipped).

## §1 Task, restated

GhanaLSS `food_acquired` serves `s='produced'` rows for 1998-99, 2005-06,
2012-13 and 2016-17 that carry a real `Quantity` in a native unit `u` and a
reported farmgate `Price`, but `Expenditure` is NaN — so
`Country('GhanaLSS').food_expenditures()` returns `<NA>` for own production in
every wave except 1987-88 / 1988-89, where the registered `12b-fortnight`
derivation mints a value. Register a second derivation that serves
`Expenditure = Quantity * Price` on exactly those produced rows where both
factors exist, under its own key, with the `Derivation` column stamped in the
wave scripts.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `derive_12b_fortnight_value` | `GhanaLSS/_/ghanalss.py:532` | mints produced `Expenditure` for GLSS1/2 from §12B | `tests/test_ghanalss_12b.py` | **model to copy** (structure, docstring, stamp site) |
| `inputs_12b` | `GhanaLSS/_/ghanalss.py:593` | re-reads `Y12B.DAT` at INPUT grain | yes | model for the `inputs` callable |
| `derive_ehcvm_purchase_value` | `lsms_library/ehcvm.py` | the corpus's other *valuation* derivation | yes | precedent; not reusable (EHCVM recall shape) |
| `y['_qp'] = Quantity * Price` | `GhanaLSS/2005-06/_/food_acquired.py:168` | already computes Q×P **internally** to resolve duplicate farmgate prices | — | proves the product is trusted locally |
| `_ADDITIVE_MEASURE_COLUMNS` | `country.py` | core SUMs `Quantity`/`Expenditure` and **re-derives `Price`** when both present | `test_gh323_explicit_reducers.py` | **invariant to verify — see §4** |

Searched and found nothing: no generic `Quantity * Price` helper exists in
`lsms_library/*.py`. Registered `derive_*` functions are the five listed by
`grep 'function: .*derive_'`; none is a farmgate valuation.

## §3 Definitions & conventions in force

- Derived value contract (key grammar, `REQUIRED_FIELDS`, `BASES`, the fit rule,
  where the stamp goes): `CLAUDE.md` §"Derived Values"; field contract in
  `lsms_library/derivations.py` module docstring.
- `BASES` = `questionnaire | variable-label | config-comment | filename |
  modelling-choice`, `lsms_library/derivations.py:119`.
- The wave is **not** in the key; scope goes in the entry's `waves:`
  (`CLAUDE.md` §"Derived Values", "Registering one").
- GhanaLSS Trap 9 (duplicate index ⇒ core sums and re-derives `Price`):
  `GhanaLSS/_/CONTENTS.org`, quoted at `2005-06/_/food_acquired.py` tail.

## §4 Invariants & assumptions

- **Stamp AFTER the uniqueness assert, before `to_parquet`** — `CLAUDE.md`
  §"Derived Values" (3); pattern at `1987-88/_/food_acquired.py:140`.
- **Stamp only rows actually computed.** A row with `Price` and no `Quantity`
  (8.5% of 2016-17, 72% of 1991-92) keeps `Expenditure` NaN and carries **no**
  key. Coverage claims are honest only if the uncovered rows are unlabelled.
- **Trap 9 / GH #871**: once `Expenditure` and `Quantity` are both present, the
  additive branch re-derives `Price = Expenditure/Quantity`. On a unique index
  that is algebraically `(Q·P)/Q = P`, a no-op — **verify empirically**, do not
  assume (§6).
- `food_prices(units='kgvalue')` = `Expenditure / Quantity_kg` will now return
  values on produced rows where it returned NaN. Intended; must be stated.
- 1991-92 is **excluded**: only 80,282 of 286,922 produced rows (28%) carry both
  factors, and its dominant unit is `All` (93,349 rows), which reads as "the
  whole harvest" rather than a countable quantity. Serving a 28%-covered wave
  would look complete and would not be.

## §5 Reuse decision

| quantity | decision | why |
|---|---|---|
| the product itself | **new**, in `ghanalss.py` | no generic Q×P exists; farmgate semantics are GhanaLSS-specific. `tests/test_derivations.py`'s drift test fails on an unregistered `derive_*`, so function and registry entry land together. |
| the stamp | reuse `1987-88` pattern | one documented site per wave script |
| the registry | reuse `derivations.yml` | sibling of `data_scheme.yml`; a `derivation:` key inside the scheme would be read as a required column |

## §6 Verification against the ledger

- [ ] served `Price` on produced rows identical before/after (Trap 9 no-op)
- [ ] key on exactly the computed rows; none on purchased; none on GLSS1/2
- [ ] `Expenditure == Quantity * Price` wherever stamped
- [ ] `inputs` callable joins to the served rows
- [ ] prose that said "Expenditure left NaN" updated in all four scripts + `CONTENTS.org`

## §7 Measured

Own-production share of food value, `produced / (produced + purchased)`:

| wave | share | basis |
|---|---|---|
| 1987-88 | 38.8% | derived, §12B |
| 1988-89 | 33.4% | derived, §12B |
| 1998-99 | 30.5% | Q×P (99.9% cov) |
| 2005-06 | 17.1% | Q×P (100%) |
| 2012-13 | 27.6% | Q×P (100%) |
| 2016-17 | 28.7% | Q×P (91.5%) |

Recorded, not editorialised: a farmgate (producer) valuation *should* sit below
the §12B consumer-price valuation, so these are **not** claimed to be
"continuous" with the 1980s figures, and 2005-06 at 17.1% is not explained here.
