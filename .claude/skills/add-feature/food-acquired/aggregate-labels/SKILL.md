---
name: food-acquired-aggregate-labels
description: This skill should be used when calling `labels='Aggregate'` (or any `labels=X`) on food tables and needing to know the API contract — what happens when a country lacks the column, the `LabelUnavailableError` / Feature-degrade behavior, and which of the 16 food countries actually curate an Aggregate column. It covers the caller/contract side and cross-country comparability. For DESIGNING the buckets themselves (the CFE β-spread test, carve-outs, `Aggregate (short)`), see the parent food-acquired skill's "Designing the Aggregate Label" section — this sub-skill does not repeat it.
---

# The `labels='Aggregate'` contract

`labels='Aggregate'` collapses a country's fine `j` food items onto a coarser
grouping for demand estimation (Uganda: **175 fine items → 80 groups**; every
`Matoke (bunch)`/`(heap)`/`(cluster)` → `Matoke`). The *design* of those buckets
— when to add them, the CFE β-spread criterion, carve-outs, the `Aggregate
(short)` companion — is owned by the parent skill's **"Designing the `Aggregate
Label` and `Aggregate (short)` columns"** section. **Read that for the content
work.** This sub-skill covers the part the parent doesn't: the **API contract**
when a country *lacks* the column, and **cross-country comparability**.

## How `labels=X` resolves (one paragraph)

`Country._relabel_j` (`lsms_library/country.py`) finds the country's food-label
table (`food_items`, else `harmonize_food`, from `categorical_mapping.org`),
resolves the column — `labels='Aggregate'` matches **`'Aggregate Label'`** first,
then bare **`'Aggregate'`** (both spellings are in use) — builds a
`{Preferred Label → Aggregate}` map and renames the `j` level. `reaggregate`
(set by the caller, not the user) is `True` for the derived tables
(`food_expenditures`, `food_quantities`) → collapsed categories are **summed**;
`False` elsewhere incl. `food_acquired` → rename only, preserving per-`u`/`s`
rows. `Price` is never summed. `labels=None`/`'Preferred'` is a no-op.

## The contract when a country lacks the column

Only **4 of 16** food countries curate an Aggregate column. A country that
didn't **cannot honour the request** — and the behavior is deliberately
different at the two API levels (Contract B, "loud structured degrade";
settled 2026-06, PR #550):

| call | behaviour |
|------|-----------|
| `Country('Benin').food_acquired(labels='Aggregate')` | raises **`LabelUnavailableError`** (subclass of `KeyError`, so existing `except KeyError` and direct callers are unaffected), listing the available columns |
| `Feature('food_acquired')(labels='Aggregate')` | **drops** each country that can't honour it, emits **one** aggregated warning, and stamps `result.attrs['labels_unavailable'] = [...]` |

The cross-country call therefore **degrades loudly** — it does *not* silently
return only the curated countries (the pre-fix footgun: omissions looked like
build failures), and it does *not* mix granularities. **Always check the marker**
after a cross-country aggregate call:

```python
df = ll.Feature('food_prices')(labels='Aggregate')
dropped = df.attrs.get('labels_unavailable')   # None, or list of countries
```

**The raise-vs-degrade line in `_relabel_j` is load-bearing — keep it:**

- *missing curation* (no food-label table, or column absent) → `LabelUnavailableError` → degradable by Feature.
- *malformed table* (present but no `'Preferred Label'` key column) → **plain `KeyError`**: a genuine defect, must surface loudly.
- *misuse* (table has no `j` level to relabel) → **plain `KeyError`**.

Do **not** convert the malformed-table or no-`j` cases to
`LabelUnavailableError` to make a cross-country call "go green" — that would
degrade over a real bug.

## Coverage: who has the column

| group | countries | what they have |
|-------|-----------|----------------|
| ✅ have it | EthiopiaRHS, Malawi, Nigeria, Uganda | `Aggregate` / `Aggregate Label` |
| ✗ EHCVM (7) | Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau, Niger, Senegal, Togo | only `Original Label` |
| ✗ wave-coded (3) | Ethiopia, Mali, Tanzania | wave columns + codes, no Aggregate |
| ✗ no label table (2) | GhanaLSS, Nepal | none |

Regenerate this if unsure:

```python
import lsms_library as ll
for c in ll.Feature('food_acquired').countries:
    cm = ll.Country(c).categorical_mapping or {}
    t = cm.get('food_items') or cm.get('harmonize_food')
    cols = [] if t is None else [x for x in t.columns if x != 'Preferred Label']
    print(c, ('Aggregate' in cols) or ('Aggregate Label' in cols), cols[:4])
```

## Adding the column to a country (contract-side checklist)

Design the buckets per the **parent skill's section** (β-spread test, carve-outs
— don't shortcut it). This checklist is only the mechanical + contract-aware
wrapper around that:

1. Add an **`Aggregate Label`** column to the country's `#+NAME: harmonize_food`
   (or `food_items`) table in `categorical_mapping.org`, one coarse group per
   `Preferred Label` row. Real shape (Malawi `Malawi/_/categorical_mapping.org`):
   ```org
   | Preferred Label       | GD Category | Aggregate Label     | ... |
   |-----------------------+-------------+---------------------+-----|
   | Tomato Sauce (Bottle) | Spices      | Spices & Condiments | ... |
   | Spices                | Spices      | Spices & Condiments | ... |
   ```
   Use the parent skill's `orgtbl.py` tooling to insert the column from a YAML
   keyed on `Preferred Label` (idempotent on re-run) rather than hand-editing
   every row — see parent "Designing the `Aggregate Label`" → "Tooling".
2. **Align to the cross-country vocabulary** (see below) before finalizing names.
3. Rebuild & verify the contract is now satisfied (config edit → v0.8.0
   content-hash auto-invalidates just this country; or `LSMS_NO_CACHE=1`):
   ```python
   c = ll.Country('Benin')
   agg = c.food_acquired(labels='Aggregate')          # must NOT raise now
   assert agg.reset_index()['j'].nunique() < c.food_acquired().reset_index()['j'].nunique()
   c.food_quantities(labels='Aggregate', reaggregate=True)   # derived: sums
   ```
4. Confirm the cross-country assembly drops nothing once the targeted countries
   all have it:
   ```python
   df = ll.Feature('food_acquired')(['Uganda','Benin'], labels='Aggregate')
   assert df.attrs.get('labels_unavailable') in (None, [])
   ```

## The cross-country comparability trap

The parent skill's β-spread test guarantees a bucket is sound **within a
country**. It says nothing about whether `Benin`'s `Maize` group lines up with
`Uganda`'s — and the whole point of `Feature(...)(labels='Aggregate')` is a basis
that is comparable **across** countries for a pooled demand system. Define each
country's groups ad hoc and you get **16 mutually-incompatible schemes**: the
rows look aggregated but don't align, and the cross-country use case silently
breaks.

So, when adding the column to a batch of countries:

- Diff your proposed group names against the four that already have it
  (EthiopiaRHS/Malawi/Nigeria/Uganda) and **reuse their names** wherever the
  concept matches (`Maize`, `Rice`, `Beans`, `Cooking Oil`, …) rather than
  inventing a parallel label.
- Read `SkunkWorks/cross_country_label_harmonization.org` first — the shared
  food-group vocabulary is exactly the (still-unimplemented) design this trap
  motivates. Prefer extending a shared vocabulary over per-country invention.

## See also

- **Parent (bucket design): `add-feature/food-acquired/SKILL.md` → "Designing
  the `Aggregate Label` and `Aggregate (short)` columns".**
- Sibling: `add-feature/food-acquired/units/SKILL.md` (the `u` index).
- Contract code: `lsms_library/errors.py` (`LabelUnavailableError`),
  `Country._relabel_j` (`country.py`), the degrade loop in `feature.py`.
  Rationale: PR #550. `labels=`/`units=` semantics: `CLAUDE.md` → "Derived Tables".
