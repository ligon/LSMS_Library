## Summary

The EHCVM `interview_date` per-visit work on `feat/interview-date-visit-ehcvm`
(commit 717e32f4, #438) introduced a **function-name collision** that breaks the
**`household_roster` YAML build for every affected EHCVM country**. Depending on
whether a country happens to have a legacy make/script fallback and how many
waves it has, the symptom is either a hard failure or — worse — a **silent drop
of entire survey waves with no error**.

It is currently *masked* on warm caches: 29/34 `household_roster` L2 parquets
were built before the regression, so a normal run only surfaces the two
cold-cache countries (Benin, Guinea-Bissau). Any `cache clear`, CI
`--rebuild-caches`, or content-hash rebuild exposes the rest.

## Root cause

The branch added an `interview_date(df)` df-edit hook to each EHCVM country
module (e.g. `countries/Benin/_/benin.py`), intended as the **table-level**
hook for the `interview_date` *table* — it calls `tools.melt_visit_intervals(df)`
and expects a whole DataFrame.

But those same countries' `household_roster` configs declare a **myvar literally
named `interview_date`** (a legacy cover-page join, e.g. Benin
`household_roster.df_cover.myvars.interview_date: s00q23a`).

`country.py::map_formatting_function` (≈ lines 697–704) auto-wraps **any myvar
whose name matches a formatting function** into the `(column, function)`
"Tricky" form. For myvars `format_id_function=False`, so the guard at line 703
does not fire. `df_data_grabber`'s `grabber` then applies the hook **per cell**:

```
df['s00q23a'].apply(interview_date)   # interview_date -> melt_visit_intervals(scalar)
```

`melt_visit_intervals` does `name in df.columns` on what is now a scalar string →
`AttributeError: 'str' object has no attribute 'columns'`.

### Secondary bug — error masking

`df_data_grabber` wraps the myvar loop in a broad `except AttributeError`
(`local_tools.py` ≈ line 1039) whose fallback does `out[k] = df[k]` with the
*renamed* key. That re-raises a misleading **`KeyError: 'interview_date'`**,
completely hiding the real `AttributeError` and its origin. This made the
failure very hard to diagnose (the top-level error is just "no wave-level build
succeeded").

## Blast radius (forced rebuild = cache cleared)

| Country | wave build (YAML) | cached rows (served now) | rebuilt rows | outcome |
|---|---|---|---|---|
| **Benin** | FAIL (only wave) | — (cold) | **RuntimeError** | country unusable |
| **Guinea-Bissau** | FAIL (only wave) | — (cold) | **RuntimeError** | country unusable |
| **Togo** | FAIL (only wave) | 27,480 (stale) | **RuntimeError** | country unusable (masked) |
| **Niger** | 2018-19, 2021-22 FAIL | 121,791 | 47,631 | **silently drops both EHCVM waves** |
| **Burkina_Faso** | 2018-19, 2021-22 FAIL | 146,517 | 78,543 | **silently drops both EHCVM waves** |
| **CotedIvoire** | 2018-19 FAIL | 111,788 | 50,672 | **silently drops 2018-19** |
| Senegal | 2018-19, 2021-22 FAIL | 129,650 | 129,650 | recovered via make/script fallback |
| Mali | 2018-19, 2021-22 OK | 188,668 | 188,668 | unaffected |

The **silent-drop** rows (Niger / Burkina_Faso / CotedIvoire) are the most
dangerous: `Country(c).household_roster()` returns successfully but omits entire
EHCVM survey rounds. Any consumer (incl. derived `household_characteristics`,
the v-join, panel analyses) silently loses those waves once the cache is rebuilt.

## Reproduce

```python
import os
os.environ['LSMS_NO_CACHE'] = '1'   # force rebuild from source
import lsms_library as ll

ll.Country('Benin').household_roster()   # RuntimeError: could not materialize

# silent wave drop:
r = ll.Country('Niger').household_roster()
sorted(set(r.index.get_level_values('t')))   # ['2011-12', '2014-15'] -- 2018-19/2021-22 GONE
```

Full per-wave traceback (Benin):

```
File ".../countries/Benin/_/benin.py", line 180, in interview_date
    return tools.melt_visit_intervals(df)
File ".../local_tools.py", line 2116, in _pick
    return next((n for n in names if n in df.columns), None)
AttributeError: 'str' object has no attribute 'columns'
  -> masked as KeyError: 'interview_date' at local_tools.py:1044 (out[k] = df[k])
```

## Suggested fixes (not yet applied)

1. **Targeted (config):** drop the `interview_date` myvar from the affected
   `household_roster` configs (Benin, Guinea-Bissau, Togo, Niger, Burkina_Faso,
   CotedIvoire, Senegal). Per CLAUDE.md, `interview_date` does not belong in
   `household_roster` and the cover-page `df_cover`/`df_temp` join is legacy —
   removing it eliminates the collision and the dead column.
2. **Framework (prevent recurrence):** don't let a *table-level* df-edit hook be
   auto-applied as a *per-myvar* formatting function purely by name. Options:
   keep table hooks in a separate namespace from column formatters, or have
   `map_formatting_function` skip the wrap when the myvar value is a plain
   string and the matched function is a registered df-edit hook.
3. **Diagnosability:** narrow `df_data_grabber`'s `except AttributeError` (it
   currently swallows a genuine error inside a user formatting function and
   re-raises a misleading `KeyError`). At minimum, chain/log the original
   exception.

## Notes
- Branch: `feat/interview-date-visit-ehcvm`; regression introduced by 717e32f4 (#438). `master` has no `interview_date()` in these country modules and builds these rosters fine.
- Related: #474, #475 (separate `interview_date` source-wiring bugs).
- Evidence scripts: `slurm_logs/2026-06-16_share_female/{evidence_table,roster_regression_probe}.py`.
