I implemented both rulings, and all cold checks, the hash census and the tests pass. The new head is `6a124b46af94cbdf41bcd8faf4fbaa4713222ea4` on `feature/gnb-labels-2021-20261113`.

**Decision for you:** all 3,945 zeroed rows are now served as purchased, which follows the Togo template exactly. But 685 of them come from households that say they never bought the item (`s07bq06` = `Nunca`), and Togo's 11 rows contained no such case. The release rounds own production and gifts to 0 as well, so Togo's test ("neither own production nor a gift reported") proves less here than it did in Togo. I could serve those 685 as `s='other'` instead, but no ruling covers that, so I did not.

## Changes since a51a616e2

**Commits**
- `b943fa81357e12f42b78858a8c27ee43cd1ab617`: the code change for both rulings.
- `6a124b46af94cbdf41bcd8faf4fbaa4713222ea4`: `CONTENTS.org` only.

**1. Zeroed rows: "Serve NA, as Togo"**
- Each consumed `s07b` row the release rounds to 0 now gets one purchased row with Quantity NA and Expenditure NA. It carries the new key `Guinea-Bissau::food_acquired::s07bq03a-rounded-to-zero-withheld` (`rows: {s: purchased}`). It does not carry the as-released key, because nothing on it is served as released; the 7-day key rides along, as in Togo.
- This replaces the deletion entry `s07b-consumed-quantity-rounded-to-zero` (`rows: 0`), which is removed together with its inputs function.
- Togo's test (a purchase is implied when neither own production nor a gift is reported) passes on all 3,945: `s07bq04` is 0 on 3,717 rows and NA on 228, and `s07bq05` is 0 on all.
- The key's inputs now carry `s07bq06`. The split is: `Últimos 7 dias` 1,897, `Ontem` 206, `Últimos 30 dias` 636, `Há mais de 30 dias` 521, `Nunca` 685. All 2,739 recorded purchase values are in the first three groups.
- I carried over Togo's collision rule. It fires on no row, because no household has both sugar codes in the same unit and size.

**Rows added** (cold, against `a51a616e2`):

| | purchased | produced | in kind |
|---|---|---|---|
| rows added | **+3,945** | 0 | 0 |
| 2021-22 total | 99,039 (was 95,094) | 20,263 | 14,027 |

- Every other `food_acquired` row is frame-equal to before, and the index is unique.
- `food_quantities`, `food_prices` and `food_expenditures` are frame-equal to before in both waves, so the spillover does not move again.
- Purchased Expenditure is NA on 18,392 of 99,039 rows (was 14,447 of 95,094). Its sum, 60,522,116.51 FCFA, is unchanged.
- `attrs['kg_factor_sources']` counts 3,945 more rows (`metric` +1,881, `unit` +1,847, `item_unit` +216, `item_unit_tight` +1), because the partition counts every input row. Their quantity is NA, so no kilogram or price moves.

**2. Leavers: "Exclude from HH size"**
- MonthsSpent is now NA on every row with `Departed` True.
- **I registered it.** The 12 rows whose recorded `s01q12` answer (all `Sim`) is withheld carry `Guinea-Bissau::household_roster::departed-monthsspent-withheld` in a new `Derivation` column. The key's inputs return the recorded answers and join those 12 rows on (t, i, pid). The registry entry is the only way to recover the withheld answer, which is why the `Departed` flag alone would not do.
- `household_characteristics` 2021-22, measured before and after:

| | Sex × age counts | total incl. `log HSize` |
|---|---|---|
| before (`a51a616e2`) | 40,331 | 50,250.68 |
| after | **40,322** | 50,240.54 |

  The 9 leavers have left the counts. The result is frame-equal to `c35c95eaf`'s, which was built when leavers were dropped.
- No other roster value moves, and the 2018-19 slice is frame-equal to before.
- **Shared-file edit:** `Derivation` is now declared optional for `household_roster` in the canonical `lsms_library/data_info.yml`, the same way `sample` declares it. The census shows it moves nothing outside Guinea-Bissau. I could not declare it in this country's `data_scheme.yml`: `test_nothing_in_a_data_scheme_names_a_derivation` refuses any key there whose name starts with "derivation".

**Checks on `b943fa813`**

| check | result |
|---|---|
| cold comparison against `a51a616e2` and `c35c95eaf` | as above (job 41279578) |
| hash census against base `f138a3d61` | 22 of 555 tables and 44 of 2,321 waves move, all Guinea-Bissau; btf 0 of 42, fwimp 0 of 38 (job 41279579) |
| 36 targeted files | 3,327 passed, 87 skipped (none Guinea-Bissau), 4 xfailed, 0 failed (job 41279580) |
| `tests/ -m "not requires_s3 and not slow"` | 15,521 passed, 35 skipped, 4 xfailed, 0 failed (job 41279581) |

**Tests changed**
- **`test_gnb_2021_wave.py`:**
  - each 2021 row carries exactly one of the two keys;
  - the 3,945 withheld rows are all purchased, NA, and join their inputs;
  - the deletion key is gone;
  - the leaver key is registered, and its function behaves as described;
  - MonthsSpent is NA on every leaver, the key is on exactly 12 rows, and the inputs join them.
- **`test_ehcvm_7day.py`:** `OTHER_KEYS` gains the withheld key.

**Text updated:** `derivations.yml` (rule texts, plus the two new entries), the hook and inputs docstrings, the `data_info.yml` comments (132,176 consumed rows: 128,229 positive, 3,945 zero, 2 blank), and `CONTENTS.org`.

**Drafted residual (tier B), "Scope Guinea-Bissau 2021-22's rounded-key rows out of the kilogram evidence":**
- **Problem:** `food_kg_factors` pools every priced purchase row across waves (#1487, #1509), so the rounded 2021-22 rows set 2018-19's factors. 2018-19 is +4.0% kg and its `food_prices` sum is −10.4%. The red team found that 2,638 rows move from the `unit` rung to an item rung estimated from 2021-22 alone, that 7,618 item-rung rows take a median across the two waves, and that four smoked-catfish cells make up 61% of the price fall.
- **Why `no_kg` does not help:** it refuses to serve a factor, but leaves the row in the evidence.
- **Fix:** an evidence filter, for example rows carrying a declared Derivation key, in `transformations.py`. That edit re-warms GhanaLSS, Uganda and Ethiopia, so bundle it with their next re-warm.
- **Acceptance:** 2018-19 returns to 226,907 kg and 64.91M FCFA.
- **Open question:** should the 2021-22 rows then still be served factors estimated from 2018-19 alone?

**Disclosures**
- A fourth accidental no-op on n0035: `python3 - 2>/dev/null` with empty stdin, left in the shell command that grepped the canonical file. It imported nothing.
- New private frames: `private/gnb/frames-41279578`.
- Probe and measurement jobs:
  - 41279256: zero-row probe, aggregates only;
  - 41278042: leaver diagnosis (41277971 failed on a sort error and touched no data).
- The census and tests ran at `b943fa813`; the head adds only `CONTENTS.org`.

---

