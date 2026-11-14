
## Nigeria: Pepper Aggregate; shipped W2/W3 food and W3 crop conversion tables; post-harvest C8 alignment

### #1537 — Pepper → Fresh Pepper (closes)
Ruling 2026-11-12, "Check codes, then Fresh".
- Every served `Pepper` row is code 76: 10,446 `food_acquired` rows and 808 post-planting `community_prices` rows. No other code decodes to Pepper.
- One label-only `harmonize_food` row maps `Pepper` to Aggregate `Fresh Pepper`. Served `j` and every quarter map are unchanged.
- Nigeria now passes no label through under `labels='Aggregate'`.

### #1502 — the World Bank's own W2 PP, W2 PH and W3 food tables (closes)
Ruling 2026-11-12, "Wire W2 and W3".
- **What is served:** `Quantity_kg = Quantity × kg_per_unit`, joined on native codes: item, the source's own unit, and on W3 code 900 the source's own Other text, matched literally.
- **Exclusions and choices:** metric codes 1–4 are excluded. W3 uses the own-zone column, which W4's `*_cvn` reproduces.
- **Keys and safeguards:** one `Derivation` key per table, registered with `inputs`, stamped after the collapse. A no-partial-sum guard covers shared keys (dormant: 0 duplicate keys).
- **Result:** 139,621 rows on `survey_kg`; total kilograms +17.8% (W2 +37.5% / +15.0%, W3 +72% / +75%). W3's median price per kg goes 400 → 261 / 272, below W4's 372–391.

### #1518 — `ag_conv_w3` as `crop_production.Quantity_kg` (partial)
- Lookup on (crop, sized unit, zone); `KgFactor` is never written.
- The 9 unit-identity conflicts and the jerry cans are held.
- 8,436 lines (8.51M kg); 2016Q1 `harvest_kg` goes 365,707 → 8,873,416 kg.
- W1/W2 remain as a residual.

### #587 — post-harvest follow-ups
- W3 post-harvest blank-size Kg/l price per unit (Kg 1.000, l 1.14 against the household unit value), so they are served: +3,433 rows.
- A blank post-harvest unit is now `Unknown` (0 rows today).

### #1713 — not changed
- There is no per-round declaration to remove.
- W1/W2 post-planting do field education; W3–W5 post-planting do not.
- Verdict rows are drafted; question back.

### Cache
Nigeria only: 24/555 tables and 240/2321 waves. `build_transforms_fingerprint` 0/42. One Nigeria re-warm.

### Tests
- Main suite: 1978 passed, 4 skipped, 0 failed (base 1965 / 4).
- 15 related files: 540 passed / 12 skipped, the same as base.
- Final-head registry tests: 350 passed.
- New file: `tests/test_gh1502_nigeria_food_conversion.py`.
- Three re-pinned tests (`test_gh587_nigeria_pp_price_reports`, `test_gh637_nigeria_w1w2_variety`, `test_gh637_nigeria_w3_obs`), each with the measured value and the ruling cited.

### For maintainer review
1. W3 own-zone instead of national (the #1452 precedent was national).
2. W2 PP is served under the PH `g_cl` dictionary although its own documentation does not survive. The data principle says not to serve "someone else's undocumented imputations without a very good reason"; the reason here is that the PP and PH tables are identical on all 336 shared keys.
3. Volume rows are served as litres read as kg (W2 `g_cl=0`, W3 liquids). This differs from #1035.
4. Metric codes 1–4 are excluded from the tables (the #1452 / #1035 approach).
5. Code 900 is matched literally on the Other text; blank Other text is never a wildcard.
6. The 167 W3 table rows with notes are served as recorded (4,533 served rows), and the note is exposed in `inputs`.
7. The no-partial-sum guard on shared keys.
8. W2 cells far from both references are served as tabled: derica, and vegetables (Tomatoes / paint rubber 30 kg).
9. Spillover onto W1-PP and W4 no-cvn rows is not scoped (that needs a `transformations.py` edit).
10. `crop_production` gains a `Quantity_kg` column in every wave (NaN outside 2016Q1).
11. The 9 bundle/bunch crop rows stay held, although W4 suggests the table mislabels them.
12. `Quantity_kg` holds a shipped crop table, which contradicts the canonical `data_info.yml` note.
13. Serving criterion for the post-harvest Kg/l rows: a matched (v, j) household unit value, Kg and l separately, beside served controls.
14. Site Q gains one low-tail report (2016Q1 crop `Quantity_kg`, 4 rows).
15. The three re-pinned tests.
16. #1713: verdict rows drafted, not filed.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

