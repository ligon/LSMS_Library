# Reconcile-draft apply check, 2026-11-12

**Reviewer:** Claude, model Opus 5.5 (`claude-opus-5-5`). Read-only and independent: I did not write the drafts. No GitHub write and no repository edit was made. No sub-agents. Git use was read-only throughout: besides the listed commands, `rev-parse`, `cat-file`, `ls-tree`, `ls-remote` and `branch --contains` were used, all read-only.

**Base:** development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`, re-confirmed as the remote head at 16:13Z.

**Inputs:** `reconcile-20261112/REPORT.md` §§4, 5.1, 8 and `TRIAGE.tsv`, checked against:
- current issue bodies and comments (`gh issue view`);
- `isa-decisions.org` at `docs/isa-sweep-20261103`;
- the code, configuration and ledgers on `e5fdd2fa8`;
- archived refs in the shared mirror.

**Drift check:** none of the 56 has changed since the drafts (2026-11-12 ~15:14Z). Re-polled at 16:13Z:
- The only issues updated today are #587 and #1457, the expected coordinator edits.
- No issue has been opened since 2026-11-11.
- The only PR activity today: #1688 and #1689 closed as superseded; #1604 still open.

## Counts (56 drafts: 3 closes, 5 splits, 3 new issues, 45 narrows)

| verdict | n |
|---|---|
| APPLY | 34 |
| APPLY-WITH-EDIT | 21 |
| HOLD | 0 |
| DROP | 1 |

`VERDICTS.tsv` has one row per draft. Every APPLY-WITH-EDIT row names a file in `corrected/` holding the full title and body.

## DROP

- **#1148 (narrow, the coordinator §8.0 supplement): drop it.** The supplement says to prepend a header and "strike item 1". In the current (2026-11-10) body, item 1 is "Adjudicate Burkina Faso 2014 passage-result evidence", which is live work. The "integrate #851" item is item 1 only in the collapsed historical section. That top section already records PR #1671 (`1c1ef12da4`) as done. The worker's "no write needed" is right.
  - If a header is wanted anyway, prepend only: "Resolved so far: PR #1671 (`1c1ef12da4`) integrated the approved #851 option-A period/exposure API; the archived `fix/851-recall-period` (`cf117d101`) is no longer a dependency." Strike nothing.

## Edits that change substance (the rest of the APPLY-WITH-EDIT rows are wording or pointer fixes)

**Out of date against today's rulings:**
- **#552 residual:** does not carry the EHCVM-2 GO ruling.
- **#1442:** says #1509's eligibility definition "is with the maintainer"; it was ruled today.
- **#1044:** calls #1068 open; it was ruled today, drop and count.
- **#1556 and #791:** point the 2021-22 wiring at "#552 / #1598" and call it blocked. It is ruled GO and gets its own issue.
- **#1565:** Benin and Guinea-Bissau rosters are now ruled to be wired, so the leaver decision gates that step.

**Wrong facts:**
- **#552 residual:** the "shock-code table" warning is really about the crop-state (`condition`) code table (`Benin/_/CONTENTS.org:758`, `Togo/_/CONTENTS.org:815`).
- **#552 parent:** says #597 "carries" the repository blind spot; #597 is closed and resolved it.
- **#1565:** says Senegal's 2021-22 roster is "not on development". It is served, and its hook drops these rows as CAPI ghost rows (`Senegal/_/CONTENTS.org:694-706`).
- **#1136:** item 3's strata country list does not match `.coder/sampling/fingerprint.csv`. Pakistan has strata there, and Burkina, Mali, GhanaSPS, Liberia and Panama are missing from the list.
- **#279:** credits "#682/#685 (`d3b3c18b3`)". The commit came in through PR #697 (`3db1d8f1c`), and #682 and #685 are unrelated items.
- **#1483 residual:** Burkina also maps the DAP catch-all to `DAP` (six countries, not five).
- **#1483 parent:** the adapter counts are in ledger §3, not §4.
- **#975:** omits 1993's household questionnaire `kyrhh (1).pdf` and misnames the 1997 files.
- **#939:** hard-codes the cutoff as 9, which was #937's J; it must be recomputed for #940.
- **New issue, `crop_diversity` docstring:** says a docstring-only `transformations.py` edit moves GhanaLSS. `_build_registry._norm` strips docstrings, so it moves nothing.
- **#845:** the claim that the diagnostic "costs no cache hash" is unsupported.

**Dropped requirements, restored:**
- **#140:** `panel_ids` (P2; `FPrimary_original`) and the unique subjective-wellbeing data.
- **#1276:** "reconcile and preserve the burkina-2021 worktree and investigation ledger".
- **#1438:** three #798 caveats (ambiguity/timezone warnings; a refresh need not wait for every defect; historical counts are not a census).
- **#562 residual:** the design-doc pointer, whose only other home is the closed parent.
- **#1107:** the #867 provenance.

**Unruled constraints added by a draft, removed:**
- **#468:** turned option 2 (a v-less wave) into a prohibition.
- **#1107:** made "follow Togo → DAP" the default. It now defers to the #1483-split ruling, with the original "leave unmatched" in the meantime.
- **#1103:** stated an unverified hypothesis as fact in its title.

**New issue split from #1571 (condition-keyed price pool):** cites `Niger/_/CONTENTS.org:3262-3265`, where this is already a held policy question. Its parent comment must move #1571's "donor/recipient state" item, or two issues will hold it.

## Closes

- **#778: close as drafted, no split.** The carried `assume_cache_fresh` note is CLAUDE.md limitation (c). #1071 never held it. Closing is also consistent with the 2026-11-09 #1066 ruling, which fixed replay in that mode while adding "no freshness check, source probe, rebuild or cache write". That scopes the replay fix; it is not a ruling against ever refusing a hashless parquet. The close comment keeps the note verbatim.
- **#798:** apply together with the #1438 narrow, using `corrected/1438.md`.
- **#913:** apply together with the #1130 narrow, which is APPLY as drafted.
  - Follow-up: #828's body says "coordinate with #913". After the close, a one-line comment on #828 ("#913 is now #1130") avoids a dead link.

## Apply order (`#NEW` substitutions)

1. **#1483:** file the residual (`corrected/1483-residual.md`) as N1. Then narrow #1483 (`corrected/1483.md`) and #1107 (`corrected/1107.md`), both citing N1.
2. **#552:** file the wiring residual (`corrected/552-residual.md`) as N2. Then:
   - narrow #552 (`corrected/552.md`) and post its parent comment;
   - apply #1556, #791 and #1565, all citing N2.
   - Tell the agent wiring EHCVM-2 its number.
3. **#1571:** file the price-pool issue (`corrected/new-condition-price-pool.md`) as N3. Then post its #1571 parent comment, which is in the same file.
4. **#562:** file the residual (`corrected/562-residual.md`) as N4. Then post the parent comment and close #562.
5. **#845:** file the Panama residual (as drafted) as N5. Then narrow #845 (`corrected/845.md`).
6. **#1460:** file the Nigeria residual (as drafted; route it via the #587 owner) as N6. Then post the parent comment.
7. **Closes:** #798 together with #1438, #913 together with #1130, and #778.
8. **The remaining narrows,** in any order.
9. **Two new issues:** Malawi `Value_sold` (as drafted) and the `crop_diversity` docstring (`corrected/new-crop-diversity-docstring.md`).

## Global apply notes

- **Keep the replaced text.** When a narrow replaces a body, keep the previous body below it in `<details><summary>Previous issue text</summary>`. That is this repository's convention (#778, #913). It also guards against any loss this check missed.
  - #1287's current body is already 68,381 characters. If GitHub refuses an edit for length, move the history block into a comment.
- **Strip the formatting from fenced drafts.** The #1281 and #1288 drafts sit in code fences with `> ` prefixes; paste them without either.
- **Use the full #1288 draft.** Its §8 section contains a `## Current residuals` line, so splitting `REPORT.md` on `## ` truncates it. Use `REPORT.md:1523-1553` whole.
- **Headers for corrective narrows (§8.0).** #816 and #1235 should open with "Resolved so far: no fix merged; body corrected 2026-11-12 (Sue)." #939 and #1107 already carry it in their corrected files.
- **Minor notes on APPLY rows** are in `VERDICTS.tsv`, for example:
  - #1235: 11,257 in the body against 11,259 in `CONTENTS.org`;
  - #1497: name the standing panel-ID hold;
  - #833: the Malawi 5x groundnut reassessment is not carried.

## Drafts superseded by today's rulings, outside the 56

These were not in scope as close, split or narrow, but they should not be put to the maintainer as written.

- **#1509 ask:** answered ("All purchase rows"). An agent is implementing it. The archived `cc962ee15` (`framework-1509-purchases-20261105`) and #1442's `d81358091` (`burkina-bundle-20261104`) both need fresh review.
- **#1068 and #1129 C items:** answered ("Drop and count"). The CLAUDE.md "Kwacha per Kwacha" sentence changes in the same PR.
- **#1502 ask and #1518's C sub-questions:** answered ("Wire W2 and W3", including `ag_conv_w3`).
- **§7 "Ask first" list:** its first three entries (#1509, #1068+#1129, #1502+#1518) are answered.
- **§3 item 1** ("restoring them is the cheapest route") and the "blocked" verdicts on #1598, #1566 and #1557: the EHCVM-2 2021-22 wiring is GO and in progress.
- **#1604:** the hold is lifted (merge after rebase, red team and data gate). Text calling it held, such as #1556's, is out of date.
- **#587:** ruled "develop the item/unit code mapping, then provide `community_prices` on those labels". It is still excluded-in-flight. The #1460 residual does not conflict: it concerns `plot_features` areas, not `community_prices`.
- **#1457 R2b ("Keep on plot row"):** §5.2's three stale `CONTENTS.org` passages (Malawi `:5660-5661`, `:5681-5682`; Ethiopia `:4613-4614`) still need the docs edit the report proposes.

## Side findings (not drafts)

- **`AGENTS.md:795`** (written 2026-11-05 in `8deebd94d`) says a failed no-Makefile wave script leaves its wave in the coverage manifest. PR #1599 (2026-11-07) made the manifest record completed waves only (`country.py:6638-6643`). The sentence may now be stale; check it before relying on it.
- **Verdict-only line references with no effect on pasted text:** #560's verdict cites `Cambodia/_/CONTENTS.org:71` for the `food_items.org` retirement, which is actually at `:348`.
