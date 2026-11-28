# Prior-Art Ledger — Uganda 2005-06 omitted coping codes

**Search tier used:** ripgrep and Git history, held source labels, UBOS v03 dictionary.

## §1 Task, restated

Restore two observed GSEC16 responses in the Uganda 2005-06 script-path
`shocks` table. Leave the separate #1734 label-policy question unchanged.

## §2 Existing machinery

| Symbol | Path | Role | Decision |
|---|---|---|---|
| `d` | `Uganda/2005-06/_/shocks.py` | One label map for three ranked positions after the existing year filter | Extend two entries |
| `normalise_coping` | `lsms_library/coping.py` | Country read-time set semantics and fillers | Reuse |
| `_fingerprint` | `tests/test_uganda_invariance.py` | Raw parquet contents, axes and dtypes | Reuse |
| `windows_build_check.py` | `scripts/` | Canonical digest on served output, including the set | Reuse |

## §3 Definitions & conventions

Follow `STANDING.md` and Uganda `_/CONTENTS.org`, under shocks coping fillers:
the three ranks consist of the recorded responses, and the set is derived at
read time. UBOS release UGA_2005-2009_UNPS_v03_M GSEC16 documents code 4 as
withdrawing children from school for wage work and 15 as self-employment.
These are neither general wage employment nor interchangeable responses.

## §4 Invariants & assumptions

Keep every row, key, native rank, other label, schema, and later wave.
The held v03 labels must agree across all original rank variables. No changes
to the year filter, read-time set logic, or #1734 codes 7/8/13/14. Compare
the raw parquet fixture and served Windows check independently.

## §5 Reuse decision

Extend the existing wave map only. Do not add a registry or alter any normalizer.
Use the current tests and pin methods without changing how the digests are made.

## §6 Open questions for the human

None for the omitted answers. The independent #1734 choice stays open.

### Phase 3 — verification

- `d` — OK (§1–§5). Exact held GSEC16 replay restores 565 ranks in 564
  records. Only those ranks and 564 derived sets change (207 were empty),
  with 14,459 keys and all other values and attributes unchanged. Confirmed
  in cold and separate warm Country, first-wave Country, and Feature calls.
- `normalise_coping` — reused unchanged, including duplicate-set handling.
- Cache fixture (§4) — re-pinned only the two actual changed parquets. Both
  old files reproduce the previous full fixture entries; shape, dtypes,
  axes, and columns stay put.
- CI (§4) — old served Windows check pin reproduced cold under Linux; the
  same check script agreed on the new digest in two independent cold Linux
  roots. The assets pin is unchanged. The Windows integration job was not run.
