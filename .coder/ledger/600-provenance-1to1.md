# Prior-Art Ledger — GH #600 (catalog_id is not a 1:1 key)

**Search tier used:** ripgrep + git floor, plus the **WB NADA datafile API as an
instrument** (`/api/catalog/{id}/data_files?id_format=id`, which returns the
file list of a catalog entry).  gitnexus not consulted (the change is confined
to two modules with no call graph to speak of).

## §1 Task, restated

`lsms_library/provenance.py` (PR #595) records **one** `#+CATALOG_ID:` per wave
directory, and `data_access.discover_waves()` decides "do we hold this WB
catalog entry?" by looking that id up.  The relation between WB catalog entries
and our wave dirs is **many-to-many**, so the scalar key produces *confident
false claims* (`local_status='no'` on studies we demonstrably hold).  Fix the
model, not the symptom, and never let a **partial** record read as a complete
one.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `WaveProvenance` | `lsms_library/provenance.py:128` | one wave dir's provenance record | `tests/test_wave_provenance.py` | **extend** (`catalog_ids`, `covers`) |
| `parse_source_org` / `render_source_org` | `provenance.py:185` / `241` | SOURCE.org ⟷ record; idempotent; keeps the bare URL first | yes | **extend** (accumulate repeated keys) |
| `local_catalog_ids` | `data_access.py:1328` | id → wave dirs holding it (already many-dirs-per-id) | yes | **extend** (many-ids-per-dir) |
| `discover_waves` | `data_access.py:1343` | the census + status ladder | yes | **extend** (ladder) |
| `_local_waves` | `data_access.py:1296` | wave *directories* on disk | yes | reuse |
| `Country.waves` / `wave_folder_map` | `country.py:1455`, `countries/Tanzania/_/tanzania.py:156` | *logical* waves; multi-round folder map | yes | reuse (read-only) |
| `scripts/backfill_wave_provenance.py:resolve` | `:255` | re-stamps every SOURCE.org from the catalog | manual script | **extend** (must not clobber the new keys) |
| `CountryCatalog.repositories` (PR #599, open) | `data_access.py` | which WB collections to search | `tests/test_repository_discovery.py` | do not touch — rebase under it |

**Do NOT reinvent:** the coverage-matrix tiering / `blocked_sources.csv` /
`absent_verdicts.csv` machinery (`.coder/coverage/`) — this task is about the
*catalog census*, a different question from *"does the feature build?"*.

## §3 Definitions & conventions in force

- **`SOURCE.org` keyword form** and the three `PROVENANCE_SOURCE` values
  (`worldbank` / `external` / `unknown`): `provenance.py:26-56`.  The bare URL
  must stay the file's **first** `http(s)://` (legacy `_read_source_url()`
  greps it): `provenance.py:241`.
- **`local_status`** tri-state (`yes` / `no` / `unknown`) and `local: bool`:
  `data_access.py:1361-1380`; `.claude/skills/add-wave/SKILL.md:37`.
- **Multi-round wave folders**: `.claude/skills/multi-round-waves.md`;
  `wave_folder_map` maps *logical* waves → one directory.
- **`sane` is not `blessed`**, and an **unevidenced negative is permanent**:
  `CLAUDE.md` §"Coverage Matrix" — the same discipline is applied here, so every
  new relation carries its evidence in the file that asserts it.

## §4 Invariants & assumptions (the landmines)

- `SOURCE.org` is **not** a cache-hash input (`Wave._input_hash` /
  `Country._table_cache_hash` cover `_/` build inputs + DVC sidecars only).  So
  nothing in this task can move a single number in any feature.  Verified by
  reading both hash functions, and by the byte-identical Feature check.
- `local` must stay a **`bool`**; `covered`/`derived` rows keep `local=False` —
  we do not hold their *files*.
- `render_source_org` must stay **idempotent** and backward-compatible with the
  123 shipped `SOURCE.org` files.
- Writing `Documentation/SOURCE.org` into a non-wave directory **promotes it
  into a wave** (`country.py:1455` scans for it).  Never create one to "fix"
  Tanzania's four logical waves.

## §5 Reuse decision

| quantity | decision | why |
|---|---|---|
| "which catalog entry does this dir hold" | **extend** `WaveProvenance.catalog_ids` | already the right home; only the arity was wrong |
| "which entries does a held release subsume" | **new** `#+CATALOG_COVERS:` | a property of the *directory's files*; no existing home |
| "which entries are derived from ours" / "same study, two ids" | **new** `lsms_library/catalog_relations.yml` | a property of the **catalog**, not of any one dir; recording it per-dir would over-claim (a dir cannot know whether the *other* constituents are held) |
| status ladder | **extend** in place | #599 does not touch these lines |

## §6 Evidence (every claim below was measured, not assumed)

**Instrument**: WB NADA `data_files` API → the file list of a catalog entry;
compare with the `*.dta.dvc` names under a wave dir.  **Validated in both
directions** before use — positive controls: 2936→`Malawi/2016-17/Cross_Sectional`
99/99, 1002→`Nigeria/2010-11` 94/99, 3814→`Tanzania/2008-15` 42/42.  Negative
controls: 2936→`Malawi/2016-17/Panel` **0/99**, 3818→`Malawi/2019-20/Panel`
**0/108** (a cross-section entry does *not* explain a Panel dir), and Uganda's
apparent 91-file residue is a pure filename-prefix artifact (89/91 match once
the catalog's `2009_` prefix is applied) — so a 0 is meaningful and a residue is
not automatically a second entry.

1. **Malawi holds TWO entries in one dir** (*the issue's Failure 1, but not
   where the issue looked*): `Malawi/2016-17/Data/Panel/` — 97 of 98 files are
   datafiles of **2939** (`MWI_2010-2016_IHPS`), 0 of 2936.
   `Malawi/2019-20/Data/Panel/` — 96 of 97 are datafiles of **3819**
   (`MWI_2010-2019_IHPS`), 0 of 3818.  We *hold* 2939 and 3819; discovery said
   `no`.  **Both the issue and the triage diagnosis got this wrong** — the
   diagnosis classified them as "cumulative re-releases derived from what we
   hold" (i.e. `covered`).  They are *held*.
2. **Tanzania `2008-15/` covers four entries it does not hold**: 0 of 76 / 1050
   / 2252 / 2862's files are present (42/42 are 3814's, the Uniform Panel
   Dataset).  Content-validated: `upd4_hh_a.dta` carries `round ∈ {1,2,3,4}`
   (6128 / 8163 / 9998 / 4961 households), and `countries/Tanzania/_/tanzania.py:45`
   maps exactly those rounds to `2008-09 / 2010-11 / 2012-13 / 2014-15`.  WB
   abstract of 3814: *"datasets generated by the four rounds of the NPS"*.
   → `covers`, **not** `catalog_id` — listing them as held would be the same
   species of false claim, pointed the other way.
3. **Nigeria 5835** (`NGA_2010-2019_NUPD`, *"Uniform Panel Data"*): 0 of its 65
   files are held; its four constituents (1002, 1952, 2734, 3557) all are.  WB
   abstract: *"datasets generated by the four rounds of the GHS"*.  → `derived`.
4. **South Africa 297 ≡ 902**: identical 71-file lists (71/71 overlap), both
   identical to `South Africa/1993/Data`.  `lsms` `ZAF_1993_IHS` and `datafirst`
   `ZAF_1993_PSLSD` are one survey.  → `same_study` alias.
5. **Malawi 3016** (`central`, idno `MWI_2010_IHS-III_v01_M_v01_A_ML`, 4 files,
   title *"…Subset for Machine Learning Comparative Assessment Project"*) is a
   derived **subset** of 1003, **not** an alias of it.  → `derived`, not
   `same_study`.  (Flattening ⊂ and ≡ into one "alias" map would repeat the
   original sin.)
6. **Left alone, deliberately**: Tanzania **3455** (`NPS-R4_v03_A_EXT`) — an
   alternative version of R4, whose master (2862) we do *not* hold; the
   `derived` rule requires *every* constituent to be held, so it stays `no` **by
   construction, not by hand-waving**.  Tanzania **2863** (Feed the Future
   Interim Survey) is a different survey and stays `no`.

## §7 What changed, and where

- `provenance.py`: repeated `#+KEY:` lines **accumulate** (they used to
  silently last-win — a data-loss trap on its own); `catalog_ids: list[str]`
  (with `catalog_id` kept as the primary, so every existing constructor and
  reader still works); `covers: list[str]`.
- `catalog_relations.yml` (new): `same_study` + `derived_from`, each entry
  carrying its evidence.  A `derived` verdict fires **only** when every
  constituent is held.
- `data_access.py`: 5-state ladder — `yes` (held, incl. via a `same_study`
  alias) → `covered` → `derived` → `unknown` → `no`.  `unknown` is extended
  from "dir with no id" to **"logical wave inside a multi-round folder that no
  record accounts for"**, so a future Tanzania-shaped folder cannot emit a
  false `no` even if nobody writes a `covers` line.
- `scripts/backfill_wave_provenance.py`: carries the new keys through a
  re-stamp (it would otherwise silently delete them).

## §8 Interaction with PR #599 (open)

Rebasing onto `origin/fix/597-widen-discovery` auto-merges `data_access.py` and
`.claude/skills/add-wave/SKILL.md`; the **only** conflict is `CLAUDE.md`, where
both PRs append a paragraph at the same anchor — resolution is *keep both*.
Done in a scratch worktree: **127 tests pass on the merged tree**
(`test_wave_provenance` + `test_repository_discovery` + `test_coverage_matrix` +
`test_capability`).

One real interaction, and it is worth stating: #599 pins South Africa to
`idno_pattern=r"_(IHS|GHS)_"`, which **excludes** 902 (`ZAF_1993_PSLSD`).  So on
#599 the duplicate never enters the census, and #599's own test asserts exactly
that.  That guard is *incidental* — it is about the **series**, not about
**identity** — and it evaporates the moment someone widens the pattern.  The
`same_study` alias is the guard that is actually about identity, so the test for
it deliberately admits the entry (a pattern-free spec) rather than relying on the
pin.  Both guards are fine; only one of them is *about* the thing it protects.
