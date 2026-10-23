---
name: add-wave
description: Use this skill to add a new survey wave to an existing LSMS-ISA country. Covers discovery of new waves on the World Bank Microdata Library, downloading and pushing data to S3 via DVC, and registering the wave so the library recognizes it.
license: Apache-2.0
---

# Add Wave to an Existing Country

## When to Use

- A new survey round has been released on the World Bank Microdata Library and you want to add it to an existing country in the LSMS Library.
- You need to download raw `.dta` files from the WB, push them to the S3 DVC remote, and wire up the wave so `Country(name).waves` includes it.

Requires the `add-feature` skill for writing `data_info.yml` configs once the wave is registered.

## Prerequisites

- `MICRODATA_API_KEY` set (env var or `~/.config/lsms_library/config.yml`)
- S3 **writer** credentials at `config.s3_write_creds_path()` (`~/.config/lsms_library/s3_write_creds` on Linux, or wherever `$LSMS_S3_WRITE_CREDS` points); see [S3 Write Credentials](#s3-write-credentials). No `.dvc/config.local` edit is needed.
- The country already exists under `lsms_library/countries/`

## Workflow

### Step 1: Discover available waves

```python
from lsms_library.data_access import discover_waves

discover_waves("Ethiopia")
```

Returns a list of dicts annotated with:

| key            | meaning                                                                 |
|----------------|-------------------------------------------------------------------------|
| `local`        | `bool` — `True` only when a wave dir *records* this catalog id.          |
| `local_status` | `"yes"` / `"stale-release"` / `"covered"` / `"derived"` / `"unknown"` / `"no"` (see below). |
| `local_waves`  | the wave dirs that back, cover, or constitute this entry.                |
| `held_idnos`   | the release idnos those dirs record holding: a primary's `#+CATALOG_IDNO:`, a secondary's `#+CATALOG_HELD:` (GH #1283) — compare the row's `idno`, the catalog's latest. |

**Acquisition candidates are `no` (a new wave) and `stale-release` (a newer
release of a wave we hold).** Acquisition stages the complete archive before
placing data files (GH #1149). For an existing wave, each member's basename
matches a unique existing `.dvc` sidecar recursively under `Data/` (only under
`Data/<subdir>/` with `subdir=`, GH #1276), ignoring case, and the file lands
at that sidecar's path. Multiple matching sidecars or
colliding destinations raise before placement. New files, and every file of a
new wave without sidecars, retain their zip-relative paths. A `no` has to be
*earned* — it is a confident claim that we neither hold this study nor
anything containing it (GH #600):

| status | meaning | `local` |
|---|---|---|
| `yes` | a wave dir records this catalog id | `True` |
| `stale-release` | a wave dir records this catalog id, but holds an older release (`#+CATALOG_IDNO:` for the dir's primary entry, its `#+CATALOG_HELD:` line for a secondary, GH #1283) than the catalog's latest — Nigeria 2023-24 was the first: it held `v01_M` of 6410 when the catalog moved to `v02_M` on 2026-09-16. The backfill records the newer one as `#+CATALOG_LATEST_IDNO:`, so `rg CATALOG_LATEST_IDNO lsms_library/countries` is the queue (GH #1128) | `True` |
| `covered` | we don't hold its files, but a release we *do* hold subsumes its content — Tanzania's `2008-15/` holds the Uniform Panel Dataset (3814), whose `round` column carries NPS rounds 1–4 (76, 1050, 2252, 2862) | `False` |
| `derived` | built *out of* entries we hold, **all** of them — Nigeria 5835 is the four GHS-Panel waves, harmonized | `False` |
| `unknown` | we cannot say: a matching dir records no WB id, **or** the label is a logical wave inside a multi-round folder that no record accounts for | `False` |
| `no` | not held, not covered, not derived: a real gap | `False` |

**Matching is on the WB catalog id, not on the wave label.** Each wave dir
records the catalog entry it came from in `Documentation/SOURCE.org`
(`#+CATALOG_ID:` — see `lsms_library/provenance.py`). Label matching used to
be the mechanism and was wrong in both directions: two different surveys can
share a year range (Nigeria's GHS-Panel W4 **3557** and Living Standards
Survey **3827** both span 2018–2019), and one catalog entry can span two of
our wave dirs (Uganda **1001** covers both `2005-06/` and `2009-10/`).

**But the id is not a 1:1 key either.** `#+CATALOG_ID:` is a **list** — one dir
can hold several entries (`Malawi/2016-17/` is `2936, 3819`: IHS4 in
`Data/Cross_Sectional/`, and in `Data/Panel/` the 2016 IHPS round as
re-released in 3819, `v06_M`, which the record says `#+CATALOG_COVERS: 2939`;
GH #1277). Entries a
held release *subsumes* go in `#+CATALOG_COVERS:` — **covered is not held**, and
claiming otherwise is the same false claim pointed the other way. Relations
*between catalog entries* (a study re-catalogued in a second repository; a
derived re-release) live in `lsms_library/catalog_relations.yml`, each with its
evidence.

**When you add a wave, check whether its `Data/` holds more than one catalog
entry** — a `Panel/` + `Cross_Sectional/` split is the tell. The WB datafile API
settles it in one call: `/index.php/api/catalog/{id}/data_files?id_format=id`
lists an entry's files; compare against the `.dta` you actually downloaded.
To *acquire* a second entry into a wave we already hold, see Step 2b.

`#+CATALOG_IDNO:` / `#+CATALOG_DOI:` name the release a directory HOLDS; only
`add_wave` / `populate_and_push` move them, once the whole release has demonstrably landed, and they clear the
`#+CATALOG_LATEST_*` keys. With `push=True`, every file must also be
successfully pushed; `push=False` records a complete local download.
Revised members are reported against the held sidecars' checksums before any
placement. The comparison uses each sidecar's DVC hash convention: explicit
`hash: md5` means raw bytes; a legacy sidecar without `hash` uses DVC's
text-aware `md5-dos2unix`. A matching sidecar permits replacement of its raw
workspace copy, using atomic per-file replacement to preserve DVC hardlinks.
An existing byte-identical member is verified without rewriting it. A differing
raw file without a matching sidecar stays unverified/skipped and prevents the
new held-release stamp. The acquisition's actual verified members, including
nested and unchanged files, are the set sent to the batched push helper.

Sidecars/files absent from the new release are reported recursively by their
resolved paths and left in place; retirement remains a reviewed step. Staging
prevents an early member from being placed before a later comparison fails.
Placement is atomic per file, not a whole-release rollback: an IO failure can
leave a partial refresh, which never receives a complete new held-release
stamp. Existing SOURCE facts stay intact until the requested acquisition
succeeds, even when it requests a different primary entry. A new directory's
initial routing record names the catalog entry without claiming a held release.

To (re)stamp provenance across the tree:

```sh
python scripts/backfill_wave_provenance.py --dry-run   # report only
python scripts/backfill_wave_provenance.py             # write SOURCE.org
```

Countries that are not WB datasets (`EthiopiaRHS`, `KenyaLPS`) are marked
`discoverable=False` in `_COUNTRY_CATALOG` and return `[]` with an explanatory
log — deliberately, rather than being silently absent.

#### Which repositories get searched (GH #597)

`discover_waves()` searches the collections listed in the country's
`repositories` field, **default `("lsms",)`**. The World Bank publishes whole
series outside `lsms` — Armenia's Integrated Living Conditions Survey (18
waves) is in `central`, South Africa's General Household Survey (21 waves) is
in `datafirst` — and a lsms-only search cannot see them *at all*.

**Adding a new country requires a one-time broad sweep** to learn which
repositories hold its series (`_wb_catalog_search(code, collection=None)`
searches everything). Curate the answer into config; do not infer it at
runtime.

> **Widening a country to a second repository REQUIRES an `idno_pattern` that
> pins the survey series.** Dropping the collection filter inflates results
> 30–400× (Findex, DHS, Afrobarometer, enterprise surveys; `datafirst` alone
> returns 320 South African rows — censuses, election studies, school
> registers). Worse, it resurfaces studies we *already hold* under a different
> catalog id in another repository: `central` 3016 (`MWI_2010_IHS-III_..._A_ML`)
> is a derived subset of Malawi IHS3 (`lsms` 1003), and `datafirst` 902 (`ZAF_1993_PSLSD`)
> is the same 1993 survey as `lsms` 297. The catalog relations
> registry (GH #600) distinguishes that subset from the South African alias.
> The series pin still excludes unrelated studies before relation matching.
> A missing-wave list nobody trusts is worse than no list.

```python
"Armenia":      CountryCatalog("ARM", idno_pattern=r"_(HBS|ILCS)_",
                               repositories=("lsms", "central")),
"South Africa": CountryCatalog("ZAF", idno_pattern=r"_(IHS|GHS)_",
                               repositories=("lsms", "datafirst")),
"Liberia":      CountryCatalog("LBR", idno_pattern=r"_(HIES|NHFS)_",
                               repositories=("lsms", "central")),
```

`idno_pattern` answers **"is this catalog row this country's?"** (identity), NOT
**"is this survey in remit?"**.  The two diverge: Liberia's NHFS is in the
pattern because it is the catalog entry backing a wave dir we *hold*, but a
forest-resources survey is not in remit.  Do not conflate them.

Note `GET /api/collections` returns HTTP 400 — collection ids cannot be
enumerated via the API. Read them off the `repositoryid` field of search rows.

### Step 2: Add the wave

```python
from lsms_library.data_access import add_wave

add_wave("Ethiopia", catalog_id="6161")
```

This will:
1. Resolve the WB catalog IDNO
2. Show a confirmation prompt (disable with `confirm=False`)
3. Create `{Country}/{wave}/Data/`, `Documentation/`, and `_/` directories
4. Write an initial routing `Documentation/SOURCE.org` if none exists
5. Download the full Stata zip from the WB NADA API
6. Stage all data members, resolve destinations, compare checksums and report
   revised members/leftovers, then place the files
7. Run batched `dvc add` + `dvc push` on the actual release membership
   (if `push=True`, the default)
8. Record the held release only after complete acquisition and requested push,
   then show a rich progress summary

Pass `verbose=True` (default) for spinners and a summary table. Use `verbose=False` for scripted/non-interactive use.

### Step 2b: A second catalog entry in a wave we already hold (GH #1276)

Some waves hold two disjoint WB entries, the #600 pattern: `Malawi/2016-17/` is
IHS4 (2936) in `Data/Cross_Sectional/` plus the 2016 IHPS round of 3819
(`MWI_2010-2019_IHPS_v06_M`) in `Data/Panel/`, and 2019-20 is 3818 plus the
2019 round of the same 3819; each record carries
`#+CATALOG_SUBDIR: 3819 Panel` and `#+CATALOG_HELD: 3819 MWI_2010-2019_IHPS_v06_M …`
(GH #1277). Burkina Faso 2021-22 is to hold 6224 (EHCVM-P, the panel
component) in `Data/Panel/` and 6277 (EHCVM-2, the cross-section) in
`Data/Cross_Section/`.

**Do not run a plain `add_wave` for the second entry.** Acquisition matches
members to existing sidecars *by file name*, anywhere under `Data/`. Without
confinement, a second entry whose names coincide with the first's overwrites
the first's files (52 of 6277's 55 member names equal files 6224 holds) and
replaces the record's primary entry, and once both sets coexist in separate
directories a re-acquisition of either raises `Ambiguous sidecars`. Use
`subdir=`, which confines extraction, sidecar matching, the leftover report and
the push to `Data/<subdir>/`. `subdir` is ONE directory name directly inside
`Data/`; nested paths are refused, so two entries' subtrees can never contain
one another.

**`SOURCE.org` records which subdirectory holds which entry**, one line per
entry:

```
#+CATALOG_SUBDIR: 6224 Panel
#+CATALOG_SUBDIR: 6277 Cross_Section
```

Confinement alone does not stop you naming the *wrong* subdirectory:
`add_wave("…", "6277", subdir="Panel")` would confine itself onto 6224's files.
So an acquisition with `subdir=` is **refused before any download** when:

- the record maps the entry to another subdirectory, or the subdirectory to
  another entry (ignoring case);
- `Data/` holds a directory whose name differs from `subdir` only in case;
- `Data/<subdir>` is a symbolic link, or not a directory;
- `Data/` holds sidecars at its top level, outside any subdirectory;
- a subdirectory holding sidecars has no `#+CATALOG_SUBDIR:` line -- unless
  it is the target itself and the record lists exactly one entry, the one
  being acquired (its own files, re-acquired; the acquisition then writes the
  line).

Each refusal names the line to add. The cost is one line per entry, written
once.

1. **Move the held entry into its own subdirectory first, and record it.**
   This is a config edit, reviewed in its own PR. `git mv` each
   `Data/<file>.dvc` to `Data/<Sub>/<file>.dvc`, and move any raw workspace
   copy with it (`*.dta` is gitignored). A sidecar's `path:` is the bare file
   name, relative to the sidecar, so it stays valid. Blobs are
   content-addressed, so nothing is re-pushed, and no `dvc` command is needed.
   Add `#+CATALOG_SUBDIR: <its id> <Sub>` to `Documentation/SOURCE.org` in the
   same PR. Alternatively, a first `add_wave(country, "<its id>", wave=...,
   subdir="<Sub>", confirm=False)` writes the line itself; that re-downloads
   the release, and verifies the held files against it. Then update every
   reference to those files: `file:` in the wave's `data_info.yml`, which
   becomes `<Sub>/<file>`; `../Data/<file>` in wave scripts; and country
   scripts that reach into the wave (`../{wave}/Data/<file>`). Those are build
   inputs, so this moves the wave's cache hashes. Moving first matters:
   top-level sidecars refuse every `subdir=` acquisition. Left at the top,
   the first entry could only be re-acquired without `subdir=`, and that is
   ambiguous once both entries coexist.
2. **Acquire the second entry into its own subdirectory:**
   ```python
   add_wave("Burkina_Faso", "6277", wave="2021-22", subdir="Cross_Section",
            confirm=False)
   ```
   The other subdirectory's files and sidecars are left byte-identical.
3. **Provenance.** Once the whole release has landed and been pushed, the new
   id is appended to `#+CATALOG_ID:` (`6224, 6277`), and two lines are added:
   ```
   #+CATALOG_SUBDIR: 6277 Cross_Section
   #+CATALOG_HELD: 6277 BFA_2021_EHCVM-2_v01_M [<doi>]
   ```
   `#+CATALOG_IDNO:` / `#+CATALOG_DOI:` still describe the primary entry (GH
   #1283 ruling: a secondary's held release is its own line, the primary
   keeps its scalars). Re-acquiring a newer release moves the `HELD` line and
   clears the entry's `#+CATALOG_LATEST:` line, under the same complete-
   release and confirmed-push gates as the primary; a failed or partial
   acquisition writes nothing. The backfill writes and clears
   `#+CATALOG_LATEST: <id> <idno> <first-seen> [<doi>]` per secondary and
   never writes `HELD`. `discover_waves` judges each entry on its own held
   release, so a secondary can read `stale-release`. A
   `#+PROVENANCE_NOTE:` is still the place for prose and evidence, as
   `Malawi/2016-17/Documentation/SOURCE.org` keeps; it no longer has to
   carry the release.
4. **Re-acquisition**, of either entry, names its own subdirectory:
   `add_wave(country, "<id>", wave=..., subdir="<its subdir>", confirm=False)`.
   `populate_and_push(country, wave, subdir=...)` fetches the entry the record
   maps to that subdirectory, else the primary entry when the record lists
   only one. It is refused when the record lists several entries and maps
   none to the subdirectory.

Without `subdir=`, acquisition behaves as it did before, including the
ambiguity error.

### Step 3: Register the wave in `{country}.py`

**This is the most commonly missed step.** Many countries have a hardcoded `Waves` dictionary in `lsms_library/countries/{Country}/_/{country}.py` that overrides directory-based wave discovery. If the dict exists, `Country(name).waves` returns only its keys -- new wave directories are invisible.

Check:
```python
import lsms_library as ll
c = ll.Country('Ethiopia')
print(c.waves)  # Does 2021-22 appear?
```

If not, open `lsms_library/countries/{Country}/_/{country}.py` and add the wave to the `Waves` dict:

```python
# Before
Waves = {'2011-12': (),
         '2013-14': ('sect_cover_hh_w2.dta', 'household_id2', 'household_id'),
         '2015-16': ('sect_cover_hh_w3.dta', 'household_id2', 'household_id'),
         '2018-19': (),
         }

# After
Waves = {'2011-12': (),
         '2013-14': ('sect_cover_hh_w2.dta', 'household_id2', 'household_id'),
         '2015-16': ('sect_cover_hh_w3.dta', 'household_id2', 'household_id'),
         '2018-19': (),
         '2021-22': (),
         }
```

The tuple values are panel linkage info `(cover_file, old_id_col, new_id_col)`. Use `()` for waves that start a new panel or where linkage is unknown.

Not all countries have a `Waves` dict. If the file doesn't exist or lacks a `Waves`/`waves` variable, the `Country` class falls back to scanning for directories with `Documentation/SOURCE.org` -- no registration step needed.

For a **new country** (or a staged directory) whose `_/data_scheme.yml` does not exist yet, `ll.Country(name)` raises `UnregisteredCountryError` (GH #915); check its waves with `ll.Country(name, allow_unregistered=True).waves` until the data scheme lands, and never leave that opt-in in analysis code.

### Step 4: Write feature configs

Now load the `add-feature` skill to write `data_info.yml` for the new wave. The raw `.dta` files are on disk (and in S3); you need to map their variable names to the country's schema.

### Step 5: Verify

```python
import lsms_library as ll
c = ll.Country('Ethiopia')
assert '2021-22' in c.waves

df = c.household_roster()
waves = sorted(df.index.get_level_values('t').unique())
assert '2021-22' in waves
```

## S3 Write Credentials

The committed `lsms_library/countries/.dvc/config` pins the S3 remote to the
**reader** key:
```ini
['remote "ligonresearch_s3"']
    url = s3://dvcbucket0/LSMS
    credentialpath = s3_creds
```

To push you need a **writer** key, in the same boto INI format as `s3_creds`:
```ini
[default]
aws_access_key_id = ...
aws_secret_access_key = ...
```

**Where it goes.** At push time, `_resolve_write_credentialpath`
(`data_access.py`) takes the first of these that exists and is non-empty:

1. `config.s3_write_creds_path()`. This is `$LSMS_S3_WRITE_CREDS` when that is
   set, and otherwise `s3_write_creds` in the user config directory, which is
   `$LSMS_CONFIG_DIR` if set and otherwise platformdirs'
   `user_config_path('lsms_library')`: `~/.config/lsms_library/` on Linux
   (`$XDG_CONFIG_HOME` relocates it), `~/Library/Application
   Support/lsms_library/` on macOS. Ask the library rather than guess:
   `python -c "from lsms_library import config; print(config.s3_write_creds_path())"`.
   **This is the place to put it.** It keeps
   the secret out of the git checkout, and the library can then be installed
   read-only. The maintainer's writer key lives here.
2. *Legacy fallback:* `lsms_library/countries/.dvc/s3_write_creds`, the
   in-tree file used before the user-config layout. It is still honoured,
   so older editable checkouts keep working. Do not create new ones.
3. Env vars: `LSMS_S3_WRITE_KEY_ID` + `LSMS_S3_WRITE_KEY` (the secret), or
   `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`. They are written to a
   temporary INI file, which is deleted after the push.

Once a reader key is found, any of the three makes `permissions()` report
`write` for the S3 remote. Set **both** values of an env-var pair:
`permissions()` already reports `write` on `LSMS_S3_WRITE_KEY` alone, but the
push needs the key id as well. `~/.aws/credentials` is never treated as a
writer key, because nothing in it says whether a key can write.

**No `config.local` editing.** An explicit `credentialpath` beats the boto
env-var chain, so a plain `dvc push` would authenticate as the reader and
fail with `AccessDenied`. `push_to_cache_batch()` (and `add_wave()` /
`populate_and_push()`, which wrap it) therefore runs the push inside
`_s3_writer_credentialpath`. That context manager snapshots
`.dvc/config.local`, points the remote's `credentialpath` at the resolved
writer file with `dvc remote modify --local`, and writes the prior
`config.local` back afterwards; it deletes the file if there was none before.
The restore goes through text mode, so a `config.local` with CRLF line endings
comes back with LF ones; DVC reads either. The
override exists only while the library is pushing. The older recipe — copy
the key to `.dvc/s3_write_creds` and hand-edit `.dvc/config.local` to point
at it — still works through the legacy fallback, but it is not needed.

**Do not** edit `.dvc/config` to point at write creds: it is tracked by git
and shared with every user.

## Batched DVC Operations

Publish blobs with `push_to_cache_batch()` (or `populate_and_push()` / `add_wave()`, which wrap it). These batch the underlying `dvc add` + `dvc push` for you: on a cluster scratch filesystem, batched operations process 68 files in ~2.5 minutes vs ~90 minutes sequentially. They also route through `_run_dvc_with_lock_retry()`, so concurrent writers queue on the global lock with backoff + jitter instead of failing.

**Do not run `dvc add` / `dvc push` by hand**, and never `rm` a DVC lock file — see `CLAUDE.md` §"Data Access". Hand-rolled invocations take the global `.dvc/tmp/lock` with no retry, which is how a parallel sweep gets a wave half-pushed.

## Data Loading in Scripts

**Always use `get_dataframe()` from `local_tools`** to read `.dta` files in feature scripts. It handles local files, DVC remotes, and path resolution transparently:

```python
from lsms_library.local_tools import get_dataframe, to_parquet

df = get_dataframe('../Data/sect1_hh_w5.dta')
```

**Do not** use these obsolete patterns:
- `dvc.api.open(fn, mode='rb')` + `from_dta(dta)` -- couples the script to DVC internals
- `from_dta('/absolute/path/to/file.dta')` -- breaks on other machines
- `pd.read_stata(fn)` -- bypasses DVC entirely, no remote fallback

The relative path `../Data/file.dta` works because scripts run from the wave's `_/` directory. `get_dataframe` resolves it through the DVC filesystem when the file isn't local.

## Cluster / Slurm Notes

- The `import lsms_library` is slow (~90s) due to DVC initialization at import time. Budget for this in job time limits.
- All file paths in Slurm scripts must be on shared storage (e.g., `$SCRATCH`), not `/tmp/`.
- Use the venv's `dvc` binary (`.venv/bin/dvc`), not the system one -- the system install may lack `dvc-s3`.
- The `_dvc_cmd()` helper in `data_access.py` resolves the venv binary automatically.

## Common Pitfalls

- **Wave invisible after download**: Check for a hardcoded `Waves` dict in `{country}.py` (Step 3 above).
- **"No writable DVC remotes"**: no writer key was found, *or no reader key was* — write access is only checked once the remote's reader key (`s3_creds`, normally unlocked from the WB API key) is present, so a missing reader key reports the same thing. Put the writer key at `config.s3_write_creds_path()` (or set `$LSMS_S3_WRITE_CREDS` to its path), or export `LSMS_S3_WRITE_KEY_ID` / `LSMS_S3_WRITE_KEY`; then check `data_access.permissions()`. The legacy `lsms_library/countries/.dvc/s3_write_creds` is still read as a fallback.
- **Push authenticates as the reader (`AccessDenied`)**: the committed `credentialpath` names the reader key. The library swaps the writer key in for the duration of its own push (a temporary `.dvc/config.local` override, restored afterwards), so push through `push_to_cache_batch()` / `add_wave()` / `populate_and_push()`, never a hand-run `dvc push`. If it still fails, no writer key was resolved — see the previous bullet.
- **`dvc add` timeout on cluster**: The default 120s timeout is too short for cluster scratch filesystems. `push_to_cache_batch()` uses adaptive timeouts (600s + 30s per file).
- **System `dvc` missing `dvc-s3`**: Use `.venv/bin/dvc`. The `_dvc_cmd()` helper handles this.
