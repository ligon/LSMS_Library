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
- S3 write credentials in `.dvc/s3_write_creds` with a `.dvc/config.local` override (see below)
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
| `local_status` | `"yes"` / `"covered"` / `"derived"` / `"unknown"` / `"no"` (see below).  |
| `local_waves`  | the wave dirs that back, cover, or constitute this entry.                |

**Only `local_status == "no"` is an acquisition candidate**, and a `no` has to
be *earned* — it is a confident claim that we neither hold this study nor
anything containing it (GH #600):

| status | meaning | `local` |
|---|---|---|
| `yes` | a wave dir records this catalog id | `True` |
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
can hold several entries (`Malawi/2016-17/` is `2936, 2939`: IHS4 in
`Data/Cross_Sectional/`, the 2016 IHPS panel wave in `Data/Panel/`). Entries a
held release *subsumes* go in `#+CATALOG_COVERS:` — **covered is not held**, and
claiming otherwise is the same false claim pointed the other way. Relations
*between catalog entries* (a study re-catalogued in a second repository; a
derived re-release) live in `lsms_library/catalog_relations.yml`, each with its
evidence.

**When you add a wave, check whether its `Data/` holds more than one catalog
entry** — a `Panel/` + `Cross_Sectional/` split is the tell. The WB datafile API
settles it in one call: `/index.php/api/catalog/{id}/data_files?id_format=id`
lists an entry's files; compare against the `.dta` you actually downloaded.

To (re)stamp provenance across the tree:

```sh
python scripts/backfill_wave_provenance.py --dry-run   # report only
python scripts/backfill_wave_provenance.py             # write SOURCE.org
```

Countries that are not WB datasets (`EthiopiaRHS`, `KenyaLPS`) are marked
`discoverable=False` in `_COUNTRY_CATALOG` and return `[]` with an explanatory
log — deliberately, rather than being silently absent.

### Step 2: Add the wave

```python
from lsms_library.data_access import add_wave

add_wave("Ethiopia", catalog_id="6161")
```

This will:
1. Resolve the WB catalog IDNO
2. Show a confirmation prompt (disable with `confirm=False`)
3. Create `{Country}/{wave}/Data/`, `Documentation/`, and `_/` directories
4. Write `Documentation/SOURCE.org` with the catalog URL
5. Download the full Stata zip from the WB NADA API
6. Extract all `.dta` files
7. Run batched `dvc add` + `dvc push` (if `push=True`, the default)
8. Show a rich progress summary

Pass `verbose=True` (default) for spinners and a summary table. Use `verbose=False` for scripted/non-interactive use.

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

DVC reads credentials from `.dvc/config`:
```ini
['remote "ligonresearch_s3"']
    url = s3://dvcbucket0/LSMS
    credentialpath = s3_creds
```

The `s3_creds` file contains the **reader** IAM key. To push, you need a **writer** key. The standard pattern:

1. Place writer credentials in `.dvc/s3_write_creds` (same format as `s3_creds`)
2. Override `credentialpath` in `.dvc/config.local` (gitignored by DVC):
   ```ini
   ['remote "ligonresearch_s3"']
       credentialpath = s3_write_creds
   ```

DVC layers config files: `config.local` overrides `config`. This keeps the reader key as the shared default while granting write access on machines that need it.

**Do not** edit `.dvc/config` to point to write creds -- that file is tracked by git and shared with all users.

## Batched DVC Operations

Always use batched `dvc add` + `dvc push` when adding multiple files. The `populate_and_push()` and `push_to_cache_batch()` functions do this automatically. On a cluster scratch filesystem, batched operations process 68 files in ~2.5 minutes vs ~90 minutes sequentially.

If running DVC commands manually, follow CONTRIBUTING.org steps 8-9:
```bash
cd lsms_library/countries
dvc add Ethiopia/2021-22/Data/*.dta
dvc push
```

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

Some older library functions (e.g., `age_sex_composition`) still use `dvc.api.open` internally -- that's fine, they accept the same relative paths. But new scripts should prefer `get_dataframe` for consistency.

## Cluster / Slurm Notes

- The `import lsms_library` is slow (~90s) due to DVC initialization at import time. Budget for this in job time limits.
- All file paths in Slurm scripts must be on shared storage (e.g., `$SCRATCH`), not `/tmp/`.
- Use the venv's `dvc` binary (`.venv/bin/dvc`), not the system one -- the system install may lack `dvc-s3`.
- The `_dvc_cmd()` helper in `data_access.py` resolves the venv binary automatically.

## Common Pitfalls

- **Wave invisible after download**: Check for a hardcoded `Waves` dict in `{country}.py` (Step 3 above).
- **"No writable DVC remotes"**: Set up `.dvc/config.local` with `credentialpath = s3_write_creds`.
- **`dvc push` uses reader key**: DVC reads `credentialpath` from config. The `config.local` override must point to the writer credentials file.
- **`dvc add` timeout on cluster**: The default 120s timeout is too short for cluster scratch filesystems. `push_to_cache_batch()` uses adaptive timeouts (600s + 30s per file).
- **System `dvc` missing `dvc-s3`**: Use `.venv/bin/dvc`. The `_dvc_cmd()` helper handles this.
