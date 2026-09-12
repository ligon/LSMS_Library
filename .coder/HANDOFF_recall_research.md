# Handoff — recall-period research (GH #851 Phase I follow-on)

**Written 2026-09-09 by Sue (Claude Opus 5).** Read this, then
`.coder/coverage/food_recall.org`, then `.coder/charter-recall-windows.md`.

## The job in one line

**33 of 67 food cells have no recorded recall period because nobody has read the
questionnaire.** Read them, and record what they say.

That is the whole task. It is not a coding task, it is a *reading* task with a
small recording step, and it is the highest-leverage thing left on this thread:
every downstream question — `period=`, cross-country pooling, any claim about
levels — improves in direct proportion to how much of this table is filled.

## Where things stand

Phase I is closed and signed off (charter §2, all ten criteria). Shipped:

- `ll.recall()` and `df.attrs['recall']` — the reference period of every food
  number, with its evidence rung. `lsms_library/recall.py`.
- `.coder/coverage/food_recall.csv` — the 67-cell evidence base. **This is what
  you edit.**
- `countries/{C}/_/recall.yml` — 20 generated files, the packaged store the
  library actually reads. **Do not hand-edit**; regenerate.

15 commits sit on `development` ahead of `ligon/development`, **unpushed**,
including a real data correction (Burkina Faso 2014: 460,438 recovered rows,
delivered expenditure 80.3M → 342.1M CFA). Publishing them is @ligon's call.

## The disk situation — measured, and less alarming than it sounds

`/home/coder` sits on a volume that was 99% full (2.4 GB free) mid-session;
4.9 GB free as of writing. ~76 GB of the 128 GB is outside `/home/coder`
entirely and is not ours.

**Documentation is NOT the problem.** Measured, the *entire* Documentation
footprint of all twelve target countries is **407 MB**:

| country | docs | PDFs | | country | docs | PDFs |
|---|---|---|---|---|---|---|
| EthiopiaRHS | 134 MB | 148 | | CotedIvoire | 39 MB | 21 |
| Uganda | 68 MB | 44 | | GhanaSPS | 29 MB | 8 |
| Malawi | 56 MB | 31 | | Burkina Faso | 26 MB | 15 |
| Mali / Niger | 17 MB each | 17 / 16 | | Senegal / Benin / Cambodia / Guinea-Bissau | 3–7 MB each | 1–3 |

So you can pull every questionnaire you need and still use under half a gigabyte.

**The actual risk is triggering a cold build.** A `Country(...).food_acquired()`
on a cold cache pulls `.dta` sources by the gigabyte — the full GhanaLSS
`g7sec9b.dta` alone is 3.2 GB, and this session pulled it (twice, once because a
sibling agent deleted it) for a single metadata read. *This research needs no
builds at all.* If you find yourself waiting on a build, stop and ask why.

### Protocol

```python
from lsms_library.data_access import get_data_file
p = get_data_file('lsms_library/countries/Uganda/2018-19/Documentation/<name>.pdf')
```

- Files land **in the repo tree** under `{country}/{wave}/Documentation/`, and
  are gitignored. They count against the 2.4–5 GB.
- `pdftotext -layout` first. If a PDF has no text layer it is a scan — render
  pages with `mutool` and read the PNGs visually. That is how the GLSS1/GLSS2
  coverage verdicts were reached; see `.coder/coverage/absent_verdicts.csv`.
- **Delete each PDF after extracting what you need.** `rm` the `.pdf`, keep the
  `.dvc` sidecar. It re-fetches on demand.
- Never invoke the `dvc` CLI. Never `rm` a DVC lock file.
- If space gets tight: `lsms-library cache clear --country X` is the sanctioned
  tool, but the parquet cache at `~/.local/share/lsms_library/` (6.3 GB, mostly
  `dvc-cache`) is **shared with other agent sessions** — clearing it slows
  everyone. Prefer deleting the PDFs you pulled.

## What to record, and what counts as evidence

Edit `.coder/coverage/food_recall.csv`, then:

```
.venv/bin/python scripts/promote_recall_records.py --check   # validates, writes nothing
.venv/bin/python scripts/promote_recall_records.py           # regenerates the 20 YAMLs
.venv/bin/python -m pytest tests/test_recall_record.py -q -m "not slow"
```

The promoter **refuses to write** on a contradiction — an unknown rung, a number
on a cell that varies within the wave, a window asserted by a `not-recorded`
cell. That is deliberate; it caught a real error in the table this session.

### The rung ladder is the point

Borrowed from `capability.py` for its reason: *a weak source must not be
laundered into a fact.*

| rung | what earns it |
|---|---|
| `questionnaire` | the instrument states it, **quoted** (or transcribed verbatim into a script) |
| `variable-label` | a Stata variable label states it |
| `config-comment` | a maintainer asserted it in repo prose — **not** evidence |
| `filename` | inferred from `conso7jours` and the like — a **guess** |
| `not-recorded` | nobody has looked |

Only 8 of the 34 currently-stated windows are `questionnaire`-grade. Your job is
to move cells *up* this ladder, and four rungs were corrected downward this
session after spot-checking (Guatemala's verbatim turned out to be a Stata label;
Panama's was a wave-script comment). **Spot-check your own rungs before
promoting** — over-grading is the failure the ladder exists to prevent.

Every non-empty `recall_verbatim` needs an `evidence` citation: `path:line`, or
a Documentation path plus page.

### Two fields that are easy to get wrong

- **`n_visits` counts ASKS, not interviewer visits.** GLSS7 fields seven visits
  and asks food at six — visit 1 is intake. `recall_days × n_asks` is arithmetic
  only on asks: 6 × 5 = 30 days of coverage, where 7 × 5 = 35 is the fieldwork
  cycle. Getting this wrong makes the product column mean two things at once.
- **`within_wave_variation` means the DELIVERED table mixes windows**, not that
  the instrument offered several. Guatemala and Panama's instruments *do* mix an
  actual 15-day recall with usual-month estimates — but both builds resolve it
  and ship only the 15-day variables, so their delivered window is
  single-valued. Source-level variation goes in `notes`.

**Blank beats plausible.** Never interpolate from a sibling wave. GhanaLSS
2005-06 has eleven interviewer visits and known ask counts and every numeric
field empty, because its spacing is unestablished and the only tempting number
descends from boilerplate belonging to GLSS3. That row is what the file is for.

## The queue, roughly by leverage

`capability.py`'s insight applies: this is a bounded human step **per series**,
not per cell. One questionnaire can close eight cells.

| series | cells | note |
|---|---|---|
| Uganda UNPS | 8 | biggest single win; food window undocumented despite a `[RECALL]` CAPI-fill discussion at `Uganda/_/CONTENTS.org:2155` that is **non-food** — do not transfer it |
| EthiopiaRHS | 5 | 148 PDFs; not a WB series (Harvard Dataverse) |
| Malawi IHS | 5 | `Malawi/_/data_scheme.yml:164` "Recall: last 7 days" is the **rCSI coping battery**, not consumption — a known trap |
| GhanaSPS | 3 | `GhanaSPS/_/README.org:26` describes **GLSS** tables, not GhanaSPS's own |
| Mali, Niger, Senegal | 2 each | EHCVM family; check whether one instrument serves all |
| Benin, Burkina Faso 2018-19, Cambodia, CotedIvoire, Guinea-Bissau | 1 each | |
| GhanaLSS 2005-06 | 1 | needs GLSS5's own manual for the visit **spacing**; ask counts already known |

Three of those trap entries were found by a red-team correcting its own earlier
grep — a country mentioning a recall period somewhere is not a country
documenting its *food* window.

## Pitfalls this session actually hit

- **`AGENTS.md` is a symlink to `CLAUDE.md`.** Edit one.
- **A warm-cache scan is structurally blind to type defects.** 193 warm parquets
  showed zero mixed-type index levels while a cold build of one of them *was*
  mixed — pyarrow launders it on write. GH #323's shape: the bug hiding behind
  the cache it poisons.
- **Plausible mechanisms arrive before correct ones.** `CLAUDE.md` §3b named
  Burkina Faso 2014 as the corpus's worst NaN-key deletion. It was read at
  session start, and when a red-team surfaced an anomaly in that exact cell both
  the reviewer and I diagnosed a *summation*, because `_ADDITIVE_MEASURE_COLUMNS`
  made that story available. The documentation was right; the diagnosis was
  wrong, twice, confidently.
- **Don't let an engineering decision acquire a research precondition.** Twice
  this session a general elicitation problem (item-list length; ask-order bias)
  was imported into a narrow plumbing decision, and both times @ligon caught it.
  See charter §6.
- **`pkill -f <pattern>` matches its own command line** and will kill your shell.

## Explicitly NOT in this job

Phase II (`period=`), any scaling or annualisation, the pooling warning, the
per-column window carrier, non-food and agricultural tables, and the
date-measurement pipeline. Phase II's gates are in charter §6 and are decisions
for @ligon, not research.

Open follow-up: **GH #864** — `food_prices` reports Burkina Faso 2014 as 64.8%
unpriceable and misattributes the cause.
