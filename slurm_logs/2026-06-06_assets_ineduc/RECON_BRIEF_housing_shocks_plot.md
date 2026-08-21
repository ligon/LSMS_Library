# Recon-triage brief — housing / shocks / plot_features (2026-06-06)

READ-ONLY triage. Do NOT edit/create/commit. Repo:
/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library (branch development).
Venv ./.venv/bin/python.

## CRITICAL DVC RULE
Read data ONLY via `from lsms_library.local_tools import get_dataframe`.
**NEVER run `dvc pull`/`dvc fetch` CLI** (deadlocks on the global lock).

## Goal
For the ASSIGNED country, triage whether each of these 3 features is
implementable, i.e. does the survey carry the corresponding module?

1. **housing** — dwelling characteristics: index (t, i). Columns vary by survey
   but typically Roof / Floor / Walls (material names), Water, Toilet/Sanitation,
   Electricity, Rooms, Tenure. Most LSMS/household surveys HAVE a housing/dwelling
   section. Study an existing impl: `grep -rl 'housing:' lsms_library/countries/*/_/data_scheme.yml` (e.g. Uganda, Malawi) — read its data_scheme + a wave data_info `housing:` block for the canonical shape.
2. **shocks** — shocks-and-coping module: "did the household experience
   drought/flood/death/job-loss/price-shock in the last N years, and how did you
   cope". This is an ISA/EHCVM-specific section; MANY non-ISA surveys OMIT it.
   Study an existing impl (e.g. Niger, Malawi, Tanzania, Nigeria).
3. **plot_features** — agricultural PLOT details: index (t, i, plot). Plot area,
   tenure, GPS, etc. Absent from non-agricultural surveys. Study an existing impl
   (Uganda, Nigeria, Malawi).

## Steps (per feature, for the assigned country)
- Confirm the country does NOT already declare it (grep its _/data_scheme.yml).
- List the country's wave dirs + scan the Data/ filenames + (via get_dataframe on
  1-2 likely files) for the relevant module. Housing: look for dwelling/housing
  section. Shocks: shock/coping/risk section. Plot: a plot/parcel/land roster.
- Decide: IMPLEMENTABLE (module present — give source file + key columns + which
  canonical columns map) OR ABSENT (module not in this survey — give the reason).

## Report (COMPACT, <250 words). One block per feature:
```
housing: IMPLEMENTABLE | ABSENT — <source file + key cols, or why absent>
shocks: IMPLEMENTABLE | ABSENT — <...>
plot_features: IMPLEMENTABLE | ABSENT — <...>
```
For IMPLEMENTABLE, note the wave(s), the household id column (must match the
roster's i), and a rough column→canonical mapping. Be decisive. Do NOT write files.
