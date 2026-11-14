# `s07bq03a` in the served EHCVM-2 2021-22 waves: Togo-style factor and CIV-style imputation check

Read-only data check for Sue (scrum master, Slurm job 41201840).  Run and written by a Claude agent,
model **Claude Opus 5.5** (`claude-opus-5-5`), on 2026-11-12.  No code, config, cache or GitHub state was
changed.  Aggregates only; no microdata rows appear here or in any file beside it.

## Verdict

| country | 2021-22 verdict (confidence) | key number | baseline wave |
|---|---|---|---|
| Senegal | **clean** of both patterns (high); minor other | q3 99.26% on the quarter grid; q3/q4 and q3/q7a modes 1.0 in every region; no eqadu1 signature.  Minor: 100 salt-in-kg rows at exactly 0.02 kg x hhsize (34% of that cell), 0.07% of served purchased rows, 0.03% of Expenditure | 2018-19 clean (98.76%) |
| Mali | **clean** (high) | q3 99.75% on grid; 0 H rows | **2018-19: other** (high on the pattern, medium on its cause). 3,357 constructed `Riz local Gambiaka` kg rows, q3 = 1.4 kg x hhsize; 57.8% of served rice-kg purchased kg |
| Niger | **clean** (high) | q3 99.96% on grid; 39 H rows, all simple fractions | 2018-19: q3 clean (99.18%).  **Side finding:** `s07bq07a` is computed on 15,264 rows (14.2%; mechanism unidentified); 26.0% of served Expenditure passes through those rows |
| Burkina Faso | **clean** (high) | q3 99.48% on grid; 0 H rows | 2018-19 clean (99.36%) |
| Benin (control) | **clean** (high), as its CONTENTS records | q3 98.99% on grid; 6 of 17,099 `Jamais` residuals | 2018-19 clean (99.07%).  A small hhsize-proportional tail in both waves (633 / 450 rows, 0.2-0.3% of served rows) |
| Togo (control) | **Togo-style factor**, reproduced exactly | 14.70% on grid; Centrale 1.6725 on 1,435 of 1,556 | 2018 clean (98.68%) |

The Côte d'Ivoire 2021-22 control reproduces the CIV-style classes.  H is 6.82% (recorded 6.67%), the H
slope 0.936 (0.94), and q3/eqadu1 repeats 52.7% (53.4%).

**What "clean" means here, and its limits.**  The quarter-grid and ratio checks cannot see an
imputation that lands on the grid.  Mali 2018-19's block is 22.6% on grid, and it was found through its
missing `vague`, not through the grid.  An adjustment applied to q3 and q4 together (the CIV caveat) would
show in q4 and q5.  Both are at 97% or more on grid in all four 2021-22 waves (Table 1), so that case is
covered.  A replacement by on-grid typical amounts is not covered, except where it leaves the eqadu1 or
hhsize signature.

## What was run

- **Code:** development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`, cloned fresh in each burst job from
  `/global/home/users/ligon/mirrors/LSMS_Library`.
  - Benin, Togo and CIV 2021-22 were read from the same clone, not from `6538bc8b8`.  The `s07b` and welfare
    `.dvc` sidecars of those waves have the same md5 on both commits: Togo `9fa32cf8…` / `9bf6b71b…`,
    CIV `d1ad349d…` / `b7d34b42…`, Benin `21b9e961…` / `cd46c4e8…`.  Only the raw files are read, so one
    checkout serves every wave and the two-checkout import trap does not arise.
  - The venv is the mounted image (`bin/savio_venv.sh mount`, generation `20261019T221223Z`).
  - Each job asserted that `lsms_library` imports from its own clone, and that `data_root()` is under the
    private root.
- **Burst jobs** (`savio4_htc`, 4 cores, 48 GB, `--exclude=n0035.savio4`):
  - 41245278: controls (Togo, CIV, Benin).
  - 41245279: Senegal, Mali, Niger, Burkina Faso.
  - 41245820: the follow-up, then a queue loop that ran four more scripts inside the same job.
- **Sources:** read only through `lsms_library.local_tools.get_dataframe`, from each wave's `_/`
  directory with the wave scripts' relative paths.  The files are `s07b_me_*.dta`, `ehcvm_welfare_*.dta`
  (`ehcvm_welfare_2b_bfa2021.dta` for Burkina 2021-22) and `s00_me_*.dta` for region where the welfare
  file has none (CIV 2018-19, Benin 2021-22).  The md5 of every file is in the appendix.  Region and
  eqadu1 match 99.96-100% of positive-q3 rows in every wave (eqadu1 wherever a welfare file exists).
- **Served table:** one, `Country('Mali').food_acquired(waves=['2018-19'])`, built cold in the private
  root.  Every other served figure is a raw reconstruction:
  - it uses the shipped `ehcvm.derive_ehcvm_purchase_value` and the reshape's keep rule;
  - it reproduces every recorded "C (served)" total to the FCFA (12 of 12 waves that record one), and
    every recorded A total (11 of 11).
- **Private root:** `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/ehcvm2-live-s07bq03a-check/`
  (mode 700).  `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG` were under
  `/local/job<ID>/`.  The issues log was never written, and the clones held no in-tree parquet.  The root
  held DVC blobs and the Mali build (501 MB) and was removed after job 41245820 ended.  All three logs report
`issues log lines: 0` and `in-tree parquet: 0`.

### Definitions

- **Quarter grid:** `|4x - round(4x)| < 1e-6`.
- **Decimals:** digits in the shortest round-trip representation, at float32 precision when the value is
  exactly float32-representable (the Stata `float` type), else at float64.  A tolerance rule
  (`|x - round(x,d)| <= 1e-7 max(1,|x|)`) agrees on 99.1-100% of positive rows in every wave but Togo 2021-22 (81.9%; its factor-scaled values are double-precision products, H under either rule).
- **H:** off grid, 4+ decimals.  **L:** off grid, up to 3 decimals.  On CIV 2021-22 my split differs from
  the recorded one by about 450 rows of 46,000 (H 20,692 vs 20,239; L 25,600 vs 26,004).  The grid share
  agrees to 0.01 point.
- **Unit type:** by `ehcvm._unit_key`.  `Unité` is code 147 or the label `unité`.  Kg/Litre is codes 100/101
  or those labels.  "Other" is everything else (`Sac (50 Kg)` included).
- **`Jamais`:** `s07bq06` labelled `Jamais`.  Its denominator is `Jamais` rows with q3 > 0; Togo's recorded
  denominator, 12,963, counts every `Jamais` row, and mine is 12,940.
- **Per-capita signature:** see the note under Table 3.

## Controls reproduce

| control | recorded (CONTENTS.org, branch `6538bc8b8`) | this run |
|---|---|---|
| Togo 2021-22, q3 on grid | 14.7% (2018: 98.7%) | 14.70% (98.68%) |
| Togo 2021-22, q3/q4 mode on never-bought own-production rows | Centrale 1.6725 on 1,435 of 1,556; Savanes 1.338 (2,341 of 2,494); Plateaux 1.2265 (1,946 of 2,235); Kara 1.0035 (1,400 of 1,594); Maritime 1.0035 (983 of 1,292); Grand Lomé 1.2 (64 of 69) | 1.6725 1,435/1,556; 1.338 2,341/2,500; 1.2265 1,946/2,235; 1.0035 1,400/1,595; 1.0035 983/1,294; 1.2 64/64 |
| Togo 2021-22, purchase-only rows | Grand Lomé 0.9143 | q3/q7a Kg/Litre mode 0.9143 on 572 of 1,110 |
| Togo `Jamais` with a residual | 11,952 (2018: 166) | 11,952 (166) |
| Togo 2018 served A / C | 32,656,157 / 31,676,795 | 32,656,157 / 31,676,795 |
| CIV 2021-22: on grid / H / L | 84.75 / 6.67 / 8.57% | 84.74 / 6.82 / 8.44% |
| CIV 2021-22: H slope log q3 on log eqadu1 (resid sd) | 0.94 (0.19) | 0.936 (0.22) |
| CIV 2021-22: H q3/eqadu1 repeats; under ±15% jitter; on hhsize; on eqadu2 | 53.4; 3.8; 11.6; 7.7% | 52.7; 0.75; 7.9; 5.6% |
| CIV 2021-22: H share in 1-member / 13+ households | 22.4 / 1.6% | 22.7 / 1.8% |
| CIV 2021-22: mode 1.0 on own-production rows; `Jamais` residuals | 19,413 of 19,717; 401 of 28,703 | 19,413 of 19,717; 401 of 28,703 |
| CIV 2021-22: served Expenditure; off-grid share of it | 121,393,491; 4.4% | 121,393,491; 4.44% |
| Benin 2021-22: q3 on grid (2018-19); `Jamais` residuals | 98.95% (99.05%); 6 of 17,099 | 98.99% (99.07%); 6 of 17,099 |

## Check 1. Grid shares

#### Table 1. Check 1, share of positive values on the quarter grid (%)
| country | wave | n q3>0 | q3 | q4 | q5 | q7a | q3 unité | q3 Kg/Litre | q3 other |
|---|---|---|---|---|---|---|---|---|---|
| Senegal | 2018-19 | 214,392 | 98.76 | 99.91 | 99.56 | 99.56 | 99.94 | 95.93 | 99.42 |
| Senegal | 2021-22 | 219,717 | 99.26 | 99.05 | 99.24 | 99.54 | 100.00 | 97.29 | 99.82 |
| Mali | 2018-19 | 159,968 | 96.83 | 99.95 | 99.85 | 99.77 | 100.00 | 90.45 | 98.75 |
| Mali | 2021-22 | 146,360 | 99.75 | 98.87 | 99.67 | 99.78 | 99.99 | 99.11 | 99.96 |
| Niger | 2018-19 | 107,216 | 99.18 | 99.77 | 99.79 | 85.77 | 100.00 | 92.52 | 99.96 |
| Niger | 2021-22 | 130,519 | 99.96 | 99.95 | 99.96 | 99.99 | 99.98 | 99.69 | 99.98 |
| Burkina_Faso | 2018-19 | 136,888 | 99.36 | 99.70 | 99.36 | 99.27 | 99.99 | 97.18 | 99.65 |
| Burkina_Faso | 2021-22 | 68,677 | 99.48 | 99.66 | 99.59 | 99.86 | 100.00 | 98.57 | 99.50 |
| Benin | 2018-19 | 187,672 | 99.07 | 98.91 | 99.29 | 99.83 | 99.93 | 96.93 | 99.17 |
| Benin | 2021-22 | 204,206 | 98.99 | 97.37 | 97.76 | 99.90 | 99.52 | 98.51 | 98.91 |
| Togo | 2018 | 112,087 | 98.68 | 99.78 | 98.79 | 99.35 | 99.98 | 92.61 | 99.17 |
| Togo | 2021-22 | 109,943 | 14.70 | 98.93 | 96.41 | 98.42 | 97.79 | 0.14 | 0.07 |
| CotedIvoire | 2018-19 | 292,741 | 99.02 | 99.78 | 99.34 | 99.88 | 99.99 | 97.16 | 98.94 |
| CotedIvoire | 2021-22 | 303,274 | 84.74 | 73.75 | 76.36 | 99.81 | 91.97 | 72.29 | 84.49 |


The four served 2021-22 waves are at 99.26-99.96% overall and 97.3-99.7% on Kg/Litre rows, against Togo
2021-22's 0.14%.  Their q4, q5 and q7a are 97% or more on grid by unit type.  Niger 2018-19 q7a (85.77%)
is the one outlier, and it is a last-purchase column, not q3 (side finding below).

## Check 2. Togo-style regional factor

#### Table 2. Check 2, the regional factor
| country | wave | regions with q3/q4 mode 1.0 (own-production, never bought, n>=20) | modes other than 1 | lowest % at mode | q3 Kg/Litre on grid, range over regions (%) | regions with q3/q7a mode 1.0 (Kg/Litre purchase-only, n>=30) | q3/q7a modes other than 1 | Jamais rows with residual > 0 | Jamais rows with q3 > 0 |
|---|---|---|---|---|---|---|---|---|---|
| Senegal | 2018-19 | 14/14 | none | 95.90 | 92.7-98.1 | 14/14 | none | 369 | 11,348 |
| Senegal | 2021-22 | 12/12 | none | 93.91 | 95.7-99.0 | 14/14 | none | 140 | 4,348 |
| Mali | 2018-19 | 11/11 | none | 91.02 | 86.1-94.9 | 11/11 | none | 519 | 15,320 |
| Mali | 2021-22 | 8/8 | none | 96.89 | 98.0-99.7 | 9/9 | none | 212 | 10,535 |
| Niger | 2018-19 | 7/7 | none | 91.12 | 78.3-97.7 | 8/8 | none | 98 | 3,086 |
| Niger | 2021-22 | 7/7 | none | 96.39 | 98.7-99.9 | 8/8 | none | 30 | 1,505 |
| Burkina_Faso | 2018-19 | 13/13 | none | 92.27 | 92.3-99.6 | 13/13 | none | 220 | 9,285 |
| Burkina_Faso | 2021-22 | 13/13 | none | 78.73 | 97.7-99.7 | 13/13 | none | 255 | 7,166 |
| Benin | 2018-19 | 11/11 | none | 95.60 | 92.3-99.3 | 12/12 | none | 301 | 17,959 |
| Benin | 2021-22 | 11/11 | none | 98.88 | 97.0-99.6 | 12/12 | none | 6 | 17,099 |
| Togo | 2018 | 6/6 | none | 97.37 | 87.5-96.6 | 6/6 | none | 166 | 15,435 |
| Togo | 2021-22 | 0/6 | 1.0035, 1.2, 1.2265, 1.338, 1.6725 | 75.97 | 0.0-0.4 | 0/6 | 0.9143, 1.0035, 1.2265, 1.338, 1.6725 | 11,952 | 12,940 |
| CotedIvoire | 2018-19 | 33/33 | none | 83.13 | 89.8-100.0 | 33/33 | none | 1,231 | 43,380 |
| CotedIvoire | 2021-22 | 33/33 | none | 91.72 | 45.3-82.7 | 32/33 | 3 | 401 | 28,703 |


Per-region tables are in `tables.md` (Check 2b).  In the four served countries the q3/q4 mode is exactly 1.0
in every region with 20 or more never-bought own-production rows, in both waves.  The q3/q7a mode on
Kg/Litre purchase-only rows is 1.0 in every region too.  That second check is the one that recovers Togo's
factors where own production is absent (Grand Lomé 0.9143).

Burkina 2021-22's lowest share at the mode is 78.7%, in Boucle du Mouhoum.  The rest of that region's rows
sit at exactly 2.0 (93 rows): an integer ratio between on-grid values, not a factor.  The same modes hold
by milieu and by vague (`out/*.json`, `other_axes`).

`Jamais` rows with a positive residual run from 30 to 519 in the four countries.  Togo 2021-22 has 11,952.

*Cross-wave Kg/Litre medians (later over earlier wave, same region, item, unit and size, cells of 10+ rows).*
The overall median is 1.0 in every country: Senegal (366 cells), Mali (199), Niger (59), Burkina Faso (92)
and Benin (149).  Region medians that are not 1 are round ratios on 5-28 cells: Mali Bamako 0.667,
Senegal Kolda 0.857, Niger Zinder 1.833, Benin Borgou 1.5.  None is a non-round constant of Togo's kind.
Togo's own run recovers 1.6725, 1.2265, 1.338 and 1.0035 on 24 cells.  This check is noisy at these cell
counts and is supporting evidence only.

## Check 3. CIV-style H/L classes

#### Table 3. Check 3, the H/L classes and the per-capita signature
| country | wave | on grid % | H n | H % | H not a simple fraction (n) | L n | L % | H slope log q3 on log eqadu1 (resid sd) | H q3/eqadu1 repeats, real / ±15% jitter (%) | H % in 1-member hh | H % in 13+ hh | off-grid q3/eqadu1 repeats, real / permuted (%) | eqadu1-signature rows | off-grid q3/hhsize repeats, real / permuted (%) | hhsize-signature rows |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Senegal | 2018-19 | 98.76 | 1,062 | 0.50 | 983 | 1,607 | 0.75 | 0.42 (0.676) | 0.28 / 0.0 | 0.06 | 0.80 | 0.22 / 0.11 | 6 | 4.31 / 2.51 | 115 |
| Senegal | 2021-22 | 99.26 | 258 | 0.12 | 231 | 1,363 | 0.62 | n=198 | 1.16 / 0.0 | 0.61 | 0.08 | 0.0 / 0.0 | 0 | 9.07 / 1.67 | 147 |
| Mali | 2018-19 | 96.83 | 1,448 | 0.91 | 0 | 3,621 | 2.26 | 0.969 (0.098) | 79.27 / 45.2 | 0.00 | 0.61 | 34.94 / 6.65 | 1,771 | 70.45 / 39.04 | 3,571 |
| Mali | 2021-22 | 99.75 | 0 | 0.00 | 0 | 373 | 0.25 | n=0 | - | 0.00 | 0.00 | 0.0 / 0.0 | 0 | 17.43 / 22.25 | 65 |
| Niger | 2018-19 | 99.18 | 146 | 0.14 | 2 | 733 | 0.68 | n=135 | 0.0 / 0.0 | 0.00 | 0.15 | 0.0 / 0.0 | 0 | 43.91 / 34.7 | 386 |
| Niger | 2021-22 | 99.96 | 39 | 0.03 | 0 | 19 | 0.01 | n=13 | 0.0 / 0.0 | 0.00 | 0.20 | 0.0 / 0.0 | 0 | 0.0 / 0.0 | 0 |
| Burkina_Faso | 2018-19 | 99.36 | 0 | 0.00 | 0 | 878 | 0.64 | n=0 | - | 0.00 | 0.00 | 0.34 / 0.0 | 3 | 10.59 / 7.4 | 93 |
| Burkina_Faso | 2021-22 | 99.48 | 0 | 0.00 | 0 | 360 | 0.52 | n=0 | - | 0.00 | 0.00 | 0.0 / 0.0 | 0 | 0.0 / 1.11 | 0 |
| Benin | 2018-19 | 99.07 | 360 | 0.19 | 106 | 1,380 | 0.74 | 1.043 (0.182) | 5.28 / 0.0 | 0.67 | 0.25 | 0.34 / 0.17 | 6 | 25.86 / 10.06 | 450 |
| Benin | 2021-22 | 98.99 | 1,322 | 0.65 | 288 | 749 | 0.37 | 0.914 (0.232) | 3.71 / 0.0 | 3.63 | 0.23 | 0.0 / 0.0 | 0 | 30.56 / 4.3 | 633 |
| Togo | 2018 | 98.68 | 0 | 0.00 | 0 | 1,474 | 1.32 | n=0 | - | 0.00 | 0.00 | 0.61 / 0.0 | 9 | 14.52 / 13.64 | 214 |
| Togo | 2021-22 | 14.70 | 82,526 | 75.06 | 81,035 | 11,251 | 10.23 | 0.278 (0.57) | 20.68 / 2.9 | 74.62 | 69.79 | 3.73 / 2.52 | 3,497 | 42.38 / 37.3 | 39,742 |
| CotedIvoire | 2018-19 | 99.02 | 0 | 0.00 | 0 | 2,868 | 0.98 | n=0 | - | - | - | - | - | - | - |
| CotedIvoire | 2021-22 | 84.74 | 20,692 | 6.82 | 20,640 | 25,600 | 8.44 | 0.936 (0.22) | 52.66 / 0.75 | 22.72 | 1.76 | 26.19 / 0.63 | 12,123 | 7.48 / 6.32 | 3,464 |


**The per-capita signature (follow-up).**  On off-grid positive q3, a row counts when q3/D, rounded to 4
significant digits, recurs on 3 or more rows **and 3 or more distinct q3 values** inside (item, unit,
size), with D = eqadu1 or hhsize.  The distinct-q3 rule is there because identical (q3, D) pairs repeat by
themselves: on-grid rows score 74-92% on hhsize without it.  The null shuffles D across rows of the same
cell.

- **CIV:** the eqadu1 signature separates cleanly (26.2% against 0.6%; 12,123 rows).
- **Togo:** shows no excess (3.7 / 2.5%).
- **The four 2021-22 waves:** eqadu1 signature 0 rows in each.

The H counts left are small and almost all simple fractions (thirds, sixths, sevenths):

- Senegal 2021-22: 231 non-fraction H rows, 0.11% of positive q3.
- Niger 2021-22: 0 non-fraction H rows.
- Mali and Burkina 2021-22: no H rows.

Small hhsize-proportional tails remain.  Each is under 0.35% of served rows:

- **Senegal 2021-22:** 100 salt rows in kg at exactly 0.02 kg x hhsize, 34% of that salt-kg cell's 294 rows.
- **Benin 2021-22:** 633 rows, mostly maize per bassine at hhsize/6, /5 or /3.  They carry 32.7% own
  production.
- **Benin 2018-19:** 450 rows of mixed items.
- **Niger 2018-19:** a 0.4 kg powdered-milk value, with no excess over the null (43.9 / 34.7%).

**Mali 2018-19 is the exception**, and it is not the CIV pattern (next section).

## Check 4. Served impact

#### Table 4. Check 4, served purchased rows (raw reconstruction) and what each pattern touches
| country | wave | served purchased rows | Expenditure C, FCFA | recorded C (CONTENTS.org) | off-grid q3, % rows | off-grid q3, % Exp | phantom Jamais purchases, n (% rows) | phantom, % Exp | eqadu1 signature, % rows / % Exp | hhsize signature, % rows / % Exp | rows with no vague, n (% served rows) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Senegal | 2018-19 | 194,173 | 147,345,571 | 147,345,571 | 1.35 | 0.67 | 369 (0.19%) | 0.00 | 0.0 / 0.0 | 0.06 / 0.01 | 0 (0%) |
| Senegal | 2021-22 | 201,080 | 182,628,314 | 182,628,314 | 0.73 | 0.63 | 140 (0.07%) | 0.00 | - | 0.07 / 0.03 | 0 (0%) |
| Mali | 2018-19 | 129,599 | 89,614,679 | 89,614,679 | 3.84 | 3.14 | 519 (0.4%) | 0.00 | 1.37 / 1.13 | 2.75 / 2.31 | 3357 (2.59%) |
| Mali | 2021-22 | 118,063 | 97,178,692 | 97,178,692 | 0.27 | 0.09 | 212 (0.18%) | 0.00 | - | 0.05 / 0.01 | 0 (0%) |
| Niger | 2018-19 | 89,819 | 53,351,991 | 53,351,991 | 0.93 | 2.32 | 98 (0.11%) | 0.00 | - | 0.41 / 1.19 | 0 (0%) |
| Niger | 2021-22 | 116,633 | 67,358,383 | 67,358,383 | 0.04 | 0.11 | 30 (0.03%) | 0.00 | - | - | 0 (0%) |
| Burkina_Faso | 2018-19 | 105,950 | 52,466,280 | 52,466,280 | 0.77 | 0.42 | 220 (0.21%) | 0.00 | 0.0 / 0.0 | 0.08 / 0.03 | 0 (0%) |
| Burkina_Faso | 2021-22 | 52,968 | 27,888,221 | 27,888,221 | 0.63 | 0.40 | 255 (0.48%) | 0.00 | - | - | 0 (0%) |
| Benin | 2018-19 | 148,254 | 68,929,997 | 68,929,997 | 1.00 | 0.56 | 301 (0.2%) | 0.00 | 0.0 / 0.0 | 0.22 / 0.31 | 0 (0%) |
| Benin | 2021-22 | 157,826 | 82,811,612 | - | 0.93 | 0.59 | 6 (0.0%) | 0.00 | - | 0.23 / 0.25 | 0 (0%) |
| Togo | 2018 | 83,552 | 31,676,795 | 31,676,795 | 1.57 | 0.75 | 166 (0.2%) | 0.00 | 0.01 / 0.0 | 0.24 / 0.05 | 0 (0%) |
| Togo | 2021-22 | 108,057 | 31,746,379 | - | 86.78 | 87.07 | 11952 (11.06%) | 0.00 | 3.24 / 3.18 | 36.78 / 36.37 | 0 (0%) |
| CotedIvoire | 2018-19 | 218,224 | 105,011,085 | 105,011,085 | 1.17 | 0.45 | 1231 (0.56%) | 0.00 | - | - | 0 (0%) |
| CotedIvoire | 2021-22 | 233,016 | 121,393,491 | 121,393,491 | 12.07 | 4.44 | 401 (0.17%) | 0.00 | 3.74 / 1.61 | 1.14 / 0.21 | 0 (0%) |

"Served purchased rows" and Expenditure C are the raw reconstruction, which matches every recorded C
exactly.  For the four 2021-22 waves:

- off-grid q3 touches 0.04-0.73% of served purchased rows and 0.09-0.63% of Expenditure;
- phantom `Jamais` purchases are 30-255 rows (0.03-0.48%), and every one has Expenditure NA, because there
  is no last purchase;
- so no 2021-22 pattern found here touches more than 0.73% of served purchased rows or 0.63% of their Expenditure.

Mali 2018-19 was also checked against the **served table**, built cold in the private root:

- 129,085 purchased rows, against 129,599 reconstructed (-0.4%, not traced);
- `Riz local Gambiaka`/`Kg` purchased Quantity is 55,895.1 kg.  That is exactly the 23,581.7 kg of
  ordinary rows plus the block's 32,313.4 kg;
- the build warned only `QuantityImplausibleWarning` (1 row).  There was no grain-collapse warning, and
  no block row was summed into an ordinary one (1 household has both).

## Baseline findings (already served on development, not recorded anywhere)

### Mali 2018-19: one constructed rice row per household

| | value |
|---|---|
| rows in `s07b_me_mli2018.dta` with `vague` missing | 3,357, **all** `Riz local Gambiaka` / `kg` / `Unité de taille unique`, all with q3 > 0 |
| q3 = 1.4 x `hhsize` exactly (0.2 kg per person per day x 7) | 3,348 of 3,357 (3,354 have a welfare record) |
| households | 3,357 of the wave's 6,596, one row each; 3,351 also have ordinary rows with a `vague`; **1** also has an ordinary rice-kg row |
| `s07bq06` | `Plus de 30 jours` on all 3,357; own production 0, gift 0, `s07bq07a` and `s07bq08` never filled |
| quarter grid | 22.6% on grid (H 1,442, L 1,158, G 757: 1.4 x hhsize in double precision reads as 4+ decimals) |
| served | purchased `Quantity` = q3 on 3,357 rows (2.59% of served purchased rows), `Expenditure` NA (0% of Expenditure) |
| served kg | 32,313.4 kg, **57.8% of served `Riz local Gambiaka`/`Kg` purchased kilograms** in that wave |

It cannot be a household's answer.  The value is a fixed ration times household size, the row has no
passage, and no purchase is recorded.  Over half the households in the wave receive it, and these are the
households that report no `Riz local Gambiaka` in kg otherwise; other rice items and units were not
checked.  No document held explains it, and Mali's CONTENTS.org does not record it.  Being `Kg` rows with
`Expenditure` NA, it should pass into `food_quantities` (expected, not measured) and cannot enter a
kg-ladder price baseline or `food_expenditures`.  2021-22 has no such block (0 rows without `vague`).  By the maintainer's
stance this is an undocumented imputation being served; whether to drop it, key it, or keep it is for
the maintainer.

### Niger 2018-19: `s07bq07a` is computed on 14% of rows (side finding, not an `s07bq03a` verdict)

| | value |
|---|---|
| positive `s07bq07a` off the quarter grid | 15,264 of 107,263 (14.2%); 2021-22: 15 of 112,923 |
| decimals of those values | 14,908 have 13-18 decimals, i.e. double-precision arithmetic, not entry |
| spread | 10.2 / 14.0 / 18.5% of a grappe's rows (p10/p50/p90); 14.2% in each vague; every unit type (Unité 9.2%, Kg 15.3%, other 19.9%) |
| unit value `s07bq08/s07bq07a` repeats within (item, unit, size, region) across 3+ distinct `s07bq08` | **92.8%** of off-grid rows against 65.4% on grid; top values 45.325 (323 rows), 49.175 (304), 143.85 (293), 45.238 (259) |
| served impact | 18.3% of valued served purchased rows, **26.0% of served Expenditure** |

So those last-purchase quantities are computed, not entered, and the served Expenditure on those rows
passes through them.  **The mechanism is not identified.**  A repeating unit value fits "`s07bq08` divided by
a reference price" and equally "quantity x a per-cell constant".  The top values are consistent with
either: 0.5516 x 45.325 = 25 FCFA, 0.5084 x 49.175 = 25, 0.3476 x 143.85 = 50.  `s07bq08` is a multiple of
25 FCFA on only 79.1% of these rows, against 99.0% on grid, which a pure reference-price division does not
explain.  Outside the brief; not chased further.  `s07bq07a` is also filled on `Jamais` and
`Plus de 30 jours` rows (3,086 and 17,924), with `s07bq08` empty there (2 exceptions), so it does not change served
Expenditure there.  Togo 2018 leaves those blank.  Niger's CONTENTS.org does not record it.

## Check 5. What the four CONTENTS.org files (development `e5fdd2fa8`) already record

| country | grid share or pattern recorded? | nearest record (quoted) |
|---|---|---|
| Senegal | no | GH #1489, 2018-19: "three rows, all product 132 …, where =s07bq03a= is 4.2e-19 or 8.9e-19 … No other positive =s07bq03a= or =s07bq07a= in the file is below 1e-3, and =s07b= of the other eleven EHCVM country-waves has none." |
| Mali | no | the #876 note shared by all four: "The construction is validated on Togo 2018 and Guinea-Bissau 2018-19, where the join works: the served rule reproduces =depan(Achat)= within 1% for 99.5% / 100% of the discriminating rows" |
| Niger | no | the same #876 note; nothing on `s07bq07a` decimals |
| Burkina Faso | no | the same #876 note; its variable list calls =s07bq03a= "quantity CONSUMED over the last 7 days" |

Each records the A / B / C Expenditure totals that this run reproduces.  None records a quarter-grid
share, a q3/q4 mode, a `Jamais` residual count, the Mali rice block or the Niger `s07bq07a` arithmetic.
Togo's branch CONTENTS warns that a `depan` match proves only the arithmetic, not that the column is raw.
That applies to the #876 note above.

**Suggested CONTENTS.org additions** (the coordinator's call; nothing was written):

- **All four:** the 2021-22 grid shares and the mode-1.0 result, as Benin records them.
- **Mali:** the 2018-19 rice block.
- **Niger:** the 2018-19 `s07bq07a` arithmetic.

## Appendix: sources and merges

| country | wave | s07b (from wave _/) | md5 | rows | welfare md5 | region from | region match % (q3>0) | eqadu1 match % (q3>0) | q3 dtype | q3>0 float32-exact % |
|---|---|---|---|---|---|---|---|---|---|---|
| Senegal | 2018-19 | Senegal/2018-19/_/../Data/s07b_me_sen2018.dta | 81ca34b9294020363cbd890c383fc529 | 214,528 | ff3466613e508629e7d8f3ba4ed1a2d0 | welfare.region | 100.00 | 100.00 | float64 | 98.80 |
| Senegal | 2021-22 | Senegal/2021-22/_/../Data/s07b_me_sen2021.dta | df37cb1b84aec05f455d8b62e6a33359 | 219,771 | 53d8bc73be643367fa62c7a2f8535f85 | welfare.region | 100.00 | 100.00 | float64 | 99.32 |
| Mali | 2018-19 | Mali/2018-19/_/../Data/s07b_me_mli2018.dta | 0ee2b494d19765edfbc67774f48e5b1e | 159,968 | f35d9e5977c7ec2ed056afa63855cb72 | welfare.region | 99.96 | 99.96 | float64 | 98.05 |
| Mali | 2021-22 | Mali/2021-22/_/../Data/s07b_me_mli2021.dta | 04bf0d33c4a3ac52246a6e041ed5c963 | 146,360 | 57211f006748ea3906aef470bf328794 | welfare.region | 100.00 | 100.00 | float64 | 99.75 |
| Niger | 2018-19 | Niger/2018-19/_/../Data/s07b_me_ner2018.dta | 7d3c5c9c6b070dc4f113c706454495a9 | 107,269 | b99d858fe0fe0c92d21583bd03f52e7f | welfare.region | 100.00 | 100.00 | float64 | 99.19 |
| Niger | 2021-22 | Niger/2021-22/_/../Data/s07b_me_ner2021.dta | 144741a1de7765624a424b0d62482794 | 737,458 | f4f967ba0b672e1bd47c86e82f632efe | welfare.region | 100.00 | 100.00 | float64 | 99.96 |
| Burkina_Faso | 2018-19 | Burkina_Faso/2018-19/_/../Data/s07b_me_bfa2018.dta | c57e9ca62c2995a144de734063476ead | 136,888 | 59728c26f4746a05bdcd9a6c098dfe20 | welfare.region | 100.00 | 100.00 | float64 | 99.36 |
| Burkina_Faso | 2021-22 | Burkina_Faso/2021-22/_/../Data/s07b_me_bfa2021.dta | f6a5a9880763d2b2fdfc986ccd6b4d2e | 584,087 | c55e6649d9ed6fe0a03494439fcbe704 | welfare.region | 100.00 | 100.00 | float64 | 99.48 |
| Benin | 2018-19 | Benin/2018-19/_/../Data/s07b_me_ben2018.dta | 8e90e75a24df0ccfa86e8d2c8d72af5e | 187,672 | 71ad72ac61da3f90afbc0b31102c551a | welfare.region | 100.00 | 100.00 | float64 | 99.21 |
| Benin | 2021-22 | Benin/2021-22/_/../Data/s07b_me_ben2021.dta | 21b9e9616b95b986e1161f538624c4a4 | 1,333,313 | cd46c4e8b64ec31522cb3dcad1f61367 | s00q01 | 100.00 | 100.00 | float64 | 99.08 |
| Togo | 2018 | Togo/2018/_/../Data1/s07b_me_tgo2018.dta | e3091f1a15f04cb2617ffc8ef17adf3e | 112,087 | 58a5b2738282d0691cd4cae62f2b864f | welfare.region | 100.00 | 100.00 | float64 | 98.68 |
| Togo | 2021-22 | Togo/2021-22/_/../Data/s07b_me_tgo2021.dta | 9fa32cf85c4f53c97df5860aecb35ec2 | 110,686 | 9bf6b71bcddb81eb3ae83dee11f303a3 | welfare.region | 100.00 | 100.00 | float64 | 14.65 |
| CotedIvoire | 2018-19 | CotedIvoire/2018-19/_/../Data/Menage/s07b_me_CIV2018.dta | a5e2a722347043325e7a86542db81e8c | 292,741 | - | s00q01 | 100.00 | - | float64 | 99.02 |
| CotedIvoire | 2021-22 | CotedIvoire/2021-22/_/../Data/s07b_me_civ2021.dta | d1ad349d011e981602756dd32c58f596 | 2,242,945 | b7d34b42a58ca4839549c72b3084d1da | welfare.region | 100.00 | 100.00 | float64 | 93.93 |


## Files (all beside this report)

| file | what |
|---|---|
| `check_s07bq03a.py` | checks 1-4, one JSON per country in `out/` |
| `followup_s07bq03a.py` | H fractions, per-capita signature with permutation null, rows without `vague`, `s07bq07a` profile, cross-wave ratio keyed on unit type (`out_followup/`; Senegal and Togo rerun after a pandas-3 NaN key fix in `out_followup_rerun/`) |
| `mali_rice_check.py` | Mali 2018-19 rice block, raw and served (`out_followup/Mali_rice.json`) |
| `niger_q7a_check.py`, `niger_q7a_uv_check.py` | the Niger 2018-19 `s07bq07a` side check (`out_followup/Niger_q7a*.json`) |
| `render_tables.py` | builds `tables.md` (every table, per-region detail) and `compact.md` (Tables 1-4 here) |
| `peek.py` | console summary used during the run |
| `run_check.sbatch`, `run_followup.sbatch`, `queue/` | the burst jobs and the queue scripts with their logs |
| `logs/` | Slurm logs |
