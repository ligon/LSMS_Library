# Feature audit interpretation notes

Frozen source: 7fa05179c8519f44ea329ba6387f5d6d6441a8ea. The deterministic
phases 1/2 scan completed all 39 features, 549 declared country/table pairs,
38 baseline Feature calls and 17 configured variants, with zero scope skips.
These notes explain selected findings, not a blanket approval of all
diagnostics. Raw results.jsonl and logs remain private; publishable/ contains
aggregate counts, coverage and artifact provenance. The five tenure failures
below remain FAIL; the 19 missing-source/unwired errors remain ERROR.

## Assets Aggregate Quantity drift: country-set difference verified

The harness compares the 4,655,360.11 baseline Quantity total to 992,524 under
labels='Aggregate' and reports 78.68% drift. The captured Contract-B warning
explicitly drops 20 countries without that label column and keeps Malawi,
Nigeria and Uganda. Their baseline served Quantity totals in the independent
corpus records are 286,536 + 705,988 + 0 = **992,524**, exactly the Aggregate
result. Uganda has no Quantity column in assets, so contributes zero to this
sum. This finding is explained by the documented loud structured degrade,
not by a lost additive total within the retained country set. It does not
resolve the separate Nigeria per-unit asset grain-loss warnings or the
unmatched-label warnings for all three retained countries.

## GhanaLSS food assembly: material pre-existing null-key deletion

Food-acquired baseline Feature assembly has 8,060,908 rows against 11,926,972
Country rows. This combines intentional visit aggregation with actual null-key
deletion; the whole difference must not be called lossless.

A targeted ordinary-API replay on a NEW physical Ghana-only cache copy
(`data-ghana-audit-loss`, never the original or active audit root) gives
5,340,606 Country rows and 1,704,880 Feature rows. Of the 3,635,726-row
reduction, 3,622,254 are non-null rows combined across visits and 13,472 are
null-key rows deleted outright: 92 with missing v and 13,380 with missing u.
The lost rows split as follows:

The following Quantity and Expenditure sums are **raw-column audit checksums**,
not kilograms or a comparable economic total across native units, waves and
currency regimes. Their purpose is to identify deleted cells; the per-wave
decomposition is retained for that reason.

| Wave | Rows | Quantity | Expenditure |
|---|---:|---:|---:|
| 1987-88 | 54 | 8,370 | 8,370 |
| 1988-89 | 38 | 5,182.017543859648 | 5,182.017543859648 |
| 2016-17 | 13,380 | 3,379.8 | 6,720.55 |
| Total | 13,472 | 16,931.817543859648 | 20,272.567543859648 |

Ordinary pandas sums on the country and Feature frames differ from those lost
row sums by 0.00000680 Quantity and 0.00015939 Expenditure, against totals near
1.6e10 and 1.9e10. The global scanner comparison adds further summation-order
rounding. The independent stable-summation replay below explains those
residuals; they are disclosed rather than rounded into an exact claim.

Independent reviewer replay is complete at
`control/countries/ghana-feature-review/replay.{py,json,log}`. It read only the
raw food and sample parquets, joined v on native (t,i), and invoked the existing
pure collapse helper; it made no API calls or cache writes. The same 92/13,380
null counts and 1,704,880 output rows were reproduced. With math.fsum,
**the non-null input checksum equals the output checksum exactly** for both
columns. A single stable sum over before, negated-after and negated-lost
elements leaves only 9.529e-14 Quantity and 1.463e-10 Expenditure. Subtracting
separately rounded large fsum totals leaves 1.08e-6/2.98e-6, explaining why
ordinary global sum subtraction should not be called exact. This separates
visit aggregation from null-key loss on the present input. The independent
source check also confirms union_keys_by_group is identical across the three
refs. Historical input counts are not claimed.

Historical source attribution is concrete: the ENTIRE feature.py Git blob is
`acb72acd21d4538dc569c685ae47ded53e209563` at v0.14.0
(`1528fa937bcf87f146b3af9ebfdbe18b8f912750`), pre-train development
`452302adcfdbf3af538ef06375df1ac6c5aaa8a0`, and frozen candidate
`7fa05179c8519f44ea329ba6387f5d6d6441a8ea`. The country helper source hashes
for most_complete_row and _normalize_dataframe_index also match at all three
refs. Feature._collapse_duplicate_index uses groupby(level=levels,
observed=True), retaining pandas' default dropna=True. Thus the mechanism
predates this release train. This is not a claim that historical data inputs
had the exact same row counts. Evidence and exact hashes are in ghana-food-loss.json.

The retained local issue evidence is stronger than an issue-number resemblance:
closed #847 explicitly says Feature assembly drops null keys and defines its
scope as a read-path report, "Deliberately not a fix -- the fix is per country".
Closed #323 describes duplicate-index row deletion. Their closure does not
adjudicate a Ghana-specific sentinel or justify losing these rows.

Named Ghana idiosyncrasy: visits have distinct schedules by wave and rural/
urban stratum; the first visit is roster/diary training, not consumption. The
documented contract says "In food_acquired we keep the visit level and sum
over it in the derived tables"; Feature canonical alignment drops it too,
with a warning. CONTENTS also explicitly warns that a missing index value is
a "deferred silent deletion -- served, then dropped by the first groupby".
This records the interface distinction and the live null-key defect; it does
not assume the country documentation stale. No source or baseline was changed.
The coordinator filed this independently reproduced residual as
https://github.com/ligon/LSMS_Library/issues/1008.

## Food Aggregate comparisons: separate country selection from null-key loss

The food Aggregate warning names 15 unavailable-label countries and retains
EthiopiaRHS, GhanaLSS, Malawi, Nigeria and Uganda. For food_expenditures,
those five countries' recorded baseline raw-column checksums sum to
16,841,208,012.354643; the Aggregate Feature checksum is
16,841,208,012.354641 (difference about 0.000002 from summation ordering).
Thus the reported 31.59% drift against all countries is explained by the
explicit country selection. These are audit checksums, not comparable money
totals across currencies or waves.

Food_quantities reports 0.26% Aggregate drift; its Aggregate checksum
15,755,335,319.931122 equals the retained-five Country checksum sum
15,755,335,319.93112 within summation rounding. This also reflects the country
set difference rather than an additional lost total inside the retained set.

For food_acquired, the retained-country baseline checksums are
15,802,337,681.10721 Quantity and 20,790,649,534.164604 Expenditure. Both the
all-country and Aggregate Feature calls also incur the Ghana null-key loss
quantified above. Country selection explains their between-call difference;
it does not make either call's null-key deletion lossless. The two mechanisms
are kept separate in the report.

## Region-market variants: GhanaSPS and Panama lack the cluster dependency

The ordinary declared-table corpus succeeds for both countries, but neither
declares cluster_features. Feature food variants with market='Region' catch
RuntimeError("cluster_features returned no data for any wave") for these
countries and warn while excluding them from that variant. These are real
dependency gaps, retained explicitly; the scanner's expected=True warning
hint does not decide whether a warning is acceptable. Their data_scheme blobs
are identical at v0.14.0, pre-train and frozen candidate. Relevant market helper
source hashes are recorded in market-dependency-history.json.

GhanaSPS CONTENTS's live TODO other_features.py says "2013-14 has no regions
or urban/rural data" and later "For now, all of 2013-14 has Ghana set as its
region." The latter claim is not a functioning Region-market fallback in the
current API, where the undeclared cluster table fails for every wave. That
contradiction is recorded rather than assumed stale. Named idiosyncrasy:
GhanaSPS is a household panel, not a post-planting/post-harvest survey; its
wave-2 administrative-data gap is distinct from the sampling-universe claim.
Cluster identity itself IS available through sample: wave 1 uses id3 alone
(a district composite would split one EA), and waves 2/3 use the right-anchored
EA substring because split counters vary in width. The missing dependency is
the cluster_features geography table; it must not be described as proof that
the source lacks cluster identities.

Panama CONTENTS has no cluster_features implementation or promise; its TODOs
include panama.py and kg conversions. Named regional-design idiosyncrasy:
ENV-97 planning-region domains exclude indigenous and hard-to-access areas,
which form separate domains. A future Region mapping cannot safely equate
those survey domains with every geographic province. No source change or
invented Region fallback was made in this validation. The completed scan has
eight such caught Region exceptions: GhanaSPS in all three food variants plus
household_characteristics and household_roster, and Panama in the three food
variants. They are counted separately from 52 caught source/unwired exceptions.
Independent source/documentation review is recorded in
control/countries/market-dependency-review/REVIEW.md. GhanaSPS remains tracked
under open umbrella #140 (#110 was superseded, not fixed); the coordinator
filed the Panama dependency gap as #1009.

## Five plot-tenure vocabulary failures: existing real defects, still FAIL

The scanner reports five failed declared_spellings checks in plot_features.
These exactly match the pre-existing KNOWN_UNHARMONIZED set in
tests/test_declared_spellings.py and ledger 602 section 7. They are real
unresolved vocabulary defects, not false positives or newly waived tests.
Closed issue 602 fixed vocabulary enforcement; its closure does not mean the
five country mappings were fixed. The independent cache-coverage suite exposed
the same five existing xfails, whereas this scanner retains five FAIL records.

| Country | Column | Non-null values outside the declared vocabulary |
|---|---|---:|
| Albania | Tenure | 12,358 / 16,013 |
| Albania | TenureSystem | 15,961 / 16,013 |
| Cambodia | Tenure | 2,951 / 2,951 |
| Cambodia | TenureSystem | 2,951 / 2,951 |
| Kosovo | Tenure | 6,469 / 6,469 |
| Kosovo | TenureSystem | 50 / 50 |
| Tajikistan | TenureSystem | 3,683 / 3,683 |
| Timor-Leste | Tenure | 6,308 / 6,308 |

The declared_spellings function, canonical plot_features schema and exact
known-defect declaration have identical SHA-256 hashes at v0.14.0, pre-train
development and the frozen candidate. All five country trees are unchanged
between v0.14.0 and the candidate except Cambodia's Makefile find-glob quoting.
Read-only comparisons also show every Tenure/TenureSystem column is exactly
equal between the old shared country parquet and the cold private corpus;
the files' sizes, mtimes and inodes were unchanged by that comparison. The
shared cache corroborates continuity; it is not presumed scientifically true.
Exact hashes and comparisons are in tenure-history.json. No historical cold
rebuild or historical exact input-count claim is made.

Relevant idiosyncrasies and documentation findings, after reading each country's
CONTENTS including all TODO/WAITING/LOGBOOK entries (the latter drawers are
absent in these five files):

- Albania's plot helper distinguishes post-socialist privatisation and title
  documents such as deed/usufruct from ownership and legal tenure systems. Its
  comment relies on the schema permitting country-specific vocabulary, but
  those extensions are not present in the closed vocabulary. CONTENTS records
  the separate panel-design constraint that 2003/2004 are sub-samples of 2002;
  it does not resolve these plot mappings. Do not force documentary categories
  into ownership merely to satisfy the check.
- Cambodia retains questionnaire labels, including inheritance/gift and land
  granted by government/local authority; those are not a scientifically
  justified one-to-one ownership recode. Its overview says "Cambodia is not yet
  accessible via the =Country()= API", while its later DONE data_scheme entry
  says "Cambodia is integrated into the =Country()= API" and names
  plot_features. That contradiction is retained, not silently marked stale.
- Kosovo's plot TenureSystem values describe payment arrangements, while its
  documented DWELLING source has one row per dwelling-type menu option. The
  housing grain issue and plot-tenure schema issue are distinct. CONTENTS's
  sampling quote also says "the first and fourth categories" are valid while
  listing the fourth as "Vacant"; the file already records that contradiction,
  and this audit does not repair it by inference.
- Tajikistan's plot TenureSystem is documentary title evidence (certificate,
  sealed act, no documents), not automatically a legal regime. CONTENTS records
  the 1999 Dangara/Varzob secondary sample intended for exclusion in national
  analysis. Its separate housing entry says "simply not yet wired", whereas
  the present declared housing API succeeds; that documentation mismatch is
  retained. The plot-area conversion finding remains separate from tenure.
- Timor-Leste's ambiguous Part owner category accounts for 5,099 of 6,308
  plots; no mapping was guessed. CONTENTS says "plot_features is implementable
  2007-08", while the registry and completed API actually implement it. Its
  designed 2001-to-2007 panel is explicitly unreleased, so matching household
  identifiers across those waves do not establish panel membership.

The tenure distinctions missing from the country notes are recorded here as
follow-up documentation proposals because source edits were forbidden during
the frozen audit. None of these residuals was normalized away or used to
justify a blanket clean-audit claim.
