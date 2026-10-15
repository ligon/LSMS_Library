# Read-only Mali interview_date warm/cold diagnosis

No source edits, table API calls, cache writes or baseline changes. The final
549-pair pass is SIGSTOPped; all owned descendants were verified quiescent.

Read Mali CONTENTS, including a search for TODO/WAITING/LOGBOOK entries (none
are present as entry headings/blocks; the file's TODO keyword declaration is
not an outstanding task). Named idiosyncrasy: 2014-15/2017-18 are EAC-I,
2018-19/2021-22 are EHCVM; survey family and source date representations differ.
The file explicitly corrects its Overview: "Only 2018-19 and 2021-22 are
EHCVM; 2014-15 and 2017-18 are EAC-I". No inference that all waves share a
survey design is made. The configured four-wave inventory remains unchanged.

Prior art: issue #798's saved September 9 evidence is the same warm-only
loss, including identical per-wave counts. Its per-wave zero-NaT probe did
not test parsing the mixed whole column. The issue's recent "both halves no
longer reproduce" claim was based on a cold Mali call and is contradicted by
the present warm evidence. Existing .coder/ledger/323-mali-pid.md explicitly
keeps interview_date/visit changes separate from its pid fix. STANDING.md,
CLAUDE.md, data_info.yml remain authoritative; no new transform is introduced.

Reuse existing `_normalize_dataframe_index`, `_enforce_declared_dtypes`,
`_enforce_canonical_dtypes`, and tests/generate_baseline.fingerprint only on
in-memory copies of the raw parquet. Compare default whole-column parsing,
per-wave parsing and format='mixed' as discriminating read-only probes, not a
source fix. Preserve indices and date values, report only aggregate counts
and digests. Direct raw parquet reads are explicitly authorized; no Country
or Feature calls or cache mutation occur.
