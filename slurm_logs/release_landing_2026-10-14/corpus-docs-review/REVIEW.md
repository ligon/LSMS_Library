# Independent corpus/coverage documentation review

APPROVE. The one stale WORKPLAN sentence about the 2,580 cache skips has been
corrected and independently re-read: the follow-up is completed and retains
the held nutrition failure. No substantive blocker remains in this docs-only
checkpoint; the release gates themselves remain open as documented.

Reviewed the uncommitted docs-only release-docs diff based on33189b532, the
new corpus/cache-coverage archives, and the four country CONTENTS additions.
Exact reviewed file fingerprints are in reviewed-state.json. No source edits,
suite execution, cache API calls, shared cache reads, or GitHub writes.

Independent evidence checks:

- Parsed corpus/table-summary.tsv:549 unique requests across38 countries,
 530success/19error; errors are Armenia2, Nepal11, Peru6. No empty successes.
- Parsed all four private follow-up XMLs and verified all8 XML/log hashes
  against archived metadata:2416passed/168skipped/5xfail/1failure. The sole
  failure is Uganda var/nutrition.parquet. None is relabeled green.
- Reconstructed pytest node IDs from XML: all2580 original gap nodes present,
  exactly10 additional cases; original outcomes2406pass/168skip/5xfail/1fail.
  Counts overlap the original full suite and the draft explicitly says so.
- Structural skip categories sum150 (66unavailable/unwired+52JSONmapping+
  32nonpersistentderived); ordinary18 comprise8raw-weight checks and10legacy
  stored-table preconditions. Coverage debt is explicit, not converted to
  successful validation. The independent replication-project15 skips belong
  to the earlier full suite and are not added to the168 follow-up skips.
- Copy proof records1519files/340885785bytes, equal hashes, distinct regular
  file inodes and unchanged original. Git comparison7fa05179→4f1f044bb changes
  only CONTENTS documents within lsms_library, supporting identical runtime
  and build inputs. No new cache verification was performed by this review.
- Feature and credentialed GitHub gates remain pending. The scientific
  nutrition hold stays open; arithmetic/source replay is not maintainer
  approval. #1003/profile changes do not claim #941 causation.

Country CONTENTS were read first, including TODO/WAITING/LOGBOOK context.
Named idiosyncrasies and preserved contradictions:

- Nepal: NSO-licensed acquisition and separately sampled NLSS-II/III panel
  components. "all features return empty DataFrames" contradicts11 current
  explicit failures; the dated addition records that observation without
  pretending that sources or panel-weight questions are resolved.
- Pakistan: deliberate urban oversampling means unweighted estimates are not
  national. The quoted claim that "sampling weights have been calculated,
  and have been incorporated into the PIHS data sets that are distributed"
  is kept alongside current4967-row sample output containing v/Rural/strata.
  The text explicitly rejects an inference that upstream weights never existed.
- Tajikistan: plotcode restarts across owned/rented subsections, requiring
  tenure-tagged IDs; AreaUnit names native sotka while Area is hectares.
  "Area is in sotka (1/100 ha)" in the registry comment contradicts q3/100
  and the canonical schema. Source/ancestry confirm dedb1c7a7 predatesv0.14;
  the88238→882.38 checksum is an old aggregate difference, not a new conversion
  or nationally weighted estimate. The1999 oversample exclusion caveat stays.
- Uganda: nutrition/income can rewrite sibling fct/earnings without footer
  metadata. "trust once, then stamp" is identified as the pre-#809 country
  policy, not current behavior. The two hashless final siblings and #788
  ownership question remain visible; no restamping or nutrition fixture
  authorization is inferred. Existing scientific and survey caveats remain.

No substantive blocker found. Private reproduction:
check_evidence.py and evidence-check.json in this control folder. The source
worktree stayed read-only. CI metadata/log monitoring remains active separately.
