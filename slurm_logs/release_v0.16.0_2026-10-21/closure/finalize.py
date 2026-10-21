"""Render approved aggregate closure exports; no Git/GitHub/source execution."""
import hashlib
import json
from pathlib import Path

ROOT=Path('/local/job40255479/control/release-v016/closure')
summary=json.loads((ROOT/'summary.json').read_text())
extra=json.loads((ROOT/'additional-16-proposals.json').read_text())
residuals=json.loads((ROOT/'retained-residuals.json').read_text())
identity=json.loads((ROOT/'static-identity.json').read_text())
ids=summary['combined48_if_parent_accepts_proposals']
assert len(ids)==48
summary['final_safe_closure_issues']=ids
summary['parent_accepted_all16_additional_proposals']=True
summary['acceptance_basis']='Parent explicitly accepted all16 after inspecting summary, scope decisions and source/test anchors; user authorized shipping0.16.0.'
(ROOT/'summary.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n')
keywords='\n'.join('Closes #'+str(n) for n in ids)+'\n'
(ROOT/'closing-keywords-48.txt').write_text(keywords)
body="""Release v0.16.0 from development, including the reviewed framework backlog fixes, country-preserving agricultural transformations, lossless held/latest provenance, cache/read safeguards, audit participation checks and bounded CI fingerprint repairs.

Validation: executable runtime/configuration/tests/build/audit files at development acbd8eb34710e85d3bf66eb04f13dd0d971d1f3c are byte-identical to validated candidate532d58700. Its credentialed Linux run passed6388 tests with2744 explicit skips, four expected failures and one non-strict unexpected pass; unit, Windows smoke and native Windows passed. The nutrition case and all87 rate regressions actually executed and passed. This is exact-head release evidence, not a claim that ordinary PR data-tests are enforced.

Close only the accepted scope of the48 audited issues below. Retain the77 referenced open residual scopes, including778 trust mode,932 enforcement,1044 decoding,1045 calendar migration,1066 assumed-fresh grain replay,1110 land-grain science and1149 acquisition staging. The19 source/wiring gaps and five tenure findings remain separate limitations. No live acquisition or scientific-default change is claimed.

Merge this development-to-master release PR with a merge commit to preserve release ancestry.

"""+keywords
body=body.replace('candidate532','candidate 532').replace('passed6388','passed 6388').replace('with2744','with 2744').replace('all87','all 87').replace('the48','the 48').replace('the77','the 77').replace('including778','including #778').replace(',932',', #932').replace(',1044',', #1044').replace(',1045',', #1045').replace(',1066',', #1066').replace(',1110',', #1110').replace('and1149','and #1149').replace('The19','The 19')
(ROOT/'release-pr-body.md').write_text(body)
lines=['#+TITLE: Final v0.16.0 closure and contents audit','#+AUTHOR: Sue','','* Result','PASS at development ='+summary['development']+'= against unchanged master/v0.15.0 ='+summary['master_v015']+'=.','The final safe closure list has 48 issues: the previous32 plus16 additional proposals explicitly accepted by the parent after source/test review.','Every closing-keyword export uses a separate Closes keyword for every issue.','All48 are currently OPEN and each remaining-scope supporting fix is merged after the tag and in actual development ancestry.','','* Fresh scope and inventory coverage','Connected GitHub refreshed130 issue bodies plus the already-merged PR917 reference, including every available comment; metadata comment counts agree for every record.','All32 prior issue comment sets and30 bodies are unchanged;1159/1160 body updates record the already-approved contracts and merged implementation.','All50 actual post-tag PRs are covered and their merged states were independently refreshed.','Git ancestry identifies49 standard Merge-pull-request messages;1153 uses a custom Merge-PR message and is separately verified by its exact GitHub merge SHA.','The fresh200-PR input is a repository inventory, not a keyword-based declaration that every mentioned issue is resolved.','Existing partial work already in v0.15 is distinguished from post-tag remaining-scope fixes in the prior audit; every cited remaining-fix merge is absent from v0.15.','','* Additional16, now accepted','| Issue | Fix PR | Accepted remaining scope |','|-------+--------+--------------------------|']
for item in extra:lines.append(f"| {item['issue']} | {item['pr']} | {item['scope']} |")
lines+=['','Each proposal record includes the complete body hash, all comment hashes/URLs, exact merged PR ancestry, runtime blob hashes, source symbol/line anchors and named test lines.','No raw issue/comment excerpts, survey record examples or identifiers are included in the safe exports.','Already-closed953/958/960 require no new closing keyword.','','* Retained scope and scientific boundaries','The retained-residuals JSON enumerates77 refreshed referenced OPEN scopes; this is not the complete repository backlog.','Keep778,932,1044,1045,1066,1110 and1149 open.','The generic fixed comment on1066 does not implement its explicit assumed-fresh replay residual: Country._read_assumed_fresh_cache still omits the grain replay (country.py4678).','The narrow805 ruling leaves DOS/format118 classes outside that accepted correction; its named805-a/805-b residuals remain disclosed.','Kinship974 folds current case lookup collisions; broader punctuation/whitespace normalization is a separate decision.','Country guidance was respected: Nigeria units vary by wave and W1-PH card weights have a quantity-mode rule; GhanaLSS screener No differs from missing; Malawi Unknown may retain authoritative survey kilograms; Burkina2014 measurement/area fields are not coordinates; community-price variety/visit/observation meanings differ by survey.','The current Uganda nutrition approval is not an ongoing historical hold; the bounded numerical variant repair does not settle FCT/scientific questions.','No new survey measurement, source acquisition, scientific inference or cache build was performed.','','* Commit closing-keyword audit','All full commit messages in the exact v0.15.0..acbd ancestry were scanned, including merged side histories.','Four closing-keyword occurrences refer only to1099 (verified closure) and917/1028 (already-merged PRs, not issues).','No unverified issue closure keyword was found; none targets778/932/1149/1110.','Conventional fix(#N) scopes are not GitHub closing keywords.','The release PR must use the explicit48-line export; partial umbrellas are not promoted by their appearance in older PR descriptions.','','* Static contents and limits',f"Only {identity['difference_path_count']} tracked evidence/handoff paths differ from validated532; the JSON enumerates every difference.",'All common runtime/package/country configurations, tests/fixtures, scripts/bin/tools, bench/audit, workflows and build/dependency blobs/types/modes are identical.','v0.15 and validated532 are development ancestors; source was read only through immutable Git objects.','This does not rerun tests or packaging, reproduce a runtime environment, or establish a tagged package version or enforced PR-event credentialed job.','Later release-note/tag/master changes require their own exact pin; this audit performs none of them.','','* Reproduction and privacy','See REPRO.org and COPYLIST.txt.','Private connected issue/PR snapshots are prerequisites for replay and are deliberately excluded from the safe copy list.','The raw private issue snapshots must never be copied into the release evidence directory.','','-- Sue']
s='\n'.join(lines)+'\n'
for a,b in [('previous32','previous 32'),('plus16','plus 16'),('All48','All 48'),('refreshed130','refreshed 130'),('PR917','PR 917'),('All32','All 32'),('and30','and 30'),(';1159','; 1159'),('All50','All 50'),('identifies49','identifies 49'),(';1153','; 1153'),('fresh200','fresh 200'),('Additional16','Additional 16'),('closed953','closed 953'),('enumerates77','enumerates 77'),('Keep778','Keep 778'),('on1066','on 1066'),('country.py4678','country.py:4678'),('narrow805','narrow 805'),('named805','named 805'),('Kinship974','Kinship 974'),('Burkina2014','Burkina 2014'),('to1099','to 1099'),('and917','and 917'),('targets778','targets 778'),('explicit48','explicit 48'),('validated532','validated 532')]:s=s.replace(a,b)
(ROOT/'REPORT.org').write_text(s)
(ROOT/'REPRO.org').write_text('''#+TITLE: Read-only release closure reproduction
#+AUTHOR: Sue

The snapshot source is immutable development acbd8eb34710e85d3bf66eb04f13dd0d971d1f3c.
The previous release is annotated v0.15.0 peeled to cb44e3522a2426ba03431ca9a218cc63fe03f6c9.
The validated executable source is532d58700fa46e52a0b390198604eca1dc4de00e.

Connected GitHub fetch_issue and fetch_issue_comments supplied every body and every comment, with count equality checked.
Connected get_pr_info independently refreshed all50 actual post-tag PRs.
The parent supplied the fresh200 merged-PR inventory; ancestry and every matching merge SHA were checked locally.
All provider snapshots remain in private-issues/ and private-prs/, mode0600, and are not safe publication artifacts.

Run the mounted image Python on reconcile.py, then finalize.py from this private report directory.
These controllers import only standard-library modules and use Git object reads; they never import repository code or execute tests/builds/data access.
reconcile.py expects the private snapshots and the unchanged source/reference pins; later issue closures or moving release refs require a separately named snapshot rather than rewriting this frozen evidence.
finalize.py records the parent's already-given acceptance of the16 additions; it does not request or infer a new approval.

For an independent GET-only refresh outside connected tools, gh api issues/N and gh api --paginate issues/N/comments provide the corresponding REST metadata.
Preserve all pages and normalize provider field names before replay; no new data source or country API is involved.
Never copy those raw responses into the public evidence directory.

This audit performs no repository/GitHub/cache mutation, release/tag, acquisition, or publish action.
''')
files=['REPORT.org','summary.json','core-32.json','additional-16-proposals.json','actual-posttag-pr-inventory.json','retained-residuals.json','already-closed.json','static-identity.json','commit-keyword-audit.json','closing-keywords-48.txt','release-pr-body.md','scope-decisions.json','reconcile.py','finalize.py','REPRO.org','COPYLIST.txt','MANIFEST.json']
(ROOT/'COPYLIST.txt').write_text('\n'.join(files)+'\n')
def inspect(value):
 if isinstance(value,dict):
  assert not ({'body','context_line','raw_rows','records_raw'}&value.keys()),value.keys()
  for item in value.values():inspect(item)
 elif isinstance(value,list):
  for item in value:inspect(item)
for name in files:
 if name.endswith('.json') and name!='MANIFEST.json':inspect(json.loads((ROOT/name).read_text()))
manifest={name:{'bytes':(ROOT/name).stat().st_size,'sha256':hashlib.sha256((ROOT/name).read_bytes()).hexdigest()} for name in files if name!='MANIFEST.json'}
(ROOT/'MANIFEST.json').write_text(json.dumps({'safe_files':manifest,'excluded_private':['private-issues/','private-prs/','commit-keywords-inventory.json'],'aggregate_only':True},indent=2,sort_keys=True)+'\n')
print(json.dumps({'safe_final_closures':48,'safe_copy_files':len(files),'all_safe_JSON_excludes_raw_body_and_record_fields':True,'all_readers_writers_stopped_after_this_controller':True},sort_keys=True))
