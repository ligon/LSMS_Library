"""Read captured terminal release evidence; publish aggregate metadata, never raw logs."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, re, shutil
P = Path(__file__).resolve().parent
SHA = 'cb44e3522a2426ba03431ca9a218cc63fe03f6c9'
RUN = 38767928967
TAG = 'v0.15.0'
def read(name): return json.loads((P / name).read_text())
def identity(path): return {'name': path.name, 'bytes': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
r = read('run.json'); release = read('release.json'); tag = read('tag-object.json'); ref = read('tag-ref.json'); py = read('pypi-version.json'); artifacts = read('artifacts.json')
assert r['databaseId'] == RUN and r['headSha'] == SHA and r['headBranch'] == TAG
assert r['status'] == 'completed' and r['conclusion'] == 'success' and r['event'] == 'release'
assert len(r['jobs']) == 2 and {j['name'] for j in r['jobs']} == {'build', 'publish'}
assert all(j['status'] == 'completed' and j['conclusion'] == 'success' for j in r['jobs'])
assert tag['object'] == {'sha': SHA, 'type': 'commit', 'url': f'https://api.github.com/repos/ligon/LSMS_Library/git/commits/{SHA}'}
assert ref['object']['sha'] == tag['sha'] and ref['object']['type'] == 'tag' and tag['tag'] == TAG
assert release['tag_name'] == TAG and not release['draft'] and not release['prerelease']
build = (P / 'build.log').read_text(); publish = (P / 'publish.log').read_text()
assert SHA in build and 'Building 0.15.0 from v0.15.0' in build and 'Building LSMS_Library (0.15.0)' in build
up = re.search(r'SHA256 digest of uploaded artifact zip is ([0-9a-f]{64})', build).group(1)
down = re.search(r'SHA256 digest of downloaded artifact is ([0-9a-f]{64})', publish).group(1)
assert up == down
artifact = next(a for a in artifacts['artifacts'] if a['name'] == 'dist')
assert artifact['digest'] == 'sha256:' + up and artifact['workflow_run']['head_sha'] == SHA
assert py['info']['version'] == '0.15.0' and len(py['urls']) == 2
files = []
for f in py['urls']:
 assert not f['yanked'] and f['filename'] in publish and f['digests']['sha256'] in publish
 files.append({k:f[k] for k in ('filename', 'size', 'packagetype', 'upload_time_iso_8601', 'url', 'yanked')} | {'sha256': f['digests']['sha256'], 'matches_publish_log_attestation_digest': True})
evidence = {
 'observed_utc': datetime.now(timezone.utc).isoformat(),
 'source_sha': SHA, 'tag': TAG, 'annotated_tag_object': tag['sha'],
 'release': {k:release[k] for k in ('id', 'html_url', 'tag_name', 'draft', 'prerelease', 'published_at')},
 'workflow': {k:r[k] for k in ('databaseId', 'headSha', 'headBranch', 'status', 'conclusion', 'event', 'createdAt', 'updatedAt', 'url')},
 'jobs': [{k:j[k] for k in ('databaseId', 'name', 'status', 'conclusion', 'startedAt', 'completedAt', 'url')} | {'log': identity(P / (j['name'] + '.log'))} for j in r['jobs']],
 'version_evidence': ['Building 0.15.0 from v0.15.0', 'Building LSMS_Library (0.15.0)'],
 'artifact': {k:artifact[k] for k in ('id', 'name', 'size_in_bytes', 'digest', 'expired')},
 'artifact_upload_download_sha256_equal': True,
 'pypi': {'json_url': 'https://pypi.org/pypi/LSMS-Library/0.15.0/json', 'project_url': 'https://pypi.org/project/LSMS-Library/0.15.0/', 'name': py['info']['name'], 'version': py['info']['version'], 'files': files},
 'capture_identities': [identity(P / n) for n in ('run.json', 'release.json', 'tag-ref.json', 'tag-object.json', 'pypi-version.json', 'artifacts.json')],
 'limits': 'Read-only workflow/log/API observation. Distribution digests are compared between the publish log attestation and public PyPI metadata; this monitor did not download/install the distributions or initiate publication.'
}
D = P.with_name('publish-ci-publishable'); D.mkdir(exist_ok=True)
(D / 'aggregate-results.json').write_text(json.dumps(evidence, indent=2) + '\n')
shutil.copy2(__file__, D / 'freeze_evidence.py')
report = f'''SCOPE DEVIATIONS

None. Read-only monitoring of the release workflow initiated by the parent. No dispatch, upload, publication, tag, source, dependency or cache changes by this agent.

Release [v0.15.0]({release['html_url']}) is published. Automatic release-event [workflow {RUN}]({r['url']}) completed successfully at exact source `{SHA}`. Annotated tag object `{tag['sha']}` peels to that source.

The build log records `Building 0.15.0 from v0.15.0` and `Building LSMS_Library (0.15.0)`. Both build and publish jobs succeeded. The uploaded and downloaded `dist` artifact SHA256 agree: `{up}`. This artifact archive digest is distinct from the individual distribution digests below.

Public [PyPI version metadata](https://pypi.org/pypi/LSMS-Library/0.15.0/json) confirms version 0.15.0 and both non-yanked files. Their SHA256 values agree with the distribution attestation subjects in the terminal publish log.

| Distribution | Bytes | SHA256 |
|---|---:|---|
'''
report += '\n'.join(f"| {f['filename']} | {f['size']} | `{f['sha256']}` |" for f in files) + '\n\n'
report += '| Job | State | Terminal log bytes | Terminal log SHA256 |\n|---|---|---:|---|\n'
report += '\n'.join(f"| [{j['name']} / {j['databaseId']}]({j['url']}) | {j['conclusion']} | {j['log']['bytes']} | `{j['log']['sha256']}` |" for j in evidence['jobs']) + '\n\n'
report += evidence['limits'] + '\n\nRaw job logs and HTTP captures remain private. This bundle contains only aggregate outcomes, public identifiers/digests and the analysis script. Earlier master CI and local make-release validation are separate evidence bundles.\n'
(D / 'REPORT.md').write_text(report)
manifest = {'source_sha': SHA, 'run_id': RUN, 'files': [identity(f) for f in sorted(D.iterdir()) if f.name != 'evidence-manifest.json']}
(D / 'evidence-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
for n in ('REPORT.md', 'evidence-manifest.json'): print(json.dumps(identity(D/n)))
