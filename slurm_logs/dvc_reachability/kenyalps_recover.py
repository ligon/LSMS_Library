"""Recover the 12 KenyaLPS blobs never pushed to S3 (GH #750) from Harvard Dataverse.

Downloads each file whose sidecar is absent from the remote, writes it to its
workspace path, md5s it and compares with the sidecar.  Does NOT push and
does NOT touch any sidecar.  Sidecars named *.dta take the ORIGINAL upload
(``?format=original``); sidecars named *.tab take Dataverse's tab-delimited
ingest derivative (the default access format), which is what was committed.
"""
import hashlib, json, sys, os, urllib.request, pathlib, yaml
UA = {'User-Agent': 'Mozilla/5.0 lsms_library-recovery (GH #750)'}
# Dataverse API token (guestbook-gated datasets).  Read from a file named in
# DATAVERSE_TOKEN_FILE; never echoed.  Sent only as the X-Dataverse-key header.
_tf = os.environ.get('DATAVERSE_TOKEN_FILE')
if _tf and pathlib.Path(_tf).exists():
    UA['X-Dataverse-key'] = pathlib.Path(_tf).read_text().strip()
C = pathlib.Path('/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/lsms_library/countries')
MISSING = '/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/dvc_reachability/missing_2026-09-02.tsv'
DOIS = {'2007-2009': 'doi:10.7910/DVN/PBFXVK', '2011-2014': 'doi:10.7910/DVN/PXVFJD', '2017-2022': 'doi:10.7910/DVN/O0BBKI'}
def get_json(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={**UA, 'Accept': 'application/json'}), timeout=60) as r:
        return json.loads(r.read())
def download(url, dest):
    h = hashlib.md5(); n = 0
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=600) as r, open(dest, 'wb') as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk: break
            f.write(chunk); h.update(chunk); n += len(chunk)
    return h.hexdigest(), n
rows = [l.split('\t') for l in open(MISSING).read().splitlines()[1:]]
targets = [(r[0], r[1]) for r in rows if r[0].startswith('KenyaLPS/')]
print(f"{len(targets)} KenyaLPS sidecars to recover", flush=True)
results = []
for wave, doi in DOIS.items():
    ds = get_json(f'https://dataverse.harvard.edu/api/datasets/:persistentId/?persistentId={doi}')
    files = ds['data']['latestVersion']['files']
    for sc_rel, md5 in targets:
        if f'/{wave}/' not in sc_rel: continue
        name = pathlib.Path(sc_rel).stem            # strip .dvc
        want_original = name.endswith('.dta')
        match = None
        for f in files:
            df = f['dataFile']; label = f.get('label') or df.get('filename'); orig = df.get('originalFileName') or label
            if (want_original and orig == name) or (not want_original and label == name):
                match = df; break
        if match is None:
            print(f"  NO MATCH  {sc_rel}", flush=True); results.append((sc_rel, 'no-match', '', 0)); continue
        dest0 = C / sc_rel[:-4]
        if dest0.exists() and hashlib.md5(dest0.read_bytes()).hexdigest() == md5:
            print(f"  ALREADY   {sc_rel} (workspace copy md5-matches)", flush=True); results.append((sc_rel, 'MATCH', md5, dest0.stat().st_size)); continue
        url = f"https://dataverse.harvard.edu/api/access/datafile/{match['id']}" + ('?format=original' if want_original else '')
        dest = C / sc_rel[:-4]
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            got, n = download(url, dest)
        except Exception as e:
            print(f"  FAILED    {sc_rel}: {e}", flush=True); results.append((sc_rel, 'failed', str(e), 0)); continue
        status = 'MATCH' if got == md5 else 'md5 DIFFERS'
        print(f"  {status:11s} {sc_rel}  {n/1e6:.1f} MB  sidecar={md5} got={got}", flush=True)
        results.append((sc_rel, status, got, n))
print("\nsummary:", {s: sum(1 for r in results if r[1] == s) for s in set(r[1] for r in results)})
