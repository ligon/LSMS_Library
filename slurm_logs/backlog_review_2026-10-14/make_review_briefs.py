"""Compose red-team review briefs: python make_review_briefs.py <key>=<fix-brief-path> ...
Each key must have a pushed branch origin/fix/<key>.  Appends to elements.txt (key branch countries)."""
import sys, subprocess, pathlib, re
RV=pathlib.Path(__file__).parent
tmpl=open('slurm_logs/backlog_fix_2026-09-24/review_briefs/810-dvc-open.md').read()
mandate=tmpl[tmpl.index('## Mandate: REFUTE'):tmpl.index('## The task the fix agent was given')]
mandate=mandate.replace('`fix/810-dvc-open`','`fix/KEY`').replace('810','ISSUE')
elements=open(RV/'elements.txt').read().splitlines() if (RV/'elements.txt').exists() else []
for arg in sys.argv[1:]:
    key,briefp=arg.split('=',1); branch=f'fix/{key}'
    fb=open(briefp).read()
    countries='-'
    m=re.search(r'^'+re.escape(key)+r' (\S+)', open(pathlib.Path(briefp).parent.parent/'elements.txt').read(), re.M)
    if m: countries=m.group(1)
    task=fb[fb.index('## Scope note'):fb.index('## Harness rule')] if '## Scope note' in fb else fb[fb.index('## The task'):fb.index('## Harness rule')]
    diffstat=subprocess.run(['git','diff','--stat',f'development...origin/{branch}'],capture_output=True,text=True).stdout
    log=subprocess.run(['git','log','--no-merges','--format=--- %h %s%n%b',f'development..origin/{branch}'],capture_output=True,text=True).stdout
    head=subprocess.run(['git','rev-parse','--short',f'origin/{branch}'],capture_output=True,text=True).stdout.strip()
    body=(f"# RED-TEAM review of branch `{branch}` @ {head} (element `{key}`)\n\n"
      f"You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `{branch}` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of {countries if countries!='-' else 'no country (framework change)'}; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/{key}/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).\n\n"
      + mandate.replace('KEY',key).replace('ISSUE',key) +
      "## Rules the fix agent was under (2026-10-14): an open issue must describe exactly the remaining work -- if the branch resolves part of an issue, the report must DRAFT the residual; check that it did.\n\n"
      "## The task the fix agent was given\n" + task + "\n"
      f"## The branch: diffstat (vs development)\n```\n{diffstat}```\n## The branch: commit messages\n{log}\n"
      )
    (RV/'briefs'/f'{key}.md').write_text(body)
    line=f'{key} {branch} {countries}'
    if line not in elements: elements.append(line)
    print('wrote',key,branch,head,countries)
(RV/'elements.txt').write_text('\n'.join(elements)+'\n')
