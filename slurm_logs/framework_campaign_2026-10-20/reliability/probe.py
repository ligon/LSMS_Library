from pathlib import Path
import importlib.util, json, os, subprocess, tempfile
import pytest
import lsms_library
import lsms_library.country as C
from lsms_library.provenance import parse_source_org, render_source_org
root=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip())
assert Path(lsms_library.__file__).resolve().is_relative_to(root.resolve()),lsms_library.__file__
spec=importlib.util.spec_from_file_location('fake1039',root/'tests/test_gh1039_make_backend_no_country_rule.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
out={}
with tempfile.TemporaryDirectory(prefix='reliability-probes-') as tmp, pytest.MonkeyPatch.context() as mp:
 gen=m.fakeland.__wrapped__(Path(tmp),mp); fake=next(gen); m.fakeland2.__wrapped__(fake)
 mp.setenv('LSMS_BUILD_BACKEND','dvc'); c=fake.build()
 try:
  df=m._barren(c);out['1099_cold']={'shape':list(df.shape)}
 except Exception as e:out['1099_cold']={'type':type(e).__name__,'error':str(e)}
 # Deterministic delete-between-exists-and-read. The fixture path and read
 # function are private; no genuine country cache is touched.
 c.assume_cache_fresh=True;fake.plant_var()
 def gone(*a,**k):raise FileNotFoundError('synthetic concurrent eviction')
 mp.setattr(C,'get_dataframe',gone)
 try:out['778_assumed_fresh_race']={'result':str(c._read_assumed_fresh_cache(m.TABLE,c.waves))}
 except Exception as e:out['778_assumed_fresh_race']={'type':type(e).__name__,'error':str(e)}
 try:next(gen)
 except StopIteration:pass
text='SOURCE\n\n(source URL not recorded)\n\n#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n#+SECONDARY_SOURCE: https://example.org/secondary\n#+CUSTOM: keep me\n'
p=parse_source_org(text,'Synthetic','2000');rendered=render_source_org(p)
out['1115']={'url_derived_from_secondary':p.url,'extra_lost':('CUSTOM' not in rendered),'placeholder_as_prose':p.notes,'second_render_changes':render_source_org(parse_source_org(rendered,'Synthetic','2000')) != rendered}
print(json.dumps(out,indent=2))
(root/'slurm_logs/framework_campaign_2026-10-20/reliability/probe-results.json').write_text(json.dumps(out,indent=2)+'\n')
