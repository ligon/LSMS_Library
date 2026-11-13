"""Read downloaded published metadata; no package import or installation."""
import email,json,tarfile,zipfile
from pathlib import Path
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
out=Path('/local/job40255479/control/release-v016/published-0.16.0')
verified=json.loads((out/'verification.json').read_text())
assert verified['version']=='0.16.0' and verified['missing_or_changed_package_files']==0
records={r['filename']:r for r in verified['downloads']}
metadata={}
for name in records:
 p=out/'dist'/name
 if name.endswith('.whl'):
  with zipfile.ZipFile(p) as z:
   names=[n for n in z.namelist() if n.endswith('.dist-info/METADATA')];assert len(names)==1
   content=z.read(names[0])
 else:
  with tarfile.open(p) as t:
   names=[m for m in t.getmembers() if m.name.count('/')==1 and m.name.endswith('/PKG-INFO')];assert len(names)==1
   content=t.extractfile(names[0]).read()
 m=email.message_from_bytes(content);assert m['Version']=='0.16.0'
 assert SpecifierSet(m['Requires-Python'])==SpecifierSet('>=3.11,<4.0')
 deps={r.name.lower().replace('_','-'):r for r in map(Requirement,m.get_all('Requires-Dist',[]))}
 assert deps['fooddatacentral'].specifier==SpecifierSet('>=1.0.13,<2.0.0')
 assert deps['pandas'].specifier==SpecifierSet('>=3.0')
 metadata[name]={'requires_python':m['Requires-Python'],'fooddatacentral':str(deps['fooddatacentral']),'pandas':str(deps['pandas'])}
r={'version':'0.16.0','assertions_enabled':__debug__,'required_floors_match_tagged_pyproject':True,'artifacts':metadata}
assert __debug__
(out/'requirements-verification.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
