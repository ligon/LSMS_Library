import re,json,pathlib,subprocess,os
top=pathlib.Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip())
root=top/'lsms_library/countries'
result=[]
for path in sorted(root.glob('*/_/Makefile')):
 lines=path.read_text().splitlines()
 for n,line in enumerate(lines,1):
  if not line or line[0].isspace() or line.startswith('#') or ':' not in line:continue
  target,deps=line.split(':',1)
  if '$(VAR_DIR)/' in target and '$(VAR_DIR)/' in deps:
   result.append({'country':path.parent.parent.name,'path':str(path.relative_to(root)), 'line':n,'rule':line})
print(json.dumps(result,indent=2));pathlib.Path(os.environ['CENSUS_OUT']).write_text(json.dumps(result,indent=2)+'\n')
