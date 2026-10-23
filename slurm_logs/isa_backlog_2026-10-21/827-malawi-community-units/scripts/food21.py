import json, sys
import pandas as pd
exec(open('scripts/served_causes.py').read().split("res = {")[0])
FOOD = {1, 2} | set(range(5, 22)) | {31, 32}
food = m['code'].isin(FOOD)
blank = food & m['u'].isna()
out = {'food21_rows': int(food.sum()), 'food21_u_blank': int(blank.sum()),
       'food21_blank_by_cause': {k: int(v) for k, v in m[blank]['cause'].value_counts().items()},
       'food21_blank_by_cause_item': {f'{c}/{j}': int(v) for (c, j), v in m[blank & m['cause'].str.startswith('struck')].groupby(['cause', 'j']).size().items()}}
print(json.dumps(out, indent=1))
json.dump(out, open(sys.argv[2], 'w'), indent=1)
