import json 

import pandas as pd

file = '/Users/nhatnguyen/Downloads/extra/geoidmapping.json'
with open(file) as f:
  data = json.load(f)
  inv_map = {v: k for k, v in data.items()}
  with open('/Users/nhatnguyen/Downloads/extra/geoid_c.json', 'w') as wf:
      json.dump(inv_map, wf)

