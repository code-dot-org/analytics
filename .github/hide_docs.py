import json

# modify manifest.json in dbt/target/ to hide things

with open('dbt/target/manifest.json', 'r') as f:
  data = json.loads(f.read())

for macro in data['macros']:
  data['macros'][macro]['docs']['show'] = False

with open('dbt/target/manifest.json', 'w') as f:
  json.dump(data, f)
